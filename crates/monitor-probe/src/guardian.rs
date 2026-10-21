//! Opt-in confirmation transport. Results never enter the public observation spool.
use crate::engine::ProbeEngine;
use chrono::{DateTime, Duration, Utc};
use monitor_core::{
    model::{CheckKind, Node, Observation, Site},
    scheduler::ScheduledJob,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuardianWork {
    pub id: Uuid,
    pub lease_token: Uuid,
    pub node: Node,
    pub check: CheckKind,
    pub site: Site,
    pub references: Vec<Node>,
    pub comparison_height: Option<u64>,
    pub expires_at: DateTime<Utc>,
    pub expected_evidence: Value,
}
impl GuardianWork {
    pub fn valid(&self, site: Site, now: DateTime<Utc>) -> bool {
        self.site == site
            && self.expires_at > now
            && self.expires_at <= now + Duration::minutes(12)
            && self.references.len() <= 3
            && matches!(
                self.check,
                CheckKind::Identity | CheckKind::StateHash | CheckKind::Peers | CheckKind::Chain
            )
            && (self.check != CheckKind::StateHash || self.comparison_height.is_some_and(|h| h > 0))
    }
    pub async fn execute(&self, engine: &ProbeEngine) -> Observation {
        if let Some(height) = self
            .comparison_height
            .filter(|_| self.check == CheckKind::StateHash)
        {
            return engine
                .guardian_state_hash(
                    &self.node,
                    &self.id.to_string(),
                    self.site,
                    &self.references,
                    height,
                )
                .await;
        }
        let job = ScheduledJob {
            job_id: self.id.to_string(),
            node_id: self.node.id.clone(),
            check: self.check,
            offset_seconds: 0,
            site_epoch_offset_seconds: 0,
            request_units: 1,
        };
        engine
            .check_all_with_references(&self.node, &job, self.site, 2, &self.references)
            .await
            .remove(0)
    }
}
pub async fn fetch(
    client: &reqwest::Client,
    base: &str,
    key: &str,
    probe_id: &str,
    site: Site,
) -> Result<Option<GuardianWork>, reqwest::Error> {
    let name = if site == Site::A { "a" } else { "b" };
    client
        .get(format!("{base}/internal/v1/guardian/work"))
        .query(&[("site", name), ("probeId", probe_id)])
        .bearer_auth(key)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
}
pub async fn submit(
    client: &reqwest::Client,
    base: &str,
    key: &str,
    probe_id: &str,
    work: &GuardianWork,
    observation: Observation,
) -> Result<(), reqwest::Error> {
    client.post(format!("{base}/internal/v1/guardian/results")).bearer_auth(key).json(&serde_json::json!({"workId":work.id,"leaseToken":work.lease_token,"probeId":probe_id,"observation":observation})).send().await?.error_for_status()?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use monitor_core::model::{Network, NodeKind};
    #[test]
    fn task_requires_supported_check_correct_site_ttl_and_exact_hash_height() {
        let now = Utc::now();
        let mut task = GuardianWork {
            id: Uuid::new_v4(),
            lease_token: Uuid::new_v4(),
            node: Node::new("node", Network::SymbolMainnet, NodeKind::Api, "example.com"),
            check: CheckKind::StateHash,
            site: Site::A,
            references: vec![],
            comparison_height: None,
            expires_at: now + Duration::minutes(12),
            expected_evidence: serde_json::json!({}),
        };
        assert!(!task.valid(Site::A, now));
        task.comparison_height = Some(42);
        assert!(task.valid(Site::A, now));
        assert!(!task.valid(Site::B, now));
        task.expires_at = now;
        assert!(!task.valid(Site::A, now));
        task.expires_at = now + Duration::minutes(13);
        assert!(!task.valid(Site::A, now));
        task.expires_at = now + Duration::minutes(1);
        task.check = CheckKind::Availability;
        assert!(!task.valid(Site::A, now));
    }
    #[derive(Debug)]
    struct FixedHeightHttp;
    #[async_trait::async_trait]
    impl crate::engine::ProbeHttpTransport for FixedHeightHttp {
        async fn get_json(
            &self,
            url: url::Url,
            _timeout: std::time::Duration,
        ) -> Result<Value, crate::engine::ProbeError> {
            assert_eq!(
                url.path(),
                "/blocks/42",
                "Guardian must never read the moving chain tip"
            );
            Ok(
                serde_json::json!({"block":{"stateHash":if url.host_str()==Some("local.example") {"A".repeat(64)} else {"B".repeat(64)}}}),
            )
        }
    }
    #[tokio::test]
    async fn state_hash_confirmation_reads_only_requested_block() {
        let local = Node::new(
            "local",
            Network::SymbolMainnet,
            NodeKind::Api,
            "local.example",
        );
        let references = (0..3)
            .map(|i| {
                Node::new(
                    format!("ref-{i}"),
                    Network::SymbolMainnet,
                    NodeKind::Api,
                    format!("ref-{i}.example"),
                )
            })
            .collect::<Vec<_>>();
        let engine = ProbeEngine::new("test@example.com").with_http_transport(FixedHeightHttp);
        let observation = engine
            .guardian_state_hash(&local, "guardian-job", Site::B, &references, 42)
            .await;
        assert_eq!(observation.evidence["comparisonHeight"], 42);
        assert!(matches!(
            observation.outcome,
            monitor_core::model::Outcome::Mismatch(_)
        ));
        assert_eq!(observation.evidence["referenceConsensusCount"], 3);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DnsEndpoint {
    pub node_id: String,
    pub network: String,
    pub endpoint_id: i64,
    pub endpoint_host: String,
    pub endpoint_port: u16,
    pub endpoint_scheme: String,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuardianDnsWork {
    pub id: Uuid,
    pub lease_token: Uuid,
    pub endpoint: DnsEndpoint,
    pub site: Site,
    pub expires_at: DateTime<Utc>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuardianDnsResult {
    pub work_id: Uuid,
    pub lease_token: Uuid,
    pub probe_id: String,
    pub endpoint: DnsEndpoint,
    pub site: Site,
    pub resolver_scope: String,
    pub status: String,
    pub error_code: Option<String>,
    pub addresses: Vec<String>,
    pub observed_at: DateTime<Utc>,
}
impl GuardianDnsWork {
    pub fn valid(&self, site: Site, now: DateTime<Utc>) -> bool {
        self.site == site
            && self.expires_at > now
            && self.expires_at <= now + Duration::minutes(5)
            && self.endpoint.endpoint_port > 0
            && self.endpoint.endpoint_host.len() <= 253
            && !self.endpoint.endpoint_host.is_empty()
            && matches!(self.endpoint.endpoint_scheme.as_str(), "http" | "https")
    }
    pub async fn resolve(&self, probe_id: &str) -> GuardianDnsResult {
        let lookup = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            tokio::net::lookup_host((
                self.endpoint.endpoint_host.as_str(),
                self.endpoint.endpoint_port,
            )),
        )
        .await;
        let (addresses, error) = match lookup {
            Err(_) => (vec![], Some("dns_timeout")),
            Ok(Err(_)) => (vec![], Some("dns_error")),
            Ok(Ok(answers)) => {
                classify_dns_answers(&self.endpoint, answers.map(|a| a.ip()).collect())
            }
        };
        GuardianDnsResult {
            work_id: self.id,
            lease_token: self.lease_token,
            probe_id: probe_id.into(),
            endpoint: self.endpoint.clone(),
            site: self.site,
            resolver_scope: "system".into(),
            status: if error.is_some() { "error" } else { "ok" }.into(),
            error_code: error.map(str::to_owned),
            addresses,
            observed_at: Utc::now(),
        }
    }
}
fn classify_dns_answers(
    endpoint: &DnsEndpoint,
    mut answers: Vec<std::net::IpAddr>,
) -> (Vec<String>, Option<&'static str>) {
    answers.sort_unstable();
    answers.dedup();
    if answers.len() > 32 {
        return (vec![], Some("too_many_answers"));
    }
    let error = if answers.is_empty() {
        Some("empty_answers")
    } else if monitor_core::safety::validate_public_target(
        &endpoint.endpoint_scheme,
        &endpoint.endpoint_host,
        &answers,
    )
    .is_err()
    {
        Some("unsafe_dns_answers")
    } else {
        None
    };
    (answers.into_iter().map(|a| a.to_string()).collect(), error)
}
pub async fn fetch_dns(
    client: &reqwest::Client,
    base: &str,
    key: &str,
    probe_id: &str,
    site: Site,
) -> Result<Option<GuardianDnsWork>, reqwest::Error> {
    client
        .get(format!("{base}/internal/v1/guardian/dns-work"))
        .query(&[
            ("site", if site == Site::A { "a" } else { "b" }),
            ("probeId", probe_id),
        ])
        .bearer_auth(key)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
}
pub async fn submit_dns(
    client: &reqwest::Client,
    base: &str,
    key: &str,
    result: &GuardianDnsResult,
) -> Result<(), reqwest::Error> {
    client
        .post(format!("{base}/internal/v1/guardian/dns-results"))
        .bearer_auth(key)
        .json(result)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
#[cfg(test)]
mod dns_tests {
    use super::*;
    #[test]
    fn dns_answers_sort_dedupe_and_reject_mixed_private_and_oversized_sets() {
        let endpoint = DnsEndpoint {
            node_id: "n".into(),
            network: "symbol-mainnet".into(),
            endpoint_id: 1,
            endpoint_host: "example.com".into(),
            endpoint_port: 443,
            endpoint_scheme: "https".into(),
        };
        let (answers, error) = classify_dns_answers(
            &endpoint,
            vec![
                "8.8.8.8".parse().unwrap(),
                "1.1.1.1".parse().unwrap(),
                "8.8.8.8".parse().unwrap(),
            ],
        );
        assert_eq!(answers, vec!["1.1.1.1", "8.8.8.8"]);
        assert_eq!(error, None);
        assert_eq!(
            classify_dns_answers(
                &endpoint,
                vec!["8.8.8.8".parse().unwrap(), "127.0.0.1".parse().unwrap()]
            )
            .1,
            Some("unsafe_dns_answers")
        );
        assert_eq!(
            classify_dns_answers(&endpoint, vec![]).1,
            Some("empty_answers")
        );
        assert_eq!(
            classify_dns_answers(
                &endpoint,
                (1..34)
                    .map(|i| format!("8.8.8.{i}").parse().unwrap())
                    .collect()
            )
            .1,
            Some("too_many_answers")
        );
    }
}
