//! Site-independent wire contracts and admission helpers for community probes.

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::model::{CheckKind, Dimension, Node, Outcome};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollRequest {
    pub public_key: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollChallenge {
    pub probe_id: String,
    pub challenge: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenRequest {
    pub probe_id: String,
    pub challenge: String,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenResponse {
    pub access_token: String,
    pub expires_at: DateTime<Utc>,
    pub admission: Admission,
    #[serde(default)]
    pub monitor_contact: String,
    #[serde(default)]
    pub release_manifest_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Admission {
    Trial,
    Active,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapacityLimits {
    pub max_concurrency: u16,
    pub requests_per_second: f64,
    pub burst: u16,
    pub daily_traffic_bytes: u64,
    pub spool_bytes: u64,
}

impl Default for CapacityLimits {
    fn default() -> Self {
        Self {
            max_concurrency: 1,
            requests_per_second: 0.2,
            burst: 1,
            daily_traffic_bytes: 100 * 1024 * 1024,
            spool_bytes: 100 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum CapacityError {
    #[error("concurrency, burst, traffic, and spool limits must be positive")]
    ZeroLimit,
    #[error("requests per second must be finite and positive")]
    InvalidRate,
}

impl CapacityLimits {
    /// Larger local limits are accepted; each probe operator controls their own budget.
    pub fn validate(&self) -> Result<(), CapacityError> {
        if self.max_concurrency == 0
            || self.burst == 0
            || self.daily_traffic_bytes == 0
            || self.spool_bytes == 0
        {
            return Err(CapacityError::ZeroLimit);
        }
        if !self.requests_per_second.is_finite() || self.requests_per_second <= 0.0 {
            return Err(CapacityError::InvalidRate);
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeCapacity {
    pub available_slots: u16,
    pub remaining_daily_traffic_bytes: u64,
    #[serde(default)]
    pub pause_reason: Option<String>,
    #[serde(default)]
    pub queue_depth: u64,
    #[serde(default)]
    pub spool_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_limit_millicores: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_usage_percent: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_memory_percent: Option<u8>,
}

impl RuntimeCapacity {
    #[must_use]
    pub fn can_accept(&self, limits: &CapacityLimits, estimated_bytes: u64) -> bool {
        limits.validate().is_ok()
            && self.pause_reason.is_none()
            && self.available_slots > 0
            && self.available_slots <= limits.max_concurrency
            && self.remaining_daily_traffic_bytes <= limits.daily_traffic_bytes
            && estimated_bytes > 0
            && estimated_bytes <= self.remaining_daily_traffic_bytes
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatRequest {
    pub version: String,
    pub limits: CapacityLimits,
    pub capacity: RuntimeCapacity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Lease {
    pub lease_id: String,
    pub job_id: String,
    pub node: Node,
    pub checks: Vec<CheckKind>,
    #[serde(default)]
    pub reference_nodes: Vec<Node>,
    pub request_units_per_target: u16,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaseResponse {
    pub leases: Vec<Lease>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommunityObservation {
    pub job_id: String,
    pub node_id: String,
    pub check_kind: CheckKind,
    pub dimension: Dimension,
    pub outcome: Outcome,
    #[serde(default)]
    pub evidence: serde_json::Value,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultBatch {
    pub lease_id: String,
    pub observations: Vec<CommunityObservation>,
}

/// Central supplies the source IP from the connection, never from a client header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observer {
    pub probe_id: String,
    pub admission: Admission,
    pub is_self_hosted: bool,
    pub verified_source_ip: Option<IpAddr>,
    pub asn: Option<u32>,
}

/// An adapter derives this from a dimension's existing thresholds and evidence gates.
/// Unknown and unsupported observations are represented by `None` in a vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Verdict {
    Healthy,
    Warning,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommunityVote {
    pub observer: Observer,
    pub node_id: String,
    pub dimension: Dimension,
    pub verdict: Option<Verdict>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsensusStatus {
    Insufficient,
    Corroborated(Verdict),
    Strong(Verdict),
    Contradictory,
}

/// Evaluate one node and dimension from observed timestamps, not arrival time.
/// Repeated reports by one registered key count once even if its IP rotates. A
/// shared verified IP also contributes at most one vote. Caller selects the
/// existing dimension-specific freshness window.
#[must_use]
pub fn evaluate_consensus(
    votes: &[CommunityVote],
    node_id: &str,
    dimension: Dimension,
    now: DateTime<Utc>,
    max_age: Duration,
) -> ConsensusStatus {
    if max_age <= Duration::zero() {
        return ConsensusStatus::Insufficient;
    }
    // Conflicts inside one origin still matter even though that origin cannot
    // supply more than one supporting vote. A key also cannot pick a verdict by
    // sending different observations with the same timestamp in a different order.
    let mut latest: HashMap<&str, &CommunityVote> = HashMap::new();
    for vote in votes.iter().filter(|v| {
        v.node_id == node_id
            && v.dimension == dimension
            && v.observer.admission == Admission::Active
            && v.observer.verified_source_ip.is_some()
            && v.observed_at >= now - max_age
            && v.observed_at <= now
    }) {
        latest
            .entry(&vote.observer.probe_id)
            .and_modify(|prior| {
                if vote.observed_at > prior.observed_at
                    || (vote.observed_at == prior.observed_at
                        && (vote.verdict.is_none() || vote.observer.is_self_hosted))
                {
                    *prior = vote;
                }
            })
            .or_insert(vote);
    }
    let mut origins: HashMap<IpAddr, Verdict> = HashMap::new();
    for vote in latest.values().filter(|v| !v.observer.is_self_hosted) {
        let Some(verdict) = vote.verdict else {
            continue;
        };
        if votes.iter().any(|v| {
            v.node_id == node_id
                && v.dimension == dimension
                && v.observer.probe_id == vote.observer.probe_id
                && v.observed_at == vote.observed_at
                && v.observer.admission == Admission::Active
                && !v.observer.is_self_hosted
                && v.verdict.is_some_and(|other| other != verdict)
        }) {
            return ConsensusStatus::Contradictory;
        }
        if origins
            .insert(
                vote.observer.verified_source_ip.expect("filtered above"),
                verdict,
            )
            .is_some_and(|other| other != verdict)
        {
            return ConsensusStatus::Contradictory;
        }
    }
    let votes = independent_votes(votes, node_id, dimension, now, max_age);
    let mut verdicts = votes.iter().filter_map(|vote| vote.verdict);
    let Some(verdict) = verdicts.next() else {
        return ConsensusStatus::Insufficient;
    };
    if verdicts.any(|other| other != verdict) {
        return ConsensusStatus::Contradictory;
    }
    if votes.len() < 2 {
        return ConsensusStatus::Insufficient;
    }
    let asns: HashSet<u32> = votes.iter().filter_map(|vote| vote.observer.asn).collect();
    if votes.len() >= 3 && asns.len() >= 2 {
        ConsensusStatus::Strong(verdict)
    } else {
        ConsensusStatus::Corroborated(verdict)
    }
}

/// Return exactly the eligible voters used for public counts and consensus.
/// A newer unknown invalidates that observer's older known result.
pub fn independent_votes<'a>(
    votes: &'a [CommunityVote],
    node_id: &str,
    dimension: Dimension,
    now: DateTime<Utc>,
    max_age: Duration,
) -> Vec<&'a CommunityVote> {
    if max_age <= Duration::zero() {
        return Vec::new();
    }
    let mut by_probe: HashMap<&str, &CommunityVote> = HashMap::new();
    for vote in votes {
        if vote.node_id != node_id
            || vote.dimension != dimension
            || vote.observer.admission != Admission::Active
            || vote.observer.verified_source_ip.is_none()
            || vote.observed_at < now - max_age
            || vote.observed_at > now
        {
            continue;
        }
        by_probe
            .entry(vote.observer.probe_id.as_str())
            .and_modify(|prior| {
                if vote.observed_at > prior.observed_at
                    || (vote.observed_at == prior.observed_at
                        && (vote.verdict.is_none() || vote.observer.is_self_hosted))
                {
                    *prior = vote;
                }
            })
            .or_insert(vote);
    }
    let mut by_ip: HashMap<IpAddr, Vec<&CommunityVote>> = HashMap::new();
    for vote in by_probe.values().filter(|v| !v.observer.is_self_hosted) {
        by_ip
            .entry(vote.observer.verified_source_ip.expect("filtered above"))
            .or_default()
            .push(*vote);
    }
    let mut result = Vec::new();
    for mut group in by_ip.into_values() {
        // Different clients sharing one source cannot conceal conflicting reports.
        if group
            .iter()
            .any(|vote| vote.verdict.is_none() || vote.verdict != group[0].verdict)
        {
            continue;
        }
        group.sort_by(|a, b| {
            b.observed_at
                .cmp(&a.observed_at)
                .then_with(|| a.observer.probe_id.cmp(&b.observer.probe_id))
        });
        result.push(group[0]);
    }
    result.sort_by(|a, b| a.observer.probe_id.cmp(&b.observer.probe_id));
    result
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use chrono::{Duration, TimeZone};

    use super::*;

    fn at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 20, 12, 0, 0).unwrap()
    }

    fn vote(id: &str, ip: [u8; 4], asn: Option<u32>, verdict: Option<Verdict>) -> CommunityVote {
        CommunityVote {
            observer: Observer {
                probe_id: id.into(),
                admission: Admission::Active,
                is_self_hosted: false,
                verified_source_ip: Some(IpAddr::V4(Ipv4Addr::from(ip))),
                asn,
            },
            node_id: "n1".into(),
            dimension: Dimension::Connectivity,
            verdict,
            observed_at: at(),
        }
    }

    #[test]
    fn defaults_are_modest_but_can_be_raised() {
        let defaults = CapacityLimits::default();
        assert_eq!(defaults.max_concurrency, 1);
        assert_eq!(defaults.requests_per_second, 0.2);
        assert_eq!(defaults.burst, 1);
        assert_eq!(defaults.daily_traffic_bytes, 100 * 1024 * 1024);
        assert_eq!(defaults.spool_bytes, 100 * 1024 * 1024);
        assert!(defaults.validate().is_ok());
        let larger = CapacityLimits {
            max_concurrency: 4,
            requests_per_second: 2.0,
            ..defaults
        };
        assert!(larger.validate().is_ok());
        assert!(CapacityLimits {
            requests_per_second: f64::NAN,
            ..larger
        }
        .validate()
        .is_err());
    }

    #[test]
    fn capacity_refuses_work_without_slot_or_remaining_traffic() {
        let limits = CapacityLimits::default();
        assert!(!RuntimeCapacity {
            available_slots: 0,
            remaining_daily_traffic_bytes: 100,
            ..RuntimeCapacity::default()
        }
        .can_accept(&limits, 1));
        assert!(!RuntimeCapacity {
            available_slots: 1,
            remaining_daily_traffic_bytes: 0,
            ..RuntimeCapacity::default()
        }
        .can_accept(&limits, 1));
        assert!(RuntimeCapacity {
            available_slots: 1,
            remaining_daily_traffic_bytes: 100,
            ..RuntimeCapacity::default()
        }
        .can_accept(&limits, 100));
        assert!(!RuntimeCapacity {
            available_slots: 2,
            remaining_daily_traffic_bytes: 100,
            ..RuntimeCapacity::default()
        }
        .can_accept(&limits, 1));
    }

    #[test]
    fn two_independent_active_fresh_votes_corroborate() {
        let votes = [
            vote("p1", [192, 0, 2, 1], Some(1), Some(Verdict::Critical)),
            vote("p2", [192, 0, 2, 2], Some(1), Some(Verdict::Critical)),
        ];
        assert_eq!(
            evaluate_consensus(
                &votes,
                "n1",
                Dimension::Connectivity,
                at(),
                Duration::minutes(30)
            ),
            ConsensusStatus::Corroborated(Verdict::Critical)
        );
    }

    #[test]
    fn newer_unknown_replaces_previous_healthy_vote() {
        let healthy = vote("p1", [1, 1, 1, 1], None, Some(Verdict::Healthy));
        let other = vote("p2", [8, 8, 8, 8], None, Some(Verdict::Healthy));
        let mut unknown = healthy.clone();
        unknown.observed_at += Duration::seconds(1);
        unknown.verdict = None;
        assert_eq!(
            evaluate_consensus(
                &[healthy, other, unknown],
                "n1",
                Dimension::Connectivity,
                at() + Duration::seconds(2),
                Duration::minutes(30)
            ),
            ConsensusStatus::Insufficient
        );
    }

    #[test]
    fn trial_revoked_missing_unknown_and_stale_do_not_vote() {
        let mut trial = vote("trial", [192, 0, 2, 2], Some(2), Some(Verdict::Critical));
        trial.observer.admission = Admission::Trial;
        let mut revoked = vote("revoked", [192, 0, 2, 3], Some(2), Some(Verdict::Critical));
        revoked.observer.admission = Admission::Revoked;
        let mut stale = vote("stale", [192, 0, 2, 4], Some(2), Some(Verdict::Critical));
        stale.observed_at -= Duration::minutes(31);
        let votes = [
            vote("p1", [192, 0, 2, 1], Some(1), Some(Verdict::Critical)),
            trial,
            revoked,
            stale,
            vote("unknown", [192, 0, 2, 5], Some(2), None),
        ];
        assert_eq!(
            evaluate_consensus(
                &votes,
                "n1",
                Dimension::Connectivity,
                at(),
                Duration::minutes(30)
            ),
            ConsensusStatus::Insufficient
        );
    }

    #[test]
    fn self_hosted_evidence_never_augments_community_quorum() {
        let mut self_hosted = vote(
            "central-a",
            [192, 0, 2, 2],
            Some(2),
            Some(Verdict::Critical),
        );
        self_hosted.observer.is_self_hosted = true;
        let votes = [
            vote("p1", [192, 0, 2, 1], Some(1), Some(Verdict::Critical)),
            self_hosted,
        ];
        assert_eq!(
            evaluate_consensus(
                &votes,
                "n1",
                Dimension::Connectivity,
                at(),
                Duration::minutes(30)
            ),
            ConsensusStatus::Insufficient
        );
    }

    #[test]
    fn one_registered_probe_or_one_source_ip_cannot_multiply_votes() {
        let mut rotated = vote("p1", [192, 0, 2, 2], Some(2), Some(Verdict::Critical));
        rotated.observed_at += Duration::seconds(1);
        let same_ip = vote("p2", [192, 0, 2, 2], Some(2), Some(Verdict::Critical));
        let votes = [
            vote("p1", [192, 0, 2, 1], Some(1), Some(Verdict::Critical)),
            rotated,
            same_ip,
        ];
        assert_eq!(
            evaluate_consensus(
                &votes,
                "n1",
                Dimension::Connectivity,
                at() + Duration::seconds(1),
                Duration::minutes(30)
            ),
            ConsensusStatus::Insufficient
        );
    }

    #[test]
    fn strong_requires_three_observers_and_two_known_asns() {
        let mut votes = vec![
            vote("p1", [192, 0, 2, 1], Some(1), Some(Verdict::Healthy)),
            vote("p2", [192, 0, 2, 2], Some(1), Some(Verdict::Healthy)),
            vote("p3", [192, 0, 2, 3], None, Some(Verdict::Healthy)),
        ];
        assert_eq!(
            evaluate_consensus(
                &votes,
                "n1",
                Dimension::Connectivity,
                at(),
                Duration::minutes(30)
            ),
            ConsensusStatus::Corroborated(Verdict::Healthy)
        );
        votes[2].observer.asn = Some(2);
        assert_eq!(
            evaluate_consensus(
                &votes,
                "n1",
                Dimension::Connectivity,
                at(),
                Duration::minutes(30)
            ),
            ConsensusStatus::Strong(Verdict::Healthy)
        );
    }

    #[test]
    fn valid_disagreement_prevents_majority_judgment() {
        let votes = [
            vote("p1", [192, 0, 2, 1], Some(1), Some(Verdict::Critical)),
            vote("p2", [192, 0, 2, 2], Some(2), Some(Verdict::Critical)),
            vote("p3", [192, 0, 2, 3], Some(3), Some(Verdict::Healthy)),
        ];
        assert_eq!(
            evaluate_consensus(
                &votes,
                "n1",
                Dimension::Connectivity,
                at(),
                Duration::minutes(30)
            ),
            ConsensusStatus::Contradictory
        );
    }

    #[test]
    fn votes_are_scoped_to_node_dimension_and_actual_observation_time() {
        let mut other = vote("p2", [192, 0, 2, 2], Some(2), Some(Verdict::Critical));
        other.node_id = "n2".into();
        let mut future = vote("p3", [192, 0, 2, 3], Some(2), Some(Verdict::Critical));
        future.observed_at += Duration::minutes(3);
        let votes = [
            vote("p1", [192, 0, 2, 1], Some(1), Some(Verdict::Critical)),
            other,
            future,
        ];
        assert_eq!(
            evaluate_consensus(
                &votes,
                "n1",
                Dimension::Connectivity,
                at(),
                Duration::minutes(30)
            ),
            ConsensusStatus::Insufficient
        );
    }
    #[test]
    fn shared_origin_conflict_is_visible_even_with_two_other_supporters() {
        let now = Utc::now();
        let make = |id: &str, ip: &str, verdict| CommunityVote {
            observer: Observer {
                probe_id: id.into(),
                admission: Admission::Active,
                verified_source_ip: Some(ip.parse().unwrap()),
                asn: None,
                is_self_hosted: false,
            },
            node_id: "conflict".into(),
            dimension: Dimension::Connectivity,
            verdict: Some(verdict),
            observed_at: now,
        };
        let votes = [
            make("a", "8.8.8.8", Verdict::Healthy),
            make("b", "1.1.1.1", Verdict::Healthy),
            make("c", "9.9.9.9", Verdict::Healthy),
            make("d", "9.9.9.9", Verdict::Critical),
        ];
        assert_eq!(
            evaluate_consensus(
                &votes,
                "conflict",
                Dimension::Connectivity,
                now,
                Duration::minutes(30)
            ),
            ConsensusStatus::Contradictory
        );
        let votes = [
            make("a", "8.8.8.8", Verdict::Healthy),
            make("b", "1.1.1.1", Verdict::Healthy),
            make("a", "8.8.8.8", Verdict::Critical),
        ];
        assert_eq!(
            evaluate_consensus(
                &votes,
                "conflict",
                Dimension::Connectivity,
                now,
                Duration::minutes(30)
            ),
            ConsensusStatus::Contradictory
        );
    }
    #[test]
    fn resource_notifications_are_optional_for_existing_participants() {
        let empty: RuntimeCapacity = serde_json::from_value(
            serde_json::json!({"availableSlots":1,"remainingDailyTrafficBytes":100}),
        )
        .unwrap();
        assert_eq!(empty.cpu_limit_millicores, None);
        let reported:RuntimeCapacity=serde_json::from_value(serde_json::json!({"availableSlots":1,"remainingDailyTrafficBytes":100,
            "cpuLimitMillicores":100,"memoryLimitBytes":268435456,"cpuUsagePercent":20,"workingMemoryPercent":30})).unwrap();
        assert_eq!(reported.cpu_limit_millicores, Some(100));
        assert_eq!(reported.memory_limit_bytes, Some(268435456));
        assert_eq!(reported.cpu_usage_percent, Some(20));
        assert_eq!(reported.working_memory_percent, Some(30));
    }
}
