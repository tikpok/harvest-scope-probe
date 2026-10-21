use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::model::Network;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    Rest,
    #[default]
    P2p,
    Websocket,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointAlias {
    pub host: String,
    pub p2p_port: u16,
    #[serde(default)]
    pub kind: EndpointKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheme: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_seen_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_at: Option<DateTime<Utc>>,
}

impl EndpointAlias {
    #[must_use]
    pub fn p2p(host: impl Into<String>, p2p_port: u16) -> Self {
        Self {
            host: normalize_host(&host.into()),
            p2p_port,
            kind: EndpointKind::P2p,
            scheme: None,
            source: None,
            first_seen_at: None,
            last_seen_at: None,
        }
    }

    fn normalize(mut self) -> Self {
        self.host = normalize_host(&self.host);
        self.scheme = self.scheme.map(|scheme| scheme.to_ascii_lowercase());
        self
    }

    fn same_endpoint(&self, other: &Self) -> bool {
        self.host == other.host
            && self.p2p_port == other.p2p_port
            && self.kind == other.kind
            && self.scheme == other.scheme
            && self.source == other.source
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateRole {
    #[default]
    Primary,
    AuxiliarySeed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryCandidate {
    pub network: Network,
    pub host: String,
    pub p2p_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_public_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_key: Option<String>,
    #[serde(default)]
    pub aliases: Vec<EndpointAlias>,
    #[serde(default)]
    pub role: CandidateRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_roles: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported_health: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported_height: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported_finalized_height: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported_source: Option<String>,
}

impl DiscoveryCandidate {
    #[must_use]
    pub fn new(network: Network, host: impl Into<String>, p2p_port: u16) -> Self {
        let host = normalize_host(&host.into());
        Self {
            network,
            host: host.clone(),
            p2p_port,
            node_public_key: None,
            public_key: None,
            aliases: vec![EndpointAlias::p2p(host, p2p_port)],
            role: CandidateRole::Primary,
            symbol_roles: None,
            reported_health: None,
            reported_height: None,
            reported_finalized_height: None,
            reported_at: None,
            reported_source: None,
        }
    }

    #[must_use]
    pub fn with_node_public_key(mut self, node_public_key: impl Into<String>) -> Self {
        self.node_public_key = Some(normalize_public_key(node_public_key));
        self
    }

    #[must_use]
    pub fn with_public_key(mut self, public_key: impl Into<String>) -> Self {
        self.public_key = Some(normalize_public_key(public_key));
        self
    }

    #[must_use]
    pub fn with_alias(mut self, alias: EndpointAlias) -> Self {
        merge_alias(&mut self.aliases, alias.normalize());
        self.sort_aliases();
        self
    }

    #[must_use]
    pub const fn as_auxiliary_seed(mut self) -> Self {
        self.role = CandidateRole::AuxiliarySeed;
        self
    }

    #[must_use]
    pub fn identity_key(&self) -> String {
        if let Some(key) = &self.node_public_key {
            return format!("{}:node-pk:{}", self.network.as_str(), key);
        }
        if let Some(key) = &self.public_key {
            return format!("{}:pk:{}", self.network.as_str(), key);
        }
        format!(
            "{}:host:{}:{}",
            self.network.as_str(),
            normalize_host(&self.host),
            self.p2p_port
        )
    }

    fn identity_priority(&self) -> u8 {
        if self.node_public_key.is_some() {
            0
        } else if self.public_key.is_some() {
            1
        } else {
            2
        }
    }

    fn normalize(mut self) -> Self {
        self.host = normalize_host(&self.host);
        self.node_public_key = self.node_public_key.map(normalize_public_key);
        self.public_key = self.public_key.map(normalize_public_key);

        let aliases = std::mem::take(&mut self.aliases);
        for alias in aliases {
            merge_alias(&mut self.aliases, alias.normalize());
        }
        if self.symbol_roles.is_none_or(|roles| roles & 1 != 0) {
            merge_alias(
                &mut self.aliases,
                EndpointAlias::p2p(self.host.clone(), self.p2p_port),
            );
        }
        self.sort_aliases();
        self
    }

    fn sort_aliases(&mut self) {
        self.aliases.sort_by(|left, right| {
            (
                &left.host,
                left.p2p_port,
                left.kind,
                &left.scheme,
                &left.source,
                left.first_seen_at,
                left.last_seen_at,
            )
                .cmp(&(
                    &right.host,
                    right.p2p_port,
                    right.kind,
                    &right.scheme,
                    &right.source,
                    right.first_seen_at,
                    right.last_seen_at,
                ))
        });
    }

    fn merge_from(&mut self, other: Self) {
        self.merge_report_from(&other);
        self.node_public_key =
            canonical_optional_key(self.node_public_key.take(), other.node_public_key);
        self.public_key = canonical_optional_key(self.public_key.take(), other.public_key);
        if other.role == CandidateRole::Primary {
            self.role = CandidateRole::Primary;
        }
        self.symbol_roles = match (self.symbol_roles, other.symbol_roles) {
            (Some(current), Some(incoming)) => Some(current | incoming),
            (current, incoming) => current.or(incoming),
        };
        for alias in other.aliases {
            merge_alias(&mut self.aliases, alias);
        }
        self.sort_aliases();
    }

    fn merge_report_from(&mut self, other: &Self) {
        let incoming_is_newer = match (&self.reported_at, &other.reported_at) {
            (None, Some(_)) => true,
            (Some(current), Some(incoming)) => incoming > current,
            _ => false,
        };
        if incoming_is_newer {
            self.reported_health = other.reported_health;
            self.reported_height = other.reported_height;
            self.reported_finalized_height = other.reported_finalized_height;
            self.reported_at = other.reported_at;
            self.reported_source.clone_from(&other.reported_source);
            return;
        }

        if self.reported_at == other.reported_at {
            self.reported_health = self.reported_health.or(other.reported_health);
            self.reported_height = self.reported_height.or(other.reported_height);
            self.reported_finalized_height = self
                .reported_finalized_height
                .or(other.reported_finalized_height);
            if self.reported_source.is_none() {
                self.reported_source.clone_from(&other.reported_source);
            }
        }
    }

    fn has_p2p_endpoint(&self, host: &str, p2p_port: u16) -> bool {
        self.aliases.iter().any(|alias| {
            alias.kind == EndpointKind::P2p && alias.host == host && alias.p2p_port == p2p_port
        })
    }
}

fn normalize_host(host: &str) -> String {
    host.trim().trim_end_matches('.').to_ascii_lowercase()
}

fn normalize_public_key(key: impl Into<String>) -> String {
    key.into().trim().to_ascii_lowercase()
}

fn canonical_optional_key(current: Option<String>, incoming: Option<String>) -> Option<String> {
    match (current, incoming) {
        (Some(current), Some(incoming)) => Some(current.min(incoming)),
        (current, incoming) => current.or(incoming),
    }
}

fn merge_alias(aliases: &mut Vec<EndpointAlias>, alias: EndpointAlias) {
    if let Some(current) = aliases
        .iter_mut()
        .find(|current| current.same_endpoint(&alias))
    {
        current.first_seen_at = match (current.first_seen_at, alias.first_seen_at) {
            (Some(current), Some(incoming)) => Some(current.min(incoming)),
            (current, incoming) => current.or(incoming),
        };
        current.last_seen_at = match (current.last_seen_at, alias.last_seen_at) {
            (Some(current), Some(incoming)) => Some(current.max(incoming)),
            (current, incoming) => current.or(incoming),
        };
    } else {
        aliases.push(alias);
    }
}

#[must_use]
pub fn merge_candidates(candidates: Vec<DiscoveryCandidate>) -> Vec<DiscoveryCandidate> {
    let mut candidates = candidates
        .into_iter()
        .map(DiscoveryCandidate::normalize)
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        (
            left.network.as_str(),
            left.identity_priority(),
            left.identity_key(),
            &left.host,
            left.p2p_port,
        )
            .cmp(&(
                right.network.as_str(),
                right.identity_priority(),
                right.identity_key(),
                &right.host,
                right.p2p_port,
            ))
    });

    let mut merged: Vec<DiscoveryCandidate> = Vec::new();
    for candidate in candidates {
        let matching_index = matching_candidate_index(&merged, &candidate);
        if let Some(index) = matching_index {
            merged[index].merge_from(candidate);
        } else {
            merged.push(candidate);
        }
    }
    merged.sort_by_key(DiscoveryCandidate::identity_key);
    merged
}

fn matching_candidate_index(
    merged: &[DiscoveryCandidate],
    candidate: &DiscoveryCandidate,
) -> Option<usize> {
    if let Some(node_public_key) = &candidate.node_public_key {
        return merged.iter().position(|current| {
            current.network == candidate.network
                && current.node_public_key.as_ref() == Some(node_public_key)
        });
    }

    if let Some(public_key) = &candidate.public_key {
        return merged.iter().position(|current| {
            current.network == candidate.network && current.public_key.as_ref() == Some(public_key)
        });
    }

    let host = normalize_host(&candidate.host);
    merged.iter().position(|current| {
        current.network == candidate.network && current.has_p2p_endpoint(&host, candidate.p2p_port)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingCadence {
    Normal,
    Daily,
    Archived,
}

#[must_use]
pub fn cadence_for_missing(last_seen: DateTime<Utc>, now: DateTime<Utc>) -> MissingCadence {
    cadence_for_missing_with_reachability(last_seen, now, None)
}

#[must_use]
pub fn cadence_for_missing_with_reachability(
    last_seen: DateTime<Utc>,
    now: DateTime<Utc>,
    directly_reachable: Option<bool>,
) -> MissingCadence {
    let age = now.signed_duration_since(last_seen);
    if age <= Duration::days(7) {
        MissingCadence::Normal
    } else if age <= Duration::days(30) {
        MissingCadence::Daily
    } else if directly_reachable == Some(false) {
        MissingCadence::Archived
    } else {
        MissingCadence::Daily
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoverySourceKind {
    SymbolPrimary,
    NemCompleteCrawl,
    HtmlAuxiliary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceRunStatus {
    Success,
    Anomaly,
    Error,
    Deferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceRunMetrics {
    pub accepted_count: usize,
    pub quarantined_count: usize,
}

impl SourceRunMetrics {
    #[must_use]
    pub const fn new(accepted_count: usize, quarantined_count: usize) -> Self {
        Self {
            accepted_count,
            quarantined_count,
        }
    }

    #[must_use]
    pub const fn total_count(self) -> usize {
        self.accepted_count.saturating_add(self.quarantined_count)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceAnomaly {
    ZeroCandidates,
    CandidateCountDrop,
    QuarantineRateIncrease,
}

#[must_use]
pub fn classify_source_run(
    adapter_failed: bool,
    deferred: bool,
    previous_healthy: Option<SourceRunMetrics>,
    current: SourceRunMetrics,
) -> SourceRunStatus {
    if adapter_failed {
        return SourceRunStatus::Error;
    }
    if deferred {
        return SourceRunStatus::Deferred;
    }
    if source_anomalies(previous_healthy, current).is_empty() {
        SourceRunStatus::Success
    } else {
        SourceRunStatus::Anomaly
    }
}

#[must_use]
pub fn source_anomalies(
    previous_healthy: Option<SourceRunMetrics>,
    current: SourceRunMetrics,
) -> Vec<SourceAnomaly> {
    let mut anomalies = Vec::new();
    if current.total_count() == 0 {
        anomalies.push(SourceAnomaly::ZeroCandidates);
    }
    if let Some(previous) = previous_healthy {
        if candidate_count_is_anomalous(previous.total_count(), current.total_count()) {
            anomalies.push(SourceAnomaly::CandidateCountDrop);
        }
        if quarantine_rate_is_anomalous(
            previous.quarantined_count,
            previous.total_count(),
            current.quarantined_count,
            current.total_count(),
        ) {
            anomalies.push(SourceAnomaly::QuarantineRateIncrease);
        }
    } else if current.total_count() > 0
        && (current.quarantined_count as u128) * 100 >= (current.total_count() as u128) * 20
    {
        anomalies.push(SourceAnomaly::QuarantineRateIncrease);
    }
    anomalies
}

#[must_use]
pub fn candidate_count_is_anomalous(previous_healthy_count: usize, current_count: usize) -> bool {
    if current_count == 0 {
        return true;
    }
    previous_healthy_count > 0
        && (current_count as u128) * 100 <= (previous_healthy_count as u128) * 70
}

#[must_use]
pub fn quarantine_rate_is_anomalous(
    previous_quarantined: usize,
    previous_total: usize,
    current_quarantined: usize,
    current_total: usize,
) -> bool {
    if current_total == 0 {
        return false;
    }

    let current_quarantined = current_quarantined as u128;
    let current_total = current_total as u128;
    let current_is_high = current_quarantined * 100 >= current_total * 20;
    let rate_increased_by_ten_points = if previous_total == 0 {
        current_quarantined * 100 >= current_total * 10
    } else {
        let previous_quarantined = previous_quarantined as u128;
        let previous_total = previous_total as u128;
        current_quarantined * previous_total * 100
            >= previous_quarantined * current_total * 100 + current_total * previous_total * 10
    };

    current_is_high || rate_increased_by_ten_points
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuarantineReasonCode {
    MissingRequiredField,
    NetworkMismatch,
    InvalidPort,
    InvalidPublicKey,
    UnsafeTarget,
    ParseError,
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuarantinedCandidate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<DiscoveryCandidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_reference: Option<String>,
    pub reason_codes: Vec<QuarantineReasonCode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeferredDiscoveryWork {
    pub host: String,
    pub p2p_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_at: Option<DateTime<Utc>>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceRunResult {
    pub source_id: String,
    pub source_kind: DiscoverySourceKind,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub status: SourceRunStatus,
    pub total_candidate_count: usize,
    pub accepted_candidates: Vec<DiscoveryCandidate>,
    pub quarantined_candidates: Vec<QuarantinedCandidate>,
    pub deferred_work: Vec<DeferredDiscoveryWork>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sanitized_error: Option<String>,
}
