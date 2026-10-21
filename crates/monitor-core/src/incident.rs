use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::model::{CheckKind, Dimension, Network, Observation, Outcome, Severity, Site};

const RETRY_NOT_BEFORE: Duration = Duration::seconds(30);
const RETRY_NOT_AFTER: Duration = Duration::seconds(60);
const CONFIRMATION_WINDOW: Duration = Duration::minutes(12);
const INCIDENT_STATE_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationContext {
    pub source_anomaly: bool,
    pub reference_quorum_usable: bool,
    pub network: Option<Network>,
}

impl Default for EvaluationContext {
    fn default() -> Self {
        Self {
            source_anomaly: false,
            reference_quorum_usable: true,
            network: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkFinalizationSample {
    pub network: Network,
    pub site: Site,
    pub expected_references: usize,
    pub responding_references: usize,
    pub stalled_references: usize,
    pub stalled_minutes: u64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentSuppression {
    NetworkAggregate,
    IdentityMerge,
    CheckNoLongerApplicable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncidentSubject {
    Node {
        node_id: String,
        network: Option<Network>,
    },
    Network {
        network: Network,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenIncidentProjection {
    pub incident_id: Uuid,
    pub subject: IncidentSubject,
    pub dimension: Dimension,
    pub check_kind: CheckKind,
    pub severity: Severity,
    pub first_observed_at: DateTime<Utc>,
    pub last_observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "transition", rename_all = "snake_case")]
pub enum IncidentTransition {
    RetryScheduled {
        node_id: String,
        dimension: Dimension,
        check_kind: CheckKind,
        site: Site,
        not_before: DateTime<Utc>,
        not_after: DateTime<Utc>,
    },
    RecoveryCheckScheduled {
        node_id: String,
        dimension: Dimension,
        check_kind: CheckKind,
        site: Site,
        not_before: DateTime<Utc>,
        not_after: DateTime<Utc>,
    },
    CandidateRecorded {
        node_id: String,
        dimension: Dimension,
        check_kind: CheckKind,
        site: Site,
        severity: Severity,
        at: DateTime<Utc>,
        evidence_summary: String,
    },
    Opened {
        incident_id: Uuid,
        node_id: String,
        dimension: Dimension,
        check_kind: CheckKind,
        severity: Severity,
        at: DateTime<Utc>,
        evidence_summary: String,
    },
    NetworkRetryScheduled {
        network: Network,
        site: Site,
        not_before: DateTime<Utc>,
        not_after: DateTime<Utc>,
    },
    NetworkRecoveryCheckScheduled {
        network: Network,
        site: Site,
        not_before: DateTime<Utc>,
        not_after: DateTime<Utc>,
    },
    NetworkCandidateRecorded {
        network: Network,
        site: Site,
        severity: Severity,
        at: DateTime<Utc>,
        evidence_summary: String,
    },
    NetworkOpened {
        incident_id: Uuid,
        network: Network,
        dimension: Dimension,
        severity: Severity,
        at: DateTime<Utc>,
        evidence_summary: String,
    },
    Updated {
        incident_id: Uuid,
        severity: Severity,
        at: DateTime<Utc>,
        evidence_summary: String,
    },
    EvidenceUpdated {
        incident_id: Uuid,
        at: DateTime<Utc>,
        evidence_summary: String,
    },
    Resolved {
        incident_id: Uuid,
        at: DateTime<Utc>,
    },
    Suppressed {
        incident_id: Uuid,
        at: DateTime<Utc>,
        reason: IncidentSuppression,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FailureEvidence {
    at: DateTime<Utc>,
    severity: Severity,
    summary: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FailureProgress {
    first: Option<FailureEvidence>,
    candidate: Option<FailureEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpenIncident {
    id: Uuid,
    severity: Severity,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Gate {
    failures: HashMap<Site, FailureProgress>,
    healthy: HashMap<Site, DateTime<Utc>>,
    last_seen: HashMap<Site, DateTime<Utc>>,
    open: Option<OpenIncident>,
    network: Option<Network>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncidentEvaluatorSnapshot {
    schema_version: u32,
    node_gates: Vec<NodeGateSnapshot>,
    network_finalization: Vec<NetworkGateSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeGateSnapshot {
    node_id: String,
    dimension: Dimension,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    check_kind: Option<CheckKind>,
    gate: Gate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NetworkGateSnapshot {
    network: Network,
    gate: Gate,
    suppressed_through: Option<DateTime<Utc>>,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum IncidentStateError {
    #[error("unsupported incident state schema version {0}")]
    UnsupportedSchema(u32),
    #[error("incident state contains a duplicate gate")]
    DuplicateGate,
    #[error("incident state contains an invalid gate")]
    InvalidGate,
}

#[derive(Debug)]
enum GateEvent {
    RetryScheduled {
        site: Site,
        not_before: DateTime<Utc>,
        not_after: DateTime<Utc>,
    },
    RecoveryCheckScheduled {
        site: Site,
        not_before: DateTime<Utc>,
        not_after: DateTime<Utc>,
    },
    CandidateRecorded {
        site: Site,
        severity: Severity,
        at: DateTime<Utc>,
        summary: String,
    },
    Opened {
        incident_id: Uuid,
        severity: Severity,
        at: DateTime<Utc>,
        summary: String,
    },
    Updated {
        incident_id: Uuid,
        severity: Severity,
        at: DateTime<Utc>,
        summary: String,
    },
    EvidenceUpdated {
        incident_id: Uuid,
        at: DateTime<Utc>,
        summary: String,
    },
    Resolved {
        incident_id: Uuid,
        at: DateTime<Utc>,
    },
}

impl Gate {
    fn record_seen(&mut self, site: Site, at: DateTime<Utc>) -> bool {
        if self
            .last_seen
            .get(&site)
            .is_some_and(|last_seen| at <= *last_seen)
        {
            return false;
        }
        self.last_seen.insert(site, at);
        true
    }

    fn latest_seen(&self) -> Option<DateTime<Utc>> {
        self.last_seen.values().copied().max()
    }

    fn observe(
        &mut self,
        site: Site,
        at: DateTime<Utc>,
        class: Classification,
        refresh_open_failure: bool,
    ) -> Vec<GateEvent> {
        if !self.record_seen(site, at) {
            return Vec::new();
        }

        match class {
            Classification::Unknown => Vec::new(),
            Classification::Healthy => self.observe_healthy(site, at),
            Classification::Failed(severity, summary) => {
                self.observe_failure(site, at, severity, summary, refresh_open_failure)
            }
        }
    }

    fn observe_healthy(&mut self, site: Site, at: DateTime<Utc>) -> Vec<GateEvent> {
        self.failures.remove(&site);
        self.healthy.insert(site, at);
        if let Some(resolved) = self.repair_completed_recovery() {
            return vec![resolved];
        }
        if self.open.is_some() {
            return vec![GateEvent::RecoveryCheckScheduled {
                site: other_site(site),
                not_before: at,
                not_after: at + CONFIRMATION_WINDOW,
            }];
        }
        Vec::new()
    }

    fn repair_completed_recovery(&mut self) -> Option<GateEvent> {
        if !paired_identity_times(&self.healthy) {
            return None;
        }
        let incident_id = self.open.take()?.id;
        let at = self.healthy.values().copied().max()?;
        self.failures.clear();
        self.healthy.clear();
        Some(GateEvent::Resolved { incident_id, at })
    }

    fn observe_failure(
        &mut self,
        site: Site,
        at: DateTime<Utc>,
        severity: Severity,
        summary: String,
        refresh_open_failure: bool,
    ) -> Vec<GateEvent> {
        self.healthy.remove(&site);

        if let Some(open) = &self.open {
            let incident_id = open.id;
            let open_severity = open.severity;
            self.failures.insert(
                site,
                FailureProgress {
                    first: None,
                    candidate: Some(FailureEvidence {
                        at,
                        severity,
                        summary: summary.clone(),
                    }),
                },
            );
            if severity <= open_severity {
                return if refresh_open_failure {
                    vec![GateEvent::EvidenceUpdated {
                        incident_id,
                        at,
                        summary,
                    }]
                } else {
                    Vec::new()
                };
            }
            let confirmed = self
                .failures
                .get(&other_site(site))
                .and_then(|progress| progress.candidate.as_ref())
                .is_some_and(|candidate| {
                    candidate.severity >= severity
                        && within_after(at, candidate.at, CONFIRMATION_WINDOW)
                });
            if confirmed {
                self.open.as_mut().expect("open incident exists").severity = severity;
                return vec![GateEvent::Updated {
                    incident_id,
                    severity,
                    at,
                    summary,
                }];
            }
            return vec![GateEvent::CandidateRecorded {
                site,
                severity,
                at,
                summary,
            }];
        }

        let other = other_site(site);
        let other_candidate = self
            .failures
            .get(&other)
            .and_then(|progress| progress.candidate.as_ref())
            .filter(|candidate| within_after(at, candidate.at, CONFIRMATION_WINDOW))
            .cloned();
        if let Some(other_candidate) = other_candidate {
            let confirmed_severity = severity.max(other_candidate.severity);
            let evidence_summary = format!(
                "site {other:?}: {}; site {site:?}: {summary}",
                other_candidate.summary
            );
            self.failures.insert(
                site,
                FailureProgress {
                    first: None,
                    candidate: Some(FailureEvidence {
                        at,
                        severity,
                        summary,
                    }),
                },
            );
            let incident_id = Uuid::new_v4();
            self.open = Some(OpenIncident {
                id: incident_id,
                severity: confirmed_severity,
            });
            return vec![GateEvent::Opened {
                incident_id,
                severity: confirmed_severity,
                at,
                summary: evidence_summary,
            }];
        }

        let progress = self.failures.entry(site).or_default();
        if let Some(candidate) = &mut progress.candidate {
            candidate.at = at;
            candidate.severity = candidate.severity.max(severity);
            candidate.summary = summary.clone();
            return vec![GateEvent::CandidateRecorded {
                site,
                severity: candidate.severity,
                at,
                summary,
            }];
        }

        if let Some(first) = &progress.first {
            let elapsed = at.signed_duration_since(first.at);
            if elapsed >= RETRY_NOT_BEFORE && elapsed <= RETRY_NOT_AFTER {
                let candidate = FailureEvidence {
                    at,
                    severity: severity.max(first.severity),
                    summary: summary.clone(),
                };
                let candidate_severity = candidate.severity;
                progress.first = None;
                progress.candidate = Some(candidate);
                return vec![GateEvent::CandidateRecorded {
                    site,
                    severity: candidate_severity,
                    at,
                    summary,
                }];
            }
            if elapsed < RETRY_NOT_BEFORE {
                return Vec::new();
            }
        }

        progress.first = Some(FailureEvidence {
            at,
            severity,
            summary,
        });
        vec![GateEvent::RetryScheduled {
            site,
            not_before: at + RETRY_NOT_BEFORE,
            not_after: at + RETRY_NOT_AFTER,
        }]
    }

    fn suppress_open(&mut self, at: DateTime<Utc>) -> Option<GateEvent> {
        if self
            .latest_seen()
            .is_some_and(|latest_seen| latest_seen > at)
        {
            return None;
        }
        self.failures.clear();
        self.healthy.clear();
        self.open.take().map(|open| GateEvent::Resolved {
            incident_id: open.id,
            at,
        })
    }
}

#[derive(Debug, Default)]
pub struct IncidentEvaluator {
    gates: HashMap<(String, Dimension, CheckKind), Gate>,
    network_finalization: HashMap<Network, Gate>,
    node_networks: HashMap<String, Network>,
    network_suppression_watermarks: HashMap<Network, DateTime<Utc>>,
}

impl IncidentEvaluator {
    #[must_use]
    pub fn retire_node_check(
        &mut self,
        node_id: &str,
        dimension: Dimension,
        check_kind: CheckKind,
        at: DateTime<Utc>,
    ) -> (bool, Option<IncidentTransition>) {
        let key = (node_id.to_owned(), dimension, check_kind);
        let Some(gate) = self.gates.get_mut(&key) else {
            return (false, None);
        };
        let retired_at = gate.latest_seen().map_or(at, |latest| latest.max(at));
        gate.failures.clear();
        gate.healthy.clear();
        for site in [Site::A, Site::B] {
            gate.record_seen(site, retired_at);
        }
        let transition = gate.open.take().map(|open| IncidentTransition::Suppressed {
            incident_id: open.id,
            at: retired_at,
            reason: IncidentSuppression::CheckNoLongerApplicable,
        });
        (true, transition)
    }

    pub fn merge_node_identity(
        &mut self,
        source_node_id: &str,
        target_node_id: &str,
        network: Network,
        merged_at: DateTime<Utc>,
        evaluate_semantic_transitions: bool,
    ) -> Result<Vec<IncidentTransition>, IncidentStateError> {
        if source_node_id == target_node_id {
            return Ok(Vec::new());
        }
        if self
            .node_networks
            .get(source_node_id)
            .is_some_and(|existing| *existing != network)
            || self
                .node_networks
                .get(target_node_id)
                .is_some_and(|existing| *existing != network)
        {
            return Err(IncidentStateError::InvalidGate);
        }
        let mut source_checks = self
            .gates
            .keys()
            .filter_map(|(node_id, dimension, check_kind)| {
                (node_id == source_node_id).then_some((*dimension, *check_kind))
            })
            .collect::<Vec<_>>();
        source_checks
            .sort_by_key(|(dimension, check_kind)| (dimension_rank(*dimension), *check_kind));
        if source_checks.iter().any(|(dimension, check_kind)| {
            self.gates
                .get(&(source_node_id.to_owned(), *dimension, *check_kind))
                .and_then(|gate| gate.network)
                .is_some_and(|existing| existing != network)
        }) {
            return Err(IncidentStateError::InvalidGate);
        }
        let mut replacements = Vec::with_capacity(source_checks.len());
        for (dimension, check_kind) in &source_checks {
            let source_key = (source_node_id.to_owned(), *dimension, *check_kind);
            let source_gate = self
                .gates
                .get(&source_key)
                .cloned()
                .ok_or(IncidentStateError::InvalidGate)?;
            let target_key = (target_node_id.to_owned(), *dimension, *check_kind);
            let (mut gate, merge_transitions) =
                if let Some(target_gate) = self.gates.get(&target_key).cloned() {
                    merge_identity_gates(
                        source_gate,
                        target_gate,
                        target_node_id,
                        *dimension,
                        *check_kind,
                        merged_at,
                        evaluate_semantic_transitions,
                    )?
                } else {
                    (source_gate, Vec::new())
                };
            gate.network = Some(network);
            validate_gate(&gate)?;
            replacements.push((*dimension, *check_kind, gate, merge_transitions));
        }
        let mut transitions = Vec::new();
        for (dimension, check_kind, gate, mut merge_transitions) in replacements {
            let source_key = (source_node_id.to_owned(), dimension, check_kind);
            let target_key = (target_node_id.to_owned(), dimension, check_kind);
            self.gates.remove(&source_key);
            self.gates.insert(target_key, gate);
            transitions.append(&mut merge_transitions);
        }
        if self.node_networks.remove(source_node_id).is_some()
            || self.node_networks.contains_key(target_node_id)
        {
            self.node_networks
                .insert(target_node_id.to_owned(), network);
        }
        Ok(transitions)
    }

    #[must_use]
    pub fn open_incidents(&self) -> Vec<OpenIncidentProjection> {
        let mut open = Vec::new();
        for ((node_id, dimension, check_kind), gate) in &self.gates {
            if let Some(projection) = open_projection(
                gate,
                IncidentSubject::Node {
                    node_id: node_id.clone(),
                    network: gate.network,
                },
                *dimension,
                *check_kind,
            ) {
                open.push(projection);
            }
        }
        for (network, gate) in &self.network_finalization {
            if let Some(projection) = open_projection(
                gate,
                IncidentSubject::Network { network: *network },
                Dimension::Finalization,
                CheckKind::Finalization,
            ) {
                open.push(projection);
            }
        }
        open.sort_by_key(|projection| projection.incident_id);
        open
    }

    #[must_use]
    pub fn ingest(&mut self, observation: Observation) -> Vec<IncidentTransition> {
        self.ingest_with_context(observation, EvaluationContext::default())
    }

    #[must_use]
    pub fn ingest_with_context(
        &mut self,
        observation: Observation,
        context: EvaluationContext,
    ) -> Vec<IncidentTransition> {
        let node_id = observation.node_id.clone();
        let dimension = observation.dimension;
        let Some(check_kind) = observation.check_kind else {
            return Vec::new();
        };
        if !check_kind.supports_dimension(dimension) {
            return Vec::new();
        }
        if let Some(network) = context.network {
            match self.node_networks.get(&node_id) {
                Some(existing) if *existing != network => return Vec::new(),
                Some(_) => {}
                None => {
                    self.node_networks.insert(node_id.clone(), network);
                }
            }
        }
        if dimension == Dimension::Finalization
            && context.network.is_some_and(|network| {
                self.network_suppression_watermarks
                    .get(&network)
                    .is_some_and(|suppressed_through| {
                        observation.observed_at <= *suppressed_through
                    })
            })
        {
            return Vec::new();
        }
        let class = classify_observation(&observation);
        let network_aggregate_open = observation.dimension == Dimension::Finalization
            && context.network.is_some_and(|network| {
                self.network_finalization
                    .get(&network)
                    .is_some_and(|gate| gate.open.is_some())
            });
        let key = (node_id.clone(), dimension, check_kind);
        let gate = self.gates.entry(key).or_default();
        if let Some(network) = context.network {
            match gate.network {
                Some(existing) if existing != network => return Vec::new(),
                Some(_) => {}
                None => gate.network = Some(network),
            }
        }
        if context.source_anomaly
            || !context.reference_quorum_usable
            || matches!(class, Classification::Unknown)
            || network_aggregate_open
        {
            gate.record_seen(observation.site, observation.observed_at);
            return Vec::new();
        }
        gate.observe(
            observation.site,
            observation.observed_at,
            class,
            matches!(dimension, Dimension::Certificate | Dimension::Finalization),
        )
        .into_iter()
        .map(|event| node_transition(&node_id, dimension, check_kind, event))
        .collect()
    }

    #[must_use]
    pub fn ingest_network_finalization(
        &mut self,
        sample: NetworkFinalizationSample,
    ) -> Vec<IncidentTransition> {
        let aggregate_already_open = self
            .network_finalization
            .get(&sample.network)
            .is_some_and(|gate| gate.open.is_some());
        let network_sample_is_suppressed = self
            .network_suppression_watermarks
            .get(&sample.network)
            .is_some_and(|suppressed_through| sample.observed_at <= *suppressed_through);
        let node_state_is_newer = !aggregate_already_open
            && self.gates.iter().any(|((_, dimension, _), gate)| {
                *dimension == Dimension::Finalization
                    && gate.network == Some(sample.network)
                    && gate
                        .latest_seen()
                        .is_some_and(|latest_seen| latest_seen > sample.observed_at)
            });
        let invalid = sample.expected_references == 0
            || sample.responding_references <= sample.expected_references / 2
            || sample.stalled_references > sample.responding_references
            || node_state_is_newer
            || network_sample_is_suppressed;
        if invalid {
            let aggregate_open = {
                let gate = self.network_finalization.entry(sample.network).or_default();
                gate.record_seen(sample.site, sample.observed_at);
                gate.open.is_some()
            };
            if aggregate_open {
                self.advance_node_finalization_watermark(sample.network, sample.observed_at);
            }
            return Vec::new();
        }
        let majority_stalled = sample.stalled_references > sample.expected_references / 2;
        let class = if !majority_stalled || sample.stalled_minutes < 30 {
            Classification::Healthy
        } else {
            let severity = if sample.stalled_minutes >= 60 {
                Severity::Critical
            } else {
                Severity::Warning
            };
            Classification::Failed(
                severity,
                format!(
                    "{}/{} reference APIs stalled for {} minutes",
                    sample.stalled_references, sample.expected_references, sample.stalled_minutes
                ),
            )
        };

        let (events, aggregate_was_open, aggregate_is_open) = {
            let gate = self.network_finalization.entry(sample.network).or_default();
            let aggregate_was_open = gate.open.is_some();
            let events = gate.observe(sample.site, sample.observed_at, class, false);
            (events, aggregate_was_open, gate.open.is_some())
        };
        let mut transitions = Vec::new();
        for event in events {
            let opened = matches!(event, GateEvent::Opened { .. });
            transitions.push(network_transition(sample.network, event));
            if opened {
                for ((_, dimension, _), gate) in &mut self.gates {
                    if *dimension == Dimension::Finalization && gate.network == Some(sample.network)
                    {
                        if let Some(GateEvent::Resolved { incident_id, at }) =
                            gate.suppress_open(sample.observed_at)
                        {
                            transitions.push(IncidentTransition::Suppressed {
                                incident_id,
                                at,
                                reason: IncidentSuppression::NetworkAggregate,
                            });
                        }
                    }
                }
            }
        }
        if aggregate_was_open || aggregate_is_open {
            self.advance_node_finalization_watermark(sample.network, sample.observed_at);
        }
        transitions
    }

    fn advance_node_finalization_watermark(
        &mut self,
        network: Network,
        observed_at: DateTime<Utc>,
    ) {
        let suppressed_through = self
            .network_suppression_watermarks
            .entry(network)
            .and_modify(|current| *current = (*current).max(observed_at))
            .or_insert(observed_at)
            .to_owned();
        for ((_, dimension, _), gate) in &mut self.gates {
            if *dimension == Dimension::Finalization && gate.network == Some(network) {
                for site in [Site::A, Site::B] {
                    gate.record_seen(site, suppressed_through);
                }
            }
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> IncidentEvaluatorSnapshot {
        let mut node_gates = self
            .gates
            .iter()
            .map(
                |((node_id, dimension, check_kind), gate)| NodeGateSnapshot {
                    node_id: node_id.clone(),
                    dimension: *dimension,
                    check_kind: Some(*check_kind),
                    gate: gate.clone(),
                },
            )
            .collect::<Vec<_>>();
        node_gates.sort_by(|left, right| {
            left.node_id
                .cmp(&right.node_id)
                .then_with(|| dimension_rank(left.dimension).cmp(&dimension_rank(right.dimension)))
                .then_with(|| left.check_kind.cmp(&right.check_kind))
        });
        let mut network_finalization = self
            .network_finalization
            .iter()
            .map(|(network, gate)| NetworkGateSnapshot {
                network: *network,
                gate: gate.clone(),
                suppressed_through: self.network_suppression_watermarks.get(network).copied(),
            })
            .collect::<Vec<_>>();
        network_finalization.sort_by_key(|snapshot| network_rank(snapshot.network));
        IncidentEvaluatorSnapshot {
            schema_version: INCIDENT_STATE_SCHEMA_VERSION,
            node_gates,
            network_finalization,
        }
    }

    pub fn from_snapshot(snapshot: IncidentEvaluatorSnapshot) -> Result<Self, IncidentStateError> {
        if !matches!(snapshot.schema_version, 1 | INCIDENT_STATE_SCHEMA_VERSION) {
            return Err(IncidentStateError::UnsupportedSchema(
                snapshot.schema_version,
            ));
        }
        let mut gates = HashMap::with_capacity(snapshot.node_gates.len());
        let mut node_networks = HashMap::new();
        let mut incident_ids = HashSet::new();
        for snapshot in snapshot.node_gates {
            validate_gate(&snapshot.gate)?;
            if snapshot.node_id.is_empty() {
                return Err(IncidentStateError::InvalidGate);
            }
            if let Some(network) = snapshot.gate.network {
                match node_networks.get(&snapshot.node_id) {
                    Some(existing) if *existing != network => {
                        return Err(IncidentStateError::InvalidGate);
                    }
                    Some(_) => {}
                    None => {
                        node_networks.insert(snapshot.node_id.clone(), network);
                    }
                }
            }
            if snapshot
                .gate
                .open
                .as_ref()
                .is_some_and(|open| !incident_ids.insert(open.id))
            {
                return Err(IncidentStateError::InvalidGate);
            }
            let check_kind = snapshot
                .check_kind
                .unwrap_or_else(|| default_check_kind(snapshot.dimension));
            if gates
                .insert(
                    (snapshot.node_id, snapshot.dimension, check_kind),
                    snapshot.gate,
                )
                .is_some()
            {
                return Err(IncidentStateError::DuplicateGate);
            }
        }
        let mut network_finalization = HashMap::with_capacity(snapshot.network_finalization.len());
        let mut network_suppression_watermarks = HashMap::new();
        for snapshot in snapshot.network_finalization {
            validate_gate(&snapshot.gate)?;
            if snapshot.gate.network.is_some() {
                return Err(IncidentStateError::InvalidGate);
            }
            if snapshot
                .gate
                .open
                .as_ref()
                .is_some_and(|open| !incident_ids.insert(open.id))
            {
                return Err(IncidentStateError::InvalidGate);
            }
            let latest_seen = snapshot.gate.latest_seen();
            match snapshot.suppressed_through {
                None if snapshot.gate.open.is_some() => {
                    return Err(IncidentStateError::InvalidGate);
                }
                Some(suppressed_through)
                    if latest_seen.is_none_or(|latest_seen| suppressed_through > latest_seen)
                        || (snapshot.gate.open.is_some()
                            && latest_seen != Some(suppressed_through)) =>
                {
                    return Err(IncidentStateError::InvalidGate);
                }
                None | Some(_) => {}
            }
            if let Some(suppressed_through) = snapshot.suppressed_through {
                network_suppression_watermarks.insert(snapshot.network, suppressed_through);
            }
            if network_finalization
                .insert(snapshot.network, snapshot.gate)
                .is_some()
            {
                return Err(IncidentStateError::DuplicateGate);
            }
        }
        for (network, network_gate) in &network_finalization {
            if network_gate.open.is_some()
                && gates.iter().any(|((_, dimension, _), gate)| {
                    *dimension == Dimension::Finalization
                        && gate.network == Some(*network)
                        && (gate.open.is_some()
                            || !gate.failures.is_empty()
                            || !gate.healthy.is_empty())
                })
            {
                return Err(IncidentStateError::InvalidGate);
            }
            if network_suppression_watermarks
                .get(network)
                .is_some_and(|suppressed_through| {
                    gates.iter().any(|((_, dimension, _), gate)| {
                        *dimension == Dimension::Finalization
                            && gate.network == Some(*network)
                            && gate_has_state_at_or_before(gate, *suppressed_through)
                    })
                })
            {
                return Err(IncidentStateError::InvalidGate);
            }
        }
        Ok(Self {
            gates,
            network_finalization,
            node_networks,
            network_suppression_watermarks,
        })
    }

    pub fn from_snapshot_repairing_completed_recoveries(
        mut snapshot: IncidentEvaluatorSnapshot,
    ) -> Result<(Self, Vec<IncidentTransition>), IncidentStateError> {
        let mut transitions = Vec::new();
        for node_gate in &mut snapshot.node_gates {
            if let Some(event) = node_gate.gate.repair_completed_recovery() {
                transitions.push(node_transition(
                    &node_gate.node_id,
                    node_gate.dimension,
                    node_gate
                        .check_kind
                        .unwrap_or_else(|| default_check_kind(node_gate.dimension)),
                    event,
                ));
            }
        }
        for network_gate in &mut snapshot.network_finalization {
            if let Some(event) = network_gate.gate.repair_completed_recovery() {
                transitions.push(network_transition(network_gate.network, event));
            }
        }
        let evaluator = Self::from_snapshot(snapshot)?;
        Ok((evaluator, transitions))
    }

    #[must_use]
    pub fn severity_for(dimension: Dimension, outcome: &Outcome) -> Option<Severity> {
        match classify(dimension, outcome) {
            Classification::Failed(severity, _) => Some(severity),
            Classification::Healthy | Classification::Unknown => None,
        }
    }

    #[must_use]
    pub const fn expiry_severity(remaining: i64) -> Option<Severity> {
        if remaining <= 7 {
            Some(Severity::Critical)
        } else if remaining <= 30 {
            Some(Severity::Warning)
        } else {
            None
        }
    }

    #[must_use]
    pub const fn certificate_expiry_severity(remaining: i64) -> Option<Severity> {
        if remaining <= 0 {
            Some(Severity::Critical)
        } else if remaining <= 30 {
            Some(Severity::Warning)
        } else {
            None
        }
    }
}

#[derive(Clone)]
enum IdentitySiteState {
    Failure(FailureProgress),
    Healthy(DateTime<Utc>),
}

fn merge_identity_gates(
    source: Gate,
    target: Gate,
    target_node_id: &str,
    dimension: Dimension,
    check_kind: CheckKind,
    merged_at: DateTime<Utc>,
    evaluate_semantic_transitions: bool,
) -> Result<(Gate, Vec<IncidentTransition>), IncidentStateError> {
    if source
        .network
        .zip(target.network)
        .is_some_and(|(source, target)| source != target)
    {
        return Err(IncidentStateError::InvalidGate);
    }
    let mut merged = Gate {
        network: target.network.or(source.network),
        ..Gate::default()
    };
    for site in [Site::A, Site::B] {
        if let Some(last_seen) = source
            .last_seen
            .get(&site)
            .into_iter()
            .chain(target.last_seen.get(&site))
            .copied()
            .max()
        {
            merged.last_seen.insert(site, last_seen);
        }
        let source_state = identity_site_state(&source, site);
        let target_state = identity_site_state(&target, site);
        let selected = match (source_state, target_state) {
            (Some(source), Some(target)) if source.0 > target.0 => Some(source.1),
            (Some(_), Some(target)) => Some(target.1),
            (Some(source), None) => Some(source.1),
            (None, Some(target)) => Some(target.1),
            (None, None) => None,
        };
        match selected {
            Some(IdentitySiteState::Failure(progress)) => {
                merged.failures.insert(site, progress);
            }
            Some(IdentitySiteState::Healthy(at)) => {
                merged.healthy.insert(site, at);
            }
            None => {}
        }
    }

    let source_open_gate = source.open.is_some().then(|| source.clone());
    let target_open_gate = target.open.is_some().then(|| target.clone());
    let source_open = source.open;
    let target_open = target.open;
    let mut transitions = Vec::new();
    let mut severity_update = None;
    merged.open = match (source_open, target_open) {
        (Some(source), Some(mut target)) => {
            if source.id != target.id {
                transitions.push(IncidentTransition::Suppressed {
                    incident_id: source.id,
                    at: merged_at,
                    reason: IncidentSuppression::IdentityMerge,
                });
            }
            if evaluate_semantic_transitions && source.severity > target.severity {
                target.severity = source.severity;
                severity_update = Some(target.severity);
            }
            Some(target)
        }
        (Some(source), None) => Some(source),
        (None, Some(target)) => Some(target),
        (None, None) => None,
    };

    if !evaluate_semantic_transitions && merged.open.is_some() {
        let survivor = target_open_gate
            .as_ref()
            .or(source_open_gate.as_ref())
            .expect("an open survivor exists");
        merged.failures.clone_from(&survivor.failures);
        merged.healthy.clone_from(&survivor.healthy);
        merged.last_seen.clone_from(&survivor.last_seen);
    } else if merged.open.is_some() {
        for progress in merged.failures.values_mut() {
            if progress.candidate.is_none() {
                progress.candidate = progress.first.take();
            }
        }
        if paired_identity_times(&merged.healthy) {
            let incident_id = merged.open.take().expect("open incident exists").id;
            merged.failures.clear();
            merged.healthy.clear();
            transitions.push(IncidentTransition::Resolved {
                incident_id,
                at: merged_at,
            });
            severity_update = None;
        }
    } else if evaluate_semantic_transitions {
        if let (Some(site_a), Some(site_b)) = (
            merged
                .failures
                .get(&Site::A)
                .and_then(|progress| progress.candidate.as_ref()),
            merged
                .failures
                .get(&Site::B)
                .and_then(|progress| progress.candidate.as_ref()),
        ) {
            let (later, earlier) = if site_a.at >= site_b.at {
                (site_a, site_b)
            } else {
                (site_b, site_a)
            };
            if within_after(later.at, earlier.at, CONFIRMATION_WINDOW) {
                let severity = site_a.severity.max(site_b.severity);
                let incident_id = Uuid::new_v4();
                merged.open = Some(OpenIncident {
                    id: incident_id,
                    severity,
                });
                transitions.push(IncidentTransition::Opened {
                    incident_id,
                    node_id: target_node_id.to_owned(),
                    dimension,
                    check_kind,
                    severity,
                    at: later.at,
                    evidence_summary: format!(
                        "site A: {}; site B: {}",
                        site_a.summary, site_b.summary
                    ),
                });
            }
        }
    }
    if let (Some(open), Some(severity)) = (&merged.open, severity_update) {
        let evidence_summary = merged
            .failures
            .values()
            .filter_map(|progress| progress.candidate.as_ref().or(progress.first.as_ref()))
            .max_by_key(|evidence| evidence.at)
            .map_or_else(
                || "severity updated after identity merge".to_owned(),
                |evidence| evidence.summary.clone(),
            );
        transitions.push(IncidentTransition::Updated {
            incident_id: open.id,
            severity,
            at: merged_at,
            evidence_summary,
        });
    }
    validate_gate(&merged)?;
    Ok((merged, transitions))
}

fn identity_site_state(gate: &Gate, site: Site) -> Option<(DateTime<Utc>, IdentitySiteState)> {
    let failure = gate.failures.get(&site).and_then(|progress| {
        progress
            .first
            .as_ref()
            .or(progress.candidate.as_ref())
            .map(|evidence| (evidence.at, IdentitySiteState::Failure(progress.clone())))
    });
    let healthy = gate
        .healthy
        .get(&site)
        .map(|at| (*at, IdentitySiteState::Healthy(*at)));
    match (failure, healthy) {
        (Some(failure), Some(healthy)) if failure.0 > healthy.0 => Some(failure),
        (Some(_), Some(healthy)) => Some(healthy),
        (Some(failure), None) => Some(failure),
        (None, Some(healthy)) => Some(healthy),
        (None, None) => None,
    }
}

fn paired_identity_times(observations: &HashMap<Site, DateTime<Utc>>) -> bool {
    let (Some(site_a), Some(site_b)) = (observations.get(&Site::A), observations.get(&Site::B))
    else {
        return false;
    };
    let elapsed = site_a.signed_duration_since(*site_b);
    elapsed >= -CONFIRMATION_WINDOW && elapsed <= CONFIRMATION_WINDOW
}

fn open_projection(
    gate: &Gate,
    subject: IncidentSubject,
    dimension: Dimension,
    check_kind: CheckKind,
) -> Option<OpenIncidentProjection> {
    let incident = gate.open.as_ref()?;
    let last_observed_at = gate.latest_seen()?;
    let first_observed_at = gate
        .failures
        .values()
        .filter_map(|progress| progress.candidate.as_ref().map(|candidate| candidate.at))
        .min()
        .unwrap_or(last_observed_at);
    Some(OpenIncidentProjection {
        incident_id: incident.id,
        subject,
        dimension,
        check_kind,
        severity: incident.severity,
        first_observed_at,
        last_observed_at,
    })
}

fn validate_gate(gate: &Gate) -> Result<(), IncidentStateError> {
    if gate.open.as_ref().is_some_and(|open| open.id.is_nil()) {
        return Err(IncidentStateError::InvalidGate);
    }
    if gate.open.is_some()
        && gate.healthy.get(&Site::A).is_some_and(|site_a| {
            gate.healthy.get(&Site::B).is_some_and(|site_b| {
                let elapsed = site_a.signed_duration_since(*site_b);
                elapsed >= -CONFIRMATION_WINDOW && elapsed <= CONFIRMATION_WINDOW
            })
        })
    {
        return Err(IncidentStateError::InvalidGate);
    }
    for site in [Site::A, Site::B] {
        let last_seen = gate.last_seen.get(&site);
        if gate.open.is_some()
            && !gate.healthy.contains_key(&site)
            && !gate.failures.contains_key(&site)
        {
            return Err(IncidentStateError::InvalidGate);
        }
        if gate.healthy.contains_key(&site) && gate.failures.contains_key(&site) {
            return Err(IncidentStateError::InvalidGate);
        }
        if let Some(healthy_at) = gate.healthy.get(&site) {
            if last_seen.is_none_or(|last_seen| healthy_at > last_seen) {
                return Err(IncidentStateError::InvalidGate);
            }
        }
        if let Some(progress) = gate.failures.get(&site) {
            if (progress.first.is_none() && progress.candidate.is_none())
                || (progress.first.is_some() && progress.candidate.is_some())
                || (gate.open.is_some() && progress.first.is_some())
            {
                return Err(IncidentStateError::InvalidGate);
            }
            for evidence in [progress.first.as_ref(), progress.candidate.as_ref()]
                .into_iter()
                .flatten()
            {
                if last_seen.is_none_or(|last_seen| evidence.at > *last_seen) {
                    return Err(IncidentStateError::InvalidGate);
                }
            }
        }
    }
    Ok(())
}

fn gate_has_state_at_or_before(gate: &Gate, at: DateTime<Utc>) -> bool {
    gate.healthy.values().any(|healthy_at| *healthy_at <= at)
        || gate.failures.values().any(|progress| {
            [progress.first.as_ref(), progress.candidate.as_ref()]
                .into_iter()
                .flatten()
                .any(|evidence| evidence.at <= at)
        })
}

fn node_transition(
    node_id: &str,
    dimension: Dimension,
    check_kind: CheckKind,
    event: GateEvent,
) -> IncidentTransition {
    match event {
        GateEvent::RetryScheduled {
            site,
            not_before,
            not_after,
        } => IncidentTransition::RetryScheduled {
            node_id: node_id.to_owned(),
            dimension,
            check_kind,
            site,
            not_before,
            not_after,
        },
        GateEvent::RecoveryCheckScheduled {
            site,
            not_before,
            not_after,
        } => IncidentTransition::RecoveryCheckScheduled {
            node_id: node_id.to_owned(),
            dimension,
            check_kind,
            site,
            not_before,
            not_after,
        },
        GateEvent::CandidateRecorded {
            site,
            severity,
            at,
            summary,
        } => IncidentTransition::CandidateRecorded {
            node_id: node_id.to_owned(),
            dimension,
            check_kind,
            site,
            severity,
            at,
            evidence_summary: summary,
        },
        GateEvent::Opened {
            incident_id,
            severity,
            at,
            summary,
        } => IncidentTransition::Opened {
            incident_id,
            node_id: node_id.to_owned(),
            dimension,
            check_kind,
            severity,
            at,
            evidence_summary: summary,
        },
        GateEvent::Updated {
            incident_id,
            severity,
            at,
            summary,
        } => IncidentTransition::Updated {
            incident_id,
            severity,
            at,
            evidence_summary: summary,
        },
        GateEvent::EvidenceUpdated {
            incident_id,
            at,
            summary,
        } => IncidentTransition::EvidenceUpdated {
            incident_id,
            at,
            evidence_summary: summary,
        },
        GateEvent::Resolved { incident_id, at } => IncidentTransition::Resolved { incident_id, at },
    }
}

fn network_transition(network: Network, event: GateEvent) -> IncidentTransition {
    match event {
        GateEvent::RetryScheduled {
            site,
            not_before,
            not_after,
        } => IncidentTransition::NetworkRetryScheduled {
            network,
            site,
            not_before,
            not_after,
        },
        GateEvent::RecoveryCheckScheduled {
            site,
            not_before,
            not_after,
        } => IncidentTransition::NetworkRecoveryCheckScheduled {
            network,
            site,
            not_before,
            not_after,
        },
        GateEvent::CandidateRecorded {
            site,
            severity,
            at,
            summary,
        } => IncidentTransition::NetworkCandidateRecorded {
            network,
            site,
            severity,
            at,
            evidence_summary: summary,
        },
        GateEvent::Opened {
            incident_id,
            severity,
            at,
            summary,
        } => IncidentTransition::NetworkOpened {
            incident_id,
            network,
            dimension: Dimension::Finalization,
            severity,
            at,
            evidence_summary: summary,
        },
        GateEvent::Updated {
            incident_id,
            severity,
            at,
            summary,
        } => IncidentTransition::Updated {
            incident_id,
            severity,
            at,
            evidence_summary: summary,
        },
        GateEvent::EvidenceUpdated { .. } => {
            unreachable!("network incidents do not refresh evidence without a severity change")
        }
        GateEvent::Resolved { incident_id, at } => IncidentTransition::Resolved { incident_id, at },
    }
}

#[derive(Debug)]
enum Classification {
    Healthy,
    Failed(Severity, String),
    Unknown,
}

fn classify_observation(observation: &Observation) -> Classification {
    let class = classify(observation.dimension, &observation.outcome);
    let Classification::Failed(severity, fallback_summary) = class else {
        return class;
    };
    if observation.dimension != Dimension::Finalization {
        return Classification::Failed(severity, fallback_summary);
    }
    let Some(height) = observation
        .evidence
        .get("height")
        .and_then(serde_json::Value::as_u64)
    else {
        return Classification::Failed(severity, fallback_summary);
    };
    let Some(finalized_height) = observation
        .evidence
        .get("finalizedHeight")
        .and_then(serde_json::Value::as_u64)
        .filter(|finalized_height| *finalized_height <= height)
    else {
        return Classification::Failed(severity, fallback_summary);
    };
    Classification::Failed(
        severity,
        format!("{} blocks behind current height", height - finalized_height),
    )
}

/// Shared interpretation for arbitrary community observers; temporal gates remain separate.
pub fn consensus_verdict(dimension: Dimension, outcome: &Outcome) -> Option<crate::community::Verdict> {
    use crate::community::Verdict;
    match classify(dimension, outcome) {
        Classification::Healthy => Some(Verdict::Healthy),
        Classification::Failed(Severity::Warning, _) => Some(Verdict::Warning),
        Classification::Failed(Severity::Critical, _) => Some(Verdict::Critical),
        Classification::Unknown => None,
    }
}

fn classify(dimension: Dimension, outcome: &Outcome) -> Classification {
    match outcome {
        Outcome::Healthy => Classification::Healthy,
        Outcome::Failure(message) => Classification::Failed(Severity::Critical, message.clone()),
        Outcome::LatencyMillis(milliseconds) if dimension == Dimension::Latency => {
            if *milliseconds >= 2_000 {
                Classification::Failed(
                    Severity::Warning,
                    format!("{milliseconds}ms response latency"),
                )
            } else {
                Classification::Healthy
            }
        }
        Outcome::LagBlocks(lag) if dimension == Dimension::Sync => {
            if *lag >= 20 {
                Classification::Failed(
                    Severity::Critical,
                    format!("{lag} blocks behind reference median"),
                )
            } else if *lag >= 5 {
                Classification::Failed(
                    Severity::Warning,
                    format!("{lag} blocks behind reference median"),
                )
            } else {
                Classification::Healthy
            }
        }
        Outcome::FinalizationStalledMinutes(minutes) if dimension == Dimension::Finalization => {
            if *minutes >= 60 {
                Classification::Failed(
                    Severity::Critical,
                    format!("finalization stalled for {minutes} minutes"),
                )
            } else if *minutes >= 30 {
                Classification::Failed(
                    Severity::Warning,
                    format!("finalization stalled for {minutes} minutes"),
                )
            } else {
                Classification::Healthy
            }
        }
        Outcome::Remaining(remaining) if dimension == Dimension::Certificate => {
            IncidentEvaluator::certificate_expiry_severity(*remaining).map_or(
                Classification::Healthy,
                |severity| {
                    Classification::Failed(severity, format!("{} remaining", (*remaining).max(0)))
                },
            )
        }
        Outcome::Remaining(remaining) if dimension == Dimension::Voting => {
            IncidentEvaluator::expiry_severity(*remaining)
                .map_or(Classification::Healthy, |severity| {
                    Classification::Failed(severity, format!("{remaining} remaining"))
                })
        }
        Outcome::ZeroPeers if dimension == Dimension::Peers => {
            Classification::Failed(Severity::Critical, "zero peers".into())
        }
        Outcome::Mismatch(value) if dimension == Dimension::StateHash => {
            Classification::Failed(Severity::Critical, format!("mismatch: {value}"))
        }
        Outcome::Unknown(_) | Outcome::Unsupported(_) => Classification::Unknown,
        Outcome::LatencyMillis(_)
        | Outcome::LagBlocks(_)
        | Outcome::FinalizationStalledMinutes(_)
        | Outcome::Remaining(_)
        | Outcome::ZeroPeers
        | Outcome::Mismatch(_) => Classification::Unknown,
    }
}

fn within_after(now: DateTime<Utc>, earlier: DateTime<Utc>, window: Duration) -> bool {
    let elapsed = now.signed_duration_since(earlier);
    elapsed >= Duration::zero() && elapsed <= window
}

const fn other_site(site: Site) -> Site {
    match site {
        Site::A => Site::B,
        Site::B => Site::A,
    }
}

const fn network_rank(network: Network) -> u8 {
    match network {
        Network::SymbolMainnet => 0,
        Network::Nis1Mainnet => 1,
    }
}

const fn dimension_rank(dimension: Dimension) -> u8 {
    match dimension {
        Dimension::Connectivity => 0,
        Dimension::Latency => 1,
        Dimension::Sync => 2,
        Dimension::Finalization => 3,
        Dimension::Peers => 4,
        Dimension::Certificate => 5,
        Dimension::Voting => 6,
        Dimension::Harvest => 7,
        Dimension::Supernode => 8,
        Dimension::StateHash => 9,
    }
}

const fn default_check_kind(dimension: Dimension) -> CheckKind {
    match dimension {
        Dimension::Connectivity | Dimension::Latency => CheckKind::Availability,
        Dimension::Sync => CheckKind::Chain,
        Dimension::Finalization => CheckKind::Finalization,
        Dimension::Peers => CheckKind::Peers,
        Dimension::Certificate => CheckKind::Certificate,
        Dimension::Voting => CheckKind::Voting,
        Dimension::Harvest => CheckKind::Harvest,
        Dimension::Supernode => CheckKind::Supernode,
        Dimension::StateHash => CheckKind::StateHash,
    }
}
