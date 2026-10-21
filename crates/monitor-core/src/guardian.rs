//! Deterministic internal investigation policy. Confirmation is evidence, never a verdict.
use crate::model::{Network, Site};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const CONFIRMATION_TTL_SECONDS: i64 = 720;
pub const CONFIRMATION_RETRY_SECONDS: i64 = 60;
pub const CONFIRMATION_COOLDOWN_SECONDS: i64 = 3600;
pub const CONFIRMATION_QUEUE_LIMIT: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardianDisposition {
    Confirmed,
    Pending,
    Unsupported,
    Clear,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardianPriority {
    Low,
    Medium,
    High,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardianConfidence {
    Low,
    Medium,
    High,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuardianEvaluation {
    pub disposition: GuardianDisposition,
    pub priority: GuardianPriority,
    pub confidence: GuardianConfidence,
    pub reason: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum GuardianEvidence {
    Identity {
        expected: Option<String>,
        observed: Option<String>,
    },
    StateHash {
        comparison_height: Option<u64>,
        hash: Option<String>,
    },
    ChainHeight {
        height: Option<u64>,
        reference_heights: [Option<u64>; 3],
    },
    Peers {
        count: Option<u64>,
        adaptive_threshold: Option<u64>,
        threshold_review_required: bool,
    },
    RegistryIdentity {
        expected: Option<String>,
        remote: Option<String>,
        source_anomalous: bool,
        semantics_verified: bool,
        enabled: bool,
    },
    MetadataOnly,
    Unsupported {
        reason: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuardianObservation {
    pub site: Site,
    pub observed_at: DateTime<Utc>,
    pub evidence: GuardianEvidence,
}
fn result(
    disposition: GuardianDisposition,
    priority: GuardianPriority,
    reason: &str,
) -> GuardianEvaluation {
    GuardianEvaluation {
        disposition,
        priority,
        confidence: if disposition == GuardianDisposition::Confirmed {
            GuardianConfidence::High
        } else {
            GuardianConfidence::Low
        },
        reason: reason.into(),
    }
}
fn pending(reason: &str) -> GuardianEvaluation {
    result(GuardianDisposition::Pending, GuardianPriority::Low, reason)
}
fn valid_key(value: &Option<String>) -> Option<String> {
    let value = value.as_ref()?.trim();
    (value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| value.to_ascii_lowercase())
}
fn identity_pair(
    a: &Option<String>,
    b: &Option<String>,
    c: &Option<String>,
    d: &Option<String>,
) -> GuardianEvaluation {
    let (Some(a), Some(b), Some(c), Some(d)) =
        (valid_key(a), valid_key(b), valid_key(c), valid_key(d))
    else {
        return pending("missing_or_invalid_identity");
    };
    if a != c {
        return pending("expected_identity_disagreement");
    }
    if a != b && c != d {
        result(
            GuardianDisposition::Confirmed,
            GuardianPriority::High,
            "identity_mismatch_confirmed",
        )
    } else if a == b && c == d {
        result(
            GuardianDisposition::Clear,
            GuardianPriority::Low,
            "identity_matches",
        )
    } else {
        pending("identity_not_reproduced_at_both_sites")
    }
}
/// Inputs must describe the same network, node, and signal. Missing facts never count as agreement.
#[must_use]
pub fn evaluate_pair(
    left: &GuardianObservation,
    right: &GuardianObservation,
    now: DateTime<Utc>,
) -> GuardianEvaluation {
    if left.site == right.site {
        return pending("distinct_sites_required");
    }
    let ttl = Duration::seconds(CONFIRMATION_TTL_SECONDS);
    if left.observed_at > now
        || right.observed_at > now
        || now - left.observed_at > ttl
        || now - right.observed_at > ttl
        || (left.observed_at - right.observed_at).abs() > ttl
    {
        return pending("confirmation_window_expired");
    }
    match (&left.evidence, &right.evidence) {
        (
            GuardianEvidence::Identity {
                expected: a,
                observed: b,
            },
            GuardianEvidence::Identity {
                expected: c,
                observed: d,
            },
        ) => identity_pair(a, b, c, d),
        (
            GuardianEvidence::StateHash {
                comparison_height: a,
                hash: b,
            },
            GuardianEvidence::StateHash {
                comparison_height: c,
                hash: d,
            },
        ) => {
            let (Some(a), Some(c), Some(b), Some(d)) = (a, c, valid_key(b), valid_key(d)) else {
                return pending("missing_or_invalid_state_hash");
            };
            if *a == 0 || *c == 0 || a != c {
                return pending("exact_common_height_required");
            }
            if b != d {
                result(
                    GuardianDisposition::Confirmed,
                    GuardianPriority::High,
                    "common_height_state_hash_divergence",
                )
            } else {
                result(
                    GuardianDisposition::Clear,
                    GuardianPriority::Low,
                    "state_hash_matches",
                )
            }
        }
        (
            GuardianEvidence::ChainHeight {
                height: a,
                reference_heights: b,
            },
            GuardianEvidence::ChainHeight {
                height: c,
                reference_heights: d,
            },
        ) => {
            fn ahead(height: Option<u64>, references: &[Option<u64>; 3]) -> Option<bool> {
                let mut values = [references[0]?, references[1]?, references[2]?];
                let height = height?;
                if height == 0 || values.contains(&0) {
                    return None;
                }
                values.sort_unstable();
                Some(height.saturating_sub(values[1]) >= 10)
            }
            match (ahead(*a, b), ahead(*c, d)) {
                (Some(true), Some(true)) => result(
                    GuardianDisposition::Confirmed,
                    GuardianPriority::Medium,
                    "chain_ahead_confirmed",
                ),
                (Some(false), Some(false)) => result(
                    GuardianDisposition::Clear,
                    GuardianPriority::Low,
                    "chain_within_reference",
                ),
                _ => pending("three_valid_references_and_both_sites_required"),
            }
        }
        (
            GuardianEvidence::Peers {
                count: a,
                adaptive_threshold: b,
                threshold_review_required: c,
            },
            GuardianEvidence::Peers {
                count: d,
                adaptive_threshold: e,
                threshold_review_required: f,
            },
        ) => {
            let (Some(a), Some(d)) = (a, d) else {
                return pending("missing_peer_count");
            };
            if *a >= 2000 && *d >= 2000 {
                return result(
                    GuardianDisposition::Confirmed,
                    GuardianPriority::High,
                    "hard_peer_limit_confirmed",
                );
            }
            let (Some(b), Some(e)) = (b, e) else {
                return pending("peer_baseline_unavailable");
            };
            if *c || *f {
                return pending("peer_threshold_operator_review_required");
            }
            if b != e {
                return pending("peer_threshold_disagreement");
            }
            if a >= b && d >= e {
                result(
                    GuardianDisposition::Confirmed,
                    GuardianPriority::Medium,
                    "adaptive_peer_limit_confirmed",
                )
            } else if a < b && d < e {
                result(
                    GuardianDisposition::Clear,
                    GuardianPriority::Low,
                    "peer_count_within_baseline",
                )
            } else {
                pending("peer_limit_not_reproduced_at_both_sites")
            }
        }
        (
            GuardianEvidence::RegistryIdentity {
                expected: a,
                remote: b,
                source_anomalous: c,
                semantics_verified: d,
                enabled: e,
            },
            GuardianEvidence::RegistryIdentity {
                expected: f,
                remote: g,
                source_anomalous: h,
                semantics_verified: i,
                enabled: j,
            },
        ) => {
            if !d || !i || !e || !j {
                return result(
                    GuardianDisposition::Unsupported,
                    GuardianPriority::Low,
                    "registry_semantics_unverified_or_disabled",
                );
            }
            if *c || *h {
                return pending("registry_source_anomalous");
            }
            identity_pair(a, b, f, g)
        }
        (GuardianEvidence::MetadataOnly, _) | (_, GuardianEvidence::MetadataOnly) => result(
            GuardianDisposition::Unsupported,
            GuardianPriority::Low,
            "metadata_is_auxiliary_only",
        ),
        (GuardianEvidence::Unsupported { reason }, _)
        | (_, GuardianEvidence::Unsupported { reason }) => result(
            GuardianDisposition::Unsupported,
            GuardianPriority::Low,
            reason,
        ),
        _ => pending("evidence_kind_disagreement"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerBaseline {
    pub active_nodes: usize,
    /// Twice the exact p99, preserving half-valued medians without floats.
    pub p99_times_two: Option<u128>,
    pub threshold: Option<u64>,
    pub operator_review_required: bool,
}
/// Each entry contains valid samples from one active node within the preceding 30 days.
/// Half-valued medians remain exact until the final threshold is rounded upward.
#[must_use]
pub fn peer_threshold(
    active_node_samples: &[Vec<u64>],
    previous_daily_threshold: Option<u64>,
) -> PeerBaseline {
    let mut medians: Vec<u128> = active_node_samples
        .iter()
        .filter_map(|samples| {
            if samples.is_empty() {
                return None;
            }
            let mut values = samples.clone();
            values.sort_unstable();
            let m = values.len() / 2;
            Some(if values.len() % 2 == 0 {
                u128::from(values[m - 1]) + u128::from(values[m])
            } else {
                u128::from(values[m]) * 2
            })
        })
        .collect();
    let active_nodes = medians.len();
    if active_nodes < 100 {
        return PeerBaseline {
            active_nodes,
            p99_times_two: None,
            threshold: None,
            operator_review_required: false,
        };
    }
    medians.sort_unstable();
    let rank = (active_nodes as u128 * 99).div_ceil(100) as usize;
    let p99_times_two = medians[rank - 1];
    let threshold = (p99_times_two * 3).div_ceil(2).min(u128::from(u64::MAX)) as u64;
    let threshold = threshold.max(250);
    let operator_review_required = previous_daily_threshold
        .is_some_and(|previous| u128::from(threshold) * 100 >= u128::from(previous) * 125);
    PeerBaseline {
        active_nodes,
        p99_times_two: Some(p99_times_two),
        threshold: Some(threshold),
        operator_review_required,
    }
}
/// Counts are current open events and new events in the preceding rolling 24 hours.
#[must_use]
pub fn promotion_allowed(new_network_events_24h: usize, open_network_events: usize) -> bool {
    new_network_events_24h < 10 && open_network_events < 50
}
#[must_use]
pub fn dedupe_key(network: Network, node_id: &str, kind: &str) -> String {
    let mut digest = Sha256::new();
    for field in [network.as_str(), node_id, kind] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field.as_bytes());
    }
    format!("{:x}", digest.finalize())
}
#[must_use]
pub fn priority_after_inactivity(
    priority: GuardianPriority,
    last_reproduced: DateTime<Utc>,
    now: DateTime<Utc>,
) -> GuardianPriority {
    if now - last_reproduced >= Duration::days(7) {
        GuardianPriority::Low
    } else {
        priority
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmationJob {
    pub key: String,
    pub probe: String,
    pub created_at: DateTime<Utc>,
    pub next_attempt_at: DateTime<Utc>,
    pub attempt: u8,
    pub running: bool,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmationQueue {
    jobs: Vec<ConfirmationJob>,
    cooldowns: BTreeMap<String, DateTime<Utc>>,
}
impl ConfirmationQueue {
    fn prune(&mut self, now: DateTime<Utc>) {
        let mut expired = Vec::new();
        self.jobs.retain(|job| {
            let valid = now - job.created_at <= Duration::seconds(CONFIRMATION_TTL_SECONDS);
            if !valid {
                expired.push(job.key.clone());
            }
            valid
        });
        for key in expired {
            self.cooldowns
                .insert(key, now + Duration::seconds(CONFIRMATION_COOLDOWN_SECONDS));
        }
        self.cooldowns.retain(|_, until| *until > now);
    }
    pub fn enqueue(&mut self, key: String, probe: String, now: DateTime<Utc>) -> bool {
        self.prune(now);
        if self.jobs.iter().any(|job| job.key == key)
            || self.cooldowns.contains_key(&key)
            || self.jobs.iter().filter(|job| job.probe == probe).count() >= CONFIRMATION_QUEUE_LIMIT
        {
            return false;
        }
        self.jobs.push(ConfirmationJob {
            key,
            probe,
            created_at: now,
            next_attempt_at: now,
            attempt: 0,
            running: false,
        });
        true
    }
    pub fn start_next(&mut self, probe: &str, now: DateTime<Utc>) -> Option<ConfirmationJob> {
        self.prune(now);
        if self
            .jobs
            .iter()
            .any(|job| job.probe == probe && job.running)
        {
            return None;
        }
        let job = self
            .jobs
            .iter_mut()
            .find(|job| job.probe == probe && !job.running && job.next_attempt_at <= now)?;
        job.running = true;
        job.attempt += 1;
        Some(job.clone())
    }
    pub fn finish(&mut self, key: &str, reproduced: bool, now: DateTime<Utc>) {
        let Some(index) = self
            .jobs
            .iter()
            .position(|job| job.key == key && job.running)
        else {
            return;
        };
        let job = &mut self.jobs[index];
        if !reproduced
            && job.attempt == 1
            && now + Duration::seconds(CONFIRMATION_RETRY_SECONDS)
                <= job.created_at + Duration::seconds(CONFIRMATION_TTL_SECONDS)
        {
            job.running = false;
            job.next_attempt_at = now + Duration::seconds(CONFIRMATION_RETRY_SECONDS);
        } else {
            self.jobs.remove(index);
            self.cooldowns.insert(
                key.into(),
                now + Duration::seconds(CONFIRMATION_COOLDOWN_SECONDS),
            );
        }
    }
    #[must_use]
    pub fn jobs(&self) -> &[ConfirmationJob] {
        &self.jobs
    }
}
