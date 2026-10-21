use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};

use crate::model::Severity;

#[must_use]
pub fn median(values: &[u64]) -> Option<u64> {
    if values.len() < 3 || values.len().is_multiple_of(2) {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted.get(sorted.len() / 2).copied()
}

#[derive(Debug, Clone, Copy)]
pub struct ReferenceQuorum {
    expected: usize,
    responding: usize,
}

impl ReferenceQuorum {
    #[must_use]
    pub const fn new(expected: usize, responding: usize) -> Self {
        Self {
            expected,
            responding,
        }
    }

    #[must_use]
    pub const fn is_usable(self) -> bool {
        self.expected > 0 && self.responding > self.expected / 2
    }

    #[must_use]
    pub fn majority_stalled(self, stalled: &[bool]) -> bool {
        self.is_usable()
            && stalled.len() >= self.responding
            && stalled.iter().filter(|value| **value).count() > self.expected / 2
    }
}

#[derive(Debug, Clone)]
struct Baseline {
    target_height: u64,
    target_finalized: u64,
    reference_height: u64,
    reference_finalized: u64,
    observed_at: DateTime<Utc>,
}

#[derive(Debug, Default)]
pub struct FinalizationTracker {
    baselines: HashMap<String, Baseline>,
}

impl FinalizationTracker {
    #[must_use]
    pub fn observe(
        &mut self,
        node_id: &str,
        target_height: u64,
        target_finalized: u64,
        reference_height: u64,
        reference_finalized: u64,
        observed_at: DateTime<Utc>,
    ) -> Option<Severity> {
        let Some(baseline) = self.baselines.get(node_id) else {
            self.baselines.insert(
                node_id.to_owned(),
                Baseline {
                    target_height,
                    target_finalized,
                    reference_height,
                    reference_finalized,
                    observed_at,
                },
            );
            return None;
        };

        if target_finalized != baseline.target_finalized {
            self.baselines.insert(
                node_id.to_owned(),
                Baseline {
                    target_height,
                    target_finalized,
                    reference_height,
                    reference_finalized,
                    observed_at,
                },
            );
            return None;
        }

        let elapsed = observed_at.signed_duration_since(baseline.observed_at);
        let target_chain_advanced = target_height.saturating_sub(baseline.target_height) >= 20;
        let reference_chain_advanced =
            reference_height.saturating_sub(baseline.reference_height) >= 20;
        let reference_finalization_advanced = reference_finalized > baseline.reference_finalized;
        if !(target_chain_advanced && reference_chain_advanced && reference_finalization_advanced) {
            return None;
        }
        if elapsed >= Duration::minutes(60) {
            Some(Severity::Critical)
        } else if elapsed >= Duration::minutes(30) {
            Some(Severity::Warning)
        } else {
            None
        }
    }
}
