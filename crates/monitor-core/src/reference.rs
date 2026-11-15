use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};

use crate::{
    community::Verdict,
    finalization::{MAX_OBSERVATION_GAP_SECONDS, block_verdict},
    model::Severity,
};

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

#[derive(Debug, Default)]
pub struct FinalizationTracker {
    baselines: HashMap<String, DateTime<Utc>>,
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
        if target_height == 0
            || target_finalized == 0
            || reference_height == 0
            || reference_finalized == 0
            || target_finalized > target_height
            || reference_finalized > reference_height
        {
            return None;
        }
        if let Some(previous) = self.baselines.get(node_id) {
            let elapsed = observed_at.signed_duration_since(*previous);
            if elapsed < Duration::zero() {
                return None;
            }
            if elapsed > Duration::seconds(MAX_OBSERVATION_GAP_SECONDS) {
                self.baselines.insert(node_id.to_owned(), observed_at);
                return None;
            }
        }
        self.baselines.insert(node_id.to_owned(), observed_at);
        match block_verdict(reference_finalized.saturating_sub(target_finalized)) {
            Verdict::Healthy => None,
            Verdict::Warning => Some(Severity::Warning),
            Verdict::Critical => Some(Severity::Critical),
        }
    }
}
