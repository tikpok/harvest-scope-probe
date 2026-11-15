//! Shared block-based finalization evidence interpretation; callers enforce freshness.

use std::collections::HashSet;

use serde_json::Value;

use crate::{community::Verdict, model::Outcome};

pub const WARNING_BLOCKS: u64 = 60;
pub const CRITICAL_BLOCKS: u64 = 120;
pub const MAX_OBSERVATION_GAP_SECONDS: i64 = 1_800;

#[must_use]
pub fn block_verdict(blocks: u64) -> Verdict {
    if blocks >= CRITICAL_BLOCKS {
        Verdict::Critical
    } else if blocks >= WARNING_BLOCKS {
        Verdict::Warning
    } else {
        Verdict::Healthy
    }
}

#[must_use]
pub fn node_outcome(outcome: &Outcome, evidence: &Value) -> Outcome {
    if matches!(
        outcome,
        Outcome::Unknown(_) | Outcome::Unsupported(_) | Outcome::Failure(_)
    ) {
        return outcome.clone();
    }
    if !matches!(
        outcome,
        Outcome::Healthy | Outcome::FinalizationStalledMinutes(_) | Outcome::LagBlocks(_)
    ) {
        return Outcome::Unknown("unsupported finalization outcome".into());
    }
    let Some(blocks) = node_gap(evidence) else {
        return Outcome::Unknown("valid finalization reference evidence unavailable".into());
    };
    if matches!(outcome, Outcome::LagBlocks(reported) if *reported != blocks) {
        return Outcome::Unknown("finalization block gap disagrees with evidence".into());
    }
    Outcome::LagBlocks(blocks)
}

fn positive(evidence: &Value, field: &str) -> Option<u64> {
    evidence.get(field)?.as_u64().filter(|value| *value > 0)
}

fn node_gap(evidence: &Value) -> Option<u64> {
    let height = positive(evidence, "height")?;
    let finalized = positive(evidence, "finalizedHeight")?;
    let reference_finalized = positive(evidence, "referenceMedianFinalizedHeight")?;
    let expected = positive(evidence, "referenceExpected")?;
    let responding = positive(evidence, "referenceResponding")?;
    if finalized > height || responding > expected || responding <= expected / 2 {
        return None;
    }
    let hosts = evidence.get("referenceHosts")?.as_array()?;
    let heights = evidence.get("referenceHeights")?.as_array()?;
    if u64::try_from(hosts.len()).ok()? != responding || hosts.len() != heights.len() {
        return None;
    }
    let mut distinct_hosts = HashSet::new();
    for host in hosts {
        let host = host
            .as_str()?
            .trim()
            .trim_end_matches('.')
            .to_ascii_lowercase();
        if host.is_empty() || !distinct_hosts.insert(host) {
            return None;
        }
    }
    let mut heights = heights
        .iter()
        .map(|height| height.as_u64().filter(|value| *value > 0))
        .collect::<Option<Vec<_>>>()?;
    heights.sort_unstable();
    let middle = heights.len() / 2;
    let median_height = if heights.len() % 2 == 1 {
        heights[middle]
    } else {
        let lower = heights[middle - 1];
        lower + (heights[middle] - lower) / 2
    };
    if reference_finalized > median_height {
        return None;
    }
    Some(reference_finalized.saturating_sub(finalized))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkEvidence {
    pub expected_references: u64,
    pub usable_references: u64,
    pub stalled_references: u64,
    pub stalled_blocks: u64,
}

#[must_use]
pub fn network_sample(evidence: &Value, leased_references: u64) -> Option<NetworkEvidence> {
    let expected = positive(evidence, "referenceExpected")?;
    let responding = evidence.get("referenceResponding")?.as_u64()?;
    let network = evidence.get("networkFinalization")?;
    if network.get("metric")?.as_str()? != "stalled_blocks" {
        return None;
    }
    let sample = NetworkEvidence {
        expected_references: network.get("expectedReferences")?.as_u64()?,
        usable_references: network.get("usableReferences")?.as_u64()?,
        stalled_references: network.get("stalledReferences")?.as_u64()?,
        stalled_blocks: network.get("stalledBlocks")?.as_u64()?,
    };
    if sample.expected_references != expected
        || expected != leased_references
        || responding > expected
        || sample.usable_references > responding
        || sample.usable_references <= expected / 2
        || sample.stalled_references > sample.usable_references
    {
        return None;
    }
    Some(sample)
}
