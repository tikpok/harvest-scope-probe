use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::model::Site;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecuritySignalKind {
    IdentityMismatch,
    DnsAnswerChanged,
    AsnChanged,
    HostingProviderChanged,
    CrossSiteDivergence,
    ChainHeightAheadOfReference,
    PeerCardinalityLimit,
    AsnConcentration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalConfidence {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShadowSecuritySignal {
    pub kind: SecuritySignalKind,
    pub confidence: SignalConfidence,
    pub evidence: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkMetadataSnapshot {
    pub addresses: BTreeSet<IpAddr>,
    pub asns: BTreeSet<u32>,
    pub hosting_providers: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SiteFingerprint {
    pub site: Site,
    pub observed_at: DateTime<Utc>,
    pub fingerprint: String,
}

#[must_use]
pub fn detect_identity_mismatch(
    expected: Option<&str>,
    observed: Option<&str>,
) -> Option<ShadowSecuritySignal> {
    let expected = normalized_identity(expected?)?;
    let observed = normalized_identity(observed?)?;
    (expected != observed).then(|| ShadowSecuritySignal {
        kind: SecuritySignalKind::IdentityMismatch,
        confidence: SignalConfidence::High,
        evidence: json!({
            "expectedIdentity": expected,
            "observedIdentity": observed,
        }),
    })
}

#[must_use]
pub fn compare_network_metadata(
    previous: &NetworkMetadataSnapshot,
    current: &NetworkMetadataSnapshot,
) -> Vec<ShadowSecuritySignal> {
    let mut signals = Vec::with_capacity(3);
    if previous.addresses != current.addresses {
        signals.push(ShadowSecuritySignal {
            kind: SecuritySignalKind::DnsAnswerChanged,
            confidence: SignalConfidence::Medium,
            evidence: json!({
                "previousAddresses": previous.addresses,
                "currentAddresses": current.addresses,
            }),
        });
    }
    if !previous.asns.is_empty() && !current.asns.is_empty() && previous.asns != current.asns {
        signals.push(ShadowSecuritySignal {
            kind: SecuritySignalKind::AsnChanged,
            confidence: SignalConfidence::Medium,
            evidence: json!({
                "previousAsns": previous.asns,
                "currentAsns": current.asns,
            }),
        });
    }
    let previous_providers = canonical_provider_set(&previous.hosting_providers);
    let current_providers = canonical_provider_set(&current.hosting_providers);
    if !previous_providers.is_empty()
        && !current_providers.is_empty()
        && previous_providers != current_providers
    {
        signals.push(ShadowSecuritySignal {
            kind: SecuritySignalKind::HostingProviderChanged,
            confidence: SignalConfidence::Low,
            evidence: json!({
                "previousHostingProviders": previous.hosting_providers,
                "currentHostingProviders": current.hosting_providers,
                "inferred": true,
            }),
        });
    }
    signals
}

#[must_use]
pub fn compare_site_fingerprints(
    left: &SiteFingerprint,
    right: &SiteFingerprint,
    max_skew: Duration,
) -> Option<ShadowSecuritySignal> {
    if max_skew < Duration::zero()
        || left.site == right.site
        || left.fingerprint.is_empty()
        || right.fingerprint.is_empty()
        || left.fingerprint == right.fingerprint
        || (left.observed_at - right.observed_at).abs() > max_skew
    {
        return None;
    }
    Some(ShadowSecuritySignal {
        kind: SecuritySignalKind::CrossSiteDivergence,
        confidence: SignalConfidence::Medium,
        evidence: json!({
            "leftSite": left.site,
            "leftObservedAt": left.observed_at,
            "leftFingerprint": left.fingerprint,
            "rightSite": right.site,
            "rightObservedAt": right.observed_at,
            "rightFingerprint": right.fingerprint,
        }),
    })
}

#[must_use]
pub fn detect_chain_height_ahead(
    target_height: u64,
    reference_median_height: u64,
    maximum_ahead_blocks: u64,
) -> Option<ShadowSecuritySignal> {
    (target_height > reference_median_height.saturating_add(maximum_ahead_blocks)).then(|| {
        ShadowSecuritySignal {
            kind: SecuritySignalKind::ChainHeightAheadOfReference,
            confidence: SignalConfidence::Medium,
            evidence: json!({
                "targetHeight": target_height,
                "referenceMedianHeight": reference_median_height,
                "maximumAheadBlocks": maximum_ahead_blocks,
            }),
        }
    })
}

#[must_use]
pub fn detect_peer_cardinality_excess(
    observed_peer_count: usize,
    maximum_peer_count: usize,
) -> Option<ShadowSecuritySignal> {
    (observed_peer_count > maximum_peer_count).then(|| ShadowSecuritySignal {
        kind: SecuritySignalKind::PeerCardinalityLimit,
        confidence: SignalConfidence::High,
        evidence: json!({
            "observedPeerCount": observed_peer_count,
            "maximumPeerCount": maximum_peer_count,
        }),
    })
}

#[must_use]
pub fn detect_asn_concentration(
    asns: &[u32],
    minimum_population: usize,
    threshold_percent: u8,
) -> Option<ShadowSecuritySignal> {
    if minimum_population == 0
        || asns.len() < minimum_population
        || !(1..=100).contains(&threshold_percent)
    {
        return None;
    }
    let mut counts = BTreeMap::new();
    for asn in asns {
        *counts.entry(*asn).or_insert(0_usize) += 1;
    }
    let (asn, count) = counts
        .into_iter()
        .max_by_key(|(asn, count)| (*count, std::cmp::Reverse(*asn)))?;
    if (count as u128) * 100 < (asns.len() as u128) * u128::from(threshold_percent) {
        return None;
    }
    Some(ShadowSecuritySignal {
        kind: SecuritySignalKind::AsnConcentration,
        confidence: SignalConfidence::Low,
        evidence: json!({
            "asn": asn,
            "matchingNodes": count,
            "totalNodes": asns.len(),
            "thresholdPercent": threshold_percent,
        }),
    })
}

fn normalized_identity(value: &str) -> Option<String> {
    let value = value.trim();
    (value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| value.to_ascii_lowercase())
}

fn canonical_provider_set(values: &BTreeSet<String>) -> BTreeSet<String> {
    values
        .iter()
        .map(|value| value.trim().to_lowercase())
        .filter(|value| !value.is_empty())
        .collect()
}
