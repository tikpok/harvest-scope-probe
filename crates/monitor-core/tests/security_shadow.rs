use std::{collections::BTreeSet, net::IpAddr, str::FromStr};

use chrono::{Duration, TimeZone, Utc};
use monitor_core::{
    model::Site,
    security::{
        compare_network_metadata, compare_site_fingerprints, detect_asn_concentration,
        detect_chain_height_ahead, detect_identity_mismatch, detect_peer_cardinality_excess,
        NetworkMetadataSnapshot, SecuritySignalKind, SignalConfidence, SiteFingerprint,
    },
};

fn metadata(addresses: &[&str], asns: &[u32], providers: &[&str]) -> NetworkMetadataSnapshot {
    NetworkMetadataSnapshot {
        addresses: addresses
            .iter()
            .map(|value| IpAddr::from_str(value).unwrap())
            .collect::<BTreeSet<_>>(),
        asns: asns.iter().copied().collect::<BTreeSet<_>>(),
        hosting_providers: providers
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<BTreeSet<_>>(),
    }
}

#[test]
fn identity_mismatch_is_high_confidence_but_case_only_changes_are_ignored() {
    let expected = "A".repeat(64);
    let same_identity = "a".repeat(64);
    assert!(detect_identity_mismatch(Some(&expected), Some(&same_identity)).is_none());

    let observed = "B".repeat(64);
    let signal = detect_identity_mismatch(Some(&expected), Some(&observed)).unwrap();
    assert_eq!(signal.kind, SecuritySignalKind::IdentityMismatch);
    assert_eq!(signal.confidence, SignalConfidence::High);
    assert_eq!(
        signal.evidence["expectedIdentity"],
        expected.to_ascii_lowercase()
    );
    assert_eq!(
        signal.evidence["observedIdentity"],
        observed.to_ascii_lowercase()
    );

    assert!(detect_identity_mismatch(None, Some(&observed)).is_none());
    assert!(detect_identity_mismatch(Some("invalid"), Some(&observed)).is_none());
}

#[test]
fn network_metadata_changes_distinguish_facts_from_hosting_inference() {
    let previous = metadata(&["203.0.113.10"], &[64_500], &["Example Host"]);
    let current = metadata(
        &["203.0.113.11", "2001:db8::11"],
        &[64_501],
        &["Other Host"],
    );

    let signals = compare_network_metadata(&previous, &current);
    assert_eq!(signals.len(), 3);
    assert_eq!(signals[0].kind, SecuritySignalKind::DnsAnswerChanged);
    assert_eq!(signals[0].confidence, SignalConfidence::Medium);
    assert_eq!(signals[1].kind, SecuritySignalKind::AsnChanged);
    assert_eq!(signals[1].confidence, SignalConfidence::Medium);
    assert_eq!(signals[2].kind, SecuritySignalKind::HostingProviderChanged);
    assert_eq!(signals[2].confidence, SignalConfidence::Low);

    let unknown_provider = metadata(&["203.0.113.10"], &[64_500], &[]);
    assert!(compare_network_metadata(&previous, &unknown_provider)
        .iter()
        .all(|signal| signal.kind != SecuritySignalKind::HostingProviderChanged));

    let formatting_only = metadata(&["203.0.113.10"], &[64_500], &["  example host  "]);
    assert!(compare_network_metadata(&previous, &formatting_only)
        .iter()
        .all(|signal| signal.kind != SecuritySignalKind::HostingProviderChanged));
}

#[test]
fn site_divergence_requires_distinct_sites_recent_samples_and_stable_fingerprints() {
    let observed_at = Utc.with_ymd_and_hms(2026, 9, 4, 0, 0, 0).unwrap();
    let site_a = SiteFingerprint {
        site: Site::A,
        observed_at,
        fingerprint: "response-hash-a".to_owned(),
    };
    let site_b = SiteFingerprint {
        site: Site::B,
        observed_at: observed_at + Duration::minutes(2),
        fingerprint: "response-hash-b".to_owned(),
    };

    let signal = compare_site_fingerprints(&site_a, &site_b, Duration::minutes(10)).unwrap();
    assert_eq!(signal.kind, SecuritySignalKind::CrossSiteDivergence);
    assert_eq!(signal.confidence, SignalConfidence::Medium);

    let stale = SiteFingerprint {
        observed_at: observed_at + Duration::minutes(11),
        ..site_b.clone()
    };
    assert!(compare_site_fingerprints(&site_a, &stale, Duration::minutes(10)).is_none());
    assert!(compare_site_fingerprints(&site_a, &site_a, Duration::minutes(10)).is_none());

    let same = SiteFingerprint {
        fingerprint: site_a.fingerprint.clone(),
        ..site_b
    };
    assert!(compare_site_fingerprints(&site_a, &same, Duration::minutes(10)).is_none());
}

#[test]
fn bounded_anomaly_detectors_fail_closed_without_overflowing() {
    assert!(detect_chain_height_ahead(1_001, 900, 100).is_some());
    assert!(detect_chain_height_ahead(1_000, 900, 100).is_none());
    assert!(detect_chain_height_ahead(u64::MAX, u64::MAX, 100).is_none());

    let peers = detect_peer_cardinality_excess(5_001, 5_000).unwrap();
    assert_eq!(peers.kind, SecuritySignalKind::PeerCardinalityLimit);
    assert_eq!(peers.confidence, SignalConfidence::High);
    assert!(detect_peer_cardinality_excess(5_000, 5_000).is_none());

    let concentration = detect_asn_concentration(&[64_500, 64_500, 64_500, 64_501], 4, 75)
        .expect("exact threshold should create a shadow signal");
    assert_eq!(concentration.kind, SecuritySignalKind::AsnConcentration);
    assert_eq!(concentration.confidence, SignalConfidence::Low);
    assert!(detect_asn_concentration(&[64_500, 64_500, 64_501], 4, 75).is_none());
    assert!(detect_asn_concentration(&[64_500; 4], 4, 101).is_none());
}
