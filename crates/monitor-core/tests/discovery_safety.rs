use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use chrono::{Duration, TimeZone, Utc};
use monitor_core::{
    discovery::{
        cadence_for_missing, cadence_for_missing_with_reachability, candidate_count_is_anomalous,
        classify_source_run, merge_candidates, quarantine_rate_is_anomalous, source_anomalies,
        CandidateRole, DiscoveryCandidate, DiscoverySourceKind, EndpointAlias, EndpointKind,
        MissingCadence, SourceAnomaly, SourceRunMetrics, SourceRunResult, SourceRunStatus,
    },
    model::Network,
    safety::{validate_public_target, TargetSafetyError},
};

#[test]
fn deduplicates_by_public_key_before_endpoint() {
    let candidates = vec![
        DiscoveryCandidate::new(Network::SymbolMainnet, "api-a.example", 7900)
            .with_public_key("ABC"),
        DiscoveryCandidate::new(Network::SymbolMainnet, "api-b.example", 7900)
            .with_public_key("abc"),
    ];
    let merged = merge_candidates(candidates);
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].aliases.len(), 2);
}

#[test]
fn does_not_merge_equal_keys_across_networks() {
    let merged = merge_candidates(vec![
        DiscoveryCandidate::new(Network::SymbolMainnet, "symbol.example", 7900)
            .with_node_public_key("ABC"),
        DiscoveryCandidate::new(Network::Nis1Mainnet, "nis.example", 7890)
            .with_node_public_key("abc"),
    ]);

    assert_eq!(merged.len(), 2);
    assert_eq!(merged[0].network, Network::Nis1Mainnet);
    assert_eq!(merged[1].network, Network::SymbolMainnet);
}

#[test]
fn node_public_key_takes_precedence_over_public_key() {
    let merged = merge_candidates(vec![
        DiscoveryCandidate::new(Network::SymbolMainnet, "one.example", 7900)
            .with_node_public_key("NODE-A")
            .with_public_key("shared"),
        DiscoveryCandidate::new(Network::SymbolMainnet, "two.example", 7900)
            .with_node_public_key("NODE-B")
            .with_public_key("shared"),
    ]);

    assert_eq!(merged.len(), 2);
    assert_eq!(merged[0].node_public_key.as_deref(), Some("node-a"));
    assert_eq!(merged[1].node_public_key.as_deref(), Some("node-b"));
}

#[test]
fn key_based_candidate_absorbs_matching_host_candidate() {
    let merged = merge_candidates(vec![
        DiscoveryCandidate::new(Network::SymbolMainnet, "NODE.EXAMPLE", 7900),
        DiscoveryCandidate::new(Network::SymbolMainnet, "node.example", 7900)
            .with_public_key("ABC"),
    ]);

    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].public_key.as_deref(), Some("abc"));
    assert_eq!(merged[0].host, "node.example");
    assert_eq!(merged[0].aliases.len(), 1);
}

#[test]
fn merge_preserves_normalized_endpoint_metadata_and_seen_range() {
    let first_seen = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
    let last_seen = Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap();
    let first_alias = EndpointAlias {
        host: "REST.EXAMPLE.".to_owned(),
        p2p_port: 3001,
        kind: EndpointKind::Rest,
        scheme: Some("HTTPS".to_owned()),
        source: Some("symbol-primary".to_owned()),
        first_seen_at: Some(first_seen),
        last_seen_at: Some(first_seen),
    };
    let last_alias = EndpointAlias {
        first_seen_at: Some(last_seen),
        last_seen_at: Some(last_seen),
        ..first_alias.clone()
    };

    let merged = merge_candidates(vec![
        DiscoveryCandidate::new(Network::SymbolMainnet, "one.example", 7900)
            .with_public_key("ABC")
            .with_alias(first_alias),
        DiscoveryCandidate::new(Network::SymbolMainnet, "two.example", 7900)
            .with_public_key("abc")
            .with_alias(last_alias),
    ]);
    let rest = merged[0]
        .aliases
        .iter()
        .find(|alias| alias.kind == EndpointKind::Rest)
        .unwrap();

    assert_eq!(rest.host, "rest.example");
    assert_eq!(rest.scheme.as_deref(), Some("https"));
    assert_eq!(rest.source.as_deref(), Some("symbol-primary"));
    assert_eq!(rest.first_seen_at, Some(first_seen));
    assert_eq!(rest.last_seen_at, Some(last_seen));
}

#[test]
fn merge_output_is_independent_of_input_order() {
    let candidates = vec![
        DiscoveryCandidate::new(Network::SymbolMainnet, "Z.EXAMPLE", 7900).with_public_key("ABC"),
        DiscoveryCandidate::new(Network::SymbolMainnet, "a.example", 7900).with_public_key("abc"),
        DiscoveryCandidate::new(Network::SymbolMainnet, "z.example", 7900),
        DiscoveryCandidate::new(Network::Nis1Mainnet, "nis.example", 7890).with_public_key("ABC"),
    ];
    let mut reversed = candidates.clone();
    reversed.reverse();

    assert_eq!(merge_candidates(candidates), merge_candidates(reversed));
}

#[test]
fn merge_uses_a_deterministic_secondary_key_for_equal_node_public_keys() {
    let candidates = vec![
        DiscoveryCandidate::new(Network::SymbolMainnet, "same.example", 7900)
            .with_node_public_key("NODE")
            .with_public_key("ZZZ"),
        DiscoveryCandidate::new(Network::SymbolMainnet, "same.example", 7900)
            .with_node_public_key("node")
            .with_public_key("AAA"),
    ];
    let mut reversed = candidates.clone();
    reversed.reverse();

    let forward = merge_candidates(candidates);
    let reverse = merge_candidates(reversed);
    assert_eq!(forward, reverse);
    assert_eq!(forward[0].node_public_key.as_deref(), Some("node"));
    assert_eq!(forward[0].public_key.as_deref(), Some("aaa"));
}

#[test]
fn merge_keeps_the_newest_external_source_report() {
    let older_at = Utc.with_ymd_and_hms(2026, 7, 28, 0, 0, 0).unwrap();
    let newer_at = older_at + Duration::minutes(5);
    let mut older = DiscoveryCandidate::new(Network::SymbolMainnet, "node.example", 7900)
        .with_node_public_key("NODE");
    older.reported_health = Some(false);
    older.reported_height = Some(100);
    older.reported_at = Some(older_at);
    older.reported_source = Some("older-source".to_owned());
    let mut newer = DiscoveryCandidate::new(Network::SymbolMainnet, "node.example", 7900)
        .with_node_public_key("node");
    newer.reported_health = Some(true);
    newer.reported_height = Some(200);
    newer.reported_at = Some(newer_at);
    newer.reported_source = Some("nodewatch-symbol-nodes".to_owned());

    let merged = merge_candidates(vec![newer, older]);

    assert_eq!(merged[0].reported_health, Some(true));
    assert_eq!(merged[0].reported_height, Some(200));
    assert_eq!(merged[0].reported_at, Some(newer_at));
    assert_eq!(
        merged[0].reported_source.as_deref(),
        Some("nodewatch-symbol-nodes")
    );
}

#[test]
fn candidate_drop_boundary_is_exact() {
    assert!(!candidate_count_is_anomalous(100, 71));
    assert!(candidate_count_is_anomalous(100, 70));
    assert!(candidate_count_is_anomalous(100, 0));
}

#[test]
fn quarantine_rate_boundaries_are_exact() {
    assert!(!quarantine_rate_is_anomalous(0, 100, 9, 100));
    assert!(quarantine_rate_is_anomalous(0, 100, 10, 100));
    assert!(!quarantine_rate_is_anomalous(5, 100, 14, 100));
    assert!(quarantine_rate_is_anomalous(5, 100, 15, 100));
    assert!(!quarantine_rate_is_anomalous(19, 100, 19, 100));
    assert!(quarantine_rate_is_anomalous(20, 100, 20, 100));
}

#[test]
fn source_run_status_distinguishes_success_anomaly_error_and_deferred() {
    let previous = SourceRunMetrics::new(100, 0);
    assert_eq!(
        classify_source_run(false, false, Some(previous), SourceRunMetrics::new(71, 0)),
        SourceRunStatus::Success
    );
    assert_eq!(
        classify_source_run(false, false, Some(previous), SourceRunMetrics::new(70, 0)),
        SourceRunStatus::Anomaly
    );
    assert_eq!(
        classify_source_run(true, false, None, SourceRunMetrics::new(0, 0)),
        SourceRunStatus::Error
    );
    assert_eq!(
        classify_source_run(false, true, None, SourceRunMetrics::new(0, 0)),
        SourceRunStatus::Deferred
    );
}

#[test]
fn source_drop_compares_total_candidates_from_healthy_runs() {
    let previous = SourceRunMetrics::new(80, 20);
    assert_eq!(
        classify_source_run(false, false, Some(previous), SourceRunMetrics::new(69, 2)),
        SourceRunStatus::Success
    );
    assert_eq!(
        classify_source_run(false, false, Some(previous), SourceRunMetrics::new(68, 2)),
        SourceRunStatus::Anomaly
    );
}

#[test]
fn source_quarantine_rate_without_a_healthy_baseline_uses_only_twenty_percent() {
    assert!(!source_anomalies(None, SourceRunMetrics::new(81, 19))
        .contains(&SourceAnomaly::QuarantineRateIncrease));
    assert!(source_anomalies(None, SourceRunMetrics::new(80, 20))
        .contains(&SourceAnomaly::QuarantineRateIncrease));
    assert_eq!(
        source_anomalies(None, SourceRunMetrics::new(0, 0)),
        vec![SourceAnomaly::ZeroCandidates]
    );
}

#[test]
fn source_quarantine_rate_with_a_healthy_baseline_keeps_ten_point_boundary() {
    let previous = SourceRunMetrics::new(95, 5);
    assert!(
        !source_anomalies(Some(previous), SourceRunMetrics::new(86, 14))
            .contains(&SourceAnomaly::QuarantineRateIncrease)
    );
    assert!(
        source_anomalies(Some(previous), SourceRunMetrics::new(85, 15))
            .contains(&SourceAnomaly::QuarantineRateIncrease)
    );
}

#[test]
fn source_run_result_uses_the_frozen_wire_shape() {
    let now = Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap();
    let result = SourceRunResult {
        source_id: "symbol-services".to_owned(),
        source_kind: DiscoverySourceKind::SymbolPrimary,
        started_at: now,
        completed_at: now,
        status: SourceRunStatus::Success,
        total_candidate_count: 1,
        accepted_candidates: vec![DiscoveryCandidate::new(
            Network::SymbolMainnet,
            "seed.example",
            7900,
        )
        .as_auxiliary_seed()],
        quarantined_candidates: Vec::new(),
        deferred_work: Vec::new(),
        sanitized_error: None,
    };

    assert_eq!(
        result.accepted_candidates[0].role,
        CandidateRole::AuxiliarySeed
    );
    assert_eq!(
        serde_json::to_value(result).unwrap(),
        serde_json::json!({
            "sourceId": "symbol-services",
            "sourceKind": "symbol_primary",
            "startedAt": "2026-07-18T00:00:00Z",
            "completedAt": "2026-07-18T00:00:00Z",
            "status": "success",
            "totalCandidateCount": 1,
            "acceptedCandidates": [{
                "network": "symbol_mainnet",
                "host": "seed.example",
                "p2pPort": 7900,
                "aliases": [{
                    "host": "seed.example",
                    "p2pPort": 7900,
                    "kind": "p2p"
                }],
                "role": "auxiliary_seed"
            }],
            "quarantinedCandidates": [],
            "deferredWork": []
        })
    );
}

#[test]
fn retains_missing_nodes_before_archiving() {
    let now = Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap();
    assert_eq!(
        cadence_for_missing(now - Duration::days(7), now),
        MissingCadence::Normal
    );
    assert_eq!(
        cadence_for_missing(now - Duration::days(8), now),
        MissingCadence::Daily
    );
    assert_eq!(
        cadence_for_missing(now - Duration::days(30), now),
        MissingCadence::Daily
    );
    assert_eq!(
        cadence_for_missing(now - Duration::days(30) - Duration::seconds(1), now),
        MissingCadence::Daily
    );
    assert_eq!(
        cadence_for_missing_with_reachability(
            now - Duration::days(30) - Duration::seconds(1),
            now,
            Some(true)
        ),
        MissingCadence::Daily
    );
    assert_eq!(
        cadence_for_missing_with_reachability(
            now - Duration::days(30) - Duration::seconds(1),
            now,
            None
        ),
        MissingCadence::Daily
    );
    assert_eq!(
        cadence_for_missing_with_reachability(
            now - Duration::days(30) - Duration::seconds(1),
            now,
            Some(false)
        ),
        MissingCadence::Archived
    );
}

#[test]
fn rejects_private_loopback_link_local_and_metadata_destinations() {
    for ip in [
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
    ] {
        assert_eq!(
            validate_public_target("https", "example", &[ip]),
            Err(TargetSafetyError::NonPublicAddress(ip))
        );
    }
}

#[test]
fn rejects_dns_rebinding_when_any_answer_is_private() {
    let public = IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8));
    let rebound = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 5));
    assert!(matches!(
        validate_public_target("https", "node.example", &[public, rebound]),
        Err(TargetSafetyError::NonPublicAddress(ip)) if ip == rebound
    ));
}

#[test]
fn rejects_ipv4_mapped_ipv6_addresses() {
    // Reject even mapped public IPv4: transports can reinterpret mapped IPv6,
    // so accepting it would make the enforced address family ambiguous.
    for ipv4 in [
        Ipv4Addr::LOCALHOST,
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(169, 254, 169, 254),
        Ipv4Addr::new(192, 0, 2, 1),
        Ipv4Addr::new(8, 8, 8, 8),
    ] {
        let mapped = IpAddr::V6(ipv4.to_ipv6_mapped());
        assert_eq!(
            validate_public_target("https", "node.example", &[mapped]),
            Err(TargetSafetyError::NonPublicAddress(mapped))
        );
    }
}

#[test]
fn rejects_additional_reserved_ipv4_and_ipv6_ranges() {
    for ip in [
        IpAddr::V4(Ipv4Addr::new(192, 0, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(192, 88, 99, 1)),
        IpAddr::V6("400::1".parse::<Ipv6Addr>().unwrap()),
        IpAddr::V6("64:ff9b:1::808:808".parse::<Ipv6Addr>().unwrap()),
        IpAddr::V6("100::1".parse::<Ipv6Addr>().unwrap()),
        IpAddr::V6("2001:2::1".parse::<Ipv6Addr>().unwrap()),
        IpAddr::V6("2002:c000:201::1".parse::<Ipv6Addr>().unwrap()),
        IpAddr::V6("3fff::1".parse::<Ipv6Addr>().unwrap()),
        IpAddr::V6("fec0::1".parse::<Ipv6Addr>().unwrap()),
    ] {
        assert_eq!(
            validate_public_target("https", "node.example", &[ip]),
            Err(TargetSafetyError::NonPublicAddress(ip))
        );
    }
}

#[test]
fn allows_public_ipv4_and_ipv6_for_supported_schemes() {
    let public_addresses = [
        IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
        IpAddr::V6("2606:4700:4700::1111".parse::<Ipv6Addr>().unwrap()),
    ];

    assert_eq!(
        validate_public_target("http", "node.example", &public_addresses),
        Ok(())
    );
    assert_eq!(
        validate_public_target("https", "node.example", &public_addresses),
        Ok(())
    );
}
