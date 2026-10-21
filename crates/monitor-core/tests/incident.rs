use chrono::{DateTime, Duration, TimeZone, Utc};
use monitor_core::{
    incident::{
        EvaluationContext, IncidentEvaluator, IncidentEvaluatorSnapshot, IncidentStateError,
        IncidentSubject, IncidentSuppression, IncidentTransition, NetworkFinalizationSample,
    },
    model::{CheckKind, Dimension, Network, Observation, Outcome, Severity, Site},
};

fn instant(second: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 3, 0, 0, 0).unwrap() + Duration::seconds(second)
}

#[test]
fn connectivity_confirmation_never_mixes_rest_availability_with_peer_tcp() {
    let mut evaluator = IncidentEvaluator::default();
    for second in [0, 30] {
        let _ = evaluator.ingest(
            observation(
                Site::A,
                second,
                Dimension::Connectivity,
                Outcome::Failure("REST unavailable".into()),
            )
            .with_check_kind(CheckKind::Availability),
        );
    }

    let peer_failure = evaluator.ingest(
        observation(
            Site::B,
            60,
            Dimension::Connectivity,
            Outcome::Failure("peer TCP unavailable".into()),
        )
        .with_check_kind(CheckKind::PeerTcp),
    );

    assert!(
        !peer_failure
            .iter()
            .any(|transition| matches!(transition, IncidentTransition::Opened { .. })),
        "different connectivity checks must not confirm each other"
    );
    assert!(evaluator.open_incidents().is_empty());

    let rest_failure = evaluator.ingest(
        observation(
            Site::B,
            61,
            Dimension::Connectivity,
            Outcome::Failure("REST unavailable".into()),
        )
        .with_check_kind(CheckKind::Availability),
    );
    assert!(matches!(
        rest_failure.as_slice(),
        [IncidentTransition::Opened { .. }]
    ));
}

#[test]
fn evaluator_ignores_a_check_kind_attached_to_the_wrong_dimension() {
    let mut evaluator = IncidentEvaluator::default();

    for (site, second) in [(Site::A, 0), (Site::A, 30), (Site::B, 60)] {
        assert!(evaluator
            .ingest(
                observation(
                    site,
                    second,
                    Dimension::Sync,
                    Outcome::Failure("not a chain result".into()),
                )
                .with_check_kind(CheckKind::PeerTcp),
            )
            .is_empty());
    }

    assert!(evaluator.open_incidents().is_empty());
}

#[test]
fn open_incident_projection_survives_snapshot_restore_with_check_kind_schema() {
    let mut evaluator = IncidentEvaluator::default();
    for (site, second) in [(Site::A, 0), (Site::A, 30), (Site::B, 60)] {
        let _ = evaluator.ingest_with_context(
            observation(
                site,
                second,
                Dimension::Connectivity,
                Outcome::Failure("down".into()),
            ),
            symbol_context(),
        );
    }

    let snapshot = evaluator.snapshot();
    assert_eq!(serde_json::to_value(&snapshot).unwrap()["schemaVersion"], 2);
    let restored = IncidentEvaluator::from_snapshot(snapshot).unwrap();
    let open = restored.open_incidents();

    assert_eq!(open.len(), 1);
    assert!(matches!(
        &open[0].subject,
        IncidentSubject::Node {
            node_id,
            network: Some(Network::SymbolMainnet)
        } if node_id == "node-1"
    ));
    assert_eq!(open[0].dimension, Dimension::Connectivity);
    assert_eq!(open[0].check_kind, CheckKind::Availability);
    assert_eq!(open[0].severity, Severity::Critical);
    assert_eq!(open[0].first_observed_at, instant(30));
    assert_eq!(open[0].last_observed_at, instant(60));
}

#[test]
fn node_identity_merge_repoints_candidate_before_snapshot_restore() {
    let mut evaluator = IncidentEvaluator::default();
    for second in [0, 30] {
        let _ = evaluator.ingest_with_context(
            observation_for_node(
                "host-id",
                Site::A,
                second,
                Dimension::Connectivity,
                Outcome::Failure("down".into()),
            ),
            symbol_context(),
        );
    }
    let transitions = evaluator
        .merge_node_identity(
            "host-id",
            "key-id",
            Network::SymbolMainnet,
            instant(45),
            true,
        )
        .expect("non-conflicting identity promotion should repoint incident state");
    assert!(transitions.is_empty());
    let snapshot = evaluator.snapshot();
    let mut restored = IncidentEvaluator::from_snapshot(snapshot).unwrap();
    let opened = restored.ingest_with_context(
        observation_for_node(
            "key-id",
            Site::B,
            60,
            Dimension::Connectivity,
            Outcome::Failure("down".into()),
        ),
        symbol_context(),
    );
    assert!(matches!(
        opened.as_slice(),
        [IncidentTransition::Opened { node_id, .. }] if node_id == "key-id"
    ));
    assert!(
        serde_json::to_value(restored.snapshot()).unwrap()["nodeGates"]
            .as_array()
            .unwrap()
            .iter()
            .all(|gate| gate["nodeId"] != "host-id")
    );
}

#[test]
fn node_identity_merge_combines_existing_healthy_gates() {
    let mut evaluator = IncidentEvaluator::default();
    for (node_id, site, second) in [("host-id", Site::A, 0), ("key-id", Site::B, 10)] {
        let _ = evaluator.ingest_with_context(
            observation_for_node(
                node_id,
                site,
                second,
                Dimension::Connectivity,
                Outcome::Healthy,
            ),
            symbol_context(),
        );
    }

    let transitions = evaluator
        .merge_node_identity(
            "host-id",
            "key-id",
            Network::SymbolMainnet,
            instant(20),
            true,
        )
        .expect("healthy history on both identities should merge");

    assert!(transitions.is_empty());
    let gates = serde_json::to_value(evaluator.snapshot()).unwrap();
    let gates = gates["nodeGates"].as_array().unwrap();
    assert_eq!(gates.len(), 1);
    assert_eq!(gates[0]["nodeId"], "key-id");
}

#[test]
fn node_identity_merge_opens_combined_cross_site_candidates() {
    let mut evaluator = IncidentEvaluator::default();
    for (node_id, site, second) in [
        ("host-id", Site::A, 0),
        ("host-id", Site::A, 30),
        ("key-id", Site::B, 10),
        ("key-id", Site::B, 40),
    ] {
        let _ = evaluator.ingest_with_context(
            observation_for_node(
                node_id,
                site,
                second,
                Dimension::Connectivity,
                Outcome::Failure("down".into()),
            ),
            symbol_context(),
        );
    }

    let transitions = evaluator
        .merge_node_identity(
            "host-id",
            "key-id",
            Network::SymbolMainnet,
            instant(50),
            true,
        )
        .expect("candidate history on both identities should merge");

    assert!(matches!(
        transitions.as_slice(),
        [IncidentTransition::Opened { node_id, at, .. }]
            if node_id == "key-id" && *at == instant(40)
    ));
    assert!(matches!(
        evaluator.open_incidents().as_slice(),
        [projection]
            if matches!(&projection.subject, IncidentSubject::Node { node_id, .. } if node_id == "key-id")
    ));
}

#[test]
fn excluded_node_identity_merge_repoints_candidates_without_opening() {
    let mut evaluator = IncidentEvaluator::default();
    for (node_id, site, second) in [
        ("host-id", Site::A, 0),
        ("host-id", Site::A, 30),
        ("key-id", Site::B, 10),
        ("key-id", Site::B, 40),
    ] {
        let _ = evaluator.ingest_with_context(
            observation_for_node(
                node_id,
                site,
                second,
                Dimension::Connectivity,
                Outcome::Failure("down".into()),
            ),
            symbol_context(),
        );
    }

    let transitions = evaluator
        .merge_node_identity(
            "host-id",
            "key-id",
            Network::SymbolMainnet,
            instant(50),
            false,
        )
        .expect("excluded identity state should still repoint atomically");

    assert!(transitions.is_empty());
    assert!(evaluator.open_incidents().is_empty());
    let opened = evaluator.ingest_with_context(
        observation_for_node(
            "key-id",
            Site::A,
            60,
            Dimension::Connectivity,
            Outcome::Failure("down".into()),
        ),
        symbol_context(),
    );
    assert!(matches!(
        opened.as_slice(),
        [IncidentTransition::Opened { .. }]
    ));
}

#[test]
fn node_identity_merge_keeps_one_survivor_when_both_gates_are_open() {
    let mut evaluator = IncidentEvaluator::default();
    let mut target_incident = None;
    for (node_id, offset) in [("host-id", 0), ("key-id", 5)] {
        for (site, second) in [
            (Site::A, offset),
            (Site::A, offset + 30),
            (Site::B, offset + 60),
        ] {
            for transition in evaluator.ingest_with_context(
                observation_for_node(
                    node_id,
                    site,
                    second,
                    Dimension::Connectivity,
                    Outcome::Failure("down".into()),
                ),
                symbol_context(),
            ) {
                if node_id == "key-id" {
                    if let IncidentTransition::Opened { incident_id, .. } = transition {
                        target_incident = Some(incident_id);
                    }
                }
            }
        }
    }
    let target_incident = target_incident.expect("target gate should open");

    let transitions = evaluator
        .merge_node_identity(
            "host-id",
            "key-id",
            Network::SymbolMainnet,
            instant(100),
            true,
        )
        .expect("two open identities should reconcile deterministically");

    assert!(matches!(
        transitions.as_slice(),
        [IncidentTransition::Suppressed {
            reason: IncidentSuppression::IdentityMerge,
            ..
        }]
    ));
    let open = evaluator.open_incidents();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].incident_id, target_incident);
    assert!(matches!(
        &open[0].subject,
        IncidentSubject::Node { node_id, .. } if node_id == "key-id"
    ));
}

fn observation(site: Site, second: i64, dimension: Dimension, outcome: Outcome) -> Observation {
    observation_for_node("node-1", site, second, dimension, outcome)
}

fn observation_for_node(
    node_id: &str,
    site: Site,
    second: i64,
    dimension: Dimension,
    outcome: Outcome,
) -> Observation {
    Observation::new(
        format!("job-{second}-{site:?}"),
        node_id,
        site,
        dimension,
        outcome,
        instant(second),
    )
}

fn normal_context() -> EvaluationContext {
    EvaluationContext::default()
}

fn symbol_context() -> EvaluationContext {
    EvaluationContext {
        network: Some(Network::SymbolMainnet),
        ..EvaluationContext::default()
    }
}

#[test]
fn first_failure_schedules_retry_then_two_sites_confirm_at_exact_boundaries() {
    let mut evaluator = IncidentEvaluator::default();
    let first = evaluator.ingest(observation(
        Site::A,
        0,
        Dimension::Connectivity,
        Outcome::Failure("timeout".into()),
    ));
    assert!(matches!(
        first.as_slice(),
        [IncidentTransition::RetryScheduled {
            site: Site::A,
            not_before,
            not_after,
            ..
        }] if *not_before == instant(30) && *not_after == instant(60)
    ));

    assert!(evaluator
        .ingest(observation(
            Site::A,
            29,
            Dimension::Connectivity,
            Outcome::Failure("timeout".into()),
        ))
        .is_empty());
    assert!(matches!(
        evaluator
            .ingest(observation(
                Site::A,
                30,
                Dimension::Connectivity,
                Outcome::Failure("timeout".into()),
            ))
            .as_slice(),
        [IncidentTransition::CandidateRecorded { site: Site::A, .. }]
    ));
    assert!(matches!(
        evaluator
            .ingest(observation(
                Site::B,
                750,
                Dimension::Connectivity,
                Outcome::Failure("timeout".into()),
            ))
            .as_slice(),
        [IncidentTransition::Opened {
            severity: Severity::Critical,
            ..
        }]
    ));

    let mut outside = IncidentEvaluator::default();
    let _ = outside.ingest(observation(
        Site::A,
        0,
        Dimension::Connectivity,
        Outcome::Failure("timeout".into()),
    ));
    let _ = outside.ingest(observation(
        Site::A,
        30,
        Dimension::Connectivity,
        Outcome::Failure("timeout".into()),
    ));
    let too_late = outside.ingest(observation(
        Site::B,
        751,
        Dimension::Connectivity,
        Outcome::Failure("timeout".into()),
    ));
    assert!(matches!(
        too_late.as_slice(),
        [IncidentTransition::RetryScheduled { site: Site::B, .. }]
    ));
}

#[test]
fn all_dimensions_need_candidates_cross_site_confirmation_and_two_site_recovery() {
    for (dimension, outcome) in [
        (
            Dimension::Connectivity,
            Outcome::Failure("connection failed".into()),
        ),
        (Dimension::Latency, Outcome::LatencyMillis(2_000)),
        (Dimension::Sync, Outcome::LagBlocks(5)),
        (
            Dimension::Finalization,
            Outcome::FinalizationStalledMinutes(30),
        ),
        (Dimension::Peers, Outcome::ZeroPeers),
        (Dimension::Certificate, Outcome::Remaining(30)),
        (Dimension::Voting, Outcome::Remaining(30)),
        (
            Dimension::Harvest,
            Outcome::Failure("harvest unavailable".into()),
        ),
        (
            Dimension::Supernode,
            Outcome::Failure("supernode failed".into()),
        ),
        (Dimension::StateHash, Outcome::Mismatch("hash".into())),
    ] {
        let mut evaluator = IncidentEvaluator::default();
        assert!(matches!(
            evaluator
                .ingest(observation(Site::A, 0, dimension, outcome.clone()))
                .as_slice(),
            [IncidentTransition::RetryScheduled { .. }]
        ));
        assert!(
            !evaluator
                .ingest(observation(Site::B, 10, dimension, outcome.clone()))
                .iter()
                .any(|transition| matches!(transition, IncidentTransition::Opened { .. })),
            "{dimension:?} must not open from one observation per site"
        );
        let _ = evaluator.ingest(observation(Site::A, 30, dimension, outcome.clone()));
        assert!(matches!(
            evaluator
                .ingest(observation(Site::B, 40, dimension, outcome))
                .as_slice(),
            [IncidentTransition::Opened { .. }]
        ));
        assert!(matches!(
            evaluator
                .ingest(observation(Site::A, 50, dimension, Outcome::Healthy))
                .as_slice(),
            [IncidentTransition::RecoveryCheckScheduled { site: Site::B, .. }]
        ));
        assert!(matches!(
            evaluator
                .ingest(observation(Site::B, 60, dimension, Outcome::Healthy))
                .as_slice(),
            [IncidentTransition::Resolved { .. }]
        ));
    }
}

#[test]
fn every_dimension_uses_the_frozen_threshold_and_invalid_combinations_are_unknown() {
    let severity = IncidentEvaluator::severity_for;
    assert_eq!(
        severity(Dimension::Connectivity, &Outcome::Failure("down".into())),
        Some(Severity::Critical)
    );
    assert_eq!(
        severity(Dimension::Latency, &Outcome::LatencyMillis(1_999)),
        None
    );
    assert_eq!(
        severity(Dimension::Latency, &Outcome::LatencyMillis(2_000)),
        Some(Severity::Warning)
    );
    assert_eq!(severity(Dimension::Sync, &Outcome::LagBlocks(4)), None);
    assert_eq!(
        severity(Dimension::Sync, &Outcome::LagBlocks(5)),
        Some(Severity::Warning)
    );
    assert_eq!(
        severity(Dimension::Sync, &Outcome::LagBlocks(20)),
        Some(Severity::Critical)
    );
    assert_eq!(
        severity(
            Dimension::Finalization,
            &Outcome::FinalizationStalledMinutes(29)
        ),
        None
    );
    assert_eq!(
        severity(
            Dimension::Finalization,
            &Outcome::FinalizationStalledMinutes(30)
        ),
        Some(Severity::Warning)
    );
    assert_eq!(
        severity(
            Dimension::Finalization,
            &Outcome::FinalizationStalledMinutes(60)
        ),
        Some(Severity::Critical)
    );
    assert_eq!(
        severity(Dimension::Peers, &Outcome::ZeroPeers),
        Some(Severity::Critical)
    );
    assert_eq!(
        severity(Dimension::Certificate, &Outcome::Remaining(31)),
        None
    );
    assert_eq!(
        severity(Dimension::Certificate, &Outcome::Remaining(30)),
        Some(Severity::Warning)
    );
    assert_eq!(
        severity(Dimension::Certificate, &Outcome::Remaining(1)),
        Some(Severity::Warning)
    );
    assert_eq!(
        severity(Dimension::Certificate, &Outcome::Remaining(0)),
        Some(Severity::Critical)
    );
    assert_eq!(
        severity(Dimension::Certificate, &Outcome::Remaining(-1)),
        Some(Severity::Critical)
    );
    assert_eq!(severity(Dimension::Voting, &Outcome::Remaining(31)), None);
    assert_eq!(
        severity(Dimension::Voting, &Outcome::Remaining(30)),
        Some(Severity::Warning)
    );
    assert_eq!(
        severity(Dimension::Voting, &Outcome::Remaining(7)),
        Some(Severity::Critical)
    );
    assert_eq!(
        severity(
            Dimension::Harvest,
            &Outcome::Failure("harvest unavailable".into())
        ),
        Some(Severity::Critical)
    );
    assert_eq!(
        severity(
            Dimension::Supernode,
            &Outcome::Failure("not qualified".into())
        ),
        Some(Severity::Critical)
    );
    assert_eq!(
        severity(Dimension::StateHash, &Outcome::Mismatch("different".into())),
        Some(Severity::Critical)
    );
    assert_eq!(
        severity(Dimension::Connectivity, &Outcome::LagBlocks(100)),
        None
    );
    assert_eq!(severity(Dimension::Peers, &Outcome::Remaining(-1)), None);
}

#[test]
fn open_certificate_incident_refreshes_remaining_days_and_clamps_expiry_at_zero() {
    let mut evaluator = IncidentEvaluator::default();
    let _ = evaluator.ingest(observation(
        Site::A,
        0,
        Dimension::Certificate,
        Outcome::Remaining(30),
    ));
    let _ = evaluator.ingest(observation(
        Site::A,
        30,
        Dimension::Certificate,
        Outcome::Remaining(30),
    ));
    assert!(matches!(
        evaluator
            .ingest(observation(
                Site::B,
                60,
                Dimension::Certificate,
                Outcome::Remaining(30),
            ))
            .as_slice(),
        [IncidentTransition::Opened {
            severity: Severity::Warning,
            ..
        }]
    ));

    let refreshed = evaluator.ingest(observation(
        Site::A,
        90,
        Dimension::Certificate,
        Outcome::Remaining(29),
    ));
    let refreshed = serde_json::to_value(refreshed).unwrap();
    assert_eq!(refreshed[0]["transition"], "evidence_updated");
    assert_eq!(refreshed[0]["evidence_summary"], "29 remaining");

    assert!(matches!(
        evaluator
            .ingest(observation(
                Site::A,
                120,
                Dimension::Certificate,
                Outcome::Remaining(0),
            ))
            .as_slice(),
        [IncidentTransition::CandidateRecorded {
            severity: Severity::Critical,
            ..
        }]
    ));
    let expired = evaluator.ingest(observation(
        Site::B,
        150,
        Dimension::Certificate,
        Outcome::Remaining(0),
    ));
    assert!(matches!(
        expired.as_slice(),
        [IncidentTransition::Updated {
            severity: Severity::Critical,
            ..
        }]
    ));
    let expired = serde_json::to_value(expired).unwrap();
    assert_eq!(expired[0]["evidence_summary"], "0 remaining");

    let clamped = evaluator.ingest(observation(
        Site::A,
        180,
        Dimension::Certificate,
        Outcome::Remaining(-1),
    ));
    let clamped = serde_json::to_value(clamped).unwrap();
    assert_eq!(clamped[0]["transition"], "evidence_updated");
    assert_eq!(clamped[0]["evidence_summary"], "0 remaining");
}

#[test]
fn open_finalization_incident_refreshes_the_gap_from_current_height() {
    let finalization = |site, second, height, finalized_height| {
        observation(
            site,
            second,
            Dimension::Finalization,
            Outcome::FinalizationStalledMinutes(30),
        )
        .with_evidence(serde_json::json!({
            "height": height,
            "finalizedHeight": finalized_height
        }))
    };
    let mut evaluator = IncidentEvaluator::default();
    let _ = evaluator.ingest(finalization(Site::A, 0, 1_000, 900));
    let _ = evaluator.ingest(finalization(Site::A, 30, 1_000, 900));
    let opened = evaluator.ingest(finalization(Site::B, 60, 1_000, 900));
    assert!(matches!(
        opened.as_slice(),
        [IncidentTransition::Opened {
            severity: Severity::Warning,
            evidence_summary,
            ..
        }] if evidence_summary == "site A: 100 blocks behind current height; site B: 100 blocks behind current height"
    ));

    let refreshed = evaluator.ingest(finalization(Site::A, 90, 1_100, 900));
    assert!(matches!(
        refreshed.as_slice(),
        [IncidentTransition::EvidenceUpdated {
            evidence_summary,
            ..
        }] if evidence_summary == "200 blocks behind current height"
    ));
}

#[test]
fn finalization_summary_falls_back_to_stalled_time_for_invalid_height_evidence() {
    let mut evaluator = IncidentEvaluator::default();
    let invalid = observation(
        Site::A,
        0,
        Dimension::Finalization,
        Outcome::FinalizationStalledMinutes(30),
    )
    .with_evidence(serde_json::json!({
        "height": 900,
        "finalizedHeight": 1_000
    }));
    let _ = evaluator.ingest(invalid.clone());
    let _ = evaluator.ingest(Observation {
        job_id: "invalid-candidate".into(),
        observed_at: instant(30),
        ..invalid.clone()
    });
    let opened = evaluator.ingest(Observation {
        job_id: "invalid-confirm".into(),
        site: Site::B,
        observed_at: instant(60),
        ..invalid
    });
    assert!(matches!(
        opened.as_slice(),
        [IncidentTransition::Opened {
            evidence_summary,
            ..
        }] if evidence_summary == "site A: finalization stalled for 30 minutes; site B: finalization stalled for 30 minutes"
    ));
}

#[test]
fn recovery_needs_two_healthy_sites_within_twelve_minutes_and_clears_candidates() {
    let mut evaluator = IncidentEvaluator::default();
    let _ = evaluator.ingest(observation(
        Site::A,
        0,
        Dimension::Connectivity,
        Outcome::Failure("down".into()),
    ));
    let _ = evaluator.ingest(observation(
        Site::A,
        30,
        Dimension::Connectivity,
        Outcome::Failure("down".into()),
    ));
    let opened = evaluator.ingest(observation(
        Site::B,
        60,
        Dimension::Connectivity,
        Outcome::Failure("down".into()),
    ));
    assert!(matches!(
        opened.as_slice(),
        [IncidentTransition::Opened { .. }]
    ));

    assert!(matches!(
        evaluator
            .ingest(observation(
                Site::A,
                600,
                Dimension::Connectivity,
                Outcome::Healthy,
            ))
            .as_slice(),
        [IncidentTransition::RecoveryCheckScheduled {
            site: Site::B,
            not_before,
            not_after,
            ..
        }] if *not_before == instant(600) && *not_after == instant(1_320)
    ));
    assert!(matches!(
        evaluator
            .ingest(observation(
                Site::B,
                1_321,
                Dimension::Connectivity,
                Outcome::Healthy,
            ))
            .as_slice(),
        [IncidentTransition::RecoveryCheckScheduled {
            site: Site::A,
            not_before,
            not_after,
            ..
        }] if *not_before == instant(1_321) && *not_after == instant(2_041)
    ));
    assert!(matches!(
        evaluator
            .ingest(observation(
                Site::A,
                1_350,
                Dimension::Connectivity,
                Outcome::Healthy,
            ))
            .as_slice(),
        [IncidentTransition::Resolved { .. }]
    ));

    let mut candidate = IncidentEvaluator::default();
    let _ = candidate.ingest(observation(
        Site::A,
        0,
        Dimension::Connectivity,
        Outcome::Failure("down".into()),
    ));
    let _ = candidate.ingest(observation(
        Site::A,
        30,
        Dimension::Connectivity,
        Outcome::Failure("down".into()),
    ));
    let _ = candidate.ingest(observation(
        Site::A,
        40,
        Dimension::Connectivity,
        Outcome::Healthy,
    ));
    assert!(matches!(
        candidate
            .ingest(observation(
                Site::B,
                50,
                Dimension::Connectivity,
                Outcome::Failure("down".into()),
            ))
            .as_slice(),
        [IncidentTransition::RetryScheduled { site: Site::B, .. }]
    ));
}

#[test]
fn out_of_order_cross_site_healthy_observations_resolve_within_the_recovery_window() {
    let mut evaluator = IncidentEvaluator::default();
    for (site, second) in [(Site::A, 0), (Site::A, 30), (Site::B, 60)] {
        let _ = evaluator.ingest(observation(
            site,
            second,
            Dimension::Sync,
            Outcome::LagBlocks(25),
        ));
    }
    assert_eq!(evaluator.open_incidents().len(), 1);

    assert!(matches!(
        evaluator
            .ingest(observation(
                Site::B,
                1_000,
                Dimension::Sync,
                Outcome::Healthy,
            ))
            .as_slice(),
        [IncidentTransition::RecoveryCheckScheduled { site: Site::A, .. }]
    ));
    let recovered = evaluator.ingest(observation(Site::A, 700, Dimension::Sync, Outcome::Healthy));

    assert!(matches!(
        recovered.as_slice(),
        [IncidentTransition::Resolved { at, .. }] if *at == instant(1_000)
    ));
    assert!(evaluator.open_incidents().is_empty());
    assert!(IncidentEvaluator::from_snapshot(evaluator.snapshot()).is_ok());
}

#[test]
fn unknown_unsupported_source_anomaly_and_insufficient_reference_never_mutate_public_state() {
    let mut evaluator = IncidentEvaluator::default();
    for outcome in [
        Outcome::Unknown("reference unavailable".into()),
        Outcome::Unsupported("REST is not exposed".into()),
    ] {
        assert!(evaluator
            .ingest(observation(Site::A, 0, Dimension::Sync, outcome))
            .is_empty());
    }
    let suppressed = observation(Site::A, 10, Dimension::Sync, Outcome::LagBlocks(25));
    assert!(evaluator
        .ingest_with_context(
            suppressed.clone(),
            EvaluationContext {
                source_anomaly: true,
                ..normal_context()
            },
        )
        .is_empty());
    assert!(evaluator
        .ingest_with_context(
            suppressed,
            EvaluationContext {
                reference_quorum_usable: false,
                ..normal_context()
            },
        )
        .is_empty());

    assert!(matches!(
        evaluator
            .ingest(observation(
                Site::A,
                20,
                Dimension::Sync,
                Outcome::LagBlocks(25),
            ))
            .as_slice(),
        [IncidentTransition::RetryScheduled { .. }]
    ));
    assert_eq!(
        serde_json::to_value(Outcome::Unsupported("not supported".into())).unwrap(),
        serde_json::json!({"kind": "unsupported", "value": "not supported"})
    );

    for suppressed in [
        (
            Outcome::Unknown("reference unavailable".into()),
            normal_context(),
        ),
        (
            Outcome::LagBlocks(25),
            EvaluationContext {
                source_anomaly: true,
                ..normal_context()
            },
        ),
        (
            Outcome::LagBlocks(25),
            EvaluationContext {
                reference_quorum_usable: false,
                ..normal_context()
            },
        ),
    ] {
        let mut watermarked = IncidentEvaluator::default();
        assert!(watermarked
            .ingest_with_context(
                observation(Site::A, 100, Dimension::Sync, suppressed.0),
                suppressed.1,
            )
            .is_empty());
        assert!(watermarked
            .ingest(observation(
                Site::A,
                90,
                Dimension::Sync,
                Outcome::LagBlocks(25),
            ))
            .is_empty());
    }
}

#[test]
fn out_of_order_cross_site_failure_cannot_confirm_a_newer_candidate() {
    let mut evaluator = IncidentEvaluator::default();
    let _ = evaluator.ingest(observation(
        Site::A,
        60,
        Dimension::Connectivity,
        Outcome::Failure("down".into()),
    ));
    let _ = evaluator.ingest(observation(
        Site::A,
        90,
        Dimension::Connectivity,
        Outcome::Failure("down".into()),
    ));
    let older = evaluator.ingest(observation(
        Site::B,
        30,
        Dimension::Connectivity,
        Outcome::Failure("down".into()),
    ));
    assert!(!older
        .iter()
        .any(|transition| matches!(transition, IncidentTransition::Opened { .. })));
}

#[test]
fn severity_escalates_after_confirmation_without_reopening() {
    let mut evaluator = IncidentEvaluator::default();
    let _ = evaluator.ingest(observation(
        Site::A,
        0,
        Dimension::Sync,
        Outcome::LagBlocks(6),
    ));
    let _ = evaluator.ingest(observation(
        Site::A,
        30,
        Dimension::Sync,
        Outcome::LagBlocks(6),
    ));
    assert!(matches!(
        evaluator
            .ingest(observation(
                Site::B,
                60,
                Dimension::Sync,
                Outcome::LagBlocks(6),
            ))
            .as_slice(),
        [IncidentTransition::Opened {
            severity: Severity::Warning,
            ..
        }]
    ));
    assert!(matches!(
        evaluator
            .ingest(observation(
                Site::A,
                90,
                Dimension::Sync,
                Outcome::LagBlocks(25),
            ))
            .as_slice(),
        [IncidentTransition::CandidateRecorded {
            site: Site::A,
            severity: Severity::Critical,
            ..
        }]
    ));
    assert!(matches!(
        evaluator
            .ingest(observation(
                Site::B,
                120,
                Dimension::Sync,
                Outcome::LagBlocks(25),
            ))
            .as_slice(),
        [IncidentTransition::Updated {
            severity: Severity::Critical,
            ..
        }]
    ));
}

#[test]
fn one_node_cannot_change_network_between_dimensions() {
    let mut evaluator = IncidentEvaluator::default();
    assert!(matches!(
        evaluator
            .ingest_with_context(
                observation(
                    Site::A,
                    0,
                    Dimension::Connectivity,
                    Outcome::Failure("down".into()),
                ),
                symbol_context(),
            )
            .as_slice(),
        [IncidentTransition::RetryScheduled { .. }]
    ));
    assert!(evaluator
        .ingest_with_context(
            observation(Site::A, 1, Dimension::Sync, Outcome::LagBlocks(25)),
            EvaluationContext {
                network: Some(Network::Nis1Mainnet),
                ..normal_context()
            },
        )
        .is_empty());
    assert!(matches!(
        evaluator
            .ingest_with_context(
                observation(Site::A, 2, Dimension::Sync, Outcome::LagBlocks(25)),
                symbol_context(),
            )
            .as_slice(),
        [IncidentTransition::RetryScheduled { .. }]
    ));
}

#[test]
fn network_majority_finalization_opens_one_network_incident_and_suppresses_nodes() {
    let mut evaluator = IncidentEvaluator::default();

    let _ = evaluator.ingest_with_context(
        observation(
            Site::A,
            0,
            Dimension::Finalization,
            Outcome::FinalizationStalledMinutes(60),
        ),
        symbol_context(),
    );
    let _ = evaluator.ingest_with_context(
        observation(
            Site::A,
            30,
            Dimension::Finalization,
            Outcome::FinalizationStalledMinutes(60),
        ),
        symbol_context(),
    );
    assert!(matches!(
        evaluator
            .ingest_with_context(
                observation(
                    Site::B,
                    60,
                    Dimension::Finalization,
                    Outcome::FinalizationStalledMinutes(60),
                ),
                symbol_context(),
            )
            .as_slice(),
        [IncidentTransition::Opened { .. }]
    ));

    let sample = |site, second| NetworkFinalizationSample {
        network: Network::SymbolMainnet,
        site,
        expected_references: 5,
        responding_references: 4,
        stalled_references: 3,
        stalled_minutes: 60,
        observed_at: instant(second),
    };
    assert!(matches!(
        evaluator
            .ingest_network_finalization(sample(Site::A, 120))
            .as_slice(),
        [IncidentTransition::NetworkRetryScheduled { .. }]
    ));
    assert!(matches!(
        evaluator
            .ingest_network_finalization(sample(Site::A, 150))
            .as_slice(),
        [IncidentTransition::NetworkCandidateRecorded { .. }]
    ));
    let opened = evaluator.ingest_network_finalization(sample(Site::B, 180));
    assert!(matches!(
        opened.as_slice(),
        [
            IncidentTransition::NetworkOpened {
                network: Network::SymbolMainnet,
                ..
            },
            IncidentTransition::Suppressed {
                reason: IncidentSuppression::NetworkAggregate,
                ..
            }
        ]
    ));

    assert!(evaluator
        .ingest_with_context(
            observation(
                Site::A,
                210,
                Dimension::Finalization,
                Outcome::FinalizationStalledMinutes(60),
            ),
            symbol_context(),
        )
        .is_empty());

    let healthy = |site, second| NetworkFinalizationSample {
        network: Network::SymbolMainnet,
        site,
        expected_references: 5,
        responding_references: 4,
        stalled_references: 0,
        stalled_minutes: 0,
        observed_at: instant(second),
    };
    assert!(matches!(
        evaluator
            .ingest_network_finalization(healthy(Site::A, 240))
            .as_slice(),
        [IncidentTransition::NetworkRecoveryCheckScheduled { site: Site::B, .. }]
    ));
    assert!(matches!(
        evaluator
            .ingest_network_finalization(healthy(Site::B, 270))
            .as_slice(),
        [IncidentTransition::Resolved { .. }]
    ));
    assert!(evaluator
        .ingest_with_context(
            observation(
                Site::A,
                200,
                Dimension::Finalization,
                Outcome::FinalizationStalledMinutes(60),
            ),
            symbol_context(),
        )
        .is_empty());
    assert!(matches!(
        evaluator
            .ingest_with_context(
                observation(
                    Site::A,
                    300,
                    Dimension::Finalization,
                    Outcome::FinalizationStalledMinutes(60),
                ),
                symbol_context(),
            )
            .as_slice(),
        [IncidentTransition::RetryScheduled { .. }]
    ));
}

#[test]
fn older_network_aggregate_never_suppresses_a_newer_node_incident() {
    let mut evaluator = IncidentEvaluator::default();
    for (site, second) in [(Site::A, 1_000), (Site::A, 1_030), (Site::B, 1_060)] {
        let _ = evaluator.ingest_with_context(
            observation(
                site,
                second,
                Dimension::Finalization,
                Outcome::FinalizationStalledMinutes(60),
            ),
            symbol_context(),
        );
    }

    let sample = |site, second| NetworkFinalizationSample {
        network: Network::SymbolMainnet,
        site,
        expected_references: 5,
        responding_references: 4,
        stalled_references: 3,
        stalled_minutes: 60,
        observed_at: instant(second),
    };
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 800));
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 830));
    let opened = evaluator.ingest_network_finalization(sample(Site::B, 850));
    assert!(!opened
        .iter()
        .any(|transition| matches!(transition, IncidentTransition::Suppressed { .. })));
    let snapshot =
        serde_json::from_value(serde_json::to_value(evaluator.snapshot()).unwrap()).unwrap();
    assert!(IncidentEvaluator::from_snapshot(snapshot).is_ok());
}

#[test]
fn network_recovery_watermarks_every_suppressed_node_gate() {
    let mut evaluator = IncidentEvaluator::default();
    for (site, second) in [(Site::A, 0), (Site::A, 30), (Site::B, 60)] {
        let _ = evaluator.ingest_with_context(
            observation(
                site,
                second,
                Dimension::Finalization,
                Outcome::FinalizationStalledMinutes(60),
            ),
            symbol_context(),
        );
    }
    let sample = |site, second, stalled_references, stalled_minutes| NetworkFinalizationSample {
        network: Network::SymbolMainnet,
        site,
        expected_references: 5,
        responding_references: 4,
        stalled_references,
        stalled_minutes,
        observed_at: instant(second),
    };
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 120, 3, 60));
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 150, 3, 60));
    let _ = evaluator.ingest_network_finalization(sample(Site::B, 180, 3, 60));
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 240, 0, 0));
    let _ = evaluator.ingest_network_finalization(sample(Site::B, 270, 0, 0));

    for (site, second) in [(Site::A, 220), (Site::A, 250), (Site::B, 260)] {
        assert!(evaluator
            .ingest_with_context(
                observation(
                    site,
                    second,
                    Dimension::Finalization,
                    Outcome::FinalizationStalledMinutes(60),
                ),
                symbol_context(),
            )
            .is_empty());
    }
}

#[test]
fn network_recovery_watermark_survives_restart_for_nodes_not_yet_seen() {
    let mut evaluator = IncidentEvaluator::default();
    let sample = |site, second, stalled_references, stalled_minutes| NetworkFinalizationSample {
        network: Network::SymbolMainnet,
        site,
        expected_references: 5,
        responding_references: 4,
        stalled_references,
        stalled_minutes,
        observed_at: instant(second),
    };
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 120, 3, 60));
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 150, 3, 60));
    let _ = evaluator.ingest_network_finalization(sample(Site::B, 180, 3, 60));
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 240, 0, 0));
    let _ = evaluator.ingest_network_finalization(sample(Site::B, 270, 0, 0));

    let snapshot = serde_json::to_value(evaluator.snapshot()).unwrap();
    assert_eq!(
        snapshot["networkFinalization"][0]["suppressedThrough"],
        serde_json::to_value(instant(270)).unwrap()
    );
    let snapshot: IncidentEvaluatorSnapshot = serde_json::from_value(snapshot).unwrap();
    let mut restarted = IncidentEvaluator::from_snapshot(snapshot).unwrap();
    for (site, second) in [(Site::A, 220), (Site::A, 250), (Site::B, 260)] {
        assert!(restarted
            .ingest_with_context(
                observation(
                    site,
                    second,
                    Dimension::Finalization,
                    Outcome::FinalizationStalledMinutes(60),
                ),
                symbol_context(),
            )
            .is_empty());
    }
    assert!(matches!(
        restarted
            .ingest_with_context(
                observation(
                    Site::A,
                    300,
                    Dimension::Finalization,
                    Outcome::FinalizationStalledMinutes(60),
                ),
                symbol_context(),
            )
            .as_slice(),
        [IncidentTransition::RetryScheduled { .. }]
    ));
}

#[test]
fn network_severity_escalation_also_requires_two_sites() {
    let mut evaluator = IncidentEvaluator::default();
    let sample = |site, second, stalled_minutes| NetworkFinalizationSample {
        network: Network::SymbolMainnet,
        site,
        expected_references: 5,
        responding_references: 4,
        stalled_references: 3,
        stalled_minutes,
        observed_at: instant(second),
    };
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 0, 30));
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 30, 30));
    assert!(matches!(
        evaluator
            .ingest_network_finalization(sample(Site::B, 60, 30))
            .as_slice(),
        [IncidentTransition::NetworkOpened {
            severity: Severity::Warning,
            ..
        }]
    ));
    assert!(matches!(
        evaluator
            .ingest_network_finalization(sample(Site::A, 90, 60))
            .as_slice(),
        [IncidentTransition::NetworkCandidateRecorded {
            site: Site::A,
            severity: Severity::Critical,
            ..
        }]
    ));
    assert!(matches!(
        evaluator
            .ingest_network_finalization(sample(Site::B, 120, 60))
            .as_slice(),
        [IncidentTransition::Updated {
            severity: Severity::Critical,
            ..
        }]
    ));
}

#[test]
fn network_finalization_requires_reference_quorum_and_two_site_recovery() {
    let mut evaluator = IncidentEvaluator::default();
    let insufficient = NetworkFinalizationSample {
        network: Network::SymbolMainnet,
        site: Site::A,
        expected_references: 5,
        responding_references: 2,
        stalled_references: 2,
        stalled_minutes: 60,
        observed_at: instant(0),
    };
    assert!(evaluator
        .ingest_network_finalization(insufficient)
        .is_empty());
    let newer_insufficient = NetworkFinalizationSample {
        observed_at: instant(20),
        ..insufficient
    };
    assert!(evaluator
        .ingest_network_finalization(newer_insufficient)
        .is_empty());

    let sample = |site, second, stalled_references, stalled_minutes| NetworkFinalizationSample {
        network: Network::SymbolMainnet,
        site,
        expected_references: 5,
        responding_references: 4,
        stalled_references,
        stalled_minutes,
        observed_at: instant(second),
    };
    assert!(evaluator
        .ingest_network_finalization(sample(Site::A, 10, 3, 60))
        .is_empty());
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 30, 3, 60));
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 60, 3, 60));
    assert!(matches!(
        evaluator
            .ingest_network_finalization(sample(Site::B, 90, 3, 60))
            .as_slice(),
        [IncidentTransition::NetworkOpened { .. }]
    ));
    assert!(matches!(
        evaluator
            .ingest_network_finalization(sample(Site::A, 120, 0, 0))
            .as_slice(),
        [IncidentTransition::NetworkRecoveryCheckScheduled { site: Site::B, .. }]
    ));
    assert!(matches!(
        evaluator
            .ingest_network_finalization(sample(Site::B, 150, 0, 0))
            .as_slice(),
        [IncidentTransition::Resolved { .. }]
    ));
}

#[test]
fn versioned_snapshot_restores_candidate_and_open_incident_identity() {
    let mut candidate = IncidentEvaluator::default();
    let _ = candidate.ingest(observation(
        Site::A,
        0,
        Dimension::Connectivity,
        Outcome::Failure("down".into()),
    ));
    let _ = candidate.ingest(observation(
        Site::A,
        30,
        Dimension::Connectivity,
        Outcome::Failure("down".into()),
    ));
    let serialized = serde_json::to_value(candidate.snapshot()).unwrap();
    assert_eq!(serialized["schemaVersion"], 2);
    let snapshot: IncidentEvaluatorSnapshot = serde_json::from_value(serialized).unwrap();
    let mut restored = IncidentEvaluator::from_snapshot(snapshot).unwrap();
    let opened = restored.ingest(observation(
        Site::B,
        60,
        Dimension::Connectivity,
        Outcome::Failure("down".into()),
    ));
    let incident_id = match opened.as_slice() {
        [IncidentTransition::Opened { incident_id, .. }] => *incident_id,
        other => panic!("restored candidate did not open: {other:?}"),
    };

    let snapshot =
        serde_json::from_value(serde_json::to_value(restored.snapshot()).unwrap()).unwrap();
    let mut restarted = IncidentEvaluator::from_snapshot(snapshot).unwrap();
    assert!(matches!(
        restarted
            .ingest(observation(
                Site::A,
                90,
                Dimension::Connectivity,
                Outcome::Healthy,
            ))
            .as_slice(),
        [IncidentTransition::RecoveryCheckScheduled { site: Site::B, .. }]
    ));
    assert!(matches!(
        restarted
            .ingest(observation(
                Site::B,
                120,
                Dimension::Connectivity,
                Outcome::Healthy,
            ))
            .as_slice(),
        [IncidentTransition::Resolved {
            incident_id: resolved_id,
            ..
        }] if *resolved_id == incident_id
    ));
}

#[test]
fn schema_v1_snapshot_without_check_kind_remains_readable() {
    let mut evaluator = IncidentEvaluator::default();
    let _ = evaluator.ingest(observation(
        Site::A,
        0,
        Dimension::Sync,
        Outcome::LagBlocks(20),
    ));
    let mut serialized = serde_json::to_value(evaluator.snapshot()).unwrap();
    serialized["schemaVersion"] = serde_json::json!(1);
    for gate in serialized["nodeGates"].as_array_mut().unwrap() {
        gate.as_object_mut().unwrap().remove("checkKind");
    }

    let snapshot: IncidentEvaluatorSnapshot = serde_json::from_value(serialized).unwrap();
    let restored = IncidentEvaluator::from_snapshot(snapshot).unwrap();

    assert_eq!(
        serde_json::to_value(restored.snapshot()).unwrap()["nodeGates"][0]["checkKind"],
        "chain"
    );
}

#[test]
fn snapshot_rejects_unknown_schema_and_duplicate_gate_rows() {
    let evaluator = IncidentEvaluator::default();
    let mut unknown = serde_json::to_value(evaluator.snapshot()).unwrap();
    unknown["schemaVersion"] = serde_json::json!(3);
    let unknown: IncidentEvaluatorSnapshot = serde_json::from_value(unknown).unwrap();
    assert!(matches!(
        IncidentEvaluator::from_snapshot(unknown),
        Err(IncidentStateError::UnsupportedSchema(3))
    ));

    let mut evaluator = IncidentEvaluator::default();
    let _ = evaluator.ingest(observation(
        Site::A,
        0,
        Dimension::Connectivity,
        Outcome::Failure("down".into()),
    ));
    let mut duplicate = serde_json::to_value(evaluator.snapshot()).unwrap();
    let gate = duplicate["nodeGates"][0].clone();
    duplicate["nodeGates"].as_array_mut().unwrap().push(gate);
    let duplicate: IncidentEvaluatorSnapshot = serde_json::from_value(duplicate).unwrap();
    assert!(matches!(
        IncidentEvaluator::from_snapshot(duplicate),
        Err(IncidentStateError::DuplicateGate)
    ));
}

#[test]
fn snapshot_rejects_duplicate_incident_ids_and_conflicting_network_state() {
    let mut duplicate_ids = IncidentEvaluator::default();
    for (node_id, dimension, outcome) in [
        (
            "node-1",
            Dimension::Connectivity,
            Outcome::Failure("down".into()),
        ),
        ("node-2", Dimension::Sync, Outcome::LagBlocks(25)),
    ] {
        for (site, second) in [(Site::A, 0), (Site::A, 30), (Site::B, 60)] {
            let _ = duplicate_ids.ingest(observation_for_node(
                node_id,
                site,
                second,
                dimension,
                outcome.clone(),
            ));
        }
    }
    let mut duplicate_ids = serde_json::to_value(duplicate_ids.snapshot()).unwrap();
    duplicate_ids["nodeGates"][1]["gate"]["open"]["id"] =
        duplicate_ids["nodeGates"][0]["gate"]["open"]["id"].clone();
    let duplicate_ids: IncidentEvaluatorSnapshot = serde_json::from_value(duplicate_ids).unwrap();
    assert!(matches!(
        IncidentEvaluator::from_snapshot(duplicate_ids),
        Err(IncidentStateError::InvalidGate)
    ));

    let mut symbol = IncidentEvaluator::default();
    let mut nis1 = IncidentEvaluator::default();
    let _ = symbol.ingest_with_context(
        observation(
            Site::A,
            0,
            Dimension::Connectivity,
            Outcome::Failure("down".into()),
        ),
        symbol_context(),
    );
    let _ = nis1.ingest_with_context(
        observation(Site::A, 0, Dimension::Sync, Outcome::LagBlocks(25)),
        EvaluationContext {
            network: Some(Network::Nis1Mainnet),
            ..normal_context()
        },
    );
    let mut conflicting_networks = serde_json::to_value(symbol.snapshot()).unwrap();
    let nis1_gate = serde_json::to_value(nis1.snapshot()).unwrap()["nodeGates"][0].clone();
    conflicting_networks["nodeGates"]
        .as_array_mut()
        .unwrap()
        .push(nis1_gate);
    let conflicting_networks: IncidentEvaluatorSnapshot =
        serde_json::from_value(conflicting_networks).unwrap();
    assert!(matches!(
        IncidentEvaluator::from_snapshot(conflicting_networks),
        Err(IncidentStateError::InvalidGate)
    ));
}

#[test]
fn snapshot_rejects_open_network_and_node_finalization_for_the_same_network() {
    let mut node = IncidentEvaluator::default();
    for (site, second) in [(Site::A, 0), (Site::A, 30), (Site::B, 60)] {
        let _ = node.ingest_with_context(
            observation(
                site,
                second,
                Dimension::Finalization,
                Outcome::FinalizationStalledMinutes(60),
            ),
            symbol_context(),
        );
    }
    let node_gate = serde_json::to_value(node.snapshot()).unwrap()["nodeGates"][0].clone();

    let mut network = IncidentEvaluator::default();
    let sample = |site, second| NetworkFinalizationSample {
        network: Network::SymbolMainnet,
        site,
        expected_references: 5,
        responding_references: 4,
        stalled_references: 3,
        stalled_minutes: 60,
        observed_at: instant(second),
    };
    let _ = network.ingest_network_finalization(sample(Site::A, 120));
    let _ = network.ingest_network_finalization(sample(Site::A, 150));
    let _ = network.ingest_network_finalization(sample(Site::B, 180));
    let mut conflicting = serde_json::to_value(network.snapshot()).unwrap();
    conflicting["nodeGates"]
        .as_array_mut()
        .unwrap()
        .push(node_gate);
    let conflicting: IncidentEvaluatorSnapshot = serde_json::from_value(conflicting).unwrap();
    assert!(matches!(
        IncidentEvaluator::from_snapshot(conflicting),
        Err(IncidentStateError::InvalidGate)
    ));

    let mut candidate = IncidentEvaluator::default();
    for second in [0, 30] {
        let _ = candidate.ingest_with_context(
            observation(
                Site::A,
                second,
                Dimension::Finalization,
                Outcome::FinalizationStalledMinutes(60),
            ),
            symbol_context(),
        );
    }
    let candidate_gate =
        serde_json::to_value(candidate.snapshot()).unwrap()["nodeGates"][0].clone();
    let mut conflicting = serde_json::to_value(network.snapshot()).unwrap();
    conflicting["nodeGates"]
        .as_array_mut()
        .unwrap()
        .push(candidate_gate);
    let conflicting: IncidentEvaluatorSnapshot = serde_json::from_value(conflicting).unwrap();
    assert!(matches!(
        IncidentEvaluator::from_snapshot(conflicting),
        Err(IncidentStateError::InvalidGate)
    ));
}

#[test]
fn snapshot_rejects_open_gate_with_impossible_recovery_state() {
    let mut evaluator = IncidentEvaluator::default();
    for (site, second) in [(Site::A, 0), (Site::A, 30), (Site::B, 60)] {
        let _ = evaluator.ingest(observation(
            site,
            second,
            Dimension::Connectivity,
            Outcome::Failure("down".into()),
        ));
    }
    let open = serde_json::to_value(evaluator.snapshot()).unwrap();

    let mut recovered = open.clone();
    recovered["nodeGates"][0]["gate"]["failures"] = serde_json::json!({});
    recovered["nodeGates"][0]["gate"]["healthy"] = serde_json::json!({
        "a": instant(70),
        "b": instant(80),
    });
    recovered["nodeGates"][0]["gate"]["last_seen"] = serde_json::json!({
        "a": instant(70),
        "b": instant(80),
    });
    let recovered: IncidentEvaluatorSnapshot = serde_json::from_value(recovered).unwrap();
    assert!(matches!(
        IncidentEvaluator::from_snapshot(recovered),
        Err(IncidentStateError::InvalidGate)
    ));

    let mut missing_site = open;
    missing_site["nodeGates"][0]["gate"]["failures"]
        .as_object_mut()
        .unwrap()
        .remove("b");
    let missing_site: IncidentEvaluatorSnapshot = serde_json::from_value(missing_site).unwrap();
    assert!(matches!(
        IncidentEvaluator::from_snapshot(missing_site),
        Err(IncidentStateError::InvalidGate)
    ));

    let mut empty_evidence = serde_json::to_value(evaluator.snapshot()).unwrap();
    empty_evidence["nodeGates"][0]["gate"]["failures"]["a"] =
        serde_json::json!({"first": null, "candidate": null});
    let empty_evidence: IncidentEvaluatorSnapshot = serde_json::from_value(empty_evidence).unwrap();
    assert!(matches!(
        IncidentEvaluator::from_snapshot(empty_evidence),
        Err(IncidentStateError::InvalidGate)
    ));
}

#[test]
fn explicit_snapshot_repair_resolves_a_completed_out_of_order_recovery() {
    let mut evaluator = IncidentEvaluator::default();
    for (site, second) in [(Site::A, 0), (Site::A, 30), (Site::B, 60)] {
        let _ = evaluator.ingest(observation(
            site,
            second,
            Dimension::Sync,
            Outcome::LagBlocks(25),
        ));
    }

    let mut invalid = serde_json::to_value(evaluator.snapshot()).unwrap();
    invalid["nodeGates"][0]["gate"]["failures"] = serde_json::json!({});
    invalid["nodeGates"][0]["gate"]["healthy"] = serde_json::json!({
        "a": instant(700),
        "b": instant(1_000),
    });
    invalid["nodeGates"][0]["gate"]["last_seen"] = serde_json::json!({
        "a": instant(700),
        "b": instant(1_000),
    });
    let invalid: IncidentEvaluatorSnapshot = serde_json::from_value(invalid).unwrap();

    assert!(matches!(
        IncidentEvaluator::from_snapshot(invalid.clone()),
        Err(IncidentStateError::InvalidGate)
    ));
    let (repaired, transitions) =
        IncidentEvaluator::from_snapshot_repairing_completed_recoveries(invalid).unwrap();

    assert!(matches!(
        transitions.as_slice(),
        [IncidentTransition::Resolved { at, .. }] if *at == instant(1_000)
    ));
    assert!(repaired.open_incidents().is_empty());
    assert!(IncidentEvaluator::from_snapshot(repaired.snapshot()).is_ok());
}

#[test]
fn snapshot_rejects_network_suppression_horizon_ahead_of_evidence() {
    let mut evaluator = IncidentEvaluator::default();
    let sample = |site, second, stalled_references| NetworkFinalizationSample {
        network: Network::SymbolMainnet,
        site,
        expected_references: 5,
        responding_references: 4,
        stalled_references,
        stalled_minutes: if stalled_references == 0 { 0 } else { 60 },
        observed_at: instant(second),
    };
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 120, 3));
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 150, 3));
    let _ = evaluator.ingest_network_finalization(sample(Site::B, 180, 3));
    let _ = evaluator.ingest_network_finalization(sample(Site::A, 240, 0));
    let _ = evaluator.ingest_network_finalization(sample(Site::B, 270, 0));
    let valid = serde_json::to_value(evaluator.snapshot()).unwrap();

    let mut future = valid.clone();
    future["networkFinalization"][0]["suppressedThrough"] =
        serde_json::to_value(instant(10_000)).unwrap();
    let future: IncidentEvaluatorSnapshot = serde_json::from_value(future).unwrap();
    assert!(matches!(
        IncidentEvaluator::from_snapshot(future),
        Err(IncidentStateError::InvalidGate)
    ));

    let mut no_evidence = valid;
    no_evidence["networkFinalization"][0]["gate"]["last_seen"] = serde_json::json!({});
    let no_evidence: IncidentEvaluatorSnapshot = serde_json::from_value(no_evidence).unwrap();
    assert!(matches!(
        IncidentEvaluator::from_snapshot(no_evidence),
        Err(IncidentStateError::InvalidGate)
    ));

    let mut candidate = IncidentEvaluator::default();
    for second in [220, 250] {
        let _ = candidate.ingest_with_context(
            observation(
                Site::A,
                second,
                Dimension::Finalization,
                Outcome::FinalizationStalledMinutes(60),
            ),
            symbol_context(),
        );
    }
    let candidate_gate =
        serde_json::to_value(candidate.snapshot()).unwrap()["nodeGates"][0].clone();
    let mut suppressed_candidate = serde_json::to_value(evaluator.snapshot()).unwrap();
    suppressed_candidate["nodeGates"]
        .as_array_mut()
        .unwrap()
        .push(candidate_gate);
    let suppressed_candidate: IncidentEvaluatorSnapshot =
        serde_json::from_value(suppressed_candidate).unwrap();
    assert!(matches!(
        IncidentEvaluator::from_snapshot(suppressed_candidate),
        Err(IncidentStateError::InvalidGate)
    ));
}
