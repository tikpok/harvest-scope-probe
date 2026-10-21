use monitor_core::{
    model::{Network, Node, NodeKind},
    scheduler::CheckKind,
};
use monitor_probe::protocol::{
    parse_harvester_count, parse_nis1_chain, parse_nis1_health, parse_nis1_identity,
    parse_peer_count, parse_supernode_info, parse_supernode_registration, parse_supernode_round,
    parse_symbol_chain, parse_symbol_health, parse_symbol_identity, parse_symbol_state_hash,
    parse_voting_remaining, request_path,
};

#[test]
fn parses_symbol_chain_numbers_encoded_as_strings() {
    let value = serde_json::json!({
        "height": "1234",
        "scoreHigh": "2",
        "scoreLow": "3",
        "latestFinalizedBlock": {"height": "1200", "finalizationEpoch": 77}
    });
    let snapshot = parse_symbol_chain(&value).unwrap();
    assert_eq!(snapshot.height, 1234);
    assert_eq!(snapshot.finalized_height, Some(1200));
    assert_eq!(snapshot.finalization_epoch, Some(77));
    assert_eq!(
        snapshot.chain_score.as_deref(),
        Some("36893488147419103235")
    );
}

#[test]
fn rejects_incomplete_symbol_chain_reference_fields() {
    let complete = serde_json::json!({
        "height": 1234,
        "scoreHigh": 2,
        "scoreLow": 3,
        "latestFinalizedBlock": {"height": 1200, "finalizationEpoch": 77}
    });
    for field in ["scoreHigh", "scoreLow", "latestFinalizedBlock"] {
        let mut value = complete.clone();
        value.as_object_mut().unwrap().remove(field);
        assert!(parse_symbol_chain(&value).is_err(), "missing {field}");
    }
    assert!(parse_symbol_chain(&serde_json::json!({
        "height": 1234,
        "scoreHigh": 2,
        "scoreLow": 3,
        "latestFinalizedBlock": {"height": 1200}
    }))
    .is_err());
}

#[test]
fn parses_nis1_chain_and_network_height() {
    let snapshot = parse_nis1_chain(
        &serde_json::json!({"height": 420}),
        Some(&serde_json::json!({"height": 425})),
        Some(&serde_json::json!({"score": "9000"})),
    )
    .unwrap();
    assert_eq!(snapshot.height, 420);
    assert_eq!(snapshot.network_height, Some(425));
    assert_eq!(snapshot.chain_score.as_deref(), Some("9000"));
}

#[test]
fn rejects_missing_or_invalid_nis1_reference_fields() {
    let height = serde_json::json!({"height": 420});
    let network_height = serde_json::json!({"height": 425});
    let score = serde_json::json!({"score": "9000"});
    assert!(parse_nis1_chain(&height, None, Some(&score)).is_err());
    assert!(parse_nis1_chain(&height, Some(&network_height), None).is_err());
    assert!(parse_nis1_chain(
        &height,
        Some(&serde_json::json!({"height": "invalid"})),
        Some(&score)
    )
    .is_err());
}

#[test]
fn never_builds_rest_chain_requests_for_peer_only_nodes() {
    let peer = Node::new(
        "peer",
        Network::SymbolMainnet,
        NodeKind::PeerOnly,
        "peer.example",
    );
    assert_eq!(request_path(&peer, CheckKind::Chain), None);
    assert_eq!(request_path(&peer, CheckKind::PeerTcp), Some("tcp"));
}

#[test]
fn selects_the_active_voting_key_range_from_account_payloads() {
    let account = serde_json::json!({
        "account": {"supplementalPublicKeys": {"voting": {"publicKeys": [
            {"startEpoch": 100, "endEpoch": 200},
            {"startEpoch": "201", "endEpoch": "300"}
        ]}}}
    });
    assert_eq!(
        parse_voting_remaining(&account, 150).unwrap(),
        Some((100, 200, 50))
    );
    assert_eq!(
        parse_voting_remaining(&account, 250).unwrap(),
        Some((201, 300, 50))
    );
}

#[test]
fn voting_parser_ignores_decoys_and_rejects_reversed_ranges() {
    let decoy = serde_json::json!({
        "startEpoch": 1,
        "endEpoch": 999,
        "account": {"supplementalPublicKeys": {"voting": {"publicKeys": []}}}
    });
    assert_eq!(parse_voting_remaining(&decoy, 150).unwrap(), None);

    let reversed = serde_json::json!({
        "account": {"supplementalPublicKeys": {"voting": {"publicKeys": [
            {"startEpoch": 200, "endEpoch": 100}
        ]}}}
    });
    assert!(parse_voting_remaining(&reversed, 150).is_err());
}

#[test]
fn rejects_incomplete_health_payloads_instead_of_treating_them_as_healthy() {
    assert!(parse_symbol_health(&serde_json::json!({"status": {"apiNode": "up"}})).is_err());
    assert!(parse_nis1_health(&serde_json::json!({"code": 1}), &serde_json::json!({})).is_err());
}

#[test]
fn treats_booted_and_synchronized_nis1_status_as_available() {
    for status_code in [5, 6] {
        assert_eq!(
            parse_nis1_health(
                &serde_json::json!({"code": 1}),
                &serde_json::json!({"code": status_code}),
            )
            .unwrap(),
            (1, status_code, true),
        );
    }

    assert_eq!(
        parse_nis1_health(
            &serde_json::json!({"code": 1}),
            &serde_json::json!({"code": 4}),
        )
        .unwrap(),
        (1, 4, false),
    );
    assert_eq!(
        parse_nis1_health(
            &serde_json::json!({"code": 0}),
            &serde_json::json!({"code": 5}),
        )
        .unwrap(),
        (0, 5, false),
    );
}

#[test]
fn distinguishes_valid_empty_collections_from_malformed_payloads() {
    assert_eq!(
        parse_peer_count(Network::SymbolMainnet, &serde_json::json!([])).unwrap(),
        0
    );
    assert!(parse_peer_count(Network::SymbolMainnet, &serde_json::json!({})).is_err());
    assert_eq!(
        parse_harvester_count(&serde_json::json!({"unlockedAccount": []})).unwrap(),
        0
    );
    assert_eq!(
        parse_harvester_count(&serde_json::json!({
            "unlockedAccount": ["A", "B"]
        }))
        .unwrap(),
        2
    );
    assert!(parse_harvester_count(&serde_json::json!([])).is_err());
    assert!(parse_harvester_count(&serde_json::json!({})).is_err());
}

#[test]
fn requires_a_non_empty_symbol_state_hash() {
    let valid = "A".repeat(64);
    assert_eq!(
        parse_symbol_state_hash(&serde_json::json!({"block": {"stateHash": valid}})).unwrap(),
        "A".repeat(64)
    );
    assert!(parse_symbol_state_hash(&serde_json::json!({"block": {"stateHash": ""}})).is_err());
    assert!(parse_symbol_state_hash(&serde_json::json!({"block": {"stateHash": "ABC"}})).is_err());
    assert!(
        parse_symbol_state_hash(&serde_json::json!({"block": {"stateHash": "Z".repeat(64)}}))
            .is_err()
    );
    assert!(parse_symbol_state_hash(&serde_json::json!({"block": {}})).is_err());
}

#[test]
fn support_matrix_rejects_unavailable_protocol_combinations() {
    let nis = Node::new("nis", Network::Nis1Mainnet, NodeKind::Api, "nis.example");
    for check in [
        CheckKind::Finalization,
        CheckKind::StateHash,
        CheckKind::Harvest,
        CheckKind::Voting,
        CheckKind::WebSocket,
    ] {
        assert_eq!(request_path(&nis, check), None, "{check:?}");
    }

    let peer = Node::new(
        "peer",
        Network::SymbolMainnet,
        NodeKind::PeerOnly,
        "peer.example",
    );
    for check in [
        CheckKind::Availability,
        CheckKind::Chain,
        CheckKind::Finalization,
        CheckKind::Peers,
        CheckKind::StateHash,
        CheckKind::Harvest,
        CheckKind::Identity,
        CheckKind::Certificate,
        CheckKind::WebSocket,
    ] {
        assert_eq!(request_path(&peer, check), None, "{check:?}");
    }
}

#[test]
fn identity_parsers_require_identity_role_and_version_fields() {
    let symbol = parse_symbol_identity(
        &serde_json::json!({
            "version": 1,
            "nodePublicKey": "A".repeat(64),
            "roles": 7
        }),
        &serde_json::json!({
            "serverInfo": {"restVersion": "1.0.14", "sdkVersion": "0.7.14"}
        }),
    )
    .unwrap();
    assert_eq!(symbol["roles"], 7);
    assert_eq!(symbol["restVersion"], "1.0.14");
    assert!(parse_symbol_identity(
        &serde_json::json!({"version": 1, "nodePublicKey": "A".repeat(64)}),
        &serde_json::json!({"serverInfo": {"restVersion": "1.0.14"}})
    )
    .is_err());

    let nis = parse_nis1_identity(&serde_json::json!({
        "node": {
            "metaData": {"networkId": -104},
            "identity": {"public-key": "B".repeat(64)}
        },
        "nisInfo": {"application": "NIS", "version": "0.6.97"}
    }))
    .unwrap();
    assert_eq!(nis["application"], "NIS");
    let unsigned_nis = parse_nis1_identity(&serde_json::json!({
        "node": {
            "metaData": {"networkId": 104},
            "identity": {"public-key": "B".repeat(64)}
        },
        "nisInfo": {"application": "NIS", "version": "0.6.97"}
    }))
    .unwrap();
    assert_eq!(unsigned_nis["networkId"], -104);
    assert!(parse_nis1_identity(&serde_json::json!({"node": {}, "nisInfo": {}})).is_err());
}

#[test]
fn supernode_registration_requires_one_exact_endpoint_match() {
    let list = serde_json::json!([
        {
            "endpoint": "http://nis.example:7890",
            "id": 72768917,
            "name": "Exact",
            "status": "active"
        },
        {
            "endpoint": "http://not-nis.example:7890",
            "id": 42,
            "name": "Substring decoy",
            "status": "active"
        }
    ]);

    let registration = parse_supernode_registration(&list, &[("NIS.EXAMPLE", 7890)])
        .unwrap()
        .unwrap();
    assert_eq!(registration.id, 72768917);
    assert!(registration.active);
    assert!(
        parse_supernode_registration(&list, &[("missing.example", 7890)])
            .unwrap()
            .is_none()
    );

    let duplicate = serde_json::json!([
        {"endpoint": "http://nis.example:7890", "id": 1, "status": "active"},
        {"endpoint": "https://nis.example:7890", "id": 2, "status": "active"}
    ]);
    assert!(parse_supernode_registration(&duplicate, &[("nis.example", 7890)]).is_err());

    let unknown_status = serde_json::json!([{
        "endpoint": "http://nis.example:7890",
        "id": 72768917,
        "status": "mystery"
    }]);
    assert!(parse_supernode_registration(&unknown_status, &[("nis.example", 7890)]).is_err());

    let unrelated_bad_rows = serde_json::json!([
        {"endpoint": "http://old.example:7890", "id": 3, "status": "stale"},
        {"endpoint": "not a URL", "status": "mystery"},
        {"endpoint": "http://nis.example:7890", "id": 72768917, "status": "active"}
    ]);
    let registration = parse_supernode_registration(&unrelated_bad_rows, &[("nis.example", 7890)])
        .unwrap()
        .unwrap();
    assert_eq!(registration.id, 72768917);
    assert_eq!(registration.status, "active");

    let stale = serde_json::json!([{
        "endpoint": "http://nis.example:7890",
        "id": 72768917,
        "status": "stale"
    }]);
    let registration = parse_supernode_registration(&stale, &[("nis.example", 7890)])
        .unwrap()
        .unwrap();
    assert!(!registration.active);
    assert_eq!(registration.status, "stale");
}

#[test]
fn supernode_info_binds_the_id_endpoint_and_latest_round() {
    let info = serde_json::json!({
        "endpoint": "http://nis.example:7890",
        "id": 72768917,
        "lastResultsRound": 5053,
        "status": "active"
    });
    assert_eq!(
        parse_supernode_info(&info, 72768917, "nis.example", 7890).unwrap(),
        5053
    );

    let mut wrong_id = info.clone();
    wrong_id["id"] = serde_json::json!(72768918);
    assert!(parse_supernode_info(&wrong_id, 72768917, "nis.example", 7890).is_err());
    let mut wrong_endpoint = info;
    wrong_endpoint["endpoint"] = serde_json::json!("http://other.example:7890");
    assert!(parse_supernode_info(&wrong_endpoint, 72768917, "nis.example", 7890).is_err());
}

#[test]
fn supernode_round_requires_all_eight_results_and_a_fresh_timestamp() {
    let now = chrono::DateTime::parse_from_rfc3339("2026-08-03T06:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let passing = serde_json::json!([{
        "balance": true,
        "bandwidth": true,
        "chainHeight": true,
        "chainPart": true,
        "computingPower": true,
        "ping": true,
        "responsiveness": true,
        "roundId": 5053,
        "timestamp": "2026-08-03T04:01:00.040826+00:00",
        "version": true
    }]);
    let result = parse_supernode_round(&passing, 5053, now).unwrap();
    assert!(result.passed);
    assert!(result.failed_tests.is_empty());

    let mut failing = passing.clone();
    failing[0]["version"] = serde_json::json!(false);
    failing[0]["computingPower"] = serde_json::json!(false);
    let result = parse_supernode_round(&failing, 5053, now).unwrap();
    assert!(!result.passed);
    assert_eq!(result.failed_tests, ["computingPower", "version"]);

    let mut incomplete = passing.clone();
    incomplete[0].as_object_mut().unwrap().remove("ping");
    assert!(parse_supernode_round(&incomplete, 5053, now).is_err());

    let stale_now = now + chrono::Duration::hours(13);
    assert!(parse_supernode_round(&passing, 5053, stale_now).is_err());
    let before_result = now - chrono::Duration::hours(3);
    assert!(parse_supernode_round(&passing, 5053, before_result).is_err());
}
