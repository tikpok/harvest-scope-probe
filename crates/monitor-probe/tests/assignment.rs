use std::{collections::HashSet, path::Path, time::Duration as StdDuration};

use chrono::{DateTime, Duration, SecondsFormat, TimeZone, Utc};
use monitor_core::{
    model::{Dimension, Network, NodeKind, Observation, Outcome, Site},
    scheduler::CheckKind,
};
use monitor_probe::assignment::{
    decode_assignment_envelope, Assignment, AssignmentCache, AssignmentClient, AssignmentEnvelope,
    AssignmentError, AssignmentProvider, AssignmentSource, AssignmentTarget, AssignmentTargetKind,
};
use monitor_probe::spool::Spool;
use reqwest::redirect::Policy;
use serde_json::Value;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[test]
fn rejects_wrong_schema_identity_invalid_times_expiry_and_truncated_json() {
    let fetched_at = instant(12, 1);
    let valid = envelope_json(fetched_at);

    let decoded = decode_assignment_envelope(&valid, "probe-a", Site::A, fetched_at).unwrap();
    assert_eq!(decoded.schema_version, 1);
    assert_eq!(decoded.assignments.len(), 1);

    let mut deadline: Value = serde_json::from_slice(&valid).unwrap();
    let not_after = fetched_at + Duration::seconds(30);
    deadline["assignments"][0]["notAfter"] = serde_json::json!(not_after);
    assert_eq!(
        decode_assignment_envelope(
            &serde_json::to_vec(&deadline).unwrap(),
            "probe-a",
            Site::A,
            fetched_at,
        )
        .unwrap()
        .assignments[0]
            .not_after,
        Some(not_after)
    );
    deadline["assignments"][0]["notAfter"] = serde_json::json!(instant(12, 0));
    assert!(decode_assignment_envelope(
        &serde_json::to_vec(&deadline).unwrap(),
        "probe-a",
        Site::A,
        fetched_at,
    )
    .is_err());

    let mut v2: Value = serde_json::from_slice(&valid).unwrap();
    v2["schemaVersion"] = serde_json::json!(2);
    assert_eq!(
        decode_assignment_envelope(
            &serde_json::to_vec(&v2).unwrap(),
            "probe-a",
            Site::A,
            fetched_at,
        )
        .unwrap()
        .schema_version,
        2
    );

    for (field, value) in [
        ("schemaVersion", serde_json::json!(3)),
        ("probeId", serde_json::json!("probe-b")),
        ("site", serde_json::json!("b")),
        ("generatedAt", serde_json::json!("not-a-time")),
        ("expiresAt", serde_json::json!("not-a-time")),
    ] {
        let mut invalid: Value = serde_json::from_slice(&valid).unwrap();
        invalid[field] = value;
        assert!(decode_assignment_envelope(
            &serde_json::to_vec(&invalid).unwrap(),
            "probe-a",
            Site::A,
            fetched_at,
        )
        .is_err());
    }

    assert!(decode_assignment_envelope(&valid, "probe-a", Site::A, instant(36, 0)).is_err());
    assert!(
        decode_assignment_envelope(&valid[..valid.len() - 1], "probe-a", Site::A, fetched_at,)
            .is_err()
    );

    let mut symbol_rest_on_peer_port: Value = serde_json::from_slice(&valid).unwrap();
    symbol_rest_on_peer_port["assignments"][0]["targets"][0]["port"] = serde_json::json!(7900);
    assert!(decode_assignment_envelope(
        &serde_json::to_vec(&symbol_rest_on_peer_port).unwrap(),
        "probe-a",
        Site::A,
        fetched_at,
    )
    .is_err());
}

#[test]
fn accepts_five_comparison_references_but_rejects_contract_fanout_overflow() {
    let fetched_at = instant(12, 1);
    let mut generic: Value = serde_json::from_slice(&envelope_json(fetched_at)).unwrap();
    generic["schemaVersion"] = serde_json::json!(2);
    generic["assignments"][0]["checks"] = serde_json::json!(["chain"]);
    generic["assignments"][0]["targets"] = serde_json::json!([{
        "kind": "rest", "scheme": "https", "host": "node.example", "port": 3001
    }]);
    for index in 0..5 {
        generic["assignments"][0]["targets"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "kind": "reference_api",
                "scheme": "https",
                "host": format!("reference-{index}.example"),
                "port": 3001
            }));
    }
    assert!(decode_assignment_envelope(
        &serde_json::to_vec(&generic).unwrap(),
        "probe-a",
        Site::A,
        fetched_at
    )
    .is_ok());
    generic["assignments"][0]["targets"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "kind": "reference_api",
            "scheme": "https",
            "host": "reference-5.example",
            "port": 3001
        }));
    assert!(decode_assignment_envelope(
        &serde_json::to_vec(&generic).unwrap(),
        "probe-a",
        Site::A,
        fetched_at
    )
    .is_err());

    let mut voting = generic;
    voting["assignments"][0]["checks"] = serde_json::json!(["voting"]);
    voting["assignments"][0]["targets"] = serde_json::json!([]);
    for index in 0..4 {
        voting["assignments"][0]["targets"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "kind": "reference_api",
                "scheme": "https",
                "host": format!("voting-{index}.example"),
                "port": 3001,
                "subjectPublicKey": "A".repeat(64)
            }));
    }
    assert!(decode_assignment_envelope(
        &serde_json::to_vec(&voting).unwrap(),
        "probe-a",
        Site::A,
        fetched_at
    )
    .is_err());
}

#[tokio::test]
async fn sqlite_cache_replaces_atomically_and_restores_after_restart() {
    let directory = TempDir::new().unwrap();
    let url = sqlite_url(&directory.path().join("assignments.db"));
    let fetched_at = instant(12, 1);
    let envelope =
        decode_assignment_envelope(&envelope_json(fetched_at), "probe-a", Site::A, fetched_at)
            .unwrap();

    let cache = AssignmentCache::connect(&url).await.unwrap();
    cache
        .replace(&envelope, fetched_at, "probe-a", Site::A)
        .await
        .unwrap();
    drop(cache);

    let cache = AssignmentCache::connect(&url).await.unwrap();
    assert_eq!(
        cache
            .load(instant(13, 0), "probe-a", Site::A)
            .await
            .unwrap(),
        envelope
    );

    let mut invalid = envelope.clone();
    invalid.expires_at = invalid.generated_at;
    assert!(cache
        .replace(&invalid, instant(13, 1), "probe-a", Site::A)
        .await
        .is_err());
    assert_eq!(
        cache
            .load(instant(13, 2), "probe-a", Site::A)
            .await
            .unwrap(),
        envelope,
        "a rejected replacement must leave the prior row intact"
    );

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_assignment_replacement
         BEFORE UPDATE ON assignment_cache
         BEGIN
           SELECT RAISE(ABORT, 'simulated partial write');
         END",
    )
    .execute(&pool)
    .await
    .unwrap();
    drop(pool);
    let mut replacement = envelope.clone();
    replacement.generated_at += Duration::minutes(10);
    replacement.expires_at += Duration::minutes(10);
    replacement.epoch += 1;
    replacement.assignments[0].scheduled_at += Duration::minutes(10);
    assert!(cache
        .replace(
            &replacement,
            fetched_at + Duration::minutes(10),
            "probe-a",
            Site::A,
        )
        .await
        .is_err());
    assert_eq!(
        cache
            .load(instant(13, 3), "probe-a", Site::A)
            .await
            .unwrap(),
        envelope,
        "a database failure during replacement must preserve the complete old row"
    );
}

#[tokio::test]
async fn deferred_assignment_survives_restart_until_explicit_completion() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("deferred.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let assignment = deferred_assignment("a", instant(12, 2));
    let deferred_at = instant(12, 3);
    let valid_until = deferred_at + Duration::minutes(10);

    let cache = AssignmentCache::connect(&url).await.unwrap();
    cache
        .defer("group-a", &assignment, deferred_at, valid_until)
        .await
        .unwrap();
    drop(cache);

    let restored = AssignmentCache::connect(&url).await.unwrap();
    let rows = restored.load_deferred(deferred_at).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_ne!(rows[0].group_key, "group-a");
    assert_eq!(rows[0].assignment, assignment);
    assert_eq!(rows[0].valid_until, valid_until);
    restored
        .complete_deferred(&rows[0].group_key)
        .await
        .unwrap();
    assert!(restored
        .load_deferred(deferred_at)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn newer_epoch_replaces_legacy_deferred_rows_for_the_same_execution_group() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("deferred-replacement.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let older = deferred_assignment("a", instant(12, 2));
    let newer = deferred_assignment("b", instant(12, 12));
    let valid_until = instant(13, 0);

    let cache = AssignmentCache::connect(&url).await.unwrap();
    cache
        .defer("legacy-epoch-a", &older, instant(12, 20), valid_until)
        .await
        .unwrap();
    cache
        .defer("legacy-epoch-b", &newer, instant(12, 13), valid_until)
        .await
        .unwrap();
    drop(cache);

    let restored = AssignmentCache::connect(&url).await.unwrap();
    let rows = restored.load_deferred(instant(12, 21)).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].assignment, newer);
    assert!(!rows[0].group_key.contains(&"b".repeat(64)));
    let stable_group_key = rows[0].group_key.clone();
    drop(restored);

    let compacted = AssignmentCache::connect(&url).await.unwrap();
    let rows = compacted.load_deferred(instant(12, 21)).await.unwrap();
    assert_eq!(rows.len(), 1, "compaction must persist across restart");
    assert_eq!(rows[0].group_key, stable_group_key);
    compacted
        .complete_deferred(&stable_group_key)
        .await
        .unwrap();
    assert!(compacted
        .load_deferred(instant(12, 21))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn failed_legacy_compaction_rolls_back_every_deferred_row() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("deferred-compaction-rollback.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let older = deferred_assignment("c", instant(12, 2));
    let newer = deferred_assignment("d", instant(12, 12));
    let valid_until = instant(13, 0);

    let cache = AssignmentCache::connect(&url).await.unwrap();
    cache
        .defer("legacy-epoch-c", &older, instant(12, 3), valid_until)
        .await
        .unwrap();
    cache
        .defer("legacy-epoch-d", &newer, instant(12, 13), valid_until)
        .await
        .unwrap();
    drop(cache);

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_deferred_compaction
         BEFORE INSERT ON deferred_assignments
         WHEN NEW.group_key LIKE 'v2|%'
         BEGIN
           SELECT RAISE(ABORT, 'simulated compaction failure');
         END",
    )
    .execute(&pool)
    .await
    .unwrap();
    drop(pool);

    let cache = AssignmentCache::connect(&url).await.unwrap();
    assert!(cache.load_deferred(instant(12, 14)).await.is_err());
    drop(cache);

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let row_count: i64 = sqlx::query_scalar("SELECT count(*) FROM deferred_assignments")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row_count, 2, "failed compaction must roll back its DELETE");
    sqlx::query("DROP TRIGGER reject_deferred_compaction")
        .execute(&pool)
        .await
        .unwrap();
    drop(pool);

    let restored = AssignmentCache::connect(&url).await.unwrap();
    let rows = restored.load_deferred(instant(12, 14)).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].assignment, newer);
}

#[tokio::test]
#[ignore = "capacity gate: run for deferred-backlog release validation"]
async fn compacts_fifty_thousand_legacy_deferred_rows_without_restoring_them_all() {
    const LEGACY_ROW_COUNT: i64 = 50_400;
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("deferred-capacity.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let assignment = deferred_assignment("e", instant(12, 2));
    let assignment_json = serde_json::to_vec(&assignment).unwrap();
    let deferred_at = instant(12, 3).to_rfc3339_opts(SecondsFormat::Nanos, true);
    let valid_until = instant(13, 0).to_rfc3339_opts(SecondsFormat::Nanos, true);

    drop(AssignmentCache::connect(&url).await.unwrap());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query(
        "WITH digits(value) AS (
             VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)
         ), sequence(value) AS (
             SELECT a.value
                  + 10 * b.value
                  + 100 * c.value
                  + 1000 * d.value
                  + 10000 * e.value
             FROM digits a
             CROSS JOIN digits b
             CROSS JOIN digits c
             CROSS JOIN digits d
             CROSS JOIN digits e
         )
         INSERT INTO deferred_assignments(
             group_key, assignment_json, deferred_at, valid_until
         )
         SELECT printf('legacy-%05d', value), ?, ?, ?
         FROM sequence
         WHERE value < ?",
    )
    .bind(assignment_json)
    .bind(deferred_at)
    .bind(valid_until)
    .bind(LEGACY_ROW_COUNT)
    .execute(&pool)
    .await
    .unwrap();
    drop(pool);

    let cache = AssignmentCache::connect(&url).await.unwrap();
    let rows = cache.load_deferred(instant(12, 14)).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].assignment, assignment);
    drop(cache);

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let stored_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM deferred_assignments")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stored_rows, 1);
}

#[tokio::test]
async fn deferred_assignment_is_atomically_pruned_at_its_hard_expiry() {
    let cache = AssignmentCache::in_memory().await.unwrap();
    let assignment = deferred_assignment("a", instant(12, 2));
    let deferred_at = instant(12, 3);
    let valid_until = deferred_at + Duration::seconds(60);

    cache
        .defer("expiring-group", &assignment, deferred_at, valid_until)
        .await
        .unwrap();

    assert_eq!(
        cache
            .load_deferred(valid_until - Duration::nanoseconds(1))
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(cache.load_deferred(valid_until).await.unwrap().is_empty());
    assert!(cache
        .load_deferred(valid_until - Duration::nanoseconds(1))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn legacy_deferred_rows_without_a_deadline_are_discarded_on_upgrade() {
    let directory = TempDir::new().unwrap();
    let url = sqlite_url(&directory.path().join("legacy-deferred.db"));
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE deferred_assignments (
             group_key TEXT PRIMARY KEY,
             assignment_json BLOB NOT NULL,
             deferred_at TEXT NOT NULL
         )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO deferred_assignments(group_key, assignment_json, deferred_at)
         VALUES ('legacy', x'7B7D', '2026-08-09T12:03:00.000000000Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    drop(pool);

    let cache = AssignmentCache::connect(&url).await.unwrap();
    assert!(cache
        .load_deferred(instant(12, 3))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn cache_uses_fetched_at_and_expires_at_and_rejects_corrupt_rows() {
    let directory = TempDir::new().unwrap();
    let url = sqlite_url(&directory.path().join("assignments.db"));
    let fetched_at = instant(12, 1);
    let envelope =
        decode_assignment_envelope(&envelope_json(fetched_at), "probe-a", Site::A, fetched_at)
            .unwrap();
    let cache = AssignmentCache::connect(&url).await.unwrap();
    cache
        .replace(&envelope, fetched_at, "probe-a", Site::A)
        .await
        .unwrap();

    assert!(matches!(
        cache.load(envelope.expires_at, "probe-a", Site::A).await,
        Err(AssignmentError::InvalidEnvelope("envelope is expired"))
    ));

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query("UPDATE assignment_cache SET fetched_at = ? WHERE singleton = 1")
        .bind((fetched_at - Duration::hours(24) - Duration::milliseconds(1)).to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        cache.load(fetched_at, "probe-a", Site::A).await,
        Err(AssignmentError::InvalidEnvelope(
            "fetchedAt is older than 24 hours"
        ))
    ));

    sqlx::query(
        "UPDATE assignment_cache SET fetched_at = ?, envelope_json = ? WHERE singleton = 1",
    )
    .bind(fetched_at.to_rfc3339())
    .bind(br#"{"schemaVersion":1"#.as_slice())
    .execute(&pool)
    .await
    .unwrap();
    assert!(cache.load(fetched_at, "probe-a", Site::A).await.is_err());
}

#[tokio::test]
async fn central_get_is_authenticated_and_valid_cache_is_the_only_fallback() {
    let fetched_at = instant(12, 1);
    let body = envelope_json(fetched_at);
    let base_url = one_shot_http(body.clone(), None).await;
    let cache = AssignmentCache::in_memory().await.unwrap();
    let client = assignment_client(&base_url, usize::MAX);
    let provider = AssignmentProvider::new(client, cache.clone());

    let fresh = provider.refresh_at(fetched_at).await.unwrap();
    assert_eq!(fresh.source, AssignmentSource::Central);

    let fallback = provider
        .refresh_at(fetched_at + Duration::minutes(5))
        .await
        .unwrap();
    assert_eq!(fallback.source, AssignmentSource::Cache);
    assert_eq!(fallback.envelope, fresh.envelope);

    let truncated_url = one_shot_http(body[..body.len() - 1].to_vec(), None).await;
    let truncated =
        AssignmentProvider::new(assignment_client(&truncated_url, usize::MAX), cache.clone())
            .refresh_at(fetched_at + Duration::minutes(6))
            .await
            .unwrap();
    assert_eq!(truncated.source, AssignmentSource::Cache);
    assert_eq!(truncated.envelope, fresh.envelope);

    let invalid_url = one_shot_http(br#"{"schemaVersion":3}"#.to_vec(), None).await;
    let empty_cache = AssignmentCache::in_memory().await.unwrap();
    assert!(
        AssignmentProvider::new(assignment_client(&invalid_url, usize::MAX), empty_cache,)
            .refresh_at(fetched_at)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn client_rejects_a_response_before_exceeding_the_decode_budget() {
    let body = vec![b' '; 2_048];
    let base_url = one_shot_http(body, None).await;
    let client = assignment_client(&base_url, 1_024);
    assert!(client.fetch_at(instant(12, 1)).await.is_err());
}

#[tokio::test]
async fn production_fetch_records_fetched_at_after_the_complete_body_arrives() {
    let generated_at = Utc::now() + Duration::milliseconds(100);
    let body = envelope_json_for_generated_at(generated_at);
    let base_url = delayed_one_shot_http(body, StdDuration::from_millis(200)).await;
    let client = assignment_client(&base_url, usize::MAX);

    let envelope = client.fetch().await.unwrap();
    assert_eq!(envelope.generated_at, generated_at);
}

#[tokio::test]
async fn retry_fetch_uses_the_authenticated_retry_only_assignment_view() {
    let generated_at = Utc::now() - Duration::seconds(1);
    let body = envelope_json_for_generated_at(generated_at);
    let base_url = retry_one_shot_http(body).await;
    let client = assignment_client(&base_url, usize::MAX);

    let envelope = client.fetch_retries().await.unwrap();
    assert_eq!(envelope.generated_at, generated_at);
}

#[test]
fn grouped_jobs_create_stable_distinct_observation_ids_and_targeted_checks() {
    let assignment = grouped_assignment();

    let first = assignment.assigned_checks().unwrap();
    let replay = assignment.assigned_checks().unwrap();
    assert_eq!(first, replay);
    assert_eq!(first.len(), 3);
    assert_eq!(
        first
            .iter()
            .map(|check| check.observation_job_id.as_str())
            .collect::<HashSet<_>>()
            .len(),
        3,
        "each observation in a grouped job needs its own idempotency key"
    );
    let peer = first
        .iter()
        .find(|check| check.check == CheckKind::PeerTcp)
        .unwrap();
    assert_eq!(peer.node.host, "peer.example");
    assert_eq!(
        peer.diagnostic_rest_node
            .as_ref()
            .map(|node| node.host.as_str()),
        Some("rest.example")
    );
    assert!(first
        .iter()
        .filter(|check| check.check != CheckKind::PeerTcp)
        .all(|check| check.node.host == "rest.example"));
}

#[test]
fn symbol_api_voting_keeps_the_subject_key_on_its_reference_target() {
    let subject_key = "b".repeat(64);
    let assignment = Assignment {
        job_id: "c".repeat(64),
        node_id: "symbol-api".into(),
        network: Network::SymbolMainnet,
        node_kind: NodeKind::Api,
        targets: vec![AssignmentTarget {
            kind: AssignmentTargetKind::ReferenceApi,
            scheme: "https".into(),
            host: "api.example".into(),
            port: 3001,
            subject_public_key: Some(subject_key.clone()),
        }],
        checks: vec![CheckKind::Voting],
        scheduled_at: instant(12, 2),
        not_after: None,
        request_units: 1,
        detailed_owner: true,
    };

    let checks = assignment.assigned_checks().unwrap();
    assert_eq!(checks.len(), 1);
    assert_eq!(checks[0].check, CheckKind::Voting);
    assert_eq!(
        checks[0].node.public_key.as_deref(),
        Some(subject_key.as_str())
    );
    assert_eq!(checks[0].node.host, "api.example");
}

#[test]
fn v2_checks_keep_local_target_and_all_distinct_reference_fallbacks() {
    let references = (0..3)
        .map(|index| AssignmentTarget {
            kind: AssignmentTargetKind::ReferenceApi,
            scheme: "https".into(),
            host: format!("reference-{index}.example"),
            port: 3001,
            subject_public_key: None,
        })
        .collect::<Vec<_>>();
    let mut targets = vec![AssignmentTarget {
        kind: AssignmentTargetKind::Rest,
        scheme: "https".into(),
        host: "local.example".into(),
        port: 3001,
        subject_public_key: None,
    }];
    targets.extend(references);
    let assignment = Assignment {
        job_id: "d".repeat(64),
        node_id: "symbol-api".into(),
        network: Network::SymbolMainnet,
        node_kind: NodeKind::Api,
        targets,
        checks: vec![CheckKind::Chain],
        scheduled_at: instant(12, 2),
        not_after: None,
        request_units: 1,
        detailed_owner: true,
    };

    let check = assignment.assigned_checks().unwrap().remove(0);
    assert_eq!(check.node.host, "local.example");
    assert_eq!(check.reference_nodes.len(), 3);
    assert_eq!(check.reference_nodes[0].host, "reference-0.example");
    assert_eq!(check.reference_nodes[2].host, "reference-2.example");
}

#[test]
fn voting_keeps_reference_fallbacks_after_the_primary() {
    let subject_key = "b".repeat(64);
    let assignment = Assignment {
        job_id: "e".repeat(64),
        node_id: "symbol-peer".into(),
        network: Network::SymbolMainnet,
        node_kind: NodeKind::PeerOnly,
        targets: (0..3)
            .map(|index| AssignmentTarget {
                kind: AssignmentTargetKind::ReferenceApi,
                scheme: "https".into(),
                host: format!("reference-{index}.example"),
                port: 3001,
                subject_public_key: Some(subject_key.clone()),
            })
            .collect(),
        checks: vec![CheckKind::Voting],
        scheduled_at: instant(12, 2),
        not_after: None,
        request_units: 1,
        detailed_owner: true,
    };

    let check = assignment.assigned_checks().unwrap().remove(0);
    assert_eq!(check.node.host, "reference-0.example");
    assert_eq!(check.reference_nodes.len(), 2);
    assert_eq!(check.reference_nodes[0].host, "reference-1.example");
    assert_eq!(check.reference_nodes[1].host, "reference-2.example");
}

#[test]
fn grouped_v2_voting_prefers_the_subject_bearing_reference_target() {
    let subject_key = "c".repeat(64);
    let assignment = Assignment {
        job_id: "1".repeat(64),
        node_id: "symbol-api".into(),
        network: Network::SymbolMainnet,
        node_kind: NodeKind::Api,
        targets: vec![
            AssignmentTarget {
                kind: AssignmentTargetKind::Rest,
                scheme: "https".into(),
                host: "local.example".into(),
                port: 3001,
                subject_public_key: None,
            },
            AssignmentTarget {
                kind: AssignmentTargetKind::ReferenceApi,
                scheme: "https".into(),
                host: "a-generic-reference.example".into(),
                port: 3001,
                subject_public_key: None,
            },
            AssignmentTarget {
                kind: AssignmentTargetKind::ReferenceApi,
                scheme: "https".into(),
                host: "local.example".into(),
                port: 3001,
                subject_public_key: Some(subject_key.clone()),
            },
        ],
        checks: vec![CheckKind::Chain, CheckKind::Voting],
        scheduled_at: instant(12, 2),
        not_after: None,
        request_units: 2,
        detailed_owner: true,
    };

    let checks = assignment.assigned_checks().unwrap();
    let chain = checks
        .iter()
        .find(|check| check.check == CheckKind::Chain)
        .unwrap();
    assert_eq!(chain.reference_nodes.len(), 1);
    assert_eq!(chain.reference_nodes[0].host, "a-generic-reference.example");
    let voting = checks
        .into_iter()
        .find(|check| check.check == CheckKind::Voting)
        .unwrap();

    assert_eq!(voting.node.host, "local.example");
    assert_eq!(
        voting.node.public_key.as_deref(),
        Some(subject_key.as_str())
    );
    assert!(voting.reference_nodes.is_empty());
}

#[test]
fn web_socket_assignment_preserves_its_scheme_and_port_for_execution() {
    let assignment = Assignment {
        job_id: "f".repeat(64),
        node_id: "symbol-api".into(),
        network: Network::SymbolMainnet,
        node_kind: NodeKind::Api,
        targets: vec![AssignmentTarget {
            kind: AssignmentTargetKind::WebSocket,
            scheme: "wss".into(),
            host: "ws.example".into(),
            port: 3001,
            subject_public_key: None,
        }],
        checks: vec![CheckKind::WebSocket],
        scheduled_at: instant(12, 2),
        not_after: None,
        request_units: 1,
        detailed_owner: true,
    };

    let check = assignment.assigned_checks().unwrap().remove(0);

    assert_eq!(check.node.host, "ws.example");
    assert_eq!(check.node.rest_scheme.as_deref(), Some("wss"));
    assert_eq!(check.node.rest_port, Some(3001));
}

#[tokio::test]
async fn grouped_observations_are_spooled_once_per_check_across_replay() {
    let checks = grouped_assignment().assigned_checks().unwrap();
    let spool = Spool::in_memory(100 * 1024 * 1024).await.unwrap();
    for _ in 0..2 {
        for check in &checks {
            let observation = Observation::new(
                &check.observation_job_id,
                &check.node.id,
                Site::A,
                Dimension::Connectivity,
                Outcome::Healthy,
                instant(12, 3),
            );
            spool
                .enqueue(
                    &observation.job_id,
                    &serde_json::to_vec(&observation).unwrap(),
                )
                .await
                .unwrap();
        }
    }

    let pending = spool.pending(10).await.unwrap();
    assert_eq!(pending.len(), checks.len());
    assert_eq!(
        pending
            .iter()
            .map(|item| item.job_id.as_str())
            .collect::<HashSet<_>>(),
        checks
            .iter()
            .map(|check| check.observation_job_id.as_str())
            .collect::<HashSet<_>>()
    );
}

fn grouped_assignment() -> Assignment {
    Assignment {
        job_id: "a".repeat(64),
        node_id: "node-1".into(),
        network: Network::SymbolMainnet,
        node_kind: NodeKind::Api,
        targets: vec![
            AssignmentTarget {
                kind: AssignmentTargetKind::Rest,
                scheme: "https".into(),
                host: "rest.example".into(),
                port: 3001,
                subject_public_key: None,
            },
            AssignmentTarget {
                kind: AssignmentTargetKind::PeerTcp,
                scheme: "tcp".into(),
                host: "peer.example".into(),
                port: 7900,
                subject_public_key: None,
            },
        ],
        checks: vec![
            CheckKind::Availability,
            CheckKind::PeerTcp,
            CheckKind::Chain,
        ],
        scheduled_at: instant(12, 2),
        not_after: None,
        request_units: 3,
        detailed_owner: true,
    }
}

fn deferred_assignment(job_byte: &str, scheduled_at: DateTime<Utc>) -> Assignment {
    Assignment {
        job_id: job_byte.repeat(64),
        node_id: "deferred-node".into(),
        network: Network::SymbolMainnet,
        node_kind: NodeKind::Api,
        targets: vec![AssignmentTarget {
            kind: AssignmentTargetKind::Rest,
            scheme: "https".into(),
            host: "deferred.example".into(),
            port: 3001,
            subject_public_key: None,
        }],
        checks: vec![CheckKind::Chain],
        scheduled_at,
        not_after: None,
        request_units: 1,
        detailed_owner: true,
    }
}

#[test]
#[ignore = "capacity gate: run once at PROBE-ASSIGNMENT completion"]
fn decodes_the_350_node_grouped_horizon_within_the_probe_memory_budget() {
    const ASSIGNMENT_COUNT: usize = 350 * 144;
    let fetched_at = instant(12, 1);
    let generated_at = fetched_at - Duration::minutes(1);
    let targets = vec![
        AssignmentTarget {
            kind: AssignmentTargetKind::Rest,
            scheme: "https".into(),
            host: "capacity-node.example".into(),
            port: 3001,
            subject_public_key: None,
        },
        AssignmentTarget {
            kind: AssignmentTargetKind::PeerTcp,
            scheme: "tcp".into(),
            host: "capacity-node.example".into(),
            port: 7900,
            subject_public_key: None,
        },
    ];
    let assignments = (0..ASSIGNMENT_COUNT)
        .map(|index| Assignment {
            job_id: format!("{index:064x}"),
            node_id: format!("capacity-node-{}", index % 350),
            network: Network::SymbolMainnet,
            node_kind: NodeKind::Api,
            targets: targets.clone(),
            checks: vec![CheckKind::Availability, CheckKind::PeerTcp],
            scheduled_at: generated_at + Duration::minutes(1),
            not_after: None,
            request_units: 2,
            detailed_owner: index % 2 == 0,
        })
        .collect();
    let source = AssignmentEnvelope {
        schema_version: 1,
        generated_at,
        expires_at: generated_at + Duration::hours(24),
        probe_id: "probe-a".into(),
        site: Site::A,
        epoch: u64::try_from(generated_at.timestamp()).unwrap() / 600,
        assignments,
    };
    let bytes = serde_json::to_vec(&source).unwrap();
    drop(source);
    assert!(bytes.len() <= 64 * 1024 * 1024);

    let decoded = decode_assignment_envelope(&bytes, "probe-a", Site::A, fetched_at).unwrap();
    assert_eq!(decoded.assignments.len(), ASSIGNMENT_COUNT);
    std::hint::black_box(&decoded);
    std::thread::sleep(StdDuration::from_secs(1));
    eprintln!(
        "capacity payload bytes: {}, decoded assignments: {}",
        bytes.len(),
        decoded.assignments.len()
    );
}

fn assignment_client(base_url: &str, max_bytes: usize) -> AssignmentClient {
    let http = reqwest::Client::builder()
        .redirect(Policy::none())
        .connect_timeout(StdDuration::from_secs(1))
        .timeout(StdDuration::from_secs(2))
        .build()
        .unwrap();
    AssignmentClient::new(http, base_url, "secret", "probe-a", Site::A)
        .unwrap()
        .with_max_response_bytes(max_bytes)
}

async fn one_shot_http(body: Vec<u8>, declared_length: Option<usize>) -> String {
    delayed_one_shot_http_with_length(body, declared_length, StdDuration::ZERO).await
}

async fn delayed_one_shot_http(body: Vec<u8>, delay: StdDuration) -> String {
    delayed_one_shot_http_with_length(body, None, delay).await
}

async fn delayed_one_shot_http_with_length(
    body: Vec<u8>,
    declared_length: Option<usize>,
    delay: StdDuration,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![0_u8; 8 * 1024];
        let read = socket.read(&mut request).await.unwrap();
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with(
            "GET /internal/v1/assignments?probeId=probe-a&site=a&schemaVersion=2 HTTP/1.1"
        ));
        assert!(request
            .to_ascii_lowercase()
            .contains("authorization: bearer secret"));
        tokio::time::sleep(delay).await;
        let length = declared_length.unwrap_or(body.len());
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n"
        );
        socket.write_all(header.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
    });
    format!("http://{address}/")
}

async fn retry_one_shot_http(body: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![0_u8; 8 * 1024];
        let read = socket.read(&mut request).await.unwrap();
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with(
            "GET /internal/v1/assignments?probeId=probe-a&site=a&schemaVersion=2&retryOnly=true HTTP/1.1"
        ));
        assert!(request
            .to_ascii_lowercase()
            .contains("authorization: bearer secret"));
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(header.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
    });
    format!("http://{address}/")
}

fn envelope_json(fetched_at: DateTime<Utc>) -> Vec<u8> {
    envelope_json_for_generated_at(fetched_at - Duration::minutes(1))
}

fn envelope_json_for_generated_at(generated_at: DateTime<Utc>) -> Vec<u8> {
    let epoch = generated_at.timestamp() / 600;
    let epoch_start = DateTime::from_timestamp(epoch * 600, 0).unwrap();
    serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 1,
        "generatedAt": generated_at,
        "expiresAt": epoch_start + Duration::hours(24),
        "probeId": "probe-a",
        "site": "a",
        "epoch": epoch,
        "assignments": [{
            "jobId": "a".repeat(64),
            "nodeId": "node-1",
            "network": "symbol_mainnet",
            "nodeKind": "api",
            "targets": [{
                "kind": "rest",
                "scheme": "https",
                "host": "node.example",
                "port": 3001
            }],
            "checks": ["availability"],
            "scheduledAt": epoch_start + Duration::minutes(1),
            "requestUnits": 1,
            "detailedOwner": true
        }]
    }))
    .unwrap()
}

fn instant(hour: u32, minute: u32) -> DateTime<Utc> {
    let day = if hour >= 24 { 30 } else { 29 };
    let hour = hour % 24;
    Utc.with_ymd_and_hms(2026, 7, day, hour, minute, 0).unwrap()
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}?mode=rwc", path.display()).replace('\\', "/")
}
