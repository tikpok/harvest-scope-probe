use std::path::Path;

use monitor_probe::spool::{Spool, SpoolError, SpoolPressure};
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;

#[tokio::test]
async fn central_outage_restart_and_duplicate_replay_preserve_each_job_once() {
    let directory = TempDir::new().unwrap();
    let url = sqlite_url(&directory.path().join("spool.db"));
    let spool = Spool::connect(&url, 100 * 1024 * 1024).await.unwrap();
    spool.enqueue("job-1", br#"{"ok":true}"#).await.unwrap();
    spool.enqueue("job-1", br#"{"ok":false}"#).await.unwrap();
    spool.enqueue("job-2", br#"{}"#).await.unwrap();

    drop(spool); // Central is unavailable, so nothing is acknowledged before restart.

    let spool = Spool::connect(&url, 100 * 1024 * 1024).await.unwrap();
    let pending = spool.pending(10).await.unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].job_id, "job-1");
    assert_eq!(pending[0].payload, br#"{"ok":true}"#);

    spool.ack(&["job-1"]).await.unwrap();
    let pending = spool.pending(10).await.unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|item| item.job_id.as_str())
            .collect::<Vec<_>>(),
        ["job-2"]
    );
    assert_eq!(spool.stats().await.unwrap().queue_depth, 1);
}

#[tokio::test]
async fn pressure_reports_normal_warning_and_full_at_frozen_boundaries() {
    let spool = Spool::in_memory(100).await.unwrap();

    spool.enqueue("normal", &[0; 79]).await.unwrap();
    assert_eq!(
        spool.stats().await.unwrap().spool_pressure,
        SpoolPressure::Normal
    );

    spool.enqueue("warning", &[0]).await.unwrap();
    let stats = spool.stats().await.unwrap();
    assert_eq!(stats.queue_depth, 2);
    assert_eq!(stats.spool_bytes, 80);
    assert_eq!(stats.spool_pressure, SpoolPressure::Warning);

    spool.enqueue("full", &[0; 20]).await.unwrap();
    assert_eq!(
        spool.stats().await.unwrap().spool_pressure,
        SpoolPressure::Full
    );
    assert_eq!(
        serde_json::to_string(&SpoolPressure::Warning).unwrap(),
        "\"warning\""
    );
}

#[tokio::test]
async fn full_spool_rejects_new_write_without_evicting_older_observations() {
    let spool = Spool::in_memory(10).await.unwrap();
    spool.enqueue("oldest", &[1; 6]).await.unwrap();

    let error = spool.enqueue("new", &[2; 5]).await.unwrap_err();
    assert!(matches!(
        error,
        SpoolError::CapacityExceeded {
            current_bytes: 6,
            payload_bytes: 5,
            max_bytes: 10
        }
    ));
    let pending = spool.pending(10).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].job_id, "oldest");
    assert_eq!(pending[0].payload, [1; 6]);
    assert_eq!(
        spool.stats().await.unwrap().spool_pressure,
        SpoolPressure::Full,
        "a rejected capacity write must be reported until a later write succeeds"
    );

    spool.ack(&["oldest"]).await.unwrap();
    assert_eq!(
        spool.stats().await.unwrap().spool_pressure,
        SpoolPressure::Full,
        "acknowledging older data must not erase the rejected-write signal"
    );
    spool.enqueue("recovered", &[3; 4]).await.unwrap();
    let stats = spool.stats().await.unwrap();
    assert_eq!(stats.spool_bytes, 4);
    assert_eq!(stats.spool_pressure, SpoolPressure::Normal);
}

#[tokio::test]
async fn sqlite_write_failure_is_atomic_and_reports_full_pressure() {
    let directory = TempDir::new().unwrap();
    let url = sqlite_url(&directory.path().join("spool.db"));
    let spool = Spool::connect(&url, 100).await.unwrap();
    spool.enqueue("old", &[1; 10]).await.unwrap();

    let control = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_spool_state_update
         BEFORE UPDATE ON spool_state
         BEGIN
           SELECT RAISE(ABORT, 'simulated partial write');
         END",
    )
    .execute(&control)
    .await
    .unwrap();

    assert!(matches!(
        spool.enqueue("partial", &[2; 5]).await,
        Err(SpoolError::Database(_))
    ));
    let pending = spool.pending(10).await.unwrap();
    assert_eq!(pending.len(), 1, "the row insert must roll back with state");
    assert_eq!(pending[0].job_id, "old");
    let stats = spool.stats().await.unwrap();
    assert_eq!(stats.queue_depth, 1);
    assert_eq!(stats.spool_bytes, 10);
    assert_eq!(stats.spool_pressure, SpoolPressure::Full);

    sqlx::query("DROP TRIGGER reject_spool_state_update")
        .execute(&control)
        .await
        .unwrap();
    spool.enqueue("recovered", &[3; 5]).await.unwrap();
    assert_eq!(
        spool.stats().await.unwrap().spool_pressure,
        SpoolPressure::Normal
    );
}

#[tokio::test]
async fn acknowledgement_failure_rolls_back_every_delete() {
    let directory = TempDir::new().unwrap();
    let url = sqlite_url(&directory.path().join("spool.db"));
    let spool = Spool::connect(&url, 100).await.unwrap();
    spool.enqueue("job-1", &[1; 10]).await.unwrap();
    spool.enqueue("job-2", &[2; 20]).await.unwrap();

    let control = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_second_ack
         BEFORE DELETE ON spool
         WHEN OLD.job_id = 'job-2'
         BEGIN
           SELECT RAISE(ABORT, 'simulated acknowledgement failure');
         END",
    )
    .execute(&control)
    .await
    .unwrap();

    assert!(matches!(
        spool.ack(&["job-1", "job-2"]).await,
        Err(SpoolError::Database(_))
    ));
    let pending = spool.pending(10).await.unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|item| item.job_id.as_str())
            .collect::<Vec<_>>(),
        ["job-1", "job-2"]
    );
    let stats = spool.stats().await.unwrap();
    assert_eq!(stats.queue_depth, 2);
    assert_eq!(stats.spool_bytes, 30);
    assert_eq!(stats.spool_pressure, SpoolPressure::Full);
}

#[tokio::test]
async fn restart_reconciles_spool_state_from_durable_rows() {
    let directory = TempDir::new().unwrap();
    let url = sqlite_url(&directory.path().join("spool.db"));
    let spool = Spool::connect(&url, 100).await.unwrap();
    spool.enqueue("job-1", &[1; 30]).await.unwrap();
    drop(spool);

    let control = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query("UPDATE spool_state SET queue_depth = 99, payload_bytes = 99 WHERE singleton = 1")
        .execute(&control)
        .await
        .unwrap();
    control.close().await;

    let spool = Spool::connect(&url, 100).await.unwrap();
    let stats = spool.stats().await.unwrap();
    assert_eq!(stats.queue_depth, 1);
    assert_eq!(stats.spool_bytes, 30);
    assert_eq!(stats.spool_pressure, SpoolPressure::Normal);
}

#[tokio::test]
#[ignore = "capacity gate: run once at SPOOL-RESILIENCE completion"]
async fn one_hundred_mib_capacity_reports_real_warning_and_full_boundaries() {
    const MIB: usize = 1024 * 1024;
    let directory = TempDir::new().unwrap();
    let url = sqlite_url(&directory.path().join("spool.db"));
    let spool = Spool::connect(&url, 100 * MIB).await.unwrap();

    spool.enqueue("warning", &vec![0; 80 * MIB]).await.unwrap();
    let warning = spool.stats().await.unwrap();
    assert_eq!(warning.spool_bytes, (80 * MIB) as u64);
    assert_eq!(warning.spool_pressure, SpoolPressure::Warning);

    spool.enqueue("full", &vec![0; 20 * MIB]).await.unwrap();
    let full = spool.stats().await.unwrap();
    assert_eq!(full.queue_depth, 2);
    assert_eq!(full.spool_bytes, (100 * MIB) as u64);
    assert_eq!(full.spool_pressure, SpoolPressure::Full);
    assert!(matches!(
        spool.enqueue("rejected", &[1]).await,
        Err(SpoolError::CapacityExceeded { .. })
    ));
    assert_eq!(spool.stats().await.unwrap().queue_depth, 2);
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}?mode=rwc", path.display()).replace('\\', "/")
}
