use chrono::{Duration, TimeZone, Utc};
use monitor_probe::history::{FinalizationAssessment, FinalizationHistory, FinalizationSample};

fn sample(
    chain_height: u64,
    finalized_height: u64,
    reference_finalized_height: u64,
) -> FinalizationSample {
    FinalizationSample {
        chain_height,
        finalized_height,
        reference_finalized_height,
    }
}

#[tokio::test]
async fn requires_local_chain_and_reference_progress_before_counting_a_stall() {
    let history = FinalizationHistory::in_memory().await.unwrap();
    let started = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();

    assert_eq!(
        history
            .evaluate("node-a", sample(1_000, 900, 950), started)
            .await
            .unwrap(),
        FinalizationAssessment::InsufficientHistory
    );
    assert_eq!(
        history
            .evaluate(
                "node-a",
                sample(1_019, 900, 951),
                started + Duration::minutes(29),
            )
            .await
            .unwrap(),
        FinalizationAssessment::InsufficientHistory
    );
    assert_eq!(
        history
            .evaluate(
                "node-a",
                sample(1_020, 900, 951),
                started + Duration::minutes(30),
            )
            .await
            .unwrap(),
        FinalizationAssessment::StalledMinutes(30)
    );
    assert_eq!(
        history
            .evaluate(
                "node-a",
                sample(1_021, 901, 952),
                started + Duration::minutes(31),
            )
            .await
            .unwrap(),
        FinalizationAssessment::Progressing
    );
}

#[tokio::test]
async fn sqlite_history_survives_probe_restart() {
    let directory = tempfile::TempDir::new().unwrap();
    let path = directory.path().join("protocol-history.db");
    let url = format!(
        "sqlite://{}?mode=rwc",
        path.to_string_lossy().replace('\\', "/")
    );
    let started = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();

    {
        let history = FinalizationHistory::connect(&url).await.unwrap();
        history
            .evaluate("node-a", sample(1_000, 900, 950), started)
            .await
            .unwrap();
    }

    let restored = FinalizationHistory::connect(&url).await.unwrap();
    assert_eq!(
        restored
            .evaluate(
                "node-a",
                sample(1_020, 900, 951),
                started + Duration::minutes(30),
            )
            .await
            .unwrap(),
        FinalizationAssessment::StalledMinutes(30)
    );
}
