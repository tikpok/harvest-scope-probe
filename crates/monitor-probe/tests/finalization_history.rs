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
async fn counts_chain_blocks_without_requiring_reference_finalization_progress() {
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
                sample(1_019, 900, 950),
                started + Duration::minutes(29),
            )
            .await
            .unwrap(),
        FinalizationAssessment::StalledBlocks(19)
    );
    assert_eq!(
        history
            .evaluate(
                "node-a",
                sample(1_020, 900, 950),
                started + Duration::minutes(30),
            )
            .await
            .unwrap(),
        FinalizationAssessment::StalledBlocks(20)
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

fn nodes() -> (monitor_core::model::Node, Vec<monitor_core::model::Node>) {
    use monitor_core::model::{Network, Node, NodeKind};
    (
        Node::new(
            "target",
            Network::SymbolMainnet,
            NodeKind::Api,
            "target.example",
        ),
        vec![Node::new(
            "reference",
            Network::SymbolMainnet,
            NodeKind::Api,
            "reference.example",
        )],
    )
}

#[tokio::test]
async fn wall_clock_time_does_not_add_blocks_and_first_chain_block_counts() {
    let history = FinalizationHistory::in_memory().await.unwrap();
    let started = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();
    for (minutes, chain, expected) in [
        (0, 1_000, FinalizationAssessment::InsufficientHistory),
        (30, 1_000, FinalizationAssessment::InsufficientHistory),
        (60, 1_000, FinalizationAssessment::InsufficientHistory),
        (61, 1_001, FinalizationAssessment::StalledBlocks(1)),
    ] {
        assert_eq!(
            history
                .evaluate(
                    "node-a",
                    sample(chain, 900, 950),
                    started + Duration::minutes(minutes)
                )
                .await
                .unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn ordinary_probe_history_resets_after_more_than_thirty_minutes_without_samples() {
    let history = FinalizationHistory::in_memory().await.unwrap();
    let started = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();
    for (seconds, chain, expected) in [
        (0, 1_000, FinalizationAssessment::InsufficientHistory),
        (1800, 1_060, FinalizationAssessment::StalledBlocks(60)),
        (3601, 1_120, FinalizationAssessment::InsufficientHistory),
        (3602, 1_121, FinalizationAssessment::StalledBlocks(1)),
    ] {
        assert_eq!(
            history
                .evaluate(
                    "node-a",
                    sample(chain, 900, 950),
                    started + Duration::seconds(seconds)
                )
                .await
                .unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn ordinary_probe_context_resets_endpoint_changes_and_preserves_central_context() {
    let history = FinalizationHistory::in_memory().await.unwrap();
    let (target, mut references) = nodes();
    let started = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();
    let key = "https://reference.example:3001";
    let probe = history.for_probe_context(&target, &references);
    probe
        .evaluate(key, sample(1_000, 900, 950), started)
        .await
        .unwrap();
    assert_eq!(
        probe
            .evaluate(key, sample(1_060, 900, 950), started + Duration::seconds(1))
            .await
            .unwrap(),
        FinalizationAssessment::StalledBlocks(60)
    );
    references[0].host = "new-reference.example".into();
    let changed = history.for_probe_context(&target, &references);
    assert_eq!(
        changed
            .evaluate(
                "https://new-reference.example:3001",
                sample(1_120, 900, 950),
                started + Duration::seconds(2)
            )
            .await
            .unwrap(),
        FinalizationAssessment::InsufficientHistory
    );
    references[0].host = "reference.example".into();
    let restored = history.for_probe_context(&target, &references);
    assert_eq!(
        restored
            .evaluate(key, sample(1_180, 900, 950), started + Duration::seconds(3))
            .await
            .unwrap(),
        FinalizationAssessment::InsufficientHistory
    );
    let central = history.for_verifier_context("authority-a", &target, &references);
    central
        .evaluate(key, sample(1_180, 900, 950), started + Duration::seconds(4))
        .await
        .unwrap();
    let retained = central.for_probe_context(&target, &references);
    assert_eq!(
        retained
            .evaluate(key, sample(1_240, 900, 950), started + Duration::seconds(5))
            .await
            .unwrap(),
        FinalizationAssessment::StalledBlocks(60)
    );
}

#[tokio::test]
async fn a_clock_regression_does_not_replace_an_authority_baseline() {
    let history = FinalizationHistory::in_memory().await.unwrap();
    let (target, references) = nodes();
    let started = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();
    let original = history.for_verifier_context("authority-a", &target, &references);
    original
        .evaluate("target", sample(1_000, 900, 950), started)
        .await
        .unwrap();
    let other = history.for_verifier_context("authority-b", &target, &references);
    assert_eq!(
        other
            .evaluate(
                "target",
                sample(1_100, 901, 951),
                started - Duration::seconds(1)
            )
            .await
            .unwrap(),
        FinalizationAssessment::InsufficientHistory
    );
    assert_eq!(
        original
            .evaluate(
                "target",
                sample(1_060, 900, 950),
                started + Duration::seconds(1)
            )
            .await
            .unwrap(),
        FinalizationAssessment::StalledBlocks(60)
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
                sample(1_020, 900, 950),
                started + Duration::minutes(30),
            )
            .await
            .unwrap(),
        FinalizationAssessment::StalledBlocks(20)
    );
}
