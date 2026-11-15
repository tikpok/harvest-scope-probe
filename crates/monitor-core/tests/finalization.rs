use monitor_core::{
    community::Verdict,
    finalization::{block_verdict, network_sample, node_outcome},
    model::Outcome,
};
use serde_json::{Value, json};

fn evidence() -> Value {
    json!({
        "height": 1_000,
        "finalizedHeight": 900,
        "referenceMedianFinalizedHeight": 1_020,
        "referenceExpected": 3,
        "referenceResponding": 2,
        "referenceHosts": ["ref-a", "ref-b"],
        "referenceHeights": [1_100, 1_200],
        "networkFinalization": {
            "metric": "stalled_blocks",
            "expectedReferences": 3,
            "usableReferences": 2,
            "stalledReferences": 2,
            "stalledBlocks": 120
        }
    })
}

#[test]
fn block_thresholds_are_independent_of_duration() {
    for (blocks, verdict) in [
        (0, Verdict::Healthy),
        (59, Verdict::Healthy),
        (60, Verdict::Warning),
        (119, Verdict::Warning),
        (120, Verdict::Critical),
        (u64::MAX, Verdict::Critical),
    ] {
        assert_eq!(block_verdict(blocks), verdict);
    }
}

#[test]
fn legacy_node_results_are_derived_from_reference_finalized_height() {
    for outcome in [
        Outcome::Healthy,
        Outcome::FinalizationStalledMinutes(0),
        Outcome::FinalizationStalledMinutes(u64::MAX),
        Outcome::LagBlocks(120),
    ] {
        assert_eq!(node_outcome(&outcome, &evidence()), Outcome::LagBlocks(120));
    }
    let mut sample = evidence();
    sample["referenceMedianFinalizedHeight"] = json!(800);
    assert_eq!(
        node_outcome(&Outcome::Healthy, &sample),
        Outcome::LagBlocks(0)
    );
    for outcome in [
        Outcome::Failure("failed".into()),
        Outcome::Unknown("unknown".into()),
        Outcome::Unsupported("unsupported".into()),
    ] {
        assert_eq!(node_outcome(&outcome, &Value::Null), outcome);
    }
}

#[test]
fn node_finalization_rejects_missing_invalid_or_contradictory_evidence() {
    for field in [
        "height",
        "finalizedHeight",
        "referenceMedianFinalizedHeight",
        "referenceExpected",
        "referenceResponding",
        "referenceHosts",
        "referenceHeights",
    ] {
        let mut sample = evidence();
        sample.as_object_mut().unwrap().remove(field);
        assert!(
            matches!(
                node_outcome(&Outcome::Healthy, &sample),
                Outcome::Unknown(_)
            ),
            "{field}"
        );
    }
    for (field, value) in [
        ("height", json!(0)),
        ("height", json!(899)),
        ("finalizedHeight", json!(0)),
        ("referenceMedianFinalizedHeight", json!(0)),
        ("referenceMedianFinalizedHeight", json!(1_151)),
        ("referenceExpected", json!(0)),
        ("referenceResponding", json!(1)),
        ("referenceResponding", json!(4)),
        ("referenceHosts", json!(["ref-a", "REF-A"])),
        ("referenceHosts", json!(["ref-a", "REF-A."])),
        ("referenceHosts", json!(["ref-a", " "])),
        ("referenceHosts", json!(["ref-a"])),
        ("referenceHeights", json!([0, 1_200])),
        ("referenceHeights", json!([1_100])),
        ("referenceHeights", json!(["1100", 1_200])),
    ] {
        let mut sample = evidence();
        sample[field] = value;
        assert!(
            matches!(
                node_outcome(&Outcome::Healthy, &sample),
                Outcome::Unknown(_)
            ),
            "{field}: {sample}"
        );
    }
    assert!(matches!(
        node_outcome(&Outcome::LagBlocks(119), &evidence()),
        Outcome::Unknown(_)
    ));
    assert!(matches!(
        node_outcome(&Outcome::Remaining(1), &evidence()),
        Outcome::Unknown(_)
    ));
}

#[test]
fn shared_node_evidence_accepts_legacy_reference_set_sizes() {
    for count in [1, 5] {
        let mut sample = evidence();
        sample["referenceExpected"] = json!(count);
        sample["referenceResponding"] = json!(count);
        sample["referenceHosts"] =
            json!((0..count).map(|n| format!("ref-{n}")).collect::<Vec<_>>());
        sample["referenceHeights"] = json!(vec![1_100; count]);
        assert_eq!(
            node_outcome(&Outcome::Healthy, &sample),
            Outcome::LagBlocks(120)
        );
    }
}

#[test]
fn network_finalization_requires_explicit_block_metric_and_leased_quorum() {
    let sample = network_sample(&evidence(), 3).unwrap();
    assert_eq!(sample.expected_references, 3);
    assert_eq!(sample.usable_references, 2);
    assert_eq!(sample.stalled_references, 2);
    assert_eq!(sample.stalled_blocks, 120);
    assert!(network_sample(&evidence(), 2).is_none());
    for field in [
        "metric",
        "expectedReferences",
        "usableReferences",
        "stalledReferences",
        "stalledBlocks",
    ] {
        let mut sample = evidence();
        sample["networkFinalization"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        sample["networkFinalization"]["stalledMinutes"] = json!(60);
        assert!(network_sample(&sample, 3).is_none(), "{field}");
    }
    for (field, value) in [
        ("metric", json!("stalled_minutes")),
        ("expectedReferences", json!(4)),
        ("usableReferences", json!(1)),
        ("usableReferences", json!(3)),
        ("stalledReferences", json!(3)),
        ("stalledBlocks", json!(-1)),
    ] {
        let mut sample = evidence();
        sample["networkFinalization"][field] = value;
        assert!(network_sample(&sample, 3).is_none(), "{field}");
    }
    for (field, value) in [
        ("referenceExpected", 0),
        ("referenceResponding", 4),
        ("referenceResponding", 1),
    ] {
        let mut sample = evidence();
        sample[field] = json!(value);
        assert!(network_sample(&sample, 3).is_none(), "{field}");
    }
}
