use chrono::{Duration, TimeZone, Utc};
use monitor_core::{
    model::Severity,
    reference::{FinalizationTracker, ReferenceQuorum, median},
};

#[test]
fn computes_a_reference_median_and_requires_a_majority() {
    assert_eq!(median(&[100, 102, 900]), Some(102));
    assert_eq!(median(&[100, 102]), None);
    assert!(ReferenceQuorum::new(5, 3).is_usable());
    assert!(!ReferenceQuorum::new(5, 2).is_usable());
}

#[test]
fn finalization_tracker_uses_block_gaps_without_waiting_for_minutes() {
    let start = Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap();
    let mut tracker = FinalizationTracker::default();
    for (second, gap, severity) in [
        (0, 59, None),
        (1, 60, Some(Severity::Warning)),
        (2, 119, Some(Severity::Warning)),
        (3, 120, Some(Severity::Critical)),
    ] {
        assert_eq!(
            tracker.observe(
                "node-1",
                1_000,
                900,
                1_100,
                900 + gap,
                start + Duration::seconds(second)
            ),
            severity
        );
    }
    assert_eq!(
        tracker.observe("new-node", 1_000, 900, 1_100, 1_020, start),
        Some(Severity::Critical),
        "a first valid sample already contains its block gap"
    );
}

#[test]
fn elapsed_time_does_not_create_finalization_severity() {
    let start = Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap();
    let mut tracker = FinalizationTracker::default();
    for minute in [0, 30, 60, 90] {
        assert_eq!(
            tracker.observe(
                "node-1",
                1_000 + minute,
                900,
                1_100 + minute,
                901 + minute / 30,
                start + Duration::minutes(minute as i64)
            ),
            None
        );
    }
}

#[test]
fn finalization_tracker_rejects_invalid_and_discontinuous_samples() {
    let start = Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap();
    let mut tracker = FinalizationTracker::default();
    assert_eq!(
        tracker.observe("node-1", 1_000, 900, 1_100, 1_020, start),
        Some(Severity::Critical)
    );
    assert_eq!(
        tracker.observe(
            "node-1",
            1_000,
            900,
            1_100,
            1_020,
            start - Duration::seconds(1)
        ),
        None
    );
    assert_eq!(
        tracker.observe(
            "node-1",
            1_000,
            900,
            1_100,
            1_020,
            start + Duration::minutes(30)
        ),
        Some(Severity::Critical)
    );
    assert_eq!(
        tracker.observe(
            "node-1",
            1_000,
            900,
            1_100,
            1_020,
            start + Duration::minutes(60) + Duration::seconds(1)
        ),
        None
    );
    assert_eq!(
        tracker.observe(
            "node-1",
            1_000,
            900,
            1_100,
            1_020,
            start + Duration::minutes(60) + Duration::seconds(2)
        ),
        Some(Severity::Critical)
    );
    for (height, finalized, reference_height, reference_finalized) in [
        (0, 900, 1_100, 1_020),
        (1_000, 0, 1_100, 1_020),
        (899, 900, 1_100, 1_020),
        (1_000, 900, 0, 1_020),
        (1_000, 900, 1_100, 0),
        (1_000, 900, 1_019, 1_020),
    ] {
        assert_eq!(
            tracker.observe(
                "invalid",
                height,
                finalized,
                reference_height,
                reference_finalized,
                start
            ),
            None
        );
    }
}

#[test]
fn suppresses_node_findings_when_reference_majority_stalls() {
    let quorum = ReferenceQuorum::new(5, 4);
    assert!(quorum.majority_stalled(&[true, true, true, false, false]));
    assert!(!quorum.majority_stalled(&[true, true, false, false, false]));
}
