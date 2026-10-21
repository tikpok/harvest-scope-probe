use chrono::{Duration, TimeZone, Utc};
use monitor_core::{
    model::Severity,
    reference::{median, FinalizationTracker, ReferenceQuorum},
};

#[test]
fn computes_a_reference_median_and_requires_a_majority() {
    assert_eq!(median(&[100, 102, 900]), Some(102));
    assert_eq!(median(&[100, 102]), None);
    assert!(ReferenceQuorum::new(5, 3).is_usable());
    assert!(!ReferenceQuorum::new(5, 2).is_usable());
}

#[test]
fn detects_individual_finalization_stall_at_thirty_and_sixty_minutes() {
    let start = Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap();
    let mut tracker = FinalizationTracker::default();
    assert_eq!(
        tracker.observe("node-1", 1_000, 990, 1_000, 990, start),
        None
    );
    assert_eq!(
        tracker.observe(
            "node-1",
            1_021,
            990,
            1_021,
            991,
            start + Duration::minutes(31)
        ),
        Some(Severity::Warning)
    );
    assert_eq!(
        tracker.observe(
            "node-1",
            1_042,
            990,
            1_042,
            992,
            start + Duration::minutes(61)
        ),
        Some(Severity::Critical)
    );
}

#[test]
fn suppresses_node_findings_when_reference_majority_stalls() {
    let quorum = ReferenceQuorum::new(5, 4);
    assert!(quorum.majority_stalled(&[true, true, true, false, false]));
    assert!(!quorum.majority_stalled(&[true, true, false, false, false]));
}
