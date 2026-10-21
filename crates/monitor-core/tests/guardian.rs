use chrono::{Duration, TimeZone, Utc};
use monitor_core::{
    guardian::*,
    model::{Network, Site},
};
fn now() -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(1_800_000_000, 0).unwrap()
}
fn pair(a: GuardianEvidence, b: GuardianEvidence) -> GuardianEvaluation {
    evaluate_pair(
        &GuardianObservation {
            site: Site::A,
            observed_at: now(),
            evidence: a,
        },
        &GuardianObservation {
            site: Site::B,
            observed_at: now(),
            evidence: b,
        },
        now(),
    )
}
fn identity(expected: char, observed: char) -> GuardianEvidence {
    GuardianEvidence::Identity {
        expected: Some(expected.to_string().repeat(64)),
        observed: Some(observed.to_string().repeat(64)),
    }
}
#[test]
fn identities_require_matching_expected_keys_at_both_sites() {
    assert_eq!(
        pair(identity('a', 'b'), identity('a', 'c')).disposition,
        GuardianDisposition::Confirmed
    );
    assert_eq!(
        pair(identity('a', 'b'), identity('c', 'b')).disposition,
        GuardianDisposition::Pending
    );
    assert_eq!(
        pair(identity('a', 'b'), identity('a', 'a')).disposition,
        GuardianDisposition::Pending
    );
}
#[test]
fn state_hashes_require_exact_common_height_and_valid_hashes() {
    let hash = |height, value: &str| GuardianEvidence::StateHash {
        comparison_height: Some(height),
        hash: Some(value.repeat(64)),
    };
    assert_eq!(
        pair(hash(99, "a"), hash(99, "b")).disposition,
        GuardianDisposition::Confirmed
    );
    assert_eq!(
        pair(hash(99, "a"), hash(100, "b")).disposition,
        GuardianDisposition::Pending
    );
    assert_eq!(
        pair(hash(99, "z"), hash(99, "b")).disposition,
        GuardianDisposition::Pending
    );
}
#[test]
fn confirmation_requires_current_distinct_sites_within_twelve_minutes() {
    let a = GuardianObservation {
        site: Site::A,
        observed_at: now() - Duration::minutes(12),
        evidence: identity('a', 'b'),
    };
    let mut b = GuardianObservation {
        site: Site::B,
        observed_at: now(),
        evidence: identity('a', 'b'),
    };
    assert_eq!(
        evaluate_pair(&a, &b, now()).disposition,
        GuardianDisposition::Confirmed
    );
    b.observed_at += Duration::seconds(1);
    assert_eq!(
        evaluate_pair(&a, &b, b.observed_at).disposition,
        GuardianDisposition::Pending
    );
    b.site = Site::A;
    assert_eq!(
        evaluate_pair(&a, &b, now()).disposition,
        GuardianDisposition::Pending
    );
}
#[test]
fn chain_reference_requires_three_values_and_inclusive_ten_block_boundary() {
    let chain = |height, refs| GuardianEvidence::ChainHeight {
        height: Some(height),
        reference_heights: refs,
    };
    assert_eq!(
        pair(
            chain(110, [Some(99), Some(100), Some(101)]),
            chain(110, [Some(99), Some(100), Some(101)])
        )
        .disposition,
        GuardianDisposition::Confirmed
    );
    assert_eq!(
        pair(
            chain(109, [Some(99), Some(100), Some(101)]),
            chain(109, [Some(99), Some(100), Some(101)])
        )
        .disposition,
        GuardianDisposition::Clear
    );
    assert_eq!(
        pair(
            chain(110, [None, Some(100), Some(101)]),
            chain(110, [Some(99), Some(100), Some(101)])
        )
        .disposition,
        GuardianDisposition::Pending
    );
}
#[test]
fn baseline_uses_per_node_median_then_nearest_rank_p99_and_review_boundary() {
    let active: Vec<_> = (0..100).map(|_| vec![10, 100, 20]).collect();
    let baseline = peer_threshold(&active, None);
    assert_eq!(baseline.threshold, Some(250));
    assert!(!baseline.operator_review_required);
    let high: Vec<_> = (0..100).map(|_| vec![100, 110, 120]).collect();
    assert_eq!(peer_threshold(&high, Some(264)).threshold, Some(330));
    assert!(peer_threshold(&high, Some(264)).operator_review_required);
    assert_eq!(peer_threshold(&active[..99], None).threshold, None);
}
#[test]
fn hard_peer_limit_survives_missing_baseline_and_review() {
    let peers = |n| GuardianEvidence::Peers {
        count: Some(n),
        adaptive_threshold: None,
        threshold_review_required: true,
    };
    let outcome = pair(peers(2000), peers(2001));
    assert_eq!(outcome.disposition, GuardianDisposition::Confirmed);
    assert_eq!(outcome.priority, GuardianPriority::High);
    assert_eq!(
        pair(peers(1999), peers(1999)).disposition,
        GuardianDisposition::Pending
    );
}
#[test]
fn registry_anomaly_and_unverified_semantics_cannot_promote() {
    let registry = |anomalous, verified| GuardianEvidence::RegistryIdentity {
        expected: Some("a".repeat(64)),
        remote: Some("b".repeat(64)),
        source_anomalous: anomalous,
        semantics_verified: verified,
        enabled: true,
    };
    assert_eq!(
        pair(registry(true, true), registry(false, true)).disposition,
        GuardianDisposition::Pending
    );
    assert_eq!(
        pair(registry(false, false), registry(false, true)).disposition,
        GuardianDisposition::Unsupported
    );
    assert_eq!(
        pair(registry(false, true), registry(false, true)).priority,
        GuardianPriority::High
    );
    assert_eq!(
        pair(
            GuardianEvidence::MetadataOnly,
            GuardianEvidence::MetadataOnly
        )
        .disposition,
        GuardianDisposition::Unsupported
    );
}
#[test]
fn admission_limits_include_proposed_event_and_dedupe_is_unambiguous() {
    assert!(promotion_allowed(9, 49));
    assert!(!promotion_allowed(10, 49));
    assert!(!promotion_allowed(0, 50));
    assert_ne!(
        dedupe_key(Network::SymbolMainnet, "x:y", "z"),
        dedupe_key(Network::SymbolMainnet, "x", "y:z")
    );
}
#[test]
fn queue_enforces_capacity_ttl_retry_cooldown_and_per_probe_concurrency() {
    let mut queue = ConfirmationQueue::default();
    assert!(queue.enqueue("a".into(), "probe-a".into(), now()));
    assert!(!queue.enqueue("a".into(), "probe-a".into(), now()));
    assert!(queue.enqueue("b".into(), "probe-a".into(), now()));
    let first = queue.start_next("probe-a", now()).unwrap();
    assert_eq!(first.key, "a");
    assert!(queue.start_next("probe-a", now()).is_none());
    queue.finish("a", false, now());
    assert_eq!(queue.start_next("probe-a", now()).unwrap().key, "b");
    queue.finish("b", true, now());
    assert!(queue
        .start_next("probe-a", now() + Duration::seconds(59))
        .is_none());
    assert_eq!(
        queue
            .start_next("probe-a", now() + Duration::seconds(60))
            .unwrap()
            .attempt,
        2
    );
    queue.finish("a", false, now() + Duration::seconds(60));
    assert!(!queue.enqueue("a".into(), "probe-a".into(), now() + Duration::minutes(30)));
    assert!(queue.enqueue(
        "a".into(),
        "probe-a".into(),
        now() + Duration::hours(1) + Duration::seconds(60)
    ));
    assert!(queue
        .start_next("probe-a", now() + Duration::hours(2))
        .is_none());
}
#[test]
fn stale_priority_demotes_without_terminal_transition() {
    assert_eq!(
        priority_after_inactivity(GuardianPriority::High, now() - Duration::days(7), now()),
        GuardianPriority::Low
    );
    assert_eq!(
        priority_after_inactivity(
            GuardianPriority::High,
            now() - Duration::days(7) + Duration::seconds(1),
            now()
        ),
        GuardianPriority::High
    );
}

#[test]
fn fractional_medians_round_only_after_threshold_multiplication() {
    let samples = vec![vec![100, 101]; 100];
    assert_eq!(peer_threshold(&samples, None).threshold, Some(302));
}

#[test]
fn queue_capacity_is_per_probe_and_expired_jobs_never_start() {
    let mut queue = ConfirmationQueue::default();
    for i in 0..100 {
        assert!(queue.enqueue(format!("a{i}"), "a".into(), now()));
    }
    assert!(!queue.enqueue("overflow".into(), "a".into(), now()));
    assert!(queue.enqueue("independent".into(), "b".into(), now()));
    assert!(queue
        .start_next("a", now() + Duration::seconds(721))
        .is_none());
    assert!(queue
        .start_next("b", now() + Duration::seconds(721))
        .is_none());
}
