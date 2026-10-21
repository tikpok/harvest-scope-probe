//! Temporal confirmation for any number of authenticated observers.

use crate::community::{
    evaluate_consensus, independent_votes, CommunityVote, ConsensusStatus, Verdict,
};
use crate::model::Dimension;
use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone)]
pub struct Assessment {
    pub consensus: ConsensusStatus,
    /// Incident opening/recovery may only use confirmed evidence.
    pub confirmed: bool,
    /// Probe identities which still need their 30–60 second retry.
    pub retries: Vec<(String, DateTime<Utc>, DateTime<Utc>)>,
}

pub fn assess_temporal(
    history: &[CommunityVote],
    node_id: &str,
    dimension: Dimension,
    now: DateTime<Utc>,
    freshness: Duration,
) -> Assessment {
    let consensus = evaluate_consensus(history, node_id, dimension, now, freshness);
    let independent = independent_votes(history, node_id, dimension, now, freshness);
    let mut retries = Vec::new();
    let mut candidates = Vec::new();
    for latest in &independent {
        if latest.verdict == Some(Verdict::Healthy) {
            continue;
        }
        let mut sequence: Vec<_> = history
            .iter()
            .filter(|vote| {
                vote.observer.probe_id == latest.observer.probe_id
                    && vote.node_id == node_id
                    && vote.dimension == dimension
                    && vote.observed_at <= latest.observed_at
                    && vote.observed_at >= now - freshness
            })
            .collect();
        sequence.sort_by_key(|vote| vote.observed_at);
        let mut first: Option<&CommunityVote> = None;
        let mut candidate: Option<DateTime<Utc>> = None;
        for vote in sequence {
            if vote.verdict != latest.verdict
                || vote.observer.verified_source_ip != latest.observer.verified_source_ip
                || vote.observer.admission != latest.observer.admission
                || vote.observer.is_self_hosted
            {
                first = None;
                candidate = None;
                continue;
            }
            if candidate.is_some_and(|at| vote.observed_at - at <= Duration::minutes(12)) {
                candidate = Some(vote.observed_at);
                continue;
            }
            candidate = None;
            if let Some(previous) = first {
                let elapsed = vote.observed_at - previous.observed_at;
                if elapsed >= Duration::seconds(30) && elapsed <= Duration::seconds(60) {
                    candidate = Some(vote.observed_at);
                    first = None;
                    continue;
                }
                if elapsed < Duration::seconds(30) {
                    continue;
                }
            }
            first = Some(vote);
        }
        if let Some(at) = candidate {
            if now - at <= Duration::minutes(12) {
                candidates.push(at);
            }
        } else if let Some(first) = first {
            let before = first.observed_at + Duration::seconds(30);
            let after = first.observed_at + Duration::seconds(60);
            if now <= after {
                retries.push((latest.observer.probe_id.clone(), before, after));
            }
        }
    }
    let recent = independent
        .iter()
        .filter(|vote| now - vote.observed_at <= Duration::minutes(12))
        .count();
    let confirmed = match consensus {
        ConsensusStatus::Corroborated(Verdict::Healthy)
        | ConsensusStatus::Strong(Verdict::Healthy) => recent >= 2,
        ConsensusStatus::Corroborated(_) | ConsensusStatus::Strong(_) => {
            recent >= 2 && !candidates.is_empty()
        }
        _ => false,
    };
    Assessment {
        consensus,
        confirmed,
        retries,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::community::{Admission, Observer};
    use crate::model::Dimension;

    fn vote(id: u8, seconds: i64, verdict: Option<Verdict>) -> CommunityVote {
        CommunityVote {
            observer: Observer {
                probe_id: id.to_string(),
                admission: Admission::Active,
                verified_source_ip: Some(format!("8.8.8.{id}").parse().unwrap()),
                asn: Some(u32::from(id)),
                is_self_hosted: false,
            },
            node_id: "node".into(),
            dimension: Dimension::Connectivity,
            verdict,
            observed_at: DateTime::from_timestamp(1_700_000_000 + seconds, 0).unwrap(),
        }
    }
    fn assess(votes: &[CommunityVote], at: i64) -> Assessment {
        assess_temporal(
            votes,
            "node",
            Dimension::Connectivity,
            vote(1, at, None).observed_at,
            Duration::minutes(30),
        )
    }
    #[test]
    fn simultaneous_failures_require_a_timed_retry() {
        let a = vote(1, 0, Some(Verdict::Critical));
        let b = vote(2, 0, Some(Verdict::Critical));
        assert!(!assess(&[a.clone(), b.clone()], 0).confirmed);
        assert!(
            !assess(
                &[a.clone(), b.clone(), vote(1, 29, Some(Verdict::Critical))],
                29
            )
            .confirmed
        );
        assert!(
            assess(
                &[a.clone(), b.clone(), vote(1, 30, Some(Verdict::Critical))],
                30
            )
            .confirmed
        );
        assert!(!assess(&[a, b, vote(1, 61, Some(Verdict::Critical))], 61).confirmed);
    }
    #[test]
    fn unknown_and_contradiction_prevent_confirmation() {
        let history = vec![
            vote(1, 0, Some(Verdict::Critical)),
            vote(1, 40, Some(Verdict::Critical)),
            vote(2, 41, Some(Verdict::Critical)),
        ];
        assert!(assess(&history, 41).confirmed);
        let mut unknown = history.clone();
        unknown.push(vote(1, 42, None));
        assert!(!assess(&unknown, 42).confirmed);
        let mut conflict = history;
        conflict.push(vote(3, 42, Some(Verdict::Healthy)));
        assert_eq!(
            assess(&conflict, 42).consensus,
            ConsensusStatus::Contradictory
        );
        assert!(!assess(&conflict, 42).confirmed);
    }
    #[test]
    fn confirmation_and_recovery_keep_twelve_minute_window() {
        assert!(
            !assess(
                &[
                    vote(1, 0, Some(Verdict::Healthy)),
                    vote(2, 721, Some(Verdict::Healthy))
                ],
                721
            )
            .confirmed
        );
        assert!(
            assess(
                &[
                    vote(1, 0, Some(Verdict::Healthy)),
                    vote(2, 720, Some(Verdict::Healthy))
                ],
                720
            )
            .confirmed
        );
        assert!(
            !assess(
                &[
                    vote(1, 0, Some(Verdict::Critical)),
                    vote(1, 40, Some(Verdict::Critical)),
                    vote(2, 761, Some(Verdict::Critical))
                ],
                761
            )
            .confirmed
        );
    }
    #[test]
    fn ip_rotation_cannot_manufacture_a_retry_or_second_viewpoint() {
        let first = vote(1, 0, Some(Verdict::Critical));
        let mut moved = vote(1, 40, Some(Verdict::Critical));
        moved.observer.verified_source_ip = Some("1.1.1.1".parse().unwrap());
        assert!(!assess(&[first, moved, vote(2, 41, Some(Verdict::Critical))], 41).confirmed);
    }
    #[test]
    fn late_delivery_cannot_replace_a_newer_unknown() {
        let history = [
            vote(1, 0, Some(Verdict::Critical)),
            vote(1, 40, Some(Verdict::Critical)),
            vote(2, 41, Some(Verdict::Critical)),
            vote(1, 42, None),
            vote(1, 20, Some(Verdict::Critical)),
        ];
        assert!(!assess(&history, 42).confirmed);
    }
}
