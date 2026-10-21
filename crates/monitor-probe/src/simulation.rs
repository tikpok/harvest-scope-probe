use std::collections::HashMap;

use monitor_core::scheduler::ScheduledJob;

use crate::limiter::{Admission, AdmissionController, Limits};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulationReport {
    pub completed: usize,
    pub deferred: usize,
    pub unfinished: usize,
    pub finished_at_seconds: u64,
    pub max_started_in_one_second: usize,
    pub rate_bound_respected: bool,
}

#[must_use]
pub fn simulate_epoch(
    jobs: &[ScheduledJob],
    hosts: &HashMap<String, String>,
    epoch_seconds: u64,
) -> SimulationReport {
    let mut scheduled = jobs
        .iter()
        .cloned()
        .map(|job| (u64::from(job.offset_seconds), job))
        .collect::<Vec<_>>();
    scheduled.sort_by_key(|(due, job)| (*due, job.job_id.clone()));
    let mut scheduled = scheduled.into_iter().peekable();
    let mut pending = Vec::new();
    let limits = Limits::default();
    let mut limiter = AdmissionController::new(limits);
    let mut completed = 0;
    let mut deferred = 0;
    let mut cumulative = 0;
    let mut max_started = 0;
    let mut rate_bound_respected = true;
    let mut finished_at = 0;

    for second in 0..epoch_seconds {
        while scheduled.peek().is_some_and(|(due, _)| *due <= second) {
            if let Some((_, job)) = scheduled.next() {
                pending.push(job);
            }
        }

        let mut started_this_second = 0;
        let mut index = 0;
        while index < pending.len() {
            let job = &pending[index];
            let host = hosts
                .get(&job.node_id)
                .map_or(job.node_id.as_str(), String::as_str);
            match limiter.admit(host, second, job.request_units) {
                Admission::Granted => {
                    limiter.complete(host);
                    pending.swap_remove(index);
                    completed += 1;
                    started_this_second += 1;
                    finished_at = second;
                }
                Admission::BudgetExceeded | Admission::PressurePaused => {
                    pending.swap_remove(index);
                    deferred += 1;
                }
                Admission::RateLimited => break,
                Admission::HostBusy | Admission::ConcurrencyLimited => index += 1,
            }
        }
        cumulative += started_this_second;
        max_started = max_started.max(started_this_second);
        let token_ceiling =
            (second as f64 * limits.global_requests_per_second + limits.burst).floor() as usize;
        if cumulative > token_ceiling {
            rate_bound_respected = false;
        }
        if pending.is_empty() && scheduled.peek().is_none() {
            break;
        }
    }

    SimulationReport {
        completed,
        deferred,
        unfinished: pending.len() + scheduled.count(),
        finished_at_seconds: finished_at,
        max_started_in_one_second: max_started,
        rate_bound_respected,
    }
}
