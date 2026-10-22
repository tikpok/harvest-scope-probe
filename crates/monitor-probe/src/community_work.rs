//! Local workers keep measuring while the coordinator waits for Central.
use std::{future::Future, time::Duration};

use tokio::{task::{JoinError, JoinSet}, time::{Interval, MissedTickBehavior}};

pub(crate) enum WorkEvent<T> {
    Completed(T),
    Poll,
}

pub(crate) struct WorkQueue<T> {
    jobs: JoinSet<T>,
    poll: Interval,
}

impl<T: 'static> WorkQueue<T> {
    pub fn new() -> Self {
        let mut poll = tokio::time::interval(Duration::from_secs(15));
        poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self { jobs: JoinSet::new(), poll }
    }

    pub fn len(&self) -> usize { self.jobs.len() }

    pub fn push(&mut self, job: impl Future<Output = T> + 'static) {
        self.jobs.spawn_local(job);
    }

    pub async fn next_event(&mut self) -> Result<WorkEvent<T>, JoinError> {
        tokio::select! {
            Some(completed) = self.jobs.join_next(), if !self.jobs.is_empty() => {
                self.poll.reset_immediately();
                completed.map(WorkEvent::Completed)
            },
            _ = self.poll.tick() => Ok(WorkEvent::Poll),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, rc::Rc};
    use tokio::time::Instant;

    #[tokio::test(start_paused = true)]
    async fn measuring_progresses_during_central_io_and_completion_refills_immediately() {
        tokio::task::LocalSet::new().run_until(async {
            let mut work = WorkQueue::new();
            assert!(matches!(work.next_event().await.unwrap(), WorkEvent::Poll));
            let completed = Rc::new(Cell::new(false));
            let observed = completed.clone();
            work.push(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                observed.set(true);
                7
            });
            // The coordinator is awaiting Central, rather than polling job futures.
            tokio::time::sleep(Duration::from_secs(2)).await;
            assert!(completed.get(), "measurement must progress during Central I/O");
            assert!(matches!(work.next_event().await.unwrap(), WorkEvent::Completed(7)));
            let at = Instant::now();
            assert!(matches!(work.next_event().await.unwrap(), WorkEvent::Poll));
            assert_eq!(Instant::now(), at, "a completed slot must not wait for the 15s idle poll");
        }).await;
    }

    #[tokio::test(start_paused = true)]
    async fn two_default_workers_each_finish_one_thousand_jobs_within_ten_minutes() {
        async fn observer() {
            let mut work = WorkQueue::new();
            let limit = usize::from(monitor_core::community::CapacityLimits::default().max_concurrency);
            let (mut issued, mut received) = (0, 0);
            let started = Instant::now();
            while received < 1000 {
                match work.next_event().await.unwrap() {
                    WorkEvent::Completed(()) => received += 1,
                    WorkEvent::Poll => {
                        // Model acknowledgement/heartbeat/lease round trips, without external I/O.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        while work.len() < limit && issued < 1000 {
                            work.push(async { tokio::time::sleep(Duration::from_millis(1500)).await; });
                            issued += 1;
                        }
                    }
                }
                assert!(work.len() <= limit);
            }
            assert!(started.elapsed() < Duration::from_secs(600), "15s polling must not cap healthy default workers");
        }
        tokio::task::LocalSet::new().run_until(async {
            tokio::join!(observer(), observer());
        }).await;
    }

    #[tokio::test(start_paused = true)]
    async fn stopping_drops_workers_and_idle_poll_remains_bounded() {
        tokio::task::LocalSet::new().run_until(async {
            let completed = Rc::new(Cell::new(false));
            let observed = completed.clone();
            let mut work = WorkQueue::new();
            assert!(matches!(work.next_event().await.unwrap(), WorkEvent::Poll));
            let at = Instant::now();
            assert!(matches!(work.next_event().await.unwrap(), WorkEvent::Poll));
            assert_eq!(Instant::now() - at, Duration::from_secs(15));
            work.push(async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                observed.set(true);
            });
            tokio::task::yield_now().await;
            drop(work);
            tokio::time::sleep(Duration::from_secs(20)).await;
            assert!(!completed.get(), "stopped workers must not continue measuring");
        }).await;
    }
}
