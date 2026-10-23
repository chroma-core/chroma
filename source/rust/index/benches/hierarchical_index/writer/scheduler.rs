//! Producer-aware balancing jobs. An empty queue is final only after all active jobs finish.
use super::super::common::NodeId;
use parking_lot::{Condvar, Mutex};
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Job {
    Leaf(NodeId, u32),
    Internal(NodeId),
}
impl Job {
    fn key(self) -> (bool, NodeId) {
        match self {
            Self::Leaf(id, _) => (false, id),
            Self::Internal(id) => (true, id),
        }
    }
}
#[derive(Default)]
struct State {
    ready: VecDeque<Job>,
    pending: HashSet<(bool, NodeId)>,
    active: HashSet<(bool, NodeId)>,
    completed: usize,
    budget: usize,
    capped: usize,
}
#[derive(Default)]
pub(super) struct BalanceScheduler {
    state: Mutex<State>,
    changed: Condvar,
    pub leaf_jobs: AtomicU64,
    pub internal_jobs: AtomicU64,
    pub wait_nanos: AtomicU64,
    pub peak_active: AtomicU64,
}
impl BalanceScheduler {
    pub fn begin(&self, jobs: Vec<Job>, budget: usize) {
        let mut s = self.state.lock();
        assert!(s.active.is_empty());
        *s = State {
            budget,
            ..Default::default()
        };
        drop(s);
        for job in jobs {
            self.enqueue(job);
        }
    }
    pub fn enqueue(&self, job: Job) {
        let mut s = self.state.lock();
        if s.pending.insert(job.key()) {
            s.ready.push_back(job);
            self.changed.notify_all();
        } else if let Job::Leaf(_, depth) = job {
            // A later shallower request must not inherit a more restrictive NPA depth.
            if let Some(Job::Leaf(_, queued_depth)) =
                s.ready.iter_mut().find(|queued| queued.key() == job.key())
            {
                *queued_depth = (*queued_depth).min(depth);
            }
        }
    }
    pub fn next(&self) -> Option<Job> {
        let mut s = self.state.lock();
        loop {
            if s.completed >= s.budget {
                s.capped += s.ready.len();
                s.ready.clear();
                s.pending.clear();
                return None;
            }
            let count = s.ready.len();
            for _ in 0..count {
                let job = s.ready.pop_front().unwrap();
                let key = job.key();
                if s.active.contains(&key) {
                    s.ready.push_back(job);
                    continue;
                }
                s.pending.remove(&key);
                s.active.insert(key);
                self.peak_active
                    .fetch_max(s.active.len() as u64, Ordering::Relaxed);
                match job {
                    Job::Leaf(..) => &self.leaf_jobs,
                    Job::Internal(..) => &self.internal_jobs,
                }
                .fetch_add(1, Ordering::Relaxed);
                return Some(job);
            }
            if s.active.is_empty() {
                return None;
            }
            let wait = Instant::now();
            self.changed.wait(&mut s);
            self.wait_nanos
                .fetch_add(wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
    }
    pub fn run(&self, job: Job, action: impl FnOnce()) {
        struct Active<'a> {
            queue: &'a BalanceScheduler,
            job: Job,
        }
        impl Drop for Active<'_> {
            fn drop(&mut self) {
                self.queue.finish(self.job);
            }
        }
        let _active = Active { queue: self, job };
        action();
    }
    pub fn finish(&self, job: Job) {
        let mut s = self.state.lock();
        assert!(s.active.remove(&job.key()));
        s.completed += 1;
        self.changed.notify_all();
    }
    pub fn counts(&self) -> (usize, usize) {
        let s = self.state.lock();
        (s.completed, s.capped)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn waits_for_active_producer_and_runs_followup() {
        let q = std::sync::Arc::new(BalanceScheduler::default());
        q.begin(vec![Job::Leaf(1, 0)], 32);
        let first = q.next().unwrap();
        std::thread::scope(|scope| {
            let q = &q;
            let waiter = scope.spawn(move || {
                let job = q.next().unwrap();
                assert_eq!(job, Job::Internal(2));
                q.finish(job);
            });
            q.enqueue(Job::Internal(2));
            q.finish(first);
            waiter.join().unwrap();
        });
        assert_eq!(q.next(), None);
    }
    #[test]
    fn active_jobs_never_run_twice_and_attempts_are_bounded() {
        let q = BalanceScheduler::default();
        q.begin(vec![Job::Leaf(1, 0)], 8);
        for _ in 0..8 {
            let j = q.next().unwrap();
            q.enqueue(j);
            q.finish(j);
        }
        assert_eq!(q.next(), None);
        assert_eq!(q.counts(), (8, 1));
    }
    #[test]
    fn panic_releases_producer_and_allows_queue_termination() {
        let q = BalanceScheduler::default();
        q.begin(vec![Job::Leaf(1, 0)], 8);
        let job = q.next().unwrap();
        let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            q.run(job, || panic!("injected job failure"))
        }));
        assert!(error.is_err());
        assert_eq!(q.next(), None);
    }
    #[test]
    fn coalesced_leaf_requests_keep_least_restrictive_depth() {
        let q = BalanceScheduler::default();
        q.begin(vec![Job::Leaf(1, 4)], 8);
        q.enqueue(Job::Leaf(1, 2));
        assert_eq!(q.next(), Some(Job::Leaf(1, 2)));
    }
}
