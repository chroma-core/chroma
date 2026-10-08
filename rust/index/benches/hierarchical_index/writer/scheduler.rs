use super::super::common::NodeId;
use parking_lot::{Condvar, Mutex};
use std::collections::{HashMap, HashSet, VecDeque};

/// One scheduler owns both pending work and leaf claims. Claims protect destructive
/// leaf operations; ordinary posting appends retain their per-leaf locks.
/// A task can claim replacement children or a merge destination before exposing
/// those leaves to another task. Parent changes use the writer's tree lock.
pub(super) struct BalanceScheduler {
    state: Mutex<State>,
    changed: Condvar,
    budget: usize,
}

#[derive(Default)]
struct State {
    follow_ups: VecDeque<NodeId>,
    initial_scan: VecDeque<NodeId>,
    queued_follow_ups: HashSet<NodeId>,
    pending: HashSet<NodeId>,
    depths: HashMap<NodeId, u32>,
    active: HashMap<NodeId, u32>,
    owners: HashMap<NodeId, NodeId>,
    blocked_on: HashMap<NodeId, NodeId>,
    attempts: HashMap<NodeId, usize>,
    completed: usize,
    structural_attempts: usize,
    generation: usize,
    audited_generation: Option<usize>,
    discovering: bool,
    exhausted: bool,
    conflicts: usize,
    capped: HashSet<NodeId>,
}

impl BalanceScheduler {
    pub(super) fn new(leaves: Vec<NodeId>, budget: usize) -> Self {
        Self {
            state: Mutex::new(State {
                pending: leaves.iter().copied().collect(),
                initial_scan: leaves.into(),
                ..Default::default()
            }),
            changed: Condvar::new(),
            budget,
        }
    }

    pub(super) fn enqueue(&self, leaf: NodeId) {
        self.enqueue_task(leaf, None);
    }

    pub(super) fn enqueue_cascade(&self, leaf: NodeId, depth: u32) {
        self.enqueue_task(leaf, Some(depth));
    }

    fn enqueue_task(&self, leaf: NodeId, depth: Option<u32>) {
        let mut state = self.state.lock();
        if state.exhausted {
            return;
        }
        if let Some(depth) = depth {
            // A causal follow-up carries the same NPA depth as the direct
            // recursive call. Initial discovery and split-child cleanup do
            // not overwrite that depth with an unrelated depth-zero visit.
            // Coalesced requests keep the first causal request's depth.
            state.depths.entry(leaf).or_insert(depth);
        }
        let newly_pending = state.pending.insert(leaf);
        if (newly_pending || depth.is_some()) && state.queued_follow_ups.insert(leaf) {
            state.follow_ups.push_front(leaf);
            self.changed.notify_all();
        }
    }

    pub(super) fn next(&self) -> Option<NodeId> {
        self.next_with(|| {}).map(|(leaf, _)| leaf)
    }

    pub(super) fn next_with(&self, discover: impl Fn()) -> Option<(NodeId, u32)> {
        let mut state = self.state.lock();
        loop {
            if state.exhausted {
                return None;
            }
            if state.discovering {
                self.changed.wait(&mut state);
                continue;
            }
            // Follow the newest mutations first to advance a cascade before
            // older discoveries start independent cascades. An initial scan finds work
            // that existed before the call. Stale discovery entries are cheap
            // to discard after a leaf has been promoted to the live queue.
            for initial in [false, true] {
                let count = if initial {
                    state.initial_scan.len()
                } else {
                    state.follow_ups.len()
                };
                for _ in 0..count {
                    let leaf = if initial {
                        state.initial_scan.pop_front().unwrap()
                    } else {
                        state.follow_ups.pop_front().unwrap()
                    };
                    if !state.pending.contains(&leaf) {
                        continue;
                    }
                    if state.owners.contains_key(&leaf)
                        || state
                            .blocked_on
                            .get(&leaf)
                            .is_some_and(|id| state.owners.contains_key(id))
                    {
                        if initial {
                            state.initial_scan.push_back(leaf);
                        } else {
                            state.follow_ups.push_back(leaf);
                        }
                        continue;
                    }
                    state.pending.remove(&leaf);
                    state.queued_follow_ups.remove(&leaf);
                    let previous_target = state.blocked_on.remove(&leaf);
                    // A single persistently unproductive leaf must not monopolize
                    // workers. New leaves also share the finite global work budget.
                    let attempts = state.attempts.entry(leaf).or_default();
                    if *attempts >= 8 {
                        state.capped.insert(leaf);
                        continue;
                    }
                    if let Some(target) = previous_target {
                        state.owners.insert(target, leaf);
                    }
                    let depth = state.depths.remove(&leaf).unwrap_or(0);
                    state.active.insert(leaf, depth);
                    state.owners.insert(leaf, leaf);
                    return Some((leaf, depth));
                }
            }
            if state.active.is_empty() {
                if state.audited_generation == Some(state.generation) {
                    return None;
                }
                state.discovering = true;
                let generation = state.generation;
                drop(state);
                let discovery = DiscoveryGuard(self, generation);
                discover();
                drop(discovery);
                state = self.state.lock();
                continue;
            }
            self.changed.wait(&mut state);
        }
    }

    pub(super) fn record_attempt(&self, source: NodeId) -> bool {
        let mut state = self.state.lock();
        if state.structural_attempts >= self.budget {
            state.exhausted = true;
            self.changed.notify_all();
            return false;
        }
        *state.attempts.entry(source).or_default() += 1;
        state.structural_attempts += 1;
        state.generation += 1;
        true
    }

    /// A failed extension defers this task until the conflicting owner exits.
    /// It never waits while holding a leaf claim, so opposite merges cannot
    /// deadlock. Contention retries do not consume a leaf's attempt allowance.
    pub(super) fn claim(&self, source: NodeId, leaf: NodeId) -> bool {
        let mut state = self.state.lock();
        if let Some(&owner) = state.owners.get(&leaf) {
            if owner != source {
                state.conflicts += 1;
                state.blocked_on.insert(source, leaf);
                let depth = state.active[&source];
                state.depths.entry(source).or_insert(depth);
                state.pending.insert(source);
                if state.queued_follow_ups.insert(source) {
                    state.follow_ups.push_back(source);
                }
                *state.attempts.get_mut(&source).unwrap() -= 1;
                state.structural_attempts -= 1;
                return false;
            }
        }
        state.owners.insert(leaf, source);
        true
    }

    pub(super) fn complete(&self, source: NodeId) {
        let mut state = self.state.lock();
        state.active.remove(&source);
        state.owners.retain(|_, owner| *owner != source);
        state.completed += 1;
        state.exhausted |= std::thread::panicking();
        self.changed.notify_all();
    }

    pub(super) fn limits(&self) -> (bool, usize, usize) {
        let state = self.state.lock();
        (state.exhausted, state.capped.len(), state.conflicts)
    }

    pub(super) fn completed(&self) -> usize {
        self.state.lock().completed
    }
}

/// Wake waiting workers even if the final replica discovery panics.
struct DiscoveryGuard<'a>(&'a BalanceScheduler, usize);
impl Drop for DiscoveryGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock();
        state.audited_generation = Some(self.1);
        state.discovering = false;
        state.exhausted |= std::thread::panicking();
        self.0.changed.notify_all();
    }
}

/// Release every leaf owned by a task, including on an unwinding worker.
pub(super) struct TaskGuard<'a>(pub &'a BalanceScheduler, pub NodeId);
impl Drop for TaskGuard<'_> {
    fn drop(&mut self) {
        self.0.complete(self.1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn active_task_can_publish_work_after_the_queue_becomes_empty() {
        let queue = BalanceScheduler::new(vec![1], 100);
        assert_eq!(queue.next(), Some(1));
        std::thread::scope(|scope| {
            let (send, receive) = mpsc::channel();
            let queue = &queue;
            scope.spawn(move || send.send(queue.next()).unwrap());
            assert!(receive.recv_timeout(Duration::from_millis(20)).is_err());
            queue.enqueue(2);
            assert_eq!(
                receive.recv_timeout(Duration::from_secs(2)).unwrap(),
                Some(2)
            );
        });
        queue.complete(1);
        queue.complete(2);
        assert_eq!(queue.next(), None);
    }

    #[test]
    fn replacements_stay_claimed_until_their_parent_task_finishes() {
        let queue = BalanceScheduler::new(vec![1, 2], 100);
        assert_eq!(queue.next(), Some(1));
        assert!(queue.claim(1, 3));
        queue.enqueue(3);
        queue.enqueue(3);
        assert_eq!(queue.next(), Some(2));
        queue.complete(1);
        assert_eq!(queue.next(), Some(3));
        queue.complete(2);
        queue.complete(3);
        assert_eq!(queue.next(), None);
    }

    #[test]
    fn opposite_merges_release_claims_before_retrying() {
        let queue = BalanceScheduler::new(vec![1, 2], 100);
        assert_eq!(queue.next(), Some(1));
        assert_eq!(queue.next(), Some(2));
        queue.record_attempt(1);
        queue.record_attempt(2);
        assert!(!queue.claim(1, 2));
        assert!(!queue.claim(2, 1));
        queue.complete(1);
        queue.complete(2);
        assert_eq!(queue.next(), Some(1));
        assert!(queue.claim(1, 2));
        queue.complete(1);
        assert_eq!(queue.next(), Some(2));
        queue.complete(2);
        assert_eq!(queue.next(), None);
    }

    #[test]
    fn task_guard_releases_every_claim_on_unwind() {
        let queue = BalanceScheduler::new(vec![1], 100);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let leaf = queue.next().unwrap();
            let _guard = TaskGuard(&queue, leaf);
            assert!(queue.claim(leaf, 2));
            queue.enqueue(2);
            panic!("worker failed");
        }));
        assert_eq!(queue.next(), None);
        assert!(queue.state.lock().owners.is_empty());
        assert!(queue.state.lock().active.is_empty());
    }

    #[test]
    fn healthy_visits_do_not_consume_structural_work_budget() {
        let queue = BalanceScheduler::new(vec![1], 1);
        for _ in 0..20 {
            assert_eq!(queue.next(), Some(1));
            queue.enqueue(1);
            queue.complete(1);
        }
        assert_eq!(queue.next(), Some(1));
        assert!(queue.record_attempt(1));
        queue.complete(1);
        assert!(!queue.limits().0);
    }

    #[test]
    fn discovery_can_publish_work_after_all_active_tasks_finish() {
        let queue = BalanceScheduler::new(vec![], 10);
        let discovered = std::sync::atomic::AtomicBool::new(false);
        assert_eq!(
            queue.next_with(|| {
                if !discovered.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    queue.enqueue(2);
                }
            }),
            Some((2, 0))
        );
        queue.complete(2);
        assert_eq!(queue.next(), None);
    }

    #[test]
    fn new_cascade_work_runs_before_older_follow_ups() {
        let queue = BalanceScheduler::new(vec![1], 100);
        assert_eq!(queue.next(), Some(1));
        queue.enqueue_cascade(2, 1);
        queue.enqueue_cascade(3, 1);
        assert_eq!(queue.next_with(|| {}), Some((3, 1)));
        queue.enqueue_cascade(4, 2);
        assert_eq!(queue.next_with(|| {}), Some((4, 2)));
        queue.complete(4);
        queue.complete(3);
        assert_eq!(queue.next(), Some(2));
        queue.complete(2);
        queue.complete(1);
        assert_eq!(queue.next(), None);
    }

    #[test]
    fn causal_follow_ups_run_before_remaining_initial_discovery() {
        let queue = BalanceScheduler::new(vec![1, 2, 3], 100);
        assert_eq!(queue.next(), Some(1));
        queue.enqueue_cascade(3, 2);
        assert_eq!(queue.next_with(|| {}), Some((3, 2)));
        queue.complete(1);
        queue.complete(3);
        assert_eq!(queue.next(), Some(2));
        queue.complete(2);
        assert_eq!(queue.next(), None);
    }

    #[test]
    fn claim_retry_preserves_the_dispatched_cascade_depth() {
        let queue = BalanceScheduler::new(vec![1, 2], 10);
        queue.enqueue_cascade(1, 3);
        assert_eq!(queue.next_with(|| {}), Some((1, 3)));
        assert_eq!(queue.next(), Some(2));
        assert!(queue.record_attempt(1));
        assert!(!queue.claim(1, 2));
        queue.complete(1);
        queue.complete(2);
        assert_eq!(queue.next_with(|| {}), Some((1, 3)));
        queue.complete(1);
    }

    #[test]
    fn queued_follow_up_preserves_cascade_depth() {
        let queue = BalanceScheduler::new(vec![1], 10);
        queue.enqueue_cascade(1, 3);
        queue.enqueue_cascade(1, 1);
        queue.enqueue(1);
        assert_eq!(queue.next_with(|| {}), Some((1, 3)));
        queue.complete(1);
    }

    #[test]
    fn repeated_work_and_fresh_ids_are_bounded() {
        let queue = BalanceScheduler::new(vec![1], 100);
        for _ in 0..8 {
            assert_eq!(queue.next(), Some(1));
            queue.record_attempt(1);
            queue.enqueue(1);
            queue.complete(1);
        }
        assert_eq!(queue.next(), None);
        let queue = BalanceScheduler::new(vec![0], 8);
        for id in 0..8 {
            assert_eq!(queue.next(), Some(id));
            assert!(queue.record_attempt(id));
            queue.enqueue(id + 1);
            queue.complete(id);
        }
        assert_eq!(queue.next(), Some(8));
        assert!(!queue.record_attempt(8));
        queue.complete(8);
        assert_eq!(queue.next(), None);
    }
}
