//! Short publication reservations, independent of leaf membership claims.
//! An operation reserves the subtree rooted at the nearest common ancestor of
//! all changed records. Ancestor and descendant reservations exclude each other;
//! separate branches may publish concurrently. Ancestry is revalidated on entry.
use super::super::common::NodeId;
use parking_lot::{Condvar, Mutex};

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
#[derive(Default)]
pub(super) struct StructuralReservations {
    held: Mutex<Vec<(NodeId, Vec<NodeId>)>>,
    changed: Condvar,
    pub publications: AtomicU64,
    pub navigation_retries: AtomicU64,
    pub wait_nanos: AtomicU64,
    pub hold_nanos: AtomicU64,
}
pub(super) struct Reservation<'a> {
    owner: &'a StructuralReservations,
    root: NodeId,
    start: Instant,
}
impl StructuralReservations {
    pub fn acquire(&self, root: NodeId, ancestors: Vec<NodeId>) -> Reservation<'_> {
        let wait = Instant::now();
        let mut held = self.held.lock();
        while held
            .iter()
            .any(|(active, path)| ancestors.contains(active) || path.contains(&root))
        {
            self.changed.wait(&mut held);
        }
        held.push((root, ancestors));
        self.wait_nanos
            .fetch_add(wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.publications.fetch_add(1, Ordering::Relaxed);
        Reservation {
            owner: self,
            root,
            start: Instant::now(),
        }
    }
}
impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.owner
            .hold_nanos
            .fetch_add(self.start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let mut held = self.owner.held.lock();
        let index = held
            .iter()
            .position(|(root, _)| *root == self.root)
            .expect("reservation missing");
        held.swap_remove(index);
        self.owner.changed.notify_all();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disjoint_regions_progress_while_another_is_reserved() {
        let r = StructuralReservations::default();
        let _a = r.acquire(1, vec![1, 0]);
        let _b = r.acquire(3, vec![3, 0]);
    }
    #[test]
    fn ancestor_and_descendant_reservations_exclude_each_other() {
        use std::sync::mpsc;
        use std::time::Duration;
        for (held_root, held_path, waiting_root, waiting_path) in [
            (1, vec![1, 0], 2, vec![2, 1, 0]),
            (2, vec![2, 1, 0], 1, vec![1, 0]),
        ] {
            let reservations = StructuralReservations::default();
            let held = reservations.acquire(held_root, held_path);
            std::thread::scope(|scope| {
                let (attempting, started) = mpsc::sync_channel(0);
                let (acquired, completed) = mpsc::sync_channel(0);
                let manager = &reservations;
                let waiter = scope.spawn(move || {
                    attempting.send(()).unwrap();
                    let _guard = manager.acquire(waiting_root, waiting_path);
                    acquired.send(()).unwrap();
                });
                started.recv_timeout(Duration::from_secs(2)).unwrap();
                assert!(completed.recv_timeout(Duration::from_millis(30)).is_err());
                // A waiting overlapping operation must not block an unrelated branch.
                drop(reservations.acquire(3, vec![3, 0]));
                drop(held);
                completed.recv_timeout(Duration::from_secs(2)).unwrap();
                waiter.join().unwrap();
            });
        }
    }
}
