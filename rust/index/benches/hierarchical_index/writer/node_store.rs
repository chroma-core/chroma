//! Resident records have independent locks. The map only locates their handles;
//! no map bucket remains locked while a record is read, edited, or retired.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock};

use super::super::common::{NodeId, TreeNode};

type Resident<T> = Arc<RwLock<Option<T>>>;

pub(crate) struct NodeStore<T = TreeNode> {
    records: DashMap<NodeId, Resident<T>>,
}

pub(crate) struct NodeRead<T = TreeNode> {
    id: NodeId,
    guard: ArcRwLockReadGuard<RawRwLock, Option<T>>,
}

pub(crate) struct NodeWrite<T = TreeNode> {
    id: NodeId,
    guard: ArcRwLockWriteGuard<RawRwLock, Option<T>>,
}

impl<T> NodeStore<T> {
    pub(crate) fn new() -> Self {
        Self {
            records: DashMap::new(),
        }
    }

    fn handle(&self, id: &NodeId) -> Option<Resident<T>> {
        self.records.get(id).map(|entry| Arc::clone(entry.value()))
    }

    pub(crate) fn get(&self, id: &NodeId) -> Option<NodeRead<T>> {
        let handle = self.handle(id)?;
        let guard = handle.read_arc();
        guard.as_ref()?;
        Some(NodeRead { id: *id, guard })
    }

    pub(crate) fn get_mut(&self, id: &NodeId) -> Option<NodeWrite<T>> {
        let handle = self.handle(id)?;
        let guard = handle.write_arc();
        guard.as_ref()?;
        Some(NodeWrite { id: *id, guard })
    }

    /// Updating a live ID preserves its resident handle and lock. New logical
    /// nodes should use fresh IDs, but replacing a value cannot bypass its lock.
    pub(crate) fn insert(&self, id: NodeId, value: T) -> Option<T> {
        let mut pending = Some(value);
        loop {
            // Entry creation never waits for a resident lock while holding a
            // bucket. The fresh handle is private until the entry is installed.
            let handle = match self.records.entry(id) {
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    entry.insert(Arc::new(RwLock::new(pending.take())));
                    return None;
                }
                dashmap::mapref::entry::Entry::Occupied(entry) => Arc::clone(entry.get()),
            };
            let mut guard = handle.write_arc();
            // Removal may retire and unlink this handle while insertion waits.
            // Never revive an unlinked handle: retry against the current map.
            let current = self
                .records
                .get(&id)
                .is_some_and(|entry| Arc::ptr_eq(entry.value(), &handle));
            if current {
                return std::mem::replace(&mut *guard, pending.take());
            }
        }
    }

    pub(crate) fn remove(&self, id: &NodeId) -> Option<(NodeId, T)> {
        let handle = self.handle(id)?;
        let mut guard = handle.write_arc();
        let value = guard.take()?;
        // All lookups release bucket guards before taking resident locks, so
        // unlinking under this write lock introduces no bucket/node lock cycle.
        self.records
            .remove_if(id, |_, current| Arc::ptr_eq(current, &handle));
        Some((*id, value))
    }

    pub(crate) fn contains_key(&self, id: &NodeId) -> bool {
        self.get(id).is_some()
    }

    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    fn handles(&self) -> Vec<(NodeId, Resident<T>)> {
        self.records
            .iter()
            .map(|entry| (*entry.key(), Arc::clone(entry.value())))
            .collect()
    }

    /// Iteration snapshots handles, not values. A concurrently retired record
    /// is skipped; callers needing a consistent whole tree use a phase boundary.
    pub(crate) fn iter(&self) -> impl Iterator<Item = NodeRead<T>> {
        self.handles().into_iter().filter_map(|(id, handle)| {
            let guard = handle.read_arc();
            guard.as_ref()?;
            Some(NodeRead { id, guard })
        })
    }

    pub(crate) fn iter_mut(&self) -> impl Iterator<Item = NodeWrite<T>> {
        self.handles().into_iter().filter_map(|(id, handle)| {
            let guard = handle.write_arc();
            guard.as_ref()?;
            Some(NodeWrite { id, guard })
        })
    }
}

impl<T> NodeRead<T> {
    pub(crate) fn key(&self) -> &NodeId {
        &self.id
    }
    pub(crate) fn value(&self) -> &T {
        self
    }
}

impl<T> Deref for NodeRead<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard.as_ref().expect("live record read guard")
    }
}

impl<T> NodeWrite<T> {
    pub(crate) fn key(&self) -> &NodeId {
        &self.id
    }
    pub(crate) fn value(&self) -> &T {
        self
    }
    pub(crate) fn value_mut(&mut self) -> &mut T {
        self
    }
}

impl<T> Deref for NodeWrite<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard.as_ref().expect("live record write guard")
    }
}

impl<T> DerefMut for NodeWrite<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.guard.as_mut().expect("live record write guard")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn editing_one_record_does_not_hold_its_map_bucket() {
        let store = NodeStore {
            records: DashMap::with_shard_amount(2),
        };
        for id in 0..64 {
            store.insert(id, id);
        }
        let store = Arc::new(store);
        let held = store.get_mut(&0).unwrap();
        let (tx, rx) = mpsc::channel();
        let other = Arc::clone(&store);
        let worker = std::thread::spawn(move || {
            // Acquiring the map entry for the *same* key proves its bucket is
            // free, without waiting for the held resident record lock.
            let handle = other.handle(&0).unwrap();
            assert!(handle.try_write().is_none());
            for id in 1..64 {
                *other.get_mut(&id).unwrap() += 1;
            }
            tx.send(()).unwrap();
        });
        let result = rx.recv_timeout(Duration::from_secs(2));
        drop(held);
        worker.join().unwrap();
        result.expect("resident edits must not retain a bucket guard");
    }

    #[test]
    fn replacing_a_live_record_preserves_its_handle() {
        let store = NodeStore::new();
        store.insert(4, 10);
        let original = store.handle(&4).unwrap();
        assert_eq!(store.insert(4, 20), Some(10));
        assert!(Arc::ptr_eq(&original, &store.handle(&4).unwrap()));
        assert_eq!(*original.read().as_ref().unwrap(), 20);
    }

    #[test]
    fn removal_retires_old_handles_before_id_reinsertion() {
        let store = NodeStore::new();
        store.insert(4, 10);
        let original = store.handle(&4).unwrap();
        assert_eq!(store.remove(&4), Some((4, 10)));
        assert!(original.read().is_none());
        assert!(store.get(&4).is_none());
        store.insert(4, 30);
        assert!(!Arc::ptr_eq(&original, &store.handle(&4).unwrap()));
        assert!(original.read().is_none());
        assert_eq!(*store.get(&4).unwrap(), 30);
        assert_eq!(store.len(), 1);
    }
}
