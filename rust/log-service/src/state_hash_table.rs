//! [StateHashTable] solves the rendezvous problem.
//!
//! ```
//! use std::sync::atomic::{AtomicBool, Ordering};
//! use std::sync::Arc;
//! use chroma_log_service::state_hash_table::{Handle, Key, StateHashTable, Value};
//!
//! #[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
//! struct SampleKey {
//!     key: u64,
//! }
//!
//! impl SampleKey {
//!     const fn new(key: u64) -> Self {
//!         Self {
//!             key,
//!         }
//!     }
//! }
//!
//! impl Key for SampleKey {
//! }
//!
//! #[derive(Debug, Default)]
//! struct SampleValue {
//!     finished: AtomicBool,
//! }
//!
//! impl From<SampleKey> for SampleValue {
//!     fn from(key: SampleKey) -> Self {
//!         Self {
//!             finished: AtomicBool::default(),
//!         }
//!     }
//! }
//!
//! impl Value for SampleValue {
//!     fn finished(&self) -> bool { self.finished.load(Ordering::Relaxed) }
//! }
//!
//! // Create the state hash table.  This should be a global-ish structure.
//! let mut sht: StateHashTable<SampleKey, SampleValue> = StateHashTable::new();
//! // Everything revolves around the key.  We don't demonstrate this, but different keys are
//! // totally partitioned and do not interact except to contend on a shared lock.
//! const KEY: SampleKey = SampleKey::new(42);
//!
//! // There's nothing there until we create it.
//! assert!(sht.get_state(KEY).is_none());
//! let mut state1 = sht.create_state(KEY);
//! assert!(state1.is_some());
//! let mut state1 = state1.unwrap();
//!
//! // Attempts to create twice fail with None.
//! let mut state2 = sht.create_state(KEY);
//! assert!(state2.is_none());
//!
//! // But get_state will work.
//! let mut state3 = sht.get_state(KEY);
//! assert!(state3.is_some());
//! let mut state3 = state3.unwrap();
//!
//! // It is guaranteed that when two threads hold reference to the same hash table and have [Eq]
//! // keys they will be the same underlying value.
//!
//! Handle::is_same(&state1, &state3);
//!
//! // It is also guaranteed that when state is dropped but the work is unfinished that the value
//! // will persist in the table.  Note that there will be no handles to this state and it will
//! // persist.
//! drop(state1);
//! drop(state3);
//!
//! // Notice that we use [get_state] here.  It uses the existing state.
//! let mut state4 = sht.get_state(KEY);
//! assert!(state4.is_some());
//! let mut state4 = state4.unwrap();
//! state4.finished.store(true, Ordering::Relaxed);
//! let state4_clone = state4.clone();
//!
//! // Drop the remaining references.
//! drop(state4);
//! drop(state4_clone);
//!
//! // Get state fails because we marked it finished and dropped all references.  Only when the
//! // last reference is dropped will the item be collected, even if the outcome of the
//! // [finished()] call changes.
//! let mut state5 = sht.get_state(KEY);
//! assert!(state5.is_none());
//! ```

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

//////////////////////////////////////////////// Key ///////////////////////////////////////////////

/// A key for a state hash table.
pub trait Key: Clone + Debug + Hash + Eq + PartialEq {}

impl Key for u64 {}
impl Key for String {}

/////////////////////////////////////////////// Value //////////////////////////////////////////////

/// A value for a state hash table.
pub trait Value: Default {
    /// True iff the value is at a quiescent/finished state.  This means it can be collected, not
    /// that it will be collected.  It is perfectly acceptable to pickup a handle to finished state
    /// and take a transition that leads to it being unfinished.  Consequently, finished should be
    /// evaluated under mutual exclusion.  The way we do this is to hold a lock, check that we hold
    /// the only deferenceable copy (there's another in the map, but the lock prevents anyone else
    /// from accessing the map because it's the map's lock that we hold).  Consequently, this
    /// should be a fast computation.
    fn finished(&self) -> bool;
}

////////////////////////////////////////////// Handle //////////////////////////////////////////////

/// A Handle holds a reference to a key-value pair in a table.  Two handles that come from the same
/// table and key are guaranteed to refer to the same piece of state.
pub struct Handle<K: Key, V: Value> {
    entries: Arc<Mutex<HashMap<K, Arc<V>>>>,
    key: K,
    value: Arc<V>,
}

impl<K: Key, V: Value> Handle<K, V> {
    fn new(table: &'_ StateHashTable<K, V>, key: K, value: Arc<V>) -> Self {
        let entries = Arc::clone(&table.entries);
        Self {
            entries,
            key,
            value,
        }
    }

    #[allow(dead_code)]
    pub fn key(&self) -> &K {
        &self.key
    }

    #[allow(dead_code)]
    pub fn value(&self) -> &V {
        &self.value
    }

    /// True if and only if both handles point to the same table and state.
    #[allow(dead_code)]
    pub fn is_same(lhs: &Self, rhs: &Self) -> bool {
        Arc::ptr_eq(&lhs.entries, &rhs.entries)
            && lhs.key == rhs.key
            && Arc::ptr_eq(&lhs.value, &rhs.value)
    }
}

impl<K: Key, V: Value> Deref for Handle<K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<K: Key, V: Value> Clone for Handle<K, V> {
    fn clone(&self) -> Self {
        Self {
            entries: Arc::clone(&self.entries),
            key: self.key.clone(),
            value: Arc::clone(&self.value),
        }
    }
}

impl<K: Key, V: Value> Drop for Handle<K, V> {
    fn drop(&mut self) {
        let mut entries = self.entries.lock().unwrap();
        // us and the table; synchronized by entries intentionally.
        //
        // This intentionally calls finished() while holding the mutex.  We spec that it needs to
        // be fast.  And there's no way for anyone to come along, get the reference from us or the
        // map (per Rust borrow rules) and change the state.  So it looks like we're contending on
        // the mutex, but it's us and the map.  Only a new thread to come along can contend, and by
        // that point we've already made the decision to remove from the map, so the new thread
        // will follow the rules to create a value.
        if Arc::strong_count(&self.value) == 2 && (*self.value).finished() {
            entries.remove(&self.key);
        }
        // NOTE(rescrv):  Here we're safe to drop the handle.  If the count is less than two we've
        // already cleaned up all but self.  If the count is two we cleanup when finished.
        // Otherwise someone else will pass through two on the drop.
    }
}

////////////////////////////////////////// StateHashTable //////////////////////////////////////////

/// StateHashTable is the main collection.
pub struct StateHashTable<K: Key, V: Value> {
    entries: Arc<Mutex<HashMap<K, Arc<V>>>>,
}

impl<K: Key, V: Value> StateHashTable<K, V> {
    /// Create a new StateHashTable.  This should be an infrequent operation.
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Return a seemingly-arbitrary key from the hash table or None if there's no keys in the hash
    /// table.  This is meant to be used for draining a server of waiters.
    #[allow(dead_code)]
    pub fn arbitary_key(&self) -> Option<K> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .map(|(k, _)| k.clone())
            .next()
    }

    /// Create a new piece of state, returning None iff there already exists state for `key`.
    #[allow(dead_code)]
    pub fn create_state(&self, key: K) -> Option<Handle<K, V>>
    where
        V: From<K>,
    {
        let value = Arc::new(V::from(key.clone()));
        let valuep = Arc::clone(&value);
        let mut entries = self.entries.lock().unwrap();
        if !entries.contains_key(&key) {
            entries.insert(key.clone(), value);
            Some(Handle::new(self, key, valuep))
        } else {
            None
        }
    }

    /// Return an existing new piece of state, returning None iff there does not exist state for
    /// `key`.
    #[allow(dead_code)]
    pub fn get_state(&self, key: K) -> Option<Handle<K, V>> {
        let entries = Arc::clone(&self.entries);
        let e = self.entries.lock().unwrap();
        e.get(&key).map(|value| Handle {
            entries,
            key,
            value: Arc::clone(value),
        })
    }

    /// Return an existing piece of state, or create a new one, and always return a handle to the
    /// state for `key`.
    pub fn get_or_create_state(&self, key: K) -> Handle<K, V>
    where
        V: From<K>,
    {
        let mut value = None;
        let mut make_value = false;

        loop {
            if make_value && value.is_none() {
                value = Some(Arc::new(V::from(key.clone())));
            }
            let mut entries = self.entries.lock().unwrap();
            let state = entries.get(&key);
            match (state, &value) {
                (None, None) => {
                    make_value = true;
                }
                (None, Some(value)) => {
                    let value1 = Arc::clone(value);
                    let value2 = Arc::clone(value);
                    entries.insert(key.clone(), value1);
                    return Handle::new(self, key, value2);
                }
                (Some(state), _) => {
                    let value = Arc::clone(state);
                    return Handle::new(self, key, value);
                }
            }
        }
    }
}

impl<K: Key, V: Value> Default for StateHashTable<K, V> {
    fn default() -> Self {
        Self::new()
    }
}
