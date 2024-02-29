use crate::errors::{ChromaError, ErrorCodes};
use thiserror::Error;

use crate::blockstore::{Blockfile, BlockfileKey, HashMapBlockfile, Key, Value};

use async_trait::async_trait;
use roaring::RoaringBitmap;
use std::{
    collections::HashMap,
    ops::{BitOrAssign, SubAssign}
};

#[derive(Debug, Error)]
pub(crate) enum MetadataIndexError {
    #[error("Key not found")]
    NotFoundError,
    #[error("This operation cannot be done in a transaction")]
    InTransaction,
    #[error("This operation can only be done in a transaction")]
    NotInTransaction,
}

impl ChromaError for MetadataIndexError {
    fn code(&self) -> ErrorCodes {
        match self {
            MetadataIndexError::NotFoundError => ErrorCodes::InvalidArgument,
            MetadataIndexError::InTransaction => ErrorCodes::InvalidArgument,
            MetadataIndexError::NotInTransaction => ErrorCodes::InvalidArgument,
        }
    }
}

pub(crate) trait MetadataIndexValue {
    fn to_blockfile_key(&self) -> Key;
}
impl MetadataIndexValue for String {
    fn to_blockfile_key(&self) -> Key {
        Key::String(self.clone())
    }
}
impl MetadataIndexValue for f32 {
    fn to_blockfile_key(&self) -> Key {
        Key::Float(*self)
    }
}
impl MetadataIndexValue for bool {
    fn to_blockfile_key(&self) -> Key {
        Key::Bool(*self)
    }
}

pub(crate) trait MetadataIndex<T: MetadataIndexValue> {
    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;
    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;
    fn in_transaction(&self) -> bool;

    // Must be in a transaction to put or delete.
    fn set(&mut self, key: &str, value: T, offset_id: u32) -> Result<(), Box<dyn ChromaError>>;
    // Can delete anything -- if it's not in committed state the delete will be silently discarded.
    fn delete(&mut self, key: &str, value: T, offset_id: u32) -> Result<(), Box<dyn ChromaError>>;

    // Always reads from committed state.
    fn get(&self, key: &str, value: T) -> Result<RoaringBitmap, Box<dyn ChromaError>>;
}

struct BlockfileMetadataIndex<T> {
    // TODO this is a hack to make this struct generic, which makes types
    // much easier to read when using it.
    unused: Option<T>,
    blockfile: Box<dyn Blockfile>,
    in_transaction: bool,
    uncommitted_rbms: HashMap<BlockfileKey, RoaringBitmap>,
}

impl<T> BlockfileMetadataIndex<T> {
    pub fn new() -> Self {
        BlockfileMetadataIndex {
            unused: None,
            blockfile: Box::new(HashMapBlockfile::open(&"in-memory").unwrap()),
            in_transaction: false,
            uncommitted_rbms: HashMap::new(),
        }
    }

    fn look_up_key_and_populate_uncommitted_rbms(&mut self, key: &BlockfileKey) -> Result<(), Box<dyn ChromaError>> {
        if !self.uncommitted_rbms.contains_key(&key) {
            match self.blockfile.get(key.clone()) {
                Ok(Value::RoaringBitmapValue(rbm)) => {
                    self.uncommitted_rbms.insert(key.clone(), rbm);
                },
                _ => {
                    let rbm = RoaringBitmap::new();
                    self.uncommitted_rbms.insert(key.clone(), rbm);
                },
            };
        }
        Ok(())
    }
}

impl<T: MetadataIndexValue> MetadataIndex<T> for BlockfileMetadataIndex<T> {
    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        if self.in_transaction {
            return Err(Box::new(MetadataIndexError::InTransaction));
        }
        self.blockfile.begin_transaction()?;
        self.in_transaction = true;
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(MetadataIndexError::NotInTransaction));
        }
        for (key, rbm) in self.uncommitted_rbms.drain() {
            self.blockfile.set(key.clone(), Value::RoaringBitmapValue(rbm.clone()));
        }
        self.blockfile.commit_transaction()?;
        self.in_transaction = false;
        self.uncommitted_rbms.clear();
        Ok(())
    }

    fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    fn set(&mut self, key: &str, value: T, offset_id: u32) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(MetadataIndexError::NotInTransaction));
        }
        let blockfilekey = BlockfileKey::new(key.to_string(), value.to_blockfile_key());
        self.look_up_key_and_populate_uncommitted_rbms(&blockfilekey)?;
        let mut rbm = self.uncommitted_rbms.get_mut(&blockfilekey).unwrap();
        rbm.insert(offset_id);
        Ok(())
    }

    fn delete(&mut self, key: &str, value: T, offset_id: u32) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(MetadataIndexError::NotInTransaction));
        }
        let blockfilekey = BlockfileKey::new(key.to_string(), value.to_blockfile_key());
        self.look_up_key_and_populate_uncommitted_rbms(&blockfilekey)?;
        let mut rbm = self.uncommitted_rbms.get_mut(&blockfilekey).unwrap();
        rbm.remove(offset_id);
        Ok(()) 
    }

    fn get(&self, key: &str, value: T) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        if self.in_transaction {
            return Err(Box::new(MetadataIndexError::InTransaction));
        }
        let blockfilekey = BlockfileKey::new(key.to_string(), value.to_blockfile_key());
        match self.blockfile.get(blockfilekey) {
            Ok(Value::RoaringBitmapValue(rbm)) => Ok(rbm),
            _ => Err(Box::new(MetadataIndexError::NotFoundError)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_string_value_metadata_index_error_when_not_in_transaction() {
        let mut index = BlockfileMetadataIndex::<String>::new();
        let result = index.set("key", "value".to_string(), 1);
        assert_eq!(result.is_err(), true);
        let result = index.delete("key", "value".to_string(), 1);
        assert_eq!(result.is_err(), true);
        let result = index.commit_transaction();
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_string_value_metadata_index_empty_transaction() {
        let mut index = BlockfileMetadataIndex::<String>::new();
        index.begin_transaction().unwrap();
        index.commit_transaction().unwrap();
    }

    #[test]
    fn test_string_value_metadata_index_set_get() {
        let mut index = BlockfileMetadataIndex::<String>::new();
        index.begin_transaction().unwrap();
        index.set("key", "value".to_string(), 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", "value".to_string()).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[test]
    fn test_float_value_metadata_index_set_get() {
        let mut index = BlockfileMetadataIndex::<f32>::new();
        index.begin_transaction().unwrap();
        index.set("key", 1.0, 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", 1.0).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[test]
    fn test_bool_value_metadata_index_set_get() {
        let mut index = BlockfileMetadataIndex::<bool>::new();
        index.begin_transaction().unwrap();
        index.set("key", true, 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", true).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[test]
    fn test_string_value_metadata_index_set_delete_get() {
        let mut index = BlockfileMetadataIndex::<String>::new();
        index.begin_transaction().unwrap();
        index.set("key", "value".to_string(), 1).unwrap();
        index.delete("key", "value".to_string(), 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", "value".to_string()).unwrap();
        assert_eq!(bitmap.len(), 0);
    }

    #[test]
    fn test_string_value_metadata_index_set_delete_set_get() {
        let mut index = BlockfileMetadataIndex::<String>::new();
        index.begin_transaction().unwrap();
        index.set("key", "value".to_string(), 1).unwrap();
        index.delete("key", "value".to_string(), 1).unwrap();
        index.set("key", "value".to_string(), 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", "value".to_string()).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[test]
    fn test_string_value_metadata_index_multiple_keys() {
        let mut index = BlockfileMetadataIndex::<String>::new();
        index.begin_transaction().unwrap();
        index.set("key1", "value".to_string(), 1).unwrap();
        index.set("key2", "value".to_string(), 2).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key1", "value".to_string()).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);

        let bitmap = index.get("key2", "value".to_string()).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(2), true);
    }

    #[test]
    fn test_string_value_metadata_index_multiple_values() {
        let mut index = BlockfileMetadataIndex::<String>::new();
        index.begin_transaction().unwrap();
        index.set("key", "value1".to_string(), 1).unwrap();
        index.set("key", "value2".to_string(), 2).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", "value1".to_string()).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);

        let bitmap = index.get("key", "value2".to_string()).unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(2), true);
    }

    #[test]
    fn test_string_value_metadata_index_delete_in_standalone_transaction() {
        let mut index = BlockfileMetadataIndex::<String>::new();
        index.begin_transaction().unwrap();
        index.set("key", "value".to_string(), 1).unwrap();
        index.commit_transaction().unwrap();

        index.begin_transaction().unwrap();
        index.delete("key", "value".to_string(), 1).unwrap();
        index.commit_transaction().unwrap();

        let bitmap = index.get("key", "value".to_string()).unwrap();
        assert_eq!(bitmap.len(), 0);
    }

    use proptest::prelude::*;
    use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
    use proptest::test_runner::Config;

    #[derive(Clone, Debug)]
    pub(crate) enum MetadataIndexTransition<T: MetadataIndexValue> {
        BeginTransaction,
        CommitTransaction,
        Set(String, T, u32),
        Delete(String, T, u32),
        Get(String, T),
    }

    pub(crate) struct MetadataIndexStateMachine<T: MetadataIndexValue> {
        unused: Option<T>,
    }

    #[derive(Clone, Debug)]
    pub(crate) struct ReferenceState<T: MetadataIndexValue> {
        // Are we in a transaction?
        in_transaction: bool,
        // {metadata key: {metadata value: offset ids}}
        data: HashMap<String, HashMap<T, Vec<u32>>>,
    }

    impl<T: MetadataIndexValue> ReferenceState<T> {
        fn new() -> Self {
            ReferenceState {
                in_transaction: false,
                data: HashMap::new(),
            }
        }
    }

    // We define three separate state machines: one for each type of metadata
    // index. We _could_ hack around this by using a single state machine and
    // matching on key types within, but that has three problems:
    // 1. It's not as clear what's going on.
    // 2. It generates a lot of unnecessary transitions by trying to run
    //    data operations of the wrong type.
    // 3. Perhaps most seriously, proptest wouldn't know the input types(*) so
    //    couldn't reduce failing tests.
    //
    // (*) We could always have it generate input of all three types and ignore
    // the ones that don't match, but that makes reducing harder and makes
    // everything much harder to read.

    impl ReferenceStateMachine for MetadataIndexStateMachine<String> {
        type State = ReferenceState<String>;
        type Transition = MetadataIndexTransition<String>;

        fn init_state() -> BoxedStrategy<Self::State> {
            return Just(ReferenceState::<String>::new()).boxed();
        }

        fn transitions(_state: &Self::State) -> BoxedStrategy<Self::Transition> {
            return prop_oneof![
                Just(MetadataIndexTransition::BeginTransaction),
                Just(MetadataIndexTransition::CommitTransaction),
                // Add random data
                (".{0,10}", ".{0,10}", 1..1000).prop_map(move |(k, v, oid)| {
                    MetadataIndexTransition::Set(k.to_string(), v.to_string(), oid as u32)
                }),
                // Try to delete random data
                (".{0,10}", ".{0,10}", 1..1000).prop_map(move |(k, v, oid)| {
                    MetadataIndexTransition::Delete(k.to_string(), v.to_string(), oid as u32)
                }),
                // Try to get random data
                (".{0,10}", ".{0,10}").prop_map(move |(k, v)| {
                    MetadataIndexTransition::Get(k.to_string(), v.to_string())
                }),
                // TODO we should get set and delete data that we know is in the model
            ].boxed();
        }

        fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
            match transition {
                MetadataIndexTransition::BeginTransaction => {
                    state.in_transaction = true;
                },
                MetadataIndexTransition::CommitTransaction => {
                    state.in_transaction = false;
                },
                MetadataIndexTransition::Set(k, v, oid) => {
                    if !state.in_transaction {
                        return state;
                    }
                    let entry = state.data.entry(k.clone()).or_insert(HashMap::new());
                    entry.entry(v.clone()).or_insert(Vec::new()).push(*oid);
                },
                MetadataIndexTransition::Delete(k, v, oid) => {
                    if !state.in_transaction {
                        return state;
                    }
                    let entry = state.data.entry(k.clone()).or_insert(HashMap::new());
                    if let Some(offsets) = entry.get_mut(v) {
                        offsets.retain(|x| *x != *oid);
                    }
                },
                MetadataIndexTransition::Get(_, _) => {
                    // No-op
                },
            }
            state
        }
    }

    impl StateMachineTest for BlockfileMetadataIndex<String> {
        type SystemUnderTest = Self;
        type Reference = MetadataIndexStateMachine<String>;

        fn init_test(_ref_state: &ReferenceState<String>) -> Self::SystemUnderTest {
            // We don't need to set up on _ref_state since we always initialize
            // ref_state to empty.
            return BlockfileMetadataIndex::<String>::new();
        }

        fn apply(
            mut state: Self::SystemUnderTest,
            ref_state: &ReferenceState<String>,
            transition: MetadataIndexTransition<String>,
        ) -> Self::SystemUnderTest {
            match transition {
                MetadataIndexTransition::BeginTransaction => {
                    let already_in_transaction = state.in_transaction();
                    let res = state.begin_transaction();
                    assert!(state.in_transaction());
                    if already_in_transaction {
                        assert!(res.is_err());
                    } else {
                        assert!(res.is_ok());
                    }
                },
                MetadataIndexTransition::CommitTransaction => {
                    let in_transaction = state.in_transaction();
                    let res = state.commit_transaction();
                    assert_eq!(state.in_transaction(), false);
                    if !in_transaction {
                        assert!(res.is_err());
                    } else {
                        assert!(res.is_ok());
                    }
                },
                MetadataIndexTransition::Set(k, v, oid) => {
                    let in_transaction = state.in_transaction();
                    let res = state.set(&k, v.clone(), oid);
                    if !in_transaction {
                        assert!(res.is_err());
                    } else {
                        assert!(res.is_ok());
                    }
                },
                MetadataIndexTransition::Delete(k, v, oid) => {
                    state.delete(&k, v, oid).unwrap();
                },
                MetadataIndexTransition::Get(k, v) => {
                    let _ = state.get(&k, v).unwrap();
                },
            }
            state
        }

        fn check_invariants(state: &Self::SystemUnderTest, ref_state: &ReferenceState<String>) {
            assert_eq!(state.in_transaction(), ref_state.in_transaction);
            for (k, v) in &ref_state.data {
                for (kk, ref_data) in v {
                    let state_data = state.get(k, kk.clone()).unwrap();
                    assert_eq!(state_data.len(), ref_data.len() as u64);

                    for offset in &state_data {
                        if !ref_data.contains(&(offset)) {
                            assert!(false);
                        }
                    }
                    for offset in ref_data {
                        if !state_data.contains(*offset) {
                            assert!(false);
                        }
                    }
                }
            }
        }
    }

    prop_state_machine! {
        #![proptest_config(Config {
            // Turn failure persistence off for demonstration. This means that no
            // regression file will be captured.
            failure_persistence: None,
            // Enable verbose mode to make the state machine test print the
            // transitions for each case.
            verbose: 1,
            // Only run 10 cases by default to avoid running out of system resources
            // and taking too long to finish.
            cases: 10,
            .. Config::default()
        })]
        #[test]
        fn proptest_string_metadata_index(sequential 1..100 => BlockfileMetadataIndex<String>);
    }
}