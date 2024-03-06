use crate::errors::{ChromaError, ErrorCodes};
use thiserror::Error;

use crate::blockstore::{Blockfile, BlockfileKey, HashMapBlockfile, Key, Value};

use async_trait::async_trait;
use roaring::RoaringBitmap;
use std::{
    collections::HashMap,
    marker::PhantomData,
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
    metadata_value_type: PhantomData<T>,
    blockfile: Box<dyn Blockfile>,
    in_transaction: bool,
    uncommitted_rbms: HashMap<BlockfileKey, RoaringBitmap>,
}

impl<T> BlockfileMetadataIndex<T> {
    pub fn new() -> Self {
        BlockfileMetadataIndex {
            metadata_value_type: PhantomData,
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
            _ => Ok(RoaringBitmap::new()),
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
    use rand::prelude::{IteratorRandom, SliceRandom};
    use std::rc::Rc;

    // Utility function to check if a Vec<u32> and RoaringBitmap contain equivalent
    // sets.
    fn vec_rbm_eq(a: &Vec<u32>, b: &RoaringBitmap) -> bool {
        if a.len() != b.len() as usize {
            return false;
        }
        for offset in a {
            if !b.contains(*offset) {
                return false;
            }
        }
        for offset in b {
            if !a.contains(&offset) {
                return false;
            }
        }
        return true;
    }

    pub(crate) trait PropTestValue: MetadataIndexValue +
                                    PartialEq +
                                    Eq +
                                    Clone +
                                    std::hash::Hash +
                                    std::fmt::Debug {
        fn strategy() -> BoxedStrategy<Self>;
    }

    impl PropTestValue for String {
        fn strategy() -> BoxedStrategy<Self> {
            ".{0,10}".prop_map(|x| x.to_string()).boxed()
        }
    }

    impl PropTestValue for bool {
        fn strategy() -> BoxedStrategy<Self> {
            prop_oneof![Just(true), Just(false)].boxed()
        }
    }

    // f32 doesn't implement Hash or Eq so we need to wrap it to use
    // in our reference state machine's HashMap. This is a bit of a hack
    // but only used in tests.
    #[derive(Clone, Debug, PartialEq)]
    struct FloatWrapper(f32);

    impl std::hash::Hash for FloatWrapper {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.0.to_bits().hash(state);
        }
    }

    impl Eq for FloatWrapper {}

    impl MetadataIndexValue for FloatWrapper {
        fn to_blockfile_key(&self) -> Key {
            self.0.to_blockfile_key()
        }
    }

    impl PropTestValue for FloatWrapper {
        fn strategy() -> BoxedStrategy<Self> {
            (0..1000).prop_map(|x| FloatWrapper(x as f32)).boxed()
        }
    }

    #[derive(Clone, Debug)]
    pub(crate) enum Transition<T: PropTestValue> {
        BeginTransaction,
        CommitTransaction,
        Set(String, T, u32),
        Delete(String, T, u32),
        Get(String, T),
    }

    #[derive(Clone, Debug)]
    pub(crate) struct ReferenceState<T: PropTestValue> {
        // Are we in a transaction?
        in_transaction: bool,
        // {metadata key: {metadata value: offset ids}}
        data: HashMap<String, HashMap<T, Vec<u32>>>,
    }

    impl<T: PropTestValue> ReferenceState<T> {
        fn new() -> Self {
            ReferenceState {
                in_transaction: false,
                data: HashMap::new(),
            }
        }

        fn random_entry(self: &Self) -> Option<(String, T, u32)> {
            let k = self.data.keys().choose(&mut rand::thread_rng())?;
            let v = self.data.get(k)?.keys().choose(&mut rand::thread_rng())?;
            let vv = self.data.get(k)?.get(v)?.choose(&mut rand::thread_rng())?;
            Some((k.clone(), v.clone(), vv.clone()))
        }

        fn kv_rbm_eq(
            self: &Self,
            rbm: &RoaringBitmap,
            k: &str,
            v: &T,
        ) -> bool {
            match self.data.get(k) {
                Some(vv) => match vv.get(v) {
                    Some(rbm2) => vec_rbm_eq(rbm2, rbm),
                    None => rbm.is_empty(),
                },
                None => rbm.is_empty(),
            }
        }
    }

    pub(crate) struct ReferenceStateMachineImpl<T: PropTestValue> {
        value_type: PhantomData<T>,
    }

    impl<T: PropTestValue + 'static> ReferenceStateMachine for ReferenceStateMachineImpl<T> {
        type State = ReferenceState<T>;
        type Transition = Transition<T>;

        fn init_state() -> BoxedStrategy<Self::State> {
            return Just(ReferenceState::<T>::new()).boxed();
        }

        fn transitions(state: &ReferenceState<T>) -> BoxedStrategy<Transition<T>> {
            let random_entry_from_ref = state.random_entry();
            if random_entry_from_ref.is_none() {
                return prop_oneof![
                    Just(Transition::BeginTransaction),
                    (".{0,10}", T::strategy(), 1..1000).prop_map(move |(k, v, oid)| {
                        Transition::Set(k.to_string(), v, oid as u32)
                    }),
                ].boxed();
            }

            let random_entry_from_ref = random_entry_from_ref.unwrap();
            let (rk, rv, roid) = random_entry_from_ref;
            let rk = Rc::new(rk);
            let rv = Rc::new(rv);
            return prop_oneof![
                Just(Transition::BeginTransaction),
                Just(Transition::CommitTransaction),
                (".{0,10}", T::strategy(), 1..1000).prop_map(move |(k, v, oid)| {
                    Transition::Set(k.to_string(), v, oid as u32)
                }),
                (".{0,10}", T::strategy(), 1..1000).prop_map(move |(k, v, oid)| {
                    Transition::Delete(k.to_string(), v, oid as u32)
                }),
                (".{0,10}", T::strategy()).prop_map(move |(k, v)| {
                    Transition::Get(k.to_string(), v)
                }),

                Just(Transition::Set((*rk).clone(), (*rv).clone(), roid)),
                (".{1,10}").prop_map({
                    let rv = Rc::clone(&rv);
                    move |k| {
                        Transition::Set(k.to_string(), (*rv).clone(), roid)
                    }
                }),
                (T::strategy()).prop_map({
                    let rk = Rc::clone(&rk);
                    move |v| {
                        Transition::Set((*rk).clone(), v, roid)
                    }
                }),
                (1..1000).prop_map({
                    let rk = Rc::clone(&rk);
                    let rv = Rc::clone(&rv);
                    move |oid| {
                        Transition::Set((*rk).clone(), (*rv).clone(), oid as u32)
                    }
                }),

                Just(Transition::Delete((*rk).clone(), (*rv).clone(), roid)),
                (".{1,10}").prop_map({
                    let rv = Rc::clone(&rv);
                    move |k| {
                        Transition::Delete(k.to_string(), (*rv).clone(), roid)
                    }
                }),
                (T::strategy()).prop_map({
                    let rk = Rc::clone(&rk);
                    move |v| {
                        Transition::Delete((*rk).clone(), v, roid)
                    }
                }),
                (1..1000).prop_map({
                    let rk = Rc::clone(&rk);
                    let rv = Rc::clone(&rv);
                    move |oid| {
                        Transition::Delete((*rk).clone(), (*rv).clone(), oid as u32)
                    }
                }),

                Just(Transition::Get((*rk).clone(), (*rv).clone())),
                (".{1,10}").prop_map({
                    let rv = Rc::clone(&rv);
                    move |k| {
                        Transition::Get(k.to_string(), (*rv).clone())
                    }
                }),
                (T::strategy()).prop_map({
                    let rk = Rc::clone(&rk);
                    move |v| {
                        Transition::Get((*rk).clone(), v)
                    }
                }),
                (1..1000).prop_map({
                    let rk = Rc::clone(&rk);
                    let rv = Rc::clone(&rv);
                    move |oid| {
                        Transition::Get((*rk).clone(), (*rv).clone())
                    }
                }),
            ].boxed();
        }

        fn apply(mut state: ReferenceState<T>, transition: &Transition<T>) -> Self::State {
            match transition {
                Transition::BeginTransaction => {
                    state.in_transaction = true;
                },
                Transition::CommitTransaction => {
                    state.in_transaction = false;
                },
                Transition::Set(k, v, oid) => {
                    if !state.in_transaction {
                        return state;
                    }
                    let entry = state.data.entry(k.clone()).or_insert(HashMap::new());
                    let entry = entry.entry(v.clone()).or_insert(Vec::new());
                    if !entry.contains(oid) {
                        entry.push(*oid);
                    }
                },
                Transition::Delete(k, v, oid) => {
                    if !state.in_transaction {
                        return state;
                    }
                    if !state.data.contains_key(k) {
                        return state;
                    }
                    let entry = state.data.get_mut(k).unwrap();
                    if let Some(offsets) = entry.get_mut(v) {
                        offsets.retain(|x| *x != *oid);
                    }
                },
                Transition::Get(_, _) => {
                    // No-op
                },
            }
            state
        }
    }

    impl<T: PropTestValue + 'static> StateMachineTest for BlockfileMetadataIndex<T> {
        type SystemUnderTest = Self;
        type Reference = ReferenceStateMachineImpl<T>;

        fn init_test(_ref_state: &ReferenceState<T>) -> Self::SystemUnderTest {
            // We don't need to set up on _ref_state since we always initialize
            // ref_state to empty.
            return BlockfileMetadataIndex::<T>::new();
        }

        fn apply(
            mut state: Self::SystemUnderTest,
            ref_state: &ReferenceState<T>,
            transition: Transition<T>,
        ) -> Self::SystemUnderTest {
            match transition {
                Transition::BeginTransaction => {
                    let already_in_transaction = state.in_transaction();
                    let res = state.begin_transaction();
                    assert!(state.in_transaction());
                    if already_in_transaction {
                        assert!(res.is_err());
                    } else {
                        assert!(res.is_ok());
                    }
                },
                Transition::CommitTransaction => {
                    let in_transaction = state.in_transaction();
                    let res = state.commit_transaction();
                    assert_eq!(state.in_transaction(), false);
                    if !in_transaction {
                        assert!(res.is_err());
                    } else {
                        assert!(res.is_ok());
                    }
                },
                Transition::Set(k, v, oid) => {
                    let in_transaction = state.in_transaction();
                    let res = state.set(&k, v.clone(), oid);
                    if !in_transaction {
                        assert!(res.is_err());
                    } else {
                        assert!(res.is_ok());
                    }
                },
                Transition::Delete(k, v, oid) => {
                    let in_transaction = state.in_transaction();
                    let res = state.delete(&k, v, oid);
                    if !in_transaction {
                        assert!(res.is_err());
                    } else {
                        assert!(res.is_ok());
                    }
                },
                Transition::Get(k, v) => {
                    let in_transaction = state.in_transaction();
                    let res = state.get(&k, v.clone());
                    if in_transaction {
                        assert!(res.is_err());
                        return state;
                    }
                    let rbm = res.unwrap();
                    assert!(
                        ref_state.kv_rbm_eq(&rbm, &k, &v)
                    );
                },
            }
            state
        }

        fn check_invariants(state: &Self::SystemUnderTest, ref_state: &ReferenceState<T>) {
            assert_eq!(state.in_transaction(), ref_state.in_transaction);
            if state.in_transaction() {
                return;
            }
            for (metadata_key, metadata_values_hm) in &ref_state.data {
                for (metadata_value, ref_offset_ids) in metadata_values_hm {
                    assert!(vec_rbm_eq(
                        ref_offset_ids,
                        &state.get(metadata_key, metadata_value.clone()).unwrap()
                    ));
                }
            }
            // TODO once we have a way to iterate over all state in the blockfile,
            // add a similar check here to make sure that blockfile data is a
            // subset of reference state data.
        }
    }

    prop_state_machine! {
        #![proptest_config(Config {
            // Enable verbose mode to make the state machine test print the
            // transitions for each case.
            verbose: 0,
            cases: 100,
            .. Config::default()
        })]
        #[test]
        fn proptest_string_metadata_index(sequential 1..100 => BlockfileMetadataIndex<String>);

        #[test]
        fn proptest_boolean_metadata_index(sequential 1..100 => BlockfileMetadataIndex<bool>);

        #[test]
        fn proptest_numeric_metadata_index(sequential 1..100 => BlockfileMetadataIndex<FloatWrapper>);
    }
}