use crate::blockstore::arrow::types::{ArrowReadableKey, ArrowWriteableKey};
use crate::blockstore::provider::BlockfileProvider;
use crate::blockstore::{key::KeyWrapper, BlockfileReader, BlockfileWriter, Key, Value};
use crate::errors::{ChromaError, ErrorCodes};

use roaring::RoaringBitmap;
use serde_json::value;
use std::{collections::HashMap, marker::PhantomData};
use thiserror::Error;

pub(crate) struct MetadataIndexWriter<K: Into<KeyWrapper>> {
    metadata_value_type: PhantomData<K>,
    blockfile_writer: BlockfileWriter,
    // We use a Vec<(KeyWrapper, RoaringBitmap)> instead of a HashMap because
    // f32 doesn't implement Eq or Hash. Eq is trivial since we disallow
    // about NaN values, but Hash is harder.
    // Linear scanning is fine since we will only ever have 2^16 values
    // and the expected case is much less than that.
    uncommitted_rbms: HashMap<String, Vec<(KeyWrapper, RoaringBitmap)>>,
}

impl<K: Into<KeyWrapper> + ArrowWriteableKey> MetadataIndexWriter<K> {
    pub fn new(init_blockfile_writer: BlockfileWriter) -> Self {
        MetadataIndexWriter {
            metadata_value_type: PhantomData,
            blockfile_writer: init_blockfile_writer,
            uncommitted_rbms: HashMap::new(),
        }
    }

    fn look_up_key_and_populate_uncommitted_rbms(
        &mut self,
        prefix: &str,
        key: &KeyWrapper,
    ) -> Result<(), Box<dyn ChromaError>> {
        if !self.uncommitted_rbms.contains_key(prefix) {
            self.uncommitted_rbms.insert(prefix.to_string(), vec![]);
        }
        let rbms = self.uncommitted_rbms.get_mut(prefix).unwrap();
        if !rbms.iter().any(|(k, _)| k == key) {
            rbms.push((key.clone(), RoaringBitmap::new()));
        }
        Ok(())
    }

    pub fn set(&mut self, key: &str, value: K, offset_id: u32) -> Result<(), Box<dyn ChromaError>> {
        let value = K::into(value);
        self.look_up_key_and_populate_uncommitted_rbms(key, &value)?;
        let rbms = self.uncommitted_rbms.get_mut(key).unwrap();
        let (_, rbm) = rbms.iter_mut().find(|(k, _)| k == &value).unwrap();
        rbm.insert(offset_id);
        Ok(())
    }

    pub fn delete(
        &mut self,
        key: &str,
        value: K,
        offset_id: u32,
    ) -> Result<(), Box<dyn ChromaError>> {
        let value = K::into(value);
        self.look_up_key_and_populate_uncommitted_rbms(key, &value)?;
        let rbms = self.uncommitted_rbms.get_mut(key).unwrap();
        let (_, rbm) = rbms.iter_mut().find(|(k, _)| k == &value).unwrap();
        rbm.remove(offset_id);
        Ok(())
    }

    pub async fn write_to_blockfile(&mut self) -> Result<(), Box<dyn ChromaError>> {
        for (key, rbms) in self.uncommitted_rbms.iter() {
            for (value, rbm) in rbms.iter() {
                self.blockfile_writer.set(key, value, rbm).await?;
            }
        }
        self.uncommitted_rbms.clear();
        Ok(())
    }
}

pub(crate) struct MetadataIndexReader<
    'me,
    K: Into<KeyWrapper> + From<&'me KeyWrapper> + ArrowReadableKey<'me> + Clone,
> {
    metadata_value_type: PhantomData<K>,
    blockfile_reader: BlockfileReader<'me, K, RoaringBitmap>,
}

impl<'me, K: Into<KeyWrapper> + From<&'me KeyWrapper> + ArrowReadableKey<'me> + Clone>
    MetadataIndexReader<'me, K>
{
    pub fn new(init_blockfile_reader: BlockfileReader<'me, K, RoaringBitmap>) -> Self {
        MetadataIndexReader {
            metadata_value_type: PhantomData,
            blockfile_reader: init_blockfile_reader,
        }
    }

    pub async fn get(
        &'me self,
        key: &str,
        value: K,
    ) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        let rbm = self.blockfile_reader.get(key, value).await;
        match rbm {
            Ok(rbm) => Ok(rbm),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_new_writer() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, &str>().unwrap();
        let _writer = MetadataIndexWriter::<&str>::new(blockfile_writer);
    }

    #[tokio::test]
    async fn test_new_writer_then_reader() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, &str>().unwrap();
        let writer_id = blockfile_writer.id();
        let md_writer = MetadataIndexWriter::<&str>::new(blockfile_writer);

        let blockfile_reader = provider
            .open::<&str, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let _reader = MetadataIndexReader::<&str>::new(blockfile_reader);
    }
}

//     #[test]
//     fn test_string_value_metadata_index_set_get() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<String>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key", "value".to_string(), 1).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key", "value".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(1), true);
//     }

//     #[test]
//     fn test_float_value_metadata_index_set_get() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<f32>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key", 1.0, 1).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key", 1.0).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(1), true);
//     }

//     #[test]
//     fn test_bool_value_metadata_index_set_get() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<bool>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key", true, 1).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key", true).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(1), true);
//     }

//     #[test]
//     fn test_string_value_metadata_index_set_delete_get() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<String>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key", "value".to_string(), 1).unwrap();
//         index.delete("key", "value".to_string(), 1).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key", "value".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 0);
//     }

//     #[test]
//     fn test_string_value_metadata_index_set_delete_set_get() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<String>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key", "value".to_string(), 1).unwrap();
//         index.delete("key", "value".to_string(), 1).unwrap();
//         index.set("key", "value".to_string(), 1).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key", "value".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(1), true);
//     }

//     #[test]
//     fn test_string_value_metadata_index_multiple_keys() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<String>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key1", "value".to_string(), 1).unwrap();
//         index.set("key2", "value".to_string(), 2).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key1", "value".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(1), true);

//         let bitmap = index.get("key2", "value".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(2), true);
//     }

//     #[test]
//     fn test_string_value_metadata_index_multiple_values() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<String>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key", "value1".to_string(), 1).unwrap();
//         index.set("key", "value2".to_string(), 2).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key", "value1".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(1), true);

//         let bitmap = index.get("key", "value2".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(2), true);
//     }

//     #[test]
//     fn test_string_value_metadata_index_delete_in_standalone_transaction() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<String>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key", "value".to_string(), 1).unwrap();
//         index.commit_transaction().unwrap();

//         index.begin_transaction().unwrap();
//         index.delete("key", "value".to_string(), 1).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key", "value".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 0);
//     }

//     use proptest::prelude::*;
//     use proptest::test_runner::Config;
//     use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
//     use std::rc::Rc;

//     // Utility function to check if a Vec<u32> and RoaringBitmap contain equivalent
//     // sets.
//     fn vec_rbm_eq(a: &Vec<u32>, b: &RoaringBitmap) -> bool {
//         if a.len() != b.len() as usize {
//             return false;
//         }
//         for offset in a {
//             if !b.contains(*offset) {
//                 return false;
//             }
//         }
//         for offset in b {
//             if !a.contains(&offset) {
//                 return false;
//             }
//         }
//         return true;
//     }

//     pub(crate) trait PropTestValue:
//         MetadataIndexValue + PartialEq + Eq + Clone + std::hash::Hash + std::fmt::Debug
//     {
//         fn strategy() -> BoxedStrategy<Self>;
//     }

//     impl PropTestValue for String {
//         fn strategy() -> BoxedStrategy<Self> {
//             ".{0,10}".prop_map(|x| x.to_string()).boxed()
//         }
//     }

//     impl PropTestValue for bool {
//         fn strategy() -> BoxedStrategy<Self> {
//             prop_oneof![Just(true), Just(false)].boxed()
//         }
//     }

//     // f32 doesn't implement Hash or Eq so we need to wrap it to use
//     // in our reference state machine's HashMap. This is a bit of a hack
//     // but only used in tests.
//     #[derive(Clone, Debug, PartialEq)]
//     struct FloatWrapper(f32);

//     impl std::hash::Hash for FloatWrapper {
//         fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
//             self.0.to_bits().hash(state);
//         }
//     }

//     impl Eq for FloatWrapper {}

//     impl MetadataIndexValue for FloatWrapper {
//         fn to_blockfile_key(&self) -> Key {
//             self.0.to_blockfile_key()
//         }
//     }

//     impl PropTestValue for FloatWrapper {
//         fn strategy() -> BoxedStrategy<Self> {
//             (0..1000).prop_map(|x| FloatWrapper(x as f32)).boxed()
//         }
//     }

//     #[derive(Clone, Debug)]
//     pub(crate) enum Transition<T: PropTestValue> {
//         BeginTransaction,
//         CommitTransaction,
//         Set(String, T, u32),
//         Delete(String, T, u32),
//         Get(String, T),
//     }

//     #[derive(Clone, Debug)]
//     pub(crate) struct ReferenceState<T: PropTestValue> {
//         // Are we in a transaction?
//         in_transaction: bool,
//         // {metadata key: {metadata value: offset ids}}
//         data: HashMap<String, HashMap<T, Vec<u32>>>,
//     }

//     impl<T: PropTestValue> ReferenceState<T> {
//         fn new() -> Self {
//             ReferenceState {
//                 in_transaction: false,
//                 data: HashMap::new(),
//             }
//         }

//         fn key_from_random_number(self: &Self, r1: usize) -> Option<String> {
//             let keys = self.data.keys().collect::<Vec<&String>>();
//             if keys.len() == 0 {
//                 return None;
//             }
//             Some(keys[r1 % keys.len()].clone())
//         }

//         fn key_and_value_from_random_numbers(
//             self: &Self,
//             r1: usize,
//             r2: usize,
//         ) -> Option<(String, T)> {
//             let k = self.key_from_random_number(r1)?;

//             let values = self.data.get(&k)?.keys().collect::<Vec<&T>>();
//             if values.len() == 0 {
//                 return None;
//             }
//             let v = values[r2 % values.len()];
//             Some((k.clone(), v.clone()))
//         }

//         fn key_and_value_and_entry_from_random_numbers(
//             self: &Self,
//             r1: usize,
//             r2: usize,
//             r3: usize,
//         ) -> Option<(String, T, u32)> {
//             let (k, v) = self.key_and_value_from_random_numbers(r1, r2)?;

//             let offsets = self.data.get(&k)?.get(&v)?;
//             if offsets.len() == 0 {
//                 return None;
//             }
//             let oid = offsets[r3 % offsets.len()];
//             Some((k.clone(), v.clone(), oid))
//         }

//         fn kv_rbm_eq(self: &Self, rbm: &RoaringBitmap, k: &str, v: &T) -> bool {
//             match self.data.get(k) {
//                 Some(vv) => match vv.get(v) {
//                     Some(rbm2) => vec_rbm_eq(rbm2, rbm),
//                     None => rbm.is_empty(),
//                 },
//                 None => rbm.is_empty(),
//             }
//         }
//     }

//     pub(crate) struct ReferenceStateMachineImpl<T: PropTestValue> {
//         value_type: PhantomData<T>,
//     }

//     impl<T: PropTestValue + 'static> ReferenceStateMachine for ReferenceStateMachineImpl<T> {
//         type State = ReferenceState<T>;
//         type Transition = Transition<T>;

//         fn init_state() -> BoxedStrategy<Self::State> {
//             return Just(ReferenceState::<T>::new()).boxed();
//         }

//         fn transitions(state: &ReferenceState<T>) -> BoxedStrategy<Transition<T>> {
//             let r = state.key_and_value_and_entry_from_random_numbers(0, 0, 0);
//             if r.is_none() {
//                 // Nothing in the reference state yet.
//                 return prop_oneof![
//                     Just(Transition::BeginTransaction),
//                     Just(Transition::CommitTransaction),
//                     // Set, get, delete random values
//                     (".{0,10}", T::strategy(), 1..1000).prop_map(move |(k, v, oid)| {
//                         Transition::Set(k.to_string(), v, oid as u32)
//                     }),
//                     (".{0,10}", T::strategy(), 1..1000).prop_map(move |(k, v, oid)| {
//                         Transition::Delete(k.to_string(), v, oid as u32)
//                     }),
//                     (".{0,10}", T::strategy())
//                         .prop_map(move |(k, v)| { Transition::Get(k.to_string(), v) }),
//                 ]
//                 .boxed();
//             }

//             let state = Rc::new(state.clone());
//             return prop_oneof![
//                 Just(Transition::BeginTransaction),
//                 Just(Transition::CommitTransaction),
//                 // Set, get, delete random values
//                 (".{0,10}", T::strategy(), 1..1000).prop_map(move |(k, v, oid)| {
//                     Transition::Set(k.to_string(), v, oid as u32)
//                 }),
//                 (".{0,10}", T::strategy(), 1..1000).prop_map(move |(k, v, oid)| {
//                     Transition::Delete(k.to_string(), v, oid as u32)
//                 }),
//                 (".{0,10}", T::strategy()).prop_map(move |(k, v)| {
//                     Transition::Get(k.to_string(), v)
//                 }),

//                 // Sets on values in the reference state
//                 (0..usize::MAX, T::strategy(), 1..1000).prop_map({
//                     let state = Rc::clone(&state);
//                     move |(r1, v, oid)| {
//                         match state.key_from_random_number(r1) {
//                             Some(k) => Transition::Set(k, v, oid as u32),
//                             None => panic!("Error in the test harness: Setting on key from ref state")
//                         }
//                     }
//                 }),
//                 (0..usize::MAX, 0..usize::MAX, 1..1000).prop_map({
//                     let state = Rc::clone(&state);
//                     move |(r1, r2, oid)| {
//                         match state.key_and_value_from_random_numbers(r1, r2) {
//                             Some((k, v)) => Transition::Set(k, v, oid as u32),
//                             None => panic!("Error in the test harness: Setting on key and value from ref state")
//                         }
//                     }
//                 }),
//                 (0..usize::MAX, 0..usize::MAX, 0..usize::MAX).prop_map({
//                     let state = Rc::clone(&state);
//                     move |(r1, r2, r3)| {
//                         match state.key_and_value_and_entry_from_random_numbers(r1, r2, r3) {
//                             Some((k, v, oid)) => Transition::Set(k, v, oid),
//                             None => panic!("Error in the test harness: Setting on key, value, and entry from ref state")
//                         }
//                     }
//                 }),

//                 // Gets on values in the reference state
//                 (0..usize::MAX, T::strategy()).prop_map({
//                     let state = Rc::clone(&state);
//                     move |(r1, v)| {
//                         match state.key_from_random_number(r1) {
//                             Some(k) => Transition::Get(k, v),
//                             None => panic!("Error in the test harness: Getting on key from ref state")
//                         }
//                     }
//                 }),
//                 (0..usize::MAX, 0..usize::MAX).prop_map({
//                     let state = Rc::clone(&state);
//                     move |(r1, r2)| {
//                         match state.key_and_value_from_random_numbers(r1, r2) {
//                             Some((k, v)) => Transition::Get(k, v),
//                             None => panic!("Error in the test harness: Getting on key and value from ref state")
//                         }
//                     }
//                 }),

//                 // Deletes on values in the reference state
//                 (0..usize::MAX, T::strategy(), 1..1000).prop_map({
//                     let state = Rc::clone(&state);
//                     move |(r1, v, oid)| {
//                         match state.key_from_random_number(r1) {
//                             Some(k) => Transition::Delete(k, v, oid as u32),
//                             None => panic!("Error in the test harness: Deleting on key from ref state")
//                         }
//                     }
//                 }),
//                 (0..usize::MAX, 0..usize::MAX, 1..1000).prop_map({
//                     let state = Rc::clone(&state);
//                     move |(r1, r2, oid)| {
//                         match state.key_and_value_from_random_numbers(r1, r2) {
//                             Some((k, v)) => Transition::Delete(k, v, oid as u32),
//                             None => panic!("Error in the test harness: Deleting on key and value from ref state")
//                         }
//                     }
//                 }),
//                 (0..usize::MAX, 0..usize::MAX, 0..usize::MAX).prop_map({
//                     let state = Rc::clone(&state);
//                     move |(r1, r2, r3)| {
//                         match state.key_and_value_and_entry_from_random_numbers(r1, r2, r3) {
//                             Some((k, v, oid)) => Transition::Delete(k, v, oid as u32),
//                             None => panic!("Error in the test harness: Deleting on key, value, and entry from ref state")
//                         }
//                     }
//                 }),
//             ].boxed();
//         }

//         fn apply(mut state: ReferenceState<T>, transition: &Transition<T>) -> Self::State {
//             match transition {
//                 Transition::BeginTransaction => {
//                     state.in_transaction = true;
//                 }
//                 Transition::CommitTransaction => {
//                     state.in_transaction = false;
//                 }
//                 Transition::Set(k, v, oid) => {
//                     if !state.in_transaction {
//                         return state;
//                     }
//                     let entry = state.data.entry(k.clone()).or_insert(HashMap::new());
//                     let entry = entry.entry(v.clone()).or_insert(Vec::new());
//                     if !entry.contains(oid) {
//                         entry.push(*oid);
//                     }
//                 }
//                 Transition::Delete(k, v, oid) => {
//                     if !state.in_transaction {
//                         return state;
//                     }
//                     if !state.data.contains_key(k) {
//                         return state;
//                     }
//                     let entry = state.data.get_mut(k).unwrap();
//                     if let Some(offsets) = entry.get_mut(v) {
//                         offsets.retain(|x| *x != *oid);
//                     }

//                     // Remove the offset ID list if it's empty.
//                     let offsets_count = entry.get(v).map(|x| x.len()).unwrap_or(0);
//                     if offsets_count == 0 {
//                         entry.remove(v);
//                     }

//                     // Remove the entire hashmap for the key if it's empty.
//                     if entry.is_empty() {
//                         state.data.remove(k);
//                     }
//                 }
//                 Transition::Get(_, _) => {
//                     // No-op
//                 }
//             }
//             state
//         }
//     }

//     impl<T: PropTestValue + 'static> StateMachineTest for BlockfileMetadataIndex<T> {
//         type SystemUnderTest = Self;
//         type Reference = ReferenceStateMachineImpl<T>;

//         fn init_test(_ref_state: &ReferenceState<T>) -> Self::SystemUnderTest {
//             // We don't need to set up on _ref_state since we always initialize
//             // ref_state to empty.
//             let mut provider = HashMapBlockfileProvider::new();
//             let blockfile = provider
//                 .create("test", KeyType::String, ValueType::RoaringBitmap)
//                 .unwrap();
//             return BlockfileMetadataIndex::<T>::new(blockfile);
//         }

//         fn apply(
//             mut state: Self::SystemUnderTest,
//             ref_state: &ReferenceState<T>,
//             transition: Transition<T>,
//         ) -> Self::SystemUnderTest {
//             match transition {
//                 Transition::BeginTransaction => {
//                     let already_in_transaction = state.in_transaction();
//                     let res = state.begin_transaction();
//                     assert!(state.in_transaction());
//                     if already_in_transaction {
//                         assert!(res.is_err());
//                     } else {
//                         assert!(res.is_ok());
//                     }
//                 }
//                 Transition::CommitTransaction => {
//                     let in_transaction = state.in_transaction();
//                     let res = state.commit_transaction();
//                     assert_eq!(state.in_transaction(), false);
//                     if !in_transaction {
//                         assert!(res.is_err());
//                     } else {
//                         assert!(res.is_ok());
//                     }
//                 }
//                 Transition::Set(k, v, oid) => {
//                     let in_transaction = state.in_transaction();
//                     let res = state.set(&k, v.clone(), oid);
//                     if !in_transaction {
//                         assert!(res.is_err());
//                     } else {
//                         assert!(res.is_ok());
//                     }
//                 }
//                 Transition::Delete(k, v, oid) => {
//                     let in_transaction = state.in_transaction();
//                     let res = state.delete(&k, v, oid);
//                     if !in_transaction {
//                         assert!(res.is_err());
//                     } else {
//                         assert!(res.is_ok());
//                     }
//                 }
//                 Transition::Get(k, v) => {
//                     let in_transaction = state.in_transaction();
//                     let res = state.get(&k, v.clone());
//                     if in_transaction {
//                         assert!(res.is_err());
//                         return state;
//                     }
//                     let rbm = res.unwrap();
//                     assert!(ref_state.kv_rbm_eq(&rbm, &k, &v));
//                 }
//             }
//             state
//         }

//         fn check_invariants(state: &Self::SystemUnderTest, ref_state: &ReferenceState<T>) {
//             assert_eq!(state.in_transaction(), ref_state.in_transaction);
//             if state.in_transaction() {
//                 return;
//             }
//             for (metadata_key, metadata_values_hm) in &ref_state.data {
//                 for (metadata_value, ref_offset_ids) in metadata_values_hm {
//                     assert!(vec_rbm_eq(
//                         ref_offset_ids,
//                         &state.get(metadata_key, metadata_value.clone()).unwrap()
//                     ));
//                 }
//             }
//             // TODO once we have a way to iterate over all state in the blockfile,
//             // add a similar check here to make sure that blockfile data is a
//             // subset of reference state data.
//         }
//     }

//     prop_state_machine! {
//         #![proptest_config(Config {
//             // Enable verbose mode to make the state machine test print the
//             // transitions for each case.
//             verbose: 0,
//             cases: 100,
//             .. Config::default()
//         })]
//         #[test]
//         fn proptest_string_metadata_index(sequential 1..100 => BlockfileMetadataIndex<String>);

//         #[test]
//         fn proptest_boolean_metadata_index(sequential 1..100 => BlockfileMetadataIndex<bool>);

//         #[test]
//         fn proptest_numeric_metadata_index(sequential 1..100 => BlockfileMetadataIndex<FloatWrapper>);
//     }
// }
