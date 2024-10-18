use super::arrow::blockfile::{ArrowBlockfileReader, ArrowBlockfileWriter};
use super::arrow::flusher::ArrowBlockfileFlusher;
use super::arrow::types::{
    ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue,
};
use super::key::{InvalidKeyConversion, KeyWrapper};
use super::memory::reader_writer::{
    MemoryBlockfileFlusher, MemoryBlockfileReader, MemoryBlockfileWriter,
};
use super::memory::storage::{Readable, Writeable};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::DataRecord;
use roaring::RoaringBitmap;
use std::fmt::{Debug, Display};
use std::mem::size_of;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlockfileError {
    #[error("Key not found")]
    NotFoundError,
    #[error("Invalid Key Type")]
    InvalidKeyType,
    #[error("Invalid Value Type")]
    InvalidValueType,
    #[error("Transaction already in progress")]
    TransactionInProgress,
    #[error("Transaction not in progress")]
    TransactionNotInProgress,
    #[error("Block not found")]
    BlockNotFound,
}

impl ChromaError for BlockfileError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockfileError::NotFoundError
            | BlockfileError::InvalidKeyType
            | BlockfileError::InvalidValueType => ErrorCodes::InvalidArgument,
            BlockfileError::TransactionInProgress | BlockfileError::TransactionNotInProgress => {
                ErrorCodes::FailedPrecondition
            }
            BlockfileError::BlockNotFound => ErrorCodes::Internal,
        }
    }
}

// ===== Key Types =====
pub trait Key: PartialEq + Debug + Display + Into<KeyWrapper> + Clone {
    fn get_size(&self) -> usize;
}

impl Key for &str {
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl Key for f32 {
    fn get_size(&self) -> usize {
        4
    }
}

impl Key for bool {
    fn get_size(&self) -> usize {
        1
    }
}

impl Key for u32 {
    fn get_size(&self) -> usize {
        4
    }
}

pub trait Value: Clone {
    fn get_size(&self) -> usize;
}

impl Value for Vec<u32> {
    fn get_size(&self) -> usize {
        self.len() * size_of::<u32>()
    }
}

impl Value for &[u32] {
    fn get_size(&self) -> usize {
        std::mem::size_of_val(*self)
    }
}

impl Value for &str {
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl Value for String {
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl Value for u32 {
    fn get_size(&self) -> usize {
        4
    }
}

impl Value for RoaringBitmap {
    fn get_size(&self) -> usize {
        self.serialized_size()
    }
}

impl Value for &RoaringBitmap {
    fn get_size(&self) -> usize {
        self.serialized_size()
    }
}

impl<'a> Value for DataRecord<'a> {
    fn get_size(&self) -> usize {
        DataRecord::get_size(self)
    }
}

impl<'a> Value for &DataRecord<'a> {
    fn get_size(&self) -> usize {
        DataRecord::get_size(self)
    }
}

#[derive(Clone)]
pub enum BlockfileWriter {
    MemoryBlockfileWriter(MemoryBlockfileWriter),
    ArrowBlockfileWriter(ArrowBlockfileWriter),
}

impl BlockfileWriter {
    pub async fn commit<
        K: Key + Into<KeyWrapper> + ArrowWriteableKey,
        V: Value + Writeable + ArrowWriteableValue,
    >(
        self,
    ) -> Result<BlockfileFlusher, Box<dyn ChromaError>> {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(writer) => match writer.commit() {
                Ok(flusher) => Ok(BlockfileFlusher::MemoryBlockfileFlusher(flusher)),
                Err(e) => Err(e),
            },
            BlockfileWriter::ArrowBlockfileWriter(writer) => match writer.commit::<K, V>().await {
                Ok(flusher) => Ok(BlockfileFlusher::ArrowBlockfileFlusher(flusher)),
                Err(e) => Err(e),
            },
        }
    }

    pub async fn set<
        K: Key + Into<KeyWrapper> + ArrowWriteableKey,
        V: Value + Writeable + ArrowWriteableValue,
    >(
        &self,
        prefix: &str,
        key: K,
        value: V,
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(writer) => writer.set(prefix, key, value),
            BlockfileWriter::ArrowBlockfileWriter(writer) => writer.set(prefix, key, value).await,
        }
    }

    pub async fn delete<
        K: Key + Into<KeyWrapper> + ArrowWriteableKey,
        V: Value + Writeable + ArrowWriteableValue,
    >(
        &self,
        prefix: &str,
        key: K,
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(writer) => writer.delete::<K, V>(prefix, key),
            BlockfileWriter::ArrowBlockfileWriter(writer) => {
                writer.delete::<K, V>(prefix, key).await
            }
        }
    }

    pub fn id(&self) -> uuid::Uuid {
        match self {
            BlockfileWriter::MemoryBlockfileWriter(writer) => writer.id(),
            BlockfileWriter::ArrowBlockfileWriter(writer) => writer.id(),
        }
    }
}

pub enum BlockfileFlusher {
    MemoryBlockfileFlusher(MemoryBlockfileFlusher),
    ArrowBlockfileFlusher(ArrowBlockfileFlusher),
}

impl BlockfileFlusher {
    pub async fn flush<
        K: Key + Into<KeyWrapper> + ArrowWriteableKey,
        V: Value + Writeable + ArrowWriteableValue,
    >(
        self,
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            BlockfileFlusher::MemoryBlockfileFlusher(_) => Ok(()),
            BlockfileFlusher::ArrowBlockfileFlusher(flusher) => flusher.flush::<K, V>().await,
        }
    }

    pub fn id(&self) -> uuid::Uuid {
        match self {
            BlockfileFlusher::MemoryBlockfileFlusher(flusher) => flusher.id(),
            BlockfileFlusher::ArrowBlockfileFlusher(flusher) => flusher.id(),
        }
    }
}

#[derive(Clone)]
pub enum BlockfileReader<
    'me,
    K: Key + Into<KeyWrapper> + ArrowReadableKey<'me>,
    V: Value + ArrowReadableValue<'me>,
> {
    MemoryBlockfileReader(MemoryBlockfileReader<K, V>),
    ArrowBlockfileReader(ArrowBlockfileReader<'me, K, V>),
}

impl<
        'referred_data,
        K: Key
            + Into<KeyWrapper>
            + TryFrom<&'referred_data KeyWrapper, Error = InvalidKeyConversion>
            + ArrowReadableKey<'referred_data>,
        V: Value + Readable<'referred_data> + ArrowReadableValue<'referred_data>,
    > BlockfileReader<'referred_data, K, V>
{
    pub async fn get(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<V, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get(prefix, key).await,
        }
    }

    pub async fn contains(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<bool, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::ArrowBlockfileReader(reader) => reader.contains(prefix, key).await,
            BlockfileReader::MemoryBlockfileReader(reader) => Ok(reader.contains(prefix, key)),
        }
    }

    pub async fn count(&'referred_data self) -> Result<usize, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.count(),
            BlockfileReader::ArrowBlockfileReader(reader) => {
                let count = reader.count().await;
                match count {
                    Ok(c) => Ok(c),
                    Err(_) => Err(Box::new(BlockfileError::BlockNotFound)),
                }
            }
        }
    }

    pub async fn get_by_prefix(
        &'referred_data self,
        prefix: &str,
    ) -> Result<Vec<(K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_by_prefix(prefix),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get_by_prefix(prefix).await,
        }
    }

    pub async fn get_gt(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_gt(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get_gt(prefix, key).await,
        }
    }

    pub async fn get_lt(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_lt(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get_lt(prefix, key).await,
        }
    }

    pub async fn get_gte(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_gte(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get_gte(prefix, key).await,
        }
    }

    pub async fn get_lte(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<Vec<(K, V)>, Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_lte(prefix, key),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get_lte(prefix, key).await,
        }
    }

    pub async fn get_at_index(
        &'referred_data self,
        index: usize,
    ) -> Result<(&str, K, V), Box<dyn ChromaError>> {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.get_at_index(index),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.get_at_index(index).await,
        }
    }

    pub fn id(&self) -> uuid::Uuid {
        match self {
            BlockfileReader::MemoryBlockfileReader(reader) => reader.id(),
            BlockfileReader::ArrowBlockfileReader(reader) => reader.id(),
        }
    }

    pub async fn load_blocks_for_keys(&self, prefixes: &[&str], keys: &[K]) {
        match self {
            BlockfileReader::MemoryBlockfileReader(_reader) => unimplemented!(),
            BlockfileReader::ArrowBlockfileReader(reader) => {
                reader.load_blocks_for_keys(prefixes, keys).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chroma_storage::local::LocalStorage;
    use chroma_storage::Storage;
    use futures::executor::block_on;
    use proptest::prelude::*;
    use proptest::test_runner::Config;
    use proptest_state_machine::prop_state_machine;
    use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::arrow::provider::ArrowBlockfileProvider;
    use crate::BlockfileWriter;
    use chroma_cache::new_cache_for_test;

    #[derive(Clone, Debug)]
    pub enum Transition {
        Set(String, String, String),
        Delete(String, String),
        Commit,
    }

    #[derive(Debug, Clone)]
    pub struct RefState {
        /// This field is not used in the reference impl, but gives a block size to the real blockfile impl
        generated_max_block_size_bytes: usize,
        store: BTreeMap<(String, String), String>,
        last_commit: Option<BTreeMap<(String, String), String>>,
    }

    pub struct BlockfileWriterStateMachine {}

    impl ReferenceStateMachine for BlockfileWriterStateMachine {
        type State = RefState;
        type Transition = Transition;

        fn init_state() -> proptest::prelude::BoxedStrategy<Self::State> {
            (500..1_000usize)
                .prop_map(|block_size_bytes| RefState {
                    generated_max_block_size_bytes: block_size_bytes,
                    store: BTreeMap::new(),
                    last_commit: None,
                })
                .boxed()
        }

        fn transitions(state: &Self::State) -> proptest::prelude::BoxedStrategy<Self::Transition> {
            let keys = state.store.keys().cloned().collect::<Vec<_>>();

            let key_and_prefix_generator = (
                "[0-9a-zA-Z]{1,10}",
                "[0-9a-zA-Z]{1,10}",
                "[0-9a-zA-Z]{1,100}",
            );

            if keys.is_empty() {
                return key_and_prefix_generator
                    .prop_map(|(prefix, key, value)| Transition::Set(prefix, key, value))
                    .boxed();
            }

            prop_oneof![
                4 => key_and_prefix_generator.prop_map(|(prefix, key, value)| {
                    Transition::Set(prefix, key, value)
                }),
                2 => (0..keys.len()).prop_map(move |i| {
                    Transition::Delete(keys[i].0.clone(), keys[i].1.clone())
                }),
                1 => Just(Transition::Commit)
            ]
            .boxed()
        }

        fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
            match transition {
                Transition::Set(prefix, key, value) => {
                    state
                        .store
                        .insert((prefix.clone(), key.clone()), value.clone());
                }
                Transition::Delete(prefix, key) => {
                    state.store.remove(&(prefix.clone(), key.clone())); // todo: need clones?
                }
                Transition::Commit => {
                    state.last_commit = Some(state.store.clone());
                }
            }

            state
        }
    }

    struct BlockfileWriterWrapper {
        tmp_dir: TempDir,
        provider: ArrowBlockfileProvider,
        last_blockfile_id: Option<Uuid>,
        writer: BlockfileWriter,
    }

    impl StateMachineTest for BlockfileWriterWrapper {
        type SystemUnderTest = Self;

        type Reference = BlockfileWriterStateMachine;

        fn init_test(
            ref_state: &<Self::Reference as proptest_state_machine::ReferenceStateMachine>::State,
        ) -> Self::SystemUnderTest {
            let tmp_dir = tempfile::tempdir().unwrap();
            let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
            let block_cache = new_cache_for_test();
            let sparse_index_cache = new_cache_for_test();
            let provider = ArrowBlockfileProvider::new(
                storage,
                ref_state.generated_max_block_size_bytes,
                block_cache,
                sparse_index_cache,
            );
            let writer = provider.create::<&str, String>().unwrap();

            BlockfileWriterWrapper {
                tmp_dir,
                provider,
                last_blockfile_id: None,
                writer,
            }
        }

        fn apply(
            mut state: Self::SystemUnderTest,
            ref_state: &<Self::Reference as proptest_state_machine::ReferenceStateMachine>::State,
            transition: <Self::Reference as proptest_state_machine::ReferenceStateMachine>::Transition,
        ) -> Self::SystemUnderTest {
            match transition {
                Transition::Set(prefix, key, value) => {
                    block_on(state.writer.set(prefix.as_str(), key.as_str(), value)).unwrap();
                }
                Transition::Delete(prefix, key) => {
                    block_on(
                        state
                            .writer
                            .delete::<&str, String>(prefix.as_str(), key.as_str()),
                    )
                    .unwrap();
                }
                Transition::Commit => {
                    let id = state.writer.id();
                    let flusher = block_on(state.writer.commit::<&str, String>()).unwrap();
                    block_on(flusher.flush::<&str, String>()).unwrap();

                    state.last_blockfile_id = Some(id);
                    state.writer = block_on(state.provider.fork::<&str, String>(&id)).unwrap();
                }
            }

            state
        }

        fn check_invariants(
            state: &Self::SystemUnderTest,
            ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        ) {
            if ref_state.last_commit.is_none() || state.last_blockfile_id.is_none() {
                return;
            }

            let ref_last_commit = ref_state.last_commit.as_ref().unwrap();
            let last_blockfile_id = state.last_blockfile_id.unwrap();

            let reader = block_on(state.provider.open::<&str, &str>(&last_blockfile_id)).unwrap();

            // Check count
            assert_eq!(block_on(reader.count()).unwrap(), ref_last_commit.len());

            // Check that entries are ordered and match expected
            if let Some(min_key) = ref_last_commit.keys().next() {
                let all_entries =
                    block_on(reader.get_gte(min_key.0.as_str(), min_key.1.as_str())).unwrap();

                for (blockfile_entry, expected_entry) in
                    all_entries.iter().zip(ref_last_commit.iter())
                {
                    assert_eq!(blockfile_entry.0, expected_entry.0 .1); // key matches
                    assert_eq!(blockfile_entry.1, expected_entry.1); // value matches
                }
            }
        }
    }

    prop_state_machine! {
        #![proptest_config(Config::default())]

        #[test]
        fn blockfile_writer_test(
            sequential
            // The number of transitions to be generated for each case.
            1..100
            // Macro's boilerplate to separate the following identifier.
            =>
            // The name of the type that implements `StateMachineTest`.
            BlockfileWriterWrapper
        );
    }
}
