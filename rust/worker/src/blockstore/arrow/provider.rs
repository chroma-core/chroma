use super::{
    block::{self, delta::BlockDelta, Block},
    blockfile::{self, ArrowBlockfileReader, ArrowBlockfileWriter},
    sparse_index::{self, SparseIndex},
    types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
};
use crate::blockstore::{
    key::KeyWrapper,
    memory::storage::Readable,
    provider::{BlockfileProvider, CreateError, OpenError},
    BlockfileReader, BlockfileWriter, Key, Value,
};
use parking_lot::{Mutex, RwLock};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

/// A BlockFileProvider that creates ArrowBlockfiles (Arrow-backed blockfiles used for production).
/// For now, it keeps a simple local cache of blockfiles.
#[derive(Clone)]
pub(crate) struct ArrowBlockfileProvider {
    block_manager: BlockManager,
    sparse_index_manager: SparseIndexManager,
}

impl ArrowBlockfileProvider {
    pub(crate) fn new() -> Self {
        Self {
            block_manager: BlockManager::new(),
            sparse_index_manager: SparseIndexManager::new(),
        }
    }

    pub(crate) fn open<
        'new,
        K: Key + Into<KeyWrapper> + ArrowReadableKey<'new> + 'new,
        V: Value + Readable<'new> + ArrowReadableValue<'new> + 'new,
    >(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileReader<'new, K, V>, Box<OpenError>> {
        let sparse_index = self.sparse_index_manager.get(id);
        match sparse_index {
            Some(sparse_index) => Ok(BlockfileReader::ArrowBlockfileReader(
                ArrowBlockfileReader::new(*id, self.block_manager.clone(), sparse_index),
            )),
            None => {
                return Err(Box::new(OpenError::NotFound));
            }
        }
    }

    pub(crate) fn create<
        'new,
        K: Key + Into<KeyWrapper> + ArrowWriteableKey + 'new,
        V: Value + crate::blockstore::memory::storage::Writeable + ArrowWriteableValue + 'new,
    >(
        &self,
    ) -> Result<crate::blockstore::BlockfileWriter<K, V>, Box<CreateError>> {
        // Create a new blockfile and return a writer
        let new_id = Uuid::new_v4();
        let file = ArrowBlockfileWriter::new(
            new_id,
            self.block_manager.clone(),
            self.sparse_index_manager.clone(),
        );
        Ok(BlockfileWriter::ArrowBlockfileWriter(file))
    }

    pub(crate) fn fork<K: Key + ArrowWriteableKey, V: Value + ArrowWriteableValue>(
        &self,
        id: &uuid::Uuid,
    ) -> Result<crate::blockstore::BlockfileWriter<K, V>, Box<CreateError>> {
        let new_id = Uuid::new_v4();
        let new_sparse_index = self.sparse_index_manager.fork(id, new_id);
        let file = ArrowBlockfileWriter::from_sparse_index(
            new_id,
            self.block_manager.clone(),
            self.sparse_index_manager.clone(),
            new_sparse_index,
        );
        Ok(BlockfileWriter::ArrowBlockfileWriter(file))
    }
}

// impl BlockfileProvider for ArrowBlockfileProvider {
//     fn open<
//         'new,
//         K: Key + Into<KeyWrapper> + ArrowReadableKey<'new> + 'new,
//         V: Value + Readable<'new> + ArrowReadableValue<'new> + 'new,
//     >(
//         &self,
//         id: &uuid::Uuid,
//     ) -> Result<BlockfileReader<'new, K, V>, Box<OpenError>> {
//         let sparse_index = self.sparse_index_manager.get(id);
//         match sparse_index {
//             Some(sparse_index) => Ok(BlockfileReader::ArrowBlockfileReader(
//                 ArrowBlockfileReader::new(*id, self.block_manager.clone(), sparse_index),
//             )),
//             None => {
//                 return Err(Box::new(OpenError::NotFound));
//             }
//         }
//     }

//     fn create<
//         'new,
//         K: Key + Into<KeyWrapper> + ArrowWriteableKey + 'new,
//         V: Value + crate::blockstore::memory::storage::Writeable + ArrowWriteableValue + 'new,
//     >(
//         &self,
//     ) -> Result<crate::blockstore::BlockfileWriter<K, V>, Box<CreateError>> {
//         // Create a new blockfile and return a writer
//         let new_id = Uuid::new_v4();
//         let file = ArrowBlockfileWriter::new(
//             new_id,
//             self.block_manager.clone(),
//             self.sparse_index_manager.clone(),
//         );
//         Ok(BlockfileWriter::ArrowBlockfileWriter(file))
//     }

//     fn fork<K: Key + ArrowWriteableKey, V: Value + ArrowWriteableValue>(
//         &self,
//         id: &uuid::Uuid,
//     ) -> Result<crate::blockstore::BlockfileWriter<K, V>, Box<CreateError>> {
//         let new_id = Uuid::new_v4();
//         let new_sparse_index = self.sparse_index_manager.fork(id, new_id);
//         let file = ArrowBlockfileWriter::from_sparse_index(
//             new_id,
//             self.block_manager.clone(),
//             self.sparse_index_manager.clone(),
//             new_sparse_index,
//         );
//         Ok(BlockfileWriter::ArrowBlockfileWriter(file))
//     }
// }

/// A simple local cache of Arrow-backed blocks, the blockfile provider passes this
/// to the ArrowBlockfile when it creates a new blockfile. So that the blockfile can manage and access blocks
/// # Note
/// The implementation is currently very simple and not intended for robust production use. We should
/// introduce a more sophisticated cache that can handle tiered eviction and other features. This interface
/// is a placeholder for that.
#[derive(Clone)]
pub(super) struct BlockManager {
    read_cache: Arc<RwLock<HashMap<Uuid, Block>>>,
}

impl BlockManager {
    pub(super) fn new() -> Self {
        Self {
            read_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(super) fn create<K: ArrowWriteableKey, V: ArrowWriteableValue>(&self) -> BlockDelta {
        let new_block_id = Uuid::new_v4();
        let block = BlockDelta::new::<K, V>(new_block_id);
        block
    }

    pub(super) fn fork<KeyWrite: ArrowWriteableKey, ValueWrite: ArrowWriteableValue>(
        &self,
        id: &Uuid,
    ) -> BlockDelta {
        let cache_guard = self.read_cache.read();
        let block = cache_guard.get(id);
        let block = match block {
            Some(block) => block,
            None => {
                // TODO: Err - tried to fork a block not owned by this manager
                panic!("Tried to fork a block not owned by this manager")
            }
        };
        let new_id = Uuid::new_v4();
        let delta = BlockDelta::new::<KeyWrite, ValueWrite>(new_id);
        let populated_delta = self.fork_lifetime_scope::<KeyWrite, ValueWrite>(block, delta);
        populated_delta
    }

    fn fork_lifetime_scope<'new, KeyWrite, ValueWrite>(
        &self,
        block: &'new Block,
        delta: BlockDelta,
    ) -> BlockDelta
    where
        KeyWrite: ArrowWriteableKey,
        ValueWrite: ArrowWriteableValue,
    {
        block.to_block_delta::<KeyWrite::ReadableKey<'new>, ValueWrite::ReadableValue<'new>>(delta)
    }

    pub(super) fn commit<K: ArrowWriteableKey, V: ArrowWriteableValue>(&self, delta: &BlockDelta) {
        let record_batch = delta.finish::<K, V>();
        let block = Block::from_record_batch(delta.id, record_batch);
        self.read_cache.write().insert(block.id, block);
    }

    pub(super) fn get(&self, id: &Uuid) -> Option<Block> {
        self.read_cache.read().get(id).cloned()
    }
}

#[derive(Clone)]
pub(super) struct SparseIndexManager {
    cache: Arc<RwLock<HashMap<Uuid, SparseIndex>>>,
}

impl SparseIndexManager {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get(&self, id: &Uuid) -> Option<SparseIndex> {
        self.cache.read().get(id).cloned()
    }

    pub fn create(&self, id: &Uuid) -> SparseIndex {
        let index = SparseIndex::new(*id);
        index
    }

    pub fn commit(&self, index: SparseIndex) {
        self.cache.write().insert(index.id, index);
    }

    pub fn fork(&self, old_id: &Uuid, new_id: Uuid) -> SparseIndex {
        // TODO: error handling
        let original = self.get(old_id).unwrap();
        let forked = original.fork(new_id);
        self.cache.write().insert(new_id, forked.clone());
        forked
    }
}
