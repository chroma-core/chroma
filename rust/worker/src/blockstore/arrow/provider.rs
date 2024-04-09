use super::block::{
    delta::{BlockDelta, BlockDeltaKey, BlockDeltaValue},
    Block,
};
use crate::blockstore::{
    key::KeyWrapper,
    memory::storage::Readable,
    provider::{BlockfileProvider, CreateError, OpenError},
    BlockfileReader, Key, Value,
};
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

/// A BlockFileProvider that creates ArrowBlockfiles (Arrow-backed blockfiles used for production).
/// For now, it keeps a simple local cache of blockfiles.
pub(super) struct ArrowBlockfileProvider {
    // block_provider: ArrowBlockProvider,
}

impl BlockfileProvider for ArrowBlockfileProvider {
    fn new() -> Self {
        Self {
            // block_provider: ArrowBlockProvider::new(),
        }
    }

    fn open<'new, K: Key + Into<KeyWrapper> + 'new, V: Value + Readable<'new> + 'new>(
        &self,
        id: &uuid::Uuid,
    ) -> Result<BlockfileReader<K, V>, Box<OpenError>> {
        todo!();
    }

    fn create<
        'new,
        K: Key + Into<KeyWrapper> + 'new,
        V: Value + crate::blockstore::memory::storage::Writeable + 'new,
    >(
        &self,
    ) -> Result<crate::blockstore::BlockfileWriter<K, V>, Box<CreateError>> {
        todo!()
    }

    fn fork<K: Key, V: Value>(
        &self,
        id: &uuid::Uuid,
    ) -> Result<crate::blockstore::BlockfileWriter<K, V>, Box<CreateError>> {
        todo!()
    }
}

/// A simple local cache of Arrow-backed blocks, the blockfile provider passes this
/// to the ArrowBlockfile when it creates a new blockfile. So that the blockfile can manage and access blocks
/// # Note
/// The implementation is currently very simple and not intended for robust production use. We should
/// introduce a more sophisticated cache that can handle tiered eviction and other features. This interface
/// is a placeholder for that.
#[derive(Clone)]
pub(super) struct BlockManager {
    read_cache: Arc<RwLock<HashMap<Uuid, Block>>>,
    write_cache: Arc<RwLock<HashMap<Uuid, BlockDelta>>>,
}

impl BlockManager {
    pub(super) fn new() -> Self {
        Self {
            read_cache: Arc::new(RwLock::new(HashMap::new())),
            write_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(super) fn create<K: BlockDeltaKey, V: BlockDeltaValue>(&self) -> BlockDelta {
        let new_block_id = Uuid::new_v4();
        let block = BlockDelta::new::<K, V>(new_block_id);
        self.write_cache.write().insert(block.id, block.clone());
        block
    }

    pub(super) fn commit<K: BlockDeltaKey, V: BlockDeltaValue>(&self, delta: BlockDelta) {
        let delta = self.write_cache.write().remove(&delta.id);
        match delta {
            Some(delta) => {
                let record_batch = delta.finish::<K, V>();
                let block = Block::from_record_batch(delta.id, record_batch);
                self.read_cache.write().insert(block.id, block);
            }
            None => {
                // TODO: Err - tried to commit a delta not owned by this manager
            }
        }
    }

    pub(super) fn get<K: BlockDeltaKey, V: BlockDeltaValue>(&self, id: &Uuid) -> Option<Block> {
        self.read_cache.read().get(id).cloned()
    }
}
