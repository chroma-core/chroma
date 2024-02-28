use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use uuid::Uuid;

use super::super::provider::BlockfileProvider;
use crate::blockstore::arrow_blockfile::block::Block;
use crate::blockstore::arrow_blockfile::blockfile::ArrowBlockfile;
use crate::blockstore::types::Blockfile;

pub(super) struct ArrowBlockfileProvider {
    block_provider: ArrowBlockProvider,
}

impl BlockfileProvider for ArrowBlockfileProvider {
    fn new() -> Self {
        Self {
            block_provider: ArrowBlockProvider::new(),
        }
    }

    fn open(self, path: &str) -> Result<Box<dyn Blockfile>, Box<dyn crate::errors::ChromaError>> {
        unimplemented!();
    }

    fn create(
        &mut self,
        path: &str,
        key_type: crate::blockstore::types::KeyType,
        value_type: crate::blockstore::types::ValueType,
    ) -> Result<Box<dyn Blockfile>, Box<dyn crate::errors::ChromaError>> {
        Ok(Box::new(ArrowBlockfile::new(
            key_type,
            value_type,
            self.block_provider.clone(),
        )))
    }
}

struct ArrowBlockProviderInner {
    blocks: HashMap<Uuid, Arc<Block>>,
}

#[derive(Clone)]
pub(super) struct ArrowBlockProvider {
    inner: Arc<RwLock<ArrowBlockProviderInner>>,
}

impl ArrowBlockProvider {
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ArrowBlockProviderInner {
                blocks: HashMap::new(),
            })),
        }
    }

    pub(super) fn create_block(&self) -> Arc<Block> {
        let block = Arc::new(Block::new(Uuid::new_v4()));
        self.inner.write().blocks.insert(block.id, block.clone());
        block
    }

    pub(super) fn get_block(&self, id: &Uuid) -> Option<Arc<Block>> {
        self.inner.read().blocks.get(id).cloned()
    }
}
