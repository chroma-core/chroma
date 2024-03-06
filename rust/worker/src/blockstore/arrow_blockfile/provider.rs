use super::block::Block;
use crate::blockstore::{KeyType, ValueType};
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

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

    pub(super) fn create_block(&self, key_type: KeyType, value_type: ValueType) -> Arc<Block> {
        let block = Arc::new(Block::new(Uuid::new_v4(), key_type, value_type));
        self.inner
            .write()
            .blocks
            .insert(block.get_id(), block.clone());
        block
    }

    pub(super) fn get_block(&self, id: &Uuid) -> Option<Arc<Block>> {
        self.inner.read().blocks.get(id).cloned()
    }
}
