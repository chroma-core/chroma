use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use uuid::Uuid;

use super::super::provider::BlockfileProvider;
use crate::blockstore::arrow_blockfile::block::Block;
use crate::blockstore::arrow_blockfile::blockfile::ArrowBlockfile;
use crate::blockstore::provider::{CreateError, OpenError};
use crate::blockstore::types::{Blockfile, KeyType, ValueType};

pub(super) struct ArrowBlockfileProvider {
    block_provider: ArrowBlockProvider,
    files: HashMap<String, Box<dyn Blockfile>>,
}

impl BlockfileProvider for ArrowBlockfileProvider {
    fn new() -> Self {
        Self {
            block_provider: ArrowBlockProvider::new(),
            files: HashMap::new(),
        }
    }

    fn open(&self, path: &str) -> Result<Box<dyn Blockfile>, Box<OpenError>> {
        match self.files.get(path) {
            Some(file) => Ok(file.clone()),
            None => Err(Box::new(OpenError::NotFound)),
        }
    }

    fn create(
        &mut self,
        path: &str,
        key_type: KeyType,
        value_type: ValueType,
    ) -> Result<Box<dyn Blockfile>, Box<CreateError>> {
        match self.files.get(path) {
            Some(_) => Err(Box::new(CreateError::AlreadyExists)),
            None => {
                let blockfile = Box::new(ArrowBlockfile::new(
                    key_type,
                    value_type,
                    self.block_provider.clone(),
                ));
                self.files.insert(path.to_string(), blockfile);
                Ok(self.files.get(path).unwrap().clone())
            }
        }
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
