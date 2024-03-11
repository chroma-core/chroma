use super::{block::Block, blockfile::ArrowBlockfile};
use crate::blockstore::{
    provider::{BlockfileProvider, CreateError, OpenError},
    Blockfile, KeyType, ValueType,
};
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

/// A BlockFileProvider that creates ArrowBlockfiles (Arrow-backed blockfiles used for production).
/// For now, it keeps a simple local cache of blockfiles.
pub(crate) struct ArrowBlockfileProvider {
    block_provider: ArrowBlockProvider,
    files: HashMap<String, Box<dyn Blockfile>>,
}

impl ArrowBlockfileProvider {
    pub(crate) fn new() -> Self {
        Self {
            block_provider: ArrowBlockProvider::new(),
            files: HashMap::new(),
        }
    }
}

impl BlockfileProvider for ArrowBlockfileProvider {
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

/// A simple local cache of Arrow-backed blocks, the blockfile provider passes this
/// to the ArrowBlockfile when it creates a new blockfile. So that the blockfile can manage and access blocks
/// # Note
/// The implementation is currently very simple and not intended for robust production use. We should
/// introduce a more sophisticated cache that can handle tiered eviction and other features. This interface
/// is a placeholder for that.
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
