use super::{
    block::Block,
    provider::{BlockManager, SparseIndexManager},
    sparse_index::SparseIndex,
    types::{ArrowWriteableKey, ArrowWriteableValue},
};
use crate::errors::ChromaError;
use uuid::Uuid;

pub(crate) struct ArrowBlockfileFlusher {
    block_manager: BlockManager,
    sparse_index_manager: SparseIndexManager,
    blocks: Vec<Block>,
    sparse_index: SparseIndex,
    id: Uuid,
}

impl ArrowBlockfileFlusher {
    pub(crate) fn new(
        block_manager: BlockManager,
        sparse_index_manager: SparseIndexManager,
        blocks: Vec<Block>,
        sparse_index: SparseIndex,
        id: Uuid,
    ) -> Self {
        Self {
            block_manager,
            sparse_index_manager,
            blocks,
            sparse_index,
            id,
        }
    }

    pub(crate) async fn flush<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        self,
    ) -> Result<(), Box<dyn ChromaError>> {
        if self.sparse_index.len() == 0 {
            panic!("Invariant violation. Sparse index should be not empty during flush.");
        }
        // TODO: We could flush in parallel
        for block in &self.blocks {
            self.block_manager.flush(block).await?;
        }
        self.sparse_index_manager
            .flush::<K>(&self.sparse_index)
            .await?;
        Ok(())
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }
}
