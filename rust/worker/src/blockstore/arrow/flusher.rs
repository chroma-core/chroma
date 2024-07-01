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
        // let sparse_index = sparse_index_manager.get(&id).unwrap();
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
        // TODO: We could flush in parallel
        self.block_manager.flush(&self.blocks).await?;

        self.sparse_index_manager
            .flush::<K>(&self.sparse_index)
            .await?;
        Ok(())
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }
}
