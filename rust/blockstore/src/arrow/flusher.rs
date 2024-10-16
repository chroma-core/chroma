use super::{
    block::Block,
    provider::{BlockManager, RootManager},
    root::RootWriter,
    types::{ArrowWriteableKey, ArrowWriteableValue},
};
use chroma_error::ChromaError;
use uuid::Uuid;

pub struct ArrowBlockfileFlusher {
    block_manager: BlockManager,
    root_manager: RootManager,
    blocks: Vec<Block>,
    root: RootWriter,
    id: Uuid,
}

impl ArrowBlockfileFlusher {
    pub(in crate::arrow) fn new(
        block_manager: BlockManager,
        root_manager: RootManager,
        blocks: Vec<Block>,
        root: RootWriter,
        id: Uuid,
    ) -> Self {
        Self {
            block_manager,
            root_manager,
            blocks,
            root,
            id,
        }
    }

    #[allow(clippy::extra_unused_type_parameters)]
    pub(crate) async fn flush<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        self,
    ) -> Result<(), Box<dyn ChromaError>> {
        if self.root.sparse_index.len() == 0 {
            panic!("Invariant violation. Sparse index should be not empty during flush.");
        }
        // TODO: We could flush in parallel
        for block in &self.blocks {
            self.block_manager.flush(block).await?;
        }
        self.root_manager.flush::<K>(&self.root).await?;
        Ok(())
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }
}
