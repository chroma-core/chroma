use super::{
    block::Block,
    provider::{BlockManager, RootManager},
    root::RootWriter,
    types::{ArrowWriteableKey, ArrowWriteableValue},
};
use chroma_error::ChromaError;
use futures::{StreamExt, TryStreamExt};
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

        // Flush all blocks in parallel using futures unordered
        // NOTE(hammadb) we do not use try_join_all here because we want to flush all blocks
        // in parallel and try_join_all / join_all switches to using futures_ordered if the
        // number of futures is high.
        let mut futures = Vec::new();
        for block in &self.blocks {
            futures.push(self.block_manager.flush(block));
        }
        let num_futures = futures.len();
        // buffer_unordered hangs with 0 futures.
        if num_futures == 0 {
            self.root_manager.flush::<K>(&self.root).await?;
            return Ok(());
        }
        tracing::debug!("Flushing {} blocks", num_futures);
        futures::stream::iter(futures)
            .buffer_unordered(num_futures)
            .try_collect::<Vec<_>>()
            .await?;

        self.root_manager.flush::<K>(&self.root).await?;
        Ok(())
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }
}
