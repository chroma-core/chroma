use super::{
    block::Block,
    provider::{BlockManager, RootManager},
    root::RootWriter,
    types::{ArrowWriteableKey, ArrowWriteableValue},
};
use backon::{ExponentialBuilder, Retryable};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::Cmek;
use futures::{StreamExt, TryStreamExt};
use std::time::Duration;
use uuid::Uuid;

pub struct ArrowBlockfileFlusher {
    block_manager: BlockManager,
    root_manager: RootManager,
    blocks: Vec<Block>,
    root: RootWriter,
    id: Uuid,
    count: u64,
    cmek: Option<Cmek>,
}

impl ArrowBlockfileFlusher {
    pub(in crate::arrow) fn new(
        block_manager: BlockManager,
        root_manager: RootManager,
        blocks: Vec<Block>,
        root: RootWriter,
        id: Uuid,
        count: u64,
        cmek: Option<Cmek>,
    ) -> Self {
        Self {
            block_manager,
            root_manager,
            blocks,
            root,
            id,
            count,
            cmek,
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
        let retry_backoff = ExponentialBuilder::default()
            .with_factor(2.0)
            .with_min_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(10))
            .with_max_times(5)
            .with_jitter();
        let mut futures = Vec::new();
        for block in &self.blocks {
            let block_manager = self.block_manager.clone();
            let prefix_path = self.root.prefix_path.clone();
            let cmek = self.cmek.clone();
            let block_id = block.id;
            let block = block.clone();
            futures.push(async move {
                let flush_fn = || {
                    let block_manager = block_manager.clone();
                    let prefix_path = prefix_path.clone();
                    let cmek = cmek.clone();
                    let block = block.clone();
                    async move { block_manager.flush(&block, &prefix_path, cmek).await }
                };
                flush_fn
                    .retry(retry_backoff)
                    .when(|e| {
                        matches!(
                            e.code(),
                            ErrorCodes::ResourceExhausted | ErrorCodes::Internal
                        )
                    })
                    .notify(|e, _| {
                        tracing::warn!("Retrying flush for block {}: {}", block_id, e);
                    })
                    .await
            });
        }
        let num_futures = futures.len();
        // buffer_unordered hangs with 0 futures.
        if num_futures == 0 {
            self.root_manager.flush::<K>(&self.root, self.cmek).await?;
            return Ok(());
        }
        tracing::debug!("Flushing {} blocks", num_futures);
        // Flush n blocks at a time to reduce memory usage.
        let num_concurrent_flushes =
            num_futures.min(self.block_manager.num_concurrent_block_flushes());
        futures::stream::iter(futures)
            .buffer_unordered(num_concurrent_flushes)
            .try_collect::<Vec<_>>()
            .await?;

        self.root_manager.flush::<K>(&self.root, self.cmek).await?;
        Ok(())
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }

    pub(crate) fn count(&self) -> u64 {
        self.count
    }

    pub(crate) fn num_entries(&self) -> usize {
        self.blocks.iter().fold(0, |acc, block| acc + block.len())
    }

    pub(crate) fn prefix_path(&self) -> &str {
        &self.root.prefix_path
    }
}
