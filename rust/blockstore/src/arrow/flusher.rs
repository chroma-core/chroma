use super::{
    block::Block,
    provider::{BlockManager, RootManager},
    root::RootWriter,
    types::{ArrowWriteableKey, ArrowWriteableValue},
};
use chroma_error::ChromaError;
use chroma_types::Cmek;
use futures::{StreamExt, TryStreamExt};
use uuid::Uuid;

pub struct ArrowBlockfileFlusher {
    block_manager: BlockManager,
    root_manager: RootManager,
    blocks: Vec<Block>,
    root: RootWriter,
    id: Uuid,
    count: u64,
    cmek: Option<Cmek>,
    /// When true, compute and store value buffer byte offsets for byte-range reads.
    track_value_buffer_offsets: bool,
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
        track_value_buffer_offsets: bool,
    ) -> Self {
        Self {
            block_manager,
            root_manager,
            blocks,
            root,
            id,
            count,
            cmek,
            track_value_buffer_offsets,
        }
    }

    /// The buffer index for embedding Float32 values in DataRecord blocks.
    /// Buffer layout: 0-prefix offsets, 1-prefix data, 2-key data, 3-struct validity,
    /// 4-id offsets, 5-id data, 6-embedding validity, 7-embedding values.
    const EMBEDDING_BUFFER_INDEX: usize = 7;

    #[allow(clippy::extra_unused_type_parameters)]
    pub(crate) async fn flush<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        self,
    ) -> Result<(), Box<dyn ChromaError>> {
        if self.root.sparse_index.len() == 0 {
            panic!("Invariant violation. Sparse index should be not empty during flush.");
        }

        let num_blocks = self.blocks.len();
        // buffer_unordered hangs with 0 futures.
        if num_blocks == 0 {
            self.root_manager.flush::<K>(&self.root, self.cmek).await?;
            return Ok(());
        }

        tracing::debug!("Flushing {} blocks", num_blocks);

        if self.track_value_buffer_offsets {
            // Flush blocks and compute value buffer offsets
            let mut futures = Vec::new();
            for block in &self.blocks {
                futures.push(async {
                    let offset = self
                        .block_manager
                        .flush_with_value_buffer_offset(
                            block,
                            &self.root.prefix_path,
                            self.cmek.clone(),
                            Self::EMBEDDING_BUFFER_INDEX,
                        )
                        .await?;
                    Ok::<(Uuid, Option<u64>), Box<dyn ChromaError>>((block.id, offset))
                });
            }

            let num_concurrent_flushes =
                num_blocks.min(self.block_manager.num_concurrent_block_flushes());
            let results: Vec<(Uuid, Option<u64>)> = futures::stream::iter(futures)
                .buffer_unordered(num_concurrent_flushes)
                .try_collect()
                .await?;

            // Set value buffer offsets in the sparse index
            for (block_id, offset) in results {
                if let Some(offset) = offset {
                    if let Err(e) = self.root.sparse_index.set_value_buffer_offset(block_id, offset)
                    {
                        tracing::warn!(
                            "Failed to set value buffer offset for block {}: {}",
                            block_id,
                            e
                        );
                    }
                }
            }
        } else {
            // Standard flush without offset tracking
            let mut futures = Vec::new();
            for block in &self.blocks {
                futures.push(self.block_manager.flush(
                    block,
                    &self.root.prefix_path,
                    self.cmek.clone(),
                ));
            }

            let num_concurrent_flushes =
                num_blocks.min(self.block_manager.num_concurrent_block_flushes());
            futures::stream::iter(futures)
                .buffer_unordered(num_concurrent_flushes)
                .try_collect::<Vec<_>>()
                .await?;
        }

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
