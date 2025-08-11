use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_index::{
    config::SpannProviderConfig,
    hnsw_provider::HnswIndexProvider,
    spann::types::{GarbageCollectionContext, SpannMetrics},
};
use chroma_types::{Collection, Segment};

use crate::distributed_spann::{SpannSegmentWriter, SpannSegmentWriterError};

#[derive(Debug, Clone)]
pub struct SpannProvider {
    pub hnsw_provider: HnswIndexProvider,
    pub blockfile_provider: BlockfileProvider,
    pub garbage_collection_context: GarbageCollectionContext,
    pub metrics: SpannMetrics,
    pub pl_block_size: usize,
    pub adaptive_search_nprobe: bool,
}

#[async_trait]
impl Configurable<(HnswIndexProvider, BlockfileProvider, SpannProviderConfig)> for SpannProvider {
    async fn try_from_config(
        config: &(HnswIndexProvider, BlockfileProvider, SpannProviderConfig),
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let garbage_collection_context = GarbageCollectionContext::try_from_config(
            &(
                config.2.pl_garbage_collection.clone(),
                config.2.hnsw_garbage_collection.clone(),
            ),
            registry,
        )
        .await?;
        Ok(SpannProvider {
            hnsw_provider: config.0.clone(),
            blockfile_provider: config.1.clone(),
            garbage_collection_context,
            metrics: SpannMetrics::default(),
            pl_block_size: config.2.pl_block_size,
            adaptive_search_nprobe: config.2.adaptive_search_nprobe,
        })
    }
}

impl SpannProvider {
    pub async fn write(
        &self,
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
    ) -> Result<SpannSegmentWriter, SpannSegmentWriterError> {
        SpannSegmentWriter::from_segment(
            collection,
            segment,
            &self.blockfile_provider,
            &self.hnsw_provider,
            dimensionality,
            self.garbage_collection_context.clone(),
            self.pl_block_size,
            self.metrics.clone(),
        )
        .await
    }
}
