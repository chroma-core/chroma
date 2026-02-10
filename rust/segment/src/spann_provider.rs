use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
#[cfg(feature = "usearch")]
use chroma_index::usearch::USearchIndexProvider;
use chroma_index::{
    config::SpannProviderConfig,
    hnsw_provider::HnswIndexProvider,
    spann::types::{GarbageCollectionContext, SpannMetrics},
};
use chroma_types::{Cmek, Collection, Segment};

use crate::distributed_spann::{SpannSegmentWriter, SpannSegmentWriterError};
#[cfg(feature = "usearch")]
use crate::quantized_spann::{QuantizedSpannSegmentError, QuantizedSpannSegmentWriter};

#[derive(Clone)]
pub struct SpannProvider {
    pub adaptive_search_nprobe: bool,
    pub blockfile_provider: BlockfileProvider,
    pub garbage_collection_context: GarbageCollectionContext,
    pub hnsw_provider: HnswIndexProvider,
    pub metrics: SpannMetrics,
    pub pl_block_size: usize,
    #[cfg(feature = "usearch")]
    pub usearch_provider: USearchIndexProvider,
}

impl std::fmt::Debug for SpannProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpannProvider").finish()
    }
}

#[cfg(not(feature = "usearch"))]
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
            adaptive_search_nprobe: config.2.adaptive_search_nprobe,
            blockfile_provider: config.1.clone(),
            garbage_collection_context,
            hnsw_provider: config.0.clone(),
            metrics: SpannMetrics::default(),
            pl_block_size: config.2.pl_block_size,
        })
    }
}

#[cfg(feature = "usearch")]
#[async_trait]
impl
    Configurable<(
        HnswIndexProvider,
        BlockfileProvider,
        SpannProviderConfig,
        USearchIndexProvider,
    )> for SpannProvider
{
    async fn try_from_config(
        config: &(
            HnswIndexProvider,
            BlockfileProvider,
            SpannProviderConfig,
            USearchIndexProvider,
        ),
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
            adaptive_search_nprobe: config.2.adaptive_search_nprobe,
            blockfile_provider: config.1.clone(),
            garbage_collection_context,
            hnsw_provider: config.0.clone(),
            metrics: SpannMetrics::default(),
            pl_block_size: config.2.pl_block_size,
            usearch_provider: config.3.clone(),
        })
    }
}

impl SpannProvider {
    pub async fn write(
        &self,
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
        cmek: Option<Cmek>,
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
            cmek,
        )
        .await
    }

    #[cfg(feature = "usearch")]
    pub async fn write_quantized_usearch(
        &self,
        collection: &Collection,
        vector_segment: &Segment,
        record_segment: &Segment,
    ) -> Result<QuantizedSpannSegmentWriter, QuantizedSpannSegmentError> {
        QuantizedSpannSegmentWriter::from_segment(
            self.pl_block_size,
            collection,
            vector_segment,
            record_segment,
            &self.blockfile_provider,
            &self.usearch_provider,
        )
        .await
    }
}
