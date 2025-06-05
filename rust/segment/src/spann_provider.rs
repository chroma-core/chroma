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

use crate::distributed_spann::{
    SpannSegmentReader, SpannSegmentReaderError, SpannSegmentWriter, SpannSegmentWriterError,
};

#[derive(Debug, Clone)]
pub struct SpannProvider {
    pub hnsw_provider: HnswIndexProvider,
    pub blockfile_provider: BlockfileProvider,
    // Option because reader does not need it.
    pub garbage_collection_context: Option<GarbageCollectionContext>,
    pub metrics: SpannMetrics,
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
            garbage_collection_context: Some(garbage_collection_context),
            metrics: SpannMetrics::default(),
        })
    }
}

impl SpannProvider {
    pub async fn read(
        &self,
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
    ) -> Result<SpannSegmentReader<'_>, SpannSegmentReaderError> {
        SpannSegmentReader::from_segment(
            collection,
            segment,
            &self.blockfile_provider,
            &self.hnsw_provider,
            dimensionality,
        )
        .await
    }

    pub async fn write(
        &self,
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
    ) -> Result<SpannSegmentWriter, SpannSegmentWriterError> {
        let gc_context = self
            .garbage_collection_context
            .as_ref()
            .ok_or(SpannSegmentWriterError::InvalidArgument)?;
        SpannSegmentWriter::from_segment(
            collection,
            segment,
            &self.blockfile_provider,
            &self.hnsw_provider,
            dimensionality,
            gc_context.clone(),
            self.metrics.clone(),
        )
        .await
    }
}
