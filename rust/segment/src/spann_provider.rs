use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_index::{
    config::{GarbageCollectionPolicy, SpannProviderConfig},
    hnsw_provider::HnswIndexProvider,
};
use chroma_types::Segment;

use crate::distributed_spann::{SpannSegmentReader, SpannSegmentReaderError};

pub struct GarbageCollection {
    pub garbage_collection: bool,
    pub garbage_collection_policy: GarbageCollectionPolicy,
}

pub struct SpannProvider {
    pub hnsw_provider: HnswIndexProvider,
    pub blockfile_provider: BlockfileProvider,
    pub garbage_collection_config: GarbageCollection,
}

#[async_trait]
impl Configurable<(HnswIndexProvider, BlockfileProvider, SpannProviderConfig)> for SpannProvider {
    async fn try_from_config(
        config: &(HnswIndexProvider, BlockfileProvider, SpannProviderConfig),
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let garbage_collection_config = GarbageCollection {
            garbage_collection: config.2.garbage_collection,
            garbage_collection_policy: config.2.garbage_collection_policy.clone(),
        };
        Ok(SpannProvider {
            hnsw_provider: config.0.clone(),
            blockfile_provider: config.1.clone(),
            garbage_collection_config,
        })
    }
}

impl SpannProvider {
    pub async fn read(
        &self,
        segment: &Segment,
        dimensionality: usize,
    ) -> Result<SpannSegmentReader<'_>, SpannSegmentReaderError> {
        SpannSegmentReader::from_segment(
            segment,
            &self.blockfile_provider,
            &self.hnsw_provider,
            dimensionality,
        )
        .await
    }
}
