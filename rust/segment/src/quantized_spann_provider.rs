use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_cache::CacheConfig;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_index::usearch::USearchIndexProvider;
use chroma_storage::Storage;
use chroma_types::{Collection, Segment};
use serde::{Deserialize, Serialize};

use crate::quantized_spann::{QuantizedSpannSegmentError, QuantizedSpannSegmentWriter};

fn default_cluster_block_size() -> usize {
    3 * 1024 * 1024
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct QuantizedSpannProviderConfig {
    #[serde(default = "default_cluster_block_size")]
    pub cluster_block_size: usize,
    #[serde(default)]
    pub usearch_cache_config: CacheConfig,
}

#[derive(Clone)]
pub struct QuantizedSpannProvider {
    pub blockfile_provider: BlockfileProvider,
    pub cluster_block_size: usize,
    pub usearch_provider: USearchIndexProvider,
}

impl Debug for QuantizedSpannProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuantizedSpannProvider").finish()
    }
}

#[async_trait]
impl Configurable<(BlockfileProvider, Storage, QuantizedSpannProviderConfig)>
    for QuantizedSpannProvider
{
    async fn try_from_config(
        config: &(BlockfileProvider, Storage, QuantizedSpannProviderConfig),
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let usearch_cache = chroma_cache::from_config(&config.2.usearch_cache_config).await?;
        let usearch_provider = USearchIndexProvider::new(config.1.clone(), usearch_cache);
        Ok(QuantizedSpannProvider {
            blockfile_provider: config.0.clone(),
            cluster_block_size: config.2.cluster_block_size,
            usearch_provider,
        })
    }
}

impl QuantizedSpannProvider {
    pub async fn write(
        &self,
        collection: &Collection,
        vector_segment: &Segment,
        record_segment: &Segment,
    ) -> Result<QuantizedSpannSegmentWriter, QuantizedSpannSegmentError> {
        QuantizedSpannSegmentWriter::from_segment(
            self.cluster_block_size,
            collection,
            vector_segment,
            record_segment,
            &self.blockfile_provider,
            &self.usearch_provider,
        )
        .await
    }
}
