use chroma_cache::config::CacheConfig;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct HnswProviderConfig {
    pub(crate) hnsw_temporary_path: String,
    pub(crate) hnsw_cache_config: CacheConfig,
}
