use chroma_cache::CacheConfig;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct HnswProviderConfig {
    pub hnsw_temporary_path: String,
    pub hnsw_cache_config: CacheConfig,
}
