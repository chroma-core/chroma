use chroma_cache::CacheConfig;
use serde::Deserialize;

const fn default_num_parallel_collections() -> u32 {
    16
}

#[derive(Deserialize, Debug, Clone)]
pub struct HnswProviderConfig {
    pub hnsw_temporary_path: String,
    pub hnsw_cache_config: CacheConfig,
    #[serde(default = "default_num_parallel_collections")]
    pub num_parallel_collections: u32,
}
