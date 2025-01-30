use chroma_cache::CacheConfig;
use serde::Deserialize;

const fn default_permitted_parallelism() -> u32 {
    180
}

#[derive(Deserialize, Debug, Clone)]
pub struct HnswProviderConfig {
    pub hnsw_temporary_path: String,
    pub hnsw_cache_config: CacheConfig,
    // This is the number of collections that can be loaded in parallel
    // without contending with each other.
    // Internally the number of partitions of the partitioned mutex
    // that is used to synchronize concurrent loads is set to
    // permitted_parallelism * permitted_parallelism. This is
    // inspired by the birthday paradox.
    #[serde(default = "default_permitted_parallelism")]
    pub permitted_parallelism: u32,
}
