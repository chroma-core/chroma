use chroma_cache::CacheConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug, Clone, Default, Serialize)]
pub struct HnswProviderConfig {
    #[serde(default = "HnswProviderConfig::default_hnsw_temporary_path")]
    pub hnsw_temporary_path: String,
    #[serde(default)]
    pub hnsw_cache_config: CacheConfig,
    // This is the number of collections that can be loaded in parallel
    // without contending with each other.
    // Internally the number of partitions of the partitioned mutex
    // that is used to synchronize concurrent loads is set to
    // permitted_parallelism * permitted_parallelism. This is
    // inspired by the birthday paradox.
    #[serde(default = "HnswProviderConfig::default_permitted_parallelism")]
    pub permitted_parallelism: u32,
}

impl HnswProviderConfig {
    fn default_hnsw_temporary_path() -> String {
        "/tmp/chroma".to_string()
    }

    const fn default_permitted_parallelism() -> u32 {
        180
    }
}

fn default_garbage_collection() -> bool {
    false
}

fn default_garbage_collection_policy() -> GarbageCollectionPolicy {
    GarbageCollectionPolicy::Random10
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
pub enum GarbageCollectionPolicy {
    #[serde(rename = "full")]
    Full,
    #[serde(rename = "random10")]
    #[default]
    Random10,
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
pub struct SpannProviderConfig {
    #[serde(default = "default_garbage_collection")]
    pub garbage_collection: bool,
    #[serde(default = "default_garbage_collection_policy")]
    pub garbage_collection_policy: GarbageCollectionPolicy,
}
