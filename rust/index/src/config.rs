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
    // Control whether HNSW indices are loaded from memory
    // instead of disk. Defaults to false.
    #[serde(default = "HnswProviderConfig::default_use_direct_hnsw")]
    pub use_direct_hnsw: bool,
}

impl HnswProviderConfig {
    fn default_hnsw_temporary_path() -> String {
        "/tmp/chroma".to_string()
    }

    const fn default_permitted_parallelism() -> u32 {
        180
    }

    const fn default_use_direct_hnsw() -> bool {
        false
    }
}

fn default_pl_garbage_collection() -> PlGarbageCollectionConfig {
    PlGarbageCollectionConfig {
        enabled: false,
        policy: PlGarbageCollectionPolicyConfig::RandomSample(RandomSamplePolicyConfig::default()),
    }
}

fn default_hnsw_garbage_collection() -> HnswGarbageCollectionConfig {
    HnswGarbageCollectionConfig {
        enabled: false,
        policy: HnswGarbageCollectionPolicyConfig::DeletePercentage(
            DeletePercentageThresholdPolicyConfig::default(),
        ),
    }
}

fn default_pl_block_size() -> usize {
    5 * 1024 * 1024
}

fn default_adaptive_search_nprobe() -> bool {
    true
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub enum PlGarbageCollectionPolicyConfig {
    #[serde(rename = "random_sample")]
    RandomSample(RandomSamplePolicyConfig),
}

impl Default for PlGarbageCollectionPolicyConfig {
    fn default() -> Self {
        PlGarbageCollectionPolicyConfig::RandomSample(RandomSamplePolicyConfig::default())
    }
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
pub struct SpannProviderConfig {
    #[serde(default = "default_pl_block_size")]
    pub pl_block_size: usize,
    #[serde(default = "default_pl_garbage_collection")]
    pub pl_garbage_collection: PlGarbageCollectionConfig,
    #[serde(default = "default_hnsw_garbage_collection")]
    pub hnsw_garbage_collection: HnswGarbageCollectionConfig,
    #[serde(default = "default_adaptive_search_nprobe")]
    pub adaptive_search_nprobe: bool,
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
pub struct PlGarbageCollectionConfig {
    pub enabled: bool,
    pub policy: PlGarbageCollectionPolicyConfig,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct RandomSamplePolicyConfig {
    pub sample_size: f32,
}

impl Default for RandomSamplePolicyConfig {
    fn default() -> Self {
        RandomSamplePolicyConfig { sample_size: 0.1 }
    }
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct DeletePercentageThresholdPolicyConfig {
    pub threshold: f32,
}

impl Default for DeletePercentageThresholdPolicyConfig {
    fn default() -> Self {
        DeletePercentageThresholdPolicyConfig { threshold: 0.1 }
    }
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
pub enum HnswGarbageCollectionPolicyConfig {
    #[default]
    #[serde(rename = "full_rebuild")]
    FullRebuild,
    #[serde(rename = "delete_percentage")]
    DeletePercentage(DeletePercentageThresholdPolicyConfig),
}

#[derive(Deserialize, Debug, Clone, Serialize, Default)]
pub struct HnswGarbageCollectionConfig {
    pub enabled: bool,
    pub policy: HnswGarbageCollectionPolicyConfig,
}
