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

fn default_garbage_collection() -> PlGarbageCollectionConfig {
    PlGarbageCollectionConfig {
        enabled: false,
        policy: PlGarbageCollectionPolicyConfig::RandomSample(RandomSamplePolicyConfig::default()),
    }
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
    #[serde(default = "default_garbage_collection")]
    pub pl_garbage_collection: PlGarbageCollectionConfig,
    pub hnsw_garbage_collection: HnswGarbageCollectionConfig,
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
