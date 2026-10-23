use chroma_cache::{CacheConfig, FoyerCacheConfig};
use serde::{Deserialize, Serialize};

// A small block size for testing, so that triggering splits etc is easier
pub const TEST_MAX_BLOCK_SIZE_BYTES: usize = 16384;

#[derive(Default, Deserialize, Debug, Clone, Serialize)]
pub struct ArrowBlockfileProviderConfig {
    #[serde(default)]
    pub block_manager_config: BlockManagerConfig,
    #[serde(default)]
    #[serde(alias = "sparse_index_manager_config")]
    pub root_manager_config: RootManagerConfig,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct BlockManagerConfig {
    #[serde(default = "BlockManagerConfig::default_max_block_size_bytes")]
    pub max_block_size_bytes: usize,
    #[serde(default)]
    pub block_cache_config: CacheConfig,
    #[serde(default = "BlockManagerConfig::default_num_concurrent_block_flushes")]
    pub num_concurrent_block_flushes: usize,
}

impl BlockManagerConfig {
    fn default_max_block_size_bytes() -> usize {
        16384
    }

    pub fn default_num_concurrent_block_flushes() -> usize {
        40
    }

    pub fn validate(&self) -> bool {
        self.max_block_size_bytes != 0 && self.num_concurrent_block_flushes != 0
    }
}

impl Default for BlockManagerConfig {
    fn default() -> Self {
        BlockManagerConfig {
            max_block_size_bytes: BlockManagerConfig::default_max_block_size_bytes(),
            block_cache_config: CacheConfig::Memory(FoyerCacheConfig {
                capacity: 1000,
                ..Default::default()
            }),
            num_concurrent_block_flushes: BlockManagerConfig::default_num_concurrent_block_flushes(
            ),
        }
    }
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct RootManagerConfig {
    #[serde(alias = "sparse_index_cache_config")]
    #[serde(default)]
    pub root_cache_config: CacheConfig,
}

impl Default for RootManagerConfig {
    fn default() -> Self {
        RootManagerConfig {
            root_cache_config: CacheConfig::Memory(FoyerCacheConfig {
                capacity: 1000,
                ..Default::default()
            }),
        }
    }
}
