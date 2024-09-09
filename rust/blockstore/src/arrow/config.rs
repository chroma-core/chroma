use chroma_cache::config::CacheConfig;
use serde::Deserialize;

// A small block size for testing, so that triggering splits etc is easier
pub const TEST_MAX_BLOCK_SIZE_BYTES: usize = 16384;

#[derive(Deserialize, Debug, Clone)]
pub struct ArrowBlockfileProviderConfig {
    pub block_manager_config: BlockManagerConfig,
    pub sparse_index_manager_config: SparseIndexManagerConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BlockManagerConfig {
    pub max_block_size_bytes: usize,
    pub block_cache_config: CacheConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SparseIndexManagerConfig {
    pub sparse_index_cache_config: CacheConfig,
}
