use crate::cache::config::CacheConfig;
use serde::Deserialize;

#[cfg(test)]
// A small block size for testing, so that triggering splits etc is easier
pub(crate) const TEST_MAX_BLOCK_SIZE_BYTES: usize = 16384;

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ArrowBlockfileProviderConfig {
    // pub(crate) max_block_size_bytes: usize,
    pub(crate) block_manager_config: BlockManagerConfig,
    pub(crate) sparse_index_manager_config: SparseIndexManagerConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct BlockManagerConfig {
    pub(crate) max_block_size_bytes: usize,
    pub(crate) block_cache_config: CacheConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct SparseIndexManagerConfig {
    pub(crate) sparse_index_cache_config: CacheConfig,
}
