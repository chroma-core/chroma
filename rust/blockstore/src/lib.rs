pub mod types;

pub mod arrow;
pub mod config;
pub mod key;
pub mod memory;
pub mod provider;
// This module is not gated as [#cfg(test)] because it is used in crates external to blockstore.
pub mod test_utils;

use arrow::config::BlockManagerConfig;
use chroma_cache::new_cache_for_test;
use chroma_storage::test_storage;
use provider::BlockfileProvider;
use tempfile::TempDir;
pub use types::*;

// Re-export RootManager for external use
pub use arrow::provider::RootManager;

pub fn test_arrow_blockfile_provider(max_block_size_bytes: usize) -> (TempDir, BlockfileProvider) {
    let (temp_dir, storage) = test_storage();
    let provider = BlockfileProvider::new_arrow(
        storage,
        max_block_size_bytes,
        new_cache_for_test(),
        new_cache_for_test(),
        BlockManagerConfig::default_num_concurrent_block_flushes(),
    );
    (temp_dir, provider)
}
