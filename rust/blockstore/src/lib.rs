pub mod types;

pub mod arrow;
pub mod config;
pub mod key;
pub mod memory;
pub mod provider;
// This module is not gated as [#cfg(test)] because it is used in crates external to blockstore.
pub mod test_utils;

use chroma_cache::new_cache_for_test;
use chroma_storage::test_storage;
use provider::BlockfileProvider;
pub use types::*;

// Re-export RootManager for external use
pub use arrow::provider::RootManager;

pub fn test_arrow_blockfile_provider(max_block_size_bytes: usize) -> BlockfileProvider {
    BlockfileProvider::new_arrow(
        test_storage(),
        max_block_size_bytes,
        new_cache_for_test(),
        new_cache_for_test(),
    )
}
