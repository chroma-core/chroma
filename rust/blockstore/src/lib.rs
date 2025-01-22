pub mod types;

pub mod arrow;
pub mod config;
pub mod key;
pub mod memory;
pub mod provider;
use chroma_cache::new_cache_for_test;
use chroma_storage::test_storage;
use provider::BlockfileProvider;
pub use types::*;

pub fn test_arrow_blockfile_provider(max_block_size_bytes: usize) -> BlockfileProvider {
    BlockfileProvider::new_arrow(
        test_storage(),
        max_block_size_bytes,
        new_cache_for_test(),
        new_cache_for_test(),
    )
}
