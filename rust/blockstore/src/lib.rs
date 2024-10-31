pub mod types;

pub mod arrow;
#[cfg(test)]
mod blockfile_writer_test;
pub mod config;
pub mod key;
pub mod memory;
pub mod provider;
use arrow::config::TEST_MAX_BLOCK_SIZE_BYTES;
use chroma_cache::new_cache_for_test;
use chroma_storage::test_storage;
use provider::BlockfileProvider;
pub use types::*;

pub fn test_arrow_blockfile_provider() -> BlockfileProvider {
    BlockfileProvider::new_arrow(
        test_storage(),
        TEST_MAX_BLOCK_SIZE_BYTES,
        new_cache_for_test(),
        new_cache_for_test(),
    )
}
