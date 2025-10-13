pub mod config;
pub mod fulltext;
mod hnsw;
pub mod hnsw_provider;
pub mod metadata;
pub mod spann;
pub mod sparse;
mod types;
pub mod utils;

// Re-export types

use chroma_cache::new_non_persistent_cache_for_test;
use chroma_storage::test_storage;
pub use hnsw::*;
use hnsw_provider::HnswIndexProvider;
#[allow(unused_imports)]
pub use spann::*;
use tempfile::TempDir;
pub use types::*;

pub fn test_hnsw_index_provider() -> (TempDir, HnswIndexProvider) {
    let (temp_dir, storage) = test_storage();
    let provider = HnswIndexProvider::new(
        storage,
        temp_dir.path().to_path_buf(),
        new_non_persistent_cache_for_test(),
        16,
        false,
    );
    (temp_dir, provider)
}
