pub mod config;
pub mod fulltext;
mod hnsw;
pub mod hnsw_provider;
pub mod metadata;
mod types;
pub mod utils;

// Re-export types

use chroma_cache::new_non_persistent_cache_for_test;
use chroma_storage::test_storage;
pub use hnsw::*;
use hnsw_provider::HnswIndexProvider;
use tempfile::tempdir;
pub use types::*;

pub fn test_hnsw_index_provider() -> HnswIndexProvider {
    let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
    HnswIndexProvider::new(
        test_storage(),
        tempdir()
            .expect("Should be able to create a temporary directory")
            .into_path(),
        new_non_persistent_cache_for_test(),
        rx,
    )
}
