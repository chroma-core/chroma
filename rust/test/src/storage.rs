use chroma_blockstore::provider::BlockfileProvider;
use chroma_cache::new_cache_for_test;
use chroma_storage::{local::LocalStorage, Storage};
use tempfile::TempDir;

// 8MB block size, in case roaring bitmap has more values within.
const MAX_BLOCK_SIZE: usize = 2 << 23;

pub fn tmp_dir() -> TempDir {
    TempDir::new().expect("Should be able to create a temporary directory.")
}

pub fn storage() -> Storage {
    Storage::Local(LocalStorage::new(tmp_dir().into_path().to_str().expect(
        "Should be able to convert temporary directory path to string",
    )))
}

pub fn arrow_blockfile_provider() -> BlockfileProvider {
    BlockfileProvider::new_arrow(
        storage(),
        MAX_BLOCK_SIZE,
        new_cache_for_test(),
        new_cache_for_test(),
    )
}
