use chroma_blockstore::{
    arrow::{config::TEST_MAX_BLOCK_SIZE_BYTES, provider::ArrowBlockfileProvider},
    provider::BlockfileProvider,
};
use chroma_cache::{
    cache::{Cache, Cacheable},
    config::{CacheConfig, UnboundedCacheConfig},
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::{Segment, SegmentScope, SegmentType};
use std::{collections::HashMap, hash::Hash};
use tempfile::TempDir;
use uuid::Uuid;

pub fn tmp_dir() -> TempDir {
    TempDir::new().expect("Should be able to create a temporary directory.")
}

pub fn storage() -> Storage {
    Storage::Local(LocalStorage::new(tmp_dir().into_path().to_str().expect(
        "Should be able to convert temporary directory path to string",
    )))
}

pub fn unbounded_cache<K, V>() -> Cache<K, V>
where
    K: Send + Sync + Clone + Hash + Eq + 'static,
    V: Send + Sync + Clone + Cacheable + 'static,
{
    Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}))
}

pub fn arrow_blockfile_provider() -> BlockfileProvider {
    BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
        storage(),
        TEST_MAX_BLOCK_SIZE_BYTES,
        unbounded_cache(),
        unbounded_cache(),
    ))
}

pub fn segment(scope: SegmentScope) -> Segment {
    use SegmentScope::*;
    use SegmentType::*;
    let r#type = match scope {
        METADATA => BlockfileMetadata,
        RECORD => BlockfileRecord,
        SQLITE | VECTOR => panic!("Unsupported segment scope in testing."),
    };
    Segment {
        id: Uuid::new_v4(),
        r#type,
        scope,
        collection: Uuid::new_v4(),
        metadata: None,
        file_path: HashMap::new(),
    }
}
