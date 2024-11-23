//! Our use of object storage is very specific in that we never overwrite objects and we think in
//! blocks.  This module relies on this pattern to implement an object store that wraps two other
//! object stores, one for the cache and one for the backing store.  Data is written to the backing
//! store and then to the cache.  Reads are attempted from the cache and then from the backing
//! store.  There is no effort made to dedupe writes, because it's assumed that a single writer is
//! working on a single block at a time.  No one should write the same block to the same location
//! at the same time, and the cost of doing so is cache inefficiency.

use std::fmt::{Debug, Display};
use std::ops::Range;
use std::sync::Arc;

use futures::stream::StreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result,
};
use sync42::lru::{LeastRecentlyUsedCache, Value};

use bytes::Bytes;
use futures::stream::BoxStream;

use super::SafeObjectStore;

#[derive(Debug, Clone)]
struct EvictionStub {
    size: usize,
}

impl Value for EvictionStub {
    fn approximate_size(&self) -> usize {
        self.size
    }
}

pub struct EvictingObjectStore {
    target_disk_usage: usize,
    object_store: Arc<dyn SafeObjectStore>,
    lru: Arc<LeastRecentlyUsedCache<Path, EvictionStub>>,
}

impl EvictingObjectStore {
    pub async fn new<O: SafeObjectStore>(
        object_store: O,
        target_disk_usage: usize,
    ) -> Result<Self, object_store::Error> {
        assert!(object_store.supports_delete());
        let object_store = Arc::new(object_store);
        let lru = Arc::new(LeastRecentlyUsedCache::new(1024));
        let mut all_objects = object_store.list(None);
        while let Some(meta) = all_objects.next().await {
            let meta = meta?;
            lru.insert_no_evict(meta.location.clone(), EvictionStub { size: meta.size });
        }
        drop(all_objects);
        while lru.approximate_size() > target_disk_usage {
            let Some((key, _)) = lru.pop() else {
                break;
            };
            object_store.delete(&key).await?;
        }
        Ok(Self {
            target_disk_usage,
            object_store,
            lru,
        })
    }
}

impl Debug for EvictingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EvictingObjectStore")
    }
}

impl Display for EvictingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EvictingObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for EvictingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.lru.insert_no_evict(
            location.clone(),
            EvictionStub {
                size: payload.content_length(),
            },
        );
        while self.lru.approximate_size() > self.target_disk_usage {
            let Some((key, _)) = self.lru.pop() else {
                break;
            };
            self.object_store.delete(&key).await?;
        }
        self.object_store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        // NOTE(rescrv):  The caching object store does not insert multipart uploads into the
        // cache.  Because the evicting object store is intended to be used only around the
        // caching object store, we can fail this call.
        Err(object_store::Error::NotImplemented)
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.object_store.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        self.object_store.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.object_store.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.lru.remove(location);
        self.object_store.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.object_store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.object_store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.object_store.copy_if_not_exists(from, to).await
    }
}

impl SafeObjectStore for EvictingObjectStore {
    fn supports_delete(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use object_store::path::Path;
    use object_store::{Error, ObjectStore};

    use super::*;

    #[tokio::test]
    async fn empty() {
        let object_store = object_store::memory::InMemory::new();
        let evicting = EvictingObjectStore::new(object_store, 1024).await.unwrap();
        assert!(evicting
            .get_opts(&Path::from("noexist"), Default::default())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn fill_it_and_see_it_evict() {
        let object_store = object_store::memory::InMemory::new();
        let evicting = EvictingObjectStore::new(object_store, 1024).await.unwrap();
        for idx in 0..1024 {
            assert!(evicting
                .put_opts(
                    &Path::from(format!("key{}", idx)),
                    PutPayload::from(Bytes::from(format!("value{}", idx))),
                    Default::default()
                )
                .await
                .is_ok());
        }
        let mut expect_to_see = true;
        for idx in (0..1024).rev() {
            let key = Path::from(format!("key{}", idx));
            let res = evicting.get_opts(&key, Default::default()).await;
            if expect_to_see && matches!(res, Err(Error::NotFound { .. })) {
                expect_to_see = false;
            }
            if expect_to_see {
                assert!(res.is_ok());
            } else {
                assert!(matches!(res, Err(Error::NotFound { .. })));
            }
        }
    }

    #[tokio::test]
    async fn cold_start() {
        let object_store = object_store::memory::InMemory::new();
        for idx in 0..1024 {
            assert!(object_store
                .put_opts(
                    &Path::from(format!("key{}", idx)),
                    PutPayload::from(Bytes::from(format!("value{}", idx))),
                    Default::default()
                )
                .await
                .is_ok());
        }
        let evicting = EvictingObjectStore::new(object_store, 1024).await.unwrap();
        let mut not_found = 0;
        let mut found = 0;
        for idx in 0..1024 {
            let key = Path::from(format!("key{}", idx));
            let res = evicting.get_opts(&key, Default::default()).await;
            if matches!(res, Err(Error::NotFound { .. })) {
                not_found += 1;
            } else if res.is_ok() {
                found += 1;
            } else {
                panic!("unexpected result: {:?}", res);
            }
        }
        println!(
            "found: {}, not_found: {}, approx_size: {}",
            found,
            not_found,
            evicting.lru.approximate_size()
        );
        assert!(found >= 64, "{}", found);
    }

    #[tokio::test]
    async fn eviction_impls_safe() {
        let object_store = object_store::memory::InMemory::new();
        let evicting: &dyn SafeObjectStore =
            &EvictingObjectStore::new(object_store, 1024).await.unwrap();
        assert!(evicting.supports_delete());
    }
}
