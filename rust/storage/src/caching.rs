//! Our use of object storage is very specific in that we never overwrite objects and we think in
//! blocks.  This module relies on this pattern to implement an object store that wraps two other
//! object stores, one for the cache and one for the backing store.  Data is written to the backing
//! store and then to the cache.  Reads are attempted from the cache and then from the backing
//! store.  There is no effort made to dedupe writes, because it's assumed that a single writer is
//! working on a single block at a time.  No one should write the same block to the same location
//! at the same time, and the cost of doing so is cache inefficiency.

use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;

use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
};
use sync42::state_hash_table::{Key, StateHashTable, Value};

use bytes::Bytes;
use futures::stream::BoxStream;

use super::SafeObjectStore;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DedupeKey {
    path: Path,
}

impl Key for DedupeKey {}

#[derive(Debug, Default)]
pub struct DedupeValue {
    one_in_flight: tokio::sync::Mutex<()>,
}

impl Value for DedupeValue {
    fn finished(&self) -> bool {
        true
    }
}

impl From<DedupeKey> for DedupeValue {
    fn from(_: DedupeKey) -> Self {
        Self::default()
    }
}

#[derive(Clone)]
pub struct CachingObjectStore {
    cache: Arc<dyn SafeObjectStore>,
    backing: Arc<dyn SafeObjectStore>,
    dedupe: Arc<StateHashTable<DedupeKey, DedupeValue>>,
}

impl CachingObjectStore {
    pub fn new<C: SafeObjectStore, B: SafeObjectStore>(cache: C, backing: B) -> Self {
        assert!(cache.supports_delete());
        assert!(!backing.supports_delete());
        Self {
            cache: Arc::new(cache),
            backing: Arc::new(backing),
            dedupe: Arc::new(StateHashTable::new()),
        }
    }

    async fn warm_cache(&self, location: &Path) -> Result<GetResult> {
        let get = self
            .backing
            .get_opts(location, GetOptions::default())
            .await?;
        let meta = get.meta.clone();
        let range = get.range.clone();
        let attributes = get.attributes.clone();
        let bytes = get.bytes().await?;
        if let Err(err) = self
            .cache
            .put_opts(
                location,
                PutPayload::from(bytes.clone()),
                PutOptions::default(),
            )
            .await
        {
            tracing::error!("failed to proactively warm the cache: {}", err);
        }
        let payload: GetResultPayload =
            GetResultPayload::Stream(Box::pin(futures::stream::once(async move { Ok(bytes) })));
        Ok(GetResult {
            meta,
            range,
            attributes,
            payload,
        })
    }
}

impl Debug for CachingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CachingObjectStore")
    }
}

impl Display for CachingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CachingObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CachingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let res = self
            .backing
            .put_opts(location, payload.clone(), opts.clone())
            .await?;
        if let Err(err) = self
            .cache
            .put_opts(location, payload.clone(), opts.clone())
            .await
        {
            tracing::error!("failed to proactively warm the cache: {}", err);
        }
        Ok(res)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.backing.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.range.is_some() || options.head {
            return self.backing.get_opts(location, options).await;
        }
        // SAFETY(rescrv):  This uses double checked-locking to initialize the cache.  First, it
        // checks the linearizable cache.  If that fails, it locks the dedupe state and checks the
        // cache again.  If the cache fails again, it returns the result of warming the cache.
        //
        // This makes sure that we don't have the same file request the same path in flight twice.
        //
        // The second thread to acquire the mutex will see the warm_cache call be atomic w.r.t.
        // their call to cache.get_opts.
        if let Ok(get) = self.cache.get_opts(location, options.clone()).await {
            return Ok(get);
        }
        let dedupe_key = DedupeKey {
            path: location.clone(),
        };
        let dedupe_state = self.dedupe.get_or_create_state(dedupe_key);
        let _dedupe_mutex = dedupe_state.one_in_flight.lock().await;
        if let Ok(get) = self.cache.get_opts(location, options.clone()).await {
            return Ok(get);
        }
        return self.warm_cache(location).await;
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        // TODO(rescrv):  Perhaps find a way to encode partial fetches.
        self.backing.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        match self.cache.head(location).await {
            Ok(meta) => Ok(meta),
            Err(err) => {
                if !matches!(err, object_store::Error::NotFound { .. }) {
                    tracing::error!("failed to read from cache: {}", err);
                }
                self.backing.head(location).await
            }
        }
    }

    async fn delete(&self, _: &Path) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.backing.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.backing.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.backing.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.backing.copy_if_not_exists(from, to).await
    }
}

impl SafeObjectStore for CachingObjectStore {
    fn supports_delete(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use object_store::path::Path;
    use object_store::{ObjectStore, PutMode};

    use super::CachingObjectStore;

    use crate::non_destructive::NonDestructiveObjectStore;
    use crate::SafeObjectStore;

    #[tokio::test]
    async fn empty() {
        let cache = object_store::memory::InMemory::new();
        let backing = NonDestructiveObjectStore::new(object_store::memory::InMemory::new());
        let cached = CachingObjectStore::new(cache, backing);
        assert!(cached
            .get_opts(&Path::from("test"), Default::default())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn insert() {
        let cache = object_store::memory::InMemory::new();
        let backing = NonDestructiveObjectStore::new(object_store::memory::InMemory::new());
        let cached = CachingObjectStore::new(cache, backing);
        assert!(cached
            .put_opts(
                &Path::from("test"),
                "hello 42".into(),
                PutMode::Create.into()
            )
            .await
            .is_ok());

        assert_eq!(
            "hello 42".as_bytes(),
            cached
                .get_opts(&Path::from("test"), Default::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn not_in_cache_but_populates() {
        let cache = object_store::memory::InMemory::new();
        let backing = NonDestructiveObjectStore::new(object_store::memory::InMemory::new());
        let cached = CachingObjectStore::new(cache, backing);
        assert!(cached
            .backing
            .put_opts(
                &Path::from("test"),
                "hello 42".into(),
                PutMode::Create.into()
            )
            .await
            .is_ok());

        assert!(cached
            .cache
            .get_opts(&Path::from("test"), Default::default())
            .await
            .is_err());

        assert_eq!(
            "hello 42".as_bytes(),
            cached
                .get_opts(&Path::from("test"), Default::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        );
        assert_eq!(
            "hello 42".as_bytes(),
            cached
                .get_opts(&Path::from("test"), Default::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn in_cache_only_still_serves() {
        let cache = object_store::memory::InMemory::new();
        let backing = NonDestructiveObjectStore::new(object_store::memory::InMemory::new());
        let cached = CachingObjectStore::new(cache, backing);
        assert!(cached
            .cache
            .put_opts(
                &Path::from("test"),
                "hello 42".into(),
                PutMode::Create.into()
            )
            .await
            .is_ok());

        assert!(cached
            .backing
            .get_opts(&Path::from("test"), Default::default())
            .await
            .is_err());

        assert_eq!(
            "hello 42".as_bytes(),
            cached
                .get_opts(&Path::from("test"), Default::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        );
        assert_eq!(
            "hello 42".as_bytes(),
            cached
                .get_opts(&Path::from("test"), Default::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        );

        assert!(cached
            .backing
            .get_opts(&Path::from("test"), Default::default())
            .await
            .is_err());
    }

    // This test verifies invariants necessary to prevent false successes in caching_cannot_delete.
    #[test]
    fn caching_cannot_delete_aux() {
        let backing = object_store::memory::InMemory::new();
        assert!(backing.supports_delete());
    }

    #[test]
    #[should_panic]
    fn caching_cannot_delete() {
        let cache = object_store::memory::InMemory::new();
        // Use a destructive store and it will panic.
        let backing = object_store::memory::InMemory::new();
        if backing.supports_delete() {
            let _cached = CachingObjectStore::new(cache, backing);
        }
    }

    #[tokio::test]
    async fn caching_impls_safe() {
        let cache = object_store::memory::InMemory::new();
        let backing = NonDestructiveObjectStore::new(object_store::memory::InMemory::new());
        let cached = CachingObjectStore::new(cache, backing);
        assert!(!cached.supports_delete());
    }
}
