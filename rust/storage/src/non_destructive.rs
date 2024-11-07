//! A non-destructive wrapper around object store.  It turns `delete` operations into
//! `not-implemented` errors.  It makes sure the put mode is create for all block writes.
//! Unfortunately the multi-part upload cannot exhibit this same safety.  Copy will be silently
//! transformed to a copy-if-not-exist.

use std::fmt::{Debug, Display};
use std::ops::Range;
use std::sync::Arc;

use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
};

use bytes::Bytes;
use futures::stream::BoxStream;

use super::SafeObjectStore;

#[derive(Clone)]
pub struct NonDestructiveObjectStore {
    object_store: Arc<dyn ObjectStore>,
}

impl NonDestructiveObjectStore {
    pub fn new<O: ObjectStore>(object_store: O) -> Self {
        let object_store = Arc::new(object_store);
        Self { object_store }
    }
}

impl Debug for NonDestructiveObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NonDestructiveObjectStore")
    }
}

impl Display for NonDestructiveObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NonDestructiveObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for NonDestructiveObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        match opts.mode {
            PutMode::Overwrite => {
                return Err(object_store::Error::NotImplemented);
            }
            PutMode::Create | PutMode::Update(_) => {}
        };
        self.object_store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.object_store.put_multipart_opts(location, opts).await
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

    async fn delete(&self, _: &Path) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.object_store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.object_store.copy_if_not_exists(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.object_store.copy_if_not_exists(from, to).await
    }
}

impl SafeObjectStore for NonDestructiveObjectStore {
    fn supports_delete(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use object_store::path::Path;
    use object_store::{ObjectStore, PutMode};

    use super::NonDestructiveObjectStore;

    use crate::SafeObjectStore;

    #[tokio::test]
    async fn empty() {
        let backing = object_store::memory::InMemory::new();
        let non_destructive = NonDestructiveObjectStore::new(backing);
        assert!(!non_destructive.supports_delete());
    }

    #[tokio::test]
    async fn insert() {
        let backing = object_store::memory::InMemory::new();
        let non_destructive = NonDestructiveObjectStore::new(backing);
        assert!(non_destructive
            .put_opts(
                &Path::from("test"),
                "hello 42".into(),
                PutMode::Create.into()
            )
            .await
            .is_ok());
        assert_eq!(
            "hello 42".as_bytes(),
            non_destructive
                .object_store
                .get_opts(&Path::from("test"), Default::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn overwrite_fails() {
        let backing = object_store::memory::InMemory::new();
        let non_destructive = NonDestructiveObjectStore::new(backing);
        assert!(non_destructive
            .put_opts(
                &Path::from("test"),
                "hello 42".into(),
                PutMode::Overwrite.into()
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn delete_fails() {
        let backing = object_store::memory::InMemory::new();
        let non_destructive = NonDestructiveObjectStore::new(backing);
        assert!(non_destructive
            .put_opts(
                &Path::from("test"),
                "hello 42".into(),
                PutMode::Create.into()
            )
            .await
            .is_ok());
        assert_eq!(
            "hello 42".as_bytes(),
            non_destructive
                .object_store
                .get_opts(&Path::from("test"), Default::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        );
        assert!(non_destructive.delete(&Path::from("test")).await.is_err());
    }
}
