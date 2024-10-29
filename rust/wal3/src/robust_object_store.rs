use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::{select_biased, FutureExt};
use futures_core::stream::BoxStream;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartId, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
};

///////////////////////////////////////// RobustObjectStore ////////////////////////////////////////

pub struct RobustObjectStore<O: ObjectStore> {
    object_store: Arc<O>,
}

impl<O: ObjectStore> RobustObjectStore<O> {
    pub fn new(object_store: O) -> Self {
        let object_store = Arc::new(object_store);
        Self { object_store }
    }
}

impl<O: ObjectStore> std::fmt::Debug for RobustObjectStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RobustObjectStore({:?})", self.object_store)
    }
}

impl<O: ObjectStore> std::fmt::Display for RobustObjectStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RobustObjectStore({})", self.object_store)
    }
}

#[async_trait::async_trait]
impl<O: ObjectStore> ObjectStore for RobustObjectStore<O> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let mut request = {
            let object_store = Arc::clone(&self.object_store);
            let location = location.clone();
            let payload = payload.clone();
            let opts = opts.clone();
            Box::pin(async move { object_store.put_opts(&location, payload, opts).await }.fuse())
        };
        select_biased! {
            x = request => { x },
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)).fuse() => {
                let mut request2 = {
                    let object_store = Arc::clone(&self.object_store);
                    let location = location.clone();
                    Box::pin(async move { object_store.put_opts(&location, payload, opts).await }.fuse())
                };
                select_biased! {
                    x = request => {
                        match x {
                            Ok(res) => Ok(res),
                            Err(_) => {
                                request2.await
                            },
                        }
                    },
                    x = request2 => {
                        match x {
                            Ok(res) => Ok(res),
                            Err(_) => {
                                request.await
                            },
                        }
                    },
                }
            },
        }
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

    async fn delete(&self, location: &Path) -> Result<()> {
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

#[async_trait::async_trait]
impl<O: MultipartStore + ObjectStore> MultipartStore for RobustObjectStore<O> {
    async fn create_multipart(&self, path: &Path) -> Result<MultipartId> {
        self.object_store.create_multipart(path).await
    }

    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        part_idx: usize,
        payload: PutPayload,
    ) -> Result<PartId> {
        self.object_store
            .put_part(path, id, part_idx, payload)
            .await
    }

    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        self.object_store.complete_multipart(path, id, parts).await
    }

    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> Result<()> {
        self.object_store.abort_multipart(path, id).await
    }
}
