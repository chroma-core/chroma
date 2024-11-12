use std::ops::Range;
use std::sync::Mutex;
use std::time::Instant;

use bytes::Bytes;
use futures_core::stream::BoxStream;
use guacamole::combinators::*;
use guacamole::Guacamole;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartId, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
};

///////////////////////////////////////// SimulationOptions ////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimulationOptions {
    put_opts_ms: u64,
    put_multipart_opts_ms: u64,
    get_opts_ms: u64,
    get_ranges_ms: u64,
    head_ms: u64,
    delete_ms: u64,
    list_ms: u64,
    list_with_delimiter_ms: u64,
    copy_ms: u64,
    copy_if_not_exists_ms: u64,
    create_multipart_ms: u64,
    put_part_ms: u64,
    complete_multipart_ms: u64,
    abort_multipart_ms: u64,
}

impl Default for SimulationOptions {
    fn default() -> Self {
        SimulationOptions {
            put_opts_ms: 100u64,
            put_multipart_opts_ms: 100u64,
            get_opts_ms: 100u64,
            get_ranges_ms: 100u64,
            head_ms: 100u64,
            delete_ms: 100u64,
            list_ms: 100u64,
            list_with_delimiter_ms: 100u64,
            copy_ms: 100u64,
            copy_if_not_exists_ms: 100u64,
            create_multipart_ms: 100u64,
            put_part_ms: 100u64,
            complete_multipart_ms: 100u64,
            abort_multipart_ms: 100u64,
        }
    }
}

/////////////////////////////////// LatencyControlledObjectStore ///////////////////////////////////

pub struct LatencyControlledObjectStore<O: ObjectStore> {
    object_store: O,
    guacamole: Mutex<Guacamole>,
    options: SimulationOptions,
}

impl<O: ObjectStore> LatencyControlledObjectStore<O> {
    pub fn new(options: SimulationOptions, object_store: O, guacamole: Guacamole) -> Self {
        let guacamole = Mutex::new(guacamole);
        Self {
            object_store,
            guacamole,
            options,
        }
    }
}

impl<O: ObjectStore> std::fmt::Debug for LatencyControlledObjectStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LatencyControlledObjectStore({:?})", self.object_store)
    }
}

impl<O: ObjectStore> std::fmt::Display for LatencyControlledObjectStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LatencyControlledObjectStore({})", self.object_store)
    }
}

macro_rules! slow_operation {
    ($this:ident, $interarrival_ms:expr, $operation:expr) => {{
        // Exponentially distributed interarrival times
        let interarrival_rate = 1_000.0 / ($interarrival_ms as f64);
        let duration = {
            let mut guac = $this.guacamole.lock().unwrap();
            interarrival_duration(interarrival_rate)(&mut guac)
        };
        let start = Instant::now();
        let res = $operation;
        let elapsed = start.elapsed();
        if elapsed < duration {
            tokio::time::sleep(duration - elapsed).await;
        }
        res
    }};
}

#[async_trait::async_trait]
impl<O: ObjectStore> ObjectStore for LatencyControlledObjectStore<O> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        slow_operation!(
            self,
            self.options.put_opts_ms,
            self.object_store.put_opts(location, payload, opts).await
        )
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        slow_operation!(
            self,
            self.options.put_multipart_opts_ms,
            self.object_store.put_multipart_opts(location, opts).await
        )
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        slow_operation!(
            self,
            self.options.get_opts_ms,
            self.object_store.get_opts(location, options).await
        )
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        slow_operation!(
            self,
            self.options.get_ranges_ms,
            self.object_store.get_ranges(location, ranges).await
        )
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        slow_operation!(
            self,
            self.options.head_ms,
            self.object_store.head(location).await
        )
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        slow_operation!(
            self,
            self.options.delete_ms,
            self.object_store.delete(location).await
        )
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        slow_operation!(
            self,
            self.options.list_with_delimiter_ms,
            self.object_store.list_with_delimiter(prefix).await
        )
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        slow_operation!(
            self,
            self.options.copy_ms,
            self.object_store.copy(from, to).await
        )
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        slow_operation!(
            self,
            self.options.copy_if_not_exists_ms,
            self.object_store.copy_if_not_exists(from, to).await
        )
    }
}

#[async_trait::async_trait]
impl<O: MultipartStore + ObjectStore> MultipartStore for LatencyControlledObjectStore<O> {
    async fn create_multipart(&self, path: &Path) -> Result<MultipartId> {
        slow_operation!(
            self,
            self.options.create_multipart_ms,
            self.object_store.create_multipart(path).await
        )
    }

    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        part_idx: usize,
        payload: PutPayload,
    ) -> Result<PartId> {
        slow_operation!(
            self,
            self.options.put_part_ms,
            self.object_store
                .put_part(path, id, part_idx, payload)
                .await
        )
    }

    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        slow_operation!(
            self,
            self.options.complete_multipart_ms,
            self.object_store.complete_multipart(path, id, parts).await
        )
    }

    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> Result<()> {
        slow_operation!(
            self,
            self.options.abort_multipart_ms,
            self.object_store.abort_multipart(path, id).await
        )
    }
}
