use std::ops::Range;
use std::sync::Mutex;
use std::time::Instant;

use biometrics::{Collector, Counter, Moments};
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

//////////////////////////////////////////// biometrics ////////////////////////////////////////////

static PUT_OPTS_SUCCESS: Counter = Counter::new("wal3.put_ops_success");
static PUT_MULTIPART_OPTS_SUCCESS: Counter = Counter::new("wal3.put_multipart_ops_success");
static GET_OPTS_SUCCESS: Counter = Counter::new("wal3.get_opts.success");
static GET_RANGES_SUCCESS: Counter = Counter::new("wal3.get_ranges.success");
static HEAD_SUCCESS: Counter = Counter::new("wal3.head.success");
static DELETE_SUCCESS: Counter = Counter::new("wal3.delete.success");
static LIST_SUCCESS: Counter = Counter::new("wal3.list.success");
static LIST_WITH_DELIMITER_SUCCESS: Counter = Counter::new("wal3.list_with_delimiter.success");
static COPY_SUCCESS: Counter = Counter::new("wal3.copy.success");
static COPY_IF_NOT_EXISTS_SUCCESS: Counter = Counter::new("wal3.copy_if_not_exists.success");
static CREATE_MULTIPART_SUCCESS: Counter = Counter::new("wal3.create_multipart.success");
static PUT_PART_SUCCESS: Counter = Counter::new("wal3.put_part.success");
static COMPLETE_MULTIPART_SUCCESS: Counter = Counter::new("wal3.complete_multipart.success");
static ABORT_MULTIPART_SUCCESS: Counter = Counter::new("wal3.abort_multipart.success");

static PUT_OPTS_TOO_SLOW: Counter = Counter::new("wal3.put_ops.too_slow");
static PUT_MULTIPART_OPTS_TOO_SLOW: Counter = Counter::new("wal3.put_multipart_ops.too_slow");
static GET_OPTS_TOO_SLOW: Counter = Counter::new("wal3.get_opts.too_slow");
static GET_RANGES_TOO_SLOW: Counter = Counter::new("wal3.get_ranges.too_slow");
static HEAD_TOO_SLOW: Counter = Counter::new("wal3.head.too_slow");
static DELETE_TOO_SLOW: Counter = Counter::new("wal3.delete.too_slow");
static LIST_TOO_SLOW: Counter = Counter::new("wal3.list.too_slow");
static LIST_WITH_DELIMITER_TOO_SLOW: Counter = Counter::new("wal3.list_with_delimiter.too_slow");
static COPY_TOO_SLOW: Counter = Counter::new("wal3.copy.too_slow");
static COPY_IF_NOT_EXISTS_TOO_SLOW: Counter = Counter::new("wal3.copy_if_not_exists.too_slow");
static CREATE_MULTIPART_TOO_SLOW: Counter = Counter::new("wal3.create_multipart.too_slow");
static PUT_PART_TOO_SLOW: Counter = Counter::new("wal3.put_part.too_slow");
static COMPLETE_MULTIPART_TOO_SLOW: Counter = Counter::new("wal3.complete_multipart.too_slow");
static ABORT_MULTIPART_TOO_SLOW: Counter = Counter::new("wal3.abort_multipart.too_slow");

static PUT_OPTS_LATENCY: Moments = Moments::new("wal3.put_ops.latency");
static PUT_MULTIPART_OPTS_LATENCY: Moments = Moments::new("wal3.put_multipart_ops.latency");
static GET_OPTS_LATENCY: Moments = Moments::new("wal3.get_opts.latency");
static GET_RANGES_LATENCY: Moments = Moments::new("wal3.get_ranges.latency");
static HEAD_LATENCY: Moments = Moments::new("wal3.head.latency");
static DELETE_LATENCY: Moments = Moments::new("wal3.delete.latency");
static LIST_LATENCY: Moments = Moments::new("wal3.list.latency");
static LIST_WITH_DELIMITER_LATENCY: Moments = Moments::new("wal3.list_with_delimiter.latency");
static COPY_LATENCY: Moments = Moments::new("wal3.copy.latency");
static COPY_IF_NOT_EXISTS_LATENCY: Moments = Moments::new("wal3.copy_if_not_exists.latency");
static CREATE_MULTIPART_LATENCY: Moments = Moments::new("wal3.create_multipart.latency");
static PUT_PART_LATENCY: Moments = Moments::new("wal3.put_part.latency");
static COMPLETE_MULTIPART_LATENCY: Moments = Moments::new("wal3.complete_multipart.latency");
static ABORT_MULTIPART_LATENCY: Moments = Moments::new("wal3.abort_multipart.latency");

pub fn register_biometrics(collector: &Collector) {
    collector.register_counter(&PUT_OPTS_SUCCESS);
    collector.register_counter(&PUT_MULTIPART_OPTS_SUCCESS);
    collector.register_counter(&GET_OPTS_SUCCESS);
    collector.register_counter(&GET_RANGES_SUCCESS);
    collector.register_counter(&HEAD_SUCCESS);
    collector.register_counter(&DELETE_SUCCESS);
    collector.register_counter(&LIST_SUCCESS);
    collector.register_counter(&LIST_WITH_DELIMITER_SUCCESS);
    collector.register_counter(&COPY_SUCCESS);
    collector.register_counter(&COPY_IF_NOT_EXISTS_SUCCESS);
    collector.register_counter(&CREATE_MULTIPART_SUCCESS);
    collector.register_counter(&PUT_PART_SUCCESS);
    collector.register_counter(&COMPLETE_MULTIPART_SUCCESS);
    collector.register_counter(&ABORT_MULTIPART_SUCCESS);

    collector.register_counter(&PUT_OPTS_TOO_SLOW);
    collector.register_counter(&PUT_MULTIPART_OPTS_TOO_SLOW);
    collector.register_counter(&GET_OPTS_TOO_SLOW);
    collector.register_counter(&GET_RANGES_TOO_SLOW);
    collector.register_counter(&HEAD_TOO_SLOW);
    collector.register_counter(&DELETE_TOO_SLOW);
    collector.register_counter(&LIST_TOO_SLOW);
    collector.register_counter(&LIST_WITH_DELIMITER_TOO_SLOW);
    collector.register_counter(&COPY_TOO_SLOW);
    collector.register_counter(&COPY_IF_NOT_EXISTS_TOO_SLOW);
    collector.register_counter(&CREATE_MULTIPART_TOO_SLOW);
    collector.register_counter(&PUT_PART_TOO_SLOW);
    collector.register_counter(&COMPLETE_MULTIPART_TOO_SLOW);
    collector.register_counter(&ABORT_MULTIPART_TOO_SLOW);

    collector.register_moments(&PUT_OPTS_LATENCY);
    collector.register_moments(&PUT_MULTIPART_OPTS_LATENCY);
    collector.register_moments(&GET_OPTS_LATENCY);
    collector.register_moments(&GET_RANGES_LATENCY);
    collector.register_moments(&HEAD_LATENCY);
    collector.register_moments(&DELETE_LATENCY);
    collector.register_moments(&LIST_LATENCY);
    collector.register_moments(&LIST_WITH_DELIMITER_LATENCY);
    collector.register_moments(&COPY_LATENCY);
    collector.register_moments(&COPY_IF_NOT_EXISTS_LATENCY);
    collector.register_moments(&CREATE_MULTIPART_LATENCY);
    collector.register_moments(&PUT_PART_LATENCY);
    collector.register_moments(&COMPLETE_MULTIPART_LATENCY);
    collector.register_moments(&ABORT_MULTIPART_LATENCY);
}

///////////////////////////////////////// SimulationOptions ////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, arrrg_derive::CommandLine)]
pub struct SimulationOptions {
    #[arrrg(optional, "Microseconds of average latency for put_opts.")]
    put_opts_ms: u64,
    #[arrrg(optional, "Microseconds of average latency for put_multipart_opts.")]
    put_multipart_opts_ms: u64,
    #[arrrg(optional, "Microseconds of average latency for get_opts.")]
    get_opts_ms: u64,
    #[arrrg(optional, "Microseconds of average latency for get_ranges.")]
    get_ranges_ms: u64,
    #[arrrg(optional, "Microseconds of average latency for head.")]
    head_ms: u64,
    #[arrrg(optional, "Microseconds of average latency for delete.")]
    delete_ms: u64,
    #[arrrg(optional, "Microseconds of average latency for list.")]
    list_ms: u64,
    #[arrrg(optional, "Microseconds of average latency for list_with_delimiter.")]
    list_with_delimiter_ms: u64,
    #[arrrg(optional, "Microseconds of average latency for copy.")]
    copy_ms: u64,
    #[arrrg(optional, "Microseconds of average latency for copy_if_not_exists.")]
    copy_if_not_exists_ms: u64,
    #[arrrg(optional, "Microseconds of average latency for create_multipart.")]
    create_multipart_ms: u64,
    #[arrrg(optional, "Microseconds of average latency for put_part.")]
    put_part_ms: u64,
    #[arrrg(optional, "Microseconds of average latency for complete_multipart.")]
    complete_multipart_ms: u64,
    #[arrrg(optional, "Microseconds of average latency for abort_multipart.")]
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
    ($this:ident, $interarrival_ms:expr, $operation:expr, $moments:expr, $fast_counter:expr, $slow_counter:expr) => {{
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
            $fast_counter.click();
        } else {
            $slow_counter.click();
        }
        $moments.add(start.elapsed().as_micros() as f64);
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
            self.object_store.put_opts(location, payload, opts).await,
            PUT_OPTS_LATENCY,
            PUT_OPTS_SUCCESS,
            PUT_OPTS_TOO_SLOW
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
            self.object_store.put_multipart_opts(location, opts).await,
            PUT_MULTIPART_OPTS_LATENCY,
            PUT_MULTIPART_OPTS_SUCCESS,
            PUT_MULTIPART_OPTS_TOO_SLOW
        )
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        slow_operation!(
            self,
            self.options.get_opts_ms,
            self.object_store.get_opts(location, options).await,
            GET_OPTS_LATENCY,
            GET_OPTS_SUCCESS,
            GET_OPTS_TOO_SLOW
        )
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        slow_operation!(
            self,
            self.options.get_ranges_ms,
            self.object_store.get_ranges(location, ranges).await,
            GET_RANGES_LATENCY,
            GET_RANGES_SUCCESS,
            GET_RANGES_TOO_SLOW
        )
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        slow_operation!(
            self,
            self.options.head_ms,
            self.object_store.head(location).await,
            HEAD_LATENCY,
            HEAD_SUCCESS,
            HEAD_TOO_SLOW
        )
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        slow_operation!(
            self,
            self.options.delete_ms,
            self.object_store.delete(location).await,
            DELETE_LATENCY,
            DELETE_SUCCESS,
            DELETE_TOO_SLOW
        )
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        slow_operation!(
            self,
            self.options.list_with_delimiter_ms,
            self.object_store.list_with_delimiter(prefix).await,
            LIST_WITH_DELIMITER_LATENCY,
            LIST_WITH_DELIMITER_SUCCESS,
            LIST_WITH_DELIMITER_TOO_SLOW
        )
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        slow_operation!(
            self,
            self.options.copy_ms,
            self.object_store.copy(from, to).await,
            COPY_LATENCY,
            COPY_SUCCESS,
            COPY_TOO_SLOW
        )
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        slow_operation!(
            self,
            self.options.copy_if_not_exists_ms,
            self.object_store.copy_if_not_exists(from, to).await,
            COPY_IF_NOT_EXISTS_LATENCY,
            COPY_IF_NOT_EXISTS_SUCCESS,
            COPY_IF_NOT_EXISTS_TOO_SLOW
        )
    }
}

#[async_trait::async_trait]
impl<O: MultipartStore + ObjectStore> MultipartStore for LatencyControlledObjectStore<O> {
    async fn create_multipart(&self, path: &Path) -> Result<MultipartId> {
        slow_operation!(
            self,
            self.options.create_multipart_ms,
            self.object_store.create_multipart(path).await,
            CREATE_MULTIPART_LATENCY,
            CREATE_MULTIPART_SUCCESS,
            CREATE_MULTIPART_TOO_SLOW
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
                .await,
            PUT_PART_LATENCY,
            PUT_PART_SUCCESS,
            PUT_PART_TOO_SLOW
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
            self.object_store.complete_multipart(path, id, parts).await,
            COMPLETE_MULTIPART_LATENCY,
            COMPLETE_MULTIPART_SUCCESS,
            COMPLETE_MULTIPART_TOO_SLOW
        )
    }

    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> Result<()> {
        slow_operation!(
            self,
            self.options.abort_multipart_ms,
            self.object_store.abort_multipart(path, id).await,
            ABORT_MULTIPART_LATENCY,
            ABORT_MULTIPART_SUCCESS,
            ABORT_MULTIPART_TOO_SLOW
        )
    }
}
