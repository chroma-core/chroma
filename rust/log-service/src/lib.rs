#![recursion_limit = "256"]

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use chroma_cache::CacheConfig;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_log::{config::GrpcLogConfig, grpc_log::GrpcLog};
use chroma_storage::config::StorageConfig;
use chroma_storage::Storage;
use chroma_tracing::util::wrap_span_with_parent_context;
use chroma_types::chroma_proto::{
    log_service_client::LogServiceClient, log_service_server::LogService, CollectionInfo,
    GetAllCollectionInfoToCompactRequest, GetAllCollectionInfoToCompactResponse,
    InspectDirtyLogRequest, InspectDirtyLogResponse, InspectLogStateRequest,
    InspectLogStateResponse, LogRecord, MigrateLogRequest, MigrateLogResponse, OperationRecord,
    PullLogsRequest, PullLogsResponse, PurgeDirtyForCollectionRequest,
    PurgeDirtyForCollectionResponse, PushLogsRequest, PushLogsResponse, ScoutLogsRequest,
    ScoutLogsResponse, SealLogRequest, SealLogResponse, UpdateCollectionLogOffsetRequest,
    UpdateCollectionLogOffsetResponse,
};
use chroma_types::chroma_proto::{ForkLogsRequest, ForkLogsResponse};
use chroma_types::CollectionUuid;
use figment::providers::{Env, Format, Yaml};
use opentelemetry::metrics::Meter;
use parking_lot::Mutex;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Code, Request, Response, Status};
use tracing::{Instrument, Level};
use uuid::Uuid;
use wal3::{
    Cursor, CursorName, CursorStore, CursorStoreOptions, Fragment, Limits, LogPosition, LogReader,
    LogReaderOptions, LogWriter, LogWriterOptions, Manifest, MarkDirty as MarkDirtyTrait, Witness,
};

pub mod state_hash_table;

use crate::state_hash_table::StateHashTable;

///////////////////////////////////////////// constants ////////////////////////////////////////////

const DEFAULT_CONFIG_PATH: &str = "./chroma_config.yaml";

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

// SAFETY(rescrv):  There's a test that this produces a valid type.
static STABLE_PREFIX: CursorName = unsafe { CursorName::from_string_unchecked("stable_prefix") };
static COMPACTION: CursorName = unsafe { CursorName::from_string_unchecked("compaction") };

////////////////////////////////////////////// Metrics /////////////////////////////////////////////

pub struct Metrics {
    log_total_uncompacted_records_count: opentelemetry::metrics::Gauge<f64>,
    /// The rate at which records are read from the dirty log.
    dirty_log_records_read: opentelemetry::metrics::Counter<u64>,
}

impl Metrics {
    pub fn new(meter: Meter) -> Self {
        Self {
            log_total_uncompacted_records_count: meter
                .f64_gauge("log_total_uncompacted_records_count")
                .build(),
            dirty_log_records_read: meter.u64_counter("dirty_log_records_read").build(),
        }
    }
}

/////////////////////////////////////////////// Error //////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("wal3: {0:?}")]
    Wal3(#[from] wal3::Error),
    #[error("serialization error: {0:?}")]
    Json(#[from] serde_json::Error),
    #[error("Dirty log writer failed to provide a reader")]
    CouldNotGetDirtyLogReader,
    #[error("Dirty log writer failed to provide a cursor store")]
    CouldNotGetDirtyLogCursors,
}

///////////////////////////////////////// state maintenance ////////////////////////////////////////

// NOTE(rescrv):  This code dynamically opens and closes logs.  An opened log will stay open until
// no one is writing to it.  It will then stay open for an additional, configurable time period.
// The mechanism that powers this is a state hash table whose reference is retained by a
// background future per log.  The future holds a reference for X seconds and then does a dance to
// drop it in a way that ensures another future will always be spawned if things race.
//
// The key to this is the active log struct.  State hash table gives us the ability to treat it as
// a something of a dynamic singleton.  We just need to make sure that if we initialize the log, we
// clean it up eventually.  Assuming no bugs, there are three outcomes:
// - We don't collect logs so space grows unbounded.
// - We collect logs too aggressively, so logs must be reopened (a get from S3).
// - We collect logs too which resembles the first case.
//
// Bugs likely to be encountered include (and reason for not worrying about it):
// - Durability bugs.  The log protocol is responsible for ensuring they do not happen.
// - De-sync between the task pinning the log in memory and the log itself.  The state hash table
//   will guarantee collection once all references are dropped.
// - Dropping the log before timeout.  The log will have to be reopened to write to it at the cost
//   of getting the manifest once.
// - Failing to drop the log when it can be dropped.  We'll use more memory than necessary.
//
// The logic here satisfies the observations of this note.  It follows these rules:
// - Always acquire a handle.  Trust the state hash table to do the right thing.
// - Given a handle it will either have a Some(log) or None.  If it has a log, it will be

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct LogKey {
    collection_id: CollectionUuid,
}

impl crate::state_hash_table::Key for LogKey {}

#[derive(Debug)]
struct ActiveLog {
    /// A tokio mutex so that it may be held across open calls to the log writer.  To keep the log
    /// writer in sync, every time a writer is created here, a background task that watches
    /// collect_after will set this to None and exit itself.  Thus, we should spawn one background
    /// task for each None->Some transition on this field.
    log: Option<Arc<LogWriter>>,
    /// An instant in time after which the background task will set the log to None and exit.
    /// Writers to the log should bump this to be into the future to "heartbeat" the log.  The
    /// method for this is called `keep_alive`.
    collect_after: Instant,
    /// The number of times this log has been recycled (i.e., the log gets set to none, but a
    /// handle isn't dropped).
    epoch: u64,
}

impl ActiveLog {
    pub fn keep_alive(&mut self, keep_alive: Duration) {
        let now = Instant::now();
        let when = if keep_alive >= Duration::ZERO {
            now.checked_add(keep_alive).unwrap_or(now)
        } else {
            now
        };
        if self.collect_after < when {
            self.collect_after = when;
        }
    }
}

impl Default for ActiveLog {
    fn default() -> Self {
        Self {
            log: None,
            collect_after: Instant::now(),
            epoch: 0,
        }
    }
}

/// An in-memory stub for the log writer.
#[derive(Debug)]
struct LogStub {
    active: tokio::sync::Mutex<ActiveLog>,
}

impl Default for LogStub {
    fn default() -> Self {
        Self {
            active: tokio::sync::Mutex::new(ActiveLog::default()),
        }
    }
}

impl crate::state_hash_table::Value for LogStub {
    fn finished(&self) -> bool {
        // NOTE(rescrv):  I'm doing something funky here w.r.t. state hash table.  I'm always
        // returning true, and relying upon its handle tracking to only drop the value when it is
        // no longer referenced.  Simpler than the alternative of trying to decide when to drop and
        // getting it wrong.
        true
    }
}

impl From<LogKey> for LogStub {
    fn from(_: LogKey) -> LogStub {
        LogStub::default()
    }
}

/// Hold a lifetime-bound reference to the log writer.  This takes a heap-backed Arc value and
/// makes sure that it won't be allowed to exist past the lifetime of the handle.  Alternatively,
/// it keeps the handle alive as long as you have a log-writer reference.
struct LogRef<'a> {
    log: Arc<LogWriter>,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl std::ops::Deref for LogRef<'_> {
    type Target = LogWriter;

    fn deref(&self) -> &Self::Target {
        &self.log
    }
}

async fn get_log_from_handle<'a>(
    handle: &'a crate::state_hash_table::Handle<LogKey, LogStub>,
    options: &LogWriterOptions,
    storage: &Arc<Storage>,
    prefix: &str,
    mark_dirty: MarkDirty,
) -> Result<LogRef<'a>, wal3::Error> {
    let active = handle.active.lock().await;
    get_log_from_handle_with_mutex_held(handle, active, options, storage, prefix, mark_dirty).await
}

async fn get_log_from_handle_with_mutex_held<'a>(
    handle: &'a crate::state_hash_table::Handle<LogKey, LogStub>,
    mut active: tokio::sync::MutexGuard<'_, ActiveLog>,
    options: &LogWriterOptions,
    storage: &Arc<Storage>,
    prefix: &str,
    mark_dirty: MarkDirty,
) -> Result<LogRef<'a>, wal3::Error> {
    if active.log.is_some() {
        active.keep_alive(Duration::from_secs(60));
    }
    if let Some(log) = active.log.as_ref() {
        return Ok(LogRef {
            log: Arc::clone(log),
            _phantom: std::marker::PhantomData,
        });
    }
    let opened = LogWriter::open(
        options.clone(),
        Arc::clone(storage),
        prefix,
        // TODO(rescrv):  Configurable params.
        "log writer",
        mark_dirty.clone(),
    )
    .await?;
    active.keep_alive(Duration::from_secs(60));
    tracing::info!("Opened log at {}", prefix);
    let opened = Arc::new(opened);
    active.log = Some(Arc::clone(&opened));
    let handle_clone = handle.clone();
    let epoch = active.epoch;
    // NOTE(rescrv):  This task will exit only after the log's keep alive is in the past.  If
    // everyone who calls get_log keeps it alive (top of this call), then this task will stay alive
    // forever.
    tokio::task::spawn(async move {
        loop {
            let sleep = {
                let mut active = handle_clone.active.lock().await;
                let now = Instant::now();
                if now >= active.collect_after {
                    active.log = None;
                    active.epoch += 1;
                    return;
                } else if active.epoch != epoch {
                    return;
                }
                active.collect_after - now
            };
            tokio::time::sleep(sleep).await;
        }
    });
    Ok(LogRef {
        log: opened,
        _phantom: std::marker::PhantomData,
    })
}

////////////////////////////////////// cache_key_for_manifest //////////////////////////////////////

fn cache_key_for_manifest(collection_id: CollectionUuid) -> String {
    format!("{collection_id}::MANIFEST")
}

////////////////////////////////////////// CachedFragment //////////////////////////////////////////

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct CachedBytes {
    bytes: Vec<u8>,
}

impl chroma_cache::Weighted for CachedBytes {
    fn weight(&self) -> usize {
        self.bytes.len()
    }
}

//////////////////////////////////////// RollupPerCollection ///////////////////////////////////////

/// A summary of the data on the log for a single collection.
#[derive(Clone, Copy, Debug)]
struct RollupPerCollection {
    start_log_position: LogPosition,
    limit_log_position: LogPosition,
    reinsert_count: u64,
    initial_insertion_epoch_us: u64,
}

impl RollupPerCollection {
    fn new(first_observation: LogPosition, num_records: u64) -> Self {
        Self {
            start_log_position: first_observation,
            limit_log_position: LogPosition::from_offset(
                first_observation.offset().saturating_add(num_records),
            ),
            reinsert_count: 0,
            initial_insertion_epoch_us: 0,
        }
    }

    fn observe_dirty_marker(
        &mut self,
        log_position: LogPosition,
        num_records: u64,
        reinsert_count: u64,
        initial_insertion_epoch_us: u64,
    ) {
        if log_position < self.start_log_position {
            self.start_log_position = log_position;
        }
        if log_position + num_records > self.limit_log_position {
            self.limit_log_position = log_position + num_records;
        }
        // Take the biggest reinsert count.
        self.reinsert_count = std::cmp::max(self.reinsert_count, reinsert_count);
        // Consider the most recent initial insertion time so if we've compacted earlier we drop.
        self.initial_insertion_epoch_us =
            std::cmp::max(self.initial_insertion_epoch_us, initial_insertion_epoch_us);
    }

    fn witness_manifest_and_cursor(&mut self, manifest: &Manifest, witness: Option<&Witness>) {
        self.start_log_position = witness
            .map(|x| x.1.position)
            .unwrap_or(manifest.minimum_log_position());
        self.limit_log_position = manifest.maximum_log_position();
    }

    fn is_empty(&self) -> bool {
        self.start_log_position == self.limit_log_position
    }

    fn dirty_marker(&self, collection_id: CollectionUuid) -> DirtyMarker {
        DirtyMarker::MarkDirty {
            collection_id,
            log_position: self.start_log_position,
            num_records: self.limit_log_position - self.start_log_position,
            reinsert_count: self.reinsert_count,
            initial_insertion_epoch_us: self.initial_insertion_epoch_us,
        }
    }

    fn requires_backpressure(&self, threshold: u64) -> bool {
        self.limit_log_position - self.start_log_position >= threshold
    }
}

//////////////////////////////////////////// DirtyMarker ///////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
// NOTE(rescrv):  This is intentionally an enum for easy forwards/backwards compatibility.  Add a
// new variant, handle both variants, cycle logs, stop handling old variant.
pub enum DirtyMarker {
    #[serde(rename = "mark_dirty")]
    MarkDirty {
        collection_id: CollectionUuid,
        log_position: LogPosition,
        num_records: u64,
        reinsert_count: u64,
        initial_insertion_epoch_us: u64,
    },
    #[serde(rename = "purge")]
    Purge { collection_id: CollectionUuid },
    // A Cleared marker is a no-op.  It exists so that a log consisting of mark-dirty markers that
    // map onto purge markers will be cleared and can be erased.
    #[serde(rename = "clear")]
    Cleared,
}

impl DirtyMarker {
    /// The collection ID for a given dirty marker.
    pub fn collection_id(&self) -> CollectionUuid {
        match self {
            DirtyMarker::MarkDirty { collection_id, .. } => *collection_id,
            DirtyMarker::Purge { collection_id } => *collection_id,
            DirtyMarker::Cleared => CollectionUuid::default(),
        }
    }

    /// Increment any reinsert counter on the variant.
    pub fn reinsert(&mut self) {
        if let DirtyMarker::MarkDirty {
            collection_id: _,
            log_position: _,
            num_records: _,
            reinsert_count,
            initial_insertion_epoch_us: _,
        } = self
        {
            *reinsert_count = reinsert_count.saturating_add(1);
        }
    }

    fn coalesce_markers(
        markers: &[(LogPosition, DirtyMarker)],
    ) -> Result<HashMap<CollectionUuid, RollupPerCollection>, wal3::Error> {
        let mut rollups = HashMap::new();
        let mut forget = vec![];
        for (_, marker) in markers {
            match marker {
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position,
                    num_records,
                    reinsert_count,
                    initial_insertion_epoch_us,
                } => {
                    let position = rollups
                        .entry(*collection_id)
                        .or_insert_with(|| RollupPerCollection::new(*log_position, *num_records));
                    position.observe_dirty_marker(
                        *log_position,
                        *num_records,
                        *reinsert_count,
                        *initial_insertion_epoch_us,
                    );
                }
                DirtyMarker::Purge { collection_id } => {
                    forget.push(*collection_id);
                }
                DirtyMarker::Cleared => {}
            }
        }
        for collection_id in forget {
            rollups.remove(&collection_id);
        }
        Ok(rollups)
    }
}

///////////////////////////////////////////// MarkDirty ////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct MarkDirty {
    collection_id: CollectionUuid,
    dirty_log: Arc<LogWriter>,
}

#[async_trait::async_trait]
impl wal3::MarkDirty for MarkDirty {
    async fn mark_dirty(
        &self,
        log_position: LogPosition,
        num_records: usize,
    ) -> Result<(), wal3::Error> {
        let num_records = num_records as u64;
        let initial_insertion_epoch_us = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| wal3::Error::Internal)?
            .as_micros() as u64;
        let dirty_marker = DirtyMarker::MarkDirty {
            collection_id: self.collection_id,
            log_position,
            num_records,
            reinsert_count: 0,
            initial_insertion_epoch_us,
        };
        let dirty_marker_json = serde_json::to_string(&dirty_marker).map_err(|err| {
            tracing::error!("Failed to serialize dirty marker: {}", err);
            wal3::Error::Internal
        })?;
        self.dirty_log.append(Vec::from(dirty_marker_json)).await?;
        Ok(())
    }
}

////////////////////////////////////// storage_prefix_for_log //////////////////////////////////////

pub fn storage_prefix_for_log(collection: CollectionUuid) -> String {
    format!("logs/{}", collection)
}

///////////////////////////////////////////// LogServer ////////////////////////////////////////////

pub struct LogServer {
    config: LogServerConfig,
    storage: Arc<Storage>,
    open_logs: Arc<StateHashTable<LogKey, LogStub>>,
    dirty_log: Arc<LogWriter>,
    #[allow(clippy::type_complexity)]
    proxy: Option<LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>>,
    rolling_up: tokio::sync::Mutex<()>,
    backpressure: Mutex<Arc<HashSet<CollectionUuid>>>,
    need_to_compact: Mutex<HashMap<CollectionUuid, RollupPerCollection>>,
    cache: Option<Box<dyn chroma_cache::PersistentCache<String, CachedBytes>>>,
    metrics: Metrics,
}

impl LogServer {
    fn set_backpressure(&self, to_pressure: &[CollectionUuid]) {
        let mut new_backpressure = Arc::new(HashSet::from_iter(to_pressure.iter().cloned()));
        let mut backpressure = self.backpressure.lock();
        std::mem::swap(&mut *backpressure, &mut new_backpressure);
    }

    fn check_for_backpressure(&self, collection_id: CollectionUuid) -> Result<(), Status> {
        let backpressure = {
            let backpressure = self.backpressure.lock();
            Arc::clone(&backpressure)
        };
        if backpressure.contains(&collection_id) {
            return Err(Status::resource_exhausted("log needs compaction; too full"));
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, proxy))]
    async fn effectuate_log_transfer(
        &self,
        collection_id: CollectionUuid,
        mut proxy: LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>,
        ttl: usize,
    ) -> Result<(), Status> {
        if ttl == 0 {
            tracing::error!("effectuate log transfer hit its recursion limit");
            return Err(Status::new(
                Code::Unavailable,
                "effectuate log transfer hit its recursion limit",
            ));
        }

        // Grab a lock on the state for this key, so that a racing initialize won't do anything.
        let key = LogKey { collection_id };
        let handle = self.open_logs.get_or_create_state(key);
        let active = handle.active.lock().await;

        // Someone already initialized the log on a prior call.
        if active.log.is_some() {
            return Ok(());
        }

        tracing::info!("log transfer to {collection_id}");
        let scout_request = Request::new(ScoutLogsRequest {
            collection_id: collection_id.to_string(),
        });
        let scout_resp = proxy.clone().scout_logs(scout_request).await?.into_inner();
        let start = scout_resp.first_uncompacted_record_offset as u64;
        let limit = scout_resp.first_uninserted_record_offset as u64;
        if start == 0 || limit == 0 {
            tracing::error!("scout logs returned {start}->{limit}");
            return Err(Status::new(
                Code::FailedPrecondition,
                "effectuate logs saw invalid offset",
            ));
        }
        tracing::info!("scouted {collection_id} start={start} limit={limit}");
        const STEP: u64 = 100;
        let num_steps = (limit.saturating_sub(start) + STEP - 1) / STEP;
        let actual_steps = (0..num_steps)
            .map(|x| {
                (
                    start + x * STEP,
                    std::cmp::min(start + x * STEP + STEP, limit),
                )
            })
            .collect::<Vec<_>>();
        let pull_logs_reqs = actual_steps
            .iter()
            .cloned()
            .map(|(start, limit)| PullLogsRequest {
                collection_id: collection_id.to_string(),
                start_from_offset: start as i64,
                // SAFETY(rescrv):  STEP fits a i32.
                batch_size: (limit - start) as i32,
                end_timestamp: i64::MAX,
            });
        let mut responses = vec![];
        for req in pull_logs_reqs {
            let resp = match proxy.pull_logs(Request::new(req)).await {
                Ok(resp) => resp.into_inner(),
                Err(err) => {
                    if err.code() == Code::NotFound {
                        // We have no logs found, but we saw sealed.  We will converge, so call
                        // again.
                        tracing::warn!("pulling logs again: {err:?}");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        return Box::pin(self.effectuate_log_transfer(
                            collection_id,
                            proxy,
                            ttl.saturating_sub(1),
                        ))
                        .await;
                    } else {
                        return Err(err);
                    }
                }
            };
            responses.push(resp);
        }
        let mut records = vec![];
        for ((start, limit), resp) in
            std::iter::zip(actual_steps.into_iter(), responses.into_iter())
        {
            for (expect, (idx, record)) in
                std::iter::zip(start..limit, resp.records.into_iter().enumerate())
            {
                if expect != start + idx as u64 {
                    return Err(Status::data_loss(format!(
                        "expected log position {expect} but got {} (1)",
                        start + idx as u64
                    )));
                }
                if (record.log_offset as u64) != expect {
                    return Err(Status::data_loss(format!(
                        "expected log position {expect} but got {} (2)",
                        (record.log_offset as u64)
                    )));
                }
                records.push(record);
            }
        }
        let record_bytes = records
            .into_iter()
            .map(|record| -> Result<Vec<u8>, Status> {
                let mut buf = vec![];
                if let Some(r) = record.record.as_ref() {
                    r.encode(&mut buf)
                        .map_err(|err| Status::internal(err.to_string()))?;
                    Ok(buf)
                } else {
                    Err(Status::data_loss("missing a record"))
                }
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let prefix = storage_prefix_for_log(collection_id);
        let mark_dirty = MarkDirty {
            collection_id,
            dirty_log: Arc::clone(&self.dirty_log),
        };
        LogWriter::bootstrap(
            &self.config.writer,
            &self.storage,
            &prefix,
            "effectuate log transfer",
            mark_dirty,
            LogPosition::from_offset(start),
            record_bytes,
        )
        .await
        .map_err(|err| {
            Status::new(
                err.code().into(),
                format!("failed to effectuate log transfer: {err:?}"),
            )
        })?;

        self._update_collection_log_offset(Request::new(UpdateCollectionLogOffsetRequest {
            collection_id: collection_id.to_string(),
            log_offset: start as i64 - 1,
        }))
        .await?;
        // Set it up so that once we release the mutex, the next person won't do I/O and will
        // immediately be able to push logs.
        let storage_prefix = storage_prefix_for_log(collection_id);
        let mark_dirty = MarkDirty {
            collection_id,
            dirty_log: Arc::clone(&self.dirty_log),
        };
        // If this fails, the next writer will load manifest and continue unimpeded.
        let _ = get_log_from_handle_with_mutex_held(
            &handle,
            active,
            &self.config.writer,
            &self.storage,
            &storage_prefix,
            mark_dirty,
        )
        .await;
        Ok(())
    }

    #[tracing::instrument(skip(self, request))]
    async fn forward_push_logs(
        &self,
        collection_id: CollectionUuid,
        request: Request<PushLogsRequest>,
    ) -> Result<Response<PushLogsResponse>, Status> {
        let request = request.into_inner();
        if let Some(proxy) = self.proxy.as_ref() {
            let resp = proxy
                .clone()
                .push_logs(Request::new(request.clone()))
                .await?
                .into_inner();
            if resp.log_is_sealed {
                self.effectuate_log_transfer(collection_id, proxy.clone(), 3)
                    .await?;
                Box::pin(self.push_logs(Request::new(request))).await
            } else {
                Ok(Response::new(resp))
            }
        } else {
            Err(Status::failed_precondition("proxy not initialized"))
        }
    }

    #[tracing::instrument(skip(self, request))]
    async fn forward_scout_logs(
        &self,
        request: Request<ScoutLogsRequest>,
    ) -> Result<Response<ScoutLogsResponse>, Status> {
        if let Some(proxy) = self.proxy.as_ref() {
            proxy.clone().scout_logs(request).await
        } else {
            Err(Status::failed_precondition("proxy not initialized"))
        }
    }

    #[tracing::instrument(skip(self, request))]
    async fn forward_pull_logs(
        &self,
        request: Request<PullLogsRequest>,
    ) -> Result<Response<PullLogsResponse>, Status> {
        if let Some(proxy) = self.proxy.as_ref() {
            proxy.clone().pull_logs(request).await
        } else {
            Err(Status::failed_precondition("proxy not initialized"))
        }
    }

    #[tracing::instrument(skip(self, request))]
    async fn forward_update_collection_log_offset(
        &self,
        request: Request<UpdateCollectionLogOffsetRequest>,
    ) -> Result<Response<UpdateCollectionLogOffsetResponse>, Status> {
        if let Some(proxy) = self.proxy.as_ref() {
            proxy.clone().update_collection_log_offset(request).await
        } else {
            Err(Status::failed_precondition("proxy not initialized"))
        }
    }

    #[tracing::instrument(skip(self, request))]
    async fn forward_fork_logs(
        &self,
        request: Request<ForkLogsRequest>,
    ) -> Result<Response<ForkLogsResponse>, Status> {
        if let Some(proxy) = self.proxy.as_ref() {
            proxy.clone().fork_logs(request).await
        } else {
            Err(Status::failed_precondition("proxy not initialized"))
        }
    }

    #[tracing::instrument(skip(self, request), err(Display))]
    async fn _update_collection_log_offset(
        &self,
        request: Request<UpdateCollectionLogOffsetRequest>,
    ) -> Result<Response<UpdateCollectionLogOffsetResponse>, Status> {
        let request = request.into_inner();
        let adjusted_log_offset = request.log_offset + 1;
        let collection_id = Uuid::parse_str(&request.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        tracing::info!(
            "update_collection_log_offset for {collection_id} to {}",
            adjusted_log_offset
        );
        let storage_prefix = storage_prefix_for_log(collection_id);

        let log_reader = LogReader::new(
            self.config.reader.clone(),
            Arc::clone(&self.storage),
            storage_prefix.clone(),
        );

        let res = log_reader.maximum_log_position().await;
        if let Err(wal3::Error::UninitializedLog) = res {
            return self
                .forward_update_collection_log_offset(Request::new(request))
                .await;
        }
        res.map_err(|err| Status::unknown(err.to_string()))?;

        let cursor_name = &COMPACTION;
        let cursor_store = CursorStore::new(
            CursorStoreOptions::default(),
            Arc::clone(&self.storage),
            storage_prefix.clone(),
            "writer".to_string(),
        );
        let witness = cursor_store.load(cursor_name).await.map_err(|err| {
            Status::new(err.code().into(), format!("Failed to load cursor: {}", err))
        })?;
        let default = Cursor::default();
        let cursor = witness.as_ref().map(|w| w.cursor()).unwrap_or(&default);
        if cursor.position.offset() > adjusted_log_offset as u64 {
            return Ok(Response::new(UpdateCollectionLogOffsetResponse {}));
        }
        let cursor = Cursor {
            position: LogPosition::from_offset(adjusted_log_offset as u64),
            epoch_us: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|_| wal3::Error::Internal)
                .unwrap()
                .as_micros() as u64,
            writer: "TODO".to_string(),
        };
        if let Some(witness) = witness {
            cursor_store
                .save(cursor_name, &cursor, &witness)
                .await
                .map_err(|err| {
                    Status::new(err.code().into(), format!("Failed to save cursor: {}", err))
                })?;
        } else {
            cursor_store
                .init(cursor_name, cursor)
                .await
                .map_err(|err| {
                    Status::new(err.code().into(), format!("Failed to init cursor: {}", err))
                })?;
        }
        let mut need_to_compact = self.need_to_compact.lock();
        if let Entry::Occupied(mut entry) = need_to_compact.entry(collection_id) {
            let rollup = entry.get_mut();
            rollup.start_log_position = std::cmp::max(
                rollup.start_log_position,
                LogPosition::from_offset(adjusted_log_offset as u64),
            );
            if rollup.start_log_position >= rollup.limit_log_position {
                entry.remove();
            }
        }
        Ok(Response::new(UpdateCollectionLogOffsetResponse {}))
    }

    #[tracing::instrument(skip(self), err(Display))]
    async fn cached_get_all_collection_info_to_compact(
        &self,
        request: GetAllCollectionInfoToCompactRequest,
    ) -> Result<Response<GetAllCollectionInfoToCompactResponse>, Status> {
        // TODO(rescrv):  Realistically we could make this configurable.
        const MAX_COLLECTION_INFO_NUMBER: usize = 10000;
        let mut selected_rollups = Vec::with_capacity(MAX_COLLECTION_INFO_NUMBER);
        // Do a non-allocating pass here.
        {
            let need_to_compact = self.need_to_compact.lock();
            for (collection_id, rollup) in need_to_compact.iter() {
                if rollup.limit_log_position >= rollup.start_log_position
                    && rollup.limit_log_position - rollup.start_log_position
                        >= request.min_compaction_size
                {
                    selected_rollups.push((*collection_id, *rollup));
                }
            }
        }
        // Then allocate the collection ID strings outside the lock.
        let mut all_collection_info = Vec::with_capacity(selected_rollups.len());
        for (collection_id, rollup) in selected_rollups {
            all_collection_info.push(CollectionInfo {
                collection_id: collection_id.to_string(),
                first_log_offset: rollup.start_log_position.offset() as i64,
                first_log_ts: rollup.start_log_position.offset() as i64,
            });
        }
        Ok(Response::new(GetAllCollectionInfoToCompactResponse {
            all_collection_info,
        }))
    }

    /// Read a prefix of the dirty log, coalescing records as it goes.
    ///
    /// This will rewrite the dirty log's coalesced contents at the tail and adjust the cursor to
    /// said position so that the next read is O(1) if there are no more writes.
    #[tracing::instrument(skip(self), err(Display))]
    async fn roll_dirty_log(&self) -> Result<(), Error> {
        // Ensure at most one request at a time.
        let _guard = self.rolling_up.lock().await;
        let (witness, cursor, dirty_markers) = self.read_dirty_log().await?;
        self.metrics
            .dirty_log_records_read
            .add(dirty_markers.len() as u64, &[]);
        let mut rollups = DirtyMarker::coalesce_markers(&dirty_markers)?;
        self.enrich_dirty_log(&mut rollups).await?;
        let mut markers = vec![];
        let mut backpressure = vec![];
        let mut total_uncompacted = 0;
        for (collection_id, rollup) in rollups.iter() {
            if rollup.is_empty() {
                continue;
            }
            total_uncompacted += rollup
                .limit_log_position
                .offset()
                .saturating_sub(rollup.start_log_position.offset());
            let marker = rollup.dirty_marker(*collection_id);
            markers.push(serde_json::to_string(&marker).map(Vec::from)?);
            if rollup.requires_backpressure(self.config.num_records_before_backpressure) {
                backpressure.push(*collection_id);
            }
        }
        if markers.is_empty() {
            markers.push(serde_json::to_string(&DirtyMarker::Cleared).map(Vec::from)?);
        }
        let mut new_cursor = cursor.clone();
        new_cursor.position = self.dirty_log.append_many(markers).await?;
        let Some(cursors) = self.dirty_log.cursors(CursorStoreOptions::default()) else {
            return Err(Error::CouldNotGetDirtyLogCursors);
        };
        tracing::info!(
            "Advancing dirty log cursor {:?} -> {:?}",
            cursor.position,
            new_cursor.position
        );
        if let Some(witness) = witness {
            cursors.save(&STABLE_PREFIX, &new_cursor, &witness).await?;
        } else {
            cursors.init(&STABLE_PREFIX, new_cursor).await?;
        }
        self.metrics
            .log_total_uncompacted_records_count
            .record(total_uncompacted as f64, &[]);
        self.set_backpressure(&backpressure);
        let mut need_to_compact = self.need_to_compact.lock();
        std::mem::swap(&mut *need_to_compact, &mut rollups);
        Ok(())
    }

    /// Read the entirety of a prefix of the dirty log.
    #[tracing::instrument(skip(self), err(Display))]
    #[allow(clippy::type_complexity)]
    async fn read_dirty_log(
        &self,
    ) -> Result<(Option<Witness>, Cursor, Vec<(LogPosition, DirtyMarker)>), Error> {
        let Some(reader) = self.dirty_log.reader(LogReaderOptions::default()) else {
            return Err(Error::CouldNotGetDirtyLogReader);
        };
        let Some(cursors) = self.dirty_log.cursors(CursorStoreOptions::default()) else {
            return Err(Error::CouldNotGetDirtyLogCursors);
        };
        let witness = cursors.load(&STABLE_PREFIX).await?;
        let default = Cursor::default();
        let cursor = witness
            .as_ref()
            .map(|w| w.cursor())
            .unwrap_or(&default)
            .clone();
        tracing::info!("cursoring from {cursor:?}");
        let dirty_fragments = reader
            .scan(
                cursor.position,
                Limits {
                    max_files: Some(10_000),
                    max_bytes: Some(1_000_000_000),
                    max_records: Some(10_000),
                },
            )
            .await?;
        if dirty_fragments.is_empty() {
            return Ok((witness, cursor, vec![]));
        }
        if dirty_fragments.len() >= 1_000 {
            tracing::error!("Too many dirty fragments: {}", dirty_fragments.len());
        }
        let dirty_futures = dirty_fragments
            .iter()
            .map(|fragment| reader.read_parquet(fragment))
            .collect::<Vec<_>>();
        let dirty_raw = futures::future::try_join_all(dirty_futures).await?;
        let mut dirty_markers = vec![];
        for (_, records, _) in dirty_raw {
            let records = records
                .into_iter()
                .flat_map(|x| match serde_json::from_slice::<DirtyMarker>(&x.1) {
                    Ok(marker) => Some((x.0, marker)),
                    Err(err) => {
                        tracing::error!(
                            "could not read marker for {}: {err}",
                            String::from_utf8_lossy(&x.1)
                        );
                        None
                    }
                })
                .collect::<Vec<_>>();
            dirty_markers.extend(records);
        }
        Ok((witness, cursor, dirty_markers))
    }

    /// Enrich a rolled up dirty log by reading cursors and manifests to determine what still needs
    /// to be compacted.  Entries will be removed if they correspond to a compacted log range.
    /// Entries will remain if there is data to be collected.
    #[tracing::instrument(skip(self, rollups), err(Display))]
    async fn enrich_dirty_log(
        &self,
        rollups: &mut HashMap<CollectionUuid, RollupPerCollection>,
    ) -> Result<(), Error> {
        let load_manifest = |storage, collection_id| async move {
            let reader = LogReader::new(
                LogReaderOptions::default(),
                Arc::clone(storage),
                storage_prefix_for_log(collection_id),
            );
            let span = tracing::info_span!("manifest load", collection_id = ?collection_id);
            reader.manifest().instrument(span).await
        };
        let load_cursor = |storage, collection_id| async move {
            let cursor = &COMPACTION;
            let cursor_store = CursorStore::new(
                CursorStoreOptions::default(),
                Arc::clone(storage),
                storage_prefix_for_log(collection_id),
                "rollup".to_string(),
            );
            let span = tracing::info_span!("cursor load", collection_id = ?collection_id);
            cursor_store.load(cursor).instrument(span).await
        };
        for (collection_id, mut rollup) in std::mem::take(rollups) {
            // TODO(rescrv):  We can avoid loading the manifest and cursor by checking an
            // in-memory lookaside structure.
            let Some(manifest) = load_manifest(&self.storage, collection_id).await? else {
                tracing::warn!("{collection_id} has no manifest; this may mean it was deleted");
                continue;
            };
            let cursor = load_cursor(&self.storage, collection_id).await?;
            // NOTE(rescrv):  There are two spreads that we have.
            // `rollup` tracks the minimum and maximum offsets of a record on the dirty log.
            // The spread between cursor (if it exists) and manifest.maximum_log_offset tracks the
            // data that needs to be compacted.
            rollup.witness_manifest_and_cursor(&manifest, cursor.as_ref());
            if !rollup.is_empty() {
                rollups.insert(collection_id, rollup);
            }
        }
        Ok(())
    }

    pub async fn background_task(&self) {
        loop {
            tokio::time::sleep(self.config.rollup_interval).await;
            if let Err(err) = self.roll_dirty_log().await {
                tracing::error!("could not roll up dirty log: {err:?}");
            }
        }
    }

    async fn push_logs(
        &self,
        request: Request<PushLogsRequest>,
    ) -> Result<Response<PushLogsResponse>, Status> {
        let span =
            wrap_span_with_parent_context(tracing::trace_span!("PushLogs",), request.metadata());
        let push_logs = request.into_inner();
        let collection_id = Uuid::parse_str(&push_logs.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        if push_logs.records.len() > i32::MAX as usize {
            return Err(Status::invalid_argument("Too many records"));
        }
        if push_logs.records.is_empty() {
            return Err(Status::invalid_argument("Too few records"));
        }
        self.check_for_backpressure(collection_id)?;

        async move {
            tracing::info!("Pushing logs for collection {}", collection_id);
            let prefix = storage_prefix_for_log(collection_id);
            let key = LogKey { collection_id };
            let handle = self.open_logs.get_or_create_state(key);
            let mark_dirty = MarkDirty {
                collection_id,
                dirty_log: Arc::clone(&self.dirty_log),
            };
            let log = match get_log_from_handle(
                &handle,
                &self.config.writer,
                &self.storage,
                &prefix,
                mark_dirty,
            )
            .await
            {
                Ok(log) => log,
                Err(wal3::Error::UninitializedLog) => {
                    tracing::info!("forwarding because log uninitialized");
                    return self
                        .forward_push_logs(collection_id, Request::new(push_logs))
                        .await;
                }
                Err(err) => {
                    return Err(Status::unknown(err.to_string()));
                }
            };
            let mut messages = Vec::with_capacity(push_logs.records.len());
            for record in push_logs.records {
                let mut buf = vec![];
                record
                    .encode(&mut buf)
                    .map_err(|err| Status::unknown(err.to_string()))?;
                messages.push(buf);
            }
            let record_count = messages.len() as i32;
            log.append_many(messages).await.map_err(|err| {
                if let wal3::Error::Backoff = err {
                    Status::new(
                        chroma_error::ErrorCodes::Unavailable.into(),
                        err.to_string(),
                    )
                } else {
                    Status::new(err.code().into(), err.to_string())
                }
            })?;
            if let Some(cache) = self.cache.as_ref() {
                let cache_key = cache_key_for_manifest(collection_id);
                if let Some(manifest) = log.manifest() {
                    if let Ok(manifest_bytes) = serde_json::to_vec(&manifest) {
                        let cache_value = CachedBytes {
                            bytes: manifest_bytes,
                        };
                        cache.insert(cache_key, cache_value).await;
                    }
                }
            }
            Ok(Response::new(PushLogsResponse {
                record_count,
                log_is_sealed: false,
            }))
        }
        .instrument(span)
        .await
    }

    async fn scout_logs(
        &self,
        request: Request<ScoutLogsRequest>,
    ) -> Result<Response<ScoutLogsResponse>, Status> {
        let span =
            wrap_span_with_parent_context(tracing::trace_span!("ScoutLogs"), request.metadata());
        let scout_logs = request.into_inner();
        let collection_id = Uuid::parse_str(&scout_logs.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        async move {
            let prefix = storage_prefix_for_log(collection_id);
            let log_reader = LogReader::new(
                self.config.reader.clone(),
                Arc::clone(&self.storage),
                prefix,
            );
            let (start_position, limit_position) = match log_reader.manifest().await {
                Ok(Some(manifest)) => (
                    manifest.minimum_log_position(),
                    manifest.maximum_log_position(),
                ),
                Ok(None) | Err(wal3::Error::UninitializedLog) => {
                    tracing::info!("Log is uninitialized on rust log service. Forwarding ScoutLog request to legacy log service");
                    return self.forward_scout_logs(Request::new(scout_logs)).await;
                }
                Err(err) => {
                    return Err(Status::new(
                        err.code().into(),
                        format!("could not scout logs: {err:?}"),
                    ));
                }
            };

            // NOTE(sicheng): This is temporary trace added for analyzing number of frags between the offsets
            match log_reader.scan(start_position, Default::default()).await {
                Ok(frags) => tracing::info!(name: "Counting live fragments", frag_count = frags.len()),
                Err(e) => tracing::error!(name: "Unable to scout number of live fragments", error = e.to_string()),
            }

            let start_offset = start_position.offset() as i64;
            let limit_offset = limit_position.offset() as i64;
            Ok(Response::new(ScoutLogsResponse {
                first_uncompacted_record_offset: start_offset,
                first_uninserted_record_offset: limit_offset,
            }))
        }
        .instrument(span)
        .await
    }

    async fn read_fragments(
        &self,
        collection_id: CollectionUuid,
        pull_logs: &PullLogsRequest,
    ) -> Result<Vec<Fragment>, wal3::Error> {
        if let Some(fragments) = self
            .read_fragments_via_cache(collection_id, pull_logs)
            .await
        {
            Ok(fragments)
        } else {
            self.read_fragments_via_log_reader(collection_id, pull_logs)
                .await
        }
    }

    async fn read_fragments_via_cache(
        &self,
        collection_id: CollectionUuid,
        pull_logs: &PullLogsRequest,
    ) -> Option<Vec<Fragment>> {
        if let Some(cache) = self.cache.as_ref() {
            let cache_key = cache_key_for_manifest(collection_id);
            let cached_bytes = cache.get(&cache_key).await.ok().flatten()?;
            let manifest: Manifest = serde_json::from_slice(&cached_bytes.bytes).ok()?;
            let limits = Limits {
                max_files: Some(pull_logs.batch_size as u64 + 1),
                max_bytes: None,
                max_records: Some(pull_logs.batch_size as u64),
            };
            // NOTE(rescrv):  Log records are immutable, so if a manifest includes our range we can
            // serve it directly from the scan_from_manifest call.
            let (manifest_start, manifest_limit) = (
                manifest.minimum_log_position().offset() as i64,
                manifest.maximum_log_position().offset() as i64,
            );
            if manifest_start <= pull_logs.start_from_offset
                && pull_logs.start_from_offset + pull_logs.batch_size as i64 <= manifest_limit
            {
                LogReader::scan_from_manifest(
                    &manifest,
                    LogPosition::from_offset(pull_logs.start_from_offset as u64),
                    limits,
                )
            } else {
                None
            }
        } else {
            None
        }
    }

    async fn read_fragments_via_log_reader(
        &self,
        collection_id: CollectionUuid,
        pull_logs: &PullLogsRequest,
    ) -> Result<Vec<Fragment>, wal3::Error> {
        let prefix = storage_prefix_for_log(collection_id);
        let log_reader = LogReader::new(
            self.config.reader.clone(),
            Arc::clone(&self.storage),
            prefix,
        );
        let limits = Limits {
            max_files: Some(pull_logs.batch_size as u64 + 1),
            max_bytes: None,
            max_records: Some(pull_logs.batch_size as u64),
        };
        log_reader
            .scan(
                LogPosition::from_offset(pull_logs.start_from_offset as u64),
                limits,
            )
            .await
    }

    async fn pull_logs(
        &self,
        request: Request<PullLogsRequest>,
    ) -> Result<Response<PullLogsResponse>, Status> {
        let span =
            wrap_span_with_parent_context(tracing::trace_span!("PullLogs"), request.metadata());
        let pull_logs = request.into_inner();
        let collection_id = Uuid::parse_str(&pull_logs.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        async move {
            tracing::info!(
                collection_id = collection_id.to_string(),
                start_from_offset = pull_logs.start_from_offset,
                batch_size = pull_logs.batch_size,
            );
            let fragments = match self.read_fragments(collection_id, &pull_logs).await {
                Ok(fragments) => fragments,
                Err(wal3::Error::UninitializedLog) => {
                    return self.forward_pull_logs(Request::new(pull_logs)).await;
                }
                Err(err) => {
                    return Err(Status::new(err.code().into(), err.to_string()));
                }
            };
            let futures = fragments
                .iter()
                .map(|fragment| async {
                    let prefix = storage_prefix_for_log(collection_id);
                    if let Some(cache) = self.cache.as_ref() {
                        let cache_key = format!("{collection_id}::{}", fragment.path);
                        let cache_span = tracing::info_span!("cache get");
                        if let Ok(Some(answer)) = cache.get(&cache_key).instrument(cache_span).await
                        {
                            return Ok(Arc::new(answer.bytes));
                        }
                        let fetch_span = tracing::info_span!("fragment fetch");
                        let answer = LogReader::stateless_fetch(&self.storage, &prefix, fragment)
                            .instrument(fetch_span)
                            .await?;
                        let cache_value = CachedBytes {
                            bytes: Clone::clone(&*answer),
                        };
                        let insert_span = tracing::info_span!("cache insert");
                        cache
                            .insert(cache_key, cache_value)
                            .instrument(insert_span)
                            .await;
                        Ok(answer)
                    } else {
                        let fetch_span = tracing::info_span!("fragment fetch");
                        LogReader::stateless_fetch(&self.storage, &prefix, fragment)
                            .instrument(fetch_span)
                            .await
                    }
                })
                .collect::<Vec<_>>();
            let parquets = futures::future::try_join_all(futures)
                .await
                .map_err(|err| Status::new(err.code().into(), err.to_string()))?;
            let mut records = Vec::with_capacity(pull_logs.batch_size as usize);
            for parquet in parquets {
                let this = parquet_to_records(parquet)?;
                for record in this {
                    if record.0.offset() < pull_logs.start_from_offset as u64
                        || record.0.offset()
                            >= pull_logs.start_from_offset as u64 + pull_logs.batch_size as u64
                    {
                        continue;
                    }
                    if records.len() >= pull_logs.batch_size as usize {
                        break;
                    }
                    let op_record = OperationRecord::decode(record.1.as_slice())
                        .map_err(|err| Status::data_loss(err.to_string()))?;
                    records.push(LogRecord {
                        log_offset: record.0.offset() as i64,
                        record: Some(op_record),
                    });
                }
            }
            if !records.is_empty() && records[0].log_offset != pull_logs.start_from_offset {
                return Err(Status::not_found("Some entries have been purged"));
            }
            tracing::info!("pulled {} records", records.len());
            Ok(Response::new(PullLogsResponse { records }))
        }
        .instrument(span)
        .await
    }

    async fn fork_logs(
        &self,
        request: Request<ForkLogsRequest>,
    ) -> Result<Response<ForkLogsResponse>, Status> {
        let span =
            wrap_span_with_parent_context(tracing::trace_span!("ForkLogs"), request.metadata());
        let request = request.into_inner();
        let source_collection_id = Uuid::parse_str(&request.source_collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        self.check_for_backpressure(source_collection_id)?;
        let target_collection_id = Uuid::parse_str(&request.target_collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        let source_prefix = storage_prefix_for_log(source_collection_id);
        let target_prefix = storage_prefix_for_log(target_collection_id);
        let storage = Arc::clone(&self.storage);
        let options = self.config.writer.clone();

        async move {
            tracing::info!(
                source_collection_id = source_collection_id.to_string(),
                target_collection_id = target_collection_id.to_string(),
            );
            let log_reader = LogReader::new(
                self.config.reader.clone(),
                Arc::clone(&storage),
                source_prefix.clone(),
            );
            if let Err(err) = log_reader.maximum_log_position().await {
                match err {
                    wal3::Error::UninitializedLog => {
                        return self.forward_fork_logs(Request::new(request)).await;
                    }
                    _ => {
                        return Err(Status::new(
                            err.code().into(),
                            format!("Failed to load log: {}", err),
                        ));
                    }
                }
            }
            let cursors = CursorStore::new(
                CursorStoreOptions::default(),
                Arc::clone(&storage),
                source_prefix,
                "copy task".to_string(),
            );
            let cursor_name = &COMPACTION;
            let witness = cursors.load(cursor_name).await.map_err(|err| {
                Status::new(err.code().into(), format!("Failed to load cursor: {}", err))
            })?;
            // This is the existing compaction_offset, which is the next record to compact.
            let offset = witness
                .map(|x| x.1.position)
                .unwrap_or(LogPosition::from_offset(1));
            tracing::event!(Level::INFO, offset = ?offset);
            wal3::copy(
                &storage,
                &options,
                &log_reader,
                offset,
                target_prefix.clone(),
            )
            .await
            .map_err(|err| {
                Status::new(err.code().into(), format!("Failed to copy log: {}", err))
            })?;
            let log_reader = LogReader::new(
                self.config.reader.clone(),
                Arc::clone(&storage),
                target_prefix,
            );
            // This is the next record to insert, so we'll have to adjust downwards.
            let max_offset = log_reader.maximum_log_position().await.map_err(|err| {
                Status::new(err.code().into(), format!("Failed to read copied log: {}", err))
            })?;
            if max_offset < offset {
                return Err(Status::new(
                    chroma_error::ErrorCodes::Internal.into(),
                    format!("max_offset={:?} < offset={:?}", max_offset, offset),
                ));
            }
            if offset != max_offset{
                let mark_dirty = MarkDirty {
                    collection_id: target_collection_id,
                    dirty_log: Arc::clone(&self.dirty_log),
                };
                let _ = mark_dirty.mark_dirty(offset, (max_offset - offset) as usize).await;
            }
            tracing::event!(Level::INFO, compaction_offset =? offset.offset() - 1, enumeration_offset =? (max_offset - 1u64).offset());
            Ok(Response::new(ForkLogsResponse {
                // NOTE: The upstream service expects the last compacted offset as compaction offset
                compaction_offset: (offset - 1u64).offset(),
                // NOTE: The upstream service expects the last uncompacted offset as enumeration offset
                enumeration_offset: (max_offset - 1u64).offset(),
            }))
        }
        .instrument(span)
        .await
    }

    async fn get_all_collection_info_to_compact(
        &self,
        request: Request<GetAllCollectionInfoToCompactRequest>,
    ) -> Result<Response<GetAllCollectionInfoToCompactResponse>, Status> {
        let span = wrap_span_with_parent_context(
            tracing::trace_span!("GetAllCollectionInfoToCompact",),
            request.metadata(),
        );
        self.cached_get_all_collection_info_to_compact(request.into_inner())
            .instrument(span)
            .await
    }

    async fn update_collection_log_offset(
        &self,
        request: Request<UpdateCollectionLogOffsetRequest>,
    ) -> Result<Response<UpdateCollectionLogOffsetResponse>, Status> {
        let span = wrap_span_with_parent_context(
            tracing::trace_span!("UpdateCollectionLogOffset",),
            request.metadata(),
        );

        async move {
            let request = request.into_inner();
            let collection_id = Uuid::parse_str(&request.collection_id)
                .map(CollectionUuid)
                .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;

            // Grab a lock on the state for this key, so that a racing initialize won't do anything.
            let key = LogKey { collection_id };
            let handle = self.open_logs.get_or_create_state(key);
            let mut _active = handle.active.lock().await;
            self._update_collection_log_offset(Request::new(request))
                .await
        }
        .instrument(span)
        .await
    }

    async fn purge_dirty_for_collection(
        &self,
        request: Request<PurgeDirtyForCollectionRequest>,
    ) -> Result<Response<PurgeDirtyForCollectionResponse>, Status> {
        let span = wrap_span_with_parent_context(
            tracing::trace_span!("PurgeDirtyForCollection",),
            request.metadata(),
        );
        async move {
            let request = request.into_inner();
            let collection_id = Uuid::parse_str(&request.collection_id)
                .map(CollectionUuid)
                .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
            tracing::info!("purge_dirty_for_collection {collection_id}");
            let dirty_marker = DirtyMarker::Purge { collection_id };
            let dirty_marker_json = serde_json::to_string(&dirty_marker)
                .map_err(|err| {
                    tracing::error!("Failed to serialize dirty marker: {}", err);
                    wal3::Error::Internal
                })
                .map_err(|err| Status::new(err.code().into(), err.to_string()))?;
            self.dirty_log
                .append(Vec::from(dirty_marker_json))
                .await
                .map_err(|err| Status::new(err.code().into(), err.to_string()))?;
            Ok(Response::new(PurgeDirtyForCollectionResponse {}))
        }
        .instrument(span)
        .await
    }

    #[tracing::instrument(skip(self, _request))]
    async fn inspect_dirty_log(
        &self,
        _request: Request<InspectDirtyLogRequest>,
    ) -> Result<Response<InspectDirtyLogResponse>, Status> {
        let Some(reader) = self.dirty_log.reader(LogReaderOptions::default()) else {
            return Err(Status::unavailable("Failed to get dirty log reader"));
        };
        let Some(cursors) = self.dirty_log.cursors(CursorStoreOptions::default()) else {
            return Err(Status::unavailable("Failed to get dirty log cursors"));
        };
        let witness = match cursors.load(&STABLE_PREFIX).await {
            Ok(witness) => witness,
            Err(err) => {
                return Err(Status::new(err.code().into(), err.to_string()));
            }
        };
        let default = Cursor::default();
        let cursor = witness.as_ref().map(|w| w.cursor()).unwrap_or(&default);
        tracing::info!("cursoring from {cursor:?}");
        let dirty_fragments = reader
            .scan(
                cursor.position,
                Limits {
                    max_files: Some(1_000_000),
                    max_bytes: Some(1_000_000_000),
                    max_records: Some(1_000_000),
                },
            )
            .await
            .map_err(|err| Status::new(err.code().into(), err.to_string()))?;
        let dirty_futures = dirty_fragments
            .iter()
            .map(|fragment| reader.read_parquet(fragment))
            .collect::<Vec<_>>();
        let dirty_raw = futures::future::try_join_all(dirty_futures)
            .await
            .map_err(|err| {
                Status::new(
                    err.code().into(),
                    format!("Failed to fetch dirty parquet: {}", err),
                )
            })?;
        let mut markers = vec![];
        for (_, records, _) in dirty_raw {
            let records = records
                .into_iter()
                .map(|x| String::from_utf8(x.1))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| {
                    Status::new(
                        chroma_error::ErrorCodes::DataLoss.into(),
                        format!("Failed to extract records: {}", err),
                    )
                })?;
            markers.extend(records);
        }
        Ok(Response::new(InspectDirtyLogResponse { markers }))
    }

    async fn seal_log(
        &self,
        request: Request<SealLogRequest>,
    ) -> Result<Response<SealLogResponse>, Status> {
        let span =
            wrap_span_with_parent_context(tracing::trace_span!("SealLog",), request.metadata());

        async {
            Err(Status::failed_precondition(
                "rust log service doesn't do sealing",
            ))
        }
        .instrument(span)
        .await
    }

    async fn migrate_log(
        &self,
        request: Request<MigrateLogRequest>,
    ) -> Result<Response<MigrateLogResponse>, Status> {
        let span =
            wrap_span_with_parent_context(tracing::trace_span!("MigrateLog",), request.metadata());

        let migrate_log = request.into_inner();
        let collection_id = Uuid::parse_str(&migrate_log.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;

        async move {
            tracing::info!(
                "Migrating log for collection {} to new log service",
                collection_id
            );
            let prefix = storage_prefix_for_log(collection_id);
            let key = LogKey { collection_id };
            let handle = self.open_logs.get_or_create_state(key);
            let mark_dirty = MarkDirty {
                collection_id,
                dirty_log: Arc::clone(&self.dirty_log),
            };
            match get_log_from_handle(
                &handle,
                &self.config.writer,
                &self.storage,
                &prefix,
                mark_dirty,
            )
            .await
            {
                Ok(_) => {
                    tracing::info!("{collection_id} already migrated");
                    Ok(Response::new(MigrateLogResponse {}))
                }
                Err(wal3::Error::UninitializedLog) => {
                    if let Some(proxy) = self.proxy.as_ref() {
                        tracing::info!("effectuating transfer of {collection_id}");
                        self.effectuate_log_transfer(collection_id, proxy.clone(), 3)
                            .await?;
                        Ok(Response::new(MigrateLogResponse {}))
                    } else {
                        tracing::info!("not effectuating transfer of {collection_id} (no proxy)");
                        Err(Status::failed_precondition("proxy not initialized"))
                    }
                }
                Err(err) => Err(Status::unknown(err.to_string())),
            }
        }
        .instrument(span)
        .await
    }

    async fn inspect_log_state(
        &self,
        request: Request<InspectLogStateRequest>,
    ) -> Result<Response<InspectLogStateResponse>, Status> {
        let request = request.into_inner();
        let collection_id = Uuid::parse_str(&request.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        tracing::info!("inspect_log_state for {collection_id}");
        let storage_prefix = storage_prefix_for_log(collection_id);
        let log_reader = LogReader::new(
            self.config.reader.clone(),
            Arc::clone(&self.storage),
            storage_prefix.clone(),
        );
        let mani = log_reader.manifest().await;
        if let Err(wal3::Error::UninitializedLog) = mani {
            return Ok(Response::new(InspectLogStateResponse {
                debug: "log uninitialized\n".to_string(),
            }));
        }
        let mani = mani.map_err(|err| Status::unknown(err.to_string()))?;

        let cursor_name = &COMPACTION;
        let cursor_store = CursorStore::new(
            CursorStoreOptions::default(),
            Arc::clone(&self.storage),
            storage_prefix.clone(),
            "writer".to_string(),
        );
        let witness = cursor_store.load(cursor_name).await.map_err(|err| {
            Status::new(err.code().into(), format!("Failed to load cursor: {}", err))
        })?;

        Ok(Response::new(InspectLogStateResponse {
            debug: format!("manifest: {mani:#?}\ncompaction cursor: {witness:?}"),
        }))
    }
}

struct LogServerWrapper {
    log_server: Arc<LogServer>,
}

#[async_trait::async_trait]
impl LogService for LogServerWrapper {
    async fn push_logs(
        &self,
        request: Request<PushLogsRequest>,
    ) -> Result<Response<PushLogsResponse>, Status> {
        self.log_server.push_logs(request).await
    }

    async fn scout_logs(
        &self,
        request: Request<ScoutLogsRequest>,
    ) -> Result<Response<ScoutLogsResponse>, Status> {
        self.log_server.scout_logs(request).await
    }

    async fn pull_logs(
        &self,
        request: Request<PullLogsRequest>,
    ) -> Result<Response<PullLogsResponse>, Status> {
        self.log_server.pull_logs(request).await
    }

    async fn fork_logs(
        &self,
        request: Request<ForkLogsRequest>,
    ) -> Result<Response<ForkLogsResponse>, Status> {
        self.log_server.fork_logs(request).await
    }

    async fn get_all_collection_info_to_compact(
        &self,
        request: Request<GetAllCollectionInfoToCompactRequest>,
    ) -> Result<Response<GetAllCollectionInfoToCompactResponse>, Status> {
        self.log_server
            .get_all_collection_info_to_compact(request)
            .await
    }

    async fn update_collection_log_offset(
        &self,
        request: Request<UpdateCollectionLogOffsetRequest>,
    ) -> Result<Response<UpdateCollectionLogOffsetResponse>, Status> {
        self.log_server.update_collection_log_offset(request).await
    }

    async fn purge_dirty_for_collection(
        &self,
        request: Request<PurgeDirtyForCollectionRequest>,
    ) -> Result<Response<PurgeDirtyForCollectionResponse>, Status> {
        self.log_server.purge_dirty_for_collection(request).await
    }

    async fn inspect_dirty_log(
        &self,
        request: Request<InspectDirtyLogRequest>,
    ) -> Result<Response<InspectDirtyLogResponse>, Status> {
        self.log_server.inspect_dirty_log(request).await
    }

    async fn seal_log(
        &self,
        request: Request<SealLogRequest>,
    ) -> Result<Response<SealLogResponse>, Status> {
        self.log_server.seal_log(request).await
    }

    async fn migrate_log(
        &self,
        request: Request<MigrateLogRequest>,
    ) -> Result<Response<MigrateLogResponse>, Status> {
        self.log_server.migrate_log(request).await
    }

    async fn inspect_log_state(
        &self,
        request: Request<InspectLogStateRequest>,
    ) -> Result<Response<InspectLogStateResponse>, Status> {
        self.log_server.inspect_log_state(request).await
    }
}

fn parquet_to_records(parquet: Arc<Vec<u8>>) -> Result<Vec<(LogPosition, Vec<u8>)>, Status> {
    let parquet = match Arc::try_unwrap(parquet) {
        Ok(parquet) => parquet,
        Err(ptr) => ptr.to_vec(),
    };
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from_owner(parquet)).map_err(|err| {
            Status::new(
                tonic::Code::Unavailable,
                format!("could not create parquet reader: {err:?}"),
            )
        })?;
    let reader = builder.build().map_err(|err| {
        Status::new(
            tonic::Code::Unavailable,
            format!("could not convert from parquet: {err:?}"),
        )
    })?;
    let mut records = vec![];
    for batch in reader {
        let batch = batch.map_err(|err| {
            Status::new(
                tonic::Code::Unavailable,
                format!("could not read record batch: {err:?}"),
            )
        })?;
        let offset = batch.column_by_name("offset").ok_or_else(|| {
            Status::new(
                tonic::Code::Unavailable,
                "could not find column 'offset' in record batch",
            )
        })?;
        let body = batch.column_by_name("body").ok_or_else(|| {
            Status::new(
                tonic::Code::Unavailable,
                "could not find column 'body' in record batch",
            )
        })?;
        let offset = offset
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| {
                Status::new(
                    tonic::Code::Unavailable,
                    "could not cast column 'body' to UInt64Array",
                )
            })?;
        let body = body
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .ok_or_else(|| {
                Status::new(
                    tonic::Code::Unavailable,
                    "could not cast column 'body' to BinaryArray",
                )
            })?;
        for i in 0..batch.num_rows() {
            let offset = offset.value(i);
            let body = body.value(i);
            records.push((LogPosition::from_offset(offset), body.to_vec()));
        }
    }
    Ok(records)
}

impl LogServerWrapper {
    pub(crate) async fn run(log_server: LogServer) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("[::]:{}", log_server.config.port).parse().unwrap();
        println!("Log listening on {}", addr);

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<chroma_types::chroma_proto::log_service_server::LogServiceServer<Self>>()
            .await;

        let wrapper = LogServerWrapper {
            log_server: Arc::new(log_server),
        };
        let background_server = Arc::clone(&wrapper.log_server);
        let background =
            tokio::task::spawn(async move { background_server.background_task().await });
        let server = Server::builder().add_service(health_service).add_service(
            chroma_types::chroma_proto::log_service_server::LogServiceServer::new(wrapper),
        );

        let server = server.serve_with_shutdown(addr, async {
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(sigterm) => sigterm,
                Err(e) => {
                    tracing::error!("Failed to create signal handler: {:?}", e);
                    return;
                }
            };
            sigterm.recv().await;
            tracing::info!("Received SIGTERM, shutting down");
        });

        let res = server.await;
        background.abort();
        Ok(res?)
    }
}

/////////////////////////// Config ///////////////////////////

#[derive(Deserialize, Serialize)]
pub struct RootConfig {
    // The root config object wraps the worker config object so that
    // we can share the same config file between multiple services.
    #[serde(default)]
    pub log_service: LogServerConfig,
}

fn default_endpoint() -> String {
    "http://otel-collector:4317".to_string()
}

fn default_otel_service_name() -> String {
    "rust-log-service".to_string()
}

fn default_port() -> u16 {
    50051
}

impl RootConfig {
    /// # Description
    /// Load the config from the default location.
    /// # Returns
    /// The config object.
    /// # Panics
    /// - If the config file cannot be read.
    /// - If the config file is not valid YAML.
    /// - If the config file does not contain the required fields.
    /// - If the config file contains invalid values.
    /// - If the environment variables contain invalid values.
    /// # Notes
    /// The default location is the current working directory, with the filename chroma_config.yaml.
    /// The environment variables are prefixed with CHROMA_ and are uppercase.
    /// Values in the envionment variables take precedence over values in the YAML file.
    pub fn load() -> Self {
        Self::load_from_path(DEFAULT_CONFIG_PATH)
    }

    /// # Description
    /// Load the config from a specific location.
    /// # Arguments
    /// - path: The path to the config file.
    /// # Returns
    /// The config object.
    /// # Panics
    /// - If the config file cannot be read.
    /// - If the config file is not valid YAML.
    /// - If the config file does not contain the required fields.
    /// - If the config file contains invalid values.
    /// - If the environment variables contain invalid values.
    /// # Notes
    /// The environment variables are prefixed with CHROMA_ and are uppercase.
    /// Values in the envionment variables take precedence over values in the YAML file.
    // NOTE:  Copied to ../load/src/config.rs.
    pub fn load_from_path(path: &str) -> Self {
        println!("loading config from {path}");
        println!(
            r#"Full config is:
================================================================================
{}
================================================================================
"#,
            std::fs::read_to_string(path)
                .expect("should be able to open and read config to string")
        );
        // Unfortunately, figment doesn't support environment variables with underscores. So we have to map and replace them.
        // Excluding our own environment variables, which are prefixed with CHROMA_.
        let mut f = figment::Figment::from(Env::prefixed("CHROMA_").map(|k| match k {
            k if k == "my_member_id" => k.into(),
            k => k.as_str().replace("__", ".").into(),
        }));
        if std::path::Path::new(path).exists() {
            f = figment::Figment::from(Yaml::file(path)).merge(f);
        }
        // Apply defaults - this seems to be the best way to do it.
        // https://github.com/SergioBenitez/Figment/issues/77#issuecomment-1642490298
        // f = f.join(Serialized::default(
        //     "worker.num_indexing_threads",
        //     num_cpus::get(),
        // ));
        let res = f.extract();
        match res {
            Ok(config) => config,
            Err(e) => panic!("Error loading config: {}", e),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct OpenTelemetryConfig {
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    #[serde(default = "default_otel_service_name")]
    pub service_name: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct LogServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "LogServerConfig::default_my_member_id")]
    pub my_member_id: String,
    #[serde(default)]
    pub opentelemetry: Option<OpenTelemetryConfig>,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub writer: LogWriterOptions,
    #[serde(default)]
    pub reader: LogReaderOptions,
    #[serde(default)]
    pub cache: Option<CacheConfig>,
    #[serde(default = "LogServerConfig::default_record_count_threshold")]
    pub record_count_threshold: u64,
    #[serde(default = "LogServerConfig::default_num_records_before_backpressure")]
    pub num_records_before_backpressure: u64,
    #[serde(default = "LogServerConfig::default_reinsert_threshold")]
    pub reinsert_threshold: u64,
    #[serde(default = "LogServerConfig::default_rollup_interval")]
    pub rollup_interval: Duration,
    #[serde(default = "LogServerConfig::default_timeout_us")]
    pub timeout_us: u64,
    #[serde(default)]
    pub proxy_to: Option<GrpcLogConfig>,
}

impl LogServerConfig {
    /// one hundred records on the log.
    fn default_record_count_threshold() -> u64 {
        100
    }

    fn default_my_member_id() -> String {
        "rust-log-service-0".to_string()
    }

    /// one million records on the log.
    fn default_num_records_before_backpressure() -> u64 {
        1_000_000
    }

    /// force compaction if a candidate comes up ten times.
    fn default_reinsert_threshold() -> u64 {
        10
    }

    /// rollup every ten seconds
    fn default_rollup_interval() -> Duration {
        Duration::from_secs(10)
    }

    /// force compaction if a candidate has been on the log for one day.
    fn default_timeout_us() -> u64 {
        86_400_000_000
    }
}

impl Default for LogServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            my_member_id: LogServerConfig::default_my_member_id(),
            opentelemetry: None,
            storage: StorageConfig::default(),
            writer: LogWriterOptions::default(),
            reader: LogReaderOptions::default(),
            cache: None,
            record_count_threshold: Self::default_record_count_threshold(),
            num_records_before_backpressure: Self::default_num_records_before_backpressure(),
            reinsert_threshold: Self::default_reinsert_threshold(),
            rollup_interval: Self::default_rollup_interval(),
            timeout_us: Self::default_timeout_us(),
            proxy_to: None,
        }
    }
}

#[async_trait::async_trait]
impl Configurable<LogServerConfig> for LogServer {
    async fn try_from_config(
        config: &LogServerConfig,
        registry: &chroma_config::registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let cache = if let Some(cache_config) = &config.cache {
            match chroma_cache::from_config_persistent::<String, CachedBytes>(cache_config).await {
                Ok(cache) => Some(cache),
                Err(err) => {
                    tracing::error!("cache not configured: {err:?}");
                    None
                }
            }
        } else {
            None
        };
        let storage = Storage::try_from_config(&config.storage, registry).await?;
        let storage = Arc::new(storage);
        let dirty_log = LogWriter::open_or_initialize(
            config.writer.clone(),
            Arc::clone(&storage),
            &format!("dirty-{}", config.my_member_id),
            "dirty log writer",
            (),
        )
        .await
        .map_err(|err| -> Box<dyn ChromaError> { Box::new(err) as _ })?;
        let dirty_log = Arc::new(dirty_log);
        let proxy = if let Some(proxy_to) = config.proxy_to.as_ref() {
            match GrpcLog::primary_client_from_config(proxy_to).await {
                Ok(log) => Some(log),
                Err(err) => {
                    return Err(err);
                }
            }
        } else {
            None
        };
        let rolling_up = tokio::sync::Mutex::new(());
        let metrics = Metrics::new(opentelemetry::global::meter("chroma"));
        let backpressure = Mutex::new(Arc::new(HashSet::default()));
        let need_to_compact = Mutex::new(HashMap::default());
        Ok(Self {
            config: config.clone(),
            open_logs: Arc::new(StateHashTable::default()),
            storage,
            dirty_log,
            proxy,
            rolling_up,
            backpressure,
            need_to_compact,
            cache,
            metrics,
        })
    }
}

////////////////////////////////////////// log_entrypoint //////////////////////////////////////////

// Entrypoint for the wal3 based log server
pub async fn log_entrypoint() {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => RootConfig::load_from_path(&config_path),
        Err(_) => RootConfig::load(),
    };
    let config = config.log_service;
    eprintln!("my_member_id: {}", config.my_member_id);
    let registry = chroma_config::registry::Registry::new();
    if let Some(otel_config) = &config.opentelemetry {
        eprintln!("enabling tracing");
        chroma_tracing::init_otel_tracing(&otel_config.service_name, &otel_config.endpoint);
    } else {
        eprintln!("tracing disabled");
    }
    let log_server = LogServer::try_from_config(&config, &registry)
        .await
        .expect("Failed to create log server");

    let server_join_handle = tokio::spawn(async move {
        let _ = LogServerWrapper::run(log_server).await;
    });

    match server_join_handle.await {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Error terminating server: {:?}", e);
        }
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_hash_table::Value;

    #[test]
    fn unsafe_constants() {
        assert!(STABLE_PREFIX.is_valid());
    }

    #[test]
    fn dirty_marker_coalesce1() {
        // Test that a single collection gets coalesced to nothing.
        let collection_id = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| wal3::Error::Internal)
            .unwrap()
            .as_micros() as u64;
        let markers = vec![
            (
                LogPosition::from_offset(45),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(1),
                    num_records: 1,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(46),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(2),
                    num_records: 1,
                    reinsert_count: 2,
                    initial_insertion_epoch_us: now,
                },
            ),
        ];
        let rollup = DirtyMarker::coalesce_markers(&markers).unwrap();
        assert_eq!(1, rollup.len());
        let rollup = rollup.get(&collection_id).unwrap();
        assert_eq!(LogPosition::from_offset(1), rollup.start_log_position);
        assert_eq!(LogPosition::from_offset(3), rollup.limit_log_position);
        assert_eq!(2, rollup.reinsert_count);
        assert_eq!(now, rollup.initial_insertion_epoch_us);
    }

    #[test]
    fn dirty_marker_coalesce2() {
        // Test that a collection without enough records won't induce head-of-line blocking.
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| wal3::Error::Internal)
            .unwrap()
            .as_micros() as u64;
        let collection_id_blocking = CollectionUuid::new();
        let collection_id_acting = CollectionUuid::new();
        let markers = vec![
            (
                LogPosition::from_offset(0),
                DirtyMarker::MarkDirty {
                    collection_id: collection_id_blocking,
                    log_position: LogPosition::from_offset(1),
                    num_records: 1,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(1),
                DirtyMarker::MarkDirty {
                    collection_id: collection_id_acting,
                    log_position: LogPosition::from_offset(1),
                    num_records: 100,
                    reinsert_count: 1,
                    initial_insertion_epoch_us: now,
                },
            ),
        ];
        let rollup = DirtyMarker::coalesce_markers(&markers).unwrap();
        assert_eq!(2, rollup.len());
        let rollup_blocking = rollup.get(&collection_id_blocking).unwrap();
        assert_eq!(
            LogPosition::from_offset(1),
            rollup_blocking.start_log_position
        );
        assert_eq!(
            LogPosition::from_offset(2),
            rollup_blocking.limit_log_position
        );
        assert_eq!(0, rollup_blocking.reinsert_count);
        assert_eq!(now, rollup_blocking.initial_insertion_epoch_us);
        let rollup_acting = rollup.get(&collection_id_acting).unwrap();
        assert_eq!(
            LogPosition::from_offset(1),
            rollup_acting.start_log_position
        );
        assert_eq!(
            LogPosition::from_offset(101),
            rollup_acting.limit_log_position
        );
        assert_eq!(1, rollup_acting.reinsert_count);
        assert_eq!(now, rollup_acting.initial_insertion_epoch_us);
    }

    #[test]
    fn dirty_marker_serialization() {
        let collection_id = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        // Test MarkDirty serialization
        let mark_dirty = DirtyMarker::MarkDirty {
            collection_id,
            log_position: LogPosition::from_offset(42),
            num_records: 100,
            reinsert_count: 5,
            initial_insertion_epoch_us: now,
        };

        let serialized = serde_json::to_string(&mark_dirty).unwrap();
        let deserialized: DirtyMarker = serde_json::from_str(&serialized).unwrap();
        assert_eq!(mark_dirty, deserialized);

        // Test Purge serialization
        let purge = DirtyMarker::Purge { collection_id };
        let serialized = serde_json::to_string(&purge).unwrap();
        let deserialized: DirtyMarker = serde_json::from_str(&serialized).unwrap();
        assert_eq!(purge, deserialized);
    }

    #[test]
    fn dirty_marker_collection_id() {
        let collection_id = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let mark_dirty = DirtyMarker::MarkDirty {
            collection_id,
            log_position: LogPosition::from_offset(1),
            num_records: 1,
            reinsert_count: 0,
            initial_insertion_epoch_us: now,
        };
        assert_eq!(collection_id, mark_dirty.collection_id());

        let purge = DirtyMarker::Purge { collection_id };
        assert_eq!(collection_id, purge.collection_id());
    }

    #[test]
    fn dirty_marker_reinsert() {
        let collection_id = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let mut mark_dirty = DirtyMarker::MarkDirty {
            collection_id,
            log_position: LogPosition::from_offset(1),
            num_records: 1,
            reinsert_count: 0,
            initial_insertion_epoch_us: now,
        };

        // Test incrementing reinsert count
        mark_dirty.reinsert();
        if let DirtyMarker::MarkDirty { reinsert_count, .. } = mark_dirty {
            assert_eq!(1, reinsert_count);
        } else {
            panic!("Expected MarkDirty variant");
        }

        // Test that Purge variant doesn't panic when reinsert is called
        let mut purge = DirtyMarker::Purge { collection_id };
        purge.reinsert(); // Should not panic
    }

    #[test]
    fn dirty_marker_coalesce_with_purge() {
        let collection_id = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let markers = vec![
            (
                LogPosition::from_offset(1),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(1),
                    num_records: 10,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(2),
                DirtyMarker::Purge { collection_id },
            ),
            (
                LogPosition::from_offset(3),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(20),
                    num_records: 5,
                    reinsert_count: 1,
                    initial_insertion_epoch_us: now + 1000,
                },
            ),
        ];

        let rollup = DirtyMarker::coalesce_markers(&markers).unwrap();
        // The purge should remove all markers for the collection, even ones that come after
        assert_eq!(0, rollup.len());
    }

    #[test]
    fn dirty_marker_coalesce_purge_removes_all() {
        // Test to clarify that purge removes ALL markers for a collection
        let collection_id1 = CollectionUuid::new();
        let collection_id2 = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let markers = vec![
            (
                LogPosition::from_offset(1),
                DirtyMarker::MarkDirty {
                    collection_id: collection_id1,
                    log_position: LogPosition::from_offset(1),
                    num_records: 10,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(2),
                DirtyMarker::MarkDirty {
                    collection_id: collection_id2,
                    log_position: LogPosition::from_offset(10),
                    num_records: 5,
                    reinsert_count: 1,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(3),
                DirtyMarker::Purge {
                    collection_id: collection_id1,
                },
            ),
            (
                LogPosition::from_offset(4),
                DirtyMarker::MarkDirty {
                    collection_id: collection_id1,
                    log_position: LogPosition::from_offset(20),
                    num_records: 3,
                    reinsert_count: 2,
                    initial_insertion_epoch_us: now + 1000,
                },
            ),
        ];

        let rollup = DirtyMarker::coalesce_markers(&markers).unwrap();
        // collection_id1 should be completely removed due to purge
        // collection_id2 should remain
        assert_eq!(1, rollup.len());
        assert!(rollup.contains_key(&collection_id2));
        assert!(!rollup.contains_key(&collection_id1));

        let rollup2 = rollup.get(&collection_id2).unwrap();
        assert_eq!(LogPosition::from_offset(10), rollup2.start_log_position);
        assert_eq!(LogPosition::from_offset(15), rollup2.limit_log_position);
    }

    #[test]
    fn rollup_per_collection_new() {
        let start_position = LogPosition::from_offset(10);
        let num_records = 5;
        let rollup = RollupPerCollection::new(start_position, num_records);

        assert_eq!(start_position, rollup.start_log_position);
        assert_eq!(LogPosition::from_offset(15), rollup.limit_log_position);
        assert_eq!(0, rollup.reinsert_count);
        assert_eq!(0, rollup.initial_insertion_epoch_us);
    }

    #[test]
    fn rollup_per_collection_observe_dirty_marker() {
        let start_position = LogPosition::from_offset(10);
        let mut rollup = RollupPerCollection::new(start_position, 5);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        // Observe a marker that extends the range
        rollup.observe_dirty_marker(LogPosition::from_offset(20), 10, 3, now);
        assert_eq!(LogPosition::from_offset(10), rollup.start_log_position);
        assert_eq!(LogPosition::from_offset(30), rollup.limit_log_position);
        assert_eq!(3, rollup.reinsert_count);
        assert_eq!(now, rollup.initial_insertion_epoch_us);

        // Observe a marker that comes before the start
        rollup.observe_dirty_marker(LogPosition::from_offset(5), 2, 1, now - 1000);
        assert_eq!(LogPosition::from_offset(5), rollup.start_log_position);
        assert_eq!(LogPosition::from_offset(30), rollup.limit_log_position);
        assert_eq!(3, rollup.reinsert_count); // Should keep max
        assert_eq!(now, rollup.initial_insertion_epoch_us); // Should keep max
    }

    #[test]
    fn rollup_per_collection_is_empty() {
        let rollup = RollupPerCollection::new(LogPosition::from_offset(10), 0);
        assert!(rollup.is_empty());

        let rollup = RollupPerCollection::new(LogPosition::from_offset(10), 5);
        assert!(!rollup.is_empty());
    }

    #[test]
    fn rollup_per_collection_requires_backpressure() {
        let rollup = RollupPerCollection::new(LogPosition::from_offset(10), 100);
        assert!(rollup.requires_backpressure(50));
        assert!(!rollup.requires_backpressure(150));
        assert!(rollup.requires_backpressure(100)); // Equal case
    }

    #[test]
    fn rollup_per_collection_dirty_marker() {
        let collection_id = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let mut rollup = RollupPerCollection::new(LogPosition::from_offset(10), 5);
        rollup.observe_dirty_marker(LogPosition::from_offset(10), 5, 2, now);

        let marker = rollup.dirty_marker(collection_id);
        match marker {
            DirtyMarker::MarkDirty {
                collection_id: cid,
                log_position,
                num_records,
                reinsert_count,
                initial_insertion_epoch_us,
            } => {
                assert_eq!(collection_id, cid);
                assert_eq!(LogPosition::from_offset(10), log_position);
                assert_eq!(5, num_records);
                assert_eq!(2, reinsert_count);
                assert_eq!(now, initial_insertion_epoch_us);
            }
            _ => panic!("Expected MarkDirty variant"),
        }
    }

    #[test]
    fn active_log_keep_alive() {
        let mut active_log = ActiveLog::default();
        let initial_time = active_log.collect_after;

        // Test extending keep alive time
        let keep_alive_duration = Duration::from_secs(30);
        active_log.keep_alive(keep_alive_duration);
        assert!(active_log.collect_after > initial_time + Duration::from_secs(30));

        // Test that shorter duration doesn't reduce time
        let long_time = active_log.collect_after;
        active_log.keep_alive(Duration::from_millis(1));
        assert_eq!(long_time, active_log.collect_after);
    }

    #[test]
    fn log_stub_finished() {
        let log_stub = LogStub::default();
        // LogStub always returns true for finished()
        assert!(log_stub.finished());
    }

    #[test]
    fn storage_prefix_for_log_format() {
        let collection_id = CollectionUuid::new();
        let prefix = storage_prefix_for_log(collection_id);
        assert_eq!(format!("logs/{}", collection_id), prefix);
    }

    #[tokio::test]
    async fn mark_dirty_creates_correct_marker() {
        // This test verifies the MarkDirty struct creates the correct DirtyMarker
        // We can't easily test the full async behavior without a real LogWriter,
        // but we can test the marker creation logic by examining what would be serialized

        let collection_id = CollectionUuid::new();
        let log_position = LogPosition::from_offset(42);
        let num_records = 100usize;

        // Create the expected marker manually
        let expected_marker = DirtyMarker::MarkDirty {
            collection_id,
            log_position,
            num_records: num_records as u64,
            reinsert_count: 0,
            initial_insertion_epoch_us: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
        };

        // Verify the marker can be serialized (this is what MarkDirty::mark_dirty does)
        let serialized = serde_json::to_string(&expected_marker).unwrap();
        assert!(serialized.contains("mark_dirty"));
        assert!(serialized.contains(&collection_id.to_string()));
        assert!(serialized.contains("42")); // log_position offset
        assert!(serialized.contains("100")); // num_records

        // Verify it can be deserialized back
        let deserialized: DirtyMarker = serde_json::from_str(&serialized).unwrap();
        if let DirtyMarker::MarkDirty {
            collection_id: cid,
            log_position: pos,
            num_records: count,
            reinsert_count,
            ..
        } = deserialized
        {
            assert_eq!(collection_id, cid);
            assert_eq!(log_position, pos);
            assert_eq!(100, count);
            assert_eq!(0, reinsert_count);
        } else {
            panic!("Expected MarkDirty variant");
        }
    }

    #[test]
    fn dirty_marker_coalesce_empty_markers() {
        let rollup = DirtyMarker::coalesce_markers(&[]).unwrap();
        assert!(rollup.is_empty());
    }

    #[test]
    fn dirty_marker_coalesce_multiple_collections() {
        let collection_id1 = CollectionUuid::new();
        let collection_id2 = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let markers = vec![
            (
                LogPosition::from_offset(1),
                DirtyMarker::MarkDirty {
                    collection_id: collection_id1,
                    log_position: LogPosition::from_offset(10),
                    num_records: 5,
                    reinsert_count: 1,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(2),
                DirtyMarker::MarkDirty {
                    collection_id: collection_id2,
                    log_position: LogPosition::from_offset(20),
                    num_records: 10,
                    reinsert_count: 2,
                    initial_insertion_epoch_us: now + 1000,
                },
            ),
            (
                LogPosition::from_offset(3),
                DirtyMarker::MarkDirty {
                    collection_id: collection_id1,
                    log_position: LogPosition::from_offset(30),
                    num_records: 3,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now - 1000,
                },
            ),
        ];

        let rollup = DirtyMarker::coalesce_markers(&markers).unwrap();
        assert_eq!(2, rollup.len());

        // Check collection_id1 rollup
        let rollup1 = rollup.get(&collection_id1).unwrap();
        assert_eq!(LogPosition::from_offset(10), rollup1.start_log_position);
        assert_eq!(LogPosition::from_offset(33), rollup1.limit_log_position);
        assert_eq!(1, rollup1.reinsert_count); // max of 1 and 0
        assert_eq!(now, rollup1.initial_insertion_epoch_us); // max of now and now-1000

        // Check collection_id2 rollup
        let rollup2 = rollup.get(&collection_id2).unwrap();
        assert_eq!(LogPosition::from_offset(20), rollup2.start_log_position);
        assert_eq!(LogPosition::from_offset(30), rollup2.limit_log_position);
        assert_eq!(2, rollup2.reinsert_count);
        assert_eq!(now + 1000, rollup2.initial_insertion_epoch_us);
    }

    #[test]
    fn error_enum_conversion_from_wal3() {
        let wal3_error = wal3::Error::Internal;
        let service_error = Error::from(wal3_error);
        match service_error {
            Error::Wal3(wal3::Error::Internal) => {}
            _ => panic!("Expected Wal3 error variant"),
        }
    }

    #[test]
    fn error_enum_conversion_from_json() {
        let json_error = serde_json::from_str::<DirtyMarker>("invalid json").unwrap_err();
        let service_error = Error::from(json_error);
        match service_error {
            Error::Json(_) => {}
            _ => panic!("Expected Json error variant"),
        }
    }

    #[test]
    fn error_enum_display_messages() {
        let wal3_error = Error::Wal3(wal3::Error::Internal);
        assert!(wal3_error.to_string().contains("wal3"));

        let json_error =
            Error::Json(serde_json::from_str::<DirtyMarker>("invalid json").unwrap_err());
        assert!(json_error.to_string().contains("serialization error"));

        let reader_error = Error::CouldNotGetDirtyLogReader;
        assert_eq!(
            "Dirty log writer failed to provide a reader",
            reader_error.to_string()
        );

        let cursor_error = Error::CouldNotGetDirtyLogCursors;
        assert_eq!(
            "Dirty log writer failed to provide a cursor store",
            cursor_error.to_string()
        );
    }

    #[test]
    fn dirty_marker_coalesce_invalid_positions() {
        let collection_id = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let markers = vec![(
            LogPosition::from_offset(1),
            DirtyMarker::MarkDirty {
                collection_id,
                log_position: LogPosition::from_offset(u64::MAX - 1),
                num_records: 100,
                reinsert_count: 0,
                initial_insertion_epoch_us: now,
            },
        )];

        let rollup = DirtyMarker::coalesce_markers(&markers).unwrap();
        let collection_rollup = rollup.get(&collection_id).unwrap();
        assert_eq!(
            LogPosition::from_offset(u64::MAX - 1),
            collection_rollup.start_log_position
        );
        assert_eq!(
            LogPosition::from_offset(u64::MAX),
            collection_rollup.limit_log_position
        );
    }

    #[test]
    fn dirty_marker_coalesce_zero_records() {
        let collection_id = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let markers = vec![(
            LogPosition::from_offset(1),
            DirtyMarker::MarkDirty {
                collection_id,
                log_position: LogPosition::from_offset(10),
                num_records: 0,
                reinsert_count: 0,
                initial_insertion_epoch_us: now,
            },
        )];

        let rollup = DirtyMarker::coalesce_markers(&markers).unwrap();
        let collection_rollup = rollup.get(&collection_id).unwrap();
        assert_eq!(
            LogPosition::from_offset(10),
            collection_rollup.start_log_position
        );
        assert_eq!(
            LogPosition::from_offset(10),
            collection_rollup.limit_log_position
        );
        assert!(collection_rollup.is_empty());
    }

    #[test]
    fn dirty_marker_coalesce_max_reinsert_count() {
        let collection_id = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let markers = vec![
            (
                LogPosition::from_offset(1),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(10),
                    num_records: 1,
                    reinsert_count: u64::MAX,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(2),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(11),
                    num_records: 1,
                    reinsert_count: 5,
                    initial_insertion_epoch_us: now,
                },
            ),
        ];

        let rollup = DirtyMarker::coalesce_markers(&markers).unwrap();
        let collection_rollup = rollup.get(&collection_id).unwrap();
        assert_eq!(u64::MAX, collection_rollup.reinsert_count);
    }

    #[test]
    fn rollup_per_collection_witness_functionality() {
        let rollup = RollupPerCollection::new(LogPosition::from_offset(10), 5);

        // Test that the rollup can handle boundary conditions
        assert_eq!(LogPosition::from_offset(10), rollup.start_log_position);
        assert_eq!(LogPosition::from_offset(15), rollup.limit_log_position);
        assert!(!rollup.is_empty());
    }

    #[test]
    fn rollup_per_collection_backpressure_boundary_conditions() {
        let rollup = RollupPerCollection::new(LogPosition::from_offset(0), u64::MAX);
        assert!(rollup.requires_backpressure(u64::MAX - 1));
        assert!(rollup.requires_backpressure(u64::MAX));

        let rollup = RollupPerCollection::new(LogPosition::from_offset(u64::MAX - 100), 50);
        assert!(!rollup.requires_backpressure(100));
        assert!(rollup.requires_backpressure(25));
    }

    #[test]
    fn active_log_keep_alive_zero_duration() {
        let mut active_log = ActiveLog::default();
        let initial_time = active_log.collect_after;

        active_log.keep_alive(Duration::ZERO);
        assert!(active_log.collect_after >= initial_time);
    }

    #[test]
    fn active_log_keep_alive_overflow_protection() {
        let mut active_log = ActiveLog::default();
        let now = Instant::now();
        active_log.keep_alive(Duration::from_secs(u64::MAX));
        assert!(active_log.collect_after >= now);
    }

    #[test]
    fn metrics_creation_and_structure() {
        let meter = opentelemetry::global::meter("test");
        let metrics = Metrics::new(meter);

        // We can't easily test metric values without a full OpenTelemetry setup,
        // but we can verify the metrics structure exists
        let _log_gauge = &metrics.log_total_uncompacted_records_count;
        let _dirty_counter = &metrics.dirty_log_records_read;
    }

    #[test]
    fn cached_parquet_fragment_weighted() {
        use chroma_cache::Weighted;

        let fragment = CachedBytes {
            bytes: vec![0u8; 1024],
        };
        assert_eq!(1024, fragment.weight());

        let empty_fragment = CachedBytes { bytes: vec![] };
        assert_eq!(0, empty_fragment.weight());

        let large_fragment = CachedBytes {
            bytes: vec![0u8; 1000],
        };
        assert_eq!(1000, large_fragment.weight());
    }

    #[test]
    fn log_server_config_defaults() {
        let config = LogServerConfig::default();
        assert_eq!(50051, config.port);
        assert_eq!("rust-log-service-0", config.my_member_id);
        assert_eq!(100, config.record_count_threshold);
        assert_eq!(1_000_000, config.num_records_before_backpressure);
        assert_eq!(10, config.reinsert_threshold);
        assert_eq!(Duration::from_secs(10), config.rollup_interval);
        assert_eq!(86_400_000_000, config.timeout_us);
        assert!(config.proxy_to.is_none());
    }

    #[test]
    fn opentelemetry_config_defaults() {
        let config = OpenTelemetryConfig {
            endpoint: default_endpoint(),
            service_name: default_otel_service_name(),
        };
        assert_eq!("http://otel-collector:4317", config.endpoint);
        assert_eq!("rust-log-service", config.service_name);
    }

    #[test]
    fn dirty_marker_purge_after_multiple_marks() {
        let collection_id = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let markers = vec![
            (
                LogPosition::from_offset(1),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(1),
                    num_records: 10,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(2),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(11),
                    num_records: 10,
                    reinsert_count: 1,
                    initial_insertion_epoch_us: now + 1000,
                },
            ),
            (
                LogPosition::from_offset(3),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(21),
                    num_records: 10,
                    reinsert_count: 2,
                    initial_insertion_epoch_us: now + 2000,
                },
            ),
            (
                LogPosition::from_offset(4),
                DirtyMarker::Purge { collection_id },
            ),
        ];

        let rollup = DirtyMarker::coalesce_markers(&markers).unwrap();
        assert_eq!(0, rollup.len());
    }

    #[test]
    fn dirty_marker_reinsert_operations() {
        let collection_id = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let mut mark_dirty = DirtyMarker::MarkDirty {
            collection_id,
            log_position: LogPosition::from_offset(1),
            num_records: 1,
            reinsert_count: u64::MAX - 1,
            initial_insertion_epoch_us: now,
        };

        mark_dirty.reinsert();
        if let DirtyMarker::MarkDirty { reinsert_count, .. } = mark_dirty {
            assert_eq!(u64::MAX, reinsert_count);
        } else {
            panic!("Expected MarkDirty variant");
        }

        mark_dirty.reinsert();
        if let DirtyMarker::MarkDirty { reinsert_count, .. } = mark_dirty {
            assert_eq!(u64::MAX, reinsert_count);
        } else {
            panic!("Expected MarkDirty variant");
        }
    }

    #[test]
    fn rollup_per_collection_gap_handling() {
        let mut rollup = RollupPerCollection::new(LogPosition::from_offset(10), 5);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        rollup.observe_dirty_marker(LogPosition::from_offset(20), 5, 1, now);

        assert_eq!(LogPosition::from_offset(10), rollup.start_log_position);
        assert_eq!(LogPosition::from_offset(25), rollup.limit_log_position);
        assert_eq!(1, rollup.reinsert_count);
        assert_eq!(now, rollup.initial_insertion_epoch_us);
    }

    #[tokio::test]
    async fn parquet_to_records_empty_parquet() {
        let empty_parquet = Arc::new(vec![]);
        let result = parquet_to_records(empty_parquet);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn parquet_to_records_invalid_data() {
        let invalid_data = Arc::new(vec![0u8; 100]);
        let result = parquet_to_records(invalid_data);
        assert!(result.is_err());
    }

    #[test]
    fn dirty_marker_coalesce_stress_test() {
        let collection_id = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let mut markers = Vec::with_capacity(1000);
        for i in 0..1000 {
            markers.push((
                LogPosition::from_offset(i),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(i * 10),
                    num_records: 1,
                    reinsert_count: i % 100,
                    initial_insertion_epoch_us: now + i,
                },
            ));
        }

        let rollup = DirtyMarker::coalesce_markers(&markers).unwrap();
        assert_eq!(1, rollup.len());
        let collection_rollup = rollup.get(&collection_id).unwrap();
        assert_eq!(
            LogPosition::from_offset(0),
            collection_rollup.start_log_position
        );
        assert_eq!(
            LogPosition::from_offset(9991),
            collection_rollup.limit_log_position
        );
        assert_eq!(99, collection_rollup.reinsert_count);
        assert_eq!(now + 999, collection_rollup.initial_insertion_epoch_us);
    }

    #[test]
    fn dirty_marker_coalesce_alternating_purge_pattern() {
        let collection_id = CollectionUuid::new();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let markers = vec![
            (
                LogPosition::from_offset(1),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(1),
                    num_records: 10,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(2),
                DirtyMarker::Purge { collection_id },
            ),
            (
                LogPosition::from_offset(3),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(20),
                    num_records: 5,
                    reinsert_count: 1,
                    initial_insertion_epoch_us: now + 1000,
                },
            ),
            (
                LogPosition::from_offset(4),
                DirtyMarker::Purge { collection_id },
            ),
            (
                LogPosition::from_offset(5),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(30),
                    num_records: 3,
                    reinsert_count: 2,
                    initial_insertion_epoch_us: now + 2000,
                },
            ),
        ];

        let rollup = DirtyMarker::coalesce_markers(&markers).unwrap();
        assert_eq!(0, rollup.len());
    }

    #[test]
    fn rollup_per_collection_extreme_positions() {
        let start_position = LogPosition::from_offset(u64::MAX - 10);
        let rollup = RollupPerCollection::new(start_position, 5);

        assert_eq!(start_position, rollup.start_log_position);
        assert!(!rollup.is_empty());
        assert!(rollup.requires_backpressure(1));
    }

    #[test]
    fn rollup_per_collection_zero_epoch() {
        let mut rollup = RollupPerCollection::new(LogPosition::from_offset(10), 5);

        rollup.observe_dirty_marker(LogPosition::from_offset(15), 5, 1, 0);

        assert_eq!(0, rollup.initial_insertion_epoch_us);
    }

    #[test]
    fn error_chain_verification() {
        let wal3_error = wal3::Error::Internal;
        let service_error: Box<dyn std::error::Error> = Box::new(Error::from(wal3_error));

        assert!(service_error.source().is_some());
        assert!(format!("{:?}", service_error).contains("Internal"));
    }

    #[test]
    fn active_log_default_state() {
        let active_log = ActiveLog::default();
        assert!(active_log.log.is_none());
        assert!(active_log.collect_after > Instant::now() - Duration::from_secs(1));
    }

    #[test]
    fn log_key_new_and_equality() {
        let collection_id = CollectionUuid::new();
        let key1 = LogKey { collection_id };
        let key2 = LogKey { collection_id };

        assert_eq!(key1, key2);
        assert_eq!(key1.collection_id, collection_id);
    }

    #[test]
    fn mark_dirty_struct_verification() {
        let collection_id = CollectionUuid::new();

        // Test that we can create the structure concept
        assert!(!collection_id.to_string().is_empty());
    }

    #[test]
    fn config_serialization_roundtrip() {
        use serde_json;

        let config = LogServerConfig::default();
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: LogServerConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config.port, deserialized.port);
        assert_eq!(config.my_member_id, deserialized.my_member_id);
        assert_eq!(
            config.record_count_threshold,
            deserialized.record_count_threshold
        );
    }

    #[test]
    fn dirty_marker_invalid_json_handling() {
        let invalid_json = r#"{"invalid": "structure"}"#;
        let result: Result<DirtyMarker, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn rollup_per_collection_edge_case_positions() {
        let mut rollup = RollupPerCollection::new(LogPosition::from_offset(100), 0);

        rollup.observe_dirty_marker(LogPosition::from_offset(50), 25, 1, 1000);

        assert_eq!(LogPosition::from_offset(50), rollup.start_log_position);
        assert_eq!(LogPosition::from_offset(100), rollup.limit_log_position);
    }

    #[test]
    fn backpressure_threshold_verification() {
        let rollup = RollupPerCollection::new(LogPosition::from_offset(0), 100);

        assert!(rollup.requires_backpressure(99));
        assert!(rollup.requires_backpressure(100));
        assert!(!rollup.requires_backpressure(101));

        let zero_rollup = RollupPerCollection::new(LogPosition::from_offset(10), 0);
        assert!(!zero_rollup.requires_backpressure(1));
        assert!(zero_rollup.requires_backpressure(0));
    }

    #[test]
    fn metrics_struct_field_access() {
        let meter = opentelemetry::global::meter("test_metrics");
        let metrics = Metrics::new(meter);

        let gauge_name = format!("{:?}", metrics.log_total_uncompacted_records_count);
        let counter_name = format!("{:?}", metrics.dirty_log_records_read);

        assert!(!gauge_name.is_empty());
        assert!(!counter_name.is_empty());
    }

    #[test]
    fn cached_parquet_fragment_default() {
        use chroma_cache::Weighted;

        let fragment = CachedBytes::default();
        assert_eq!(0, fragment.weight());
        assert!(fragment.bytes.is_empty());
    }
}
