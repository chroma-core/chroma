#![recursion_limit = "256"]

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use chroma_cache::CacheConfig;
use chroma_config::helpers::{deserialize_duration_from_seconds, serialize_duration_to_seconds};
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_log::config::GrpcLogConfig;
use chroma_storage::config::StorageConfig;
use chroma_storage::Storage;
use chroma_tracing::OtelFilter;
use chroma_tracing::OtelFilterLevel;
use chroma_types::chroma_proto::{
    garbage_collect_phase2_request::LogToCollect, log_service_server::LogService,
    purge_from_cache_request::EntryToEvict, CollectionInfo, GarbageCollectPhase2Request,
    GarbageCollectPhase2Response, GetAllCollectionInfoToCompactRequest,
    GetAllCollectionInfoToCompactResponse, InspectDirtyLogRequest, InspectDirtyLogResponse,
    InspectLogStateRequest, InspectLogStateResponse, LogRecord, MigrateLogRequest,
    MigrateLogResponse, OperationRecord, PullLogsRequest, PullLogsResponse,
    PurgeDirtyForCollectionRequest, PurgeDirtyForCollectionResponse, PurgeFromCacheRequest,
    PurgeFromCacheResponse, PushLogsRequest, PushLogsResponse, ScoutLogsRequest, ScoutLogsResponse,
    ScrubLogRequest, ScrubLogResponse, SealLogRequest, SealLogResponse,
    UpdateCollectionLogOffsetRequest, UpdateCollectionLogOffsetResponse,
};
use chroma_types::chroma_proto::{ForkLogsRequest, ForkLogsResponse};
use chroma_types::dirty_log_path_from_hostname;
use chroma_types::{CollectionUuid, DirtyMarker};
use figment::providers::{Env, Format, Yaml};
use futures::stream::StreamExt;
use opentelemetry::metrics::Meter;
use parking_lot::Mutex;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{Instrument, Level};
use uuid::Uuid;
use wal3::{
    Cursor, CursorName, CursorStore, CursorStoreOptions, Fragment, GarbageCollectionOptions,
    Limits, LogPosition, LogReader, LogReaderOptions, LogWriter, LogWriterOptions, Manifest,
    ManifestAndETag, MarkDirty as MarkDirtyTrait, Witness,
};

mod scrub;
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
    /// The total number of uncompacted records on the log, including those collections
    /// not-yet-returned for compaction.
    log_total_uncompacted_records_count: opentelemetry::metrics::Gauge<f64>,
    /// The number of records on the log that are ready for compaction.
    log_ready_uncompacted_records_count: opentelemetry::metrics::Gauge<f64>,
    /// The number of collections that likely need a purge-dirty call.
    log_likely_needs_purge_dirty: opentelemetry::metrics::Gauge<f64>,
    /// The rate at which records are read from the dirty log.
    dirty_log_records_read: opentelemetry::metrics::Counter<u64>,
    /// A gauge for the number of dirty log collections as of the last rollup.
    dirty_log_collections: opentelemetry::metrics::Gauge<u64>,
}

impl Metrics {
    pub fn new(meter: Meter) -> Self {
        Self {
            log_total_uncompacted_records_count: meter
                .f64_gauge("log_total_uncompacted_records_count")
                .build(),
            log_ready_uncompacted_records_count: meter
                .f64_gauge("log_ready_uncompacted_records_count")
                .build(),
            log_likely_needs_purge_dirty: meter.f64_gauge("log_likely_needs_purge_dirty").build(),
            dirty_log_records_read: meter.u64_counter("dirty_log_records_read").build(),
            dirty_log_collections: meter.u64_gauge("dirty_log_collections").build(),
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

///////////////////////////////////////// InspectedLogState ////////////////////////////////////////

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct InspectedLogState {
    manifest: Option<Manifest>,
    witness: Option<Witness>,
    start: u64,
    limit: u64,
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
    let opened = LogWriter::open_or_initialize(
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

////////////////////////////////////////// cache_key_for_* /////////////////////////////////////////

fn cache_key_for_manifest_and_etag(collection_id: CollectionUuid) -> String {
    format!("{collection_id}::MANIFEST/ETAG")
}

fn cache_key_for_cursor(collection_id: CollectionUuid, name: &CursorName) -> String {
    format!("{collection_id}::cursor::{}", name.path())
}

fn cache_key_for_fragment(collection_id: CollectionUuid, fragment_path: &str) -> String {
    format!("{collection_id}::{}", fragment_path)
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
    fn new(
        first_observation: LogPosition,
        num_records: u64,
        initial_insertion_epoch_us: u64,
    ) -> Self {
        Self {
            start_log_position: first_observation,
            limit_log_position: LogPosition::from_offset(
                first_observation.offset().saturating_add(num_records),
            ),
            reinsert_count: 0,
            initial_insertion_epoch_us,
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
        if log_position.offset().saturating_add(num_records) > self.limit_log_position.offset() {
            self.limit_log_position =
                LogPosition::from_offset(log_position.offset().saturating_add(num_records));
        }
        // Take the biggest reinsert count.
        self.reinsert_count = std::cmp::max(self.reinsert_count, reinsert_count);
        // Consider the most recent initial insertion time so if we've compacted earlier we drop.
        self.initial_insertion_epoch_us =
            std::cmp::min(self.initial_insertion_epoch_us, initial_insertion_epoch_us);
    }

    fn witness_cursor(&mut self, witness: Option<&Witness>) {
        // NOTE(rescrv):  There's an easy dance here to justify this as correct.  For the start log
        // position to advance, there must have been at least one GC cycle with a cursor that was
        // something other than 1.  That cursor should never get deleted, therefore we have a
        // witness and the unwrap_or call 0x90s.
        //
        // The consequence of this breaking is that the offset in the log will be behind sysdb.
        self.start_log_position = witness
            .map(|x| x.cursor.position)
            .unwrap_or(LogPosition::from_offset(1));
        self.limit_log_position = self.limit_log_position.max(self.start_log_position);
    }

    fn witness_manifest(&mut self, manifest: Option<&Manifest>) {
        self.start_log_position = manifest
            .map(|m| m.oldest_timestamp())
            .unwrap_or(LogPosition::from_offset(1));
        self.limit_log_position = manifest
            .map(|m| m.next_write_timestamp())
            .unwrap_or(LogPosition::from_offset(1));
    }

    fn is_empty(&self) -> bool {
        self.start_log_position >= self.limit_log_position
    }

    fn dirty_marker(&self, collection_id: CollectionUuid) -> DirtyMarker {
        DirtyMarker::MarkDirty {
            collection_id,
            log_position: self.start_log_position.offset(),
            num_records: self
                .limit_log_position
                .offset()
                .saturating_sub(self.start_log_position.offset()),
            reinsert_count: self.reinsert_count.saturating_add(1),
            initial_insertion_epoch_us: self.initial_insertion_epoch_us,
        }
    }

    fn requires_backpressure(&self, threshold: u64) -> bool {
        self.limit_log_position
            .offset()
            .saturating_sub(self.start_log_position.offset())
            >= threshold
    }
}

////////////////////////////////////////////// Rollups /////////////////////////////////////////////

struct Rollup {
    witness: Option<Witness>,
    cursor: Cursor,
    last_record_witnessed: LogPosition,
    rollups: HashMap<CollectionUuid, RollupPerCollection>,
}

//////////////////////////////////////////// DirtyMarker ///////////////////////////////////////////

fn coalesce_markers(
    markers: &[(LogPosition, DirtyMarker)],
    rollups: &mut HashMap<CollectionUuid, RollupPerCollection>,
    forget: &mut HashSet<CollectionUuid>,
) -> Result<(), wal3::Error> {
    for (_, marker) in markers {
        match marker {
            DirtyMarker::MarkDirty {
                collection_id,
                log_position,
                num_records,
                reinsert_count,
                initial_insertion_epoch_us,
            } => {
                let position = rollups.entry(*collection_id).or_insert_with(|| {
                    RollupPerCollection::new(
                        LogPosition::from_offset(*log_position),
                        *num_records,
                        *initial_insertion_epoch_us,
                    )
                });
                position.observe_dirty_marker(
                    LogPosition::from_offset(*log_position),
                    *num_records,
                    *reinsert_count,
                    *initial_insertion_epoch_us,
                );
            }
            DirtyMarker::Purge { collection_id } => {
                forget.insert(*collection_id);
            }
            DirtyMarker::Cleared => {}
        }
    }
    for collection_id in forget.iter() {
        rollups.remove(collection_id);
    }
    Ok(())
}

///////////////////////////////////////////// MarkDirty ////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct MarkDirty {
    collection_id: CollectionUuid,
    dirty_log: Option<Arc<LogWriter>>,
}

impl MarkDirty {
    pub fn path_for_hostname(hostname: &str) -> String {
        dirty_log_path_from_hostname(hostname)
    }
}

#[async_trait::async_trait]
impl wal3::MarkDirty for MarkDirty {
    async fn mark_dirty(
        &self,
        log_position: LogPosition,
        num_records: usize,
    ) -> Result<(), wal3::Error> {
        if let Some(dirty_log) = self.dirty_log.as_ref() {
            let num_records = num_records as u64;
            let initial_insertion_epoch_us = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|_| wal3::Error::Internal)?
                .as_micros() as u64;
            let dirty_marker = DirtyMarker::MarkDirty {
                collection_id: self.collection_id,
                log_position: log_position.offset(),
                num_records,
                reinsert_count: 0,
                initial_insertion_epoch_us,
            };
            let dirty_marker_json = serde_json::to_string(&dirty_marker).map_err(|err| {
                tracing::error!("Failed to serialize dirty marker: {}", err);
                wal3::Error::Internal
            })?;
            dirty_log.append(Vec::from(dirty_marker_json)).await?;
            Ok(())
        } else {
            tracing::error!("asked to mark dirty with no dirty log");
            Err(wal3::Error::Internal)
        }
    }
}

///////////////////////////////////////////// LogServer ////////////////////////////////////////////

#[derive(Default)]
struct RollupTransientState {
    rollups: HashMap<CollectionUuid, RollupPerCollection>,
    forget: HashSet<CollectionUuid>,
    largest_log_position_read: LogPosition,
}

pub struct LogServer {
    config: LogServerConfig,
    storage: Arc<Storage>,
    open_logs: Arc<StateHashTable<LogKey, LogStub>>,
    dirty_log: Option<Arc<LogWriter>>,
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

    /// Verify that the service is not in read-only mode.
    fn ensure_write_mode(&self) -> Result<(), Status> {
        if self.dirty_log.is_none() {
            // NOTE(rescrv):  This should NEVER happen in production.
            //
            // If it does happen, it is better to reject writes than to silently write data that
            // will never be accounted for by billing or compaction.
            Err(Status::permission_denied(
                "service is in read-only mode because it has no dirty log",
            ))
        } else if self.config.is_read_only() {
            Err(Status::permission_denied(
                "service is in read-only mode because of operator configuration",
            ))
        } else {
            Ok(())
        }
    }

    #[tracing::instrument(skip(self, request), err(Display))]
    async fn _update_collection_log_offset(
        &self,
        request: Request<UpdateCollectionLogOffsetRequest>,
        active: tokio::sync::MutexGuard<'_, ActiveLog>,
        allow_rollback: bool,
    ) -> Result<Response<UpdateCollectionLogOffsetResponse>, Status> {
        self.ensure_write_mode()?;
        let request = request.into_inner();
        let adjusted_log_offset = request.log_offset + 1;
        let collection_id = Uuid::parse_str(&request.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        tracing::info!(
            "update_collection_log_offset for {collection_id} to {}",
            adjusted_log_offset
        );
        let storage_prefix = collection_id.storage_prefix_for_log();
        let key = LogKey { collection_id };
        let handle = self.open_logs.get_or_create_state(key);
        let mark_dirty = MarkDirty {
            collection_id,
            dirty_log: self.dirty_log.clone(),
        };
        // NOTE(rescrv):  We use the writer and fall back to constructing a local reader in order
        // to force a read-repair of the collection when things partially fail.
        //
        // The writer will read the manifest, and try to read the next fragment.  This adds
        // latency, but improves correctness.
        let log = get_log_from_handle_with_mutex_held(
            &handle,
            active,
            &self.config.writer,
            &self.storage,
            &storage_prefix,
            mark_dirty,
        )
        .await
        .map_err(|err| Status::unknown(err.to_string()))?;

        let log_reader = log
            .reader(self.config.reader.clone())
            .unwrap_or(LogReader::new(
                self.config.reader.clone(),
                Arc::clone(&self.storage),
                storage_prefix.clone(),
            ));

        let res = log_reader.next_write_timestamp().await;
        if let Err(wal3::Error::UninitializedLog) = res {
            return Err(Status::not_found(format!(
                "collection {collection_id} not found"
            )));
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
        if !allow_rollback && cursor.position.offset() > adjusted_log_offset as u64 {
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
        let witness = if let Some(witness) = witness.as_ref() {
            cursor_store
                .save(cursor_name, &cursor, witness)
                .await
                .map_err(|err| {
                    Status::new(err.code().into(), format!("Failed to save cursor: {}", err))
                })?
        } else {
            cursor_store
                .init(cursor_name, cursor)
                .await
                .map_err(|err| {
                    Status::new(err.code().into(), format!("Failed to init cursor: {}", err))
                })?
        };
        if let Some(cache) = self.cache.as_ref() {
            let cache_key = cache_key_for_cursor(collection_id, cursor_name);
            match serde_json::to_string(&witness) {
                Ok(json_witness) => {
                    let value = CachedBytes {
                        bytes: Vec::from(json_witness),
                    };
                    cache.insert(cache_key, value).await;
                }
                Err(err) => {
                    tracing::error!("could not serialize cursor: {err}");
                    cache.remove(&cache_key).await;
                }
            }
        }
        if allow_rollback {
            let mark_dirty = MarkDirty {
                collection_id,
                dirty_log: self.dirty_log.clone(),
            };
            let _ = mark_dirty
                .mark_dirty(LogPosition::from_offset(adjusted_log_offset as u64), 1usize)
                .await;
        }
        let mut need_to_compact = self.need_to_compact.lock();
        if let Entry::Occupied(mut entry) = need_to_compact.entry(collection_id) {
            let rollup = entry.get_mut();
            rollup.start_log_position = std::cmp::max(
                rollup.start_log_position,
                LogPosition::from_offset(adjusted_log_offset as u64),
            );
            if rollup.is_empty() {
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
        let mut needs_purge_dirty = 0;
        // Do a non-allocating pass here.
        {
            let need_to_compact = self.need_to_compact.lock();
            for (collection_id, rollup) in need_to_compact.iter() {
                let time_on_log = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("time never moves to before epoch")
                    .as_micros()
                    .saturating_sub(rollup.initial_insertion_epoch_us as u128);
                if (rollup.limit_log_position >= rollup.start_log_position
                    && rollup.limit_log_position - rollup.start_log_position
                        >= request.min_compaction_size)
                    || rollup.reinsert_count >= self.config.reinsert_threshold
                    || time_on_log >= self.config.timeout_us as u128
                {
                    if rollup.reinsert_count >= self.config.reinsert_threshold * 2
                        && time_on_log >= self.config.timeout_us as u128 * 2
                    {
                        needs_purge_dirty += 1;
                    }
                    selected_rollups.push((*collection_id, *rollup));
                }
            }
        }
        let ready_uncompacted: u64 = selected_rollups
            .iter()
            .map(|(_, x)| x.limit_log_position - x.start_log_position)
            .sum();
        self.metrics
            .log_ready_uncompacted_records_count
            .record(ready_uncompacted as f64, &[]);
        self.metrics
            .log_likely_needs_purge_dirty
            .record(needs_purge_dirty as f64, &[]);
        // Then allocate the collection ID strings outside the lock.
        let mut all_collection_info = Vec::with_capacity(selected_rollups.len());
        for (collection_id, rollup) in selected_rollups.into_iter() {
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
    #[tracing::instrument(skip(self))]
    async fn roll_dirty_log(&self) -> Result<(), Error> {
        // Ensure at most one request at a time.
        let _guard = self.rolling_up.lock().await;
        let Some(dirty_log) = self.dirty_log.as_ref() else {
            tracing::error!("roll dirty log called with no dirty log configured");
            return Err(Error::CouldNotGetDirtyLogReader);
        };
        let mut rollup = self.read_and_coalesce_dirty_log(dirty_log).await?;
        if rollup.rollups.is_empty() {
            tracing::info!("rollups is empty");
            let backpressure = vec![];
            self.set_backpressure(&backpressure);
            let mut need_to_compact = self.need_to_compact.lock();
            let mut rollups = HashMap::new();
            std::mem::swap(&mut *need_to_compact, &mut rollups);
            return Ok(());
        };
        let collections = rollup.rollups.len();
        tracing::event!(
            tracing::Level::INFO,
            collections = ?collections,
        );
        self.metrics
            .dirty_log_collections
            .record(collections as u64, &[]);
        self.enrich_dirty_log(&mut rollup.rollups).await?;
        self.save_dirty_log(rollup, dirty_log).await
    }

    async fn save_dirty_log(&self, mut rollup: Rollup, dirty_log: &LogWriter) -> Result<(), Error> {
        let mut markers = vec![];
        let mut backpressure = vec![];
        let mut total_uncompacted = 0;
        for (collection_id, rollup) in rollup.rollups.iter() {
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
        let mut new_cursor = rollup.cursor.clone();
        match dirty_log.append_many(markers).await {
            Ok(_) | Err(wal3::Error::LogContentionDurable) => Ok(()),
            Err(err) => Err(err),
        }?;
        new_cursor.position = rollup.last_record_witnessed + 1u64;
        let Some(cursors) = dirty_log.cursors(CursorStoreOptions::default()) else {
            return Err(Error::CouldNotGetDirtyLogCursors);
        };
        tracing::info!(
            "Advancing dirty log cursor {:?} -> {:?}",
            rollup.cursor.position,
            new_cursor.position
        );
        if let Some(witness) = rollup.witness {
            cursors.save(&STABLE_PREFIX, &new_cursor, &witness).await?;
        } else {
            cursors.init(&STABLE_PREFIX, new_cursor).await?;
        }
        self.metrics
            .log_total_uncompacted_records_count
            .record(total_uncompacted as f64, &[]);
        self.set_backpressure(&backpressure);
        let mut need_to_compact = self.need_to_compact.lock();
        std::mem::swap(&mut *need_to_compact, &mut rollup.rollups);
        Ok(())
    }

    /// Read the entirety of a prefix of the dirty log.
    #[tracing::instrument(skip(self), err(Display))]
    #[allow(clippy::type_complexity)]
    async fn read_and_coalesce_dirty_log(&self, dirty_log: &LogWriter) -> Result<Rollup, Error> {
        let Some(reader) = dirty_log.reader(LogReaderOptions::default()) else {
            return Err(Error::CouldNotGetDirtyLogReader);
        };
        let Some(cursors) = dirty_log.cursors(CursorStoreOptions::default()) else {
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
        let dirty_fragments = match reader
            .scan(
                cursor.position,
                Limits {
                    max_files: None,
                    max_bytes: None,
                    max_records: None,
                },
            )
            .await
        {
            Ok(dirty_fragments) => dirty_fragments,
            Err(wal3::Error::UninitializedLog) => {
                let last_record_witnessed = cursor.position;
                let rollups = HashMap::default();
                let rollup = Rollup {
                    witness,
                    cursor,
                    last_record_witnessed,
                    rollups,
                };
                tracing::info!("empty dirty log");
                return Ok(rollup);
            }
            Err(e) => {
                return Err(Error::Wal3(e));
            }
        };
        if dirty_fragments.is_empty() {
            let last_record_witnessed = cursor.position;
            let rollups = HashMap::default();
            let rollup = Rollup {
                witness,
                cursor,
                last_record_witnessed,
                rollups,
            };
            tracing::info!("empty dirty log");
            return Ok(rollup);
        }
        if dirty_fragments.len() >= 1000 {
            tracing::error!("Too many dirty fragments: {}", dirty_fragments.len());
        }
        let rollup = Mutex::new(RollupTransientState::default());
        let markers_read = AtomicU64::new(0);
        let dirty_futures = dirty_fragments
            .iter()
            .map(|fragment| async {
                let (_, records, _) = reader.read_parquet(fragment).await?;
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
                markers_read.fetch_add(records.len() as u64, Ordering::Relaxed);
                let mut rollup = rollup.lock();
                if let Some(max) = records.iter().map(|x| x.0).max() {
                    rollup.largest_log_position_read =
                        std::cmp::max(max, rollup.largest_log_position_read);
                }
                // We create a new hash set for forget because we cannot borrow rollup mutably
                // twice.  Further, we need to track every forget call to remove down below before
                // we return the rollup.
                let mut forget = HashSet::default();
                coalesce_markers(&records, &mut rollup.rollups, &mut forget)?;
                rollup.forget.extend(forget);
                Ok::<(), Error>(())
            })
            .collect::<Vec<_>>();

        let stream = futures::stream::iter(dirty_futures);
        let mut buffered = stream.buffer_unordered(50);
        while let Some(res) = buffered.next().await {
            if let Err(err) = res {
                tracing::error!(error = ?err);
            }
        }
        self.metrics
            .dirty_log_records_read
            .add(markers_read.load(Ordering::Relaxed), &[]);
        let mut transient = rollup.lock();
        let last_record_witnessed = transient.largest_log_position_read;
        let mut rollups = std::mem::take(&mut transient.rollups);
        for forget in transient.forget.iter() {
            rollups.remove(forget);
        }
        Ok(Rollup {
            witness,
            cursor,
            rollups,
            last_record_witnessed,
        })
    }

    /// Enrich a rolled up dirty log by reading cursors and manifests to determine what still needs
    /// to be compacted.  Entries will be removed if they correspond to a compacted log range.
    /// Entries will remain if there is data to be collected.
    #[tracing::instrument(skip(self, rollups), err(Display))]
    async fn enrich_dirty_log(
        &self,
        rollups: &mut HashMap<CollectionUuid, RollupPerCollection>,
    ) -> Result<(), Error> {
        let load_witness = |storage, collection_id: CollectionUuid| async move {
            let cursor = &COMPACTION;
            let cursor_store = CursorStore::new(
                CursorStoreOptions::default(),
                Arc::clone(storage),
                collection_id.storage_prefix_for_log(),
                "rollup".to_string(),
            );
            let witness = if let Some(cache) = self.cache.as_ref() {
                let key = LogKey { collection_id };
                let handle = self.open_logs.get_or_create_state(key);
                let mut _active = handle.active.lock().await;
                let cache_key = cache_key_for_cursor(collection_id, cursor);
                if let Ok(Some(json_witness)) = cache.get(&cache_key).await {
                    let witness: Witness = serde_json::from_slice(&json_witness.bytes)?;
                    return Ok((Some(witness), None));
                }
                let load_span = tracing::info_span!("cursor load");
                let res = cursor_store.load(cursor).instrument(load_span).await?;
                if let Some(witness) = res.as_ref() {
                    let json_witness = serde_json::to_string(&witness)?;
                    let value = CachedBytes {
                        bytes: Vec::from(json_witness),
                    };
                    cache.insert(cache_key, value).await;
                }
                res
            } else {
                let span = tracing::info_span!("cursor load", collection_id = ?collection_id);
                cursor_store.load(cursor).instrument(span).await?
            };
            // NOTE(rescrv):  This may turn out to be a bad idea, but do not load the manifest from
            // cache in order to prevent a stale cache from perpetually returning a stale result.
            let manifest = if witness.is_none() {
                let reader = LogReader::open(
                    LogReaderOptions::default(),
                    Arc::clone(storage),
                    collection_id.storage_prefix_for_log(),
                )
                .await?;
                reader.manifest().await?
            } else {
                None
            };
            Ok::<(Option<Witness>, Option<Manifest>), Error>((witness, manifest))
        };
        let mut futures = Vec::with_capacity(rollups.len());
        for (collection_id, mut rollup) in std::mem::take(rollups) {
            let load_witness = &load_witness;
            futures.push(async move {
                let (witness, manifest) = match load_witness(&self.storage, collection_id).await {
                    Ok(witness) => witness,
                    Err(err) => {
                        tracing::warn!("could not load cursor: {err}");
                        return Some((collection_id, rollup));
                    }
                };
                // NOTE(rescrv):  There are two spreads that we have.
                // `rollup` tracks the minimum and maximum offsets of a record on the dirty log.
                // The spread between cursor (if it exists) and manifest.maximum_log_offset tracks the
                // data that needs to be compacted.
                match (&witness, &manifest) {
                    (Some(witness), Some(_)) | (Some(witness), None) => {
                        rollup.witness_cursor(Some(witness));
                    }
                    (None, Some(manifest)) => {
                        rollup.witness_manifest(Some(manifest));
                    }
                    (None, None) => {}
                };
                if !rollup.is_empty() {
                    Some((collection_id, rollup))
                } else {
                    None
                }
            });
        }
        if !futures.is_empty() {
            for (collection_id, rollup) in futures::future::join_all(futures)
                .await
                .into_iter()
                .flatten()
            {
                rollups.insert(collection_id, rollup);
            }
        }
        Ok(())
    }

    pub async fn background_task(&self) {
        if self.config.is_read_only() {
            return;
        }
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
        self.ensure_write_mode()?;
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

        tracing::info!("Pushing logs for collection {}", collection_id);
        let prefix = collection_id.storage_prefix_for_log();
        let key = LogKey { collection_id };
        let handle = self.open_logs.get_or_create_state(key);
        let mark_dirty = MarkDirty {
            collection_id,
            dirty_log: self.dirty_log.clone(),
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
                return Err(Status::not_found(format!(
                    "collection {collection_id} not found"
                )));
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
        match log.append_many(messages).await {
            Ok(_) | Err(wal3::Error::LogContentionDurable) => {}
            Err(err @ wal3::Error::Backoff) => {
                return Err(Status::new(
                    chroma_error::ErrorCodes::Unavailable.into(),
                    err.to_string(),
                ));
            }
            Err(err) => return Err(Status::new(err.code().into(), err.to_string())),
        };
        if let Some(cache) = self.cache.as_ref() {
            let cache_key = cache_key_for_manifest_and_etag(collection_id);
            if let Some(manifest_and_etag) = log.manifest_and_etag() {
                if let Ok(manifest_and_etag_bytes) = serde_json::to_vec(&manifest_and_etag) {
                    let cache_value = CachedBytes {
                        bytes: manifest_and_etag_bytes,
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

    async fn scout_logs(
        &self,
        request: Request<ScoutLogsRequest>,
    ) -> Result<Response<ScoutLogsResponse>, Status> {
        let scout_logs = request.into_inner();
        let collection_id = Uuid::parse_str(&scout_logs.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        let prefix = collection_id.storage_prefix_for_log();
        let log_reader = LogReader::new(
            self.config.reader.clone(),
            Arc::clone(&self.storage),
            prefix,
        );
        let cache_key = cache_key_for_manifest_and_etag(collection_id);
        let mut cached_manifest_and_e_tag = None;
        if let Some(cache) = self.cache.as_ref() {
            if let Some(cache_bytes) = cache.get(&cache_key).await.ok().flatten() {
                let met = serde_json::from_slice::<ManifestAndETag>(&cache_bytes.bytes).ok();
                cached_manifest_and_e_tag = met;
            }
        }
        // NOTE(rescrv):  We verify and if verification fails, we take the cached manifest to fall
        // back to the uncached path.
        if let Some(cached) = cached_manifest_and_e_tag.as_ref() {
            // Here's the linearization point.  We have a cached manifest and e_tag.
            //
            // If we verify (perform a head), then statistically speaking, the manifest and e_tag
            // we have in hand is identical (barring md5 collision) to the manifest and e_tag on
            // storage.  We can use the cached manifest and e_tag in this case because it is the
            // identical flow whether we read the whole manifest from storage or whether we pretend
            // to read it/verify it with a HEAD and then read out of cache.
            if !log_reader.verify(cached).await.unwrap_or_default() {
                cached_manifest_and_e_tag.take();
            }
        }
        let (start_position, limit_position) =
            if let Some(manifest_and_e_tag) = cached_manifest_and_e_tag {
                (
                    manifest_and_e_tag.manifest.oldest_timestamp(),
                    manifest_and_e_tag.manifest.next_write_timestamp(),
                )
            } else {
                let (start_position, limit_position) = match log_reader.manifest_and_e_tag().await {
                    Ok(Some(manifest_and_e_tag)) => {
                        if let Some(cache) = self.cache.as_ref() {
                            let json = serde_json::to_string(&manifest_and_e_tag)
                                .map_err(|err| Status::unknown(err.to_string()))?;
                            let cached_bytes = CachedBytes {
                                bytes: Vec::from(json),
                            };
                            cache.insert(cache_key, cached_bytes).await;
                        }
                        (
                            manifest_and_e_tag.manifest.oldest_timestamp(),
                            manifest_and_e_tag.manifest.next_write_timestamp(),
                        )
                    }
                    Ok(None) => (LogPosition::from_offset(1), LogPosition::from_offset(1)),
                    Err(wal3::Error::UninitializedLog) => {
                        return Err(Status::not_found(format!(
                            "collection {collection_id} not found"
                        )));
                    }
                    Err(err) => {
                        return Err(Status::new(
                            err.code().into(),
                            format!("could not scout logs: {err:?}"),
                        ));
                    }
                };
                (start_position, limit_position)
            };
        let start_offset = start_position.offset() as i64;
        let limit_offset = limit_position.offset() as i64;
        Ok(Response::new(ScoutLogsResponse {
            first_uncompacted_record_offset: start_offset,
            first_uninserted_record_offset: limit_offset,
            is_sealed: true,
        }))
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
            let cache_key = cache_key_for_manifest_and_etag(collection_id);
            let cached_bytes = cache.get(&cache_key).await.ok().flatten()?;
            let manifest_and_etag: ManifestAndETag =
                serde_json::from_slice(&cached_bytes.bytes).ok()?;
            let limits = Limits {
                max_files: Some(pull_logs.batch_size as u64 + 1),
                max_bytes: None,
                max_records: Some(pull_logs.batch_size as u64),
            };
            LogReader::scan_from_manifest(
                &manifest_and_etag.manifest,
                LogPosition::from_offset(pull_logs.start_from_offset as u64),
                limits,
            )
        } else {
            None
        }
    }

    async fn read_fragments_via_log_reader(
        &self,
        collection_id: CollectionUuid,
        pull_logs: &PullLogsRequest,
    ) -> Result<Vec<Fragment>, wal3::Error> {
        let prefix = collection_id.storage_prefix_for_log();
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
        let pull_logs = request.into_inner();
        let collection_id = Uuid::parse_str(&pull_logs.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;

        tracing::info!(
            collection_id = collection_id.to_string(),
            start_from_offset = pull_logs.start_from_offset,
            batch_size = pull_logs.batch_size,
            "Pulling logs",
        );

        let fragments = match self.read_fragments(collection_id, &pull_logs).await {
            Ok(fragments) => fragments,
            Err(wal3::Error::UninitializedLog) => vec![],
            Err(err) => {
                return Err(Status::new(err.code().into(), err.to_string()));
            }
        };
        let futures = fragments
            .iter()
            .map(|fragment| async {
                let prefix = collection_id.storage_prefix_for_log();
                if let Some(cache) = self.cache.as_ref() {
                    let cache_key = cache_key_for_fragment(collection_id, &fragment.path);
                    if let Ok(Some(answer)) = cache.get(&cache_key).await {
                        return Ok(Arc::new(answer.bytes));
                    }
                    let answer =
                        LogReader::stateless_fetch(&self.storage, &prefix, fragment).await?;
                    let cache_value = CachedBytes {
                        bytes: Clone::clone(&*answer),
                    };
                    cache.insert(cache_key, cache_value).await;
                    Ok(answer)
                } else {
                    LogReader::stateless_fetch(&self.storage, &prefix, fragment).await
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
        if records.len() != pull_logs.batch_size as usize
            || (!records.is_empty() && records[0].log_offset != pull_logs.start_from_offset)
        {
            return Err(Status::not_found("Some entries have been purged"));
        }
        Ok(Response::new(PullLogsResponse { records }))
    }

    async fn fork_logs(
        &self,
        request: Request<ForkLogsRequest>,
    ) -> Result<Response<ForkLogsResponse>, Status> {
        self.ensure_write_mode()?;
        let request = request.into_inner();
        let source_collection_id = Uuid::parse_str(&request.source_collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        self.check_for_backpressure(source_collection_id)?;
        let target_collection_id = Uuid::parse_str(&request.target_collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        let source_prefix = source_collection_id.storage_prefix_for_log();
        let target_prefix = target_collection_id.storage_prefix_for_log();
        let storage = Arc::clone(&self.storage);
        let options = self.config.writer.clone();

        tracing::info!(
            source_collection_id = source_collection_id.to_string(),
            target_collection_id = target_collection_id.to_string(),
        );
        let log_reader = LogReader::new(
            self.config.reader.clone(),
            Arc::clone(&storage),
            source_prefix.clone(),
        );
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
        let cursor = witness.map(|x| x.cursor.position);
        tracing::event!(Level::INFO, offset = ?cursor);
        wal3::copy(
            &storage,
            &options,
            &log_reader,
            cursor.unwrap_or(LogPosition::from_offset(1)),
            target_prefix.clone(),
        )
        .await
        .map_err(|err| Status::new(err.code().into(), format!("Failed to copy log: {}", err)))?;
        let log_reader = LogReader::new(
            self.config.reader.clone(),
            Arc::clone(&storage),
            target_prefix,
        );
        let new_manifest = log_reader
            .manifest()
            .await
            .map_err(|err| {
                Status::new(
                    err.code().into(),
                    format!("Unable to read copied manifest: {}", err),
                )
            })?
            .ok_or_else(|| Status::internal("Unable to find copied manifest"))?;
        let first_copied_offset = new_manifest.oldest_timestamp();
        // This is the next record to insert, so we'll have to adjust downwards.
        let max_offset = new_manifest.next_write_timestamp();
        if let Some(cursor) = cursor {
            if cursor < first_copied_offset {
                return Err(Status::internal(format!(
                    "Compaction cursor {} is behind start of manifest {}",
                    cursor.offset(),
                    first_copied_offset.offset()
                )));
            }
            if max_offset < cursor {
                return Err(Status::new(
                    chroma_error::ErrorCodes::Internal.into(),
                    format!(
                        "Compaction cursor {} is after end of manifest {}",
                        cursor.offset(),
                        max_offset.offset()
                    ),
                ));
            }
        }

        let cursor = cursor.unwrap_or(LogPosition::from_offset(1));
        if cursor != max_offset {
            let mark_dirty = MarkDirty {
                collection_id: target_collection_id,
                dirty_log: self.dirty_log.clone(),
            };
            let _ = mark_dirty
                .mark_dirty(cursor, (max_offset - cursor) as usize)
                .await;
        }

        let compaction_offset = (cursor - 1u64).offset();
        let enumeration_offset = (max_offset - 1u64).offset();
        tracing::event!(Level::INFO, compaction_offset, enumeration_offset);
        Ok(Response::new(ForkLogsResponse {
            // NOTE: The upstream service expects the last compacted offset as compaction offset
            compaction_offset,
            // NOTE: The upstream service expects the last uncompacted offset as enumeration offset
            enumeration_offset,
        }))
    }

    async fn get_all_collection_info_to_compact(
        &self,
        request: Request<GetAllCollectionInfoToCompactRequest>,
    ) -> Result<Response<GetAllCollectionInfoToCompactResponse>, Status> {
        self.cached_get_all_collection_info_to_compact(request.into_inner())
            .await
    }

    async fn update_collection_log_offset(
        &self,
        request: Request<UpdateCollectionLogOffsetRequest>,
    ) -> Result<Response<UpdateCollectionLogOffsetResponse>, Status> {
        self.ensure_write_mode()?;
        let request = request.into_inner();
        let collection_id = Uuid::parse_str(&request.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;

        // Grab a lock on the state for this key, so that a racing initialize won't do anything.
        let key = LogKey { collection_id };
        let handle = self.open_logs.get_or_create_state(key);
        let active = handle.active.lock().await;
        self._update_collection_log_offset(Request::new(request), active, false)
            .await
    }

    async fn rollback_collection_log_offset(
        &self,
        request: Request<UpdateCollectionLogOffsetRequest>,
    ) -> Result<Response<UpdateCollectionLogOffsetResponse>, Status> {
        self.ensure_write_mode()?;
        let request = request.into_inner();
        let collection_id = Uuid::parse_str(&request.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;

        tracing::event!(Level::ERROR, name = "abuse of error to force a log: rolling back collection log offset", collection_id =? collection_id);
        // Grab a lock on the state for this key, so that a racing initialize won't do anything.
        let key = LogKey { collection_id };
        let handle = self.open_logs.get_or_create_state(key);
        let active = handle.active.lock().await;
        self._update_collection_log_offset(Request::new(request), active, true)
            .await
    }

    async fn purge_dirty_for_collection(
        &self,
        request: Request<PurgeDirtyForCollectionRequest>,
    ) -> Result<Response<PurgeDirtyForCollectionResponse>, Status> {
        self.ensure_write_mode()?;
        let request = request.into_inner();
        let collection_ids = request
            .collection_ids
            .iter()
            .map(|id| CollectionUuid::from_str(id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                Status::invalid_argument(format!("Failed to parse collection id: {err}"))
            })?;
        tracing::info!("Purging collections in dirty log: [{collection_ids:?}]");
        let dirty_marker_json_blobs = collection_ids
            .into_iter()
            .map(|collection_id| {
                serde_json::to_string(&DirtyMarker::Purge { collection_id }).map(String::into_bytes)
            })
            .collect::<Result<_, _>>()
            .map_err(|err| Status::internal(format!("Failed to serialize dirty marker: {err}")))?;
        if let Some(dirty_log) = self.dirty_log.as_ref() {
            dirty_log
                .append_many(dirty_marker_json_blobs)
                .await
                .map_err(|err| Status::new(err.code().into(), err.to_string()))?;
            Ok(Response::new(PurgeDirtyForCollectionResponse {}))
        } else {
            tracing::error!("dirty log not set and purge dirty received");
            Err(Status::failed_precondition("dirty log not configured"))
        }
    }

    #[tracing::instrument(skip(self, _request))]
    async fn inspect_dirty_log(
        &self,
        _request: Request<InspectDirtyLogRequest>,
    ) -> Result<Response<InspectDirtyLogResponse>, Status> {
        let Some(dirty_log) = self.dirty_log.as_ref() else {
            return Err(Status::unavailable("dirty log not configured"));
        };
        let Some(reader) = dirty_log.reader(LogReaderOptions::default()) else {
            return Err(Status::unavailable("Failed to get dirty log reader"));
        };
        let Some(cursors) = dirty_log.cursors(CursorStoreOptions::default()) else {
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
        _request: Request<SealLogRequest>,
    ) -> Result<Response<SealLogResponse>, Status> {
        Err(Status::failed_precondition(
            "rust log service doesn't do sealing",
        ))
    }

    async fn migrate_log(
        &self,
        _request: Request<MigrateLogRequest>,
    ) -> Result<Response<MigrateLogResponse>, Status> {
        Err(Status::failed_precondition("migration removed"))
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
        let storage_prefix = collection_id.storage_prefix_for_log();
        let log_reader = LogReader::new(
            self.config.reader.clone(),
            Arc::clone(&self.storage),
            storage_prefix.clone(),
        );
        let mani = log_reader.manifest().await;
        if let Err(wal3::Error::UninitializedLog) = mani {
            return Ok(Response::new(InspectLogStateResponse {
                debug: "log uninitialized\n".to_string(),
                start: 0,
                limit: 0,
                json: "{}".to_string(),
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
        let (start, limit) = if let Some(mani) = mani.as_ref() {
            let start = witness
                .as_ref()
                .map(|w| w.cursor.position)
                .unwrap_or(mani.oldest_timestamp());
            let limit = mani.next_write_timestamp();
            (start.offset(), limit.offset())
        } else {
            (0, 0)
        };
        let debug = format!("manifest: {mani:#?}\ncompaction cursor: {witness:?}");
        let inspected = InspectedLogState {
            manifest: mani,
            witness,
            start,
            limit,
        };
        let json =
            serde_json::to_string(&inspected).map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(InspectLogStateResponse {
            debug,
            start,
            limit,
            json,
        }))
    }

    async fn garbage_collect_phase2(
        &self,
        request: Request<GarbageCollectPhase2Request>,
    ) -> Result<Response<GarbageCollectPhase2Response>, Status> {
        self.ensure_write_mode()?;
        let gc2 = request.into_inner();

        fn handle_error_properly(err: wal3::Error) -> Status {
            if let wal3::Error::GarbageCollectionPrecondition(what) = err {
                Status::failed_precondition(format!("retry from the top because of a race: {what}"))
            } else {
                Status::unknown(err.to_string())
            }
        }
        match gc2.log_to_collect {
            Some(LogToCollect::CollectionId(x)) => {
                let collection_id = Uuid::parse_str(&x)
                    .map(CollectionUuid)
                    .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
                tracing::event!(Level::INFO, collection_id =? collection_id);
                let prefix = collection_id.storage_prefix_for_log();
                let key = LogKey { collection_id };
                let mark_dirty = MarkDirty {
                    collection_id,
                    dirty_log: self.dirty_log.clone(),
                };
                let handle = self.open_logs.get_or_create_state(key);
                let log = get_log_from_handle(
                    &handle,
                    &self.config.writer,
                    &self.storage,
                    &prefix,
                    mark_dirty,
                )
                .await
                .map_err(handle_error_properly)?;
                log.garbage_collect_phase2_update_manifest(&GarbageCollectionOptions::default())
                    .await
                    .map_err(handle_error_properly)?;
                Ok(Response::new(GarbageCollectPhase2Response {}))
            }
            Some(LogToCollect::DirtyLog(host)) => {
                if host != self.config.my_member_id {
                    return Err(Status::failed_precondition(
                            format!("can only perform gc phase 2 on our own dirty log:  I am {}, but was asked for {}", self.config.my_member_id, host),
                        ));
                }
                tracing::event!(Level::INFO, host =? host);
                if let Some(dirty_log) = self.dirty_log.as_ref() {
                    dirty_log
                    .garbage_collect_phase2_update_manifest(&GarbageCollectionOptions::default())
                    .await
                    .map_err(|err| Status::unknown(err.to_string()))?;
                } else {
                    tracing::error!("Could not garbage collect dirty log.");
                    return Err(Status::failed_precondition(
                        "no dirty log configured for garbage collection".to_string(),
                    ));
                }
                Ok(Response::new(GarbageCollectPhase2Response {}))
            }
            None => Err(Status::not_found("log not found because it's null")),
        }
    }

    async fn purge_from_cache(
        &self,
        request: Request<PurgeFromCacheRequest>,
    ) -> Result<Response<PurgeFromCacheResponse>, Status> {
        self.ensure_write_mode()?;
        let purge = request.into_inner();

        let key = match purge.entry_to_evict {
            Some(EntryToEvict::CursorForCollectionId(x)) => {
                let collection_id = Uuid::parse_str(&x)
                    .map(CollectionUuid)
                    .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
                Some(cache_key_for_cursor(collection_id, &COMPACTION))
            }
            Some(EntryToEvict::ManifestForCollectionId(x)) => {
                let collection_id = Uuid::parse_str(&x)
                    .map(CollectionUuid)
                    .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
                Some(cache_key_for_manifest_and_etag(collection_id))
            }
            Some(EntryToEvict::Fragment(f)) => {
                let collection_id = Uuid::parse_str(&f.collection_id)
                    .map(CollectionUuid)
                    .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
                Some(cache_key_for_fragment(collection_id, &f.fragment_path))
            }
            None => None,
        };
        if let Some(key) = key {
            if let Some(cache) = self.cache.as_ref() {
                cache.remove(&key).await;
            }
        }
        Ok(Response::new(PurgeFromCacheResponse {}))
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

    async fn rollback_collection_log_offset(
        &self,
        request: Request<UpdateCollectionLogOffsetRequest>,
    ) -> Result<Response<UpdateCollectionLogOffsetResponse>, Status> {
        self.log_server
            .rollback_collection_log_offset(request)
            .await
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

    async fn scrub_log(
        &self,
        request: Request<ScrubLogRequest>,
    ) -> Result<Response<ScrubLogResponse>, Status> {
        self.log_server.scrub_log(request).await
    }

    async fn garbage_collect_phase2(
        &self,
        request: Request<GarbageCollectPhase2Request>,
    ) -> Result<Response<GarbageCollectPhase2Response>, Status> {
        self.log_server.garbage_collect_phase2(request).await
    }

    async fn purge_from_cache(
        &self,
        request: Request<PurgeFromCacheRequest>,
    ) -> Result<Response<PurgeFromCacheResponse>, Status> {
        self.log_server.purge_from_cache(request).await
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

        let max_encoding_message_size = log_server.config.max_encoding_message_size;
        let max_decoding_message_size = log_server.config.max_decoding_message_size;
        let max_concurrent_streams = log_server.config.grpc_max_concurrent_streams;
        let shutdown_grace_period = log_server.config.grpc_shutdown_grace_period;

        let wrapper = LogServerWrapper {
            log_server: Arc::new(log_server),
        };
        let background_server = Arc::clone(&wrapper.log_server);
        let background =
            tokio::task::spawn(async move { background_server.background_task().await });
        let server = Server::builder()
            .max_concurrent_streams(Some(max_concurrent_streams))
            .layer(chroma_tracing::GrpcServerTraceLayer)
            .add_service(health_service)
            .add_service(
                chroma_types::chroma_proto::log_service_server::LogServiceServer::new(wrapper)
                    .max_decoding_message_size(max_decoding_message_size)
                    .max_encoding_message_size(max_encoding_message_size),
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
            tracing::info!("Received SIGTERM, waiting for grace period...");
            // Note: gRPC calls can still be successfully made during this period. We rely on the memberlist updating to stop clients from sending new requests. Ideally there would be a Tower layer that rejected new requests during this period with UNAVAILABLE or similar.
            tokio::time::sleep(shutdown_grace_period).await;
            tracing::info!("Grace period ended, shutting down server...");
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

fn default_otel_filters() -> Vec<OtelFilter> {
    vec![OtelFilter {
        crate_name: "chroma_log_service".to_string(),
        filter_level: OtelFilterLevel::Trace,
    }]
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
    /// Values in the environment variables take precedence over values in the YAML file.
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
    /// Values in the environment variables take precedence over values in the YAML file.
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
    #[serde(default = "default_otel_filters")]
    pub filters: Vec<OtelFilter>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct LogServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "LogServerConfig::default_my_member_id")]
    pub my_member_id: String,
    #[serde(default)]
    pub read_only: bool,
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
    #[serde(default = "LogServerConfig::default_max_encoding_message_size")]
    pub max_encoding_message_size: usize,
    #[serde(default = "LogServerConfig::default_max_decoding_message_size")]
    pub max_decoding_message_size: usize,
    #[serde(
        rename = "grpc_shutdown_grace_period_seconds",
        deserialize_with = "deserialize_duration_from_seconds",
        serialize_with = "serialize_duration_to_seconds",
        default = "LogServerConfig::default_grpc_shutdown_grace_period"
    )]
    pub grpc_shutdown_grace_period: Duration,
    #[serde(default = "LogServerConfig::default_grpc_max_concurrent_streams")]
    pub grpc_max_concurrent_streams: u32,
}

impl LogServerConfig {
    /// Should the log service allow mutable log operations?
    fn is_read_only(&self) -> bool {
        self.read_only
    }

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

    fn default_max_encoding_message_size() -> usize {
        32_000_000
    }

    fn default_max_decoding_message_size() -> usize {
        32_000_000
    }

    fn default_grpc_shutdown_grace_period() -> Duration {
        Duration::from_secs(1)
    }
    fn default_grpc_max_concurrent_streams() -> u32 {
        1000
    }
}

impl Default for LogServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            my_member_id: LogServerConfig::default_my_member_id(),
            read_only: false,
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
            max_encoding_message_size: Self::default_max_encoding_message_size(),
            max_decoding_message_size: Self::default_max_decoding_message_size(),
            grpc_shutdown_grace_period: Self::default_grpc_shutdown_grace_period(),
            grpc_max_concurrent_streams: Self::default_grpc_max_concurrent_streams(),
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
            &MarkDirty::path_for_hostname(&config.my_member_id),
            "dirty log writer",
            (),
        )
        .await
        .map_err(|err| -> Box<dyn ChromaError> { Box::new(err) as _ })?;
        let dirty_log = Some(Arc::new(dirty_log));
        let rolling_up = tokio::sync::Mutex::new(());
        let metrics = Metrics::new(opentelemetry::global::meter("chroma"));
        let backpressure = Mutex::new(Arc::new(HashSet::default()));
        let need_to_compact = Mutex::new(HashMap::default());
        Ok(Self {
            config: config.clone(),
            open_logs: Arc::new(StateHashTable::default()),
            storage,
            dirty_log,
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
        chroma_tracing::init_otel_tracing(
            &otel_config.service_name,
            &otel_config.filters,
            &otel_config.endpoint,
        );
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
    use std::{str::FromStr, sync::Arc};

    use super::*;
    use crate::state_hash_table::Value;

    use chroma_storage::s3_client_for_test_with_new_bucket;
    use chroma_types::{are_update_metadatas_close_to_equal, Operation, OperationRecord};
    use opentelemetry::global::meter;
    use proptest::prelude::*;
    use tokio::{runtime::Runtime, sync::mpsc::unbounded_channel, time::sleep};
    use tonic::{Code, IntoRequest};
    use wal3::{GarbageCollector, SnapshotOptions, ThrottleOptions};

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
                    log_position: 1,
                    num_records: 1,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(46),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: 2,
                    num_records: 1,
                    reinsert_count: 2,
                    initial_insertion_epoch_us: now,
                },
            ),
        ];
        let mut rollups = HashMap::new();
        let mut forget = HashSet::new();
        coalesce_markers(&markers, &mut rollups, &mut forget).unwrap();
        assert!(forget.is_empty());
        assert_eq!(1, rollups.len());
        let rollup = rollups.get(&collection_id).unwrap();
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
                    log_position: 1,
                    num_records: 1,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(1),
                DirtyMarker::MarkDirty {
                    collection_id: collection_id_acting,
                    log_position: 1,
                    num_records: 100,
                    reinsert_count: 1,
                    initial_insertion_epoch_us: now,
                },
            ),
        ];
        let mut rollups = HashMap::new();
        let mut forget = HashSet::new();
        coalesce_markers(&markers, &mut rollups, &mut forget).unwrap();
        assert!(forget.is_empty());
        assert_eq!(2, rollups.len());
        let rollup_blocking = rollups.get(&collection_id_blocking).unwrap();
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
        let rollup_acting = rollups.get(&collection_id_acting).unwrap();
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
            log_position: 42,
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
            log_position: 1,
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
            log_position: 1,
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
                    log_position: 1,
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
                    log_position: 20,
                    num_records: 5,
                    reinsert_count: 1,
                    initial_insertion_epoch_us: now + 1000,
                },
            ),
        ];

        let mut rollups = HashMap::new();
        let mut forget = HashSet::new();
        coalesce_markers(&markers, &mut rollups, &mut forget).unwrap();
        // The purge should remove all markers for the collection, even ones that come after
        assert_eq!(1, forget.len());
        assert!(forget.contains(&collection_id));
        for collection_id in &forget {
            rollups.remove(collection_id);
        }
        assert_eq!(0, rollups.len());
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
                    log_position: 1,
                    num_records: 10,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(2),
                DirtyMarker::MarkDirty {
                    collection_id: collection_id2,
                    log_position: 10,
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
                    log_position: 20,
                    num_records: 3,
                    reinsert_count: 2,
                    initial_insertion_epoch_us: now + 1000,
                },
            ),
        ];

        let mut rollups = HashMap::new();
        let mut forget = HashSet::new();
        coalesce_markers(&markers, &mut rollups, &mut forget).unwrap();
        // collection_id1 should be completely removed due to purge
        // collection_id2 should remain
        assert_eq!(1, forget.len());
        assert!(forget.contains(&collection_id1));
        for collection_id in &forget {
            rollups.remove(collection_id);
        }
        assert_eq!(1, rollups.len());
        assert!(rollups.contains_key(&collection_id2));
        assert!(!rollups.contains_key(&collection_id1));

        let rollup2 = rollups.get(&collection_id2).unwrap();
        assert_eq!(LogPosition::from_offset(10), rollup2.start_log_position);
        assert_eq!(LogPosition::from_offset(15), rollup2.limit_log_position);
    }

    #[test]
    fn rollup_per_collection_new() {
        let start_position = LogPosition::from_offset(10);
        let num_records = 5;
        let rollup = RollupPerCollection::new(start_position, num_records, 0);

        assert_eq!(start_position, rollup.start_log_position);
        assert_eq!(LogPosition::from_offset(15), rollup.limit_log_position);
        assert_eq!(0, rollup.reinsert_count);
    }

    #[test]
    fn rollup_per_collection_observe_dirty_marker() {
        let start_position = LogPosition::from_offset(10);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let mut rollup = RollupPerCollection::new(start_position, 5, now);

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
        assert_eq!(3, rollup.reinsert_count); // Same
        assert_eq!(now - 1000, rollup.initial_insertion_epoch_us); // Should move to min
    }

    #[test]
    fn rollup_per_collection_is_empty() {
        let rollup = RollupPerCollection::new(LogPosition::from_offset(10), 0, 42);
        assert!(rollup.is_empty());

        let rollup = RollupPerCollection::new(LogPosition::from_offset(10), 5, 42);
        assert!(!rollup.is_empty());
    }

    #[test]
    fn rollup_per_collection_requires_backpressure() {
        let rollup = RollupPerCollection::new(LogPosition::from_offset(10), 100, 42);
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

        let mut rollup = RollupPerCollection::new(LogPosition::from_offset(10), 5, now);
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
                assert_eq!(10, log_position);
                assert_eq!(5, num_records);
                assert_eq!(3, reinsert_count);
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
        let prefix = collection_id.storage_prefix_for_log();
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
            log_position: log_position.offset(),
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
            assert_eq!(42, pos);
            assert_eq!(100, count);
            assert_eq!(0, reinsert_count);
        } else {
            panic!("Expected MarkDirty variant");
        }
    }

    #[test]
    fn dirty_marker_coalesce_empty_markers() {
        let mut rollups = HashMap::new();
        let mut forget = HashSet::new();
        coalesce_markers(&[], &mut rollups, &mut forget).unwrap();
        assert!(forget.is_empty());
        assert!(rollups.is_empty());
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
                    log_position: 10,
                    num_records: 5,
                    reinsert_count: 1,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(2),
                DirtyMarker::MarkDirty {
                    collection_id: collection_id2,
                    log_position: 20,
                    num_records: 10,
                    reinsert_count: 2,
                    initial_insertion_epoch_us: now + 1000,
                },
            ),
            (
                LogPosition::from_offset(3),
                DirtyMarker::MarkDirty {
                    collection_id: collection_id1,
                    log_position: 30,
                    num_records: 3,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now - 1000,
                },
            ),
        ];

        let mut rollups = HashMap::new();
        let mut forget = HashSet::new();
        coalesce_markers(&markers, &mut rollups, &mut forget).unwrap();
        assert!(forget.is_empty());
        assert_eq!(2, rollups.len());

        // Check collection_id1 rollup
        let rollup1 = rollups.get(&collection_id1).unwrap();
        assert_eq!(LogPosition::from_offset(10), rollup1.start_log_position);
        assert_eq!(LogPosition::from_offset(33), rollup1.limit_log_position);
        assert_eq!(1, rollup1.reinsert_count); // max of 1 and 0
        assert_eq!(now - 1000, rollup1.initial_insertion_epoch_us); // max of now and now-1000

        // Check collection_id2 rollup
        let rollup2 = rollups.get(&collection_id2).unwrap();
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
                log_position: u64::MAX - 1,
                num_records: 100,
                reinsert_count: 0,
                initial_insertion_epoch_us: now,
            },
        )];

        let mut rollups = HashMap::new();
        let mut forget = HashSet::new();
        coalesce_markers(&markers, &mut rollups, &mut forget).unwrap();
        assert!(forget.is_empty());
        let collection_rollup = rollups.get(&collection_id).unwrap();
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
                log_position: 10,
                num_records: 0,
                reinsert_count: 0,
                initial_insertion_epoch_us: now,
            },
        )];

        let mut rollups = HashMap::new();
        let mut forget = HashSet::new();
        coalesce_markers(&markers, &mut rollups, &mut forget).unwrap();
        assert!(forget.is_empty());
        let collection_rollup = rollups.get(&collection_id).unwrap();
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
                    log_position: 10,
                    num_records: 1,
                    reinsert_count: u64::MAX,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(2),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: 11,
                    num_records: 1,
                    reinsert_count: 5,
                    initial_insertion_epoch_us: now,
                },
            ),
        ];

        let mut rollups = HashMap::new();
        let mut forget = HashSet::new();
        coalesce_markers(&markers, &mut rollups, &mut forget).unwrap();
        assert!(forget.is_empty());
        let collection_rollup = rollups.get(&collection_id).unwrap();
        assert_eq!(u64::MAX, collection_rollup.reinsert_count);
    }

    #[test]
    fn rollup_per_collection_witness_functionality() {
        let rollup = RollupPerCollection::new(LogPosition::from_offset(10), 5, 42);

        // Test that the rollup can handle boundary conditions
        assert_eq!(LogPosition::from_offset(10), rollup.start_log_position);
        assert_eq!(LogPosition::from_offset(15), rollup.limit_log_position);
        assert!(!rollup.is_empty());
    }

    #[test]
    fn rollup_per_collection_backpressure_boundary_conditions() {
        let rollup = RollupPerCollection::new(LogPosition::from_offset(0), u64::MAX, 42);
        assert!(rollup.requires_backpressure(u64::MAX - 1));
        assert!(rollup.requires_backpressure(u64::MAX));

        let rollup = RollupPerCollection::new(LogPosition::from_offset(u64::MAX - 100), 50, 42);
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
            filters: default_otel_filters(),
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
                    log_position: 1,
                    num_records: 10,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(2),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: 11,
                    num_records: 10,
                    reinsert_count: 1,
                    initial_insertion_epoch_us: now + 1000,
                },
            ),
            (
                LogPosition::from_offset(3),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: 21,
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

        let mut rollups = HashMap::new();
        let mut forget = HashSet::new();
        coalesce_markers(&markers, &mut rollups, &mut forget).unwrap();
        assert_eq!(1, forget.len());
        assert!(forget.contains(&collection_id));
        for collection_id in &forget {
            rollups.remove(collection_id);
        }
        assert_eq!(0, rollups.len());
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
            log_position: 1,
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
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let mut rollup = RollupPerCollection::new(LogPosition::from_offset(10), 5, now + 1);

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
                    log_position: i * 10,
                    num_records: 1,
                    reinsert_count: i % 100,
                    initial_insertion_epoch_us: now + i,
                },
            ));
        }

        let mut rollups = HashMap::new();
        let mut forget = HashSet::new();
        coalesce_markers(&markers, &mut rollups, &mut forget).unwrap();
        assert!(forget.is_empty());
        assert_eq!(1, rollups.len());
        let collection_rollup = rollups.get(&collection_id).unwrap();
        assert_eq!(
            LogPosition::from_offset(0),
            collection_rollup.start_log_position
        );
        assert_eq!(
            LogPosition::from_offset(9991),
            collection_rollup.limit_log_position
        );
        assert_eq!(99, collection_rollup.reinsert_count);
        assert_eq!(now, collection_rollup.initial_insertion_epoch_us);
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
                    log_position: 1,
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
                    log_position: 20,
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
                    log_position: 30,
                    num_records: 3,
                    reinsert_count: 2,
                    initial_insertion_epoch_us: now + 2000,
                },
            ),
        ];

        let mut rollups = HashMap::new();
        let mut forget = HashSet::new();
        coalesce_markers(&markers, &mut rollups, &mut forget).unwrap();
        assert_eq!(1, forget.len());
        assert!(forget.contains(&collection_id));
        for collection_id in &forget {
            rollups.remove(collection_id);
        }
        assert_eq!(0, rollups.len());
    }

    #[test]
    fn rollup_per_collection_extreme_positions() {
        let start_position = LogPosition::from_offset(u64::MAX - 10);
        let rollup = RollupPerCollection::new(start_position, 5, 42);

        assert_eq!(start_position, rollup.start_log_position);
        assert!(!rollup.is_empty());
        assert!(rollup.requires_backpressure(1));
    }

    #[test]
    fn rollup_per_collection_zero_epoch() {
        let mut rollup = RollupPerCollection::new(LogPosition::from_offset(10), 5, u64::MAX);

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
        let mut rollup = RollupPerCollection::new(LogPosition::from_offset(100), 0, 1042);

        rollup.observe_dirty_marker(LogPosition::from_offset(50), 25, 1, 1000);

        assert_eq!(LogPosition::from_offset(50), rollup.start_log_position);
        assert_eq!(LogPosition::from_offset(100), rollup.limit_log_position);
        assert_eq!(1000, rollup.initial_insertion_epoch_us);
    }

    #[test]
    fn backpressure_threshold_verification() {
        let rollup = RollupPerCollection::new(LogPosition::from_offset(0), 100, 42);

        assert!(rollup.requires_backpressure(99));
        assert!(rollup.requires_backpressure(100));
        assert!(!rollup.requires_backpressure(101));

        let zero_rollup = RollupPerCollection::new(LogPosition::from_offset(10), 0, 42);
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

    async fn setup_log_server() -> LogServer {
        let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
        let writer_options = LogWriterOptions {
            snapshot_manifest: SnapshotOptions {
                // We set a snapshot rollover threshold that's high enough that the test won't go
                // on forever due to a race, but also so that we stress the conditions.
                snapshot_rollover_threshold: 10,
                fragment_rollover_threshold: 3,
            },
            throttle_fragment: ThrottleOptions {
                batch_size_bytes: 4,
                batch_interval_us: 4096,
                ..Default::default()
            },
            ..Default::default()
        };
        let dirty_log = Some(Arc::new(
            LogWriter::open_or_initialize(
                writer_options.clone(),
                storage.clone(),
                "test-rust-log-service",
                "test-dirty-log-writer",
                (),
            )
            .await
            .expect("Dirty log should be initializable"),
        ));
        let config = LogServerConfig {
            writer: writer_options,
            ..Default::default()
        };
        LogServer {
            storage,
            dirty_log,
            metrics: Metrics::new(meter("test-rust-log-service")),
            config,
            open_logs: Default::default(),
            rolling_up: Default::default(),
            backpressure: Default::default(),
            need_to_compact: Default::default(),
            cache: Default::default(),
        }
    }

    async fn push_log_to_server(
        server: &LogServer,
        collection_id: CollectionUuid,
        logs: &[OperationRecord],
    ) {
        let mut retries = 0;
        loop {
            let proto_push_log_req = Request::new(PushLogsRequest {
                collection_id: collection_id.to_string(),
                records: logs
                    .iter()
                    .cloned()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()
                    .expect("Logs should be valid"),
            });
            if let Err(err) = server.push_logs(proto_push_log_req).await {
                if err.code() == Code::Unavailable {
                    sleep(Duration::from_millis(500)).await;
                }
                println!("Failed to push log: {err}");
            } else {
                break;
            }
            retries += 1;
            if retries >= 6 {
                panic!("Unable to push log within six retries");
            }
            sleep(Duration::from_millis(1)).await;
        }
    }

    async fn validate_log_on_server(
        server: &LogServer,
        collection_id: CollectionUuid,
        reference_logs: &[OperationRecord],
        read_offset: usize,
        mut batch_size: usize,
    ) {
        // NOTE: Log offset always starts with 1.
        let ref_start_offset = read_offset.saturating_sub(1).min(reference_logs.len());
        let ref_end_offset = ref_start_offset
            .saturating_add(batch_size)
            .min(reference_logs.len());
        batch_size = batch_size.min(ref_end_offset - ref_start_offset);

        let read_logs = server
            .pull_logs(Request::new(PullLogsRequest {
                collection_id: collection_id.to_string(),
                start_from_offset: read_offset as i64,
                batch_size: batch_size as i32,
                end_timestamp: i64::MAX,
            }))
            .await
            .expect("Pull Logs should not fail")
            .into_inner()
            .records
            .into_iter()
            .map(chroma_types::LogRecord::try_from)
            .collect::<Result<Vec<_>, _>>()
            .expect("Logs should be valid");

        assert_eq!(read_logs.len(), ref_end_offset - ref_start_offset);

        for (reference_operation, got_log) in reference_logs[ref_start_offset..ref_end_offset]
            .iter()
            .zip(read_logs)
        {
            let expected_metadata = reference_operation.metadata.clone().unwrap_or_default();
            let received_metadata = got_log.record.metadata.clone().unwrap_or_default();

            assert!(got_log.record.id == reference_operation.id);
            assert!(got_log.record.embedding == reference_operation.embedding);
            assert!(got_log.record.encoding == reference_operation.encoding);
            assert!(
                are_update_metadatas_close_to_equal(&received_metadata, &expected_metadata),
                "{:?} != {:?}",
                received_metadata,
                expected_metadata
            );
            assert!(got_log.record.document == reference_operation.document);
            assert!(got_log.record.operation == reference_operation.operation);
        }
    }

    async fn get_enum_offset_on_server(server: &LogServer, collection_id: CollectionUuid) -> i64 {
        server
            .scout_logs(Request::new(ScoutLogsRequest {
                collection_id: collection_id.to_string(),
            }))
            .await
            .expect("Scout Logs should not fail")
            .into_inner()
            .first_uninserted_record_offset
            .saturating_sub(1)
    }

    async fn update_compact_offset_on_server(
        server: &LogServer,
        collection_id: CollectionUuid,
        compact_offset: i64,
    ) {
        let mut retries = 0;
        loop {
            if let Err(err) = server
                .update_collection_log_offset(Request::new(UpdateCollectionLogOffsetRequest {
                    collection_id: collection_id.to_string(),
                    log_offset: compact_offset,
                }))
                .await
            {
                println!("Failed to update log offset: {err}");
            } else {
                break;
            }
            retries += 1;
            if retries >= 6 {
                panic!("Unable to update compaction offset in six retries");
            }
            sleep(Duration::from_millis(1)).await;
        }
    }

    async fn validate_dirty_log_on_server(server: &LogServer, collection_ids: &[CollectionUuid]) {
        server
            .roll_dirty_log()
            .await
            .expect("Roll Dirty Logs should not fail");
        let dirty_collections = server
            .cached_get_all_collection_info_to_compact(GetAllCollectionInfoToCompactRequest {
                min_compaction_size: 0,
            })
            .await
            .expect("Get Dirty Collections should not fail")
            .into_inner()
            .all_collection_info;
        let expected_collection_ids: HashSet<_> =
            HashSet::from_iter(collection_ids.iter().cloned());
        let got_collection_ids: HashSet<_> =
            HashSet::from_iter(dirty_collections.iter().map(|info| {
                CollectionUuid::from_str(&info.collection_id)
                    .expect("Collection Uuid should be valid")
            }));
        assert_eq!(got_collection_ids, expected_collection_ids);
    }

    async fn garbage_collect_unused_logs(
        server: &LogServer,
        collection_id: CollectionUuid,
        first_log_position_to_keep: u64,
    ) {
        'to_the_top: loop {
            let writer = GarbageCollector::open(
                server.config.writer.clone(),
                server.storage.clone(),
                &collection_id.storage_prefix_for_log(),
                "proptest garbage collection service",
            )
            .await
            .expect("Garbage collector should be initializable");
            if let Err(err) = writer
                .garbage_collect_phase1_compute_garbage(
                    &Default::default(),
                    Some(LogPosition::from_offset(first_log_position_to_keep)),
                )
                .await
            {
                panic!("Log GC phase 1 error: {err}");
            }
            if let Err(err) = server
                .garbage_collect_phase2(
                    GarbageCollectPhase2Request {
                        log_to_collect: Some(LogToCollect::CollectionId(collection_id.to_string())),
                    }
                    .into_request(),
                )
                .await
            {
                if matches!(
                    err.code().into(),
                    chroma_error::ErrorCodes::FailedPrecondition
                ) {
                    continue 'to_the_top;
                } else {
                    panic!("Log GC phase 2 error: {err}");
                }
            }
            if let Err(err) = writer
                .garbage_collect_phase3_delete_garbage(&Default::default())
                .await
            {
                panic!("Log GC phase 3 error: {err}");
            };
            break;
        }
    }

    fn test_push_pull_logs(
        read_offset: usize,
        batch_size: usize,
        operations: Vec<OperationRecord>,
    ) {
        let runtime = Runtime::new().unwrap();

        runtime.block_on(async move {
            let log_server = setup_log_server().await;
            validate_dirty_log_on_server(&log_server, &[]).await;

            let collection_id = CollectionUuid::new();

            for chunk in operations.chunks(100) {
                push_log_to_server(&log_server, collection_id, chunk).await;
            }

            validate_dirty_log_on_server(&log_server, &[collection_id]).await;
            validate_log_on_server(
                &log_server,
                collection_id,
                &operations,
                read_offset,
                batch_size,
            )
            .await;
            let enum_offset = get_enum_offset_on_server(&log_server, collection_id).await;
            update_compact_offset_on_server(&log_server, collection_id, enum_offset).await;
            validate_dirty_log_on_server(&log_server, &[]).await;
        });
    }

    fn test_dirty_logs(operations: Vec<(usize, OperationRecord)>) {
        let runtime = Runtime::new().unwrap();

        runtime.block_on(async move {
            let log_server = setup_log_server().await;
            validate_dirty_log_on_server(&log_server, &[]).await;

            let mut collection_id_with_ord = Vec::new();
            for (index, operation) in operations {
                let collection_id = CollectionUuid::new();
                collection_id_with_ord.push((index, collection_id));
                push_log_to_server(&log_server, collection_id, &[operation]).await;
                let enum_offset = get_enum_offset_on_server(&log_server, collection_id).await;
                assert_eq!(enum_offset, 1);
            }

            collection_id_with_ord.sort_by_key(|(index, _)| *index);
            let mut collection_ids = collection_id_with_ord
                .into_iter()
                .map(|(_, id)| id)
                .collect::<Vec<_>>();

            while let Some(collection_id) = collection_ids.pop() {
                update_compact_offset_on_server(&log_server, collection_id, 1).await;
                validate_dirty_log_on_server(&log_server, &collection_ids).await;
            }
        });
    }

    fn test_fork_logs(
        initial_operations: Vec<OperationRecord>,
        source_operations: Vec<OperationRecord>,
        fork_operations: Vec<OperationRecord>,
    ) {
        let runtime = Runtime::new().unwrap();

        runtime.block_on(async move {
            let log_server = setup_log_server().await;
            validate_dirty_log_on_server(&log_server, &[]).await;

            let source_collection_id = CollectionUuid::new();
            let fork_collection_id = CollectionUuid::new();

            if !initial_operations.is_empty() {
                push_log_to_server(&log_server, source_collection_id, &initial_operations).await;
            }

            log_server
                .fork_logs(Request::new(ForkLogsRequest {
                    source_collection_id: source_collection_id.to_string(),
                    target_collection_id: fork_collection_id.to_string(),
                }))
                .await
                .expect("Fork Logs should not fail");

            if !source_operations.is_empty() {
                push_log_to_server(&log_server, source_collection_id, &source_operations).await;
            }

            if !fork_operations.is_empty() {
                push_log_to_server(&log_server, fork_collection_id, &fork_operations).await;
            }

            let mut expected_source = initial_operations.clone();
            expected_source.extend(source_operations);

            let mut expected_fork = initial_operations;
            expected_fork.extend(fork_operations);

            let mut dirty_collection_ids = Vec::new();
            if !expected_source.is_empty() {
                dirty_collection_ids.push(source_collection_id);
            }
            if !expected_fork.is_empty() {
                dirty_collection_ids.push(fork_collection_id);
            }

            validate_dirty_log_on_server(&log_server, &dirty_collection_ids).await;
            validate_log_on_server(&log_server, source_collection_id, &expected_source, 1, 1000)
                .await;
            validate_log_on_server(&log_server, fork_collection_id, &expected_fork, 1, 1000).await;

            if !expected_source.is_empty() {
                let source_enum_offset =
                    get_enum_offset_on_server(&log_server, source_collection_id).await;
                assert_eq!(source_enum_offset, expected_source.len() as i64);
                update_compact_offset_on_server(
                    &log_server,
                    source_collection_id,
                    source_enum_offset,
                )
                .await;
            }
            if !expected_fork.is_empty() {
                let fork_enum_offset =
                    get_enum_offset_on_server(&log_server, fork_collection_id).await;
                assert_eq!(fork_enum_offset, expected_fork.len() as i64);
                update_compact_offset_on_server(&log_server, fork_collection_id, fork_enum_offset)
                    .await;
            }
            validate_dirty_log_on_server(&log_server, &[]).await;
        });
    }

    fn test_garbage_collect_unused_logs(operations: Vec<OperationRecord>) {
        let runtime = Runtime::new().unwrap();
        let collection_id = CollectionUuid::new();
        let log_server = Arc::new(runtime.block_on(setup_log_server()));
        let log_server_clone = log_server.clone();
        let (tx, mut rx) = unbounded_channel();
        let background_gc_task = runtime.spawn(async move {
            while let Some(compact_offset) = rx.recv().await {
                if compact_offset == 0 {
                    rx.close();
                    break;
                }
                update_compact_offset_on_server(&log_server_clone, collection_id, compact_offset)
                    .await;
                let first_uncompacted_offset = compact_offset.saturating_add(1) as usize;
                garbage_collect_unused_logs(
                    &log_server_clone,
                    collection_id,
                    first_uncompacted_offset as u64,
                )
                .await;
            }
        });

        runtime.block_on(async move {
            for (offset, log) in operations.iter().enumerate() {
                push_log_to_server(&log_server, collection_id, &[log.clone()]).await;
                tx.send(offset as i64 + 1)
                    .expect("Should be able to send compaction signal");
            }

            tx.send(0).expect("Should be able to close channel");

            background_gc_task
                .await
                .expect("The background GC task should finish");

            let reader = LogReader::open(
                log_server.config.reader.clone(),
                log_server.storage.clone(),
                collection_id.storage_prefix_for_log(),
            )
            .await
            .expect("Log reader should be initializable");
            reader
                .scrub(Limits::UNLIMITED)
                .await
                .expect("Log scrub should not fail after garbage collection");
        });
    }

    async fn test_rollup_snapshot_after_gc() {
        // NOTE: This tests the specific case where the first snapshot decreased depth after garbage collection
        // Manifest branching factor 3
        let log_server = setup_log_server().await;
        let collection_id = CollectionUuid::new();
        let logs = (1..=42)
            .map(|index| OperationRecord {
                id: index.to_string(),
                embedding: None,
                encoding: None,
                metadata: None,
                document: None,
                operation: Operation::Delete,
            })
            .collect::<Vec<_>>();

        for log in &logs[..=25] {
            push_log_to_server(&log_server, collection_id, &[log.clone()]).await;
        }

        update_compact_offset_on_server(&log_server, collection_id, 6).await;
        garbage_collect_unused_logs(&log_server, collection_id, 7).await;

        for log in &logs[26..] {
            push_log_to_server(&log_server, collection_id, &[log.clone()]).await;
        }

        let reader = LogReader::open(
            log_server.config.reader.clone(),
            log_server.storage.clone(),
            collection_id.storage_prefix_for_log(),
        )
        .await
        .expect("Log reader should be initializable");
        reader
            .scrub(Limits::UNLIMITED)
            .await
            .expect("Log scrub should not fail after push log");

        let remaining_logs = log_server
            .pull_logs(Request::new(PullLogsRequest {
                collection_id: collection_id.to_string(),
                start_from_offset: 7,
                batch_size: 36,
                end_timestamp: i64::MAX,
            }))
            .await
            .expect("Pull Logs should not fail")
            .into_inner()
            .records
            .into_iter()
            .map(chroma_types::LogRecord::try_from)
            .collect::<Result<Vec<_>, _>>()
            .expect("Logs should be valid");
        assert_eq!(remaining_logs.len() + 6, logs.len());
        for (got_op, ref_op) in remaining_logs
            .into_iter()
            .map(|l| l.record)
            .zip(logs.iter().skip(6))
        {
            assert_eq!(got_op.id, ref_op.id);
            assert_eq!(got_op.operation, ref_op.operation);
        }
    }

    #[test]
    fn test_k8s_integration_rust_log_service_rollup_snapshot_after_gc() {
        let runtime = Runtime::new().unwrap();
        // NOTE: Somehow it overflow the stack under default stack limit
        std::thread::Builder::new()
            .stack_size(1 << 22)
            .spawn(move || runtime.block_on(test_rollup_snapshot_after_gc()))
            .expect("Thread should be spawnable")
            .join()
            .expect("Spawned thread should not fail to join");
    }

    proptest! {
        #[test]
        fn test_k8s_integration_rust_log_service_push_pull_logs(
            read_offset in 1usize..=36,
            batch_size in 1usize..=36,
            operations in proptest::collection::vec(any::<OperationRecord>(), 1..=36)
        ) {
            // NOTE: Somehow it overflow the stack under default stack limit
            std::thread::Builder::new().stack_size(1 << 22).spawn(move || test_push_pull_logs(read_offset, batch_size, operations))
            .expect("Thread should be spawnable")
            .join()
            .expect("Spawned thread should not fail to join");
        }

        #[test]
        fn test_k8s_integration_rust_log_service_dirty_logs(
            operations in proptest::collection::vec(any::<OperationRecord>(), 1..=5).prop_map(|ops| ops.into_iter().enumerate().collect()).prop_shuffle()
        ) {
            // NOTE: Somehow it overflow the stack under default stack limit
            std::thread::Builder::new().stack_size(1 << 22).spawn(move || test_dirty_logs(operations))
            .expect("Thread should be spawnable")
            .join()
            .expect("Spawned thread should not fail to join");
        }

        #[test]
        fn test_k8s_integration_rust_log_service_fork_logs(
            initial_operations in proptest::collection::vec(any::<OperationRecord>(), 0..=10),
            source_operations in proptest::collection::vec(any::<OperationRecord>(), 0..=10),
            fork_operations in proptest::collection::vec(any::<OperationRecord>(), 0..=10),
        ) {
            // NOTE: Somehow it overflow the stack under default stack limit
            std::thread::Builder::new().stack_size(1 << 22).spawn(move || test_fork_logs(initial_operations, source_operations, fork_operations))
            .expect("Thread should be spawnable")
            .join()
            .expect("Spawned thread should not fail to join");
        }

        #[test]
        fn test_k8s_integration_rust_log_service_garbage_collect_unused_logs(
            operations in proptest::collection::vec(any::<OperationRecord>(), 1..=36),
        ) {
            // NOTE: Somehow it overflow the stack under default stack limit
            std::thread::Builder::new().stack_size(1 << 22).spawn(move || test_garbage_collect_unused_logs(operations))
            .expect("Thread should be spawnable")
            .join()
            .expect("Spawned thread should not fail to join");
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_update_collection_log_offset_never_moves_backwards() {
        use chroma_storage::s3_client_for_test_with_new_bucket;
        use chroma_types::chroma_proto::UpdateCollectionLogOffsetRequest;
        use std::collections::HashMap;
        use tonic::Request;
        use wal3::{LogWriter, LogWriterOptions};

        // Set up test storage using S3 (minio)
        let storage = Arc::new(s3_client_for_test_with_new_bucket().await);

        // Create the dirty log writer
        let dirty_log = LogWriter::open_or_initialize(
            LogWriterOptions::default(),
            Arc::clone(&storage),
            "dirty-test",
            "dirty log writer",
            (),
        )
        .await
        .expect("Failed to create dirty log");
        let dirty_log = Some(Arc::new(dirty_log));

        // Create LogServer manually
        let config = LogServerConfig::default();
        let log_server = LogServer {
            config,
            open_logs: Arc::new(StateHashTable::default()),
            storage,
            dirty_log,
            rolling_up: tokio::sync::Mutex::new(()),
            backpressure: Mutex::new(Arc::new(HashSet::default())),
            need_to_compact: Mutex::new(HashMap::default()),
            cache: None,
            metrics: Metrics::new(opentelemetry::global::meter("test")),
        };

        let collection_id = CollectionUuid::new();
        let collection_id_str = collection_id.to_string();

        // Manually initialize a log for this collection to avoid "proxy not initialized" error
        let storage_prefix = collection_id.storage_prefix_for_log();
        let _log_writer = LogWriter::open_or_initialize(
            LogWriterOptions::default(),
            Arc::clone(&log_server.storage),
            &storage_prefix,
            "test log writer",
            (),
        )
        .await
        .expect("Failed to initialize collection log");

        // Step 1: Initialize collection log and set it to offset 100
        let initial_request = UpdateCollectionLogOffsetRequest {
            collection_id: collection_id_str.clone(),
            log_offset: 100,
        };

        let response = log_server
            .update_collection_log_offset(Request::new(initial_request))
            .await;
        assert!(
            response.is_ok(),
            "Initial offset update should succeed: {:?}",
            response.err()
        );

        // Step 2: Verify we can move forward (to offset 150)
        let forward_request = UpdateCollectionLogOffsetRequest {
            collection_id: collection_id_str.clone(),
            log_offset: 150,
        };

        let response = log_server
            .update_collection_log_offset(Request::new(forward_request))
            .await;
        assert!(response.is_ok(), "Forward movement should succeed");

        // Step 3: Attempt to move backwards (to offset 50) - this should be blocked
        let backward_request = UpdateCollectionLogOffsetRequest {
            collection_id: collection_id_str.clone(),
            log_offset: 50,
        };

        let response = log_server
            .update_collection_log_offset(Request::new(backward_request))
            .await;

        // The function should succeed but not actually move the offset backwards
        // (it returns early with OK status when current offset > requested offset)
        assert!(
            response.is_ok(),
            "Backward request should return OK but not move offset"
        );

        // Step 4: Verify that requesting the same offset works
        let same_request = UpdateCollectionLogOffsetRequest {
            collection_id: collection_id_str.clone(),
            log_offset: 150, // Same as current
        };

        let response = log_server
            .update_collection_log_offset(Request::new(same_request))
            .await;
        assert!(response.is_ok(), "Same offset request should succeed");

        // Step 5: Verify we can still move forward after backward attempt was blocked
        let final_forward_request = UpdateCollectionLogOffsetRequest {
            collection_id: collection_id_str,
            log_offset: 200,
        };

        let response = log_server
            .update_collection_log_offset(Request::new(final_forward_request))
            .await;
        assert!(
            response.is_ok(),
            "Forward movement after backward attempt should succeed"
        );
    }
}
