use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_storage::config::StorageConfig;
use chroma_storage::Storage;
use chroma_types::chroma_proto::{
    log_service_server::LogService, CollectionInfo, GetAllCollectionInfoToCompactRequest,
    GetAllCollectionInfoToCompactResponse, LogRecord, OperationRecord, PullLogsRequest,
    PullLogsResponse, PurgeDirtyForCollectionRequest, PurgeDirtyForCollectionResponse,
    PushLogsRequest, PushLogsResponse, UpdateCollectionLogOffsetRequest,
    UpdateCollectionLogOffsetResponse,
};
use chroma_types::CollectionUuid;
use figment::providers::{Env, Format, Yaml};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;
use wal3::{
    CursorName, CursorStoreOptions, Limits, LogPosition, LogReader, LogReaderOptions, LogWriter,
    LogWriterOptions, Witness,
};

pub mod state_hash_table;

use crate::state_hash_table::StateHashTable;

///////////////////////////////////////////// constants ////////////////////////////////////////////

const DEFAULT_CONFIG_PATH: &str = "./chroma_config.yaml";

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

// SAFETY(rescrv):  There's a test that this produces a valid type.
static STABLE_PREFIX: CursorName = unsafe { CursorName::from_string_unchecked("stable_prefix") };

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
        let when = if keep_alive > Duration::ZERO {
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
    let mut active = handle.active.lock().await;
    active.keep_alive(Duration::from_secs(60));
    if let Some(log) = active.log.as_ref() {
        return Ok(LogRef {
            log: Arc::clone(log),
            _phantom: std::marker::PhantomData,
        });
    }
    tracing::info!("Opening log at {}", prefix);
    let opened = LogWriter::open_or_initialize(
        options.clone(),
        Arc::clone(storage),
        prefix,
        // TODO(rescrv):  Configurable params.
        "log writer",
        mark_dirty.clone(),
    )
    .await?;
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

////////////////////////////////////////////// Rollup //////////////////////////////////////////////

#[derive(Debug, Default)]
struct Rollup {
    advance_to: LogPosition,
    reinsert: Vec<DirtyMarker>,
    compactable: Vec<CollectionInfo>,
}

////////////////////////////////////// ContiguouslyDirtyRange //////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ContiguouslyDirtyRange {
    start: LogPosition,
    num_records: u64,
    reinsert_count: u64,
    initial_insertion_epoch_us: u64,
}

impl ContiguouslyDirtyRange {
    fn new(
        start: LogPosition,
        num_records: u64,
        initial_insertion_epoch_us: u64,
    ) -> Result<Self, wal3::Error> {
        Ok(Self {
            start,
            num_records,
            reinsert_count: 0,
            initial_insertion_epoch_us,
        })
    }

    fn coalesce(dirty: &mut Vec<ContiguouslyDirtyRange>) {
        dirty.sort_by_key(|range| range.start);
        if dirty.len() < 2 {
            return;
        }
        let mut i = 0;
        while i + 1 < dirty.len() {
            if dirty[i].start + dirty[i].num_records == dirty[i + 1].start {
                dirty[i].num_records += dirty[i + 1].num_records;
                dirty[i].reinsert_count =
                    std::cmp::max(dirty[i].reinsert_count, dirty[i + 1].reinsert_count);
                dirty[i].initial_insertion_epoch_us = std::cmp::min(
                    dirty[i].initial_insertion_epoch_us,
                    dirty[i + 1].initial_insertion_epoch_us,
                );
                dirty.remove(i + 1);
            } else {
                i += 1;
            }
        }
    }

    fn num_records_after(&self, start: LogPosition) -> u64 {
        if self.start + self.num_records <= start {
            0
        } else if self.start < start {
            self.num_records - (start - self.start)
        } else {
            self.num_records
        }
    }
}

//////////////////////////////////////// RollupPerCollection ///////////////////////////////////////

#[derive(Debug)]
struct RollupPerCollection {
    dirty: Vec<ContiguouslyDirtyRange>,
    collected_up_to: LogPosition,
    forgettable: bool,
}

impl RollupPerCollection {
    fn new() -> Self {
        Self {
            dirty: vec![],
            collected_up_to: LogPosition::MIN,
            forgettable: false,
        }
    }
}

//////////////////////////////////////////// DirtyMarker ///////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DirtyMarker {
    MarkDirty {
        collection_id: CollectionUuid,
        log_position: LogPosition,
        num_records: u64,
        reinsert_count: u64,
        initial_insertion_epoch_us: u64,
    },
    MarkCollected {
        collection_id: CollectionUuid,
        log_position: LogPosition,
    },
    MarkForgettable {
        collection_id: CollectionUuid,
    },
}

impl DirtyMarker {
    /// The collection ID for a given dirty marker.
    pub fn collection_id(&self) -> CollectionUuid {
        match self {
            DirtyMarker::MarkDirty { collection_id, .. } => *collection_id,
            DirtyMarker::MarkCollected { collection_id, .. } => *collection_id,
            DirtyMarker::MarkForgettable { collection_id, .. } => *collection_id,
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
            *reinsert_count += 1;
        }
    }

    /// Given a contiguous prefix of markers, process the log into a rollup.  That is, a set of
    /// markers to reinsert, a set of collections to compact, and an advance_to log position.
    fn rollup(
        markers: &[(LogPosition, DirtyMarker)],
        record_count_threshold: u64,
        reinsert_threshold: u64,
        timeout_us: u64,
    ) -> Result<Rollup, wal3::Error> {
        // NOTE(rescrv);  This is complicated code because it's a hard problem to do efficiently.
        // To cut complexity, I've chosen to do it in a way that is not the most efficient but is
        // readable and maintainable.  The most efficient way would be to do this in a single pass.
        // Someone better can do that if it's ever necessary.
        let per_collection = Self::coalesce_markers(markers)?;
        let mut reinsert = vec![];
        let mut compactable = vec![];
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| wal3::Error::Internal)?
            .as_micros() as u64;
        // First we process and reinsert the highest completion marker for each log.
        for (collection_id, rollup_pc) in per_collection.iter() {
            reinsert.push(DirtyMarker::MarkCollected {
                collection_id: *collection_id,
                log_position: rollup_pc.collected_up_to,
            });
        }
        // Now we process the MarkDirty variant.  It's been compressed in the per_collections
        // coalesce_markers call.
        for (collection_id, rollup_pc) in per_collection.iter() {
            let num_records = rollup_pc
                .dirty
                .iter()
                .map(|range| range.num_records_after(rollup_pc.collected_up_to))
                .sum::<u64>();
            let initial_insertion_epoch_us = rollup_pc
                .dirty
                .iter()
                .map(|range| range.initial_insertion_epoch_us)
                .max()
                .unwrap_or(now);
            let reinsert_count = rollup_pc
                .dirty
                .iter()
                .map(|range| range.reinsert_count)
                .max()
                .unwrap_or(0);
            let to_compact = num_records >= record_count_threshold
                || now - initial_insertion_epoch_us >= timeout_us
                || reinsert_count >= reinsert_threshold;
            if to_compact && !rollup_pc.forgettable {
                let first_log_offset = rollup_pc.collected_up_to.offset() as i64;
                let first_log_ts = first_log_offset;
                compactable.push(CollectionInfo {
                    collection_id: collection_id.to_string(),
                    first_log_offset,
                    first_log_ts,
                });
            } else if rollup_pc.forgettable {
                // intentionally drop the markers for this collection.
            } else {
                for dirty in rollup_pc.dirty.iter() {
                    let mut dirty = dirty.clone();
                    if dirty.start + dirty.num_records <= rollup_pc.collected_up_to {
                        continue;
                    } else if dirty.start < rollup_pc.collected_up_to {
                        dirty.num_records -= rollup_pc.collected_up_to - dirty.start;
                        dirty.start = rollup_pc.collected_up_to;
                    }
                    let mut marker = DirtyMarker::MarkDirty {
                        collection_id: *collection_id,
                        log_position: dirty.start,
                        num_records: dirty.num_records,
                        reinsert_count: dirty.reinsert_count,
                        initial_insertion_epoch_us: dirty.initial_insertion_epoch_us,
                    };
                    marker.reinsert();
                    reinsert.push(marker);
                }
            }
        }
        let mut advance_to = LogPosition::MIN;
        for (log_position, marker) in markers {
            if compactable
                .iter()
                .any(|c| c.collection_id == marker.collection_id().to_string())
            {
                break;
            }
            advance_to = std::cmp::max(advance_to, *log_position + 1u64);
        }
        Ok(Rollup {
            advance_to,
            reinsert,
            compactable,
        })
    }

    fn coalesce_markers(
        markers: &[(LogPosition, DirtyMarker)],
    ) -> Result<HashMap<CollectionUuid, RollupPerCollection>, wal3::Error> {
        let mut rollups = HashMap::new();
        for (_, marker) in markers {
            match marker {
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position,
                    num_records,
                    reinsert_count: _,
                    initial_insertion_epoch_us,
                } => {
                    let rollup_pc = match rollups.entry(*collection_id) {
                        Entry::Vacant(entry) => entry.insert(RollupPerCollection::new()),
                        Entry::Occupied(entry) => entry.into_mut(),
                    };
                    rollup_pc.dirty.push(ContiguouslyDirtyRange::new(
                        *log_position,
                        *num_records,
                        *initial_insertion_epoch_us,
                    )?);
                    ContiguouslyDirtyRange::coalesce(&mut rollup_pc.dirty);
                }
                DirtyMarker::MarkCollected {
                    collection_id,
                    log_position,
                } => {
                    let rollup_pc = match rollups.entry(*collection_id) {
                        Entry::Vacant(entry) => entry.insert(RollupPerCollection::new()),
                        Entry::Occupied(entry) => entry.into_mut(),
                    };
                    rollup_pc.collected_up_to =
                        std::cmp::max(rollup_pc.collected_up_to, *log_position);
                }
                DirtyMarker::MarkForgettable { collection_id } => {
                    let rollup_pc = match rollups.entry(*collection_id) {
                        Entry::Vacant(entry) => entry.insert(RollupPerCollection::new()),
                        Entry::Occupied(entry) => entry.into_mut(),
                    };
                    rollup_pc.forgettable = true;
                }
            }
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
}

#[async_trait::async_trait]
impl LogService for LogServer {
    #[tracing::instrument(skip(self, request), err(Display))]
    async fn push_logs(
        &self,
        request: Request<PushLogsRequest>,
    ) -> Result<Response<PushLogsResponse>, Status> {
        let push_logs = request.into_inner();
        let collection_id = Uuid::parse_str(&push_logs.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        tracing::info!("Pushing logs for collection {}", collection_id);
        if push_logs.records.len() > i32::MAX as usize {
            return Err(Status::invalid_argument("Too many records"));
        }
        if push_logs.records.is_empty() {
            return Err(Status::invalid_argument("Too few records"));
        }
        let prefix = storage_prefix_for_log(collection_id);
        let key = LogKey { collection_id };
        let handle = self.open_logs.get_or_create_state(key);
        let mark_dirty = MarkDirty {
            collection_id,
            dirty_log: Arc::clone(&self.dirty_log),
        };
        let log = get_log_from_handle(
            &handle,
            &self.config.writer,
            &self.storage,
            &prefix,
            mark_dirty,
        )
        .await
        // TODO(rescrv): better error handling.
        .map_err(|err| Status::unknown(err.to_string()))?;
        let mut messages = Vec::with_capacity(push_logs.records.len());
        for record in push_logs.records {
            let mut buf = vec![];
            record
                .encode(&mut buf)
                .map_err(|err| Status::unknown(err.to_string()))?;
            messages.push(buf);
        }
        let record_count = messages.len() as i32;
        log.append_many(messages)
            .await
            .map_err(|err| Status::unknown(err.to_string()))?;
        Ok(Response::new(PushLogsResponse { record_count }))
    }

    #[tracing::instrument(skip(self, request), err(Display))]
    async fn pull_logs(
        &self,
        request: Request<PullLogsRequest>,
    ) -> Result<Response<PullLogsResponse>, Status> {
        let pull_logs = request.into_inner();
        let collection_id = Uuid::parse_str(&pull_logs.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        tracing::info!(
            "Pulling logs for collection {} from offset {}",
            collection_id,
            pull_logs.start_from_offset
        );
        let prefix = storage_prefix_for_log(collection_id);
        let log_reader = LogReader::new(
            self.config.reader.clone(),
            Arc::clone(&self.storage),
            prefix,
        );
        let limits = Limits {
            max_files: Some(pull_logs.batch_size as u64),
            max_bytes: Some(pull_logs.batch_size as u64 * 32_768),
        };
        let fragments = match log_reader
            .scan(
                LogPosition::from_offset(pull_logs.start_from_offset as u64),
                limits,
            )
            .await
        {
            Ok(fragments) => fragments,
            Err(err) => {
                if let wal3::Error::UninitializedLog = err {
                    return Ok(Response::new(PullLogsResponse { records: vec![] }));
                } else {
                    return Err(Status::new(err.code().into(), err.to_string()));
                }
            }
        };
        let futures = fragments
            .iter()
            .map(|fragment| async { log_reader.fetch(fragment).await })
            .collect::<Vec<_>>();
        let parquets = futures::future::try_join_all(futures)
            .await
            .map_err(|err| Status::new(err.code().into(), err.to_string()))?;
        let mut records = Vec::with_capacity(pull_logs.batch_size as usize);
        for parquet in parquets {
            let this = parquet_to_records(parquet)?;
            for record in this {
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
        Ok(Response::new(PullLogsResponse { records }))
    }

    #[tracing::instrument(skip(self, request), err(Display))]
    async fn get_all_collection_info_to_compact(
        &self,
        request: Request<GetAllCollectionInfoToCompactRequest>,
    ) -> Result<Response<GetAllCollectionInfoToCompactResponse>, Status> {
        let request = request.into_inner();
        let Some(reader) = self.dirty_log.reader(LogReaderOptions::default()) else {
            return Err(Status::unavailable("Failed to get dirty log reader"));
        };
        let Some(cursors) = self.dirty_log.cursors(CursorStoreOptions::default()) else {
            return Err(Status::unavailable("Failed to get dirty log cursors"));
        };
        let witness = match cursors.load(&STABLE_PREFIX).await {
            Ok(Some(witness)) => witness,
            Ok(None) => Witness::init(),
            Err(err) => {
                return Err(Status::new(err.code().into(), err.to_string()));
            }
        };
        let dirty_fragments = reader
            .scan(
                witness.cursor().position,
                Limits {
                    max_files: Some(1_000_000),
                    max_bytes: Some(1_000_000_000),
                },
            )
            .await
            .map_err(|err| Status::new(err.code().into(), err.to_string()))?;
        if dirty_fragments.is_empty() {
            return Ok(Response::new(GetAllCollectionInfoToCompactResponse {
                all_collection_info: vec![],
            }));
        }
        if dirty_fragments.len() >= 500_000 {
            tracing::error!("Too many dirty fragments: {}", dirty_fragments.len());
        }
        if dirty_fragments.len() >= 1_000_000 {
            return Err(Status::resource_exhausted("Too many dirty fragments"));
        }
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
        let mut dirty_markers = vec![];
        for (_, records, _) in dirty_raw {
            let records = records
                .into_iter()
                .map(|x| {
                    let marker = serde_json::from_slice::<DirtyMarker>(&x.1)
                        .map_err(|err| Status::unavailable(err.to_string()))?;
                    Ok::<_, Status>((x.0, marker))
                })
                .collect::<Result<Vec<_>, _>>()?;
            dirty_markers.extend(records);
        }
        let rollup = DirtyMarker::rollup(
            &dirty_markers,
            std::cmp::min(
                self.config.record_count_threshold,
                request.min_compaction_size,
            ),
            self.config.reinsert_threshold,
            self.config.timeout_us,
        )
        .map_err(|err| Status::unavailable(err.to_string()))?;
        let reinsert_dirty_markers = rollup
            .reinsert
            .into_iter()
            .map(|marker| {
                serde_json::to_string(&marker)
                    .map(Vec::from)
                    .map_err(|err| Status::unavailable(err.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.dirty_log
            .append_many(reinsert_dirty_markers)
            .await
            .map_err(|err| Status::new(err.code().into(), err.to_string()))?;
        let mut new_cursor = witness.cursor().clone();
        new_cursor.position = rollup.advance_to;
        cursors
            .save(&STABLE_PREFIX, &new_cursor, &witness)
            .await
            .map_err(|err| Status::new(err.code().into(), err.to_string()))?;
        Ok(Response::new(GetAllCollectionInfoToCompactResponse {
            all_collection_info: rollup.compactable,
        }))
    }

    #[tracing::instrument(skip(self, request), err(Display))]
    async fn update_collection_log_offset(
        &self,
        request: Request<UpdateCollectionLogOffsetRequest>,
    ) -> Result<Response<UpdateCollectionLogOffsetResponse>, Status> {
        let request = request.into_inner();
        let collection_id = Uuid::parse_str(&request.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        let dirty_marker = DirtyMarker::MarkCollected {
            collection_id,
            log_position: LogPosition::from_offset(request.log_offset as u64),
        };
        let dirty_marker = serde_json::to_string(&dirty_marker)
            .map(Vec::from)
            .map_err(|err| Status::unavailable(err.to_string()))?;
        self.dirty_log
            .append(dirty_marker)
            .await
            .map_err(|err| Status::new(err.code().into(), err.to_string()))?;
        Ok(Response::new(UpdateCollectionLogOffsetResponse {}))
    }

    #[tracing::instrument(skip(self, request), err(Display))]
    async fn purge_dirty_for_collection(
        &self,
        request: Request<PurgeDirtyForCollectionRequest>,
    ) -> Result<Response<PurgeDirtyForCollectionResponse>, Status> {
        let request = request.into_inner();
        let collection_id = Uuid::parse_str(&request.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        let dirty_marker = DirtyMarker::MarkForgettable { collection_id };
        let dirty_marker = serde_json::to_string(&dirty_marker)
            .map(Vec::from)
            .map_err(|err| Status::unavailable(err.to_string()))?;
        self.dirty_log
            .append(dirty_marker)
            .await
            .map_err(|err| Status::new(err.code().into(), err.to_string()))?;
        Ok(Response::new(PurgeDirtyForCollectionResponse {}))
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

impl LogServer {
    pub(crate) async fn run(log_server: LogServer) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("[::]:{}", log_server.config.port).parse().unwrap();
        println!("Log listening on {}", addr);

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<chroma_types::chroma_proto::log_service_server::LogServiceServer<Self>>()
            .await;

        let server = Server::builder().add_service(health_service).add_service(
            chroma_types::chroma_proto::log_service_server::LogServiceServer::new(log_server),
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

        server.await?;

        Ok(())
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
    #[serde(default)]
    pub opentelemetry: Option<OpenTelemetryConfig>,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub writer: LogWriterOptions,
    #[serde(default)]
    pub reader: LogReaderOptions,
    #[serde(default = "LogServerConfig::default_record_count_threshold")]
    pub record_count_threshold: u64,
    #[serde(default = "LogServerConfig::default_reinsert_threshold")]
    pub reinsert_threshold: u64,
    #[serde(default = "LogServerConfig::default_timeout_us")]
    pub timeout_us: u64,
}

impl LogServerConfig {
    /// one hundred records on the log.
    fn default_record_count_threshold() -> u64 {
        100
    }

    /// force compaction if a candidate comes up ten times.
    fn default_reinsert_threshold() -> u64 {
        10
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
            opentelemetry: None,
            storage: StorageConfig::default(),
            writer: LogWriterOptions::default(),
            reader: LogReaderOptions::default(),
            record_count_threshold: Self::default_record_count_threshold(),
            reinsert_threshold: Self::default_reinsert_threshold(),
            timeout_us: Self::default_timeout_us(),
        }
    }
}

#[async_trait::async_trait]
impl Configurable<LogServerConfig> for LogServer {
    async fn try_from_config(
        config: &LogServerConfig,
        registry: &chroma_config::registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let storage = Storage::try_from_config(&config.storage, registry).await?;
        let storage = Arc::new(storage);
        let dirty_log = LogWriter::open_or_initialize(
            config.writer.clone(),
            Arc::clone(&storage),
            "dirty",
            "dirty log writer",
            (),
        )
        .await
        .map_err(|err| -> Box<dyn ChromaError> { Box::new(err) as _ })?;
        let dirty_log = Arc::new(dirty_log);
        Ok(Self {
            config: config.clone(),
            open_logs: Arc::new(StateHashTable::default()),
            storage,
            dirty_log,
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
        let _ = LogServer::run(log_server).await;
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

    #[test]
    fn dirty_marker_coalesce1() {
        // Test that a single collection gets coalesced to its completion marker.
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| wal3::Error::Internal)
            .unwrap()
            .as_micros() as u64;
        let collection_id = CollectionUuid::new();
        let markers = vec![
            (
                LogPosition::from_offset(0),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(0),
                    num_records: 1,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(1),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(1),
                    num_records: 1,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
        ];
        let rollup = DirtyMarker::rollup(&markers, 1, 1, 86_400_000_000).unwrap();
        assert_eq!(LogPosition::from_offset(0), rollup.advance_to);
        println!("{:?}", rollup);
        assert_eq!(1, rollup.reinsert.len());
        assert_eq!(
            DirtyMarker::MarkCollected {
                collection_id,
                log_position: LogPosition::from_offset(0),
            },
            rollup.reinsert[0]
        );
        assert_eq!(1, rollup.compactable.len());
        assert_eq!(
            collection_id.to_string(),
            rollup.compactable[0].collection_id
        );
    }

    #[test]
    fn dirty_marker_coalesce2() {
        // Test that a single collection gets reinserted when there are not enough records.
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| wal3::Error::Internal)
            .unwrap()
            .as_micros() as u64;
        let collection_id = CollectionUuid::new();
        let markers = vec![
            (
                LogPosition::from_offset(0),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(0),
                    num_records: 1,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(1),
                DirtyMarker::MarkDirty {
                    collection_id,
                    log_position: LogPosition::from_offset(1),
                    num_records: 1,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
        ];
        let rollup = DirtyMarker::rollup(&markers, 3, 1, 86_400_000_000).unwrap();
        assert_eq!(LogPosition::from_offset(2), rollup.advance_to);
        assert_eq!(0, rollup.compactable.len());
        assert_eq!(2, rollup.reinsert.len());
        assert_eq!(collection_id, rollup.reinsert[0].collection_id());
        assert_eq!(collection_id, rollup.reinsert[1].collection_id());
        // NOTE(rescrv):  MarkCollected are necessarily inserted before MarkDirty.
        assert_eq!(
            DirtyMarker::MarkCollected {
                collection_id,
                log_position: LogPosition::from_offset(0),
            },
            rollup.reinsert[0]
        );
        assert_eq!(
            DirtyMarker::MarkDirty {
                collection_id,
                log_position: LogPosition::from_offset(0),
                num_records: 2,
                reinsert_count: 1,
                initial_insertion_epoch_us: now
            },
            rollup.reinsert[1]
        );
    }

    #[test]
    fn dirty_marker_coalesce3() {
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
                    log_position: LogPosition::from_offset(0),
                    num_records: 1,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
            (
                LogPosition::from_offset(1),
                DirtyMarker::MarkDirty {
                    collection_id: collection_id_acting,
                    log_position: LogPosition::from_offset(0),
                    num_records: 100,
                    reinsert_count: 0,
                    initial_insertion_epoch_us: now,
                },
            ),
        ];
        let rollup = DirtyMarker::rollup(&markers, 3, 1, 86_400_000_000).unwrap();
        assert_eq!(LogPosition::from_offset(1), rollup.advance_to);
        assert_eq!(1, rollup.compactable.len());
        assert_eq!(
            collection_id_acting.to_string(),
            rollup.compactable[0].collection_id
        );
        assert_eq!(3, rollup.reinsert.len());
        assert!(
            (rollup.reinsert[0].collection_id() == collection_id_blocking
                && rollup.reinsert[1].collection_id() == collection_id_acting)
                || (rollup.reinsert[1].collection_id() == collection_id_blocking
                    && rollup.reinsert[0].collection_id() == collection_id_acting)
        );
        assert_eq!(
            DirtyMarker::MarkDirty {
                collection_id: collection_id_blocking,
                log_position: LogPosition::from_offset(0),
                num_records: 1,
                reinsert_count: 1,
                initial_insertion_epoch_us: now,
            },
            rollup.reinsert[2]
        );
    }

    #[test]
    fn unsafe_constants() {
        assert!(STABLE_PREFIX.is_valid());
    }
}
