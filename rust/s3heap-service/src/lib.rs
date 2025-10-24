use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use figment::providers::{Env, Format, Yaml};
use futures::stream::StreamExt;
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Request, Response, Status};

use chroma_config::helpers::{deserialize_duration_from_seconds, serialize_duration_to_seconds};
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_storage::config::StorageConfig;
use chroma_storage::Storage;
use chroma_sysdb::{SysDb, SysDbConfig};
use chroma_tracing::OtelFilter;
use chroma_tracing::OtelFilterLevel;
use chroma_types::chroma_proto::heap_tender_service_server::{
    HeapTenderService, HeapTenderServiceServer,
};
use chroma_types::chroma_proto::{HeapSummaryRequest, HeapSummaryResponse};
use chroma_types::{dirty_log_path_from_hostname, CollectionUuid, DirtyMarker, ScheduleEntry};
use chrono::{DateTime, Utc};
use s3heap::{
    heap_path_from_hostname, Configuration, HeapPruner, HeapReader, HeapWriter, Schedule,
    Triggerable,
};
use wal3::{
    Cursor, CursorName, CursorStore, CursorStoreOptions, LogPosition, LogReader, LogReaderOptions,
    Witness,
};

mod scheduler;

pub use scheduler::SysDbScheduler;

/// gRPC client for heap tender service
pub mod client;

//////////////////////////////////////////// conversions ///////////////////////////////////////////

/// Error type for conversion failures.
#[derive(Debug)]
pub struct ConversionError(pub String);

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "conversion error: {}", self.0)
    }
}

impl std::error::Error for ConversionError {}

mod conversions {
    use super::ConversionError;
    use chroma_types::chroma_proto;
    use chrono::{DateTime, Utc};
    use prost_types::Timestamp;
    use s3heap::{HeapItem, Limits, PruneStats, Schedule, Triggerable};
    use uuid::Uuid;

    /// Convert proto Triggerable to s3heap Triggerable.
    pub fn triggerable_from_proto(
        proto: chroma_proto::Triggerable,
    ) -> Result<Triggerable, ConversionError> {
        let partitioning_uuid = Uuid::parse_str(&proto.partitioning_uuid)
            .map_err(|e| ConversionError(format!("invalid partitioning_uuid: {}", e)))?;
        let scheduling_uuid = Uuid::parse_str(&proto.scheduling_uuid)
            .map_err(|e| ConversionError(format!("invalid scheduling_uuid: {}", e)))?;
        Ok(Triggerable {
            partitioning: partitioning_uuid.into(),
            scheduling: scheduling_uuid.into(),
        })
    }

    /// Convert s3heap Triggerable to proto Triggerable.
    pub fn triggerable_to_proto(triggerable: Triggerable) -> chroma_proto::Triggerable {
        chroma_proto::Triggerable {
            partitioning_uuid: triggerable.partitioning.to_string(),
            scheduling_uuid: triggerable.scheduling.to_string(),
        }
    }

    /// Convert proto Schedule to s3heap Schedule.
    pub fn schedule_from_proto(proto: chroma_proto::Schedule) -> Result<Schedule, ConversionError> {
        let triggerable = proto
            .triggerable
            .ok_or_else(|| ConversionError("missing triggerable".to_string()))
            .and_then(triggerable_from_proto)?;
        let next_scheduled = proto
            .next_scheduled
            .ok_or_else(|| ConversionError("missing next_scheduled".to_string()))?;
        let next_scheduled = DateTime::from_timestamp(
            next_scheduled.seconds,
            next_scheduled.nanos.try_into().map_err(|_| {
                ConversionError("invalid nanos value in next_scheduled".to_string())
            })?,
        )
        .ok_or_else(|| ConversionError("invalid next_scheduled timestamp".to_string()))?;
        let nonce = Uuid::parse_str(&proto.nonce)
            .map_err(|e| ConversionError(format!("invalid nonce: {}", e)))?;
        Ok(Schedule {
            triggerable,
            next_scheduled,
            nonce,
        })
    }

    /// Convert s3heap HeapItem with bucket time to proto HeapItem.
    pub fn heap_item_to_proto(
        scheduled_time: DateTime<Utc>,
        item: HeapItem,
    ) -> chroma_proto::HeapItem {
        chroma_proto::HeapItem {
            triggerable: Some(triggerable_to_proto(item.trigger)),
            nonce: item.nonce.to_string(),
            scheduled_time: Some(Timestamp {
                seconds: scheduled_time.timestamp(),
                nanos: scheduled_time.timestamp_subsec_nanos() as i32,
            }),
        }
    }

    /// Convert proto Limits to s3heap Limits.
    pub fn limits_from_proto(proto: chroma_proto::Limits) -> Result<Limits, ConversionError> {
        let buckets_to_read = proto.buckets_to_read.map(|v| v as usize);
        let max_items = proto.max_items.map(|v| v as usize);
        let time_cut_off = proto
            .time_cut_off
            .map(|ts| {
                let nanos = ts.nanos.try_into().map_err(|_| {
                    ConversionError("invalid nanos value in time_cut_off".to_string())
                })?;
                DateTime::from_timestamp(ts.seconds, nanos)
                    .ok_or_else(|| ConversionError("invalid time_cut_off timestamp".to_string()))
            })
            .transpose()?;
        Ok(Limits {
            buckets_to_read,
            max_items,
            time_cut_off,
        })
    }

    /// Convert s3heap PruneStats to proto PruneStats.
    pub fn prune_stats_to_proto(stats: PruneStats) -> chroma_proto::PruneStats {
        chroma_proto::PruneStats {
            items_pruned: stats.items_pruned as u32,
            items_retained: stats.items_retained as u32,
            buckets_deleted: stats.buckets_deleted as u32,
            buckets_updated: stats.buckets_updated as u32,
        }
    }
}

/////////////////////////////////////////////// Error //////////////////////////////////////////////

/// Custom error type that can handle errors from multiple sources.
#[derive(Debug)]
pub enum Error {
    /// Error from s3heap operations.
    S3Heap(s3heap::Error),
    /// Error from wal3 operations.
    Wal3(wal3::Error),
    /// Error from sysdb operations.
    SysDb(chroma_sysdb::PeekScheduleError),
    /// Error from JSON serialization/deserialization.
    Json(serde_json::Error),
    /// Internal error with a message.
    Internal(String),
}

impl From<s3heap::Error> for Error {
    fn from(e: s3heap::Error) -> Self {
        Error::S3Heap(e)
    }
}

impl From<wal3::Error> for Error {
    fn from(e: wal3::Error) -> Self {
        Error::Wal3(e)
    }
}

impl From<chroma_sysdb::PeekScheduleError> for Error {
    fn from(e: chroma_sysdb::PeekScheduleError) -> Self {
        Error::SysDb(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::S3Heap(e) => write!(f, "s3heap error: {}", e),
            Error::Wal3(e) => write!(f, "wal3 error: {}", e),
            Error::SysDb(e) => write!(f, "sysdb error: {}", e),
            Error::Json(e) => write!(f, "json error: {}", e),
            Error::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for Error {}

///////////////////////////////////////////// constants ////////////////////////////////////////////

const DEFAULT_CONFIG_PATH: &str = "./chroma_config.yaml";

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

/// The cursor name used by HeapTender to track its position in the dirty log.
pub static HEAP_TENDER_CURSOR_NAME: CursorName =
    unsafe { CursorName::from_string_unchecked("heap_tender") };

//////////////////////////////////////////// HeapTender ////////////////////////////////////////////

/// Manages heap compaction by reading dirty logs and coordinating with HeapWriter.
pub struct HeapTender {
    #[allow(dead_code)]
    sysdb: SysDb,
    reader: LogReader,
    cursor: CursorStore,
    writer: HeapWriter,
    heap_reader: HeapReader,
    heap_pruner: HeapPruner,
}

impl HeapTender {
    /// Creates a new HeapTender.
    pub fn new(
        sysdb: SysDb,
        reader: LogReader,
        cursor: CursorStore,
        writer: HeapWriter,
        heap_reader: HeapReader,
        heap_pruner: HeapPruner,
    ) -> Self {
        Self {
            sysdb,
            reader,
            cursor,
            writer,
            heap_reader,
            heap_pruner,
        }
    }

    /// Tends to the heap by reading and coalescing the dirty log, then updating the cursor.
    pub async fn tend_to_heap(&self) -> Result<(), Error> {
        let (witness, cursor, tended) = self.read_and_coalesce_dirty_log().await?;
        if !tended.is_empty() {
            let collection_ids = tended.iter().map(|t| t.0).collect::<Vec<_>>();
            let scheduled = self
                .sysdb
                .clone()
                .peek_schedule_by_collection_id(&collection_ids)
                .await?;
            let triggerables: Vec<Option<Schedule>> = scheduled
                .into_iter()
                .map(|s: ScheduleEntry| -> Result<_, Error> {
                    let triggerable = Triggerable {
                        partitioning: s3heap::UnitOfPartitioningUuid::new(s.collection_id.0),
                        scheduling: s3heap::UnitOfSchedulingUuid::new(s.attached_function_id),
                    };
                    if let Some(next_scheduled) = s.when_to_run {
                        let schedule = Schedule {
                            triggerable,
                            next_scheduled,
                            nonce: s.attached_function_run_nonce.0,
                        };
                        Ok(Some(schedule))
                    } else {
                        Ok(None)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            let triggerables: Vec<Schedule> = triggerables.into_iter().flatten().collect();
            if !triggerables.is_empty() {
                self.writer.push(&triggerables).await?;
            }
        }
        if let Some(witness) = witness.as_ref() {
            self.cursor
                .save(&HEAP_TENDER_CURSOR_NAME, &cursor, witness)
                .await?;
        } else {
            self.cursor
                .init(&HEAP_TENDER_CURSOR_NAME, cursor.clone())
                .await?;
        }
        Ok(())
    }

    /// Reads the dirty log and coalesces entries by collection.
    pub async fn read_and_coalesce_dirty_log(
        &self,
    ) -> Result<(Option<Witness>, Cursor, Vec<(CollectionUuid, LogPosition)>), Error> {
        let witness = self.cursor.load(&HEAP_TENDER_CURSOR_NAME).await?;
        let position = match self.reader.oldest_timestamp().await {
            Ok(position) => position,
            Err(wal3::Error::UninitializedLog) => {
                tracing::info!("empty dirty log");
                let default_cursor = Cursor {
                    position: LogPosition::from_offset(0),
                    epoch_us: 0,
                    writer: "heap-tender".to_string(),
                };
                return Ok((witness, default_cursor, vec![]));
            }
            Err(e) => return Err(Error::Wal3(e)),
        };
        let default = Cursor {
            position,
            epoch_us: position.offset(),
            writer: "heap-tender".to_string(),
        };
        let start_cursor = witness
            .as_ref()
            .map(|w| w.cursor())
            .unwrap_or(&default)
            .clone();
        let mut limit_cursor = start_cursor.clone();
        tracing::info!("cursoring from {start_cursor:?}");
        let dirty_fragments = match self
            .reader
            .scan(
                start_cursor.position,
                wal3::Limits {
                    max_files: None,
                    max_bytes: None,
                    max_records: None,
                },
            )
            .await
        {
            Ok(dirty_fragments) => dirty_fragments,
            Err(wal3::Error::UninitializedLog) => {
                tracing::info!("empty dirty log");
                return Ok((witness, limit_cursor, vec![]));
            }
            Err(e) => {
                return Err(Error::Wal3(e));
            }
        };
        let dirty_futures = dirty_fragments
            .iter()
            .map(|fragment| async {
                let (_, records, _) = self.reader.read_parquet(fragment).await?;
                let dirty_markers = records
                    .into_iter()
                    .map(|x| -> Result<(LogPosition, DirtyMarker), Error> {
                        let dirty = serde_json::from_slice::<DirtyMarker>(&x.1)?;
                        Ok((x.0, dirty))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<_, Error>(dirty_markers)
            })
            .collect::<Vec<_>>();
        let stream = futures::stream::iter(dirty_futures);
        let mut buffered = stream.buffer_unordered(10);
        let mut collections: HashMap<CollectionUuid, LogPosition> = HashMap::default();
        while let Some(res) = buffered.next().await {
            for (position, marker) in res? {
                limit_cursor.position = std::cmp::max(limit_cursor.position, position + 1u64);
                if let DirtyMarker::MarkDirty {
                    collection_id,
                    log_position,
                    num_records,
                    reinsert_count,
                    ..
                } = marker
                {
                    if reinsert_count == 0 {
                        let collection_position = collections.entry(collection_id).or_default();
                        *collection_position = std::cmp::max(
                            *collection_position,
                            LogPosition::from_offset(
                                log_position
                                    .checked_add(num_records)
                                    .ok_or(Error::Internal("log position overflow".to_string()))?,
                            ),
                        );
                    }
                }
            }
        }
        Ok((witness, limit_cursor, collections.into_iter().collect()))
    }

    async fn background(tender: Arc<Self>, poll_interval: Duration) {
        loop {
            tokio::time::sleep(poll_interval).await;
            if let Err(err) = tender.tend_to_heap().await {
                tracing::error!("could not roll up dirty log: {err:?}");
            }
        }
    }
}

///////////////////////////////////////// HeapTenderServer /////////////////////////////////////////

struct HeapTenderServer {
    config: HeapTenderServerConfig,
    tender: Arc<HeapTender>,
}

impl HeapTenderServer {
    async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("[::]:{}", self.config.port).parse().unwrap();
        println!("Heap tender listening on {}", addr);

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<HeapTenderServiceServer<Self>>()
            .await;

        let max_encoding_message_size = self.config.max_encoding_message_size;
        let max_decoding_message_size = self.config.max_decoding_message_size;
        let shutdown_grace_period = self.config.grpc_shutdown_grace_period;
        let tender = Arc::clone(&self.tender);
        let background =
            tokio::task::spawn(HeapTender::background(tender, self.config.poll_interval));

        let server = Server::builder()
            .layer(chroma_tracing::GrpcServerTraceLayer)
            .add_service(health_service)
            .add_service(
                HeapTenderServiceServer::new(self)
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
            tokio::time::sleep(shutdown_grace_period).await;
            tracing::info!("Grace period ended, shutting down server...");
        });

        let res = server.await;
        background.abort();
        Ok(res?)
    }
}

#[async_trait::async_trait]
impl Configurable<HeapTenderServerConfig> for HeapTenderServer {
    async fn try_from_config(
        config: &HeapTenderServerConfig,
        registry: &chroma_config::registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match &config.sysdb {
            chroma_sysdb::SysDbConfig::Grpc(_) => {}
            chroma_sysdb::SysDbConfig::Sqlite(_) => {
                panic!("Expected grpc sysdb config, got sqlite sysdb config")
            }
        };
        let sysdb = SysDb::try_from_config(&config.sysdb, registry).await?;
        let storage = Storage::try_from_config(&config.storage, registry).await?;
        let dirty_log_prefix = dirty_log_path_from_hostname(&config.my_member_id);
        let reader = LogReader::new(
            config.reader.clone(),
            Arc::new(storage.clone()),
            dirty_log_prefix.clone(),
        );
        let cursor = CursorStore::new(
            config.cursor.clone(),
            Arc::new(storage.clone()),
            dirty_log_prefix.clone(),
            "s3heap-tender".to_string(),
        );
        let heap_prefix = heap_path_from_hostname(&config.my_member_id);
        let scheduler = Arc::new(SysDbScheduler::new(sysdb.clone())) as _;
        let writer = HeapWriter::new(storage.clone(), heap_prefix.clone(), Arc::clone(&scheduler))
            .await
            .map_err(|e| -> Box<dyn chroma_error::ChromaError> { Box::new(e) })?;
        let heap_reader =
            HeapReader::new(storage.clone(), heap_prefix.clone(), Arc::clone(&scheduler))
                .await
                .map_err(|e| -> Box<dyn chroma_error::ChromaError> { Box::new(e) })?;
        let heap_pruner = HeapPruner::new(storage, heap_prefix, Arc::clone(&scheduler))
            .map_err(|e| -> Box<dyn chroma_error::ChromaError> { Box::new(e) })?;
        let tender = Arc::new(HeapTender {
            sysdb,
            reader,
            cursor,
            writer,
            heap_reader,
            heap_pruner,
        });
        Ok(Self {
            config: config.clone(),
            tender,
        })
    }
}

#[async_trait::async_trait]
impl HeapTenderService for HeapTenderServer {
    async fn push(
        &self,
        request: Request<chroma_types::chroma_proto::PushRequest>,
    ) -> Result<Response<chroma_types::chroma_proto::PushResponse>, Status> {
        let schedules: Vec<Schedule> = request
            .into_inner()
            .schedules
            .into_iter()
            .map(conversions::schedule_from_proto)
            .collect::<Result<Vec<_>, ConversionError>>()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let count = schedules.len();
        let count_u32 = count
            .try_into()
            .map_err(|_| Status::invalid_argument("too many schedules to push"))?;
        self.tender
            .writer
            .push(&schedules)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(chroma_types::chroma_proto::PushResponse {
            schedules_added: count_u32,
        }))
    }

    async fn peek(
        &self,
        request: Request<chroma_types::chroma_proto::PeekRequest>,
    ) -> Result<Response<chroma_types::chroma_proto::PeekResponse>, Status> {
        let req = request.into_inner();
        let limits: s3heap::Limits = req
            .limits
            .ok_or_else(|| Status::invalid_argument("missing limits"))
            .and_then(|l| {
                conversions::limits_from_proto(l)
                    .map_err(|e| Status::invalid_argument(e.to_string()))
            })?;

        let filter = req.filter;
        let filter_fn = move |triggerable: &Triggerable, _: DateTime<Utc>| {
            if let Some(ref f) = filter {
                if let Some(ref partitioning_uuid) = f.partitioning_uuid {
                    if triggerable.partitioning.to_string() != *partitioning_uuid {
                        return false;
                    }
                }
                if let Some(ref scheduling_uuid) = f.scheduling_uuid {
                    if triggerable.scheduling.to_string() != *scheduling_uuid {
                        return false;
                    }
                }
            }
            true
        };

        let items = self
            .tender
            .heap_reader
            .peek(filter_fn, limits)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let proto_items: Vec<chroma_types::chroma_proto::HeapItem> = items
            .into_iter()
            .map(|(dt, item)| conversions::heap_item_to_proto(dt, item))
            .collect();

        Ok(Response::new(chroma_types::chroma_proto::PeekResponse {
            items: proto_items,
        }))
    }

    async fn prune(
        &self,
        request: Request<chroma_types::chroma_proto::PruneRequest>,
    ) -> Result<Response<chroma_types::chroma_proto::PruneResponse>, Status> {
        let limits: s3heap::Limits = request
            .into_inner()
            .limits
            .ok_or_else(|| Status::invalid_argument("missing limits"))
            .and_then(|l| {
                conversions::limits_from_proto(l)
                    .map_err(|e| Status::invalid_argument(e.to_string()))
            })?;

        let stats = self
            .tender
            .heap_pruner
            .prune(limits)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(chroma_types::chroma_proto::PruneResponse {
            stats: Some(conversions::prune_stats_to_proto(stats)),
        }))
    }

    async fn prune_bucket(
        &self,
        request: Request<chroma_types::chroma_proto::PruneBucketRequest>,
    ) -> Result<Response<chroma_types::chroma_proto::PruneBucketResponse>, Status> {
        let timestamp = request
            .into_inner()
            .bucket
            .ok_or_else(|| Status::invalid_argument("missing bucket timestamp"))?;

        let bucket =
            DateTime::from_timestamp(timestamp.seconds, timestamp.nanos.try_into().unwrap_or(0))
                .ok_or_else(|| Status::invalid_argument("invalid bucket timestamp"))?;

        let stats = self
            .tender
            .heap_pruner
            .prune_bucket(bucket)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(
            chroma_types::chroma_proto::PruneBucketResponse {
                stats: Some(conversions::prune_stats_to_proto(stats)),
            },
        ))
    }

    async fn list_buckets(
        &self,
        request: Request<chroma_types::chroma_proto::ListBucketsRequest>,
    ) -> Result<Response<chroma_types::chroma_proto::ListBucketsResponse>, Status> {
        let max_buckets = request.into_inner().max_buckets.map(|v| v as usize);

        let buckets = self
            .tender
            .heap_reader
            .list_buckets(max_buckets)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let proto_buckets: Vec<prost_types::Timestamp> = buckets
            .into_iter()
            .map(|dt| prost_types::Timestamp {
                seconds: dt.timestamp(),
                nanos: dt.timestamp_subsec_nanos() as i32,
            })
            .collect();

        Ok(Response::new(
            chroma_types::chroma_proto::ListBucketsResponse {
                buckets: proto_buckets,
            },
        ))
    }

    async fn summary(
        &self,
        _request: Request<HeapSummaryRequest>,
    ) -> Result<Response<HeapSummaryResponse>, Status> {
        let buckets = self
            .tender
            .heap_reader
            .list_buckets(None)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let bucket_count = buckets.len() as u32;
        let oldest_bucket = buckets.first().map(|dt| prost_types::Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        });
        let newest_bucket = buckets.last().map(|dt| prost_types::Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        });

        let items = self
            .tender
            .heap_reader
            .peek(
                |_, _| true,
                s3heap::Limits::default().with_time_cut_off(Utc::now()),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let total_items = items.len() as u32;

        Ok(Response::new(HeapSummaryResponse {
            total_items,
            oldest_bucket,
            newest_bucket,
            bucket_count,
        }))
    }
}

//////////////////////////////////////////// RootConfig ////////////////////////////////////////////

/// Root configuration for the heap tender service.
///
/// This is the top-level configuration structure loaded from YAML config files
/// and environment variables. It contains all configuration sections needed to
/// run the heap tender service.
#[derive(serde::Deserialize, serde::Serialize)]
pub struct RootConfig {
    /// Configuration for the heap tender gRPC service.
    #[serde(default)]
    pub heap_tender_service: HeapTenderServerConfig,
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

/// Configuration for OpenTelemetry tracing and observability.
///
/// Controls how the service exports traces and metrics to an OpenTelemetry collector.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct OpenTelemetryConfig {
    /// The OpenTelemetry collector endpoint URL.
    #[serde(default = "OpenTelemetryConfig::default_endpoint")]
    pub endpoint: String,
    /// The service name reported in traces.
    #[serde(default = "OpenTelemetryConfig::default_otel_service_name")]
    pub service_name: String,
    /// Trace level filters for different crates.
    #[serde(default = "OpenTelemetryConfig::default_otel_filters")]
    pub filters: Vec<OtelFilter>,
}

impl OpenTelemetryConfig {
    fn default_endpoint() -> String {
        "http://otel-collector:4317".to_string()
    }

    fn default_otel_service_name() -> String {
        "rust-log-service".to_string()
    }

    fn default_otel_filters() -> Vec<OtelFilter> {
        vec![OtelFilter {
            crate_name: "s3heap".to_string(),
            filter_level: OtelFilterLevel::Trace,
        }]
    }
}

/// Configuration for the heap tender gRPC service.
///
/// Contains all settings needed to run the heap tender server, including network
/// configuration, storage backend settings, and gRPC message size limits.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct HeapTenderServerConfig {
    /// The port to bind the gRPC server to.
    #[serde(default = "HeapTenderServerConfig::default_port")]
    pub port: u16,
    /// The member ID of this service instance in the cluster.
    #[serde(default = "HeapTenderServerConfig::default_my_member_id")]
    pub my_member_id: String,
    /// Optional OpenTelemetry configuration for tracing.
    #[serde(default)]
    pub opentelemetry: Option<OpenTelemetryConfig>,
    /// Configuration for the sysdb backend.
    #[serde(default = "HeapTenderServerConfig::default_sysdb_config")]
    pub sysdb: SysDbConfig,
    /// Configuration for the S3 storage backend.
    #[serde(default)]
    pub storage: StorageConfig,
    /// wal3 configuration of the dirty log reader.
    #[serde(default)]
    pub reader: LogReaderOptions,
    /// wal3 configuration of the cursor store.
    #[serde(default)]
    pub cursor: CursorStoreOptions,
    /// Configuration of the HeapWriter.
    #[serde(default)]
    pub writer: Configuration,
    /// Periodicity of poll from end of one poll of the dirty log to start of the next.
    #[serde(default = "HeapTenderServerConfig::default_poll_interval")]
    pub poll_interval: Duration,
    /// Maximum size in bytes for outgoing gRPC messages.
    #[serde(default = "HeapTenderServerConfig::default_max_encoding_message_size")]
    pub max_encoding_message_size: usize,
    /// Maximum size in bytes for incoming gRPC messages.
    #[serde(default = "HeapTenderServerConfig::default_max_decoding_message_size")]
    pub max_decoding_message_size: usize,
    /// Grace period to wait before shutting down the gRPC server on SIGTERM.
    #[serde(
        rename = "grpc_shutdown_grace_period_seconds",
        deserialize_with = "deserialize_duration_from_seconds",
        serialize_with = "serialize_duration_to_seconds",
        default = "HeapTenderServerConfig::default_grpc_shutdown_grace_period"
    )]
    pub grpc_shutdown_grace_period: Duration,
}

impl HeapTenderServerConfig {
    fn default_port() -> u16 {
        50052
    }

    fn default_my_member_id() -> String {
        "rust-log-service-0".to_string()
    }

    fn default_max_encoding_message_size() -> usize {
        32_000_000
    }

    fn default_max_decoding_message_size() -> usize {
        32_000_000
    }

    fn default_poll_interval() -> Duration {
        Duration::from_secs(10)
    }

    fn default_sysdb_config() -> SysDbConfig {
        SysDbConfig::Grpc(Default::default())
    }

    fn default_grpc_shutdown_grace_period() -> Duration {
        Duration::from_secs(1)
    }
}

impl Default for HeapTenderServerConfig {
    fn default() -> Self {
        Self {
            port: HeapTenderServerConfig::default_port(),
            my_member_id: HeapTenderServerConfig::default_my_member_id(),
            opentelemetry: None,
            sysdb: HeapTenderServerConfig::default_sysdb_config(),
            storage: StorageConfig::default(),
            reader: LogReaderOptions::default(),
            cursor: CursorStoreOptions::default(),
            writer: Configuration::default(),
            poll_interval: Self::default_poll_interval(),
            max_encoding_message_size: Self::default_max_encoding_message_size(),
            max_decoding_message_size: Self::default_max_decoding_message_size(),
            grpc_shutdown_grace_period: Self::default_grpc_shutdown_grace_period(),
        }
    }
}

//////////////////////////////////////////// entrypoint ////////////////////////////////////////////

/// Main entrypoint for the heap tender service.
///
/// Loads configuration from YAML files and environment variables, initializes
/// OpenTelemetry tracing if configured, creates the heap tender server, and runs
/// the gRPC service until a SIGTERM is received.
pub async fn entrypoint() {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => RootConfig::load_from_path(&config_path),
        Err(_) => RootConfig::load(),
    };
    let config = config.heap_tender_service;
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
    let heap_tender_server = HeapTenderServer::try_from_config(&config, &registry)
        .await
        .expect("Failed to create heap tender server");

    let server_join_handle = tokio::spawn(async move {
        match heap_tender_server.run().await {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Server terminated with error: {:?}", e);
            }
        }
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
    use chroma_types::chroma_proto;
    use chrono::TimeZone;
    use s3heap::{HeapItem, Limits, PruneStats, Triggerable};
    use uuid::Uuid;

    #[test]
    fn triggerable_round_trip() {
        let partitioning_uuid = Uuid::new_v4();
        let scheduling_uuid = Uuid::new_v4();

        let original = Triggerable {
            partitioning: partitioning_uuid.into(),
            scheduling: scheduling_uuid.into(),
        };

        let proto = conversions::triggerable_to_proto(original);
        let recovered = conversions::triggerable_from_proto(proto).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn schedule_round_trip() {
        let partitioning_uuid = Uuid::new_v4();
        let scheduling_uuid = Uuid::new_v4();
        let nonce = Uuid::new_v4();
        let next_scheduled = Utc.with_ymd_and_hms(2024, 3, 15, 14, 30, 0).unwrap();

        let original = Schedule {
            triggerable: Triggerable {
                partitioning: partitioning_uuid.into(),
                scheduling: scheduling_uuid.into(),
            },
            next_scheduled,
            nonce,
        };

        let proto = chroma_proto::Schedule {
            triggerable: Some(conversions::triggerable_to_proto(original.triggerable)),
            next_scheduled: Some(prost_types::Timestamp {
                seconds: next_scheduled.timestamp(),
                nanos: next_scheduled.timestamp_subsec_nanos() as i32,
            }),
            nonce: nonce.to_string(),
        };
        let recovered = conversions::schedule_from_proto(proto).unwrap();

        assert_eq!(original.triggerable, recovered.triggerable);
        assert_eq!(original.nonce, recovered.nonce);
        assert_eq!(original.next_scheduled, recovered.next_scheduled);
    }

    #[test]
    fn heap_item_round_trip() {
        let partitioning_uuid = Uuid::new_v4();
        let scheduling_uuid = Uuid::new_v4();
        let nonce = Uuid::new_v4();
        let scheduled_time = Utc.with_ymd_and_hms(2024, 3, 15, 14, 30, 0).unwrap();

        let original_item = HeapItem {
            trigger: Triggerable {
                partitioning: partitioning_uuid.into(),
                scheduling: scheduling_uuid.into(),
            },
            nonce,
        };

        let proto = conversions::heap_item_to_proto(scheduled_time, original_item.clone());

        assert_eq!(
            proto.triggerable.as_ref().unwrap().partitioning_uuid,
            partitioning_uuid.to_string()
        );
        assert_eq!(
            proto.triggerable.as_ref().unwrap().scheduling_uuid,
            scheduling_uuid.to_string()
        );
        assert_eq!(proto.nonce, nonce.to_string());
        assert_eq!(
            proto.scheduled_time.as_ref().unwrap().seconds,
            scheduled_time.timestamp()
        );
        assert_eq!(
            proto.scheduled_time.as_ref().unwrap().nanos,
            scheduled_time.timestamp_subsec_nanos() as i32
        );
    }

    #[test]
    fn limits_round_trip() {
        let original = Limits {
            buckets_to_read: Some(100),
            max_items: Some(50),
            time_cut_off: Some(Utc.with_ymd_and_hms(2024, 3, 15, 14, 30, 0).unwrap()),
        };

        let proto = chroma_proto::Limits {
            buckets_to_read: original.buckets_to_read.map(|v| v as u32),
            max_items: original.max_items.map(|v| v as u32),
            time_cut_off: original.time_cut_off.map(|dt| prost_types::Timestamp {
                seconds: dt.timestamp(),
                nanos: dt.timestamp_subsec_nanos() as i32,
            }),
        };
        let recovered = conversions::limits_from_proto(proto).unwrap();

        assert_eq!(original.buckets_to_read, recovered.buckets_to_read);
        assert_eq!(original.max_items, recovered.max_items);
        assert_eq!(original.time_cut_off, recovered.time_cut_off);
    }

    #[test]
    fn limits_round_trip_with_none() {
        let original = Limits {
            buckets_to_read: None,
            max_items: None,
            time_cut_off: None,
        };

        let proto = chroma_proto::Limits {
            buckets_to_read: None,
            max_items: None,
            time_cut_off: None,
        };
        let recovered = conversions::limits_from_proto(proto).unwrap();

        assert_eq!(original.buckets_to_read, recovered.buckets_to_read);
        assert_eq!(original.max_items, recovered.max_items);
        assert_eq!(original.time_cut_off, recovered.time_cut_off);
    }

    #[test]
    fn prune_stats_round_trip() {
        let original = PruneStats {
            items_pruned: 42,
            items_retained: 100,
            buckets_deleted: 5,
            buckets_updated: 10,
        };

        let proto = conversions::prune_stats_to_proto(original.clone());
        assert_eq!(proto.items_pruned, 42);
        assert_eq!(proto.items_retained, 100);
        assert_eq!(proto.buckets_deleted, 5);
        assert_eq!(proto.buckets_updated, 10);
    }
}
