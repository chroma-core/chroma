// Note: This service is not fully functional after nonce removal
// Keeping code structure intact for potential future reimplementation
// TODO(tanujnay112): Remove this after reimplementation
#![allow(unused_imports)]
#![allow(dead_code)]

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
use chroma_types::{CollectionUuid, DirtyMarker};
use chrono::{DateTime, Utc};
use s3heap::{
    heap_path_from_hostname, Configuration, HeapPruner, HeapReader, HeapWriter, Schedule,
    Triggerable,
};
use wal3::{
    Cursor, CursorName, CursorStore, CursorStoreOptions, CursorWitness, FragmentPuller,
    FragmentSeqNo, LogPosition, LogReader, LogReaderOptions, ManifestReader,
};

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

/// Concrete type alias for the LogReader with S3 consumers.
type S3LogReader = LogReader<(FragmentSeqNo, LogPosition), FragmentPuller, ManifestReader>;

/// Manages heap compaction by reading dirty logs and coordinating with HeapWriter.
pub struct HeapTender {
    #[allow(dead_code)]
    sysdb: SysDb,
    reader: S3LogReader,
    cursor: CursorStore,
    writer: HeapWriter,
    heap_reader: HeapReader,
    heap_pruner: HeapPruner,
}

impl HeapTender {
    /// Creates a new HeapTender.
    pub fn new(
        sysdb: SysDb,
        reader: S3LogReader,
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
        Err(Error::Internal("Not implemented".to_string()))
    }

    /// Reads the dirty log and coalesces entries by collection.
    pub async fn read_and_coalesce_dirty_log(
        &self,
    ) -> Result<
        (
            Option<CursorWitness>,
            Cursor,
            Vec<(CollectionUuid, LogPosition)>,
        ),
        Error,
    > {
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

//////////////////////////////////////////// entrypoint ////////////////////////////////////////////
/// Main entrypoint for the heap tender service.
pub async fn entrypoint() {
    eprintln!("Heap tender service is not currently implemented");
    eprintln!("The heap scheduling functionality was removed");
    std::process::exit(1);
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
