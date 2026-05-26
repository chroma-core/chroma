//! # Revision History Function
//!
//! Archives every version of a record into a lightweight output collection (expected to have no vector/metadata
//! indexes). Each mutation in the source collection produces an immutable revision in the
//! history collection, enabling pagination over a record's full change timeline.
//!
//! ## Data Flow
//!
//! ```text
//!   Source Collection                   History Collection (record-only)
//!  ┌─────────────────┐                ┌──────────────────────────────────┐
//!  │ id: "page-1"    │  ──compaction──▶│ "page-1::v1"  {version:1, ...}  │
//!  │ metadata:       │                │ "page-1::v2"  {version:2, ...}  │
//!  │   version: 3    │                │ "page-1::v3"  {version:3, ...}  │
//!  │   title: "..."  │                │ "page-1::v0"  {max_version:3,   │
//!  └─────────────────┘                │                generation_...}   │
//!                                     └──────────────────────────────────┘
//! ```
//!
//! ## Key Concepts
//!
//! - **Composite IDs**: History records are keyed as `"{original_id}::v{version}"`.
//! - **v0 Tracker**: A special record at `"{original_id}::v0"` stores `max_version` and
//!   generation boundaries, enabling efficient version assignment without scanning.
//! - **Effective Version**: A monotonic, gapless counter (1, 2, 3, ...) assigned by this
//!   function, independent of the source application's version counter.
//! - **Resurrection**: When a deleted record is re-created with the same ID, the source
//!   version counter resets. The tracker detects this and starts a new generation while
//!   continuing the effective version sequence.
//! - **Tombstones**: Deletes produce a revision with `is_delete: true`. The tombstone
//!   position is computed as `source_version + 1` (read from the deleted record's metadata)
//!   to ensure all chunks of the same document get the same tombstone effective_version,
//!   regardless of how many compaction cycles each chunk missed.
//!
//! ## Chunking Assumption
//!
//! This function assumes it receives **all records for a compaction cycle in a single
//! `execute()` call**. The in-memory tracker state (built from the v0 record at the start)
//! is shared across all records in the batch — if the same original_id appears multiple
//! times within one batch, versions are assigned sequentially from the in-memory tracker.
//!
//! If a document is chunked across multiple records (each chunk having its own ID), each
//! chunk is tracked independently with its own v0 tracker and version timeline. The
//! source application is responsible for ensuring the `version_key` metadata is consistent
//! across all chunks belonging to the same logical document revision.
//!
//! Even if chunks for the same version land in different compaction cycles, the system
//! converges: each chunk's tracker is independent and persisted after every cycle.
//! For the UI, chunk-0 can serve as the canonical version timeline for listing/pagination,
//! since all chunks advance atomically from the source application's perspective. The
//! facade expands to all chunks only when displaying a specific revision's full content.
//!
//!
//! NOTE(hammadb): I hate this design. It is all too clever.

use crate::execution::operators::execute_task::AttachedFunctionExecutor;
use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_segment::blockfile_record::{RecordSegmentReaderOptions, RecordSegmentReaderShard};
use chroma_segment::types::HydratedMaterializedLogRecord;
use chroma_types::{
    AttachedFunction, Chunk, LogRecord, MaterializedLogOperation, MetadataValue, Operation,
    OperationRecord, UpdateMetadataValue,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_VERSION_KEY: &str = "version";

/// Minimal placeholder embedding required by the materialization pipeline.
/// The output collection should have no index, so this is stored but never indexed.
/// TODO: allow this to occur so indexing doesn't occur
const PLACEHOLDER_EMBEDDING: [f32; 1] = [0.0];

/// Composite ID for a revision record: `"{original_id}::v{version}"`.
fn revision_id(original_id: &str, version: i64) -> String {
    format!("{original_id}::v{version}")
}

/// Composite ID for the v0 tracker record: `"{original_id}::v0"`.
fn tracker_id(original_id: &str) -> String {
    format!("{original_id}::v0")
}

/// Per-record version tracker stored at `{original_id}::v0` in the output collection.
/// Maintains the monotonic version counter and generation boundaries across resurrection cycles.
///
/// v0 is a metadata-only record — no document, no embedding, no revision content.
/// Real revisions start at v1. Effective versions are 1-indexed; source versions are
/// app-defined (no assumption on start value or indexing).
#[derive(Debug)]
struct RevisionTracker {
    /// Highest effective_version assigned so far. 0 means no revisions yet.
    /// Real revisions occupy positions v1..=max_version.
    max_version: i64,
    /// Effective_version of the first record in the current generation (always >= 1).
    generation_start_pos: i64,
    /// The source_version (app-provided) that corresponds to generation_start_pos.
    /// Together with generation_start_pos, defines the linear mapping:
    ///   effective = generation_start_pos + (source_version - generation_start_source_ver)
    generation_start_source_ver: i64,
}

impl RevisionTracker {
    fn new() -> Self {
        Self {
            max_version: 0,
            generation_start_pos: 0,
            generation_start_source_ver: 0,
        }
    }

    /// Detects whether `source_version` represents a new generation (resurrection).
    ///
    /// A new generation starts when the source application resets its version counter
    /// (e.g. after a delete + re-create of the same record ID). We detect this by
    /// computing the expected max source_version for the current generation:
    ///   expected = start_source_ver + (max_version - start_pos)
    /// If the incoming source_version is strictly below this expected max, the source
    /// counter has been reset and we're starting a new generation. Equal means we're
    /// seeing the same version again (idempotent re-compaction).
    fn is_new_generation(&self, source_version: i64) -> bool {
        if self.max_version == 0 {
            return true;
        }
        let expected_max_source_ver =
            self.generation_start_source_ver + (self.max_version - self.generation_start_pos);
        // Guard: if generation fields are uninitialized (both zero with max_version > 0),
        // all previous records went through the fallback path (no version_key). We can't
        // do resurrection detection without a valid generation baseline, so skip it.
        source_version < expected_max_source_ver
            && !(self.generation_start_pos == 0 && self.generation_start_source_ver == 0)
    }

    /// Compute the next effective_version for a record with a known source_version.
    ///
    /// Maps source_version deterministically to a history position:
    ///   effective = generation_start_pos + (source_version - generation_start_source_ver)
    ///
    /// If the record starts a new generation, the generation boundary is updated first.
    /// Returns `None` if this version has already been archived (idempotency).
    fn next_version_for_source(&mut self, source_version: i64) -> Option<i64> {
        if self.is_new_generation(source_version) {
            let new_start = self.max_version + 1;
            self.generation_start_pos = new_start;
            self.generation_start_source_ver = source_version;
        }

        let effective =
            self.generation_start_pos + (source_version - self.generation_start_source_ver);

        // Already archived in a previous compaction cycle — skip to avoid duplicates.
        if effective <= self.max_version {
            return None;
        }

        self.max_version = effective;
        Some(effective)
    }

    /// Compute the next effective_version for a record without a source_version
    /// (deletes, or records missing the version key). Falls back to max_version + 1.
    fn next_version_fallback(&mut self) -> i64 {
        self.max_version += 1;
        self.max_version
    }

    /// Load tracker state from the output collection's `{original_id}::v0` record.
    /// Returns a fresh tracker if the record doesn't exist yet.
    async fn from_reader(
        output_reader: Option<&RecordSegmentReaderShard<'_>>,
        original_id: &str,
    ) -> Self {
        let Some(reader) = output_reader else {
            return Self::new();
        };

        let tid = tracker_id(original_id);
        let offset_id = match reader
            .get_offset_id_for_user_id(&tid, &RecordSegmentReaderOptions::default())
            .await
        {
            Ok(Some(id)) => id,
            _ => return Self::new(),
        };

        let data_record = match reader.get_data_for_offset_id(offset_id).await {
            Ok(Some(record)) => record,
            _ => return Self::new(),
        };

        let metadata = match &data_record.metadata {
            Some(m) => m,
            None => return Self::new(),
        };

        let max_version = match metadata.get("max_version") {
            Some(MetadataValue::Int(v)) => *v,
            _ => 0,
        };
        let generation_start_pos = match metadata.get("generation_start_pos") {
            Some(MetadataValue::Int(v)) => *v,
            _ => 0,
        };
        let generation_start_source_ver = match metadata.get("generation_start_source_ver") {
            Some(MetadataValue::Int(v)) => *v,
            _ => 0,
        };

        Self {
            max_version,
            generation_start_pos,
            generation_start_source_ver,
        }
    }

    fn to_log_record(&self, original_id: &str) -> LogRecord {
        let metadata = HashMap::from([
            (
                "max_version".to_string(),
                UpdateMetadataValue::Int(self.max_version),
            ),
            (
                "generation_start_pos".to_string(),
                UpdateMetadataValue::Int(self.generation_start_pos),
            ),
            (
                "generation_start_source_ver".to_string(),
                UpdateMetadataValue::Int(self.generation_start_source_ver),
            ),
            (
                "original_id".to_string(),
                UpdateMetadataValue::Str(original_id.to_string()),
            ),
        ]);

        LogRecord {
            // log_offset is unused for function output records; the compaction
            // pipeline assigns offsets when writing to the output collection.
            log_offset: 0,
            record: OperationRecord {
                id: tracker_id(original_id),
                embedding: Some(PLACEHOLDER_EMBEDDING.to_vec()),
                encoding: None,
                metadata: Some(metadata),
                document: None,
                operation: Operation::Upsert,
            },
        }
    }
}

/// Metadata written to each revision record in the output collection.
/// Provides a typed interface instead of ad-hoc HashMap construction.
#[derive(Debug)]
struct RevisionMetadata {
    original_id: String,
    version: i64,
    source_version: Option<i64>,
    archived_at: i64,
    is_delete: bool,
}

impl RevisionMetadata {
    fn into_update_metadata(
        self,
        original_metadata: Option<&HashMap<String, MetadataValue>>,
    ) -> HashMap<String, UpdateMetadataValue> {
        let mut metadata = HashMap::new();
        metadata.insert(
            "original_id".to_string(),
            UpdateMetadataValue::Str(self.original_id),
        );
        metadata.insert(
            "version".to_string(),
            UpdateMetadataValue::Int(self.version),
        );
        metadata.insert(
            "archived_at".to_string(),
            UpdateMetadataValue::Int(self.archived_at),
        );
        metadata.insert(
            "is_delete".to_string(),
            UpdateMetadataValue::Bool(self.is_delete),
        );

        if let Some(sv) = self.source_version {
            metadata.insert("source_version".to_string(), UpdateMetadataValue::Int(sv));
        }

        if let Some(orig) = original_metadata {
            for (key, value) in orig {
                let update_value = match value {
                    MetadataValue::Bool(b) => UpdateMetadataValue::Bool(*b),
                    MetadataValue::Int(i) => UpdateMetadataValue::Int(*i),
                    MetadataValue::Float(f) => UpdateMetadataValue::Float(*f),
                    MetadataValue::Str(s) => UpdateMetadataValue::Str(s.clone()),
                    _ => continue,
                };
                metadata.entry(key.clone()).or_insert(update_value);
            }
        }

        metadata
    }
}

#[derive(Debug)]
pub struct RevisionHistoryExecutor {
    version_key: String,
}

impl RevisionHistoryExecutor {
    pub fn from_attached_function(af: &AttachedFunction) -> Result<Self, Box<dyn ChromaError>> {
        let version_key = if let Some(ref params_str) = af.params {
            let params: serde_json::Value =
                serde_json::from_str(params_str).unwrap_or(serde_json::Value::Null);
            params
                .get("version_key")
                .and_then(|v| v.as_str())
                .unwrap_or(DEFAULT_VERSION_KEY)
                .to_string()
        } else {
            DEFAULT_VERSION_KEY.to_string()
        };

        Ok(Self { version_key })
    }
}

#[async_trait]
impl AttachedFunctionExecutor for RevisionHistoryExecutor {
    async fn execute(
        &self,
        input_records: Vec<Chunk<HydratedMaterializedLogRecord<'_, '_>>>,
        output_reader: Option<&RecordSegmentReaderShard<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        if input_records.is_empty() {
            return Ok(Chunk::new(Arc::from(Vec::new())));
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut trackers: HashMap<String, RevisionTracker> = HashMap::new();
        let mut output = Vec::new();

        for batch in input_records {
            for (record, _index) in batch.iter() {
                let original_id = record.get_user_id().to_string();

                if !trackers.contains_key(&original_id) {
                    let state = RevisionTracker::from_reader(output_reader, &original_id).await;
                    trackers.insert(original_id.clone(), state);
                }

                // SAFETY(hammadb): We know the tracker exists because we just inserted it above.
                let tracker = trackers.get_mut(&original_id).unwrap();

                if record.get_operation() == MaterializedLogOperation::DeleteExisting {
                    // Read the source_version from the record being deleted so all chunks
                    // of the same document get the same tombstone position (virtual_sv = sv + 1).
                    let merged_metadata = record.merged_metadata();
                    let source_version = match merged_metadata.get(&self.version_key) {
                        Some(MetadataValue::Int(v)) => Some(*v),
                        _ => None,
                    };

                    let effective_version = if let Some(sv) = source_version {
                        let virtual_sv = sv + 1;
                        match tracker.next_version_for_source(virtual_sv) {
                            Some(v) => v,
                            None => continue,
                        }
                    } else {
                        tracker.next_version_fallback()
                    };

                    let composite_id = revision_id(&original_id, effective_version);

                    let rev_meta = RevisionMetadata {
                        original_id: original_id.clone(),
                        version: effective_version,
                        source_version,
                        archived_at: now,
                        is_delete: true,
                    };

                    output.push(LogRecord {
                        log_offset: 0,
                        record: OperationRecord {
                            id: composite_id,
                            embedding: Some(PLACEHOLDER_EMBEDDING.to_vec()),
                            encoding: None,
                            metadata: Some(rev_meta.into_update_metadata(None)),
                            document: None,
                            operation: Operation::Upsert,
                        },
                    });
                } else {
                    let merged_metadata = record.merged_metadata();
                    let source_version = match merged_metadata.get(&self.version_key) {
                        Some(MetadataValue::Int(v)) => Some(*v),
                        _ => {
                            tracing::warn!(
                                record_id = %original_id,
                                version_key = %self.version_key,
                                "version_key not found in record metadata; archiving without source_version"
                            );
                            None
                        }
                    };

                    let effective_version = if let Some(sv) = source_version {
                        match tracker.next_version_for_source(sv) {
                            Some(v) => v,
                            None => continue, // already archived
                        }
                    } else {
                        tracker.next_version_fallback()
                    };

                    let composite_id = revision_id(&original_id, effective_version);

                    let rev_meta = RevisionMetadata {
                        original_id: original_id.clone(),
                        version: effective_version,
                        source_version,
                        archived_at: now,
                        is_delete: false,
                    };

                    let document = record.merged_document_ref().map(|s| s.to_string());

                    output.push(LogRecord {
                        log_offset: 0,
                        record: OperationRecord {
                            id: composite_id,
                            embedding: Some(PLACEHOLDER_EMBEDDING.to_vec()),
                            encoding: None,
                            metadata: Some(rev_meta.into_update_metadata(Some(&merged_metadata))),
                            document,
                            operation: Operation::Upsert,
                        },
                    });
                }
            }
        }

        for (original_id, tracker) in &trackers {
            output.push(tracker.to_log_record(original_id));
        }

        Ok(Chunk::new(Arc::from(output)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chroma_segment::{
        blockfile_record::{RecordSegmentReaderOptions, RecordSegmentReaderShard},
        test::TestDistributedSegment,
        types::{materialize_logs, MaterializeLogsResult},
    };
    use chroma_types::{
        AttachedFunction, Chunk, LogRecord, Operation, OperationRecord, SegmentShard,
        UpdateMetadataValue,
    };

    use super::*;

    fn make_executor(version_key: Option<&str>) -> RevisionHistoryExecutor {
        let params = version_key.map(|k| format!(r#"{{"version_key": "{}"}}"#, k));
        let af = AttachedFunction {
            id: chroma_types::AttachedFunctionUuid::new(),
            name: "test_revision_history".to_string(),
            function_id: uuid::Uuid::nil(),
            input_collection_id: chroma_types::CollectionUuid(uuid::Uuid::nil()),
            output_collection_name: "test_output".to_string(),
            output_collection_id: Some(chroma_types::CollectionUuid(uuid::Uuid::nil())),
            params,
            tenant_id: "test".to_string(),
            database_id: "test".to_string(),
            last_run: None,
            completion_offset: 0,
            min_records_for_invocation: 0,
            is_deleted: false,
            is_async: false,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
        };
        RevisionHistoryExecutor::from_attached_function(&af).unwrap()
    }

    fn build_record(id: &str, metadata: HashMap<String, UpdateMetadataValue>) -> LogRecord {
        build_record_with_doc(id, Operation::Upsert, metadata, Some("doc content"))
    }

    fn build_record_with_doc(
        id: &str,
        operation: Operation,
        metadata: HashMap<String, UpdateMetadataValue>,
        document: Option<&str>,
    ) -> LogRecord {
        LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: id.to_string(),
                embedding: Some(vec![0.0]),
                encoding: None,
                metadata: Some(metadata),
                document: document.map(|s| s.to_string()),
                operation,
            },
        }
    }

    fn build_delete_record(id: &str) -> LogRecord {
        LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: id.to_string(),
                embedding: None,
                encoding: None,
                metadata: None,
                document: None,
                operation: Operation::Delete,
            },
        }
    }

    async fn hydrate_records<'a>(
        materialized: &'a MaterializeLogsResult,
        record_reader: Option<&'a RecordSegmentReaderShard<'a>>,
    ) -> Vec<HydratedMaterializedLogRecord<'a, 'a>> {
        let mut hydrated_records = Vec::new();
        for borrowed_record in materialized.iter() {
            let hydrated = borrowed_record
                .hydrate(record_reader)
                .await
                .expect("hydration should succeed");
            hydrated_records.push(hydrated);
        }
        hydrated_records
    }

    /// Build a tracker record suitable for seeding a TestDistributedSegment.
    fn build_seed_tracker(original_id: &str, tracker: &RevisionTracker) -> LogRecord {
        tracker.to_log_record(original_id)
    }

    fn find_record_by_id<'a>(output: &'a Chunk<LogRecord>, id: &str) -> Option<&'a LogRecord> {
        output
            .iter()
            .find(|(r, _)| r.record.id == id)
            .map(|(r, _)| r)
    }

    #[test]
    fn test_tracker_new_generation_detection() {
        let mut tracker = RevisionTracker::new();
        assert!(tracker.is_new_generation(1));

        tracker.max_version = 1;
        tracker.generation_start_pos = 1;
        tracker.generation_start_source_ver = 1;

        assert!(!tracker.is_new_generation(2));

        tracker.max_version = 3;
        assert!(!tracker.is_new_generation(4));

        // source_version=1 IS a new generation (reset)
        assert!(tracker.is_new_generation(1));
    }

    #[test]
    fn test_default_version_key() {
        let executor = make_executor(None);
        assert_eq!(executor.version_key, "version");
    }

    #[test]
    fn test_custom_version_key() {
        let executor = make_executor(Some("rev"));
        assert_eq!(executor.version_key, "rev");
    }

    #[test]
    fn test_build_tracker_record() {
        let tracker = RevisionTracker {
            max_version: 5,
            generation_start_pos: 4,
            generation_start_source_ver: 1,
        };
        let record = tracker.to_log_record("page-1");
        assert_eq!(record.record.id, "page-1::v0");
        assert_eq!(record.record.operation, Operation::Upsert);
        assert!(record.record.document.is_none());
        assert_eq!(record.record.embedding, Some(vec![0.0]));

        let metadata = record.record.metadata.unwrap();
        assert_eq!(
            metadata.get("max_version"),
            Some(&UpdateMetadataValue::Int(5))
        );
        assert_eq!(
            metadata.get("generation_start_pos"),
            Some(&UpdateMetadataValue::Int(4))
        );
        assert_eq!(
            metadata.get("generation_start_source_ver"),
            Some(&UpdateMetadataValue::Int(1))
        );
        assert_eq!(
            metadata.get("original_id"),
            Some(&UpdateMetadataValue::Str("page-1".to_string()))
        );
    }

    #[tokio::test]
    async fn test_basic_add_archival() {
        let executor = make_executor(None);

        let records = vec![
            build_record(
                "page-1",
                HashMap::from([("version".to_string(), UpdateMetadataValue::Int(1))]),
            ),
            build_record(
                "page-2",
                HashMap::from([("version".to_string(), UpdateMetadataValue::Int(1))]),
            ),
            build_record(
                "page-3",
                HashMap::from([("version".to_string(), UpdateMetadataValue::Int(1))]),
            ),
        ];

        let logs = Chunk::new(records.into());
        let materialized =
            materialize_logs(&None, logs, None, &RecordSegmentReaderOptions::default())
                .await
                .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, None).await;
        let input = Chunk::new(Arc::from(hydrated));

        let output = executor
            .execute(vec![input], None)
            .await
            .expect("execution succeeds");

        // 3 revision records + 3 v0 trackers = 6
        assert_eq!(output.len(), 6);

        // Check revision records
        let rev1 = find_record_by_id(&output, "page-1::v1").expect("page-1::v1 should exist");
        assert_eq!(rev1.record.operation, Operation::Upsert);
        let meta = rev1.record.metadata.as_ref().unwrap();
        assert_eq!(meta.get("version"), Some(&UpdateMetadataValue::Int(1)));
        assert_eq!(
            meta.get("source_version"),
            Some(&UpdateMetadataValue::Int(1))
        );
        assert_eq!(
            meta.get("is_delete"),
            Some(&UpdateMetadataValue::Bool(false))
        );
        assert_eq!(
            meta.get("original_id"),
            Some(&UpdateMetadataValue::Str("page-1".to_string()))
        );
        assert!(meta.get("archived_at").is_some());
        assert_eq!(rev1.record.document.as_deref(), Some("doc content"));
        assert_eq!(rev1.record.embedding, Some(vec![0.0]));

        // Check v0 tracker
        let tracker = find_record_by_id(&output, "page-1::v0").expect("page-1::v0 should exist");
        let tracker_meta = tracker.record.metadata.as_ref().unwrap();
        assert_eq!(
            tracker_meta.get("max_version"),
            Some(&UpdateMetadataValue::Int(1))
        );
    }

    #[tokio::test]
    async fn test_sequential_versions_same_id() {
        let executor = make_executor(None);

        let records = vec![
            build_record(
                "page-1",
                HashMap::from([("version".to_string(), UpdateMetadataValue::Int(1))]),
            ),
            build_record(
                "page-1",
                HashMap::from([("version".to_string(), UpdateMetadataValue::Int(2))]),
            ),
            build_record(
                "page-1",
                HashMap::from([("version".to_string(), UpdateMetadataValue::Int(3))]),
            ),
        ];

        let logs = Chunk::new(records.into());
        let materialized =
            materialize_logs(&None, logs, None, &RecordSegmentReaderOptions::default())
                .await
                .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, None).await;
        let input = Chunk::new(Arc::from(hydrated));

        let output = executor
            .execute(vec![input], None)
            .await
            .expect("execution succeeds");

        // Materialization merges records with the same ID into one, so we only get 1 revision + 1 tracker
        // The final state has version=3
        let tracker = find_record_by_id(&output, "page-1::v0").expect("tracker should exist");
        let tracker_meta = tracker.record.metadata.as_ref().unwrap();
        assert_eq!(
            tracker_meta.get("max_version"),
            Some(&UpdateMetadataValue::Int(1))
        );

        let rev1 = find_record_by_id(&output, "page-1::v1").expect("page-1::v1 should exist");
        let meta = rev1.record.metadata.as_ref().unwrap();
        assert_eq!(
            meta.get("source_version"),
            Some(&UpdateMetadataValue::Int(3))
        );
    }

    #[tokio::test]
    async fn test_delete_produces_tombstone() {
        let executor = make_executor(None);

        // First, create an input segment with the record
        let mut input_segment = TestDistributedSegment::new().await;
        let initial_record = build_record(
            "page-1",
            HashMap::from([("version".to_string(), UpdateMetadataValue::Int(2))]),
        );
        Box::pin(input_segment.compact_log(Chunk::new(vec![initial_record].into()), 1)).await;

        let input_record_segment_shard =
            SegmentShard::try_from((&input_segment.record_segment, 0)).expect("valid shard index");
        let input_record_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &input_record_segment_shard,
            &input_segment.blockfile_provider,
            None,
        ))
        .await
        .expect("input record reader creation succeeds");

        // Create output segment with v0 tracker at max_version=2
        let mut output_segment = TestDistributedSegment::new().await;
        let tracker_record = build_seed_tracker(
            "page-1",
            &RevisionTracker {
                max_version: 2,
                generation_start_pos: 1,
                generation_start_source_ver: 1,
            },
        );
        Box::pin(output_segment.compact_log(Chunk::new(vec![tracker_record].into()), 1)).await;

        let output_record_segment_shard =
            SegmentShard::try_from((&output_segment.record_segment, 0)).expect("valid shard index");
        let output_record_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &output_record_segment_shard,
            &output_segment.blockfile_provider,
            None,
        ))
        .await
        .expect("output record reader creation succeeds");

        // Delete the record
        let logs = Chunk::new(vec![build_delete_record("page-1")].into());
        let materialized = materialize_logs(
            &Some(input_record_reader.clone()),
            logs,
            None,
            &RecordSegmentReaderOptions::default(),
        )
        .await
        .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, Some(&input_record_reader)).await;
        let input = Chunk::new(Arc::from(hydrated));

        let output = executor
            .execute(vec![input], Some(&output_record_reader))
            .await
            .expect("execution succeeds");

        // Should have: page-1::v3 (tombstone) + page-1::v0 (updated tracker)
        let tombstone =
            find_record_by_id(&output, "page-1::v3").expect("tombstone page-1::v3 should exist");
        let meta = tombstone.record.metadata.as_ref().unwrap();
        assert_eq!(
            meta.get("is_delete"),
            Some(&UpdateMetadataValue::Bool(true))
        );
        assert_eq!(meta.get("version"), Some(&UpdateMetadataValue::Int(3)));
        assert!(tombstone.record.document.is_none());
        assert_eq!(tombstone.record.embedding, Some(vec![0.0]));

        let tracker = find_record_by_id(&output, "page-1::v0").expect("tracker should exist");
        let tracker_meta = tracker.record.metadata.as_ref().unwrap();
        assert_eq!(
            tracker_meta.get("max_version"),
            Some(&UpdateMetadataValue::Int(3))
        );
    }

    #[tokio::test]
    async fn test_resurrection_version_collision() {
        let executor = make_executor(None);

        // Create output segment with v0 tracker at max_version=5 (simulating previous life)
        let mut output_segment = TestDistributedSegment::new().await;
        let tracker_record = build_seed_tracker(
            "page-1",
            &RevisionTracker {
                max_version: 5,
                generation_start_pos: 1,
                generation_start_source_ver: 1,
            },
        );
        Box::pin(output_segment.compact_log(Chunk::new(vec![tracker_record].into()), 1)).await;

        let output_record_segment_shard =
            SegmentShard::try_from((&output_segment.record_segment, 0)).expect("valid shard index");
        let output_record_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &output_record_segment_shard,
            &output_segment.blockfile_provider,
            None,
        ))
        .await
        .expect("output record reader creation succeeds");

        // New add with source_version=1 (app reset its counter after resurrection)
        let records = vec![build_record(
            "page-1",
            HashMap::from([("version".to_string(), UpdateMetadataValue::Int(1))]),
        )];
        let logs = Chunk::new(records.into());
        let materialized =
            materialize_logs(&None, logs, None, &RecordSegmentReaderOptions::default())
                .await
                .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, None).await;
        let input = Chunk::new(Arc::from(hydrated));

        let output = executor
            .execute(vec![input], Some(&output_record_reader))
            .await
            .expect("execution succeeds");

        // Should produce page-1::v6 (not v1!) with source_version=1
        let rev = find_record_by_id(&output, "page-1::v6").expect("page-1::v6 should exist");
        let meta = rev.record.metadata.as_ref().unwrap();
        assert_eq!(meta.get("version"), Some(&UpdateMetadataValue::Int(6)));
        assert_eq!(
            meta.get("source_version"),
            Some(&UpdateMetadataValue::Int(1))
        );

        // Tracker should show max_version=6 with updated generation info
        let tracker = find_record_by_id(&output, "page-1::v0").expect("tracker should exist");
        let tracker_meta = tracker.record.metadata.as_ref().unwrap();
        assert_eq!(
            tracker_meta.get("max_version"),
            Some(&UpdateMetadataValue::Int(6))
        );
        assert_eq!(
            tracker_meta.get("generation_start_pos"),
            Some(&UpdateMetadataValue::Int(6))
        );
        assert_eq!(
            tracker_meta.get("generation_start_source_ver"),
            Some(&UpdateMetadataValue::Int(1))
        );
    }

    #[tokio::test]
    async fn test_missing_version_key() {
        let executor = make_executor(None);

        // Record with no "version" key in metadata
        let records = vec![build_record(
            "page-1",
            HashMap::from([(
                "title".to_string(),
                UpdateMetadataValue::Str("hello".to_string()),
            )]),
        )];

        let logs = Chunk::new(records.into());
        let materialized =
            materialize_logs(&None, logs, None, &RecordSegmentReaderOptions::default())
                .await
                .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, None).await;
        let input = Chunk::new(Arc::from(hydrated));

        let output = executor
            .execute(vec![input], None)
            .await
            .expect("execution succeeds");

        let rev = find_record_by_id(&output, "page-1::v1").expect("page-1::v1 should exist");
        let meta = rev.record.metadata.as_ref().unwrap();
        assert_eq!(meta.get("version"), Some(&UpdateMetadataValue::Int(1)));
        // source_version should be absent when version_key missing
        assert!(meta.get("source_version").is_none());
        assert_eq!(
            meta.get("is_delete"),
            Some(&UpdateMetadataValue::Bool(false))
        );
        // Original metadata should be preserved
        assert_eq!(
            meta.get("title"),
            Some(&UpdateMetadataValue::Str("hello".to_string()))
        );
    }

    #[tokio::test]
    async fn test_empty_batch() {
        let executor = make_executor(None);

        let logs = Chunk::new(Vec::new().into());
        let materialized =
            materialize_logs(&None, logs, None, &RecordSegmentReaderOptions::default())
                .await
                .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, None).await;
        let input = Chunk::new(Arc::from(hydrated));

        let output = executor
            .execute(vec![input], None)
            .await
            .expect("execution succeeds");
        assert_eq!(output.len(), 0);
    }

    #[tokio::test]
    async fn test_mixed_operations() {
        let executor = make_executor(None);

        // Create input segment with a record for ID-C so we can delete it
        let mut input_segment = TestDistributedSegment::new().await;
        let initial_c = build_record(
            "id-c",
            HashMap::from([("version".to_string(), UpdateMetadataValue::Int(2))]),
        );
        Box::pin(input_segment.compact_log(Chunk::new(vec![initial_c].into()), 1)).await;

        let input_record_segment_shard =
            SegmentShard::try_from((&input_segment.record_segment, 0)).expect("valid shard index");
        let input_record_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &input_record_segment_shard,
            &input_segment.blockfile_provider,
            None,
        ))
        .await
        .expect("input record reader creation succeeds");

        // Create output segment with v0 tracker for id-c at max_version=2
        let mut output_segment = TestDistributedSegment::new().await;
        let tracker_c = build_seed_tracker(
            "id-c",
            &RevisionTracker {
                max_version: 2,
                generation_start_pos: 1,
                generation_start_source_ver: 1,
            },
        );
        Box::pin(output_segment.compact_log(Chunk::new(vec![tracker_c].into()), 1)).await;

        let output_record_segment_shard =
            SegmentShard::try_from((&output_segment.record_segment, 0)).expect("valid shard index");
        let output_record_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &output_record_segment_shard,
            &output_segment.blockfile_provider,
            None,
        ))
        .await
        .expect("output record reader creation succeeds");

        // Add for id-a, add for id-b, delete for id-c
        let logs = Chunk::new(
            vec![
                build_record(
                    "id-a",
                    HashMap::from([("version".to_string(), UpdateMetadataValue::Int(1))]),
                ),
                build_record(
                    "id-b",
                    HashMap::from([("version".to_string(), UpdateMetadataValue::Int(1))]),
                ),
                build_delete_record("id-c"),
            ]
            .into(),
        );

        let materialized = materialize_logs(
            &Some(input_record_reader.clone()),
            logs,
            None,
            &RecordSegmentReaderOptions::default(),
        )
        .await
        .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, Some(&input_record_reader)).await;
        let input = Chunk::new(Arc::from(hydrated));

        let output = executor
            .execute(vec![input], Some(&output_record_reader))
            .await
            .expect("execution succeeds");

        // id-a: v1 revision + v0 tracker
        assert!(find_record_by_id(&output, "id-a::v1").is_some());
        let tracker_a = find_record_by_id(&output, "id-a::v0").expect("id-a tracker");
        assert_eq!(
            tracker_a
                .record
                .metadata
                .as_ref()
                .unwrap()
                .get("max_version"),
            Some(&UpdateMetadataValue::Int(1))
        );

        // id-b: v1 revision + v0 tracker
        assert!(find_record_by_id(&output, "id-b::v1").is_some());
        let tracker_b = find_record_by_id(&output, "id-b::v0").expect("id-b tracker");
        assert_eq!(
            tracker_b
                .record
                .metadata
                .as_ref()
                .unwrap()
                .get("max_version"),
            Some(&UpdateMetadataValue::Int(1))
        );

        // id-c: v3 tombstone + v0 tracker (max goes from 2 to 3)
        let tombstone_c = find_record_by_id(&output, "id-c::v3").expect("id-c tombstone");
        assert_eq!(
            tombstone_c
                .record
                .metadata
                .as_ref()
                .unwrap()
                .get("is_delete"),
            Some(&UpdateMetadataValue::Bool(true))
        );
        let tracker_c_out = find_record_by_id(&output, "id-c::v0").expect("id-c tracker");
        assert_eq!(
            tracker_c_out
                .record
                .metadata
                .as_ref()
                .unwrap()
                .get("max_version"),
            Some(&UpdateMetadataValue::Int(3))
        );
    }

    #[tokio::test]
    async fn test_custom_version_key_extraction() {
        let executor = make_executor(Some("rev"));

        let records = vec![build_record(
            "page-1",
            HashMap::from([("rev".to_string(), UpdateMetadataValue::Int(42))]),
        )];

        let logs = Chunk::new(records.into());
        let materialized =
            materialize_logs(&None, logs, None, &RecordSegmentReaderOptions::default())
                .await
                .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, None).await;
        let input = Chunk::new(Arc::from(hydrated));

        let output = executor
            .execute(vec![input], None)
            .await
            .expect("execution succeeds");

        let rev = find_record_by_id(&output, "page-1::v1").expect("page-1::v1 should exist");
        let meta = rev.record.metadata.as_ref().unwrap();
        assert_eq!(
            meta.get("source_version"),
            Some(&UpdateMetadataValue::Int(42))
        );
    }

    #[test]
    fn test_next_version_for_source_deterministic() {
        let mut tracker = RevisionTracker {
            max_version: 3,
            generation_start_pos: 1,
            generation_start_source_ver: 1,
        };

        // source_version=4 → effective = 1 + (4 - 1) = 4
        assert_eq!(tracker.next_version_for_source(4), Some(4));
        assert_eq!(tracker.max_version, 4);

        // source_version=7 → effective = 1 + (7 - 1) = 7 (skipped 5, 6 — that's fine)
        assert_eq!(tracker.next_version_for_source(7), Some(7));
        assert_eq!(tracker.max_version, 7);
    }

    #[test]
    fn test_next_version_for_source_idempotent() {
        let mut tracker = RevisionTracker {
            max_version: 5,
            generation_start_pos: 1,
            generation_start_source_ver: 1,
        };

        // source_version=5 → effective = 1 + (5 - 1) = 5, but max_version is already 5
        assert_eq!(tracker.next_version_for_source(5), None);
        assert_eq!(tracker.max_version, 5);
    }

    #[test]
    fn test_lower_source_version_triggers_new_generation() {
        let mut tracker = RevisionTracker {
            max_version: 5,
            generation_start_pos: 1,
            generation_start_source_ver: 1,
        };

        // source_version=3 with max=5: this looks like a counter reset (resurrection).
        // In practice, this only happens after a delete event already bumped max_version.
        // The function correctly starts a new generation.
        assert_eq!(tracker.next_version_for_source(3), Some(6));
        assert_eq!(tracker.max_version, 6);
        assert_eq!(tracker.generation_start_pos, 6);
        assert_eq!(tracker.generation_start_source_ver, 3);
    }

    #[test]
    fn test_next_version_chunks_align() {
        // Two independent chunks for the same logical document, same generation boundaries
        let mut chunk_0 = RevisionTracker {
            max_version: 3,
            generation_start_pos: 1,
            generation_start_source_ver: 1,
        };
        let mut chunk_1 = RevisionTracker {
            max_version: 3,
            generation_start_pos: 1,
            generation_start_source_ver: 1,
        };

        // Both see source_version=4 → both compute effective=4
        assert_eq!(chunk_0.next_version_for_source(4), Some(4));
        assert_eq!(chunk_1.next_version_for_source(4), Some(4));

        // Both see source_version=5 → both compute effective=5
        assert_eq!(chunk_0.next_version_for_source(5), Some(5));
        assert_eq!(chunk_1.next_version_for_source(5), Some(5));
    }

    #[test]
    fn test_next_version_chunks_different_compaction_cycles() {
        // chunk-0 was compacted with source_version=4 already, chunk-1 was not
        let mut chunk_0 = RevisionTracker {
            max_version: 4,
            generation_start_pos: 1,
            generation_start_source_ver: 1,
        };
        let mut chunk_1 = RevisionTracker {
            max_version: 3,
            generation_start_pos: 1,
            generation_start_source_ver: 1,
        };

        // Now both see source_version=5 in the next cycle
        assert_eq!(chunk_0.next_version_for_source(5), Some(5));
        assert_eq!(chunk_1.next_version_for_source(5), Some(5));
        // chunk_1 skipped effective=4 (it was never seen) — that's expected; it was
        // a version that existed only transiently between compactions for chunk_1.
        // Both are now aligned at max_version=5.
        assert_eq!(chunk_0.max_version, 5);
        assert_eq!(chunk_1.max_version, 5);
    }

    #[test]
    fn test_next_version_resurrection_across_chunks() {
        // Both chunks had max_version=5 from gen 1, now the record is re-created
        let mut chunk_0 = RevisionTracker {
            max_version: 5,
            generation_start_pos: 1,
            generation_start_source_ver: 1,
        };
        let mut chunk_1 = RevisionTracker {
            max_version: 5,
            generation_start_pos: 1,
            generation_start_source_ver: 1,
        };

        // Both see source_version=1 after resurrection
        assert_eq!(chunk_0.next_version_for_source(1), Some(6));
        assert_eq!(chunk_1.next_version_for_source(1), Some(6));

        // New generation: start_pos=6, start_source_ver=1
        assert_eq!(chunk_0.generation_start_pos, 6);
        assert_eq!(chunk_1.generation_start_pos, 6);

        // Next version in new generation
        assert_eq!(chunk_0.next_version_for_source(2), Some(7));
        assert_eq!(chunk_1.next_version_for_source(2), Some(7));
    }

    #[tokio::test]
    async fn test_idempotent_recompaction() {
        let executor = make_executor(None);

        // Simulate: tracker already at max_version=3, gen starts at pos=1, source_ver=1
        // Source record arrives with version=3 (already archived)
        let mut output_segment = TestDistributedSegment::new().await;
        let tracker_record = build_seed_tracker(
            "page-1",
            &RevisionTracker {
                max_version: 3,
                generation_start_pos: 1,
                generation_start_source_ver: 1,
            },
        );
        Box::pin(output_segment.compact_log(Chunk::new(vec![tracker_record].into()), 1)).await;

        let output_record_segment_shard =
            SegmentShard::try_from((&output_segment.record_segment, 0)).expect("valid shard index");
        let output_record_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &output_record_segment_shard,
            &output_segment.blockfile_provider,
            None,
        ))
        .await
        .expect("output record reader creation succeeds");

        // Record with source_version=3 (already archived — effective would be 3 <= max 3)
        let records = vec![build_record(
            "page-1",
            HashMap::from([("version".to_string(), UpdateMetadataValue::Int(3))]),
        )];
        let logs = Chunk::new(records.into());
        let materialized =
            materialize_logs(&None, logs, None, &RecordSegmentReaderOptions::default())
                .await
                .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, None).await;
        let input = Chunk::new(Arc::from(hydrated));

        let output = executor
            .execute(vec![input], Some(&output_record_reader))
            .await
            .expect("execution succeeds");

        // Should only have the tracker (no new revision emitted)
        assert!(find_record_by_id(&output, "page-1::v3").is_none());
        assert!(find_record_by_id(&output, "page-1::v4").is_none());
        let tracker = find_record_by_id(&output, "page-1::v0").expect("tracker should exist");
        let tracker_meta = tracker.record.metadata.as_ref().unwrap();
        assert_eq!(
            tracker_meta.get("max_version"),
            Some(&UpdateMetadataValue::Int(3))
        );
    }

    #[tokio::test]
    async fn test_version_skips_between_compactions() {
        let executor = make_executor(None);

        // Tracker at max_version=2 (source versions 1,2 archived). Source jumped to 5.
        let mut output_segment = TestDistributedSegment::new().await;
        let tracker_record = build_seed_tracker(
            "page-1",
            &RevisionTracker {
                max_version: 2,
                generation_start_pos: 1,
                generation_start_source_ver: 1,
            },
        );
        Box::pin(output_segment.compact_log(Chunk::new(vec![tracker_record].into()), 1)).await;

        let output_record_segment_shard =
            SegmentShard::try_from((&output_segment.record_segment, 0)).expect("valid shard index");
        let output_record_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &output_record_segment_shard,
            &output_segment.blockfile_provider,
            None,
        ))
        .await
        .expect("output record reader creation succeeds");

        // Materialized record has source_version=5 (versions 3,4 were never seen)
        let records = vec![build_record(
            "page-1",
            HashMap::from([("version".to_string(), UpdateMetadataValue::Int(5))]),
        )];
        let logs = Chunk::new(records.into());
        let materialized =
            materialize_logs(&None, logs, None, &RecordSegmentReaderOptions::default())
                .await
                .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, None).await;
        let input = Chunk::new(Arc::from(hydrated));

        let output = executor
            .execute(vec![input], Some(&output_record_reader))
            .await
            .expect("execution succeeds");

        // effective = 1 + (5 - 1) = 5. Versions 3,4 are gaps — that's correct,
        // those versions were never observed by the function.
        let rev = find_record_by_id(&output, "page-1::v5").expect("page-1::v5 should exist");
        let meta = rev.record.metadata.as_ref().unwrap();
        assert_eq!(meta.get("version"), Some(&UpdateMetadataValue::Int(5)));
        assert_eq!(
            meta.get("source_version"),
            Some(&UpdateMetadataValue::Int(5))
        );

        let tracker = find_record_by_id(&output, "page-1::v0").expect("tracker should exist");
        let tracker_meta = tracker.record.metadata.as_ref().unwrap();
        assert_eq!(
            tracker_meta.get("max_version"),
            Some(&UpdateMetadataValue::Int(5))
        );
    }

    #[test]
    fn test_delete_tombstone_aligns_across_chunks() {
        // Two chunks with different max_versions (chunk-1 missed some compactions).
        // Both are "deleted" with the same last source_version=5 on the record.
        // virtual_sv = 5 + 1 = 6. effective = 1 + (6-1) = 6 for BOTH.

        let mut chunk_0 = RevisionTracker {
            max_version: 5,
            generation_start_pos: 1,
            generation_start_source_ver: 1,
        };
        let mut chunk_1 = RevisionTracker {
            max_version: 3, // missed sv=4,5 compactions
            generation_start_pos: 1,
            generation_start_source_ver: 1,
        };

        // Delete uses virtual_sv = last_source_version + 1 = 6
        let virtual_sv = 5 + 1; // source_version was 5 on the deleted record
        let eff_0 = chunk_0.next_version_for_source(virtual_sv);
        let eff_1 = chunk_1.next_version_for_source(virtual_sv);

        // Both produce the same effective_version
        assert_eq!(eff_0, Some(6));
        assert_eq!(eff_1, Some(6));
        assert_eq!(chunk_0.max_version, 6);
        assert_eq!(chunk_1.max_version, 6);

        // After resurrection (sv=1), both start new gen at same position
        let res_0 = chunk_0.next_version_for_source(1);
        let res_1 = chunk_1.next_version_for_source(1);
        assert_eq!(res_0, Some(7));
        assert_eq!(res_1, Some(7));
        assert_eq!(chunk_0.generation_start_pos, 7);
        assert_eq!(chunk_1.generation_start_pos, 7);
    }
}
