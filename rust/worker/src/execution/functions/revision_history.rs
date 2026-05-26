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

#[derive(Debug)]
struct TrackerState {
    max_version: i64,
    current_life_start_pos: i64,
    current_life_start_source_ver: i64,
}

impl TrackerState {
    fn new() -> Self {
        Self {
            max_version: 0,
            current_life_start_pos: 0,
            current_life_start_source_ver: 0,
        }
    }

    fn is_new_generation(&self, source_version: i64) -> bool {
        if self.max_version == 0 {
            return true;
        }
        let expected_max_source_ver =
            self.current_life_start_source_ver + (self.max_version - self.current_life_start_pos);
        source_version <= expected_max_source_ver
            && !(self.current_life_start_pos == 0 && self.current_life_start_source_ver == 0)
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

    async fn read_tracker(
        output_reader: Option<&RecordSegmentReaderShard<'_>>,
        original_id: &str,
    ) -> TrackerState {
        let Some(reader) = output_reader else {
            return TrackerState::new();
        };

        let tracker_id = format!("{original_id}::v0");
        let offset_id = match reader
            .get_offset_id_for_user_id(&tracker_id, &RecordSegmentReaderOptions::default())
            .await
        {
            Ok(Some(id)) => id,
            _ => return TrackerState::new(),
        };

        let data_record = match reader.get_data_for_offset_id(offset_id).await {
            Ok(Some(record)) => record,
            _ => return TrackerState::new(),
        };

        let metadata = match &data_record.metadata {
            Some(m) => m,
            None => return TrackerState::new(),
        };

        let max_version = match metadata.get("max_version") {
            Some(MetadataValue::Int(v)) => *v,
            _ => 0,
        };
        let current_life_start_pos = match metadata.get("current_life_start_pos") {
            Some(MetadataValue::Int(v)) => *v,
            _ => 0,
        };
        let current_life_start_source_ver = match metadata.get("current_life_start_source_ver") {
            Some(MetadataValue::Int(v)) => *v,
            _ => 0,
        };

        TrackerState {
            max_version,
            current_life_start_pos,
            current_life_start_source_ver,
        }
    }

    fn build_tracker_record(original_id: &str, tracker: &TrackerState) -> LogRecord {
        let mut metadata = HashMap::new();
        metadata.insert(
            "max_version".to_string(),
            UpdateMetadataValue::Int(tracker.max_version),
        );
        metadata.insert(
            "current_life_start_pos".to_string(),
            UpdateMetadataValue::Int(tracker.current_life_start_pos),
        );
        metadata.insert(
            "current_life_start_source_ver".to_string(),
            UpdateMetadataValue::Int(tracker.current_life_start_source_ver),
        );
        metadata.insert(
            "original_id".to_string(),
            UpdateMetadataValue::Str(original_id.to_string()),
        );

        LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: format!("{original_id}::v0"),
                embedding: None,
                encoding: None,
                metadata: Some(metadata),
                document: None,
                operation: Operation::Upsert,
            },
        }
    }
}

#[async_trait]
impl AttachedFunctionExecutor for RevisionHistoryExecutor {
    async fn execute(
        &self,
        input_records: Chunk<HydratedMaterializedLogRecord<'_, '_>>,
        output_reader: Option<&RecordSegmentReaderShard<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        if input_records.is_empty() {
            return Ok(Chunk::new(Arc::from(Vec::new())));
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut trackers: HashMap<String, TrackerState> = HashMap::new();
        let mut output = Vec::new();

        for (record, _index) in input_records.iter() {
            let original_id = record.get_user_id().to_string();

            if !trackers.contains_key(&original_id) {
                let state = Self::read_tracker(output_reader, &original_id).await;
                trackers.insert(original_id.clone(), state);
            }

            let tracker = trackers.get_mut(&original_id).unwrap();
            let effective_version = tracker.max_version + 1;
            tracker.max_version = effective_version;

            let composite_id = format!("{original_id}::v{effective_version}");

            if record.get_operation() == MaterializedLogOperation::DeleteExisting {
                let mut metadata = HashMap::new();
                metadata.insert(
                    "original_id".to_string(),
                    UpdateMetadataValue::Str(original_id.clone()),
                );
                metadata.insert(
                    "version".to_string(),
                    UpdateMetadataValue::Int(effective_version),
                );
                metadata.insert("archived_at".to_string(), UpdateMetadataValue::Int(now));
                metadata.insert("is_delete".to_string(), UpdateMetadataValue::Bool(true));

                output.push(LogRecord {
                    log_offset: 0,
                    record: OperationRecord {
                        id: composite_id,
                        embedding: None,
                        encoding: None,
                        metadata: Some(metadata),
                        document: None,
                        operation: Operation::Upsert,
                    },
                });
            } else {
                let merged_metadata = record.merged_metadata();
                let source_version = match merged_metadata.get(&self.version_key) {
                    Some(MetadataValue::Int(v)) => Some(*v),
                    _ => None,
                };

                if let Some(sv) = source_version {
                    if tracker.is_new_generation(sv) {
                        tracker.current_life_start_pos = effective_version;
                        tracker.current_life_start_source_ver = sv;
                    }
                }

                let mut out_metadata = HashMap::new();
                out_metadata.insert(
                    "original_id".to_string(),
                    UpdateMetadataValue::Str(original_id.clone()),
                );
                out_metadata.insert(
                    "version".to_string(),
                    UpdateMetadataValue::Int(effective_version),
                );
                out_metadata.insert("archived_at".to_string(), UpdateMetadataValue::Int(now));
                out_metadata.insert("is_delete".to_string(), UpdateMetadataValue::Bool(false));

                if let Some(sv) = source_version {
                    out_metadata.insert("source_version".to_string(), UpdateMetadataValue::Int(sv));
                }

                for (key, value) in &merged_metadata {
                    let update_value = match value {
                        MetadataValue::Bool(b) => UpdateMetadataValue::Bool(*b),
                        MetadataValue::Int(i) => UpdateMetadataValue::Int(*i),
                        MetadataValue::Float(f) => UpdateMetadataValue::Float(*f),
                        MetadataValue::Str(s) => UpdateMetadataValue::Str(s.clone()),
                        _ => continue,
                    };
                    out_metadata.entry(key.clone()).or_insert(update_value);
                }

                let document = record.merged_document_ref().map(|s| s.to_string());

                output.push(LogRecord {
                    log_offset: 0,
                    record: OperationRecord {
                        id: composite_id,
                        embedding: None,
                        encoding: None,
                        metadata: Some(out_metadata),
                        document,
                        operation: Operation::Upsert,
                    },
                });
            }
        }

        for (original_id, tracker) in &trackers {
            output.push(Self::build_tracker_record(original_id, tracker));
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

    /// Build a tracker record with a dummy embedding, suitable for seeding a TestDistributedSegment
    /// (which requires embeddings for materialization).
    fn build_seed_tracker(original_id: &str, tracker: &TrackerState) -> LogRecord {
        let mut record = RevisionHistoryExecutor::build_tracker_record(original_id, tracker);
        record.record.embedding = Some(vec![0.0]);
        record
    }

    fn find_record_by_id<'a>(output: &'a Chunk<LogRecord>, id: &str) -> Option<&'a LogRecord> {
        output
            .iter()
            .find(|(r, _)| r.record.id == id)
            .map(|(r, _)| r)
    }

    #[test]
    fn test_tracker_new_generation_detection() {
        let mut tracker = TrackerState::new();
        assert!(tracker.is_new_generation(1));

        tracker.max_version = 1;
        tracker.current_life_start_pos = 1;
        tracker.current_life_start_source_ver = 1;

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
        let tracker = TrackerState {
            max_version: 5,
            current_life_start_pos: 4,
            current_life_start_source_ver: 1,
        };
        let record = RevisionHistoryExecutor::build_tracker_record("page-1", &tracker);
        assert_eq!(record.record.id, "page-1::v0");
        assert_eq!(record.record.operation, Operation::Upsert);
        assert!(record.record.document.is_none());
        assert!(record.record.embedding.is_none());

        let metadata = record.record.metadata.unwrap();
        assert_eq!(
            metadata.get("max_version"),
            Some(&UpdateMetadataValue::Int(5))
        );
        assert_eq!(
            metadata.get("current_life_start_pos"),
            Some(&UpdateMetadataValue::Int(4))
        );
        assert_eq!(
            metadata.get("current_life_start_source_ver"),
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
            .execute(input, None)
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
        assert!(rev1.record.embedding.is_none());

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
            .execute(input, None)
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
            &TrackerState {
                max_version: 2,
                current_life_start_pos: 1,
                current_life_start_source_ver: 1,
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
            .execute(input, Some(&output_record_reader))
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
        assert!(tombstone.record.embedding.is_none());

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
            &TrackerState {
                max_version: 5,
                current_life_start_pos: 1,
                current_life_start_source_ver: 1,
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
            .execute(input, Some(&output_record_reader))
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
            tracker_meta.get("current_life_start_pos"),
            Some(&UpdateMetadataValue::Int(6))
        );
        assert_eq!(
            tracker_meta.get("current_life_start_source_ver"),
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
            .execute(input, None)
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
            .execute(input, None)
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
            &TrackerState {
                max_version: 2,
                current_life_start_pos: 1,
                current_life_start_source_ver: 1,
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
            .execute(input, Some(&output_record_reader))
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
            .execute(input, None)
            .await
            .expect("execution succeeds");

        let rev = find_record_by_id(&output, "page-1::v1").expect("page-1::v1 should exist");
        let meta = rev.record.metadata.as_ref().unwrap();
        assert_eq!(
            meta.get("source_version"),
            Some(&UpdateMetadataValue::Int(42))
        );
    }
}
