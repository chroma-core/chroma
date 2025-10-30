//! This implements a statistics module that returns the token frequency of metadata.
//!
//! The core idea is the following: For each key-value pair associated with a record, aggregate so
//! (key, value) -> count.  This gives a count of how frequently each key appears.
//!
//! For now it's not incremental.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_segment::blockfile_record::RecordSegmentReader;
use chroma_types::{
    Chunk, LogRecord, MetadataValue, Operation, OperationRecord, UpdateMetadataValue,
};
use futures::StreamExt;

use crate::execution::operators::execute_task::AttachedFunctionExecutor;

/// Create an accumulator for statistics.
pub trait StatisticsFunctionFactory: std::fmt::Debug + Send + Sync {
    fn create(&self) -> Box<dyn StatisticsFunction>;
}

/// Accumulate statistics.  Must be an associative and commutative over a sequence of `observe` calls.
pub trait StatisticsFunction: std::fmt::Debug + Send {
    fn observe(&mut self, log_record: &LogRecord);
    fn output(&self) -> UpdateMetadataValue;
}

#[derive(Debug, Default)]
pub struct CounterFunctionFactory;

impl StatisticsFunctionFactory for CounterFunctionFactory {
    fn create(&self) -> Box<dyn StatisticsFunction> {
        Box::new(CounterFunction::default())
    }
}

#[derive(Debug, Default)]
pub struct CounterFunction {
    acc: i64,
}

impl StatisticsFunction for CounterFunction {
    fn observe(&mut self, _: &LogRecord) {
        self.acc = self.acc.saturating_add(1);
    }

    fn output(&self) -> UpdateMetadataValue {
        UpdateMetadataValue::Int(self.acc)
    }
}

/// Canonical representation of metadata values tracked by the statistics executor.
#[derive(Clone, Debug)]
enum StatisticsValue {
    /// Boolean metadata value associated with a record.
    Bool(bool),
    /// Integer metadata value associated with a record.
    Int(i64),
    /// Floating point metadata value associated with a record.
    Float(f64),
    /// String metadata value associated with a record.
    Str(String),
    /// Sparse vector index observed in metadata.
    SparseVector(u32),
}

impl StatisticsValue {
    /// A stable type string for the statistics's type.
    fn stable_type(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::Str(_) => "str",
            Self::SparseVector(_) => "sparse",
        }
    }

    /// A stable type prefix for stable_string format.
    fn type_prefix(&self) -> &'static str {
        match self {
            Self::Bool(_) => "b",
            Self::Int(_) => "i",
            Self::Float(_) => "f",
            Self::Str(_) => "s",
            Self::SparseVector(_) => "sv",
        }
    }

    /// A stable representation of the statistics's value.
    fn stable_value(&self) -> String {
        match self {
            Self::Bool(b) => {
                format!("{b}")
            }
            Self::Int(i) => {
                format!("{i}")
            }
            Self::Str(s) => s.clone(),
            Self::Float(f) => format!("{f:.16e}"),
            Self::SparseVector(index) => {
                format!("{index}")
            }
        }
    }

    /// A stable string representation of a statistics value with type tag.
    /// Separate so display repr can change.
    fn stable_string(&self) -> String {
        format!("{}:{}", self.type_prefix(), self.stable_value())
    }

    /// Convert MetadataValue to a vector of StatisticsValue.
    /// Returns a vector because sparse vectors expand to multiple values.
    fn from_metadata_value(value: &MetadataValue) -> Vec<StatisticsValue> {
        match value {
            MetadataValue::Bool(b) => vec![StatisticsValue::Bool(*b)],
            MetadataValue::Int(i) => vec![StatisticsValue::Int(*i)],
            MetadataValue::Float(f) => vec![StatisticsValue::Float(*f)],
            MetadataValue::Str(s) => vec![StatisticsValue::Str(s.clone())],
            MetadataValue::SparseVector(sparse) => sparse
                .indices
                .iter()
                .map(|index| StatisticsValue::SparseVector(*index))
                .collect(),
        }
    }
}

impl std::fmt::Display for StatisticsValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.stable_string())
    }
}

impl PartialEq for StatisticsValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(lhs), Self::Bool(rhs)) => lhs == rhs,
            (Self::Int(lhs), Self::Int(rhs)) => lhs == rhs,
            (Self::Float(lhs), Self::Float(rhs)) => lhs.to_bits() == rhs.to_bits(),
            (Self::Str(lhs), Self::Str(rhs)) => lhs == rhs,
            (Self::SparseVector(lhs), Self::SparseVector(rhs)) => lhs == rhs,
            _ => false,
        }
    }
}

impl Eq for StatisticsValue {}

impl Hash for StatisticsValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            StatisticsValue::Bool(value) => value.hash(state),
            StatisticsValue::Int(value) => value.hash(state),
            StatisticsValue::Float(value) => value.to_bits().hash(state),
            StatisticsValue::Str(value) => value.hash(state),
            StatisticsValue::SparseVector(value) => value.hash(state),
        }
    }
}

/// Task executor that aggregates metadata value frequencies for the statistics task.
#[derive(Debug)]
pub struct StatisticsFunctionExecutor(pub Box<dyn StatisticsFunctionFactory>);

#[async_trait]
impl AttachedFunctionExecutor for StatisticsFunctionExecutor {
    async fn execute(
        &self,
        input_records: Chunk<LogRecord>,
        output_reader: Option<&RecordSegmentReader<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        let mut counts: HashMap<String, HashMap<StatisticsValue, Box<dyn StatisticsFunction>>> =
            HashMap::default();
        for (log_record, _) in input_records.iter() {
            if matches!(log_record.record.operation, Operation::Delete) {
                continue;
            }

            if let Some(update_metadata) = log_record.record.metadata.as_ref() {
                for (key, update_value) in update_metadata.iter() {
                    let value: Option<MetadataValue> = update_value.try_into().ok();
                    if let Some(value) = value {
                        let inner_map = counts.entry(key.clone()).or_default();
                        for stats_value in StatisticsValue::from_metadata_value(&value) {
                            inner_map
                                .entry(stats_value)
                                .or_insert_with(|| self.0.create())
                                .observe(log_record);
                        }
                    }
                }
            }
        }
        let mut keys = HashSet::with_capacity(counts.len());
        let mut records = Vec::with_capacity(counts.len());
        for (key, inner_map) in counts.into_iter() {
            for (stats_value, count) in inner_map.into_iter() {
                let stable_value = stats_value.stable_value();
                let stable_string = stats_value.stable_string();
                let record_id = format!("{key}::{stable_string}");
                let document = format!("statistics about {key} for {stable_string}");

                let mut metadata = HashMap::with_capacity(4);
                metadata.insert("count".to_string(), count.output());
                metadata.insert("key".to_string(), UpdateMetadataValue::Str(key.clone()));
                metadata.insert(
                    "type".to_string(),
                    UpdateMetadataValue::Str(stats_value.stable_type().to_string()),
                );
                metadata.insert("value".to_string(), UpdateMetadataValue::Str(stable_value));

                keys.insert(record_id.clone());
                records.push(LogRecord {
                    log_offset: 0,
                    record: OperationRecord {
                        id: record_id,
                        // NOTE(rescrv): We need to provide some embedding, so give a zero.
                        embedding: Some(vec![0.0]),
                        encoding: None,
                        metadata: Some(metadata),
                        document: Some(document),
                        operation: Operation::Upsert,
                    },
                });
            }
        }
        // Delete records we didn't recreate.
        if let Some(output_reader) = output_reader {
            let max_offset_id = output_reader.get_max_offset_id();
            let mut stream = output_reader.get_data_stream(0..=max_offset_id).await;

            while let Some(record) = stream.next().await {
                let (_, record) = record?;
                if !keys.contains(record.id) {
                    records.push(LogRecord {
                        log_offset: 0,
                        record: OperationRecord {
                            id: record.id.to_owned(),
                            embedding: None,
                            encoding: None,
                            metadata: None,
                            document: None,
                            operation: Operation::Delete,
                        },
                    })
                }
            }
        }
        Ok(Chunk::new(records.into()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chroma_segment::{blockfile_record::RecordSegmentReader, test::TestDistributedSegment};
    use chroma_types::{
        Chunk, LogRecord, Operation, OperationRecord, SparseVector, UpdateMetadata,
        UpdateMetadataValue,
    };

    use super::*;

    fn build_record(id: &str, metadata: HashMap<String, UpdateMetadataValue>) -> LogRecord {
        build_record_with_operation(id, Operation::Upsert, metadata)
    }

    fn build_record_with_operation(
        id: &str,
        operation: Operation,
        metadata: HashMap<String, UpdateMetadataValue>,
    ) -> LogRecord {
        LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: id.to_string(),
                embedding: None,
                encoding: None,
                metadata: Some(metadata),
                document: None,
                operation,
            },
        }
    }

    fn extract_metadata_tuple(metadata: &UpdateMetadata) -> (i64, String, String, String) {
        let count = match metadata.get("count") {
            Some(UpdateMetadataValue::Int(value)) => *value,
            other => panic!("unexpected count metadata: {other:?}"),
        };
        let key = match metadata.get("key") {
            Some(UpdateMetadataValue::Str(value)) => value.clone(),
            other => panic!("unexpected key metadata: {other:?}"),
        };
        let value_type = match metadata.get("type") {
            Some(UpdateMetadataValue::Str(value)) => value.clone(),
            other => panic!("unexpected type metadata: {other:?}"),
        };
        let value = match metadata.get("value") {
            Some(UpdateMetadataValue::Str(value)) => value.clone(),
            other => panic!("unexpected value metadata: {other:?}"),
        };
        (count, key, value_type, value)
    }

    fn collect_statistics_map(
        output: &Chunk<LogRecord>,
    ) -> HashMap<String, (i64, String, String, String)> {
        let mut actual: HashMap<String, (i64, String, String, String)> = HashMap::new();
        for (log_record, _) in output.iter() {
            let record = &log_record.record;
            assert_eq!(record.operation, Operation::Upsert);
            assert_eq!(record.embedding.as_deref(), Some(&[0.0][..]));

            let metadata = record
                .metadata
                .as_ref()
                .expect("statistics executor always sets metadata");

            actual.insert(record.id.clone(), extract_metadata_tuple(metadata));
        }
        actual
    }

    fn build_statistics_metadata(
        count: i64,
        key: &str,
        value_type: &str,
        value: &str,
    ) -> UpdateMetadata {
        HashMap::from([
            ("count".to_string(), UpdateMetadataValue::Int(count)),
            ("key".to_string(), UpdateMetadataValue::Str(key.to_string())),
            (
                "type".to_string(),
                UpdateMetadataValue::Str(value_type.to_string()),
            ),
            (
                "value".to_string(),
                UpdateMetadataValue::Str(value.to_string()),
            ),
        ])
    }

    fn build_statistics_record(id: &str, metadata: UpdateMetadata, document: &str) -> LogRecord {
        LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: id.to_string(),
                embedding: Some(vec![0.0]),
                encoding: None,
                metadata: Some(metadata),
                document: Some(document.to_string()),
                operation: Operation::Upsert,
            },
        }
    }

    fn build_complete_statistics_record(
        key: &str,
        value: &str,
        value_type: &str,
        type_prefix: &str,
        count: i64,
    ) -> LogRecord {
        let metadata = build_statistics_metadata(count, key, value_type, value);
        let id = format!("{key}::{type_prefix}:{value}");
        let document = format!("statistics about {key} for {type_prefix}:{value}");
        build_statistics_record(&id, metadata, &document)
    }

    fn partition_output(
        output: &Chunk<LogRecord>,
    ) -> (HashMap<String, OperationRecord>, Vec<String>) {
        let mut upserts: HashMap<String, OperationRecord> = HashMap::new();
        let mut deletes: Vec<String> = Vec::new();

        for (log_record, _) in output.iter() {
            match log_record.record.operation {
                Operation::Upsert => {
                    upserts.insert(log_record.record.id.clone(), log_record.record.clone());
                }
                Operation::Delete => {
                    deletes.push(log_record.record.id.clone());
                    assert!(log_record.record.metadata.is_none());
                    assert!(log_record.record.embedding.is_none());
                }
                other => panic!("unexpected operation in statistics output: {:?}", other),
            }
        }

        (upserts, deletes)
    }

    fn partition_output_expect_no_upserts(output: &Chunk<LogRecord>) -> Vec<String> {
        let mut deletes: Vec<String> = Vec::new();

        for (log_record, _) in output.iter() {
            match log_record.record.operation {
                Operation::Delete => {
                    deletes.push(log_record.record.id.clone());
                    assert!(log_record.record.metadata.is_none());
                    assert!(log_record.record.embedding.is_none());
                }
                Operation::Upsert => {
                    panic!("unexpected upsert in empty-input statistics output");
                }
                other => panic!("unexpected operation in statistics output: {:?}", other),
            }
        }

        deletes
    }

    #[tokio::test]
    async fn statistics_executor_counts_all_metadata_values() {
        let executor = StatisticsFunctionExecutor(Box::new(CounterFunctionFactory));

        let record_one = build_record(
            "record-1",
            HashMap::from([
                ("bool_key".to_string(), UpdateMetadataValue::Bool(true)),
                ("int_key".to_string(), UpdateMetadataValue::Int(7)),
                ("float_key".to_string(), UpdateMetadataValue::Float(2.5)),
                (
                    "str_key".to_string(),
                    UpdateMetadataValue::Str("alpha".to_string()),
                ),
                (
                    "sparse_key".to_string(),
                    UpdateMetadataValue::SparseVector(SparseVector::new(
                        vec![1, 3],
                        vec![0.25, 0.75],
                    )),
                ),
            ]),
        );
        let record_two = build_record(
            "record-2",
            HashMap::from([
                ("bool_key".to_string(), UpdateMetadataValue::Bool(false)),
                ("int_key".to_string(), UpdateMetadataValue::Int(7)),
                ("float_key".to_string(), UpdateMetadataValue::Float(2.5)),
                (
                    "str_key".to_string(),
                    UpdateMetadataValue::Str("alpha".to_string()),
                ),
                (
                    "sparse_key".to_string(),
                    UpdateMetadataValue::SparseVector(SparseVector::new(vec![3], vec![0.5])),
                ),
            ]),
        );

        let input = Chunk::new(vec![record_one, record_two].into());

        let output = executor
            .execute(input, None)
            .await
            .expect("execution succeeds");

        let actual = collect_statistics_map(&output);

        let float_value = format!("{:.16e}", 2.5_f64);
        let expected: HashMap<String, (i64, String, String, String)> = HashMap::from([
            (
                format!("bool_key::b:{}", true),
                (
                    1,
                    "bool_key".to_string(),
                    "bool".to_string(),
                    format!("{}", true),
                ),
            ),
            (
                format!("bool_key::b:{}", false),
                (
                    1,
                    "bool_key".to_string(),
                    "bool".to_string(),
                    format!("{}", false),
                ),
            ),
            (
                format!("int_key::i:{}", 7),
                (
                    2,
                    "int_key".to_string(),
                    "int".to_string(),
                    format!("{}", 7),
                ),
            ),
            (
                format!("float_key::f:{float_value}"),
                (
                    2,
                    "float_key".to_string(),
                    "float".to_string(),
                    float_value.clone(),
                ),
            ),
            (
                format!("str_key::s:{}", "alpha"),
                (
                    2,
                    "str_key".to_string(),
                    "str".to_string(),
                    "alpha".to_string(),
                ),
            ),
            (
                format!("sparse_key::sv:{}", 1),
                (
                    1,
                    "sparse_key".to_string(),
                    "sparse".to_string(),
                    format!("{}", 1),
                ),
            ),
            (
                format!("sparse_key::sv:{}", 3),
                (
                    2,
                    "sparse_key".to_string(),
                    "sparse".to_string(),
                    format!("{}", 3),
                ),
            ),
        ]);

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn statistics_executor_groups_nan_float_values() {
        let executor = StatisticsFunctionExecutor(Box::new(CounterFunctionFactory));

        let record_one = build_record(
            "nan-1",
            HashMap::from([(
                "float_key".to_string(),
                UpdateMetadataValue::Float(f64::NAN),
            )]),
        );
        let record_two = build_record(
            "nan-2",
            HashMap::from([(
                "float_key".to_string(),
                UpdateMetadataValue::Float(f64::NAN),
            )]),
        );

        let input = Chunk::new(vec![record_one, record_two].into());
        let output = executor
            .execute(input, None)
            .await
            .expect("execution succeeds");

        let actual = collect_statistics_map(&output);
        assert_eq!(actual.len(), 1);

        let float_string = format!("{:.16e}", f64::NAN);
        let expected_id = format!("float_key::f:{float_string}");
        let expected_entry = (
            2,
            "float_key".to_string(),
            "float".to_string(),
            float_string.clone(),
        );
        assert_eq!(
            actual
                .get(&expected_id)
                .expect("NaN metadata should be grouped under a single entry"),
            &expected_entry
        );
    }

    #[tokio::test]
    async fn statistics_executor_ignores_delete_operations() {
        let executor = StatisticsFunctionExecutor(Box::new(CounterFunctionFactory));

        let upsert_record = build_record(
            "record-upsert",
            HashMap::from([("bool_key".to_string(), UpdateMetadataValue::Bool(true))]),
        );
        let delete_record = build_record_with_operation(
            "record-delete",
            Operation::Delete,
            HashMap::from([("bool_key".to_string(), UpdateMetadataValue::Bool(false))]),
        );

        let input = Chunk::new(vec![upsert_record, delete_record].into());
        let output = executor
            .execute(input, None)
            .await
            .expect("execution succeeds");

        let actual = collect_statistics_map(&output);
        assert_eq!(actual.len(), 1);

        let true_id = format!("bool_key::b:{}", true);
        assert!(
            actual.contains_key(&true_id),
            "upserted metadata should still be counted"
        );

        let false_id = format!("bool_key::b:{}", false);
        assert!(
            !actual.contains_key(&false_id),
            "delete metadata should be ignored by the statistics executor"
        );
    }

    #[tokio::test]
    async fn statistics_executor_handles_empty_sparse_vectors() {
        let executor = StatisticsFunctionExecutor(Box::new(CounterFunctionFactory));

        let record = build_record(
            "sparse-empty",
            HashMap::from([(
                "sparse_key".to_string(),
                UpdateMetadataValue::SparseVector(SparseVector::new(
                    Vec::<u32>::new(),
                    Vec::<f32>::new(),
                )),
            )]),
        );

        let input = Chunk::new(vec![record].into());
        let output = executor
            .execute(input, None)
            .await
            .expect("execution succeeds");

        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn statistics_executor_skips_unconvertible_metadata_values() {
        let executor = StatisticsFunctionExecutor(Box::new(CounterFunctionFactory));

        let record = build_record(
            "only",
            HashMap::from([("skip".to_string(), UpdateMetadataValue::None)]),
        );

        let input = Chunk::new(vec![record].into());

        let output = executor
            .execute(input, None)
            .await
            .expect("execution succeeds");

        assert_eq!(output.total_len(), 0);
        assert_eq!(output.len(), 0);
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn statistics_executor_deletes_stale_records_from_segment() {
        let executor = StatisticsFunctionExecutor(Box::new(CounterFunctionFactory));

        let mut test_segment = TestDistributedSegment::new().await;

        let stale_record = build_complete_statistics_record("obsolete_key", "true", "bool", "b", 1);

        let fresh_record = build_complete_statistics_record("fresh_key", "1", "int", "i", 3);

        let existing_chunk = Chunk::new(vec![stale_record, fresh_record].into());

        Box::pin(test_segment.compact_log(existing_chunk, 1)).await;

        let record_reader = Box::pin(RecordSegmentReader::from_segment(
            &test_segment.record_segment,
            &test_segment.blockfile_provider,
        ))
        .await
        .expect("record segment reader creation succeeds");

        let input = Chunk::new(
            vec![build_record(
                "input-1",
                HashMap::from([("fresh_key".to_string(), UpdateMetadataValue::Int(1))]),
            )]
            .into(),
        );

        let output = executor
            .execute(input, Some(&record_reader))
            .await
            .expect("execution succeeds");

        let (upserts, deletes) = partition_output(&output);

        assert_eq!(deletes, vec!["obsolete_key::b:true".to_string()]);

        let fresh_stats = upserts
            .get("fresh_key::i:1")
            .expect("fresh statistics record should be recreated");
        let metadata = fresh_stats
            .metadata
            .as_ref()
            .expect("statistics executor always sets metadata");

        let (count, key, value_type, value) = extract_metadata_tuple(metadata);

        assert_eq!(count, 1);
        assert_eq!(key, "fresh_key");
        assert_eq!(value_type, "int");
        assert_eq!(value, "1");
    }

    #[tokio::test]
    async fn statistics_executor_zeroes_output_when_input_empty() {
        let executor = StatisticsFunctionExecutor(Box::new(CounterFunctionFactory));

        let mut test_segment = TestDistributedSegment::new().await;

        let record = build_complete_statistics_record("empty_key", "initial", "str", "s", 2);

        let existing_chunk = Chunk::new(vec![record].into());
        Box::pin(test_segment.compact_log(existing_chunk, 1)).await;

        let record_reader = Box::pin(RecordSegmentReader::from_segment(
            &test_segment.record_segment,
            &test_segment.blockfile_provider,
        ))
        .await
        .expect("record segment reader creation succeeds");

        let empty_input: Chunk<LogRecord> = Chunk::new(Vec::<LogRecord>::new().into());

        let output = executor
            .execute(empty_input, Some(&record_reader))
            .await
            .expect("execution succeeds");

        let deletes = partition_output_expect_no_upserts(&output);

        assert_eq!(deletes, vec!["empty_key::s:initial".to_string()]);
    }

    #[tokio::test]
    async fn test_k8s_integration_statistics_function() {
        use crate::config::RootConfig;
        use crate::execution::orchestration::CompactOrchestrator;
        use chroma_config::{registry::Registry, Configurable};
        use chroma_log::in_memory_log::{InMemoryLog, InternalLogRecord};
        use chroma_log::Log;
        use chroma_segment::test::TestDistributedSegment;
        use chroma_sysdb::SysDb;
        use chroma_system::{Dispatcher, Orchestrator, System};
        use chroma_types::{CollectionUuid, Operation, OperationRecord, UpdateMetadataValue};
        use s3heap_service::client::{GrpcHeapService, GrpcHeapServiceConfig};
        use std::collections::HashMap;

        // Setup test environment
        let config = RootConfig::default();
        let system = System::default();
        let registry = Registry::new();
        let dispatcher = Dispatcher::try_from_config(&config.query_service.dispatcher, &registry)
            .await
            .expect("Should be able to initialize dispatcher");
        let dispatcher_handle = system.start_component(dispatcher);

        // Connect to Grpc SysDb (requires Tilt running)
        let grpc_sysdb = chroma_sysdb::GrpcSysDb::try_from_config(
            &chroma_sysdb::GrpcSysDbConfig {
                host: "localhost".to_string(),
                port: 50051,
                connect_timeout_ms: 5000,
                request_timeout_ms: 10000,
                num_channels: 4,
            },
            &registry,
        )
        .await
        .expect("Should connect to grpc sysdb");
        let mut sysdb = SysDb::Grpc(grpc_sysdb);

        // Connect to Grpc Heap Service (requires Tilt running)
        let heap_service = GrpcHeapService::try_from_config(
            &(GrpcHeapServiceConfig::default(), system.clone()),
            &registry,
        )
        .await
        .expect("Should connect to grpc heap service");

        let test_segments = TestDistributedSegment::new().await;
        let mut in_memory_log = InMemoryLog::new();

        // Create input collection
        let collection_name = format!("test_statistics_{}", uuid::Uuid::new_v4());
        let collection_id = CollectionUuid::new();

        sysdb
            .create_collection(
                test_segments.collection.tenant,
                test_segments.collection.database,
                collection_id,
                collection_name,
                vec![
                    test_segments.record_segment.clone(),
                    test_segments.metadata_segment.clone(),
                    test_segments.vector_segment.clone(),
                ],
                None,
                None,
                None,
                test_segments.collection.dimension,
                false,
            )
            .await
            .expect("Collection create should be successful");

        let tenant = "default_tenant".to_string();
        let db = "default_database".to_string();

        // Set initial log position
        sysdb
            .flush_compaction(
                tenant.clone(),
                collection_id,
                -1,
                0,
                std::sync::Arc::new([]),
                0,
                0,
                None,
            )
            .await
            .expect("Should be able to update log_position");

        // Add 15 records with specific metadata we can verify
        // 10 records with color="red", 5 with color="blue"
        // 8 records with size=10, 7 with size=20
        for i in 0..15 {
            let mut metadata = HashMap::new();

            // First 10 are red, last 5 are blue
            let color = if i < 10 { "red" } else { "blue" };
            metadata.insert(
                "color".to_string(),
                UpdateMetadataValue::Str(color.to_string()),
            );

            // First 8 are size 10, last 7 are size 20
            let size = if i < 8 { 10 } else { 20 };
            metadata.insert("size".to_string(), UpdateMetadataValue::Int(size));

            let log_record = LogRecord {
                log_offset: i as i64,
                record: OperationRecord {
                    id: format!("record_{}", i),
                    embedding: Some(vec![
                        0.0;
                        test_segments.collection.dimension.unwrap_or(384)
                            as usize
                    ]),
                    encoding: None,
                    metadata: Some(metadata),
                    document: Some(format!("doc {}", i)),
                    operation: Operation::Upsert,
                },
            };

            in_memory_log.add_log(
                collection_id,
                InternalLogRecord {
                    collection_id,
                    log_offset: i as i64,
                    log_ts: i as i64,
                    record: log_record,
                },
            )
        }

        let log = Log::InMemory(in_memory_log);
        let attached_function_name = "test_statistics";
        let output_collection_name = format!("test_stats_output_{}", uuid::Uuid::new_v4());

        // Create statistics attached function via sysdb
        let attached_function_id = sysdb
            .create_attached_function(
                attached_function_name.to_string(),
                "statistics".to_string(),
                collection_id,
                output_collection_name,
                serde_json::Value::Null,
                tenant.clone(),
                db.clone(),
                10,
            )
            .await
            .expect("Attached function creation should succeed");

        // Initial compaction
        let compact_orchestrator = CompactOrchestrator::new(
            collection_id,
            false,
            50,
            1000,
            50,
            log.clone(),
            sysdb.clone(),
            test_segments.blockfile_provider.clone(),
            test_segments.hnsw_provider.clone(),
            test_segments.spann_provider.clone(),
            dispatcher_handle.clone(),
            None,
        );

        let result = compact_orchestrator.run(system.clone()).await;
        assert!(
            result.is_ok(),
            "Initial compaction should succeed: {:?}",
            result.err()
        );

        // Get nonce for attached function run
        let attached_function = sysdb
            .get_attached_function_by_name(collection_id, attached_function_name.to_string())
            .await
            .expect("Attached function should be found");
        let execution_nonce = attached_function.lowest_live_nonce.unwrap();

        // Run statistics function
        let compact_orchestrator = CompactOrchestrator::new_for_attached_function(
            collection_id,
            false,
            50,
            1000,
            50,
            log.clone(),
            sysdb.clone(),
            heap_service,
            test_segments.blockfile_provider.clone(),
            test_segments.hnsw_provider.clone(),
            test_segments.spann_provider.clone(),
            dispatcher_handle,
            None,
            attached_function_id,
            execution_nonce,
        );

        let result = compact_orchestrator.run(system).await;
        assert!(
            result.is_ok(),
            "Statistics function execution should succeed: {:?}",
            result.err()
        );

        // Verify statistics were generated
        let updated_attached_function = sysdb
            .get_attached_function_by_name(collection_id, attached_function_name.to_string())
            .await
            .expect("Attached function should be found");

        // Note: completion_offset is 13, but all 15 records (0-14) were processed
        assert_eq!(
            updated_attached_function.completion_offset, 13,
            "Completion offset should be 13"
        );

        let output_collection_id = updated_attached_function.output_collection_id.unwrap();

        // Read statistics from output collection
        let output_info = sysdb
            .get_collection_with_segments(output_collection_id)
            .await
            .expect("Should get output collection");
        let reader = Box::pin(RecordSegmentReader::from_segment(
            &output_info.record_segment,
            &test_segments.blockfile_provider,
        ))
        .await
        .expect("Should create reader");

        // Verify statistics records exist
        let max_offset_id = reader.get_max_offset_id();
        assert!(
            max_offset_id > 0,
            "Statistics function should have created records"
        );

        // Verify actual statistics content
        use futures::stream::StreamExt;
        let mut stream = reader.get_data_stream(0..=max_offset_id).await;
        let mut stats_by_key_value: HashMap<(String, String), i64> = HashMap::new();

        while let Some(result) = stream.next().await {
            let (_, record) = result.expect("Should read record");

            // Verify metadata structure
            let metadata = record
                .metadata
                .expect("Statistics records should have metadata");

            // All statistics records should have these fields
            assert!(metadata.contains_key("count"), "Should have count field");
            assert!(metadata.contains_key("key"), "Should have key field");
            assert!(metadata.contains_key("type"), "Should have type field");
            assert!(metadata.contains_key("value"), "Should have value field");

            // Extract key, value, and count
            let key = match metadata.get("key") {
                Some(chroma_types::MetadataValue::Str(k)) => k.clone(),
                _ => panic!("key should be a string"),
            };
            let value = match metadata.get("value") {
                Some(chroma_types::MetadataValue::Str(v)) => v.clone(),
                _ => panic!("value should be a string"),
            };
            let count = match metadata.get("count") {
                Some(chroma_types::MetadataValue::Int(c)) => *c,
                _ => panic!("count should be an int"),
            };

            stats_by_key_value.insert((key, value), count);
        }

        // Verify expected statistics:
        // All 15 records (0-14) were processed
        // Expected: color="red" -> 10 (records 0-9), color="blue" -> 5 (records 10-14)
        // Expected: size=10 -> 8 (records 0-7), size=20 -> 7 (records 8-14)
        assert_eq!(
            stats_by_key_value.get(&("color".to_string(), "red".to_string())),
            Some(&10),
            "Should have 10 records with color=red (records 0-9)"
        );
        assert_eq!(
            stats_by_key_value.get(&("color".to_string(), "blue".to_string())),
            Some(&5),
            "Should have 5 records with color=blue (records 10-14)"
        );
        assert_eq!(
            stats_by_key_value.get(&("size".to_string(), "10".to_string())),
            Some(&8),
            "Should have 8 records with size=10 (records 0-7)"
        );
        assert_eq!(
            stats_by_key_value.get(&("size".to_string(), "20".to_string())),
            Some(&7),
            "Should have 7 records with size=20 (records 8-14)"
        );

        // Verify we found exactly 4 unique statistics (2 colors + 2 sizes)
        assert_eq!(
            stats_by_key_value.len(),
            4,
            "Should have exactly 4 unique statistics"
        );

        // Verify total count is 30 (15 records × 2 metadata keys)
        let total_count: i64 = stats_by_key_value.values().sum();
        assert_eq!(
            total_count, 30,
            "Total count should be 30 (15 records × 2 metadata keys)"
        );

        tracing::info!(
            "Statistics function test completed successfully. Found {} unique statistics with correct counts",
            stats_by_key_value.len()
        );
    }
}
