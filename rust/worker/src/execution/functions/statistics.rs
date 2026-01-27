//! This implements a statistics module that returns the token frequency of metadata.
//!
//! The core idea is the following: For each key-value pair associated with a record, aggregate so
//! (key, value) -> count.  This gives a count of how frequently each key appears.
//!
//! The statistics executor is incremental - it loads existing counts from the output_reader
//! and updates them with new records.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_segment::blockfile_record::RecordSegmentReader;
use chroma_segment::types::HydratedMaterializedLogRecord;
use chroma_types::{
    Chunk, LogRecord, MaterializedLogOperation, MetadataValue, Operation, OperationRecord,
    UpdateMetadataValue,
};
use futures::StreamExt;

use crate::execution::operators::execute_task::AttachedFunctionExecutor;

/// Create an accumulator for statistics.
pub trait StatisticsFunctionFactory: std::fmt::Debug + Send + Sync {
    fn create(&self) -> Box<dyn StatisticsFunction>;
}

/// Accumulate statistics.  Must be an associative and commutative over a sequence of `observe` calls.
pub trait StatisticsFunction: std::fmt::Debug + Send {
    // TODO(tanujnay112): Look into changing the abstraction layer to not have to switch
    // on the type of the record.
    fn observe_insert(&mut self, hydrated_record: &HydratedMaterializedLogRecord<'_, '_>);
    fn observe_delete(&mut self, hydrated_record: &HydratedMaterializedLogRecord<'_, '_>);
    fn output(&self) -> UpdateMetadataValue;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    fn is_empty(&self) -> bool;
    fn is_changed(&self) -> bool;
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
    is_changed: bool,
}

impl CounterFunction {
    /// Create a CounterFunction with an initial value.
    pub fn with_initial_value(value: i64) -> Self {
        Self {
            acc: value,
            is_changed: false,
        }
    }
}

impl StatisticsFunction for CounterFunction {
    fn observe_insert(&mut self, _: &HydratedMaterializedLogRecord<'_, '_>) {
        self.acc = self.acc.saturating_add(1);
        self.is_changed = true;
    }

    fn observe_delete(&mut self, _: &HydratedMaterializedLogRecord<'_, '_>) {
        self.acc = self.acc.saturating_sub(1);
        self.is_changed = true;
    }

    fn output(&self) -> UpdateMetadataValue {
        UpdateMetadataValue::Int(self.acc)
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn is_changed(&self) -> bool {
        self.is_changed
    }

    fn is_empty(&self) -> bool {
        self.acc == 0
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
    SparseVector(u32, Option<String>),
}

impl StatisticsValue {
    /// A stable type string for the statistics's type.
    fn stable_type(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::Str(_) => "str",
            Self::SparseVector(_, _) => "sparse",
        }
    }

    /// A stable type prefix for stable_string format.
    fn type_prefix(&self) -> &'static str {
        match self {
            Self::Bool(_) => "b",
            Self::Int(_) => "i",
            Self::Float(_) => "f",
            Self::Str(_) => "s",
            Self::SparseVector(_, _) => "sv",
        }
    }

    /// A stable representation of the statistics's value.
    fn stable_value_index(&self) -> String {
        match self {
            Self::Bool(b) => {
                format!("{b}")
            }
            Self::Int(i) => {
                format!("{i}")
            }
            Self::Str(s) => s.clone(),
            Self::Float(f) => format!("{f:.16e}"),
            Self::SparseVector(index, _) => {
                format!("{index}")
            }
        }
    }

    /// A stable representation of the statistics's value.
    fn stable_value_label(&self) -> Option<String> {
        match self {
            Self::Bool(_) => None,
            Self::Int(_) => None,
            Self::Str(_) => None,
            Self::Float(_) => None,
            Self::SparseVector(_, label) => label.clone(),
        }
    }

    /// A stable string representation of a statistics value with type tag.
    /// Separate so display repr can change.
    fn stable_value_string(&self) -> String {
        format!("{}:{}", self.type_prefix(), self.stable_value_index())
    }

    /// Convert MetadataValue to a vector of StatisticsValue.
    /// Returns a vector because sparse vectors expand to multiple values.
    fn from_metadata_value(value: &MetadataValue) -> Vec<StatisticsValue> {
        match value {
            MetadataValue::Bool(b) => vec![StatisticsValue::Bool(*b)],
            MetadataValue::Int(i) => vec![StatisticsValue::Int(*i)],
            MetadataValue::Float(f) => vec![StatisticsValue::Float(*f)],
            MetadataValue::Str(s) => vec![StatisticsValue::Str(s.clone())],
            MetadataValue::SparseVector(sparse) => {
                if let Some(tokens) = sparse.tokens.as_ref() {
                    sparse
                        .indices
                        .iter()
                        .zip(tokens.iter())
                        .map(|(index, token)| {
                            StatisticsValue::SparseVector(*index, Some(token.clone()))
                        })
                        .collect()
                } else {
                    sparse
                        .indices
                        .iter()
                        .map(|index| StatisticsValue::SparseVector(*index, None))
                        .collect()
                }
            }
        }
    }
}

impl std::fmt::Display for StatisticsValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.stable_value_string())
    }
}

impl PartialEq for StatisticsValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(lhs), Self::Bool(rhs)) => lhs == rhs,
            (Self::Int(lhs), Self::Int(rhs)) => lhs == rhs,
            (Self::Float(lhs), Self::Float(rhs)) => lhs.to_bits() == rhs.to_bits(),
            (Self::Str(lhs), Self::Str(rhs)) => lhs == rhs,
            (Self::SparseVector(lhs1, lhs2), Self::SparseVector(rhs1, rhs2)) => {
                lhs1 == rhs1 && lhs2 == rhs2
            }
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
            StatisticsValue::SparseVector(value, label) => {
                value.hash(state);
                label.hash(state);
            }
        }
    }
}

/// Special key for storing summary statistics (e.g., total record count).
const SUMMARY_KEY: &str = "summary";

/// StatisticsValue for tracking total record count in the summary.
fn total_count_value() -> StatisticsValue {
    StatisticsValue::Str("total_count".to_string())
}

/// Task executor that aggregates metadata value frequencies for the statistics task.
#[derive(Debug)]
pub struct StatisticsFunctionExecutor(pub Box<dyn StatisticsFunctionFactory>);

impl StatisticsFunctionExecutor {
    /// Load existing statistics from the output reader.
    /// Returns a HashMap with the same structure as the counts HashMap.
    async fn load_existing_statistics(
        &self,
        output_reader: Option<&RecordSegmentReader<'_>>,
    ) -> Result<
        HashMap<String, HashMap<StatisticsValue, Box<dyn StatisticsFunction>>>,
        Box<dyn ChromaError>,
    > {
        let mut counts: HashMap<String, HashMap<StatisticsValue, Box<dyn StatisticsFunction>>> =
            HashMap::default();

        let Some(reader) = output_reader else {
            return Ok(counts);
        };

        let max_offset_id = reader.get_max_offset_id();
        let mut stream = reader.get_data_stream(0..=max_offset_id).await;

        while let Some(record_result) = stream.next().await {
            let (_, record) = record_result?;

            // Parse the record to extract key, value, type, and count
            let Some(metadata) = &record.metadata else {
                continue;
            };

            let key = match metadata.get("key") {
                Some(MetadataValue::Str(k)) => k.clone(),
                _ => continue,
            };

            let value_type = match metadata.get("type") {
                Some(MetadataValue::Str(t)) => t.as_str(),
                _ => continue,
            };

            let value_str = match metadata.get("value") {
                Some(MetadataValue::Str(v)) => v.as_str(),
                _ => continue,
            };

            let count = match metadata.get("count") {
                Some(MetadataValue::Int(c)) => *c,
                _ => continue,
            };

            // Reconstruct the StatisticsValue from type and value
            let stats_value = match value_type {
                "bool" => match value_str {
                    "true" => StatisticsValue::Bool(true),
                    "false" => StatisticsValue::Bool(false),
                    _ => continue,
                },
                "int" => match value_str.parse::<i64>() {
                    Ok(i) => StatisticsValue::Int(i),
                    _ => continue,
                },
                "float" => match value_str.parse::<f64>() {
                    Ok(f) => StatisticsValue::Float(f),
                    _ => continue,
                },
                "str" => StatisticsValue::Str(value_str.to_string()),
                "sparse" => match value_str.parse::<u32>() {
                    Ok(index) => {
                        let label = match metadata.get("value_label") {
                            Some(MetadataValue::Str(v)) => Some(v.clone()),
                            _ => None,
                        };
                        StatisticsValue::SparseVector(index, label)
                    }
                    _ => continue,
                },
                _ => continue,
            };

            // Create a statistics function initialized with the existing count
            let stats_function =
                Box::new(CounterFunction::with_initial_value(count)) as Box<dyn StatisticsFunction>;

            counts
                .entry(key)
                .or_default()
                .insert(stats_value, stats_function);
        }

        Ok(counts)
    }
}

#[async_trait]
impl AttachedFunctionExecutor for StatisticsFunctionExecutor {
    async fn execute(
        &self,
        input_records: Chunk<HydratedMaterializedLogRecord<'_, '_>>,
        output_reader: Option<&RecordSegmentReader<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        // Load existing statistics from output_reader if available
        let mut counts = self.load_existing_statistics(output_reader).await?;

        // Process new input records and update counts
        for (hydrated_record, _index) in input_records.iter() {
            if hydrated_record.get_operation() == MaterializedLogOperation::DeleteExisting {
                for (key, old_value) in hydrated_record.merged_metadata() {
                    for stats_value in StatisticsValue::from_metadata_value(&old_value) {
                        let inner_map = counts.entry(key.to_string()).or_default();
                        inner_map
                            .entry(stats_value)
                            .or_insert_with(|| self.0.create())
                            .observe_delete(hydrated_record);
                    }
                }

                counts
                    .entry(SUMMARY_KEY.to_string())
                    .or_default()
                    .entry(total_count_value())
                    .or_insert_with(|| self.0.create())
                    .observe_delete(hydrated_record);
                continue;
            }

            if hydrated_record.get_operation() == MaterializedLogOperation::AddNew {
                counts
                    .entry(SUMMARY_KEY.to_string())
                    .or_default()
                    .entry(total_count_value())
                    .or_insert_with(|| self.0.create())
                    .observe_insert(hydrated_record);
            }

            let metadata_delta = hydrated_record.compute_metadata_delta();

            // Decrement counts for deleted metadata
            for (key, old_value) in metadata_delta.metadata_to_delete {
                for stats_value in StatisticsValue::from_metadata_value(old_value) {
                    let inner_map = counts.entry(key.to_string()).or_default();
                    inner_map
                        .entry(stats_value)
                        .or_insert_with(|| self.0.create())
                        .observe_delete(hydrated_record);
                }
            }

            // Decrement counts for old values in updates
            for (key, (old_value, _new_value)) in &metadata_delta.metadata_to_update {
                for stats_value in StatisticsValue::from_metadata_value(old_value) {
                    let inner_map = counts.entry(key.to_string()).or_default();
                    inner_map
                        .entry(stats_value)
                        .or_insert_with(|| self.0.create())
                        .observe_delete(hydrated_record);
                }
            }

            // Increment counts for new values in both updates and inserts
            for (key, value) in metadata_delta
                .metadata_to_update
                .iter()
                .map(|(k, (_old, new))| (*k, *new))
                .chain(
                    metadata_delta
                        .metadata_to_insert
                        .iter()
                        .map(|(k, v)| (*k, *v)),
                )
            {
                for stats_value in StatisticsValue::from_metadata_value(value) {
                    let inner_map = counts.entry(key.to_string()).or_default();
                    inner_map
                        .entry(stats_value)
                        .or_insert_with(|| self.0.create())
                        .observe_insert(hydrated_record);
                }
            }
        }
        let mut records = Vec::with_capacity(counts.len());
        for (key, inner_map) in counts.into_iter() {
            for (stats_value, count) in inner_map.into_iter() {
                if !count.is_changed() {
                    continue;
                }
                let stable_value_index = stats_value.stable_value_index();
                let stable_value_string = stats_value.stable_value_string();
                let record_id = format!("{key}::{stable_value_string}");
                let document = format!("statistics about {key} for {stable_value_string}");

                if key != SUMMARY_KEY && count.is_empty() {
                    records.push(LogRecord {
                        log_offset: 0,
                        record: OperationRecord {
                            id: record_id,
                            embedding: None,
                            encoding: None,
                            metadata: None,
                            document: None,
                            operation: Operation::Delete,
                        },
                    });
                    continue;
                }

                let mut metadata = HashMap::with_capacity(4);
                metadata.insert("count".to_string(), count.output());
                metadata.insert("key".to_string(), UpdateMetadataValue::Str(key.clone()));
                metadata.insert(
                    "type".to_string(),
                    UpdateMetadataValue::Str(stats_value.stable_type().to_string()),
                );
                metadata.insert(
                    "value".to_string(),
                    UpdateMetadataValue::Str(stable_value_index),
                );
                if let Some(stable_value_label) = stats_value.stable_value_label() {
                    metadata.insert(
                        "value_label".to_string(),
                        UpdateMetadataValue::Str(stable_value_label),
                    );
                }

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
        Ok(Chunk::new(records.into()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chroma_segment::{
        blockfile_record::RecordSegmentReader,
        test::TestDistributedSegment,
        types::{materialize_logs, MaterializeLogsResult},
    };
    use chroma_types::{
        Chunk, DatabaseName, LogRecord, Operation, OperationRecord, SparseVector, UpdateMetadata,
        UpdateMetadataValue,
    };

    use crate::execution::orchestration::compact;

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
                embedding: Some(vec![0.0]),
                encoding: None,
                metadata: Some(metadata),
                document: None,
                operation,
            },
        }
    }

    async fn hydrate_records<'a>(
        materialized: &'a MaterializeLogsResult,
        record_reader: Option<&'a RecordSegmentReader<'a>>,
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
                    UpdateMetadataValue::SparseVector(
                        SparseVector::new(vec![1, 3], vec![0.25, 0.75])
                            .expect("sparse vector creation should succeed"),
                    ),
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
                    UpdateMetadataValue::SparseVector(
                        SparseVector::new(vec![3], vec![0.5])
                            .expect("sparse vector creation should succeed"),
                    ),
                ),
            ]),
        );

        let logs = Chunk::new(vec![record_one, record_two].into());
        let materialized = materialize_logs(&None, logs, None)
            .await
            .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, None).await;
        let input = Chunk::new(std::sync::Arc::from(hydrated));

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
            (
                "summary::s:total_count".to_string(),
                (
                    2,
                    "summary".to_string(),
                    "str".to_string(),
                    "total_count".to_string(),
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

        let logs = Chunk::new(vec![record_one, record_two].into());
        let materialized = materialize_logs(&None, logs, None)
            .await
            .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, None).await;
        let input = Chunk::new(std::sync::Arc::from(hydrated));

        let output = executor
            .execute(input, None)
            .await
            .expect("execution succeeds");

        let actual = collect_statistics_map(&output);
        assert_eq!(actual.len(), 2);

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

        let summary_entry = (
            2,
            "summary".to_string(),
            "str".to_string(),
            "total_count".to_string(),
        );
        assert_eq!(
            actual
                .get("summary::s:total_count")
                .expect("Should have summary total_count"),
            &summary_entry
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

        let logs = Chunk::new(vec![upsert_record, delete_record].into());
        let materialized = materialize_logs(&None, logs, None)
            .await
            .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, None).await;
        let input = Chunk::new(std::sync::Arc::from(hydrated));

        let output = executor
            .execute(input, None)
            .await
            .expect("execution succeeds");

        let actual = collect_statistics_map(&output);
        assert_eq!(actual.len(), 2);

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

        assert!(
            actual.contains_key("summary::s:total_count"),
            "should have summary total_count for the upsert"
        );
    }

    #[tokio::test]
    async fn statistics_executor_handles_empty_sparse_vectors() {
        let executor = StatisticsFunctionExecutor(Box::new(CounterFunctionFactory));

        let record = build_record(
            "sparse-empty",
            HashMap::from([(
                "sparse_key".to_string(),
                UpdateMetadataValue::SparseVector(
                    SparseVector::new(Vec::<u32>::new(), Vec::<f32>::new())
                        .expect("valid sparse vector"),
                ),
            )]),
        );

        let logs = Chunk::new(vec![record].into());
        let materialized = materialize_logs(&None, logs, None)
            .await
            .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, None).await;
        let input = Chunk::new(std::sync::Arc::from(hydrated));

        let output = executor
            .execute(input, None)
            .await
            .expect("execution succeeds");

        let actual = collect_statistics_map(&output);
        assert_eq!(actual.len(), 1);
        assert!(
            actual.contains_key("summary::s:total_count"),
            "should have summary total_count even for empty sparse vector"
        );
    }

    #[tokio::test]
    async fn statistics_executor_skips_unconvertible_metadata_values() {
        let executor = StatisticsFunctionExecutor(Box::new(CounterFunctionFactory));

        let record = build_record(
            "only",
            HashMap::from([("skip".to_string(), UpdateMetadataValue::None)]),
        );

        let logs = Chunk::new(vec![record].into());
        let materialized = materialize_logs(&None, logs, None)
            .await
            .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, None).await;
        let input = Chunk::new(std::sync::Arc::from(hydrated));

        let output = executor
            .execute(input, None)
            .await
            .expect("execution succeeds");

        let actual = collect_statistics_map(&output);
        assert_eq!(actual.len(), 1);
        assert!(
            actual.contains_key("summary::s:total_count"),
            "should have summary total_count even when metadata value is unconvertible"
        );
    }

    #[tokio::test]
    async fn statistics_executor_deletes_stale_records_from_segment() {
        let executor = StatisticsFunctionExecutor(Box::new(CounterFunctionFactory));

        // Create input collection segment with records
        let mut input_segment = TestDistributedSegment::new().await;
        let input_record_with_obsolete_key = build_record(
            "input-0",
            HashMap::from([("obsolete_key".to_string(), UpdateMetadataValue::Bool(true))]),
        );
        let input_chunk = Chunk::new(vec![input_record_with_obsolete_key].into());
        Box::pin(input_segment.compact_log(input_chunk, 1)).await;

        let input_record_reader = Box::pin(RecordSegmentReader::from_segment(
            &input_segment.record_segment,
            &input_segment.blockfile_provider,
        ))
        .await
        .expect("input record segment reader creation succeeds");

        // Create output collection segment with existing statistics
        let mut output_segment = TestDistributedSegment::new().await;
        let stale_record = build_complete_statistics_record("obsolete_key", "true", "bool", "b", 1);
        let fresh_record = build_complete_statistics_record("fresh_key", "1", "int", "i", 3);
        let existing_output_chunk = Chunk::new(vec![stale_record, fresh_record].into());
        Box::pin(output_segment.compact_log(existing_output_chunk, 1)).await;

        let output_record_reader = Box::pin(RecordSegmentReader::from_segment(
            &output_segment.record_segment,
            &output_segment.blockfile_provider,
        ))
        .await
        .expect("output record segment reader creation succeeds");

        // Create logs: update fresh_key and delete obsolete_key
        let logs = Chunk::new(
            vec![
                build_record(
                    "input-1",
                    HashMap::from([("fresh_key".to_string(), UpdateMetadataValue::Int(1))]),
                ),
                build_record_with_operation("input-0", Operation::Delete, HashMap::new()),
            ]
            .into(),
        );
        let materialized = materialize_logs(&Some(input_record_reader.clone()), logs, None)
            .await
            .expect("materialization should succeed");

        // Hydrate from INPUT collection to get proper metadata for the delete
        let hydrated = hydrate_records(&materialized, Some(&input_record_reader)).await;
        let input = Chunk::new(std::sync::Arc::from(hydrated));

        // Execute with OUTPUT collection reader to load existing statistics
        let output = executor
            .execute(input, Some(&output_record_reader))
            .await
            .expect("execution succeeds");

        let (upserts, deletes) = partition_output(&output);

        assert_eq!(deletes, vec!["obsolete_key::b:true".to_string()]);

        let fresh_stats = upserts
            .get("fresh_key::i:1")
            .expect("fresh statistics record should be updated");
        let metadata = fresh_stats
            .metadata
            .as_ref()
            .expect("statistics executor always sets metadata");

        let (count, key, value_type, value) = extract_metadata_tuple(metadata);

        assert_eq!(count, 4); // 3 (existing) + 1 (new)
        assert_eq!(key, "fresh_key");
        assert_eq!(value_type, "int");
        assert_eq!(value, "1");
    }

    #[tokio::test]
    async fn statistics_executor_does_not_emit_when_input_empty() {
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

        let empty_logs: Chunk<LogRecord> = Chunk::new(Vec::<LogRecord>::new().into());
        let materialized = materialize_logs(&None, empty_logs, None)
            .await
            .expect("materialization should succeed");
        let hydrated = hydrate_records(&materialized, Some(&record_reader)).await;
        let empty_input = Chunk::new(std::sync::Arc::from(hydrated));

        let output = executor
            .execute(empty_input, Some(&record_reader))
            .await
            .expect("execution succeeds");

        let deletes = partition_output_expect_no_upserts(&output);

        assert!(deletes.is_empty());
    }

    #[tokio::test]
    async fn statistics_executor_decrements_count_on_delete() {
        let executor = StatisticsFunctionExecutor(Box::new(CounterFunctionFactory));

        // Create input collection with two records having the same metadata value
        let mut input_segment = TestDistributedSegment::new().await;
        let input_record1 = build_record(
            "input-1",
            HashMap::from([(
                "category".to_string(),
                UpdateMetadataValue::Str("tech".to_string()),
            )]),
        );
        let input_record2 = build_record(
            "input-2",
            HashMap::from([(
                "category".to_string(),
                UpdateMetadataValue::Str("tech".to_string()),
            )]),
        );
        let input_chunk = Chunk::new(vec![input_record1, input_record2].into());
        Box::pin(input_segment.compact_log(input_chunk, 1)).await;

        let input_record_reader = Box::pin(RecordSegmentReader::from_segment(
            &input_segment.record_segment,
            &input_segment.blockfile_provider,
        ))
        .await
        .expect("input record segment reader creation succeeds");

        // Create output collection with existing statistic: category=tech with count=2
        let mut output_segment = TestDistributedSegment::new().await;
        let existing_stat = build_complete_statistics_record("category", "tech", "str", "s", 2);
        let existing_chunk = Chunk::new(vec![existing_stat].into());
        Box::pin(output_segment.compact_log(existing_chunk, 1)).await;

        let output_record_reader = Box::pin(RecordSegmentReader::from_segment(
            &output_segment.record_segment,
            &output_segment.blockfile_provider,
        ))
        .await
        .expect("output record segment reader creation succeeds");

        // Delete one of the records with category=tech
        let logs = Chunk::new(
            vec![build_record_with_operation(
                "input-1",
                Operation::Delete,
                HashMap::new(),
            )]
            .into(),
        );
        let materialized = materialize_logs(&Some(input_record_reader.clone()), logs, None)
            .await
            .expect("materialization should succeed");

        // Hydrate from INPUT collection to get proper metadata for the delete
        let hydrated = hydrate_records(&materialized, Some(&input_record_reader)).await;
        let input = Chunk::new(std::sync::Arc::from(hydrated));

        // Execute with OUTPUT collection reader to load existing statistics
        let output = executor
            .execute(input, Some(&output_record_reader))
            .await
            .expect("execution succeeds");

        let (upserts, deletes) = partition_output(&output);

        // Should have no deletes (statistic still exists)
        assert!(deletes.is_empty());

        // Should have an update for category=tech with decremented count
        let stat_key = "category::s:tech";
        let updated_stat = upserts
            .get(stat_key)
            .expect("category=tech statistic should be updated");

        let metadata = updated_stat
            .metadata
            .as_ref()
            .expect("statistics executor always sets metadata");

        let (count, key, value_type, value) = extract_metadata_tuple(metadata);

        assert_eq!(count, 1); // 2 (existing) - 1 (deleted)
        assert_eq!(key, "category");
        assert_eq!(value_type, "str");
        assert_eq!(value, "tech");
    }

    #[tokio::test]
    async fn test_k8s_integration_statistics_function() {
        use crate::config::RootConfig;
        use chroma_config::{registry::Registry, Configurable};
        use chroma_log::in_memory_log::{InMemoryLog, InternalLogRecord};
        use chroma_log::Log;
        use chroma_segment::test::TestDistributedSegment;
        use chroma_sysdb::SysDb;
        use chroma_system::{Dispatcher, System};
        use chroma_types::{CollectionUuid, Operation, OperationRecord, UpdateMetadataValue};
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
            &(
                chroma_sysdb::GrpcSysDbConfig {
                    host: "localhost".to_string(),
                    port: 50051,
                    connect_timeout_ms: 5000,
                    request_timeout_ms: 10000,
                    num_channels: 4,
                },
                None,
            ),
            &registry,
        )
        .await
        .expect("Should connect to grpc sysdb");
        let mut sysdb = SysDb::Grpc(grpc_sysdb);

        let test_segments = TestDistributedSegment::new().await;
        let mut in_memory_log = InMemoryLog::new();

        // Create input collection
        let collection_name = format!("test_statistics_{}", uuid::Uuid::new_v4());
        let collection_id = CollectionUuid::new();
        let database_name =
            chroma_types::DatabaseName::new(test_segments.collection.database.clone())
                .expect("database name should be valid");

        sysdb
            .create_collection(
                test_segments.collection.tenant,
                database_name,
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
        let test_run_id = uuid::Uuid::new_v4();
        let attached_function_name = format!("test_statistics_{}", test_run_id);
        let output_collection_name = format!("test_stats_output_{}", test_run_id);

        // Create statistics attached function via sysdb
        let (attached_function_id, _created) = sysdb
            .create_attached_function(
                attached_function_name.to_string(),
                "statistics".to_string(),
                collection_id,
                output_collection_name.clone(),
                serde_json::Value::Null,
                tenant.clone(),
                db.clone(),
                10,
            )
            .await
            .expect("Attached function creation should succeed");
        let mut output_schema = chroma_types::Schema::new_default(chroma_types::KnnIndex::Hnsw);
        output_schema.source_attached_function_id = Some(attached_function_id.0.to_string());
        let output_schema_str = serde_json::to_string(&output_schema).unwrap();
        sysdb
            .finish_create_attached_function(attached_function_id, output_schema_str)
            .await
            .unwrap();

        let database_name = DatabaseName::new("test_db").expect("database name should be valid");
        Box::pin(compact::compact(
            system.clone(),
            collection_id,
            database_name,
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
            false,
            None,
        ))
        .await
        .expect("Compaction should succeed");

        // Verify statistics were generated
        let attached_functions = sysdb
            .get_attached_functions(
                None,
                Some(attached_function_name.clone()),
                Some(collection_id),
                true,
            )
            .await
            .expect("Attached function query should succeed");
        let updated_attached_function = attached_functions
            .into_iter()
            .next()
            .expect("Attached function should be found");

        // Note: completion_offset is 14, all 15 records (0-14) were processed
        assert_eq!(
            updated_attached_function.completion_offset, 14,
            "Completion offset should be 14 (last processed record)"
        );

        let output_collection_id = updated_attached_function.output_collection_id.unwrap();

        // Read statistics from output collection
        let output_info = sysdb
            .get_collection_with_segments(None, output_collection_id)
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
            5,
            "Should have exactly 5 unique statistics"
        );

        // Verify total count is 45 (15 records × 3 metadata keys)
        let total_count: i64 = stats_by_key_value.values().sum();
        assert_eq!(
            total_count, 45,
            "Total count should be 45 (15 records × 3 metadata keys)"
        );

        tracing::info!(
            "Statistics function test completed successfully. Found {} unique statistics with correct counts",
            stats_by_key_value.len()
        );
    }
}
