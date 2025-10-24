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
use chroma_types::Chunk;
use chroma_types::LogRecord;
use chroma_types::MetadataValue;
use chroma_types::Operation;
use chroma_types::OperationRecord;
use chroma_types::UpdateMetadataValue;
use futures::StreamExt;

use crate::execution::operators::execute_task::TaskExecutor;

#[derive(Clone, Debug, PartialEq)]
pub enum StatisticsValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
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

    /// A stable representation of the statistics's value.
    fn stable_value(&self) -> String {
        // NOTE(rescrv):  Keep in sync with stable_string.  Done separately to avoid clone.
        match self {
            Self::Bool(b) => {
                format!("{b}")
            }
            Self::Int(i) => {
                format!("{i}")
            }
            Self::Str(s) => s.clone(),
            Self::Float(f) => {
                // Kinda error-prone, but supported.
                // A footgun.
                format!("{f}")
            }
            Self::SparseVector(index) => {
                format!("{index}")
            }
        }
    }

    /// A stable string representation of a statistics value with type tag.
    /// Separate so display repr can change.
    fn stable_string(&self) -> String {
        // NOTE(rescrv):  Keep in sync with stable_value.  Done separately to avoid clone.
        match self {
            Self::Bool(b) => {
                format!("b:{b}")
            }
            Self::Int(i) => {
                format!("i:{i}")
            }
            Self::Str(s) => {
                format!("s:{s}")
            }
            Self::Float(f) => {
                // Kinda error-prone, but supported.
                // A footgun.
                format!("f:{f}")
            }
            Self::SparseVector(index) => {
                format!("sv:{index}")
            }
        }
    }
}

impl std::fmt::Display for StatisticsValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.stable_string())
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

#[derive(Debug)]
pub struct StatisticsFunctionExecutor;

#[async_trait]
impl TaskExecutor for StatisticsFunctionExecutor {
    async fn execute(
        &self,
        input_records: Chunk<LogRecord>,
        output_reader: Option<&RecordSegmentReader<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        // Consume the whole input segment.
        let mut counts: HashMap<(String, StatisticsValue), i64> = HashMap::default();
        for (log_record, _) in input_records.iter() {
            if let Some(update_metadata) = log_record.record.metadata.as_ref() {
                for (key, update_value) in update_metadata.iter() {
                    let value: Option<MetadataValue> = update_value.try_into().ok();
                    if let Some(value) = value {
                        match value {
                            MetadataValue::Bool(b) => {
                                *counts
                                    .entry((key.clone(), StatisticsValue::Bool(b)))
                                    .or_default() += 1;
                            }
                            MetadataValue::Int(i) => {
                                *counts
                                    .entry((key.clone(), StatisticsValue::Int(i)))
                                    .or_default() += 1;
                            }
                            MetadataValue::Float(f) => {
                                *counts
                                    .entry((key.clone(), StatisticsValue::Float(f)))
                                    .or_default() += 1;
                            }
                            MetadataValue::Str(s) => {
                                *counts
                                    .entry((key.clone(), StatisticsValue::Str(s)))
                                    .or_default() += 1;
                            }
                            MetadataValue::SparseVector(sparse) => {
                                for index in sparse.indices.iter() {
                                    *counts
                                        .entry((key.clone(), StatisticsValue::SparseVector(*index)))
                                        .or_default() += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        // Prepare the records for insertion.
        let mut keys = HashSet::with_capacity(counts.len());
        let mut records = Vec::with_capacity(counts.len());
        for ((key, stats_value), count) in counts.into_iter() {
            let metadata = HashMap::from_iter([
                ("count".to_string(), UpdateMetadataValue::Int(count)),
                ("term".to_string(), key.clone().into()),
                ("type".to_string(), stats_value.stable_type().into()),
                ("value".to_string(), stats_value.stable_value().into()),
            ]);
            let record = LogRecord {
                log_offset: 0,
                record: OperationRecord {
                    id: format!("{}::{}", key, stats_value.stable_string()),
                    embedding: Some(vec![0.0]),
                    encoding: None,
                    metadata: Some(metadata),
                    document: Some(format!("statistics about {key} for {stats_value}")),
                    operation: Operation::Upsert,
                },
            };
            keys.insert(record.record.id.clone());
            records.push(record);
        }
        // Delete records we didn't recreate.
        if let Some(output_reader) = output_reader {
            let mut stream = output_reader
                .get_data_stream(0..output_reader.get_max_offset_id())
                .await;

            while let Some(record) = stream.next().await {
                let (_, record) = record?;
                if !keys.contains(record.id) {
                    records.push(LogRecord {
                        log_offset: 0,
                        record: OperationRecord {
                            id: record.id.to_string(),
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
    use super::*;
    use std::collections::HashMap;

    use chroma_segment::{
        blockfile_record::RecordSegmentReader,
        test::TestDistributedSegment,
    };
    use chroma_types::{
        Chunk, LogRecord, Operation, OperationRecord, SparseVector, UpdateMetadata,
        UpdateMetadataValue,
    };

    fn build_record(id: &str, metadata: HashMap<String, UpdateMetadataValue>) -> LogRecord {
        LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: id.to_string(),
                embedding: None,
                encoding: None,
                metadata: Some(metadata),
                document: None,
                operation: Operation::Upsert,
            },
        }
    }

    #[tokio::test]
    async fn statistics_executor_counts_all_metadata_values() {
        let executor = StatisticsFunctionExecutor;

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

        let mut actual: HashMap<String, (i64, String, String, String)> = HashMap::new();
        for (log_record, _) in output.iter() {
            let record = &log_record.record;
            assert_eq!(record.operation, Operation::Upsert);
            assert_eq!(record.embedding.as_deref(), Some(&[0.0][..]));

            let metadata = record
                .metadata
                .as_ref()
                .expect("statistics executor always sets metadata");

            let count = match metadata.get("count") {
                Some(UpdateMetadataValue::Int(value)) => *value,
                other => panic!("unexpected count metadata: {other:?}"),
            };
            let term = match metadata.get("term") {
                Some(UpdateMetadataValue::Str(value)) => value.clone(),
                other => panic!("unexpected term metadata: {other:?}"),
            };
            let value_type = match metadata.get("type") {
                Some(UpdateMetadataValue::Str(value)) => value.clone(),
                other => panic!("unexpected type metadata: {other:?}"),
            };
            let value = match metadata.get("value") {
                Some(UpdateMetadataValue::Str(value)) => value.clone(),
                other => panic!("unexpected value metadata: {other:?}"),
            };

            actual.insert(record.id.clone(), (count, term, value_type, value));
        }

        let float_value = format!("{}", 2.5_f64);
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
    async fn statistics_executor_skips_unconvertible_metadata_values() {
        let executor = StatisticsFunctionExecutor;

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
        let executor = StatisticsFunctionExecutor;

        let mut test_segment = TestDistributedSegment::new().await;

        let existing_metadata: UpdateMetadata = HashMap::from([
            ("count".to_string(), UpdateMetadataValue::Int(1)),
            (
                "term".to_string(),
                UpdateMetadataValue::Str("obsolete_key".to_string()),
            ),
            (
                "type".to_string(),
                UpdateMetadataValue::Str("bool".to_string()),
            ),
            (
                "value".to_string(),
                UpdateMetadataValue::Str("true".to_string()),
            ),
        ]);

        let stale_record = LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: "obsolete_key::b:true".to_string(),
                embedding: Some(vec![0.0]),
                encoding: None,
                metadata: Some(existing_metadata),
                document: Some("statistics about obsolete_key for b:true".to_string()),
                operation: Operation::Upsert,
            },
        };

        let fresh_metadata: UpdateMetadata = HashMap::from([
            ("count".to_string(), UpdateMetadataValue::Int(3)),
            ("term".to_string(), UpdateMetadataValue::Str("fresh_key".to_string())),
            ("type".to_string(), UpdateMetadataValue::Str("int".to_string())),
            ("value".to_string(), UpdateMetadataValue::Str("1".to_string())),
        ]);

        let fresh_record = LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: "fresh_key::i:1".to_string(),
                embedding: Some(vec![0.0]),
                encoding: None,
                metadata: Some(fresh_metadata),
                document: Some("statistics about fresh_key for i:1".to_string()),
                operation: Operation::Upsert,
            },
        };

        let existing_chunk = Chunk::new(vec![stale_record, fresh_record].into());

        test_segment.compact_log(existing_chunk, 1).await;

        let record_reader = RecordSegmentReader::from_segment(
            &test_segment.record_segment,
            &test_segment.blockfile_provider,
        )
        .await
        .expect("record segment reader creation succeeds");

        let input = Chunk::new(
            vec![build_record(
                "input-1",
                HashMap::from([(
                    "fresh_key".to_string(),
                    UpdateMetadataValue::Int(1),
                )]),
            )]
            .into(),
        );

        let output = executor
            .execute(input, Some(&record_reader))
            .await
            .expect("execution succeeds");

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

        assert_eq!(deletes, vec!["obsolete_key::b:true".to_string()]);

        let fresh_stats = upserts
            .get("fresh_key::i:1")
            .expect("fresh statistics record should be recreated");
        let metadata = fresh_stats
            .metadata
            .as_ref()
            .expect("statistics executor always sets metadata");

        let count = match metadata.get("count") {
            Some(UpdateMetadataValue::Int(value)) => *value,
            other => panic!("unexpected count metadata: {other:?}"),
        };
        let term = match metadata.get("term") {
            Some(UpdateMetadataValue::Str(value)) => value.clone(),
            other => panic!("unexpected term metadata: {other:?}"),
        };
        let value_type = match metadata.get("type") {
            Some(UpdateMetadataValue::Str(value)) => value.clone(),
            other => panic!("unexpected type metadata: {other:?}"),
        };
        let value = match metadata.get("value") {
            Some(UpdateMetadataValue::Str(value)) => value.clone(),
            other => panic!("unexpected value metadata: {other:?}"),
        };

        assert_eq!(count, 1);
        assert_eq!(term, "fresh_key");
        assert_eq!(value_type, "int");
        assert_eq!(value, "1");
    }
}
