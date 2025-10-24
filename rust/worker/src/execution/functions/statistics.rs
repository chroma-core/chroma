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
            Self::Str(s) => {
                format!("{s}")
            }
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
