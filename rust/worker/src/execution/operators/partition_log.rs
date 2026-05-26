use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::Operator;
use chroma_types::{Chunk, LogRecord};
use std::collections::HashMap;
use thiserror::Error;

/// Grouping key used to assign a record to a partition.
///
/// A record whose id ends in `-{digits}` (e.g. `mydoc-0`, `mydoc-12`)
/// is grouped under its base (`mydoc`), so that all chunks of one
/// document land in the same partition. The partition operator
/// guarantees in-order processing *within* a partition but not across
/// partitions, so co-locating a document's chunks is what lets a
/// downstream consumer (e.g. a foundation attached function) observe a
/// trailing end-of-job marker on `{base}-0` only after every sibling
/// chunk — see ADR 0001 §6 in chroma-core/foundation.
///
/// This only ever *enlarges* groups: a given concrete id always maps to
/// a single key, so the existing "all ops for one id in one partition"
/// invariant is preserved. Records whose id does not match `{base}-{digits}`
/// are returned unchanged.
///
/// Trade-off (flagged for review): ids that legitimately end in
/// `-{digits}` but are NOT chunk siblings (e.g. `report-2024`,
/// `report-2023`) will be co-located in one partition. This is
/// correctness-neutral — they remain distinct records — but can reduce
/// compaction parallelism for collections that use such an id scheme.
/// If that proves too coarse, this should be gated behind a
/// per-collection flag rather than applied globally.
fn chunk_grouping_key(id: &str) -> &str {
    match id.rsplit_once('-') {
        Some((base, suffix))
            if !base.is_empty()
                && !suffix.is_empty()
                && suffix.bytes().all(|b| b.is_ascii_digit()) =>
        {
            base
        }
        _ => id,
    }
}

#[derive(Debug)]
/// The partition Operator takes a DataChunk and presents a copy-free
/// view of N partitions by breaking the data into partitions by max_partition_size. It will group operations
/// on the same key into the same partition. Due to this, the max_partition_size is a
/// soft-limit, since if there are more operations to a key than max_partition_size we cannot
/// partition the data.
///
/// Keys are computed by [`chunk_grouping_key`], which folds chunk
/// siblings (`{base}-{idx}`) onto a shared base so a document's chunks
/// stay in one partition.
pub struct PartitionOperator {}

/// The input to the partition operator.
/// # Parameters
/// * `records` - The records to partition.
#[derive(Debug)]
pub struct PartitionInput {
    pub(crate) records: Chunk<LogRecord>,
    pub(crate) max_partition_size: usize,
}

impl PartitionInput {
    /// Create a new partition input.
    /// # Parameters
    /// * `records` - The records to partition.
    /// * `max_partition_size` - The maximum size of a partition. Since we are trying to
    ///   partition the records by id, which can casue the partition size to be larger than this
    ///   value.
    pub fn new(records: Chunk<LogRecord>, max_partition_size: usize) -> Self {
        PartitionInput {
            records,
            max_partition_size,
        }
    }
}

/// The output of the partition operator.
/// # Parameters
/// * `records` - The partitioned records.
#[derive(Debug)]
pub struct PartitionOutput {
    pub(crate) records: Vec<Chunk<LogRecord>>,
}

#[derive(Debug, Error)]
#[error("Failed to partition records.")]
pub struct PartitionError;

impl ChromaError for PartitionError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

impl PartitionOperator {
    pub fn new() -> Box<Self> {
        Box::new(PartitionOperator {})
    }

    pub fn partition(
        &self,
        records: &Chunk<LogRecord>,
        partition_size: usize,
    ) -> Vec<Chunk<LogRecord>> {
        let mut map = HashMap::new();
        for data in records.iter() {
            let log_record = data.0;
            let index = data.1;
            let key = chunk_grouping_key(&log_record.record.id).to_string();
            map.entry(key).or_insert_with(Vec::new).push(index);
        }
        let mut result = Vec::new();
        // Create a new DataChunk for each parition of records with partition_size without
        // data copying.
        let mut current_batch_size = 0;
        let mut new_partition = true;
        let mut visibility = vec![false; records.total_len()];
        for (_, v) in map.iter() {
            // create DataChunk with partition_size by masking the visibility of the records
            // in the partition.
            if new_partition {
                visibility = vec![false; records.total_len()];
                new_partition = false;
            }
            for i in v.iter() {
                visibility[*i] = true;
            }
            current_batch_size += v.len();
            if current_batch_size >= partition_size {
                let mut new_data_chunk = records.clone();
                new_data_chunk.set_visibility(visibility.clone());
                result.push(new_data_chunk);
                new_partition = true;
                current_batch_size = 0;
            }
        }
        // handle the case that the last group is smaller than the group_size.
        if !new_partition {
            let mut new_data_chunk = records.clone();
            new_data_chunk.set_visibility(visibility.clone());
            result.push(new_data_chunk);
        }
        result
    }

    fn determine_partition_size(&self, num_records: usize, threshold: usize) -> usize {
        if num_records < threshold {
            num_records
        } else {
            threshold
        }
    }
}

#[async_trait]
impl Operator<PartitionInput, PartitionOutput> for PartitionOperator {
    type Error = PartitionError;

    fn get_name(&self) -> &'static str {
        "PartitionOperator"
    }

    async fn run(&self, input: &PartitionInput) -> Result<PartitionOutput, PartitionError> {
        let records = &input.records;
        let partition_size = self.determine_partition_size(records.len(), input.max_partition_size);
        let deduped_records = self.partition(records, partition_size);
        return Ok(PartitionOutput {
            records: deduped_records,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::{LogRecord, Operation, OperationRecord};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_partition_operator() {
        let data = vec![
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
        ];
        let data: Arc<[LogRecord]> = data.into();

        // Test group size is larger than the number of records
        let chunk = Chunk::new(data.clone());
        let operator = PartitionOperator::new();
        let input = PartitionInput::new(chunk, 4);
        let result = operator.run(&input).await.unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].len(), 3);

        // Test group size is the same as the number of records
        let chunk = Chunk::new(data.clone());
        let operator = PartitionOperator::new();
        let input = PartitionInput::new(chunk, 3);
        let result = operator.run(&input).await.unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].len(), 3);

        // Test group size is smaller than the number of records
        let chunk = Chunk::new(data.clone());
        let operator = PartitionOperator::new();
        let input = PartitionInput::new(chunk, 2);
        let mut result = operator.run(&input).await.unwrap();

        // The result can be 1 or 2 groups depending on the order of the records.
        assert!(result.records.len() == 2 || result.records.len() == 1);
        if result.records.len() == 2 {
            result.records.sort_by_key(|x| x.len());
            assert_eq!(result.records[0].len(), 1);
            assert_eq!(result.records[1].len(), 2);
        } else {
            assert_eq!(result.records[0].len(), 3);
        }

        // Test group size is smaller than the number of records
        let chunk = Chunk::new(data.clone());
        let operator = PartitionOperator::new();
        let input = PartitionInput::new(chunk, 1);
        let mut result = operator.run(&input).await.unwrap();
        assert_eq!(result.records.len(), 2);
        result.records.sort_by_key(|x| x.len());
        assert_eq!(result.records[0].len(), 1);
        assert_eq!(result.records[1].len(), 2);
    }

    #[test]
    fn test_chunk_grouping_key() {
        // Chunk siblings fold onto their base.
        assert_eq!(chunk_grouping_key("mydoc-0"), "mydoc");
        assert_eq!(chunk_grouping_key("mydoc-12"), "mydoc");
        assert_eq!(chunk_grouping_key("sha256hex-9999"), "sha256hex");
        // Only the final `-{digits}` segment is stripped.
        assert_eq!(chunk_grouping_key("a-b-0"), "a-b");
        // Non-chunk ids pass through unchanged.
        assert_eq!(chunk_grouping_key("mydoc"), "mydoc");
        assert_eq!(chunk_grouping_key("mydoc-abc"), "mydoc-abc");
        assert_eq!(chunk_grouping_key("mydoc-"), "mydoc-");
        assert_eq!(chunk_grouping_key("-0"), "-0"); // empty base: leave alone
        assert_eq!(chunk_grouping_key(""), "");
        // Documented trade-off: a legitimately-numbered id is folded too.
        assert_eq!(chunk_grouping_key("report-2024"), "report");
    }

    #[tokio::test]
    async fn test_chunk_siblings_share_a_partition() {
        // Three chunks of one document plus one unrelated record. Even
        // at partition_size == 1, the chunk siblings must stay together
        // (they share grouping key "doc"), so we get exactly 2
        // partitions: {doc-0, doc-1, doc-2} and {other}.
        let mk = |offset: i64, id: &str| LogRecord {
            log_offset: offset,
            record: OperationRecord {
                id: id.to_string(),
                embedding: None,
                encoding: None,
                metadata: None,
                document: None,
                operation: Operation::Add,
            },
        };
        let data: Arc<[LogRecord]> = vec![
            mk(1, "doc-0"),
            mk(2, "doc-1"),
            mk(3, "doc-2"),
            mk(4, "other"),
        ]
        .into();

        let operator = PartitionOperator::new();
        let input = PartitionInput::new(Chunk::new(data), 1);
        let mut result = operator.run(&input).await.unwrap();

        assert_eq!(result.records.len(), 2);
        result.records.sort_by_key(|x| x.len());
        assert_eq!(result.records[0].len(), 1); // {other}
        assert_eq!(result.records[1].len(), 3); // {doc-0, doc-1, doc-2}
    }
}
