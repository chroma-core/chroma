use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::Operator;
use chroma_types::{Chunk, LogRecord};
use std::collections::HashMap;
use thiserror::Error;

/// Collection-metadata key that opts a collection into chunk-sibling
/// grouping during partitioning (see [`chunk_grouping_key`]). A
/// `MetadataValue::Bool(true)` enables it; absent or any other value
/// leaves the default (group by exact id) in place.
///
/// The flag is *read* here; the code that *sets* it on foundation source
/// collections lives in the foundation `/init` endpoint. Re-exported from
/// `chroma_types` so reader and writer share one definition.
pub use chroma_types::CHROMA_GROUP_CHUNK_SIBLINGS_KEY as GROUP_CHUNK_SIBLINGS_METADATA_KEY;

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
/// This folding is **opt-in per collection** via the
/// [`GROUP_CHUNK_SIBLINGS_METADATA_KEY`] collection-metadata flag, so it
/// only affects collections that have explicitly enabled it (foundation
/// source collections). Collections without the flag continue to group
/// by exact id. The opt-in gate exists because ids that legitimately end
/// in `-{digits}` but are NOT chunk siblings (e.g. `report-2024`,
/// `report-2023`) would otherwise be co-located in one partition —
/// correctness-neutral (they stay distinct records) but a parallelism
/// cost we don't want to impose on collections that don't need the
/// ordering guarantee.
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
    /// When true, chunk siblings (`{base}-{idx}`) are grouped under their
    /// shared base so they land in one partition (see [`chunk_grouping_key`]).
    /// Gated per-collection via the [`GROUP_CHUNK_SIBLINGS_METADATA_KEY`]
    /// collection-metadata flag; defaults to false so behavior is unchanged
    /// for every collection that hasn't opted in.
    pub(crate) group_chunk_siblings: bool,
}

impl PartitionInput {
    /// Create a new partition input.
    /// # Parameters
    /// * `records` - The records to partition.
    /// * `max_partition_size` - The maximum size of a partition. Since we are trying to
    ///   partition the records by id, which can casue the partition size to be larger than this
    ///   value.
    /// * `group_chunk_siblings` - When true, fold chunk siblings onto a
    ///   shared base when partitioning (opt-in per collection).
    pub fn new(
        records: Chunk<LogRecord>,
        max_partition_size: usize,
        group_chunk_siblings: bool,
    ) -> Self {
        PartitionInput {
            records,
            max_partition_size,
            group_chunk_siblings,
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
        group_chunk_siblings: bool,
    ) -> Vec<Chunk<LogRecord>> {
        let mut map = HashMap::new();
        for data in records.iter() {
            let log_record = data.0;
            let index = data.1;
            // Only fold chunk siblings onto a shared base when the
            // collection has opted in; otherwise group by the exact id
            // (unchanged behavior for every other collection).
            let key = if group_chunk_siblings {
                chunk_grouping_key(&log_record.record.id).to_string()
            } else {
                log_record.record.id.clone()
            };
            map.entry(key).or_insert_with(Vec::new).push(index);
        }
        let mut result = Vec::new();
        // Create a new DataChunk for each parition of records with partition_size without
        // data copying.
        let mut current_batch_size = 0;
        let mut new_partition = true;
        let mut visibility = vec![false; records.total_len()];
        for v in map.values() {
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
        let deduped_records = self.partition(records, partition_size, input.group_chunk_siblings);
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
        let input = PartitionInput::new(chunk, 4, false);
        let result = operator.run(&input).await.unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].len(), 3);

        // Test group size is the same as the number of records
        let chunk = Chunk::new(data.clone());
        let operator = PartitionOperator::new();
        let input = PartitionInput::new(chunk, 3, false);
        let result = operator.run(&input).await.unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].len(), 3);

        // Test group size is smaller than the number of records
        let chunk = Chunk::new(data.clone());
        let operator = PartitionOperator::new();
        let input = PartitionInput::new(chunk, 2, false);
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
        let input = PartitionInput::new(chunk, 1, false);
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

    fn chunk_record(offset: i64, id: &str) -> LogRecord {
        LogRecord {
            log_offset: offset,
            record: OperationRecord {
                id: id.to_string(),
                embedding: None,
                encoding: None,
                metadata: None,
                document: None,
                operation: Operation::Add,
            },
        }
    }

    #[tokio::test]
    async fn test_chunk_siblings_share_a_partition_when_opted_in() {
        // Three chunks of one document plus one unrelated record, with
        // group_chunk_siblings = true. Even at partition_size == 1, the
        // chunk siblings stay together (shared grouping key "doc"), so we
        // get exactly 2 partitions: {doc-0, doc-1, doc-2} and {other}.
        let data: Arc<[LogRecord]> = vec![
            chunk_record(1, "doc-0"),
            chunk_record(2, "doc-1"),
            chunk_record(3, "doc-2"),
            chunk_record(4, "other"),
        ]
        .into();

        let operator = PartitionOperator::new();
        let input = PartitionInput::new(Chunk::new(data), 1, true);
        let mut result = operator.run(&input).await.unwrap();

        assert_eq!(result.records.len(), 2);
        result.records.sort_by_key(|x| x.len());
        assert_eq!(result.records[0].len(), 1); // {other}
        assert_eq!(result.records[1].len(), 3); // {doc-0, doc-1, doc-2}
    }

    #[tokio::test]
    async fn test_chunk_siblings_not_grouped_when_opted_out() {
        // Same input with group_chunk_siblings = false (the default):
        // each chunk is a distinct id, so at partition_size == 1 they do
        // NOT fold together. We get one partition per distinct id (4).
        let data: Arc<[LogRecord]> = vec![
            chunk_record(1, "doc-0"),
            chunk_record(2, "doc-1"),
            chunk_record(3, "doc-2"),
            chunk_record(4, "other"),
        ]
        .into();

        let operator = PartitionOperator::new();
        let input = PartitionInput::new(Chunk::new(data), 1, false);
        let result = operator.run(&input).await.unwrap();

        // Four distinct grouping keys -> four single-record partitions.
        assert_eq!(result.records.len(), 4);
        assert!(result.records.iter().all(|r| r.len() == 1));
    }
}
