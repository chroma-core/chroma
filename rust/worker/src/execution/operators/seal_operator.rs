use std::collections::BinaryHeap;
use std::sync::atomic::AtomicU32;

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_segment::{
    bloom_filter::BloomFilterManager,
    spann_provider::SpannProvider,
    types::{MaterializeLogsResultIter, PartitionedMaterializeLogsResult},
};
use chroma_system::Operator;
use chroma_types::MaterializedLogOperation;
use thiserror::Error;

use crate::execution::orchestration::compact::CreateNewShardError;

#[derive(Error, Debug)]
pub enum SealOperatorError {
    #[error("Failed to create new shards: {0}")]
    CreateNewShardError(#[from] CreateNewShardError),
    #[error("Segment error: {0}")]
    SegmentError(#[from] Box<dyn ChromaError>),
    #[error("Invariant violation: {0}")]
    InvariantViolation(String),
}

impl ChromaError for SealOperatorError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            SealOperatorError::InvariantViolation { .. } => {
                chroma_error::ErrorCodes::InvalidArgument
            }
            _ => chroma_error::ErrorCodes::Internal,
        }
    }
}

struct PartitionCursor<'a> {
    offset_id: u32,
    partition_idx: usize,
    iter: MaterializeLogsResultIter<'a>,
}

impl PartialEq for PartitionCursor<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.offset_id == other.offset_id && self.partition_idx == other.partition_idx
    }
}

impl Eq for PartitionCursor<'_> {}

impl PartialOrd for PartitionCursor<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PartitionCursor<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reversed for min-heap: smallest offset_id has highest priority
        other
            .offset_id
            .cmp(&self.offset_id)
            .then_with(|| other.partition_idx.cmp(&self.partition_idx))
    }
}

#[derive(Debug)]
pub struct SealOperator {}

impl SealOperator {
    pub fn new() -> Box<Self> {
        Box::new(SealOperator {})
    }
}

#[derive(Debug, Clone)]
pub struct SealInput {
    pub writers: crate::execution::orchestration::compact::CompactWriters,
    pub materialized_outputs: Vec<PartitionedMaterializeLogsResult>,
    pub shard_size: Option<u64>,
    pub collection: chroma_types::Collection,
    pub blockfile_provider: BlockfileProvider,
    pub bloom_filter_manager: Option<BloomFilterManager>,
    pub spann_provider: SpannProvider,
    #[cfg(test)]
    pub poison_offset: Option<u32>,
}

impl SealInput {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        writers: crate::execution::orchestration::compact::CompactWriters,
        materialized_outputs: Vec<PartitionedMaterializeLogsResult>,
        shard_size: Option<u64>,
        collection: chroma_types::Collection,
        blockfile_provider: BlockfileProvider,
        bloom_filter_manager: Option<BloomFilterManager>,
        spann_provider: SpannProvider,
        #[cfg(test)] poison_offset: Option<u32>,
    ) -> Self {
        SealInput {
            writers,
            materialized_outputs,
            shard_size,
            collection,
            blockfile_provider,
            bloom_filter_manager,
            spann_provider,
            #[cfg(test)]
            poison_offset,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SealOutput {
    pub sealed_writers: crate::execution::orchestration::compact::CompactWriters,
    pub split_materialized_outputs: Vec<PartitionedMaterializeLogsResult>,
}

/// Implementation of the SealOperator.
///
/// IMPORTANT: This operator assumes it will create at most one new shard during the sealing
/// process. It does not perform recursive splitting - if the active shard exceeds the configured
/// shard size, it will only split once, moving overflow records into a single new shard.
/// The new shard may itself exceed the shard size limit, but further splitting would require
/// another compaction cycle.
#[async_trait]
impl Operator<SealInput, SealOutput> for SealOperator {
    type Error = SealOperatorError;

    async fn run(&self, input: &SealInput) -> Result<SealOutput, Self::Error> {
        let shard_size = match input.shard_size {
            Some(size) if size > 0 => size,
            _ => {
                return Ok(SealOutput {
                    sealed_writers: input.writers.clone(),
                    split_materialized_outputs: input.materialized_outputs.clone(),
                });
            }
        };

        let active_shard_record_count = match input.writers.record_reader.as_ref() {
            Some(reader) => reader.get_active_shard_record_count().await? as u64,
            None => 0u64,
        };

        let mut active_shard_delta = 0i32;
        for output in &input.materialized_outputs {
            active_shard_delta += output.get_active_record_delta();
        }

        tracing::info!(
            "Seal operator: active shard has {} existing records, {} new adds, shard_size = {}",
            active_shard_record_count,
            active_shard_delta,
            shard_size
        );

        if active_shard_delta <= 0 {
            return Ok(SealOutput {
                sealed_writers: input.writers.clone(),
                split_materialized_outputs: input.materialized_outputs.clone(),
            });
        }

        // Calculate how many records would be in the active shard after adding new records
        let total_in_active =
            active_shard_record_count.saturating_add_signed(active_shard_delta as i64);
        // Calculate excess: how many records exceed the shard size
        let excess_record_count = total_in_active.saturating_sub(shard_size);

        if excess_record_count == 0 {
            return Ok(SealOutput {
                sealed_writers: input.writers.clone(),
                split_materialized_outputs: input.materialized_outputs.clone(),
            });
        }

        tracing::info!(
            "Active shard overflow detected: {} + {} > {} with {} excess records. Creating new active shard",
            active_shard_record_count,
            active_shard_delta,
            shard_size,
            excess_record_count
        );

        let mut sealed_writers = input.writers.clone();
        sealed_writers
            .create_new_shard(
                &input.collection,
                &input.blockfile_provider,
                input.bloom_filter_manager.clone(),
                &input.spann_provider,
            )
            .await?;

        // Use count-based split to move exactly the excess records to the new shard
        let new_shard_outputs = split_materialized_outputs_by_count(
            &input.materialized_outputs,
            excess_record_count as usize,
        )?;

        Ok(SealOutput {
            sealed_writers,
            split_materialized_outputs: new_shard_outputs,
        })
    }
}

fn split_materialized_outputs_by_count(
    outputs: &[PartitionedMaterializeLogsResult],
    count_to_move: usize,
) -> Result<Vec<PartitionedMaterializeLogsResult>, SealOperatorError> {
    // MaterializeLogsResult::split moves AddNew records with offset_id >= pivot to the new
    // shard. Since AddNew records are assigned monotonically increasing offset_ids, the HIGHEST
    // offsets move and the lowest stay. To move exactly `count_to_move` records, we therefore
    // need to identify the pivot as the offset_id of the first AddNew record that should move,
    // i.e. skip the `count_to_stay` lowest-offset AddNew records, then pivot on the next.
    //
    // Algorithm:
    // 1. Count total AddNew records across all partitions' active shards.
    // 2. count_to_stay = total_add_new - count_to_move (clamped to 0).
    // 3. Initialize PQ with first AddNew from each partition, pop `count_to_stay` records
    //    (smallest offset_ids), and set pivot = next AddNew offset_id.

    let total_add_new: usize = outputs
        .iter()
        .filter_map(|p| p.shards.last())
        .map(|shard| {
            shard
                .iter()
                .filter(|r| r.get_operation() == MaterializedLogOperation::AddNew)
                .count()
        })
        .sum();

    let count_to_stay = total_add_new.saturating_sub(count_to_move);

    let mut pq = BinaryHeap::new();

    for (partition_idx, partition) in outputs.iter().enumerate() {
        debug_assert!(
            !partition.shards.is_empty(),
            "Partition {} has no shards - this is an invariant violation",
            partition_idx
        );

        if let Some(active_shard) = partition.shards.last() {
            let mut iter = active_shard.iter();
            if let Some(first_record) = advance_to_next_add_new(&mut iter) {
                pq.push(PartitionCursor {
                    offset_id: first_record.get_offset_id(),
                    partition_idx,
                    iter,
                });
            }
        }
    }

    if pq.is_empty() {
        return Err(SealOperatorError::InvariantViolation(format!(
            "No movable records in active shards, but count_to_move = {count_to_move}",
        )));
    }

    if count_to_stay == 0 {
        let pivot = pq.peek().map(|c| c.offset_id).unwrap_or(u32::MAX);
        return split_materialized_outputs(outputs, pivot);
    }

    let mut records_processed = 0;

    let pivot_offset_id = loop {
        let Some(mut cursor) = pq.pop() else {
            return Err(SealOperatorError::InvariantViolation(format!(
                "Exhausted all records after {records_processed} but needed to keep {count_to_stay}",
            )));
        };

        records_processed += 1;

        if records_processed > count_to_stay {
            // This cursor's offset_id is the pivot - it's the first record to be moved
            break cursor.offset_id;
        }

        if let Some(next_record) = advance_to_next_add_new(&mut cursor.iter) {
            cursor.offset_id = next_record.get_offset_id();
            pq.push(cursor);
        }
    };

    split_materialized_outputs(outputs, pivot_offset_id)
}

fn advance_to_next_add_new<'a>(
    iter: &mut MaterializeLogsResultIter<'a>,
) -> Option<chroma_segment::types::BorrowedMaterializedLogRecord<'a>> {
    iter.find(|r| r.get_operation() == MaterializedLogOperation::AddNew)
}

fn split_materialized_outputs(
    outputs: &[PartitionedMaterializeLogsResult],
    pivot_offset_id: u32,
) -> Result<Vec<PartitionedMaterializeLogsResult>, SealOperatorError> {
    let mut new_shard_results = Vec::new();
    let next_new_offset_id = AtomicU32::new(1);

    for output_partition in outputs {
        let new_partition = output_partition
            .split(pivot_offset_id, Some(&next_new_offset_id))
            .ok_or_else(|| {
                SealOperatorError::InvariantViolation(
                    "Failed to split partition: no shards found".to_string(),
                )
            })?;
        new_shard_results.push(new_partition);
    }

    Ok(new_shard_results)
}
