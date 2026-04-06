use crate::execution::orchestration::compact::CreateNewShardError;
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_segment::{
    bloom_filter::BloomFilterManager, spann_provider::SpannProvider,
    types::PartitionedMaterializeLogsResult,
};
use chroma_system::Operator;
use thiserror::Error;

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

        // TODO(tanujnay112): Change this to actually calculate a good pivot to move
        let min_offset_to_move = 0;

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

        let new_shard_outputs =
            split_materialized_outputs(&input.materialized_outputs, min_offset_to_move)?;

        Ok(SealOutput {
            sealed_writers,
            split_materialized_outputs: new_shard_outputs,
        })
    }
}

fn split_materialized_outputs(
    outputs: &[PartitionedMaterializeLogsResult],
    pivot_offset_id: u32,
) -> Result<Vec<PartitionedMaterializeLogsResult>, SealOperatorError> {
    let mut new_shard_results = Vec::new();

    for output_partition in outputs {
        let new_partition = output_partition.split(pivot_offset_id).ok_or_else(|| {
            SealOperatorError::InvariantViolation(
                "Failed to split partition: no shards found".to_string(),
            )
        })?;
        new_shard_results.push(new_partition);
    }

    Ok(new_shard_results)
}
