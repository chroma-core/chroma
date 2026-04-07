use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{blockfile_record::partition_logs_to_shard, bloom_filter::BloomFilterManager};
use chroma_system::{Operator, OperatorType};
use chroma_types::Segment;
use thiserror::Error;

use super::fetch_log::FetchLogOutput;

#[derive(Clone, Debug)]
pub struct FilterLogsForShardOperator {
    pub shard_index: u32,
    pub num_shards: u32,
    pub record_segment: Segment,
    pub blockfile_provider: BlockfileProvider,
    pub bloom_filter_manager: Option<BloomFilterManager>,
}

pub type FilterLogsForShardOutput = FetchLogOutput;

#[derive(Error, Debug)]
pub enum FilterLogsForShardError {
    #[error("Error partitioning logs to shard: {0}")]
    Partition(String),
}

impl ChromaError for FilterLogsForShardError {
    fn code(&self) -> ErrorCodes {
        match self {
            FilterLogsForShardError::Partition(_) => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<FetchLogOutput, FilterLogsForShardOutput> for FilterLogsForShardOperator {
    type Error = FilterLogsForShardError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &FetchLogOutput,
    ) -> Result<FilterLogsForShardOutput, FilterLogsForShardError> {
        partition_logs_to_shard(
            input.clone(),
            self.shard_index,
            self.num_shards,
            &self.record_segment,
            &self.blockfile_provider,
            self.bloom_filter_manager.clone(),
        )
        .await
        .map_err(|e| FilterLogsForShardError::Partition(e.to_string()))
    }
}
