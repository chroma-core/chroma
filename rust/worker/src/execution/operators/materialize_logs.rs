use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_segment::blockfile_record::{
    RecordSegmentReader, RecordSegmentReaderOptions, RecordSegmentReaderShardCreationError,
};
use chroma_segment::types::{
    materialize_logs, LogMaterializerError, PartitionedMaterializeLogsResult,
};
use chroma_system::Operator;
use chroma_types::{Chunk, LogRecord};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MaterializeLogOperatorError {
    #[error("Could not create record segment reader: {0}")]
    RecordSegmentReaderShardCreationFailed(#[from] RecordSegmentReaderShardCreationError),
    #[error("Log materialization failed: {0}")]
    LogMaterializationFailed(#[from] LogMaterializerError),
    #[error("Failed to resolve shard for record: {0}")]
    ShardResolutionFailed(Box<dyn ChromaError>),
    #[error("Failed to hydrate record: {0}")]
    HydrationFailed(Box<dyn ChromaError>),
    #[error("Partitioning the logs failed: {0}")]
    MaterializePartition(Box<dyn ChromaError>),
}

impl ChromaError for MaterializeLogOperatorError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            MaterializeLogOperatorError::RecordSegmentReaderShardCreationFailed(e) => e.code(),
            MaterializeLogOperatorError::LogMaterializationFailed(e) => e.code(),
            MaterializeLogOperatorError::ShardResolutionFailed(e) => e.code(),
            MaterializeLogOperatorError::HydrationFailed(e) => e.code(),
            MaterializeLogOperatorError::MaterializePartition(e) => e.code(),
        }
    }
}

#[derive(Debug)]
pub struct MaterializeLogOperator {}

impl MaterializeLogOperator {
    pub fn new() -> Box<Self> {
        Box::new(MaterializeLogOperator {})
    }
}

#[derive(Debug)]
pub struct MaterializeLogInput {
    logs: Chunk<LogRecord>,
    record_reader: Option<RecordSegmentReader<'static>>,
    offset_ids: Vec<Arc<AtomicU32>>,
    plan: RecordSegmentReaderOptions,
}

impl MaterializeLogInput {
    pub fn new(
        logs: Chunk<LogRecord>,
        record_reader: Option<RecordSegmentReader<'static>>,
        offset_ids: Vec<Arc<AtomicU32>>,
        plan: RecordSegmentReaderOptions,
    ) -> Self {
        MaterializeLogInput {
            logs,
            record_reader,
            offset_ids,
            plan,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MaterializeLogOutput {
    pub result: PartitionedMaterializeLogsResult,
    pub collection_logical_size_delta: i64,
}

#[async_trait]
impl Operator<MaterializeLogInput, MaterializeLogOutput> for MaterializeLogOperator {
    type Error = MaterializeLogOperatorError;

    async fn run(&self, input: &MaterializeLogInput) -> Result<MaterializeLogOutput, Self::Error> {
        tracing::debug!("Materializing {} log entries", input.logs.total_len());

        let shard_logs = match &input.record_reader {
            Some(reader) => reader
                .partition_logs(&input.logs, &input.plan)
                .await
                .map_err(MaterializeLogOperatorError::MaterializePartition)?,
            None => vec![input.logs.clone()],
        };

        // Materialize each shard's logs
        let mut shards = Vec::new();
        let mut total_collection_logical_size_delta = 0i64;

        for (shard_idx, logs) in shard_logs.into_iter().enumerate() {
            // Get the shard reader for this specific shard
            let shard_reader = input
                .record_reader
                .as_ref()
                .and_then(|reader: &RecordSegmentReader| reader.get_shards().get(shard_idx))
                .unwrap_or(&None);
            tracing::info!("Sending {} logs to shard index {}", logs.len(), shard_idx);

            // Get offset_id for this shard, or None if not available
            let offset_id = input.offset_ids.get(shard_idx).cloned();

            let result = materialize_logs(shard_reader, logs, offset_id, &input.plan)
                .await
                .map_err(MaterializeLogOperatorError::LogMaterializationFailed)?;

            // Calculate logical size delta for this shard
            let mut shard_delta = 0i64;
            for record in &result {
                let hydrated =
                    record.hydrate(shard_reader.as_ref()).await.map_err(|e| {
                        MaterializeLogOperatorError::HydrationFailed(
                            Box::new(e) as Box<dyn ChromaError>
                        )
                    })?;
                shard_delta += hydrated.compute_logical_size_delta_bytes();
            }
            total_collection_logical_size_delta += shard_delta;

            shards.push(result);
        }

        let result = PartitionedMaterializeLogsResult { shards };

        Ok(MaterializeLogOutput {
            result,
            collection_logical_size_delta: total_collection_logical_size_delta,
        })
    }
}
