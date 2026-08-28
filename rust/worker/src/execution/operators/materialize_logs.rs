use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_segment::blockfile_record::{
    RecordSegmentReader, RecordSegmentReaderOptions, RecordSegmentReaderShardCreationError,
};
use chroma_segment::types::{
    materialize_logs, LogMaterializerError, PartitionedMaterializeLogsResult,
};
use chroma_system::Operator;
use chroma_types::{
    logical_size_of_metadata, Chunk, FunctionWorkload, LogRecord, MaterializedLogOperation,
};
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
    /// Workload facts observed while materializing these logs.
    pub function_workload: Option<FunctionWorkload>,
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
        let mut function_workload = FunctionWorkload::current();
        function_workload.source_log_records = input.logs.len() as u64;
        function_workload.source_log_bytes = input
            .logs
            .iter()
            .map(|(record, _)| record.size_bytes())
            .sum();

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
                let merged_metadata = hydrated.merged_metadata();
                shard_delta +=
                    hydrated.compute_logical_size_delta_bytes_with_metadata(&merged_metadata);
                function_workload.materialized_records =
                    function_workload.materialized_records.saturating_add(1);

                if hydrated.get_operation() != MaterializedLogOperation::DeleteExisting {
                    let id_bytes = hydrated.get_user_id().len() as u64;
                    let document_bytes = hydrated
                        .merged_document_ref()
                        .map_or(0, |document| document.len() as u64);
                    let metadata_bytes = logical_size_of_metadata(&merged_metadata) as u64;
                    let embedding_bytes =
                        std::mem::size_of_val(hydrated.merged_embeddings_ref()) as u64;
                    let non_embedding_record_bytes = id_bytes
                        .saturating_add(document_bytes)
                        .saturating_add(metadata_bytes);

                    function_workload.non_delete_records =
                        function_workload.non_delete_records.saturating_add(1);
                    function_workload.id_bytes =
                        function_workload.id_bytes.saturating_add(id_bytes);
                    function_workload.document_bytes = function_workload
                        .document_bytes
                        .saturating_add(document_bytes);
                    function_workload.metadata_bytes = function_workload
                        .metadata_bytes
                        .saturating_add(metadata_bytes);
                    function_workload.embedding_bytes = function_workload
                        .embedding_bytes
                        .saturating_add(embedding_bytes);
                    function_workload.metadata_entries = function_workload
                        .metadata_entries
                        .saturating_add(merged_metadata.len() as u64);
                    function_workload.max_non_embedding_record_bytes = function_workload
                        .max_non_embedding_record_bytes
                        .max(non_embedding_record_bytes);
                }
            }
            total_collection_logical_size_delta += shard_delta;

            shards.push(result);
        }

        let result = PartitionedMaterializeLogsResult { shards };

        Ok(MaterializeLogOutput {
            result,
            collection_logical_size_delta: total_collection_logical_size_delta,
            function_workload: Some(function_workload),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_segment::blockfile_record::RecordSegmentReaderOptions;
    use chroma_system::Operator;
    use chroma_types::{Operation, OperationRecord, UpdateMetadataValue};
    use std::collections::HashMap;

    #[tokio::test]
    async fn records_workload_facts_during_hydration() {
        let logs = Chunk::new(
            vec![LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "id".to_string(),
                    embedding: Some(vec![1.0, 2.0]),
                    encoding: None,
                    metadata: Some(HashMap::from([(
                        "k".to_string(),
                        UpdateMetadataValue::Str("value".to_string()),
                    )])),
                    document: Some("doc".to_string()),
                    operation: Operation::Add,
                },
            }]
            .into(),
        );

        let output = MaterializeLogOperator::new()
            .run(&MaterializeLogInput::new(
                logs,
                None,
                vec![],
                RecordSegmentReaderOptions::default(),
            ))
            .await
            .unwrap();

        assert_eq!(
            output.function_workload.unwrap(),
            FunctionWorkload {
                format_version: 1,
                source_log_records: 1,
                source_log_bytes: 27,
                materialized_records: 1,
                non_delete_records: 1,
                id_bytes: 2,
                document_bytes: 3,
                metadata_bytes: 6,
                embedding_bytes: 8,
                metadata_entries: 1,
                max_non_embedding_record_bytes: 11,
            }
        );
    }
}
