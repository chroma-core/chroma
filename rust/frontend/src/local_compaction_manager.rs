use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_segment::local_segment_manager::LocalSegmentManager;
use chroma_segment::sqlite_metadata::SqliteMetadataWriter;
use chroma_system::Handler;
use chroma_system::{Component, ComponentContext};
use chroma_types::{Chunk, CollectionUuid, LogRecord, Segment, SegmentUuid};
use thiserror::Error;

pub struct LocalCompactionManager {
    #[allow(dead_code)]
    log: Box<Log>,
    metadata_writer: SqliteMetadataWriter,
    hnsw_segment_manager: LocalSegmentManager,
    // TODO(Sanket): config
}

impl LocalCompactionManager {
    #[allow(dead_code)]
    pub fn new(
        log: Box<Log>,
        metadata_writer: SqliteMetadataWriter,
        hnsw_segment_manager: LocalSegmentManager,
    ) -> Self {
        LocalCompactionManager {
            log,
            metadata_writer,
            hnsw_segment_manager,
        }
    }
}

#[async_trait]
impl Component for LocalCompactionManager {
    fn get_name() -> &'static str {
        "Local Compaction manager"
    }

    fn queue_size(&self) -> usize {
        // TODO(Sanket): Make this configurable.
        1000
    }

    async fn start(&mut self, _: &ComponentContext<Self>) -> () {}
}

#[derive(Clone, Debug)]
pub struct CompactionMessage {
    collection_id: CollectionUuid,
    segment_id: SegmentUuid,
    start_offset: i64,
    end_offset: i64,
    segment: Segment,
    dimensionality: usize,
    persist_path: String,
}

impl Debug for LocalCompactionManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalCompactionManager").finish()
    }
}

#[derive(Error, Debug)]
pub enum CompactionManagerError {
    #[error("Failed to pull logs from the log store")]
    PullLogsFailure,
    #[error("Failed to apply logs to the metadata segment")]
    MetadataApplyLogsFailed,
    #[error("Failed to get hnsw segment writer")]
    GetHnswWriterFailed,
    #[error("Failed to apply logs to the hnsw segment writer")]
    HnswApplyLogsError,
}

impl ChromaError for CompactionManagerError {
    fn code(&self) -> ErrorCodes {
        match self {
            CompactionManagerError::PullLogsFailure => ErrorCodes::InvalidArgument,
            CompactionManagerError::MetadataApplyLogsFailed => ErrorCodes::Internal,
            CompactionManagerError::GetHnswWriterFailed => ErrorCodes::Internal,
            CompactionManagerError::HnswApplyLogsError => ErrorCodes::Internal,
        }
    }
}

// ============== Handlers ==============
#[async_trait]
impl Handler<CompactionMessage> for LocalCompactionManager {
    type Result = Result<(), CompactionManagerError>;

    async fn handle(
        &mut self,
        message: CompactionMessage,
        _: &ComponentContext<LocalCompactionManager>,
    ) -> Self::Result {
        let data = self
            .log
            .read(
                message.collection_id,
                message.start_offset,
                (message.end_offset - message.start_offset) as i32,
                None,
            )
            .await
            .map_err(|_| CompactionManagerError::PullLogsFailure)?;
        let data_chunk: Chunk<LogRecord> = Chunk::new(data.into());
        // Apply the records to the metadata writer.
        let mut tx = self
            .metadata_writer
            .begin()
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        self.metadata_writer
            .apply_logs(data_chunk.clone(), message.segment_id, &mut tx)
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        tx.commit()
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        // Next apply it to the hnsw writer.
        let mut hnsw_writer = self
            .hnsw_segment_manager
            .get_hnsw_writer(
                &message.segment,
                message.dimensionality,
                message.persist_path,
            )
            .await
            .map_err(|_| CompactionManagerError::GetHnswWriterFailed)?;
        hnsw_writer
            .apply_log_chunk(data_chunk)
            .await
            .map_err(|_| CompactionManagerError::HnswApplyLogsError)?;
        Ok(())
    }
}
