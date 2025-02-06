use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::local_segment_manager::LocalSegmentManager;
use chroma_segment::sqlite_metadata::SqliteMetadataWriter;
use chroma_sysdb::SysDb;
use chroma_system::Handler;
use chroma_system::{Component, ComponentContext};
use chroma_types::{Chunk, CollectionUuid, GetCollectionWithSegmentsError, LogRecord};
use thiserror::Error;

use crate::Log;

pub struct LocalCompactionManager {
    #[allow(dead_code)]
    log: Box<Log>,
    metadata_writer: SqliteMetadataWriter,
    hnsw_segment_manager: LocalSegmentManager,
    sysdb: Box<SysDb>,
    // TODO(Sanket): config
}

impl LocalCompactionManager {
    #[allow(dead_code)]
    pub fn new(
        log: Box<Log>,
        metadata_writer: SqliteMetadataWriter,
        hnsw_segment_manager: LocalSegmentManager,
        sysdb: Box<SysDb>,
    ) -> Self {
        LocalCompactionManager {
            log,
            metadata_writer,
            hnsw_segment_manager,
            sysdb,
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

impl Debug for LocalCompactionManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalCompactionManager").finish()
    }
}

#[derive(Error, Debug)]
pub enum CompactionManagerError {
    #[error("Collection uninitialized")]
    CollectionUninitialized,
    #[error("Failed to pull logs from the log store")]
    PullLogsFailure,
    #[error("Failed to apply logs to the metadata segment")]
    MetadataApplyLogsFailed,
    #[error("Failed to get hnsw segment writer")]
    GetHnswWriterFailed,
    #[error("Failed to apply logs to the hnsw segment writer")]
    HnswApplyLogsError,
    #[error("Error getting collection with segments")]
    GetCollectionWithSegmentsError(#[from] GetCollectionWithSegmentsError),
}

impl ChromaError for CompactionManagerError {
    fn code(&self) -> ErrorCodes {
        match self {
            CompactionManagerError::CollectionUninitialized => ErrorCodes::Internal,
            CompactionManagerError::PullLogsFailure => ErrorCodes::InvalidArgument,
            CompactionManagerError::MetadataApplyLogsFailed => ErrorCodes::Internal,
            CompactionManagerError::GetHnswWriterFailed => ErrorCodes::Internal,
            CompactionManagerError::HnswApplyLogsError => ErrorCodes::Internal,
            CompactionManagerError::GetCollectionWithSegmentsError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CompactionMessage {
    pub collection_id: CollectionUuid,
    pub start_offset: i64,
    pub end_offset: i64,
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
        let collection_segments = self
            .sysdb
            .get_collection_with_segments(message.collection_id)
            .await?;
        let collection_dimension = collection_segments
            .collection
            .dimension
            .ok_or(CompactionManagerError::CollectionUninitialized)?;
        // Apply the records to the metadata writer.
        let mut tx = self
            .metadata_writer
            .begin()
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        self.metadata_writer
            .apply_logs(
                data_chunk.clone(),
                collection_segments.metadata_segment.id,
                &mut *tx,
            )
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        tx.commit()
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        // Next apply it to the hnsw writer.
        // TODO(Sanket): Avoid unwrap here.
        let mut hnsw_writer = self
            .hnsw_segment_manager
            .get_hnsw_writer(
                &collection_segments.vector_segment,
                collection_dimension as usize,
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
