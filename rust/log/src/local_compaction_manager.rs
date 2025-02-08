use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::local_hnsw::LocalHnswSegmentReaderError;
use chroma_segment::local_segment_manager::{LocalSegmentManager, LocalSegmentManagerError};
use chroma_segment::sqlite_metadata::{
    SqliteMetadataError, SqliteMetadataReader, SqliteMetadataWriter,
};
use chroma_sqlite::db::SqliteDb;
use chroma_sysdb::SysDb;
use chroma_system::Handler;
use chroma_system::{Component, ComponentContext};
use chroma_types::{
    Chunk, CollectionAndSegments, CollectionUuid, GetCollectionWithSegmentsError, LogRecord,
};
use thiserror::Error;

use crate::Log;

pub struct LocalCompactionManager {
    log: Box<Log>,
    sqlite_db: SqliteDb,
    hnsw_segment_manager: LocalSegmentManager,
    sysdb: Box<SysDb>,
    // TODO(Sanket): config
}

impl LocalCompactionManager {
    pub fn new(
        log: Box<Log>,
        sqlite_db: SqliteDb,
        hnsw_segment_manager: LocalSegmentManager,
        sysdb: Box<SysDb>,
    ) -> Self {
        LocalCompactionManager {
            log,
            sqlite_db,
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
    #[error("Error reading from metadata segment reader")]
    MetadataReaderError(#[from] SqliteMetadataError),
    #[error("Error reading from hnsw segment reader")]
    HnswReaderError(#[from] LocalHnswSegmentReaderError),
    #[error("Error constructing hnsw segment reader")]
    HnswReaderConstructionError(#[from] LocalSegmentManagerError),
    #[error("Error purging logs")]
    PurgeLogsFailure,
}

impl ChromaError for CompactionManagerError {
    fn code(&self) -> ErrorCodes {
        match self {
            CompactionManagerError::PullLogsFailure => ErrorCodes::InvalidArgument,
            CompactionManagerError::MetadataApplyLogsFailed => ErrorCodes::Internal,
            CompactionManagerError::GetHnswWriterFailed => ErrorCodes::Internal,
            CompactionManagerError::HnswApplyLogsError => ErrorCodes::Internal,
            CompactionManagerError::GetCollectionWithSegmentsError(_) => ErrorCodes::Internal,
            CompactionManagerError::MetadataReaderError(e) => e.code(),
            CompactionManagerError::HnswReaderError(e) => e.code(),
            CompactionManagerError::HnswReaderConstructionError(e) => e.code(),
            CompactionManagerError::PurgeLogsFailure => ErrorCodes::Internal,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CompactionMessage {
    pub collection_id: CollectionUuid,
    pub start_offset: i64,
    pub total_records: i64,
}

#[derive(Clone, Debug)]
pub struct PurgeLogsMessage {
    pub collection_id: CollectionUuid,
}

#[derive(Clone, Debug)]
pub struct BackfillMessage {
    pub collection_and_segment: CollectionAndSegments,
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
                message.total_records as i32,
                None,
            )
            .await
            .map_err(|_| CompactionManagerError::PullLogsFailure)?;
        let data_chunk: Chunk<LogRecord> = Chunk::new(data.into());
        let collection_segments = self
            .sysdb
            .get_collection_with_segments(message.collection_id)
            .await?;
        let collection_dimension = match collection_segments.collection.dimension {
            Some(dim) => dim as usize,
            None => return Ok(()),
        };
        let metadata_writer = SqliteMetadataWriter::new(self.sqlite_db.clone());
        // Apply the records to the metadata writer.
        let mut tx = metadata_writer
            .begin()
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        metadata_writer
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
        let mut hnsw_writer = self
            .hnsw_segment_manager
            .get_hnsw_writer(&collection_segments.vector_segment, collection_dimension)
            .await
            .map_err(|_| CompactionManagerError::GetHnswWriterFailed)?;
        hnsw_writer
            .apply_log_chunk(data_chunk)
            .await
            .map_err(|_| CompactionManagerError::HnswApplyLogsError)?;
        Ok(())
    }
}

#[async_trait]
impl Handler<BackfillMessage> for LocalCompactionManager {
    type Result = Result<(), CompactionManagerError>;

    async fn handle(
        &mut self,
        message: BackfillMessage,
        _: &ComponentContext<LocalCompactionManager>,
    ) -> Self::Result {
        // If collection is uninitialized, that means nothing has been written yet.
        let dim = match message.collection_and_segment.collection.dimension {
            Some(dim) => dim,
            None => return Ok(()),
        };
        // Get the current max seq ids.
        let metadata_reader = SqliteMetadataReader::new(self.sqlite_db.clone());
        let mt_max_seq_id = metadata_reader
            .current_max_seq_id(&message.collection_and_segment.metadata_segment.id)
            .await?;
        let hnsw_reader = self
            .hnsw_segment_manager
            .get_hnsw_reader(&message.collection_and_segment.vector_segment, dim as usize)
            .await;
        let hnsw_max_seq_id = match hnsw_reader {
            Ok(reader) => {
                reader
                    .current_max_seq_id(&message.collection_and_segment.vector_segment.id)
                    .await?
            }
            Err(LocalSegmentManagerError::LocalHnswSegmentReaderError(
                LocalHnswSegmentReaderError::UninitializedSegment,
            )) => 0,
            Err(e) => return Err(CompactionManagerError::HnswReaderConstructionError(e)),
        };
        // Get the logs from log service beyond this offset to backfill.
        let logs = self
            .log
            .read(
                message.collection_and_segment.collection.collection_id,
                mt_max_seq_id.min(hnsw_max_seq_id) as i64,
                -1,
                None,
            )
            .await
            .map_err(|_| CompactionManagerError::PullLogsFailure)?;
        // Set the visibility of the records to be backfilled in the metadata segment.
        let mut mt_visibility = vec![true; logs.len()];
        let mut hnsw_visibility = vec![true; logs.len()];
        let data_chunk: Chunk<LogRecord> = Chunk::new(logs.into());
        let mut mt_data_chunk = data_chunk.clone();
        let mut hnsw_data_chunk = data_chunk.clone();
        for (data, index) in data_chunk.iter() {
            if data.log_offset <= mt_max_seq_id as i64 {
                mt_visibility[index] = false;
            }
            if data.log_offset <= hnsw_max_seq_id as i64 {
                hnsw_visibility[index] = false;
            }
        }
        mt_data_chunk.set_visibility(mt_visibility);
        hnsw_data_chunk.set_visibility(hnsw_visibility);
        // Apply the records to the metadata writer.
        let metadata_writer = SqliteMetadataWriter::new(self.sqlite_db.clone());
        let mut tx = metadata_writer
            .begin()
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        metadata_writer
            .apply_logs(
                mt_data_chunk,
                message.collection_and_segment.metadata_segment.id,
                &mut *tx,
            )
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        tx.commit()
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        // Next apply it to the hnsw writer.
        let mut hnsw_writer = self
            .hnsw_segment_manager
            .get_hnsw_writer(&message.collection_and_segment.vector_segment, dim as usize)
            .await
            .map_err(|_| CompactionManagerError::GetHnswWriterFailed)?;
        hnsw_writer
            .apply_log_chunk(hnsw_data_chunk)
            .await
            .map_err(|_| CompactionManagerError::HnswApplyLogsError)?;
        Ok(())
    }
}

#[async_trait]
impl Handler<PurgeLogsMessage> for LocalCompactionManager {
    type Result = Result<(), CompactionManagerError>;

    async fn handle(
        &mut self,
        message: PurgeLogsMessage,
        _: &ComponentContext<LocalCompactionManager>,
    ) -> Self::Result {
        let collection_segments = self
            .sysdb
            .get_collection_with_segments(message.collection_id)
            .await?;
        // If dimension is None, that means nothing has been written yet.
        let dim = match collection_segments.collection.dimension {
            Some(dim) => dim,
            None => return Ok(()),
        };
        let metadata_reader = SqliteMetadataReader::new(self.sqlite_db.clone());
        let mt_max_seq_id = metadata_reader
            .current_max_seq_id(&collection_segments.metadata_segment.id)
            .await?;
        let hnsw_reader = self
            .hnsw_segment_manager
            .get_hnsw_reader(&collection_segments.vector_segment, dim as usize)
            .await;
        let hnsw_max_seq_id = match hnsw_reader {
            Ok(reader) => {
                reader
                    .current_max_seq_id(&collection_segments.vector_segment.id)
                    .await?
            }
            Err(LocalSegmentManagerError::LocalHnswSegmentReaderError(
                LocalHnswSegmentReaderError::UninitializedSegment,
            )) => 0,
            Err(e) => return Err(CompactionManagerError::HnswReaderConstructionError(e)),
        };
        let max_seq_id = mt_max_seq_id.min(hnsw_max_seq_id);
        self.log
            .purge_logs(message.collection_id, max_seq_id)
            .await
            .map_err(|_| CompactionManagerError::PurgeLogsFailure)
    }
}
