use std::fmt::{Debug, Formatter};

use crate::Log;
use async_trait::async_trait;
use chroma_config::registry::{Injectable, Registry};
use chroma_config::Configurable;
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
    Chunk, CollectionUuid, GetCollectionWithSegmentsError, LogRecord, Schema, SchemaError,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalCompactionManagerConfig {}

#[derive(Clone)]
pub struct LocalCompactionManager {
    log: Log,
    sqlite_db: SqliteDb,
    hnsw_segment_manager: LocalSegmentManager,
    sysdb: SysDb,
}

impl Injectable for LocalCompactionManager {}

#[async_trait]
impl Configurable<LocalCompactionManagerConfig> for LocalCompactionManager {
    async fn try_from_config(
        _config: &LocalCompactionManagerConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let log = registry.get::<Log>().map_err(|e| e.boxed())?;
        let sqlite_db = registry.get::<SqliteDb>().map_err(|e| e.boxed())?;
        let hnsw_segment_manager = registry
            .get::<LocalSegmentManager>()
            .map_err(|e| e.boxed())?;
        let sysdb = registry.get::<SysDb>().map_err(|e| e.boxed())?;
        let res = Self {
            log,
            sqlite_db,
            hnsw_segment_manager,
            sysdb,
        };
        registry.register(res.clone());
        Ok(res)
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

    async fn on_start(&mut self, _: &ComponentContext<Self>) -> () {}
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
    #[error("Error getting collection with segments: {0}")]
    GetCollectionWithSegmentsError(#[from] GetCollectionWithSegmentsError),
    #[error("Error reading from metadata segment reader: {0} ")]
    MetadataReaderError(#[from] SqliteMetadataError),
    #[error("Error reading from hnsw segment reader: {0}")]
    HnswReaderError(#[from] LocalHnswSegmentReaderError),
    #[error("Error constructing hnsw segment reader: {0}")]
    HnswReaderConstructionError(#[from] LocalSegmentManagerError),
    #[error("Error purging logs")]
    PurgeLogsFailure,
    #[error("Failed to reconcile collection schema: {0}")]
    SchemaReconcileError(#[from] SchemaError),
}

impl ChromaError for CompactionManagerError {
    fn code(&self) -> ErrorCodes {
        match self {
            CompactionManagerError::PullLogsFailure => ErrorCodes::InvalidArgument,
            CompactionManagerError::MetadataApplyLogsFailed => ErrorCodes::Internal,
            CompactionManagerError::GetHnswWriterFailed => ErrorCodes::Internal,
            CompactionManagerError::HnswApplyLogsError => ErrorCodes::Internal,
            CompactionManagerError::GetCollectionWithSegmentsError(e) => e.code(),
            CompactionManagerError::MetadataReaderError(e) => e.code(),
            CompactionManagerError::HnswReaderError(e) => e.code(),
            CompactionManagerError::HnswReaderConstructionError(e) => e.code(),
            CompactionManagerError::PurgeLogsFailure => ErrorCodes::Internal,
            CompactionManagerError::SchemaReconcileError(e) => e.code(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PurgeLogsMessage {
    pub collection_id: CollectionUuid,
}

#[derive(Clone, Debug)]
pub struct BackfillMessage {
    pub collection_id: CollectionUuid,
}

#[async_trait]
impl Handler<BackfillMessage> for LocalCompactionManager {
    type Result = Result<(), CompactionManagerError>;

    async fn handle(
        &mut self,
        message: BackfillMessage,
        _: &ComponentContext<LocalCompactionManager>,
    ) -> Self::Result {
        let mut collection_and_segments = self
            .sysdb
            .get_collection_with_segments(message.collection_id)
            .await?;
        let schema_previously_persisted = collection_and_segments.collection.schema.is_some();
        if !schema_previously_persisted {
            collection_and_segments.collection.schema = Some(
                Schema::try_from(&collection_and_segments.collection.config)
                    .map_err(CompactionManagerError::SchemaReconcileError)?,
            );
        }
        // If collection is uninitialized, that means nothing has been written yet.
        let dim = match collection_and_segments.collection.dimension {
            Some(dim) => dim,
            None => return Ok(()),
        };
        // Get the current max seq ids.
        let metadata_reader = SqliteMetadataReader::new(self.sqlite_db.clone());
        let mt_max_seq_id = metadata_reader
            .current_max_seq_id(&collection_and_segments.metadata_segment.id)
            .await?;
        let hnsw_reader = self
            .hnsw_segment_manager
            .get_hnsw_reader(
                &collection_and_segments.collection,
                &collection_and_segments.vector_segment,
                dim as usize,
            )
            .await;
        let hnsw_max_seq_id = match hnsw_reader {
            Ok(reader) => {
                reader
                    .current_max_seq_id(&collection_and_segments.vector_segment.id)
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
                &collection_and_segments.collection.tenant,
                collection_and_segments.collection.collection_id,
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
        let apply_outcome = metadata_writer
            .apply_logs(
                mt_data_chunk,
                collection_and_segments.metadata_segment.id,
                if schema_previously_persisted {
                    collection_and_segments.collection.schema.clone()
                } else {
                    None
                },
                &mut *tx,
            )
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        if schema_previously_persisted {
            if let Some(updated_schema) = apply_outcome.schema_update {
                metadata_writer
                    .update_collection_schema(
                        collection_and_segments.collection.collection_id,
                        &updated_schema,
                        &mut *tx,
                    )
                    .await
                    .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
            }
        }
        tx.commit()
            .await
            .map_err(|_| CompactionManagerError::MetadataApplyLogsFailed)?;
        // Next apply it to the hnsw writer.
        let mut hnsw_writer = self
            .hnsw_segment_manager
            .get_hnsw_writer(
                &collection_and_segments.collection,
                &collection_and_segments.vector_segment,
                dim as usize,
            )
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
        let mut collection = collection_segments.collection.clone();
        if collection.schema.is_none() {
            collection.schema = Some(
                Schema::try_from(&collection.config)
                    .map_err(CompactionManagerError::SchemaReconcileError)?,
            );
        }
        // If dimension is None, that means nothing has been written yet.
        let dim = match collection.dimension {
            Some(dim) => dim,
            None => return Ok(()),
        };
        let metadata_reader = SqliteMetadataReader::new(self.sqlite_db.clone());
        let mt_max_seq_id = metadata_reader
            .current_max_seq_id(&collection_segments.metadata_segment.id)
            .await?;
        let hnsw_reader = self
            .hnsw_segment_manager
            .get_hnsw_reader(
                &collection,
                &collection_segments.vector_segment,
                dim as usize,
            )
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
