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
    Chunk, Collection, CollectionUuid, DatabaseName, GetCollectionWithSegmentsError,
    HnswParametersFromSegmentError, LogRecord, Schema, SchemaError, Segment, SegmentType,
    SegmentUuid,
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
    #[error("Collection has invalid HNSW configuration")]
    InvalidHnswConfiguration,
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
            CompactionManagerError::InvalidHnswConfiguration => ErrorCodes::InvalidArgument,
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

async fn max_purge_seq_id(
    sqlite_db: &SqliteDb,
    metadata_segment_id: &SegmentUuid,
    vector_segment_id: &SegmentUuid,
) -> Result<u64, CompactionManagerError> {
    let metadata_reader = SqliteMetadataReader::new(sqlite_db.clone());
    let mt_max_seq_id = metadata_reader
        .current_max_seq_id(metadata_segment_id)
        .await?;
    let hnsw_max_seq_id = metadata_reader
        .current_max_seq_id(vector_segment_id)
        .await?;
    Ok(mt_max_seq_id.min(hnsw_max_seq_id))
}

fn has_invalid_hnsw_configuration(collection: &Collection, vector_segment: &Segment) -> bool {
    if !matches!(
        vector_segment.r#type,
        SegmentType::HnswLocalMemory | SegmentType::HnswLocalPersisted
    ) {
        return false;
    }

    let owned_schema;
    let schema = match collection.schema.as_ref() {
        Some(schema) => schema,
        None => {
            owned_schema = match Schema::try_from(&collection.config) {
                Ok(schema) => schema,
                Err(_) => return true,
            };
            &owned_schema
        }
    };

    matches!(
        schema.get_internal_hnsw_config_with_legacy_fallback(vector_segment),
        Err(HnswParametersFromSegmentError::InvalidParameters(_))
    )
}

fn is_invalid_hnsw_reader_configuration_error(error: &LocalSegmentManagerError) -> bool {
    matches!(
        error,
        LocalSegmentManagerError::LocalHnswSegmentReaderError(
            LocalHnswSegmentReaderError::InvalidHnswConfiguration(_)
        )
    )
}

async fn max_purge_seq_id_for_collection(
    sqlite_db: &SqliteDb,
    collection: &Collection,
    metadata_segment: &Segment,
    vector_segment: &Segment,
) -> Result<u64, CompactionManagerError> {
    if has_invalid_hnsw_configuration(collection, vector_segment) {
        // Invalid persisted HNSW config prevents the vector watermark from
        // advancing, but metadata backfill can still bound log retention.
        let metadata_reader = SqliteMetadataReader::new(sqlite_db.clone());
        return Ok(metadata_reader
            .current_max_seq_id(&metadata_segment.id)
            .await?);
    }
    max_purge_seq_id(sqlite_db, &metadata_segment.id, &vector_segment.id).await
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
            .get_collection_with_segments(None, message.collection_id)
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
        let mut invalid_hnsw_configuration = false;
        let hnsw_max_seq_id = match hnsw_reader {
            Ok(reader) => {
                reader
                    .current_max_seq_id(&collection_and_segments.vector_segment.id)
                    .await?
            }
            Err(LocalSegmentManagerError::LocalHnswSegmentReaderError(
                LocalHnswSegmentReaderError::UninitializedSegment,
            )) => 0,
            Err(e) if is_invalid_hnsw_reader_configuration_error(&e) => {
                invalid_hnsw_configuration = true;
                0
            }
            Err(e) => return Err(CompactionManagerError::HnswReaderConstructionError(e)),
        };
        // Get the logs from log service beyond this offset to backfill.
        let dbname = DatabaseName::new(collection_and_segments.collection.database.clone())
            .ok_or(CompactionManagerError::PullLogsFailure)?;
        let logs = self
            .log
            .read(
                &collection_and_segments.collection.tenant,
                // It is up to the log impl to use or not use this.
                dbname,
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
        if invalid_hnsw_configuration {
            return Err(CompactionManagerError::InvalidHnswConfiguration);
        }
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
            .get_collection_with_segments(None, message.collection_id)
            .await?;
        // If dimension is None, that means nothing has been written yet.
        if collection_segments.collection.dimension.is_none() {
            return Ok(());
        }
        let max_seq_id = max_purge_seq_id_for_collection(
            &self.sqlite_db,
            &collection_segments.collection,
            &collection_segments.metadata_segment,
            &collection_segments.vector_segment,
        )
        .await?;
        self.log
            .purge_logs(message.collection_id, max_seq_id)
            .await
            .map_err(|_| CompactionManagerError::PurgeLogsFailure)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_config::{registry::Registry, Configurable};
    use chroma_sqlite::{
        config::{MigrationHash, MigrationMode, SqliteDBConfig},
        db::test_utils::new_test_db_persist_path,
    };
    use chroma_types::{
        Collection, CollectionUuid, Segment, SegmentScope, SegmentType, VectorIndexConfiguration,
    };
    use std::collections::HashMap;

    async fn setup_sqlite_db() -> SqliteDb {
        SqliteDb::try_from_config(
            &SqliteDBConfig {
                url: new_test_db_persist_path(),
                migration_mode: MigrationMode::Apply,
                hash_type: MigrationHash::SHA256,
            },
            &Registry::new(),
        )
        .await
        .expect("sqlite db")
    }

    async fn set_segment_seq_id(sqlite_db: &SqliteDb, segment_id: SegmentUuid, seq_id: i64) {
        sqlx::query("INSERT INTO max_seq_id (segment_id, seq_id) VALUES ($1, $2)")
            .bind(segment_id.to_string())
            .bind(seq_id)
            .execute(sqlite_db.get_conn())
            .await
            .expect("insert max_seq_id");
    }

    #[tokio::test]
    async fn max_purge_seq_id_uses_persisted_segment_watermarks() {
        let sqlite_db = setup_sqlite_db().await;
        let metadata_segment_id = SegmentUuid::new();
        let vector_segment_id = SegmentUuid::new();

        set_segment_seq_id(&sqlite_db, metadata_segment_id, 40).await;
        set_segment_seq_id(&sqlite_db, vector_segment_id, 17).await;

        assert_eq!(
            max_purge_seq_id(&sqlite_db, &metadata_segment_id, &vector_segment_id)
                .await
                .expect("purge watermark"),
            17
        );
    }

    #[tokio::test]
    async fn max_purge_seq_id_uses_metadata_watermark_for_invalid_hnsw_config() {
        let sqlite_db = setup_sqlite_db().await;
        let collection_id = CollectionUuid::new();
        let metadata_segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::Sqlite,
            scope: SegmentScope::METADATA,
            collection: collection_id,
            metadata: None,
            file_path: HashMap::new(),
        };
        let vector_segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::HnswLocalPersisted,
            scope: SegmentScope::VECTOR,
            collection: collection_id,
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut collection = Collection {
            collection_id,
            ..Collection::default()
        };
        if let VectorIndexConfiguration::Hnsw(hnsw) = &mut collection.config.vector_index {
            hnsw.max_neighbors = 129;
        }
        collection.schema = Some(
            Schema::try_from(&collection.config)
                .expect("invalid HNSW bounds should still serialize to schema"),
        );

        set_segment_seq_id(&sqlite_db, metadata_segment.id, 40).await;
        set_segment_seq_id(&sqlite_db, vector_segment.id, 17).await;

        assert_eq!(
            max_purge_seq_id_for_collection(
                &sqlite_db,
                &collection,
                &metadata_segment,
                &vector_segment
            )
            .await
            .expect("purge watermark"),
            40
        );
    }
}
