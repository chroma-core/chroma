use crate::{
    config::SqliteLogConfig, BackfillMessage, CollectionInfo, CompactionManagerError,
    LocalCompactionManager, PurgeLogsMessage,
};
use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes, WrappedSqlxError};
use chroma_sqlite::{db::SqliteDb, helpers::get_embeddings_queue_topic_name};
use chroma_system::{ChannelError, ComponentHandle, RequestError};
use chroma_types::{
    CollectionUuid, LogRecord, Operation, OperationRecord, ResetError, ResetResponse,
    ScalarEncoding, ScalarEncodingConversionError, UpdateMetadata, UpdateMetadataValue,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sqlx::{QueryBuilder, Row};
use std::{str::FromStr, sync::OnceLock};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SqlitePullLogsError {
    #[error("Query error: {0}")]
    QueryError(#[from] WrappedSqlxError),
    #[error("Failed to parse embedding encoding")]
    InvalidEncoding(#[from] ScalarEncodingConversionError),
    #[error("Failed to parse embedding: {0}")]
    InvalidEmbedding(bytemuck::PodCastError),
    #[error("Failed to parse metadata: {0}")]
    InvalidMetadata(#[from] serde_json::Error),
    #[error("Method {0} is not implemented")]
    NotImplemented(String),
}

impl ChromaError for SqlitePullLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqlitePullLogsError::QueryError(err) => err.code(),
            SqlitePullLogsError::InvalidEncoding(_) => ErrorCodes::InvalidArgument,
            SqlitePullLogsError::InvalidEmbedding(_) => ErrorCodes::InvalidArgument,
            SqlitePullLogsError::InvalidMetadata(_) => ErrorCodes::InvalidArgument,
            SqlitePullLogsError::NotImplemented(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum SqlitePushLogsError {
    #[error("Error in compaction: {0}")]
    CompactionError(#[from] CompactionManagerError),
    #[error("Error setting compactor handle")]
    CompactorHandleSetError,
    #[error("Error setting max batch size")]
    MaxBatchSizeSetError,
    #[error("Failed to serialize metadata: {0}")]
    InvalidMetadata(#[from] serde_json::Error),
    #[error("Error sending message to compactor")]
    MessageSendingError(#[from] RequestError),
    #[error("Error sending purge log msg to compactor")]
    PurgeLogSendingFailure(#[from] ChannelError),
    #[error("Query error: {0}")]
    QueryError(#[from] WrappedSqlxError),
    #[error("Error getting max batch size: {0}")]
    GetMaxBatchSizeError(#[from] SqliteGetMaxBatchSizeError),
    #[error("Unimplemented: {0}")]
    Unimplemented(String),
}

impl ChromaError for SqlitePushLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqlitePushLogsError::CompactionError(e) => e.code(),
            SqlitePushLogsError::CompactorHandleSetError => ErrorCodes::FailedPrecondition,
            SqlitePushLogsError::MaxBatchSizeSetError => ErrorCodes::FailedPrecondition,
            SqlitePushLogsError::InvalidMetadata(_) => ErrorCodes::Internal,
            SqlitePushLogsError::MessageSendingError(e) => e.code(),
            SqlitePushLogsError::QueryError(err) => err.code(),
            SqlitePushLogsError::PurgeLogSendingFailure(e) => e.code(),
            SqlitePushLogsError::GetMaxBatchSizeError(e) => e.code(),
            SqlitePushLogsError::Unimplemented(_) => ErrorCodes::Unimplemented,
        }
    }
}

const DEFAULT_VAR_OPT: u32 = 32766;
const PRAGMA_MAX_VAR_OPT: &str = "MAX_VARIABLE_NUMBER";
const VARIABLE_PER_RECORD: u32 = 6;
const DEFAULT_MAX_BATCH_SIZE: u32 = 999;

#[derive(Error, Debug)]
pub enum SqliteGetCollectionsWithNewDataError {
    #[error("Query error: {0}")]
    QueryError(#[from] WrappedSqlxError),
    #[error("Invalid collection ID: {0}")]
    InvalidCollectionId(#[from] uuid::Error),
}

impl ChromaError for SqliteGetCollectionsWithNewDataError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqliteGetCollectionsWithNewDataError::QueryError(err) => err.code(),
            SqliteGetCollectionsWithNewDataError::InvalidCollectionId(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum SqliteUpdateCollectionLogOffsetError {
    #[error("Query error: {0}")]
    QueryError(#[from] WrappedSqlxError),
}

impl ChromaError for SqliteUpdateCollectionLogOffsetError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqliteUpdateCollectionLogOffsetError::QueryError(err) => err.code(),
        }
    }
}

#[derive(Error, Debug)]
pub enum SqliteGetLegacyEmbeddingsQueueConfigError {
    #[error("Query error: {0}")]
    QueryError(#[from] WrappedSqlxError),
    #[error("Failed to parse legacy config: {0}")]
    InvalidConfig(#[from] serde_json::Error),
}

impl ChromaError for SqliteGetLegacyEmbeddingsQueueConfigError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqliteGetLegacyEmbeddingsQueueConfigError::QueryError(err) => err.code(),
            SqliteGetLegacyEmbeddingsQueueConfigError::InvalidConfig(_) => ErrorCodes::Internal,
        }
    }
}

pub fn legacy_embeddings_queue_config_default_kind() -> String {
    "EmbeddingsQueueConfigurationInternal".to_owned()
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LegacyEmbeddingsQueueConfig {
    pub automatically_purge: bool,
    #[serde(
        default = "legacy_embeddings_queue_config_default_kind",
        rename = "_type"
    )]
    pub kind: String,
}

#[derive(Clone, Debug)]
pub struct SqliteLog {
    db: SqliteDb,
    tenant_id: String,
    topic_namespace: String,
    compactor_handle: OnceLock<ComponentHandle<LocalCompactionManager>>,
    max_batch_size: OnceLock<u32>,
}

impl SqliteLog {
    pub fn new(db: SqliteDb, tenant_id: String, topic_namespace: String) -> Self {
        Self {
            db,
            tenant_id,
            topic_namespace,
            compactor_handle: OnceLock::new(),
            max_batch_size: OnceLock::new(),
        }
    }

    pub fn init_compactor_handle(
        &self,
        compactor_handle: ComponentHandle<LocalCompactionManager>,
    ) -> Result<(), SqlitePushLogsError> {
        self.compactor_handle
            .set(compactor_handle)
            .map_err(|_| SqlitePushLogsError::CompactorHandleSetError)
    }

    pub fn init_max_batch_size(&self, max_batch_size: u32) -> Result<(), SqlitePushLogsError> {
        self.max_batch_size
            .set(max_batch_size)
            .map_err(|_| SqlitePushLogsError::MaxBatchSizeSetError)
    }

    pub(super) async fn scout_logs(
        &mut self,
        _collection_id: CollectionUuid,
        _starting_offset: i64,
    ) -> Result<u64, SqlitePullLogsError> {
        Err(SqlitePullLogsError::NotImplemented(
            "scout_logs".to_string(),
        ))
    }

    pub(super) async fn read(
        &mut self,
        collection_id: CollectionUuid,
        offset: i64,
        batch_size: i32,
        end_timestamp_ns: Option<i64>,
    ) -> Result<Vec<LogRecord>, SqlitePullLogsError> {
        let topic =
            get_embeddings_queue_topic_name(&self.tenant_id, &self.topic_namespace, collection_id);

        let end_timestamp_ns = end_timestamp_ns.unwrap_or(i64::MAX);

        let mut logs;
        if batch_size < 0 {
            logs = sqlx::query(
                r#"
            SELECT
              seq_id,
              id,
              operation,
              vector,
              encoding,
              metadata
            FROM embeddings_queue
            WHERE topic = ?
            AND seq_id > ?
            AND CAST(strftime('%s', created_at) AS INTEGER) <= (? / 1000000000)
            ORDER BY seq_id ASC
            "#,
            )
            .bind(topic)
            .bind(offset)
            .bind(end_timestamp_ns)
            .fetch(self.db.get_conn());
        } else {
            logs = sqlx::query(
                r#"
            SELECT
              seq_id,
              id,
              operation,
              vector,
              encoding,
              metadata
            FROM embeddings_queue
            WHERE topic = ?
            AND seq_id > ?
            AND CAST(strftime('%s', created_at) AS INTEGER) <= (? / 1000000000)
            ORDER BY seq_id ASC
            LIMIT ?
            "#,
            )
            .bind(topic)
            .bind(offset)
            .bind(end_timestamp_ns)
            .bind(batch_size)
            .fetch(self.db.get_conn());
        }

        let mut records = Vec::new();
        while let Some(row) = logs.try_next().await.map_err(WrappedSqlxError)? {
            let log_offset: i64 = row.get("seq_id");
            let id: String = row.get("id");
            let embedding_bytes = row.get::<Option<&[u8]>, _>("vector");
            let encoding = row
                .get::<Option<&str>, _>("encoding")
                .map(ScalarEncoding::try_from)
                .transpose()?;
            let metadata_str = row.get::<Option<&str>, _>("metadata");

            // Parse embedding
            let embedding = embedding_bytes
                .map(
                    |embedding_bytes| -> Result<Option<_>, SqlitePullLogsError> {
                        match encoding {
                            Some(ScalarEncoding::FLOAT32) => {
                                let slice: &[f32] = bytemuck::try_cast_slice(embedding_bytes)
                                    .map_err(SqlitePullLogsError::InvalidEmbedding)?;
                                Ok(Some(slice.to_vec()))
                            }
                            Some(ScalarEncoding::INT32) => {
                                unimplemented!()
                            }
                            None => Ok(None),
                        }
                    },
                )
                .transpose()?
                .flatten();

            // Parse metadata
            let parsed_metadata_and_document: Option<(UpdateMetadata, Option<String>)> =
                metadata_str
                    .map(|metadata_str| {
                        let mut parsed: UpdateMetadata = serde_json::from_str(metadata_str)?;

                        let document = match parsed.remove("chroma:document") {
                            Some(UpdateMetadataValue::Str(document)) => Some(document),
                            None => None,
                            _ => panic!("Document not found in metadata"),
                        };

                        Ok::<_, SqlitePullLogsError>((parsed, document))
                    })
                    .transpose()?;
            let document = parsed_metadata_and_document
                .as_ref()
                .and_then(|(_, document)| document.clone());
            let metadata = parsed_metadata_and_document.map(|(metadata, _)| metadata);

            let operation = operation_from_code(row.get("operation"));

            records.push(LogRecord {
                log_offset,
                record: OperationRecord {
                    id,
                    embedding,
                    encoding,
                    metadata,
                    document,
                    operation,
                },
            });
        }

        Ok(records)
    }

    pub(super) async fn push_logs(
        &mut self,
        collection_id: CollectionUuid,
        records: Vec<OperationRecord>,
    ) -> Result<(), SqlitePushLogsError> {
        if records.is_empty() {
            return Ok(());
        }

        let topic =
            get_embeddings_queue_topic_name(&self.tenant_id, &self.topic_namespace, collection_id);

        let records_and_serialized_metadatas = records
            .into_iter()
            .map(|mut record| {
                let mut empty_metadata = UpdateMetadata::new();

                let metadata = record.metadata.as_mut().unwrap_or(&mut empty_metadata);
                for (key, value) in metadata.iter() {
                    if matches!(value, UpdateMetadataValue::SparseVector(_)) {
                        return Err(SqlitePushLogsError::Unimplemented(format!(
                            "Sparse vector is not supported for local chroma: {key}"
                        )));
                    }
                }
                if let Some(ref document) = record.document {
                    metadata.insert(
                        "chroma:document".to_string(),
                        UpdateMetadataValue::Str(document.clone()),
                    );
                }

                let serialized = serde_json::to_string(&metadata)?;
                Ok::<_, SqlitePushLogsError>((record, serialized))
            })
            .collect::<Result<Vec<(OperationRecord, String)>, SqlitePushLogsError>>()?;

        let max_batch_size = self
            .max_batch_size
            .get()
            .copied()
            .unwrap_or(DEFAULT_MAX_BATCH_SIZE) as usize;
        let mut tx = self.db.get_conn().begin().await.map_err(WrappedSqlxError)?;
        for batch in records_and_serialized_metadatas.chunks(max_batch_size) {
            let mut query_builder = QueryBuilder::new(
                "INSERT INTO embeddings_queue (topic, id, operation, vector, encoding, metadata) ",
            );
            query_builder.push_values(batch, |mut builder, (record, serialized_metadata)| {
                builder.push_bind(&topic);
                builder.push_bind(&record.id);
                builder.push_bind(operation_to_code(record.operation));
                builder.push_bind::<Option<Vec<u8>>>(
                    record
                        .embedding
                        .as_ref()
                        .map(|e| bytemuck::cast_slice(e.as_slice()).to_vec()),
                );
                builder.push_bind(record.encoding.as_ref().map(String::from));
                builder.push_bind(serialized_metadata);
            });
            let query = query_builder.build();
            query.execute(&mut *tx).await.map_err(WrappedSqlxError)?;
        }
        tx.commit().await.map_err(WrappedSqlxError)?;

        if let Some(handle) = self.compactor_handle.get() {
            let backfill_message = BackfillMessage { collection_id };
            handle.request(backfill_message, None).await??;
            let purge_log_msg = PurgeLogsMessage { collection_id };
            handle.clone().request(purge_log_msg, None).await??;
        }

        Ok(())
    }

    pub(super) async fn get_collections_with_new_data(
        &mut self,
        min_compaction_size: u64,
    ) -> Result<Vec<CollectionInfo>, SqliteGetCollectionsWithNewDataError> {
        let mut results = sqlx::query(
            r#"
            SELECT
                collections.id AS collection_id,
                MIN(COALESCE(CAST(max_seq_id.seq_id AS INTEGER), 0)) AS first_log_offset,
                CAST(strftime('%s', MIN(created_at)) AS INTEGER) * 1000000000 AS first_log_ts
            FROM collections
            INNER JOIN segments           ON segments.collection    = collections.id
            INNER JOIN embeddings_queue   ON embeddings_queue.topic = CONCAT('persistent://', ?, '/', ?, '/', collections.id)
            LEFT JOIN max_seq_id          ON max_seq_id.segment_id  = segments.id
            WHERE embeddings_queue.seq_id > COALESCE(CAST(max_seq_id.seq_id AS INTEGER), 0)
            GROUP BY
                collections.id
            HAVING
                COUNT(*) > ?
            ORDER BY first_log_ts ASC
            "#,
        )
        .bind(&self.tenant_id)
        .bind(&self.topic_namespace)
        .bind(min_compaction_size as i64) // (SQLite doesn't support u64)
        .fetch(self.db.get_conn());

        let mut infos = Vec::new();
        while let Some(row) = results.try_next().await.map_err(WrappedSqlxError)? {
            infos.push(CollectionInfo {
                collection_id: CollectionUuid::from_str(row.get::<&str, _>("collection_id"))?,
                first_log_offset: row.get("first_log_offset"),
                first_log_ts: row.get("first_log_ts"),
            });
        }

        Ok(infos)
    }

    pub async fn update_collection_log_offset(
        &mut self,
        collection_id: CollectionUuid,
        new_offset: i64,
    ) -> Result<(), SqliteUpdateCollectionLogOffsetError> {
        sqlx::query(
            r#"
            INSERT OR REPLACE INTO max_seq_id (seq_id, segment_id)
            SELECT ?, id
            FROM segments
            WHERE
                collection = ?
        "#,
        )
        .bind(new_offset)
        .bind(collection_id.0.to_string())
        .execute(self.db.get_conn())
        .await
        .map_err(WrappedSqlxError)?;

        Ok(())
    }

    pub async fn purge_logs(
        &mut self,
        collection_id: CollectionUuid,
        seq_id: u64,
    ) -> Result<(), SqlitePurgeLogsError> {
        let legacy_config = self.get_legacy_embeddings_queue_config().await?;
        // Skip purge if not enabled
        if !legacy_config.automatically_purge {
            return Ok(());
        }

        let topic =
            get_embeddings_queue_topic_name(&self.tenant_id, &self.topic_namespace, collection_id);

        sqlx::query("DELETE FROM embeddings_queue WHERE topic = ? AND seq_id < ?")
            .bind(topic)
            .bind(seq_id as i64)
            .execute(self.db.get_conn())
            .await
            .map_err(WrappedSqlxError)?;

        Ok(())
    }

    pub async fn get_max_batch_size(&self) -> Result<u32, SqliteGetMaxBatchSizeError> {
        let opt_strs = sqlx::query("PRAGMA compile_options")
            .fetch_all(self.db.get_conn())
            .await
            .map_err(WrappedSqlxError)?
            .into_iter()
            .map(|row| row.try_get::<String, _>(0))
            .collect::<Result<Vec<_>, _>>()?;
        let max_variable_number = opt_strs
            .into_iter()
            .filter_map(|opt_str| {
                let mut opt_val = opt_str.split("=");
                if let Some(PRAGMA_MAX_VAR_OPT) = opt_val.next() {
                    opt_val.next().and_then(|val_str| val_str.parse().ok())
                } else {
                    None
                }
            })
            .fold(DEFAULT_VAR_OPT, |_, opt| opt);
        Ok(max_variable_number / VARIABLE_PER_RECORD)
    }

    pub async fn reset(&mut self) -> Result<ResetResponse, ResetError> {
        // Populate default config
        self.get_legacy_embeddings_queue_config()
            .await
            .map_err(|e| e.boxed())?;
        Ok(ResetResponse {})
    }

    async fn get_legacy_embeddings_queue_config(
        &mut self,
    ) -> Result<LegacyEmbeddingsQueueConfig, SqliteGetLegacyEmbeddingsQueueConfigError> {
        let mut tx = self.db.get_conn().begin().await.map_err(WrappedSqlxError)?;

        let row = sqlx::query("SELECT * FROM embeddings_queue_config")
            .fetch_optional(&mut *tx)
            .await
            .map_err(WrappedSqlxError)?;

        if let Some(row) = row {
            let value: String = row.get("config_json_str");
            let config: LegacyEmbeddingsQueueConfig = serde_json::from_str(&value)?;
            return Ok(config);
        }

        // Insert default
        let log_size = sqlx::query("SELECT COUNT(*) from embeddings_queue")
            .fetch_one(&mut *tx)
            .await
            .map_err(WrappedSqlxError)?;
        let log_size = log_size.get::<i64, _>(0);

        let default_config = LegacyEmbeddingsQueueConfig {
            automatically_purge: log_size == 0,
            kind: legacy_embeddings_queue_config_default_kind(),
        };
        let value = serde_json::to_string(&default_config)?;
        sqlx::query("INSERT INTO embeddings_queue_config (config_json_str) VALUES (?)")
            .bind(value)
            .execute(&mut *tx)
            .await
            .map_err(WrappedSqlxError)?;
        tx.commit().await.map_err(WrappedSqlxError)?;

        Ok(default_config)
    }

    pub async fn update_legacy_embeddings_queue_config(
        &self,
        config: LegacyEmbeddingsQueueConfig,
    ) -> Result<LegacyEmbeddingsQueueConfig, SqliteGetLegacyEmbeddingsQueueConfigError> {
        let mut tx = self.db.get_conn().begin().await.map_err(WrappedSqlxError)?;
        let value = serde_json::to_string(&config)?;
        sqlx::query(
            "INSERT OR REPLACE INTO embeddings_queue_config (id, config_json_str) VALUES (1, ?)",
        )
        .bind(value)
        .execute(&mut *tx)
        .await
        .map_err(WrappedSqlxError)?;
        tx.commit().await.map_err(WrappedSqlxError)?;
        Ok(config)
    }
}

#[derive(Error, Debug)]
pub enum SqlitePurgeLogsError {
    #[error("Could not get legacy embedding queue config: {0}")]
    GetLegacyConfigError(#[from] SqliteGetLegacyEmbeddingsQueueConfigError),
    #[error("Delete query error: {0}")]
    DeleteQueryError(#[from] WrappedSqlxError),
}

impl ChromaError for SqlitePurgeLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqlitePurgeLogsError::GetLegacyConfigError(err) => err.code(),
            SqlitePurgeLogsError::DeleteQueryError(err) => err.code(),
        }
    }
}

#[derive(Error, Debug)]
pub enum SqliteGetMaxBatchSizeError {
    #[error("Error getting compile time options from sqlite: {0}")]
    PragmaQueryError(#[from] WrappedSqlxError),
    #[error("Error parsing row from sqlx: {0}")]
    RowParsingError(#[from] sqlx::Error),
}

impl ChromaError for SqliteGetMaxBatchSizeError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqliteGetMaxBatchSizeError::PragmaQueryError(err) => err.code(),
            SqliteGetMaxBatchSizeError::RowParsingError(_) => ErrorCodes::Internal,
        }
    }
}

fn operation_from_code(code: u32) -> Operation {
    // chromadb/db/mixins/embeddings_queue.py
    match code {
        0 => Operation::Add,
        1 => Operation::Update,
        2 => Operation::Upsert,
        3 => Operation::Delete,
        _ => panic!("Invalid operation code"),
    }
}

fn operation_to_code(operation: Operation) -> u32 {
    match operation {
        Operation::Add => 0,
        Operation::Update => 1,
        Operation::Upsert => 2,
        Operation::Delete => 3,
    }
}

#[async_trait]
impl Configurable<SqliteLogConfig> for SqliteLog {
    async fn try_from_config(
        config: &SqliteLogConfig,
        registry: &chroma_config::registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let sqlite_db = registry.get::<SqliteDb>().map_err(|e| e.boxed())?;
        let mut log = Self::new(
            sqlite_db,
            config.tenant_id.clone(),
            config.topic_namespace.clone(),
        );

        // This populates the legacy config if not present (when upgrading from an old version)
        log.get_legacy_embeddings_queue_config()
            .await
            .map_err(|e| e.boxed())?;

        Ok(log)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_config::{registry::Registry, Configurable};
    use chroma_sqlite::{config::SqliteDBConfig, db::test_utils::new_test_db_persist_path};
    use chroma_types::{are_update_metadatas_close_to_equal, CollectionUuid};
    use proptest::prelude::*;

    use tokio::runtime::Runtime;

    async fn setup_sqlite_log() -> SqliteLog {
        let db = SqliteDb::try_from_config(
            &SqliteDBConfig {
                url: new_test_db_persist_path(),
                migration_mode: chroma_sqlite::config::MigrationMode::Apply,
                hash_type: chroma_sqlite::config::MigrationHash::SHA256,
            },
            &Registry::new(),
        )
        .await
        .unwrap();

        SqliteLog::new(db, "default".to_string(), "default".to_string())
    }

    #[tokio::test]
    async fn test_log_offset() {
        let mut log = setup_sqlite_log().await;

        let collection_id = CollectionUuid::new();

        // TODO: remove this when there's a sysdb implementation in Rust
        sqlx::query(
            r#"
            INSERT INTO segments (id, type, scope, collection) VALUES ('foo', 'foo', 'foo', ?);
            INSERT INTO collections (id, name, database_id) VALUES (?, 'foo', 0);
        "#,
        )
        .bind(collection_id.0.to_string())
        .bind(collection_id.0.to_string())
        .execute(log.db.get_conn())
        .await
        .unwrap();

        let collections_with_data = log.get_collections_with_new_data(0).await.unwrap();
        assert_eq!(collections_with_data.len(), 0);

        // Push a log
        let operations = vec![OperationRecord {
            id: "id".to_string(),
            embedding: Some(vec![1.0, 2.0, 3.0]),
            encoding: Some(ScalarEncoding::FLOAT32),
            metadata: None,
            document: None,
            operation: Operation::Add,
        }];
        log.push_logs(collection_id, operations).await.unwrap();

        let collections_with_data = log.get_collections_with_new_data(0).await.unwrap();
        assert_eq!(collections_with_data.len(), 1);

        let collections_with_data = log.get_collections_with_new_data(1).await.unwrap();
        assert_eq!(collections_with_data.len(), 0);

        // Update log offset
        log.update_collection_log_offset(collection_id, 0)
            .await
            .unwrap();
        let collections_with_data = log.get_collections_with_new_data(0).await.unwrap();
        assert_eq!(collections_with_data.len(), 1);

        log.update_collection_log_offset(collection_id, 1)
            .await
            .unwrap();
        let collections_with_data = log.get_collections_with_new_data(0).await.unwrap();
        assert_eq!(collections_with_data.len(), 0);
    }

    proptest! {
        #[test]
         fn test_push_pull_logs(
            read_offset in 0usize..=100,
            batch_size in 0usize..=100,
            operations in proptest::collection::vec(any::<OperationRecord>(), 0..100)
        ) {
            let runtime = Runtime::new().unwrap();

            runtime.block_on(async {
                let mut log = setup_sqlite_log().await;

                let collection_id = CollectionUuid::new();
                log.push_logs(collection_id, operations.clone()).await.unwrap();

                let read_logs = log.read(collection_id, read_offset as i64, batch_size as i32, None)
                    .await
                    .unwrap();

                let expected_length = batch_size.min(operations.len().saturating_sub(read_offset));

                assert_eq!(read_logs.len(), expected_length);

                for i in 0..expected_length {
                    let operation = &operations[i + read_offset];
                    let log = &read_logs[i];

                    let expected_metadata = operation.metadata.clone().unwrap_or_default();
                    let received_metadata = log.record.metadata.clone().unwrap();

                    assert!(log.record.id == operation.id);
                    assert!(log.record.embedding == operation.embedding);
                    assert!(log.record.encoding == operation.encoding);
                    assert!(
                        are_update_metadatas_close_to_equal(
                            &received_metadata,
                            &expected_metadata
                        ),
                        "{:?} != {:?}",
                        received_metadata,
                        expected_metadata
                    );
                    assert!(log.record.document == operation.document);
                    assert!(log.record.operation == operation.operation);
                }
            });
        }
    }
}
