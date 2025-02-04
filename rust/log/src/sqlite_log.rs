use crate::{CollectionInfo, WrappedSqlxError};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sqlite::db::SqliteDb;
use chroma_types::{
    CollectionConversionError, CollectionUuid, LogRecord, Operation, OperationRecord,
    ScalarEncoding, ScalarEncodingConversionError, UpdateMetadata, UpdateMetadataValue,
};
use futures::TryStreamExt;
use sqlx::{QueryBuilder, Row};
use std::str::FromStr;
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
}

impl ChromaError for SqlitePullLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqlitePullLogsError::QueryError(err) => err.code(),
            SqlitePullLogsError::InvalidEncoding(_) => ErrorCodes::InvalidArgument,
            SqlitePullLogsError::InvalidEmbedding(_) => ErrorCodes::InvalidArgument,
            SqlitePullLogsError::InvalidMetadata(_) => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Error, Debug)]
pub enum SqlitePushLogsError {
    #[error("Query error: {0}")]
    QueryError(#[from] WrappedSqlxError),
    #[error("Failed to serialize metadata: {0}")]
    InvalidMetadata(#[from] serde_json::Error),
}

impl ChromaError for SqlitePushLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            SqlitePushLogsError::QueryError(err) => err.code(),
            SqlitePushLogsError::InvalidMetadata(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum SqliteGetCollectionsWithNewDataError {
    #[error("Query error: {0}")]
    QueryError(#[from] WrappedSqlxError),
    #[error("Invalid collection ID: {0}")]
    InvalidCollectionId(#[from] CollectionConversionError),
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

#[derive(Clone, Debug)]
pub struct SqliteLog {
    db: SqliteDb,
    tenant_id: String,
    topic_namespace: String,
}

impl SqliteLog {
    pub(super) async fn read(
        &mut self,
        collection_id: CollectionUuid,
        offset: i64,
        batch_size: i32,
        end_timestamp_ns: Option<i64>,
    ) -> Result<Vec<LogRecord>, SqlitePullLogsError> {
        let topic = get_topic_name(&self.tenant_id, &self.topic_namespace, collection_id);

        let end_timestamp_ns = end_timestamp_ns.unwrap_or(i64::MAX);

        let mut logs = sqlx::query(
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

        let topic = get_topic_name(&self.tenant_id, &self.topic_namespace, collection_id);

        let records_and_serialized_metadatas = records
            .into_iter()
            .map(|mut record| {
                let mut empty_metadata = UpdateMetadata::new();

                let metadata = record.metadata.as_mut().unwrap_or(&mut empty_metadata);
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

        let mut query_builder = QueryBuilder::new(
            "INSERT INTO embeddings_queue (topic, id, operation, vector, encoding, metadata) ",
        );
        query_builder.push_values(
            records_and_serialized_metadatas,
            |mut builder, (record, serialized_metadata)| {
                builder.push_bind(&topic);
                builder.push_bind(record.id);
                builder.push_bind(operation_to_code(record.operation));
                builder.push_bind::<Option<Vec<u8>>>(
                    record
                        .embedding
                        .map(|e| bytemuck::cast_slice(e.as_slice()).to_vec()),
                );
                builder.push_bind(record.encoding.map(String::from));
                builder.push_bind::<String>(serialized_metadata);
            },
        );
        let query = query_builder.build();
        query
            .execute(self.db.get_conn())
            .await
            .map_err(WrappedSqlxError)?;

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
}

fn get_topic_name(tenant: &str, namespace: &str, collection_id: CollectionUuid) -> String {
    format!("persistent://{}/{}/{}", tenant, namespace, collection_id)
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

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_sqlite::config::SqliteDBConfig;
    use chroma_types::{are_metadatas_close_to_equal, CollectionUuid};
    use proptest::prelude::*;
    use tempfile::tempdir;
    use tokio::runtime::Runtime;

    async fn setup_sqlite_log() -> SqliteLog {
        let path = tempdir().unwrap().into_path().join("test.db");
        let db = SqliteDb::try_from_config(&SqliteDBConfig {
            url: path.to_str().unwrap().to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

        SqliteLog {
            db,
            tenant_id: "default".to_string(),
            topic_namespace: "default".to_string(),
        }
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
                        are_metadatas_close_to_equal(
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
