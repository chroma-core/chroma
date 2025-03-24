use crate::grpc_log::GrpcLog;
use crate::in_memory_log::InMemoryLog;
use crate::sqlite_log::SqliteLog;
use crate::types::CollectionInfo;
use chroma_error::ChromaError;
use chroma_types::{CollectionUuid, LogRecord, OperationRecord, ResetError, ResetResponse};
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub struct CollectionRecord {
    pub collection_id: CollectionUuid,
    pub tenant_id: String,
    pub last_compaction_time: i64,
    #[allow(dead_code)]
    pub first_record_time: i64,
    pub offset: i64,
    pub collection_version: i32,
}

#[derive(Clone, Debug)]
pub enum Log {
    Sqlite(SqliteLog),
    Grpc(GrpcLog),
    #[allow(dead_code)]
    InMemory(InMemoryLog),
}

impl Log {
    pub async fn read(
        &mut self,
        collection_id: CollectionUuid,
        offset: i64,
        batch_size: i32,
        end_timestamp: Option<i64>,
    ) -> Result<Vec<LogRecord>, Box<dyn ChromaError>> {
        match self {
            Log::Sqlite(log) => log
                .read(collection_id, offset, batch_size, end_timestamp)
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
            Log::Grpc(log) => log
                .read(collection_id, offset, batch_size, end_timestamp)
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
            Log::InMemory(log) => Ok(log
                .read(collection_id, offset, batch_size, end_timestamp)
                .await),
        }
    }

    pub async fn push_logs(
        &mut self,
        collection_id: CollectionUuid,
        records: Vec<OperationRecord>,
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            Log::Sqlite(log) => log
                .push_logs(collection_id, records)
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
            Log::Grpc(log) => log
                .push_logs(collection_id, records)
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
            Log::InMemory(_) => unimplemented!(),
        }
    }

    pub async fn get_collections_with_new_data(
        &mut self,
        min_compaction_size: u64,
    ) -> Result<Vec<CollectionInfo>, Box<dyn ChromaError>> {
        match self {
            Log::Sqlite(log) => log
                .get_collections_with_new_data(min_compaction_size)
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
            Log::Grpc(log) => log
                .get_collections_with_new_data(min_compaction_size)
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
            Log::InMemory(log) => Ok(log.get_collections_with_new_data(min_compaction_size).await),
        }
    }

    pub async fn update_collection_log_offset(
        &mut self,
        collection_id: CollectionUuid,
        new_offset: i64,
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            Log::Sqlite(log) => log
                .update_collection_log_offset(collection_id, new_offset)
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
            Log::Grpc(log) => log
                .update_collection_log_offset(collection_id, new_offset)
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
            Log::InMemory(log) => {
                log.update_collection_log_offset(collection_id, new_offset)
                    .await;
                Ok(())
            }
        }
    }

    // Only supported in sqlite. Distributed has a different workflow.
    pub async fn purge_logs(
        &mut self,
        collection_id: CollectionUuid,
        seq_id: u64,
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            Log::Sqlite(log) => log
                .purge_logs(collection_id, seq_id)
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
            Log::Grpc(_) => unimplemented!(),
            Log::InMemory(_) => unimplemented!(),
        }
    }

    pub async fn get_max_batch_size(&mut self) -> Result<u32, Box<dyn ChromaError>> {
        match self {
            Log::Sqlite(log) => log
                .get_max_batch_size()
                .await
                .map_err(|err| Box::new(err) as Box<dyn ChromaError>),
            Log::Grpc(_) => Ok(100),
            Log::InMemory(_) => todo!(),
        }
    }

    pub async fn reset(&mut self) -> Result<ResetResponse, ResetError> {
        match self {
            Log::Sqlite(log) => log.reset().await,
            Log::Grpc(_) => Ok(ResetResponse {}),
            Log::InMemory(_) => Ok(ResetResponse {}),
        }
    }
}
