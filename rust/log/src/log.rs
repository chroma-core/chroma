use crate::grpc_log::{GrpcLog, GrpcSealLogError};
use crate::in_memory_log::InMemoryLog;
use crate::sqlite_log::SqliteLog;
use crate::types::CollectionInfo;
use chroma_error::ChromaError;
use chroma_memberlist::client_manager::ClientAssignmentError;
use chroma_types::{
    CollectionUuid, ForkCollectionError, ForkLogsResponse, LogRecord, OperationRecord, ResetError,
    ResetResponse,
};
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
    pub collection_logical_size_bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum GarbageCollectError {
    #[error("garbage collect error: {0}")]
    Status(#[from] tonic::Status),
    #[error(transparent)]
    ClientAssignerError(#[from] ClientAssignmentError),
    #[error("could not connect: {0}")]
    Resolution(String),
    #[error("log is not enabled/configured")]
    NotEnabled,
    #[error("log implementation not supported")]
    Unimplemented,
}

#[derive(Clone, Debug)]
pub enum Log {
    Sqlite(SqliteLog),
    Grpc(GrpcLog),
    #[allow(dead_code)]
    InMemory(InMemoryLog),
}

impl Log {
    #[tracing::instrument(skip(self))]
    pub async fn read(
        &mut self,
        tenant: &str,
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

    // ScoutLogs returns the offset of the next record to be inserted into the log.
    #[tracing::instrument(skip(self), err(Display))]
    pub async fn scout_logs(
        &mut self,
        tenant: &str,
        collection_id: CollectionUuid,
        starting_offset: u64,
    ) -> Result<u64, Box<dyn ChromaError>> {
        match self {
            Log::Sqlite(log) => log
                .scout_logs(collection_id, starting_offset as i64)
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
            Log::Grpc(log) => log
                .scout_logs(collection_id, starting_offset)
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
            Log::InMemory(log) => log
                .scout_logs(collection_id, starting_offset)
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
        }
    }

    #[tracing::instrument(skip(self, records), err(Display))]
    pub async fn push_logs(
        &mut self,
        tenant: &str,
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

    #[tracing::instrument(skip(self), err(Display))]
    pub async fn fork_logs(
        &mut self,
        tenant: &str,
        source_collection_id: CollectionUuid,
        target_collection_id: CollectionUuid,
    ) -> Result<ForkLogsResponse, ForkCollectionError> {
        match self {
            Log::Sqlite(_) => Err(ForkCollectionError::Local),
            Log::Grpc(log) => log
                .fork_logs(source_collection_id, target_collection_id)
                .await
                .map_err(|err| err.boxed().into()),
            Log::InMemory(_) => Err(ForkCollectionError::Local),
        }
    }

    #[tracing::instrument(skip(self), err(Display))]
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

    #[tracing::instrument(skip(self), err(Display))]
    pub async fn update_collection_log_offset(
        &mut self,
        tenant: &str,
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

    /// Only supported in distributed.
    #[tracing::instrument(skip(self), err(Display))]
    pub async fn update_collection_log_offset_on_every_node(
        &mut self,
        collection_id: CollectionUuid,
        new_offset: i64,
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            Log::Sqlite(_) => Ok(()),
            Log::Grpc(log) => log
                .update_collection_log_offset_on_every_node(collection_id, new_offset)
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
            Log::InMemory(_) => Ok(()),
        }
    }

    /// Only supported in distributed. Sqlite has a different workflow.
    #[tracing::instrument(skip(self), err(Display))]
    pub async fn purge_dirty_for_collection(
        &mut self,
        collection_ids: Vec<CollectionUuid>,
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            Log::Sqlite(_) => unimplemented!("not implemented for sqlite"),
            Log::Grpc(log) => Ok(log
                .purge_dirty_for_collection(collection_ids)
                .await
                .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?),
            Log::InMemory(_) => unimplemented!("not implemented for in memory"),
        }
    }

    /// Only supported in sqlite. Distributed has a different workflow.
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
            // NOTE(hammadb): This is set to a high value and may cause issues
            // the quota system should be used to limit the number of records
            // upstream.
            Log::Grpc(_) => Ok(1000),
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

    pub async fn seal_log(&mut self, _: &str, _: CollectionUuid) -> Result<(), GrpcSealLogError> {
        match self {
            Log::Grpc(_) => Err(GrpcSealLogError::NoMoreSeal),
            Log::Sqlite(_) => unimplemented!(),
            Log::InMemory(_) => unimplemented!(),
        }
    }

    pub fn is_ready(&self) -> bool {
        match self {
            Log::Sqlite(_) => true,
            Log::Grpc(log) => log.is_ready(),
            Log::InMemory(_) => true,
        }
    }

    pub async fn garbage_collect_phase2(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<(), GarbageCollectError> {
        match self {
            Log::Grpc(log) => log.garbage_collect_phase2(collection_id).await,
            Log::Sqlite(_) => Err(GarbageCollectError::Unimplemented),
            Log::InMemory(_) => Ok(()),
        }
    }

    pub async fn garbage_collect_phase2_for_dirty_log(
        &mut self,
        ordinal: u64,
    ) -> Result<(), GarbageCollectError> {
        match self {
            Log::Grpc(log) => log.garbage_collect_phase2_for_dirty_log(ordinal).await,
            Log::Sqlite(_) => Err(GarbageCollectError::Unimplemented),
            Log::InMemory(_) => Ok(()),
        }
    }
}
