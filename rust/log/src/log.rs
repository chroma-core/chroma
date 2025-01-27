use crate::grpc_log::GrpcLog;
use crate::in_memory_log::InMemoryLog;
use crate::types::{
    CollectionInfo, GetCollectionsWithNewDataError, PullLogsError, UpdateCollectionLogOffsetError,
};
use chroma_types::{CollectionUuid, LogRecord};
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
    ) -> Result<Vec<LogRecord>, PullLogsError> {
        match self {
            Log::Grpc(log) => {
                log.read(collection_id, offset, batch_size, end_timestamp)
                    .await
            }
            Log::InMemory(log) => {
                log.read(collection_id, offset, batch_size, end_timestamp)
                    .await
            }
        }
    }

    pub async fn get_collections_with_new_data(
        &mut self,
        min_compaction_size: u64,
    ) -> Result<Vec<CollectionInfo>, GetCollectionsWithNewDataError> {
        match self {
            Log::Grpc(log) => log.get_collections_with_new_data(min_compaction_size).await,
            Log::InMemory(log) => log.get_collections_with_new_data(min_compaction_size).await,
        }
    }

    pub async fn update_collection_log_offset(
        &mut self,
        collection_id: CollectionUuid,
        new_offset: i64,
    ) -> Result<(), UpdateCollectionLogOffsetError> {
        match self {
            Log::Grpc(log) => {
                log.update_collection_log_offset(collection_id, new_offset)
                    .await
            }
            Log::InMemory(log) => {
                log.update_collection_log_offset(collection_id, new_offset)
                    .await
            }
        }
    }
}
