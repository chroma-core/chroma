use crate::chroma_proto;
use crate::chroma_proto::log_service_client::LogServiceClient;
use crate::config::Configurable;
use crate::errors::ChromaError;
use crate::errors::ErrorCodes;
use crate::log::config::LogConfig;
use crate::tracing::util::client_interceptor;
use crate::types::LogRecord;
use crate::types::RecordConversionError;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;
use thiserror::Error;
use tonic::service::interceptor;
use tonic::transport::Endpoint;
use tonic::{Request, Status};
use uuid::Uuid;

/// CollectionInfo is a struct that contains information about a collection for the
/// compacting process.
/// Fields:
/// - collection_id: the id of the collection that needs to be compacted
/// - first_log_offset: the offset of the first log entry in the collection that needs to be compacted
/// - first_log_ts: the timestamp of the first log entry in the collection that needs to be compacted
#[derive(Debug)]
pub(crate) struct CollectionInfo {
    pub(crate) collection_id: String,
    pub(crate) first_log_offset: i64,
    pub(crate) first_log_ts: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct CollectionRecord {
    pub(crate) id: Uuid,
    pub(crate) tenant_id: String,
    pub(crate) last_compaction_time: i64,
    pub(crate) first_record_time: i64,
    pub(crate) offset: i64,
    pub(crate) collection_version: i32,
}

#[derive(Clone, Debug)]
pub(crate) enum Log {
    Grpc(GrpcLog),
    InMemory(InMemoryLog),
}

impl Log {
    pub(crate) async fn read(
        &mut self,
        collection_id: Uuid,
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

    pub(crate) async fn get_collections_with_new_data(
        &mut self,
    ) -> Result<Vec<CollectionInfo>, GetCollectionsWithNewDataError> {
        match self {
            Log::Grpc(log) => log.get_collections_with_new_data().await,
            Log::InMemory(log) => log.get_collections_with_new_data().await,
        }
    }

    pub(crate) async fn update_collection_log_offset(
        &mut self,
        collection_id: Uuid,
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

#[derive(Clone, Debug)]
pub(crate) struct GrpcLog {
    client: LogServiceClient<
        interceptor::InterceptedService<
            tonic::transport::Channel,
            fn(Request<()>) -> Result<Request<()>, Status>,
        >,
    >,
}

impl GrpcLog {
    pub(crate) fn new(
        client: LogServiceClient<
            interceptor::InterceptedService<
                tonic::transport::Channel,
                fn(Request<()>) -> Result<Request<()>, Status>,
            >,
        >,
    ) -> Self {
        Self { client }
    }
}

#[derive(Error, Debug)]
pub(crate) enum GrpcLogError {
    #[error("Failed to connect to log service")]
    FailedToConnect(#[from] tonic::transport::Error),
}

impl ChromaError for GrpcLogError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcLogError::FailedToConnect(_) => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Configurable<LogConfig> for GrpcLog {
    async fn try_from_config(config: &LogConfig) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            LogConfig::Grpc(my_config) => {
                let host = &my_config.host;
                let port = &my_config.port;
                // TODO: switch to logging when logging is implemented
                println!("Connecting to log service at {}:{}", host, port);
                let connection_string = format!("http://{}:{}", host, port);
                let endpoint_res = match Endpoint::from_shared(connection_string) {
                    Ok(endpoint) => endpoint,
                    Err(e) => {
                        return Err(Box::new(GrpcLogError::FailedToConnect(
                            tonic::transport::Error::from(e),
                        )))
                    }
                };
                let endpoint_res = endpoint_res
                    .connect_timeout(Duration::from_millis(my_config.connect_timeout_ms))
                    .timeout(Duration::from_millis(my_config.request_timeout_ms));
                let client = endpoint_res.connect().await;
                match client {
                    Ok(client) => {
                        let channel: LogServiceClient<
                            interceptor::InterceptedService<
                                tonic::transport::Channel,
                                fn(Request<()>) -> Result<Request<()>, Status>,
                            >,
                        > = LogServiceClient::with_interceptor(client, client_interceptor);
                        return Ok(GrpcLog::new(channel));
                    }
                    Err(e) => {
                        return Err(Box::new(GrpcLogError::FailedToConnect(e)));
                    }
                }
            }
        }
    }
}

impl GrpcLog {
    async fn read(
        &mut self,
        collection_id: Uuid,
        offset: i64,
        batch_size: i32,
        end_timestamp: Option<i64>,
    ) -> Result<Vec<LogRecord>, PullLogsError> {
        let end_timestamp = match end_timestamp {
            Some(end_timestamp) => end_timestamp,
            None => i64::MAX,
        };
        let request = self.client.pull_logs(chroma_proto::PullLogsRequest {
            collection_id: collection_id.to_string(),
            start_from_offset: offset,
            batch_size,
            end_timestamp,
        });
        let response = request.await;
        match response {
            Ok(response) => {
                let logs = response.into_inner().records;
                let mut result = Vec::new();
                for log_record_proto in logs {
                    let log_record = log_record_proto.try_into();
                    match log_record {
                        Ok(log_record) => {
                            result.push(log_record);
                        }
                        Err(err) => {
                            return Err(PullLogsError::ConversionError(err));
                        }
                    }
                }
                Ok(result)
            }
            Err(e) => {
                // TODO: switch to logging when logging is implemented
                println!("Failed to pull logs: {}", e);
                Err(PullLogsError::FailedToPullLogs(e))
            }
        }
    }

    async fn get_collections_with_new_data(
        &mut self,
    ) -> Result<Vec<CollectionInfo>, GetCollectionsWithNewDataError> {
        let response = self
            .client
            .get_all_collection_info_to_compact(
                chroma_proto::GetAllCollectionInfoToCompactRequest {},
            )
            .await;

        match response {
            Ok(response) => {
                let collections = response.into_inner().all_collection_info;
                println!("Log got collections with new data: {:?}", collections);
                let mut result = Vec::new();
                for collection in collections {
                    result.push(CollectionInfo {
                        collection_id: collection.collection_id,
                        first_log_offset: collection.first_log_offset,
                        first_log_ts: collection.first_log_ts,
                    });
                }
                Ok(result)
            }
            Err(e) => {
                // TODO: switch to logging when logging is implemented
                println!("Failed to get collections: {}", e);
                Err(GetCollectionsWithNewDataError::FailedGetCollectionsWithNewData(e))
            }
        }
    }

    async fn update_collection_log_offset(
        &mut self,
        collection_id: Uuid,
        new_offset: i64,
    ) -> Result<(), UpdateCollectionLogOffsetError> {
        let request = self.client.update_collection_log_offset(
            chroma_proto::UpdateCollectionLogOffsetRequest {
                collection_id: collection_id.to_string(),
                log_offset: new_offset,
            },
        );
        let response = request.await;
        match response {
            Ok(_) => Ok(()),
            Err(e) => Err(UpdateCollectionLogOffsetError::FailedToUpdateCollectionLogOffset(e)),
        }
    }
}

#[derive(Error, Debug)]
pub(crate) enum PullLogsError {
    #[error("Failed to fetch")]
    FailedToPullLogs(#[from] tonic::Status),
    #[error("Failed to convert proto embedding record into EmbeddingRecord")]
    ConversionError(#[from] RecordConversionError),
}

impl ChromaError for PullLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            PullLogsError::FailedToPullLogs(_) => ErrorCodes::Internal,
            PullLogsError::ConversionError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub(crate) enum GetCollectionsWithNewDataError {
    #[error("Failed to fetch")]
    FailedGetCollectionsWithNewData(#[from] tonic::Status),
}

impl ChromaError for GetCollectionsWithNewDataError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionsWithNewDataError::FailedGetCollectionsWithNewData(_) => {
                ErrorCodes::Internal
            }
        }
    }
}

#[derive(Error, Debug)]
pub(crate) enum UpdateCollectionLogOffsetError {
    #[error("Failed to update collection log offset")]
    FailedToUpdateCollectionLogOffset(#[from] tonic::Status),
}

impl ChromaError for UpdateCollectionLogOffsetError {
    fn code(&self) -> ErrorCodes {
        match self {
            UpdateCollectionLogOffsetError::FailedToUpdateCollectionLogOffset(_) => {
                ErrorCodes::Internal
            }
        }
    }
}

// This is used for testing only, it represents a log record that is stored in memory
// internal to a mock log implementation
#[derive(Clone)]
pub(crate) struct InternalLogRecord {
    pub(crate) collection_id: Uuid,
    pub(crate) log_offset: i64,
    pub(crate) log_ts: i64,
    pub(crate) record: LogRecord,
}

impl Debug for InternalLogRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogRecord")
            .field("collection_id", &self.collection_id)
            .field("log_offset", &self.log_offset)
            .field("log_ts", &self.log_ts)
            .field("record", &self.record)
            .finish()
    }
}

// This is used for testing only
#[derive(Clone, Debug)]
pub(crate) struct InMemoryLog {
    collection_to_log: HashMap<String, Vec<Box<InternalLogRecord>>>,
    offsets: HashMap<String, i64>,
}

impl InMemoryLog {
    pub fn new() -> InMemoryLog {
        InMemoryLog {
            collection_to_log: HashMap::new(),
            offsets: HashMap::new(),
        }
    }

    pub fn add_log(&mut self, collection_id: Uuid, log: Box<InternalLogRecord>) {
        let logs = self
            .collection_to_log
            .entry(collection_id.to_string())
            .or_insert(Vec::new());
        // Ensure that the log offset is correct. Since we only use the InMemoryLog for testing,
        // we expect callers to send us logs in the correct order.
        let next_offset = logs.len() as i64;
        if log.log_offset != next_offset {
            panic!(
                "Expected log offset to be {}, but got {}",
                next_offset, log.log_offset
            );
        }
        logs.push(log);
    }
}

impl InMemoryLog {
    async fn read(
        &mut self,
        collection_id: Uuid,
        offset: i64,
        batch_size: i32,
        end_timestamp: Option<i64>,
    ) -> Result<Vec<LogRecord>, PullLogsError> {
        let end_timestamp = match end_timestamp {
            Some(end_timestamp) => end_timestamp,
            None => i64::MAX,
        };

        let logs = match self.collection_to_log.get(&collection_id.to_string()) {
            Some(logs) => logs,
            None => return Ok(Vec::new()),
        };
        let mut result = Vec::new();
        for i in offset..(offset + batch_size as i64) {
            if i < logs.len() as i64 && logs[i as usize].log_ts <= end_timestamp {
                result.push(logs[i as usize].record.clone());
            }
        }
        Ok(result)
    }

    async fn get_collections_with_new_data(
        &mut self,
    ) -> Result<Vec<CollectionInfo>, GetCollectionsWithNewDataError> {
        let mut collections = Vec::new();
        for (collection_id, log_records) in self.collection_to_log.iter() {
            if log_records.is_empty() {
                continue;
            }
            let filtered_records = match self.offsets.get(collection_id) {
                Some(last_offset) => {
                    // Make sure there is at least one record past the last offset
                    let max_offset = log_records.len() as i64 - 1;
                    if *last_offset + 1 > max_offset {
                        continue;
                    }
                    &log_records[(*last_offset + 1) as usize..]
                }
                None => &log_records[..],
            };
            let mut logs = filtered_records.to_vec();
            logs.sort_by(|a, b| a.log_offset.cmp(&b.log_offset));
            collections.push(CollectionInfo {
                collection_id: collection_id.clone(),
                first_log_offset: logs[0].log_offset,
                first_log_ts: logs[0].log_ts,
            });
        }
        Ok(collections)
    }

    async fn update_collection_log_offset(
        &mut self,
        collection_id: Uuid,
        new_offset: i64,
    ) -> Result<(), UpdateCollectionLogOffsetError> {
        self.offsets.insert(collection_id.to_string(), new_offset);
        Ok(())
    }
}
