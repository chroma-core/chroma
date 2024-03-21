use crate::chroma_proto;
use crate::chroma_proto::log_service_client::LogServiceClient;
use crate::config::Configurable;
use crate::config::WorkerConfig;
use crate::errors::ChromaError;
use crate::errors::ErrorCodes;
use crate::log::config::LogConfig;
use crate::types::EmbeddingRecord;
use crate::types::EmbeddingRecordConversionError;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use thiserror::Error;

// CollectionInfo is a struct that contains information about a collection for the
// compacting process. It contains information about the collection id, the first log id,
// and the first log id timestamp since last compaction.
pub(crate) struct CollectionInfo {
    pub(crate) collection_id: String,
    pub(crate) first_log_id: i64,
    pub(crate) first_log_id_ts: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct CollectionRecord {
    pub(crate) id: String,
    pub(crate) tenant_id: String,
    pub(crate) last_compaction_time: i64,
    pub(crate) first_record_time: i64,
    pub(crate) offset: i64,
}

#[async_trait]
pub(crate) trait Log: Send + Sync + LogClone + Debug {
    async fn read(
        &mut self,
        collection_id: String,
        offset: i64,
        batch_size: i32,
        end_timestamp: Option<i64>,
    ) -> Result<Vec<Box<EmbeddingRecord>>, PullLogsError>;

    async fn get_collections_with_new_data(
        &mut self,
    ) -> Result<Vec<CollectionInfo>, GetCollectionsWithNewDataError>;
}

pub(crate) trait LogClone {
    fn clone_box(&self) -> Box<dyn Log>;
}

impl<T> LogClone for T
where
    T: 'static + Log + Clone,
{
    fn clone_box(&self) -> Box<dyn Log> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Log> {
    fn clone(&self) -> Box<dyn Log> {
        self.clone_box()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct GrpcLog {
    client: LogServiceClient<tonic::transport::Channel>,
}

impl GrpcLog {
    pub(crate) fn new(client: LogServiceClient<tonic::transport::Channel>) -> Self {
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
impl Configurable for GrpcLog {
    async fn try_from_config(worker_config: &WorkerConfig) -> Result<Self, Box<dyn ChromaError>> {
        match &worker_config.log {
            LogConfig::Grpc(my_config) => {
                let host = &my_config.host;
                let port = &my_config.port;
                // TODO: switch to logging when logging is implemented
                println!("Connecting to log service at {}:{}", host, port);
                let connection_string = format!("http://{}:{}", host, port);
                let client = LogServiceClient::connect(connection_string).await;
                match client {
                    Ok(client) => {
                        return Ok(GrpcLog::new(client));
                    }
                    Err(e) => {
                        return Err(Box::new(GrpcLogError::FailedToConnect(e)));
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Log for GrpcLog {
    async fn read(
        &mut self,
        collection_id: String,
        offset: i64,
        batch_size: i32,
        end_timestamp: Option<i64>,
    ) -> Result<Vec<Box<EmbeddingRecord>>, PullLogsError> {
        let end_timestamp = match end_timestamp {
            Some(end_timestamp) => end_timestamp,
            None => -1,
        };
        let request = self.client.pull_logs(chroma_proto::PullLogsRequest {
            collection_id,
            start_from_id: offset,
            batch_size,
            end_timestamp,
        });
        let response = request.await;
        match response {
            Ok(response) => {
                let logs = response.into_inner().records;
                let mut result = Vec::new();
                for log in logs {
                    let embedding_record = log.try_into();
                    match embedding_record {
                        Ok(embedding_record) => {
                            result.push(embedding_record);
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
        let request = self.client.get_all_collection_info_to_compact(
            chroma_proto::GetAllCollectionInfoToCompactRequest {},
        );
        let response = request.await;

        match response {
            Ok(response) => {
                let collections = response.into_inner().all_collection_info;
                let mut result = Vec::new();
                for collection in collections {
                    result.push(CollectionInfo {
                        collection_id: collection.collection_id,
                        first_log_id: collection.first_log_id,
                        first_log_id_ts: collection.first_log_id_ts,
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
}

#[derive(Error, Debug)]
pub(crate) enum PullLogsError {
    #[error("Failed to fetch")]
    FailedToPullLogs(#[from] tonic::Status),
    #[error("Failed to convert proto segment")]
    ConversionError(#[from] EmbeddingRecordConversionError),
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

// This is used for testing only
#[derive(Clone)]
pub(crate) struct LogRecord {
    pub(crate) collection_id: String,
    pub(crate) log_id: i64,
    pub(crate) log_id_ts: i64,
    pub(crate) record: Box<EmbeddingRecord>,
}

impl Debug for LogRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogRecord")
            .field("collection_id", &self.collection_id)
            .field("log_id", &self.log_id)
            .field("log_id_ts", &self.log_id_ts)
            .field("record", &self.record)
            .finish()
    }
}

// This is used for testing only
#[derive(Clone, Debug)]
pub(crate) struct InMemoryLog {
    logs: HashMap<String, Vec<Box<LogRecord>>>,
}

impl InMemoryLog {
    pub fn new() -> InMemoryLog {
        InMemoryLog {
            logs: HashMap::new(),
        }
    }

    pub fn add_log(&mut self, collection_id: String, log: Box<LogRecord>) {
        let logs = self.logs.entry(collection_id).or_insert(Vec::new());
        logs.push(log);
    }
}

#[async_trait]
impl Log for InMemoryLog {
    async fn read(
        &mut self,
        collection_id: String,
        offset: i64,
        batch_size: i32,
        end_timestamp: Option<i64>,
    ) -> Result<Vec<Box<EmbeddingRecord>>, PullLogsError> {
        let end_timestamp = match end_timestamp {
            Some(end_timestamp) => end_timestamp,
            None => i64::MAX,
        };

        let logs = match self.logs.get(&collection_id) {
            Some(logs) => logs,
            None => return Ok(Vec::new()),
        };
        let mut result = Vec::new();
        for i in offset..(offset + batch_size as i64) {
            if i < logs.len() as i64 && logs[i as usize].log_id_ts <= end_timestamp {
                result.push(logs[i as usize].record.clone());
            }
        }
        Ok(result)
    }

    async fn get_collections_with_new_data(
        &mut self,
    ) -> Result<Vec<CollectionInfo>, GetCollectionsWithNewDataError> {
        let mut collections = Vec::new();
        for (collection_id, log_record) in self.logs.iter() {
            if log_record.is_empty() {
                continue;
            }
            // sort the logs by log_id
            let mut logs = log_record.clone();
            logs.sort_by(|a, b| a.log_id.cmp(&b.log_id));
            collections.push(CollectionInfo {
                collection_id: collection_id.clone(),
                first_log_id: logs[0].log_id,
                first_log_id_ts: logs[0].log_id_ts,
            });
        }
        Ok(collections)
    }
}
