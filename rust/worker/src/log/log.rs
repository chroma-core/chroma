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
use thiserror::Error;

// CollectionInfo is a struct that contains information about a collection for the
// compacting process. It contains information about the collection id, the first log id,
// and the first log id timestamp since last compaction.
pub(crate) struct CollectionInfo {
    pub(crate) collection_id: String,
    pub(crate) first_log_id: i64,
    pub(crate) first_log_id_ts: i64,
}

#[async_trait]
pub(crate) trait Log: Send + Sync + LogClone {
    async fn read(
        &mut self,
        collection_id: String,
        offset: i64,
        batch_size: i32,
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

#[derive(Clone)]
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
    ) -> Result<Vec<Box<EmbeddingRecord>>, PullLogsError> {
        let request = self.client.pull_logs(chroma_proto::PullLogsRequest {
            collection_id: collection_id,
            start_from_id: offset,
            batch_size: batch_size,
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
