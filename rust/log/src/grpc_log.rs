use crate::config::GrpcLogConfig;
use crate::types::CollectionInfo;
use async_trait::async_trait;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::{self};
use chroma_types::{CollectionUuid, LogRecord, OperationRecord, RecordConversionError};
use std::fmt::Debug;
use std::time::Duration;
use thiserror::Error;
use tonic::transport::Endpoint;
use tower::ServiceBuilder;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum GrpcPullLogsError {
    #[error("Failed to fetch")]
    FailedToPullLogs(#[from] tonic::Status),
    #[error("Failed to convert proto embedding record into EmbeddingRecord")]
    ConversionError(#[from] RecordConversionError),
}

impl ChromaError for GrpcPullLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcPullLogsError::FailedToPullLogs(err) => err.code().into(),
            GrpcPullLogsError::ConversionError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum GrpcPushLogsError {
    #[error("Failed to push logs")]
    FailedToPushLogs(#[from] tonic::Status),
    #[error("Failed to convert records to proto")]
    ConversionError(#[from] RecordConversionError),
}

impl ChromaError for GrpcPushLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcPushLogsError::FailedToPushLogs(_) => ErrorCodes::Internal,
            GrpcPushLogsError::ConversionError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum GrpcGetCollectionsWithNewDataError {
    #[error("Failed to fetch")]
    FailedGetCollectionsWithNewData(#[from] tonic::Status),
}

impl ChromaError for GrpcGetCollectionsWithNewDataError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcGetCollectionsWithNewDataError::FailedGetCollectionsWithNewData(_) => {
                ErrorCodes::Internal
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum GrpcUpdateCollectionLogOffsetError {
    #[error("Failed to update collection log offset")]
    FailedToUpdateCollectionLogOffset(#[from] tonic::Status),
}

impl ChromaError for GrpcUpdateCollectionLogOffsetError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcUpdateCollectionLogOffsetError::FailedToUpdateCollectionLogOffset(_) => {
                ErrorCodes::Internal
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct GrpcLog {
    #[allow(clippy::type_complexity)]
    client: LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>,
}

impl GrpcLog {
    #[allow(clippy::type_complexity)]
    pub(crate) fn new(
        client: LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>,
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
impl Configurable<GrpcLogConfig> for GrpcLog {
    async fn try_from_config(
        my_config: &GrpcLogConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let host = &my_config.host;
        let port = &my_config.port;
        tracing::info!("Connecting to log service at {}:{}", host, port);
        let connection_string = format!("http://{}:{}", host, port);
        let endpoint_res = match Endpoint::from_shared(connection_string) {
            Ok(endpoint) => endpoint,
            Err(e) => return Err(Box::new(GrpcLogError::FailedToConnect(e))),
        };
        let endpoint_res = endpoint_res
            .connect_timeout(Duration::from_millis(my_config.connect_timeout_ms))
            .timeout(Duration::from_millis(my_config.request_timeout_ms));
        let client = endpoint_res.connect().await;
        match client {
            Ok(client) => {
                let channel = ServiceBuilder::new()
                    .layer(chroma_tracing::GrpcTraceLayer)
                    .service(client);

                return Ok(GrpcLog::new(LogServiceClient::new(channel)));
            }
            Err(e) => {
                return Err(Box::new(GrpcLogError::FailedToConnect(e)));
            }
        }
    }
}

impl GrpcLog {
    pub(super) async fn read(
        &mut self,
        collection_id: CollectionUuid,
        offset: i64,
        batch_size: i32,
        end_timestamp: Option<i64>,
    ) -> Result<Vec<LogRecord>, GrpcPullLogsError> {
        let end_timestamp = match end_timestamp {
            Some(end_timestamp) => end_timestamp,
            None => i64::MAX,
        };
        let request = self.client.pull_logs(chroma_proto::PullLogsRequest {
            // NOTE(rescrv):  Use the untyped string representation of the collection ID.
            collection_id: collection_id.0.to_string(),
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
                            return Err(GrpcPullLogsError::ConversionError(err));
                        }
                    }
                }
                Ok(result)
            }
            Err(e) => {
                tracing::error!("Failed to pull logs: {}", e);
                Err(GrpcPullLogsError::FailedToPullLogs(e))
            }
        }
    }

    pub(super) async fn push_logs(
        &mut self,
        collection_id: CollectionUuid,
        records: Vec<OperationRecord>,
    ) -> Result<(), GrpcPushLogsError> {
        let request = chroma_proto::PushLogsRequest {
            collection_id: collection_id.0.to_string(),

            records:
                records.into_iter().map(|r| r.try_into()).collect::<Result<
                    Vec<chroma_types::chroma_proto::OperationRecord>,
                    RecordConversionError,
                >>()?,
        };

        self.client.push_logs(request).await?;

        Ok(())
    }

    pub(super) async fn get_collections_with_new_data(
        &mut self,
        min_compaction_size: u64,
    ) -> Result<Vec<CollectionInfo>, GrpcGetCollectionsWithNewDataError> {
        let response = self
            .client
            .get_all_collection_info_to_compact(
                chroma_proto::GetAllCollectionInfoToCompactRequest {
                    min_compaction_size,
                },
            )
            .await;

        match response {
            Ok(response) => {
                let collections = response.into_inner().all_collection_info;
                let mut result = Vec::new();
                for collection in collections {
                    let collection_uuid = match Uuid::parse_str(&collection.collection_id) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            tracing::error!(
                                "Failed to parse collection id: {}",
                                collection.collection_id
                            );
                            continue;
                        }
                    };
                    let collection_id = CollectionUuid(collection_uuid);
                    result.push(CollectionInfo {
                        collection_id,
                        first_log_offset: collection.first_log_offset,
                        first_log_ts: collection.first_log_ts,
                    });
                }
                Ok(result)
            }
            Err(e) => {
                tracing::error!("Failed to get collections: {}", e);
                Err(GrpcGetCollectionsWithNewDataError::FailedGetCollectionsWithNewData(e))
            }
        }
    }

    pub(super) async fn update_collection_log_offset(
        &mut self,
        collection_id: CollectionUuid,
        new_offset: i64,
    ) -> Result<(), GrpcUpdateCollectionLogOffsetError> {
        let request = self.client.update_collection_log_offset(
            chroma_proto::UpdateCollectionLogOffsetRequest {
                // NOTE(rescrv):  Use the untyped string representation of the collection ID.
                collection_id: collection_id.0.to_string(),
                log_offset: new_offset,
            },
        );
        let response = request.await;
        match response {
            Ok(_) => Ok(()),
            Err(e) => Err(GrpcUpdateCollectionLogOffsetError::FailedToUpdateCollectionLogOffset(e)),
        }
    }
}
