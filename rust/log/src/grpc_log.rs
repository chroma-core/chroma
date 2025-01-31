use super::config::LogConfig;
use crate::types::{
    CollectionInfo, GetCollectionsWithNewDataError, PullLogsError, UpdateCollectionLogOffsetError,
};
use crate::PushLogsError;
use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_tracing::grpc_client_interceptor;
use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::{self};
use chroma_types::{CollectionUuid, LogRecord, OperationRecord, RecordConversionError};
use std::fmt::Debug;
use std::time::Duration;
use thiserror::Error;
use tonic::service::interceptor;
use tonic::transport::Endpoint;
use tonic::{Request, Status};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct GrpcLog {
    #[allow(clippy::type_complexity)]
    client: LogServiceClient<
        interceptor::InterceptedService<
            tonic::transport::Channel,
            fn(Request<()>) -> Result<Request<()>, Status>,
        >,
    >,
}

impl GrpcLog {
    #[allow(clippy::type_complexity)]
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
                        let channel: LogServiceClient<
                            interceptor::InterceptedService<
                                tonic::transport::Channel,
                                fn(Request<()>) -> Result<Request<()>, Status>,
                            >,
                        > = LogServiceClient::with_interceptor(client, grpc_client_interceptor);
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
    pub(super) async fn read(
        &mut self,
        collection_id: CollectionUuid,
        offset: i64,
        batch_size: i32,
        end_timestamp: Option<i64>,
    ) -> Result<Vec<LogRecord>, PullLogsError> {
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
                            return Err(PullLogsError::ConversionError(err));
                        }
                    }
                }
                Ok(result)
            }
            Err(e) => {
                tracing::error!("Failed to pull logs: {}", e);
                Err(PullLogsError::FailedToPullLogs(e))
            }
        }
    }

    pub(super) async fn push_logs(
        &mut self,
        collection_id: CollectionUuid,
        records: Vec<OperationRecord>,
    ) -> Result<(), PushLogsError> {
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
    ) -> Result<Vec<CollectionInfo>, GetCollectionsWithNewDataError> {
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
                Err(GetCollectionsWithNewDataError::FailedGetCollectionsWithNewData(e))
            }
        }
    }

    pub(super) async fn update_collection_log_offset(
        &mut self,
        collection_id: CollectionUuid,
        new_offset: i64,
    ) -> Result<(), UpdateCollectionLogOffsetError> {
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
            Err(e) => Err(UpdateCollectionLogOffsetError::FailedToUpdateCollectionLogOffset(e)),
        }
    }
}
