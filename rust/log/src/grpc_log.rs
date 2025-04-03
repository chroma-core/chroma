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

#[derive(Error, Debug)]
pub enum GrpcPurgeDirtyForCollectionError {
    #[error("Failed to update collection log offset")]
    FailedToPurgeDirty(#[from] tonic::Status),
}

impl ChromaError for GrpcPurgeDirtyForCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcPurgeDirtyForCollectionError::FailedToPurgeDirty(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Clone, Debug)]
pub struct GrpcLog {
    config: GrpcLogConfig,
    #[allow(clippy::type_complexity)]
    client: LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>,
    #[allow(clippy::type_complexity)]
    alt_client:
        Option<LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>>,
}

impl GrpcLog {
    #[allow(clippy::type_complexity)]
    pub(crate) fn new(
        config: GrpcLogConfig,
        client: LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>,
        alt_client: Option<
            LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>,
        >,
    ) -> Self {
        Self {
            config,
            client,
            alt_client,
        }
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
        let max_encoding_message_size = my_config.max_encoding_message_size;
        let max_decoding_message_size = my_config.max_decoding_message_size;
        let connection_string = format!("http://{}:{}", host, port);
        let client_for_conn_str =
            |connection_string: String| -> Result<LogServiceClient<_>, Box<dyn ChromaError>> {
                tracing::info!("Connecting to log service at {}", connection_string);
                let endpoint_res = match Endpoint::from_shared(connection_string) {
                    Ok(endpoint) => endpoint,
                    Err(e) => return Err(Box::new(GrpcLogError::FailedToConnect(e))),
                };
                let endpoint_res = endpoint_res
                    .connect_timeout(Duration::from_millis(my_config.connect_timeout_ms))
                    .timeout(Duration::from_millis(my_config.request_timeout_ms));
                let channel = endpoint_res.connect_lazy();
                let channel = ServiceBuilder::new()
                    .layer(chroma_tracing::GrpcTraceLayer)
                    .service(channel);
                let client = LogServiceClient::new(channel)
                    .max_encoding_message_size(max_encoding_message_size)
                    .max_decoding_message_size(max_decoding_message_size);
                Ok(client)
            };
        let client = client_for_conn_str(connection_string)?;
        let alt_client = if let Some(alt_host) = my_config.alt_host.as_ref() {
            let connection_string = format!("http://{}:{}", alt_host, port);
            tracing::info!("connecting to alt host {connection_string}");
            Some(client_for_conn_str(connection_string)?)
        } else {
            None
        };
        return Ok(GrpcLog::new(my_config.clone(), client, alt_client));
    }
}

impl GrpcLog {
    fn client_for(
        &mut self,
        collection_id: CollectionUuid,
    ) -> &mut LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>> {
        let collection_id = collection_id.to_string();
        if let Some(alt) = self.alt_client.as_mut() {
            if self.config.use_alt_host_for_everything
                || self.config.use_alt_for_collections.contains(&collection_id)
            {
                tracing::info!("using alt client for {collection_id}");
                return alt;
            }
        }
        &mut self.client
    }

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
        tracing::info!("pull_logs offset: {}, batch_size: {}", offset, batch_size);
        let request = self
            .client_for(collection_id)
            .pull_logs(chroma_proto::PullLogsRequest {
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

        self.client_for(collection_id).push_logs(request).await?;

        Ok(())
    }

    pub(crate) async fn get_collections_with_new_data(
        &mut self,
        min_compaction_size: u64,
    ) -> Result<Vec<CollectionInfo>, GrpcGetCollectionsWithNewDataError> {
        let mut norm = self
            ._get_collections_with_new_data(false, min_compaction_size)
            .await?;
        if self.config.use_alt_host_for_everything
            || !self.config.use_alt_for_collections.is_empty()
        {
            let alt = self
                ._get_collections_with_new_data(true, min_compaction_size)
                .await?;
            norm.extend(alt)
        }
        Ok(norm)
    }

    async fn _get_collections_with_new_data(
        &mut self,
        use_alt_log: bool,
        min_compaction_size: u64,
    ) -> Result<Vec<CollectionInfo>, GrpcGetCollectionsWithNewDataError> {
        let response = if use_alt_log {
            if let Some(alt_client) = self.alt_client.as_mut() {
                alt_client
                    .get_all_collection_info_to_compact(
                        chroma_proto::GetAllCollectionInfoToCompactRequest {
                            min_compaction_size,
                        },
                    )
                    .await
            } else {
                return Ok(vec![]);
            }
        } else {
            self.client
                .get_all_collection_info_to_compact(
                    chroma_proto::GetAllCollectionInfoToCompactRequest {
                        min_compaction_size,
                    },
                )
                .await
        };

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
        let request = self.client_for(collection_id).update_collection_log_offset(
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

    pub(super) async fn purge_dirty_for_collection(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<(), GrpcPurgeDirtyForCollectionError> {
        let request = self.client_for(collection_id).purge_dirty_for_collection(
            chroma_proto::PurgeDirtyForCollectionRequest {
                // NOTE(rescrv):  Use the untyped string representation of the collection ID.
                collection_id: collection_id.0.to_string(),
            },
        );
        let response = request.await;
        match response {
            Ok(_) => Ok(()),
            Err(e) => Err(GrpcPurgeDirtyForCollectionError::FailedToPurgeDirty(e)),
        }
    }
}
