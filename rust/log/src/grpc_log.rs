use crate::config::GrpcLogConfig;
use crate::types::CollectionInfo;
use async_trait::async_trait;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::{self};
use chroma_types::{
    CollectionUuid, ForkLogsResponse, LogRecord, OperationRecord, RecordConversionError,
};
use std::fmt::Debug;
use std::time::Duration;
use thiserror::Error;
use tonic::transport::Endpoint;
use tower::ServiceBuilder;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum GrpcPullLogsError {
    #[error("Please backoff exponentially and retry")]
    Backoff,
    #[error("Failed to fetch")]
    FailedToPullLogs(#[from] tonic::Status),
    #[error("Failed to scout logs: {0}")]
    FailedToScoutLogs(tonic::Status),
    #[error("Failed to convert proto embedding record into EmbeddingRecord")]
    ConversionError(#[from] RecordConversionError),
}

impl ChromaError for GrpcPullLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcPullLogsError::Backoff => ErrorCodes::Unavailable,
            GrpcPullLogsError::FailedToPullLogs(err) => err.code().into(),
            GrpcPullLogsError::FailedToScoutLogs(err) => err.code().into(),
            GrpcPullLogsError::ConversionError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum GrpcPushLogsError {
    #[error("Please backoff exponentially and retry")]
    Backoff,
    #[error("The log is sealed.  No writes can happen.")]
    Sealed,
    #[error("Failed to push logs: {0}")]
    FailedToPushLogs(#[from] tonic::Status),
    #[error("Failed to convert records to proto")]
    ConversionError(#[from] RecordConversionError),
}

impl ChromaError for GrpcPushLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcPushLogsError::Backoff => ErrorCodes::AlreadyExists,
            GrpcPushLogsError::FailedToPushLogs(_) => ErrorCodes::Internal,
            GrpcPushLogsError::ConversionError(_) => ErrorCodes::Internal,
            GrpcPushLogsError::Sealed => ErrorCodes::FailedPrecondition,
        }
    }
}

#[derive(Error, Debug)]
pub enum GrpcForkLogsError {
    #[error("Please backoff exponentially and retry")]
    Backoff,
    #[error("Failed to push logs: {0}")]
    FailedToForkLogs(#[from] tonic::Status),
}

impl ChromaError for GrpcForkLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcForkLogsError::Backoff => ErrorCodes::Unavailable,
            GrpcForkLogsError::FailedToForkLogs(_) => ErrorCodes::Internal,
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

#[derive(Error, Debug)]
pub enum GrpcSealLogError {
    #[error("Failed to seal collection: {0}")]
    FailedToSeal(#[from] tonic::Status),
}

impl ChromaError for GrpcSealLogError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcSealLogError::FailedToSeal(_) => ErrorCodes::Internal,
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
        // NOTE(rescrv):  This code is duplicated with primary_client_from_config below.  A transient hack.
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
    // NOTE(rescrv) This is a transient hack, so the code duplication is not worth eliminating.
    pub async fn primary_client_from_config(
        my_config: &GrpcLogConfig,
    ) -> Result<
        LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>,
        Box<dyn ChromaError>,
    > {
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
        client_for_conn_str(connection_string)
    }

    fn client_is_on_alt_log(to_evaluate: CollectionUuid, alt_host_threshold: Option<&str>) -> bool {
        if let Some(alt_host_threshold) = alt_host_threshold {
            let Ok(alt_host_threshold) = Uuid::parse_str(alt_host_threshold) else {
                tracing::error!("alt_host_threshold must be a valid UUID");
                return false;
            };
            let to_evaluate = to_evaluate.0.as_u64_pair().0;
            let alt_host_threshold = alt_host_threshold.as_u64_pair().0;
            to_evaluate <= alt_host_threshold
        } else {
            false
        }
    }

    fn client_for(
        &mut self,
        tenant: &str,
        collection_id: CollectionUuid,
    ) -> &mut LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>> {
        if let Some(alt) = self.alt_client.as_mut() {
            if self.config.use_alt_for_tenants.iter().any(|t| t == tenant)
                || self
                    .config
                    .use_alt_for_collections
                    .contains(&collection_id.to_string())
                || Self::client_is_on_alt_log(
                    collection_id,
                    self.config.alt_host_threshold.as_deref(),
                )
            {
                tracing::info!("using alt client for {collection_id}");
                return alt;
            }
        }
        tracing::info!("using standard client for {collection_id}");
        &mut self.client
    }

    fn client_for_purge(
        &mut self,
    ) -> Option<&mut LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>>
    {
        self.alt_client.as_mut()
    }

    // ScoutLogs returns the offset of the next record to be inserted into the log.
    #[tracing::instrument(skip(self), ret)]
    pub(super) async fn scout_logs(
        &mut self,
        tenant: &str,
        collection_id: CollectionUuid,
        start_from: u64,
    ) -> Result<u64, Box<dyn ChromaError>> {
        let request =
            self.client_for(tenant, collection_id)
                .scout_logs(chroma_proto::ScoutLogsRequest {
                    collection_id: collection_id.0.to_string(),
                });
        let response = request.await;
        let response = match response {
            Ok(response) => response,
            Err(err) => {
                tracing::error!("Failed to scout logs: {}", err);
                return Err(Box::new(GrpcPullLogsError::FailedToScoutLogs(err)));
            }
        };
        let scout = response.into_inner();
        Ok(scout.first_uninserted_record_offset as u64)
    }

    #[tracing::instrument(skip(self))]
    pub(super) async fn read(
        &mut self,
        tenant: &str,
        collection_id: CollectionUuid,
        offset: i64,
        batch_size: i32,
        end_timestamp: Option<i64>,
    ) -> Result<Vec<LogRecord>, GrpcPullLogsError> {
        let end_timestamp = match end_timestamp {
            Some(end_timestamp) => end_timestamp,
            None => i64::MAX,
        };
        let request =
            self.client_for(tenant, collection_id)
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
                if e.code() == chroma_error::ErrorCodes::Unavailable.into() {
                    Err(GrpcPullLogsError::Backoff)
                } else {
                    tracing::error!("Failed to pull logs: {}", e);
                    Err(GrpcPullLogsError::FailedToPullLogs(e))
                }
            }
        }
    }

    pub(super) async fn push_logs(
        &mut self,
        tenant: &str,
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

        let resp = self
            .client_for(tenant, collection_id)
            .push_logs(request)
            .await
            .map_err(|err| {
                if err.code() == ErrorCodes::Unavailable.into()
                    || err.code() == ErrorCodes::AlreadyExists.into()
                {
                    GrpcPushLogsError::Backoff
                } else {
                    err.into()
                }
            })?;
        let resp = resp.into_inner();
        if resp.log_is_sealed {
            Err(GrpcPushLogsError::Sealed)
        } else {
            Ok(())
        }
    }

    pub(super) async fn fork_logs(
        &mut self,
        tenant: &str,
        source_collection_id: CollectionUuid,
        target_collection_id: CollectionUuid,
    ) -> Result<ForkLogsResponse, GrpcForkLogsError> {
        let response = self
            .client_for(tenant, source_collection_id)
            .fork_logs(chroma_proto::ForkLogsRequest {
                source_collection_id: source_collection_id.to_string(),
                target_collection_id: target_collection_id.to_string(),
            })
            .await
            .map_err(|err| match err.code() {
                tonic::Code::Unavailable => GrpcForkLogsError::Backoff,
                _ => err.into(),
            })?
            .into_inner();
        Ok(ForkLogsResponse {
            compaction_offset: response.compaction_offset,
            enumeration_offset: response.enumeration_offset,
        })
    }

    pub(crate) async fn get_collections_with_new_data(
        &mut self,
        min_compaction_size: u64,
    ) -> Result<Vec<CollectionInfo>, GrpcGetCollectionsWithNewDataError> {
        let mut norm = self
            ._get_collections_with_new_data(false, min_compaction_size)
            .await?;
        if self.config.alt_host_threshold.is_some()
            || !self.config.use_alt_for_tenants.is_empty()
            || !self.config.use_alt_for_collections.is_empty()
        {
            let alt = self
                ._get_collections_with_new_data(true, min_compaction_size)
                .await?;
            norm.extend(alt)
        }
        norm.sort_by_key(|n| n.collection_id);
        norm.dedup_by_key(|n| n.collection_id);
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
        tenant: &str,
        collection_id: CollectionUuid,
        new_offset: i64,
    ) -> Result<(), GrpcUpdateCollectionLogOffsetError> {
        let request = self
            .client_for(tenant, collection_id)
            .update_collection_log_offset(chroma_proto::UpdateCollectionLogOffsetRequest {
                // NOTE(rescrv):  Use the untyped string representation of the collection ID.
                collection_id: collection_id.0.to_string(),
                log_offset: new_offset,
            });
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
        if let Some(client) = self.client_for_purge() {
            let request =
                client.purge_dirty_for_collection(chroma_proto::PurgeDirtyForCollectionRequest {
                    // NOTE(rescrv):  Use the untyped string representation of the collection ID.
                    collection_id: collection_id.0.to_string(),
                });
            let response = request.await;
            match response {
                Ok(_) => Ok(()),
                Err(e) => Err(GrpcPurgeDirtyForCollectionError::FailedToPurgeDirty(e)),
            }
        } else {
            Ok(())
        }
    }

    pub(super) async fn seal_log(
        &mut self,
        tenant: &str,
        collection_id: CollectionUuid,
    ) -> Result<(), GrpcSealLogError> {
        let _response = self
            .client_for(tenant, collection_id)
            .seal_log(chroma_proto::SealLogRequest {
                collection_id: collection_id.to_string(),
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_is_on_alt_log() {
        assert!(!GrpcLog::client_is_on_alt_log(
            CollectionUuid(Uuid::parse_str("fffdb379-d592-41d1-8de6-412abc6e0b35").unwrap()),
            None
        ));
        assert!(!GrpcLog::client_is_on_alt_log(
            CollectionUuid(Uuid::parse_str("fffdb379-d592-41d1-8de6-412abc6e0b35").unwrap()),
            Some("00088272-cfc4-419d-997a-baebfb25034a"),
        ));
        assert!(GrpcLog::client_is_on_alt_log(
            CollectionUuid(Uuid::parse_str("00088272-cfc4-419d-997a-baebfb25034a").unwrap()),
            Some("fffdb379-d592-41d1-8de6-412abc6e0b35"),
        ));
        assert!(GrpcLog::client_is_on_alt_log(
            CollectionUuid(Uuid::parse_str("fffdb379-d592-41d1-8de6-412abc6e0b35").unwrap()),
            Some("fffdb379-d592-41d1-8de6-412abc6e0b35"),
        ));
        assert!(GrpcLog::client_is_on_alt_log(
            CollectionUuid(Uuid::parse_str("fffdb379-d592-41d1-8de6-412abc6e0b35").unwrap()),
            Some("ffffffff-ffff-ffff-ffff-ffffffffffff"),
        ));
    }
}
