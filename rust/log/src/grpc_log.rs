use crate::config::GrpcLogConfig;
use crate::types::CollectionInfo;
use async_trait::async_trait;
use chroma_config::assignment::assignment_policy::AssignmentPolicy;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_memberlist::client_manager::{
    ClientAssigner, ClientAssignmentError, ClientManager, ClientOptions,
};
use chroma_memberlist::config::MemberlistProviderConfig;
use chroma_memberlist::memberlist_provider::{
    CustomResourceMemberlistProvider, MemberlistProvider,
};
use chroma_system::System;
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

//////////////// Errors ////////////////

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
    #[error(transparent)]
    ClientAssignerError(#[from] ClientAssignmentError),
}

impl ChromaError for GrpcPullLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcPullLogsError::Backoff => ErrorCodes::Unavailable,
            GrpcPullLogsError::FailedToPullLogs(err) => err.code().into(),
            GrpcPullLogsError::FailedToScoutLogs(err) => err.code().into(),
            GrpcPullLogsError::ConversionError(_) => ErrorCodes::Internal,
            GrpcPullLogsError::ClientAssignerError(e) => e.code(),
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
    #[error(transparent)]
    ClientAssignerError(#[from] ClientAssignmentError),
}

impl ChromaError for GrpcPushLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcPushLogsError::Backoff => ErrorCodes::AlreadyExists,
            GrpcPushLogsError::FailedToPushLogs(_) => ErrorCodes::Internal,
            GrpcPushLogsError::ConversionError(_) => ErrorCodes::Internal,
            GrpcPushLogsError::Sealed => ErrorCodes::FailedPrecondition,
            GrpcPushLogsError::ClientAssignerError(e) => e.code(),
        }
    }
}

#[derive(Error, Debug)]
pub enum GrpcForkLogsError {
    #[error("Please backoff exponentially and retry")]
    Backoff,
    #[error("Failed to push logs: {0}")]
    FailedToForkLogs(#[from] tonic::Status),
    #[error(transparent)]
    ClientAssignerError(#[from] ClientAssignmentError),
}

impl ChromaError for GrpcForkLogsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcForkLogsError::Backoff => ErrorCodes::Unavailable,
            GrpcForkLogsError::FailedToForkLogs(_) => ErrorCodes::Internal,
            GrpcForkLogsError::ClientAssignerError(e) => e.code(),
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
    #[error(transparent)]
    ClientAssignerError(#[from] ClientAssignmentError),
}

impl ChromaError for GrpcUpdateCollectionLogOffsetError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcUpdateCollectionLogOffsetError::FailedToUpdateCollectionLogOffset(_) => {
                ErrorCodes::Internal
            }
            GrpcUpdateCollectionLogOffsetError::ClientAssignerError(e) => e.code(),
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
    #[error(transparent)]
    ClientAssignerError(#[from] ClientAssignmentError),
}

impl ChromaError for GrpcSealLogError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcSealLogError::FailedToSeal(_) => ErrorCodes::Internal,
            GrpcSealLogError::ClientAssignerError(e) => e.code(),
        }
    }
}

#[derive(Error, Debug)]
pub enum GrpcMigrateLogError {
    #[error("Failed to migrate collection: {0}")]
    FailedToMigrate(#[from] tonic::Status),
    #[error(transparent)]
    ClientAssignerError(#[from] ClientAssignmentError),
    #[error("not supported by this service")]
    NotSupported,
}

impl ChromaError for GrpcMigrateLogError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcMigrateLogError::FailedToMigrate(status) => status.code().into(),
            GrpcMigrateLogError::ClientAssignerError(e) => e.code(),
            GrpcMigrateLogError::NotSupported => ErrorCodes::Unimplemented,
        }
    }
}

type LogClient = LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>;

#[derive(Clone, Debug)]
pub struct GrpcLog {
    config: GrpcLogConfig,
    client: LogClient,
    alt_client_assigner: Option<ClientAssigner<LogClient>>,
}

impl GrpcLog {
    #[allow(clippy::type_complexity)]
    pub fn new(
        config: GrpcLogConfig,
        client: LogClient,
        alt_client_assigner: Option<ClientAssigner<LogClient>>,
    ) -> Self {
        Self {
            config,
            client,
            alt_client_assigner,
        }
    }
}

#[derive(Error, Debug)]
pub enum GrpcLogError {
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
impl Configurable<(GrpcLogConfig, System)> for GrpcLog {
    async fn try_from_config(
        my_config: &(GrpcLogConfig, System),
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        // NOTE(rescrv):  This code is duplicated with primary_client_from_config below.  A transient hack.
        let (my_config, system) = my_config;
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

        // Alt client manager setup
        let assignment_policy =
            Box::<dyn AssignmentPolicy>::try_from_config(&my_config.assignment, registry).await?;
        let client_assigner = ClientAssigner::new(assignment_policy, 1);
        let client_manager = ClientManager::new(
            client_assigner.clone(),
            1,
            my_config.connect_timeout_ms,
            my_config.request_timeout_ms,
            ClientOptions::new(Some(my_config.max_decoding_message_size)),
        );
        let client_manager_handle = system.start_component(client_manager);

        let mut memberlist_provider = match &my_config.memberlist_provider {
            MemberlistProviderConfig::CustomResource(_memberlist_provider_config) => {
                CustomResourceMemberlistProvider::try_from_config(
                    &my_config.memberlist_provider,
                    registry,
                )
                .await?
            }
        };
        memberlist_provider.subscribe(client_manager_handle.receiver());
        let _memberlist_provider_handle = system.start_component(memberlist_provider);

        return Ok(GrpcLog::new(
            my_config.clone(),
            client,
            Some(client_assigner),
        ));
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
    ) -> Result<
        LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>,
        ClientAssignmentError,
    > {
        if let Some(client_assigner) = self.alt_client_assigner.as_mut() {
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
                // Replication factor is always 1 for log service so we grab the first assigned client.
                // NOTE(hammadb): This err should never be returned, ideally the clients() call
                // would return a provably non-empty vector, but in lieu of that, or panic'ing
                // on a impossible state, we return the underlying error here.
                return client_assigner
                    .clients(&collection_id.to_string())?
                    .drain(..)
                    .next()
                    .ok_or(ClientAssignmentError::NoClientFound(
                        "Impossible state: no client found for collection".to_string(),
                    ));
            }
        }
        tracing::info!("using standard client for {collection_id}");
        Ok(self.client.clone())
    }

    fn client_for_purge(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Option<LogServiceClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>> {
        match &mut self.alt_client_assigner {
            Some(assigner) => assigner
                .clients(&collection_id.to_string())
                .unwrap_or_default()
                .first()
                .cloned(),
            None => None,
        }
    }

    // ScoutLogs returns the offset of the next record to be inserted into the log.
    #[tracing::instrument(skip(self), ret)]
    pub(super) async fn scout_logs(
        &mut self,
        tenant: &str,
        collection_id: CollectionUuid,
        start_from: u64,
    ) -> Result<u64, Box<dyn ChromaError>> {
        let mut client = self
            .client_for(tenant, collection_id)
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        let request = client.scout_logs(chroma_proto::ScoutLogsRequest {
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
        let mut client = self.client_for(tenant, collection_id)?;
        let request = client.pull_logs(chroma_proto::PullLogsRequest {
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
            .client_for(tenant, collection_id)?
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
            .client_for(tenant, source_collection_id)?
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
        let mut combined_response = Vec::new();
        let mut error = None;

        if use_alt_log {
            // Iterate over all alt clients and gather collections
            if let Some(alt_client_assigner) = self.alt_client_assigner.as_ref() {
                let mut all_alt_clients = alt_client_assigner.all();
                if all_alt_clients.is_empty() {
                    tracing::error!(
                        "No alt clients available for getting collections with new data"
                    );
                    return Ok(vec![]);
                }
                for mut alt_client in all_alt_clients.drain(..) {
                    // We error if any subrequest errors
                    match alt_client
                        .get_all_collection_info_to_compact(
                            chroma_proto::GetAllCollectionInfoToCompactRequest {
                                min_compaction_size,
                            },
                        )
                        .await
                    {
                        Ok(response) => {
                            combined_response.push(response.into_inner());
                        }
                        Err(err) => {
                            tracing::error!("could not get all collection info to compact: {err}");
                            if error.is_none() {
                                error = Some(err);
                            }
                            continue;
                        }
                    };
                }
            } else {
                tracing::warn!(
                    "No alt client assigner available for getting collections with new data"
                );
                return Ok(vec![]);
            }
        } else {
            match self
                .client
                .get_all_collection_info_to_compact(
                    chroma_proto::GetAllCollectionInfoToCompactRequest {
                        min_compaction_size,
                    },
                )
                .await
            {
                Ok(response) => {
                    combined_response.push(response.into_inner());
                }
                Err(err) => {
                    tracing::error!("could not get all collection info to compact: {err}");
                    if error.is_none() {
                        error = Some(err);
                    }
                }
            };
        };

        if let Some(status) = error {
            if combined_response.is_empty() {
                return Err(status.into());
            }
        }

        let mut all_collections = Vec::new();
        for response in combined_response {
            let collections = response.all_collection_info;
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
                all_collections.push(CollectionInfo {
                    collection_id,
                    first_log_offset: collection.first_log_offset,
                    first_log_ts: collection.first_log_ts,
                });
            }
        }
        Ok(all_collections)
    }

    pub(super) async fn update_collection_log_offset(
        &mut self,
        tenant: &str,
        collection_id: CollectionUuid,
        new_offset: i64,
    ) -> Result<(), GrpcUpdateCollectionLogOffsetError> {
        let mut client = self.client_for(tenant, collection_id)?;
        let request =
            client.update_collection_log_offset(chroma_proto::UpdateCollectionLogOffsetRequest {
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
        if let Some(mut client) = self.client_for_purge(collection_id) {
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

    pub async fn seal_log(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<(), GrpcSealLogError> {
        // NOTE(rescrv):  Seal log only goes to the go log service, so use the classic connection.
        let _response = self
            .client
            .seal_log(chroma_proto::SealLogRequest {
                collection_id: collection_id.to_string(),
            })
            .await?;
        Ok(())
    }

    pub async fn migrate_log(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<(), GrpcMigrateLogError> {
        // NOTE(rescrv): Migrate log only goes to the rust log service, so use alt client assigner.
        if let Some(client_assigner) = self.alt_client_assigner.as_mut() {
            let mut client = client_assigner
                .clients(&collection_id.to_string())?
                .drain(..)
                .next()
                .ok_or(ClientAssignmentError::NoClientFound(
                    "Impossible state: no client found for collection".to_string(),
                ))?;
            client
                .migrate_log(chroma_proto::MigrateLogRequest {
                    collection_id: collection_id.to_string(),
                })
                .await?;
            Ok(())
        } else {
            Err(GrpcMigrateLogError::NotSupported)
        }
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
