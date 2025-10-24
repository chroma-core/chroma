use std::fmt::Debug;

use crate::client::config::GrpcHeapServiceConfig;
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
use chroma_system::{ComponentHandle, System};
use chroma_types::chroma_proto::heap_tender_service_client::HeapTenderServiceClient;
use chroma_types::chroma_proto::{self};
use thiserror::Error;
use tonic::Request;
use tracing::Instrument;

//////////////// Errors ////////////////

/// Errors that can occur when interacting with the heap service.
#[derive(Error, Debug)]
pub enum GrpcHeapServiceError {
    /// Failed to establish connection to heap service.
    #[error("Failed to connect to heap service")]
    FailedToConnect(#[from] tonic::transport::Error),
    /// Failed to push schedules to heap.
    #[error("Failed to push to heap: {0}")]
    FailedToPush(#[from] tonic::Status),
    /// Failed to prune completed tasks from heap.
    #[error("Failed to prune heap: {0}")]
    FailedToPrune(tonic::Status),
    /// Failed to get heap summary statistics.
    #[error("Failed to get heap summary: {0}")]
    FailedToGetSummary(tonic::Status),
    /// Error from client assignment (e.g., no nodes available).
    #[error(transparent)]
    ClientAssignerError(#[from] ClientAssignmentError),
}

impl ChromaError for GrpcHeapServiceError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcHeapServiceError::FailedToConnect(_) => ErrorCodes::Internal,
            GrpcHeapServiceError::FailedToPush(err) => err.code().into(),
            GrpcHeapServiceError::FailedToPrune(err) => err.code().into(),
            GrpcHeapServiceError::FailedToGetSummary(err) => err.code().into(),
            GrpcHeapServiceError::ClientAssignerError(e) => e.code(),
        }
    }
}

type HeapClient =
    HeapTenderServiceClient<chroma_tracing::GrpcClientTraceService<tonic::transport::Channel>>;

/// gRPC client for the heap tender service.
///
/// This client provides access to the heap tender service which manages scheduled tasks.
/// It uses memberlist-based service discovery to find heap service instances that are
/// colocated with log service pods.
#[derive(Clone)]
pub struct GrpcHeapService {
    config: GrpcHeapServiceConfig,
    client_assigner: ClientAssigner<HeapClient>,
    // Component handles stored to prevent orphaning - these keep the components alive
    _client_manager_handle: ComponentHandle<ClientManager<HeapClient>>,
    _memberlist_provider_handle: ComponentHandle<CustomResourceMemberlistProvider>,
}

impl Debug for GrpcHeapService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcHeapService")
            .field("config", &self.config)
            .field("client_assigner", &self.client_assigner)
            .finish()
    }
}

impl GrpcHeapService {
    /// Create a new heap service client.
    pub fn new(
        config: GrpcHeapServiceConfig,
        client_assigner: ClientAssigner<HeapClient>,
        client_manager_handle: ComponentHandle<ClientManager<HeapClient>>,
        memberlist_provider_handle: ComponentHandle<CustomResourceMemberlistProvider>,
    ) -> Self {
        Self {
            config,
            client_assigner,
            _client_manager_handle: client_manager_handle,
            _memberlist_provider_handle: memberlist_provider_handle,
        }
    }

    /// Check if the heap service is enabled in configuration
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Check if the heap service client is ready (has discovered nodes)
    pub fn is_ready(&self) -> bool {
        !self.client_assigner.is_empty()
    }

    fn client_for(&mut self, key: &str) -> Result<HeapClient, ClientAssignmentError> {
        // Replication factor is always 1 for heap service so we grab the first assigned client.
        self.client_assigner.clients(key)?.drain(..).next().ok_or(
            ClientAssignmentError::NoClientFound(
                "Improbable state: no client found for key".to_string(),
            ),
        )
    }

    /// Push schedules to the heap
    #[tracing::instrument(skip(self, schedules))]
    pub async fn push(
        &mut self,
        schedules: Vec<chroma_proto::Schedule>,
        key: &str,
    ) -> Result<chroma_proto::PushResponse, GrpcHeapServiceError> {
        let mut client = self.client_for(key)?;
        let request = Request::new(chroma_proto::PushRequest { schedules });
        let response = client
            .push(request)
            .instrument(tracing::info_span!("heap_service_push"))
            .await?;
        Ok(response.into_inner())
    }

    /// Prune completed tasks from the heap
    #[tracing::instrument(skip(self))]
    pub async fn prune(
        &mut self,
        limits: Option<chroma_proto::Limits>,
        key: &str,
    ) -> Result<chroma_proto::PruneResponse, GrpcHeapServiceError> {
        let mut client = self.client_for(key)?;
        let request = Request::new(chroma_proto::PruneRequest { limits });
        let response = client
            .prune(request)
            .instrument(tracing::info_span!("heap_service_prune"))
            .await
            .map_err(GrpcHeapServiceError::FailedToPrune)?;
        Ok(response.into_inner())
    }

    /// Get summary statistics from the heap
    #[tracing::instrument(skip(self))]
    pub async fn summary(
        &mut self,
        key: &str,
    ) -> Result<chroma_proto::HeapSummaryResponse, GrpcHeapServiceError> {
        let mut client = self.client_for(key)?;
        let request = Request::new(chroma_proto::HeapSummaryRequest {});
        let response = client
            .summary(request)
            .instrument(tracing::info_span!("heap_service_summary"))
            .await
            .map_err(GrpcHeapServiceError::FailedToGetSummary)?;
        Ok(response.into_inner())
    }
}

#[async_trait]
impl Configurable<(GrpcHeapServiceConfig, System)> for GrpcHeapService {
    async fn try_from_config(
        my_config: &(GrpcHeapServiceConfig, System),
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (my_config, system) = my_config;
        let assignment_policy =
            Box::<dyn AssignmentPolicy>::try_from_config(&my_config.assignment, registry).await?;
        let client_assigner = ClientAssigner::new(assignment_policy, 1);
        let client_manager = ClientManager::new(
            client_assigner.clone(),
            1,
            my_config.connect_timeout_ms,
            my_config.request_timeout_ms,
            my_config.port,
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
        let memberlist_provider_handle = system.start_component(memberlist_provider);

        return Ok(GrpcHeapService::new(
            my_config.clone(),
            client_assigner,
            client_manager_handle,
            memberlist_provider_handle,
        ));
    }
}
