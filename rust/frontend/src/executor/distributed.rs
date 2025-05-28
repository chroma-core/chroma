use super::config;
use async_trait::async_trait;
use backon::ExponentialBuilder;
use backon::Retryable;
use chroma_config::registry;
use chroma_config::{assignment::assignment_policy::AssignmentPolicy, Configurable};
use chroma_error::ChromaError;
use chroma_memberlist::client_manager::ClientAssigner;
use chroma_memberlist::client_manager::{ClientManager, ClientOptions};
use chroma_memberlist::{
    config::MemberlistProviderConfig,
    memberlist_provider::{CustomResourceMemberlistProvider, MemberlistProvider},
};
use chroma_system::System;
use chroma_types::chroma_proto::query_executor_client::QueryExecutorClient;
use chroma_types::SegmentType;
use chroma_types::{
    operator::{CountResult, GetResult, KnnBatchResult},
    plan::{Count, Get, Knn},
    ExecutorError,
};
use rand::seq::SliceRandom;
use tonic::Request;

// Convenience type alias for the gRPC query client used by the DistributedExecutor
type QueryClient = QueryExecutorClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>;

/// A distributed executor that routes requests to the appropriate node based on the assignment policy
/// # Fields
/// - `node_name_to_client` - A map from the node name to the gRPC client
/// - `assignment_policy` - The assignment policy to use for routing requests
/// - `replication_factor` - The target replication factor for the request
/// # Notes
/// The executor internally uses a memberlist provider to get the list of nodes to route requests to
/// this memberlist provider sends the list of nodes to the client manager which creates the gRPC clients
/// for the nodes. The ClientManager is considered internal to the DistributedExecutor and is not exposed
/// outside.
#[derive(Clone, Debug)]
pub struct DistributedExecutor {
    client_assigner: ClientAssigner<QueryClient>,
    replication_factor: usize,
    backoff: ExponentialBuilder,
}

#[async_trait]
impl Configurable<(config::DistributedExecutorConfig, System)> for DistributedExecutor {
    async fn try_from_config(
        (config, system): &(config::DistributedExecutorConfig, System),
        registry: &registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let assignment_policy =
            Box::<dyn AssignmentPolicy>::try_from_config(&config.assignment, registry).await?;
        let client_assigner = ClientAssigner::new(assignment_policy, config.replication_factor);
        let client_manager = ClientManager::new(
            client_assigner.clone(),
            config.connections_per_node,
            config.connect_timeout_ms,
            config.request_timeout_ms,
            ClientOptions::new(Some(config.max_query_service_response_size_bytes)),
        );
        let client_manager_handle = system.start_component(client_manager);

        let mut memberlist_provider = match &config.memberlist_provider {
            MemberlistProviderConfig::CustomResource(_memberlist_provider_config) => {
                CustomResourceMemberlistProvider::try_from_config(
                    &config.memberlist_provider,
                    registry,
                )
                .await?
            }
        };
        memberlist_provider.subscribe(client_manager_handle.receiver());
        let _memberlist_provider_handle = system.start_component(memberlist_provider);

        let retry_config = &config.retry;
        let backoff = retry_config.into();
        Ok(Self {
            client_assigner,
            replication_factor: config.replication_factor,
            backoff,
        })
    }
}

impl DistributedExecutor {
    pub fn get_supported_segment_types(&self) -> Vec<SegmentType> {
        vec![
            SegmentType::HnswDistributed,
            SegmentType::Spann,
            SegmentType::BlockfileRecord,
            SegmentType::BlockfileMetadata,
        ]
    }
}

impl DistributedExecutor {
    ///////////////////////// Plan Operations /////////////////////////
    pub async fn count(&mut self, plan: Count) -> Result<CountResult, ExecutorError> {
        let clients = self
            .client_assigner
            .clients(
                &plan
                    .scan
                    .collection_and_segments
                    .collection
                    .collection_id
                    .to_string(),
            )
            .map_err(|e| ExecutorError::Internal(e.boxed()))?;
        let plan: chroma_types::chroma_proto::CountPlan = plan.clone().try_into()?;
        let res = (|| async {
            choose_client(clients.as_slice())?
                .count(Request::new(plan.clone()))
                .await
        })
        .retry(self.backoff)
        .when(is_retryable_error)
        .await?;
        Ok(res.into_inner().into())
    }

    pub async fn get(&mut self, plan: Get) -> Result<GetResult, ExecutorError> {
        let clients = self
            .client_assigner
            .clients(
                &plan
                    .scan
                    .collection_and_segments
                    .collection
                    .collection_id
                    .to_string(),
            )
            .map_err(|e| ExecutorError::Internal(e.boxed()))?;
        let res = (|| async {
            choose_client(clients.as_slice())?
                .get(Request::new(plan.clone().try_into()?))
                .await
        })
        .retry(self.backoff)
        .when(is_retryable_error)
        .await?;
        Ok(res.into_inner().try_into()?)
    }

    pub async fn knn(&mut self, plan: Knn) -> Result<KnnBatchResult, ExecutorError> {
        let clients = self
            .client_assigner
            .clients(
                &plan
                    .scan
                    .collection_and_segments
                    .collection
                    .collection_id
                    .to_string(),
            )
            .map_err(|e| ExecutorError::Internal(e.boxed()))?;
        let res = (|| async {
            choose_client(clients.as_slice())?
                .knn(Request::new(plan.clone().try_into()?))
                .await
        })
        .retry(self.backoff)
        .when(is_retryable_error)
        .await?;
        Ok(res.into_inner().try_into()?)
    }

    pub async fn is_ready(&self) -> bool {
        !self.client_assigner.is_empty()
    }
}

fn choose_client(clients: &[QueryClient]) -> Result<QueryClient, tonic::Status> {
    Ok(clients
        .choose(&mut rand::thread_rng())
        .ok_or(no_clients_found_status())?
        .clone())
}

fn is_retryable_error(e: &tonic::Status) -> bool {
    e.code() == tonic::Code::Unavailable
        || e.code() == tonic::Code::DeadlineExceeded
        || e.code() == tonic::Code::Aborted
        || e.code() == tonic::Code::ResourceExhausted
}

fn no_clients_found_status() -> tonic::Status {
    tonic::Status::internal("No clients found")
}
