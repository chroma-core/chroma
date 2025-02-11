use super::client_manager::NodeNameToClient;
use super::{client_manager::ClientManager, config};
use async_trait::async_trait;
use backon::ExponentialBuilder;
use backon::Retryable;
use chroma_config::registry;
use chroma_config::{assignment::assignment_policy::AssignmentPolicy, Configurable};
use chroma_error::ChromaError;
use chroma_memberlist::{
    config::MemberlistProviderConfig,
    memberlist_provider::{CustomResourceMemberlistProvider, MemberlistProvider},
};
use chroma_system::System;
use chroma_types::{
    chroma_proto::query_executor_client::QueryExecutorClient,
    operator::{from_proto_knn_batch_result, CountResult, GetResult, KnnBatchResult},
    plan::{Count, Get, Knn},
    CollectionUuid, ExecutorError,
};
use rand::seq::SliceRandom;
use std::cmp::min;
use tonic::Request;

type Client = QueryExecutorClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>;

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
    node_name_to_client: NodeNameToClient,
    assignment_policy: Box<dyn AssignmentPolicy>,
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
        let node_name_to_client = NodeNameToClient::default();
        let client_manager = ClientManager::new(
            node_name_to_client.clone(),
            config.connections_per_node,
            config.connect_timeout_ms,
            config.request_timeout_ms,
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
            node_name_to_client,
            assignment_policy,
            replication_factor: config.replication_factor,
            backoff,
        })
    }
}

impl DistributedExecutor {
    ///////////////////////// Plan Operations /////////////////////////
    pub async fn count(&mut self, plan: Count) -> Result<CountResult, ExecutorError> {
        let clients = self.clients(plan.scan.collection_and_segments.collection.collection_id)?;
        let res = (|| async {
            choose_client(clients.as_slice())?
                .count(Request::new(plan.clone().into()))
                .await
        })
        .retry(self.backoff)
        .when(is_retryable_error)
        .await?
        .into_inner();
        Ok(res.count)
    }

    pub async fn get(&mut self, plan: Get) -> Result<GetResult, ExecutorError> {
        let clients = self.clients(plan.scan.collection_and_segments.collection.collection_id)?;
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
        let clients = self.clients(plan.scan.collection_and_segments.collection.collection_id)?;
        let res = (|| async {
            choose_client(clients.as_slice())?
                .knn(Request::new(plan.clone().try_into()?))
                .await
        })
        .retry(self.backoff)
        .when(is_retryable_error)
        .await?;
        Ok(from_proto_knn_batch_result(res.into_inner())?)
    }

    pub async fn is_ready(&self) -> bool {
        !self.node_name_to_client.read().is_empty()
    }

    ///////////////////////// Helpers /////////////////////////

    /// Get the gRPC clients for the given collection id by performing the assignment policy
    /// # Arguments
    /// - `collection_id` - The collection id for which the client is to be fetched
    /// # Returns
    /// - The gRPC clients for the given collection id in the order of the assignment policy
    /// # Errors
    /// - If no client is found for the given collection id
    /// - If the assignment policy fails to assign the collection id
    fn clients(&mut self, collection_id: CollectionUuid) -> Result<Vec<Client>, ExecutorError> {
        let node_name_to_client_guard = self.node_name_to_client.read();
        let members: Vec<String> = node_name_to_client_guard.keys().cloned().collect();
        let target_replication_factor = min(self.replication_factor, members.len());
        self.assignment_policy.set_members(members);
        let assigned = self
            .assignment_policy
            .assign(&collection_id.to_string(), target_replication_factor)?;
        let clients = assigned
            .iter()
            .map(|node_name| {
                node_name_to_client_guard
                    .get(node_name)
                    .ok_or_else(|| ExecutorError::NoClientFound(node_name.clone()))
                    .cloned()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(clients)
    }
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

fn choose_client(clients: &[Client]) -> Result<Client, tonic::Status> {
    Ok(clients
        .choose(&mut rand::thread_rng())
        .ok_or(no_clients_found_status())?
        .clone())
}
