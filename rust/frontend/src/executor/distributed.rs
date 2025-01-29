use super::{client_manager::ClientManager, config};
use async_trait::async_trait;
use chroma_config::{
    assignment::{self, assignment_policy::AssignmentPolicy},
    Configurable,
};
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
use parking_lot::Mutex;
use rand::Rng;
use std::{cmp::min, collections::HashMap, sync::Arc};
use tonic::Request;

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
    node_name_to_client:
        Arc<Mutex<HashMap<String, QueryExecutorClient<tonic::transport::Channel>>>>,
    assignment_policy: Box<dyn AssignmentPolicy>,
    replication_factor: usize,
}

#[async_trait]
impl Configurable<(config::DistributedExecutorConfig, System)> for DistributedExecutor {
    async fn try_from_config(
        (config, system): &(config::DistributedExecutorConfig, System),
    ) -> Result<Self, Box<dyn ChromaError>> {
        let assignment_policy = assignment::from_config(&config.assignment).await?;
        let node_name_to_client = Arc::new(Mutex::new(HashMap::new()));
        let client_manager = ClientManager::new(
            node_name_to_client.clone(),
            config.connections_per_node,
            config.connect_timeout_ms,
            config.request_timeout_ms,
        );
        let client_manager_handle = system.start_component(client_manager);

        let mut memberlist_provider = match &config.memberlist_provider {
            MemberlistProviderConfig::CustomResource(_memberlist_provider_config) => {
                CustomResourceMemberlistProvider::try_from_config(&config.memberlist_provider)
                    .await?
            }
        };
        memberlist_provider.subscribe(client_manager_handle.receiver());
        let _memberlist_provider_handle = system.start_component(memberlist_provider);

        Ok(Self {
            node_name_to_client,
            assignment_policy,
            replication_factor: config.replication_factor,
        })
    }
}

impl DistributedExecutor {
    ///////////////////////// Plan Operations /////////////////////////
    pub async fn count(&mut self, plan: Count) -> Result<CountResult, ExecutorError> {
        let mut client = self.client(plan.scan.collection_and_segments.collection.collection_id)?;
        Ok(client
            .count(Request::new(plan.into()))
            .await?
            .into_inner()
            .count)
    }

    pub async fn get(&mut self, plan: Get) -> Result<GetResult, ExecutorError> {
        let mut client = self.client(plan.scan.collection_and_segments.collection.collection_id)?;
        Ok(client
            .get(Request::new(plan.try_into()?))
            .await?
            .into_inner()
            .try_into()?)
    }
    pub async fn knn(&mut self, plan: Knn) -> Result<KnnBatchResult, ExecutorError> {
        let mut client = self.client(plan.scan.collection_and_segments.collection.collection_id)?;
        Ok(from_proto_knn_batch_result(
            client
                .knn(Request::new(plan.try_into()?))
                .await?
                .into_inner(),
        )?)
    }

    ///////////////////////// Helpers /////////////////////////

    /// Get the gRPC client for the given collection id by performing the assignment policy
    /// # Arguments
    /// - `collection_id` - The collection id for which the client is to be fetched
    /// # Returns
    /// - The gRPC client for the given collection id
    /// # Errors
    /// - If no client is found for the given collection id
    /// - If the assignment policy fails to assign the collection id
    fn client(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<QueryExecutorClient<tonic::transport::Channel>, ExecutorError> {
        // NOTE(hammadb): We hold the lock for the entire duration of the function
        // we could potentially hold the lock for a shorter duration
        let node_name_to_client_guard = self.node_name_to_client.lock();
        let members: Vec<String> = node_name_to_client_guard.keys().cloned().collect();

        // Ensure that the target replication factor is not greater than the number of members
        // since the assignment policy errors if the target replication factor is greater than the number of members.
        // We would prefer that we still send the request to a node rather than erroring.
        let target_replication_factor = min(self.replication_factor, members.len());
        self.assignment_policy.set_members(members);
        let assigned = self
            .assignment_policy
            .assign(&collection_id.to_string(), target_replication_factor)?;
        let random_index = rand::thread_rng().gen_range(0..assigned.len());
        let client = node_name_to_client_guard
            .get(&assigned[random_index])
            .ok_or_else(|| ExecutorError::NoClientFound(assigned[random_index].clone()))?;
        Ok(client.clone())
    }
}
