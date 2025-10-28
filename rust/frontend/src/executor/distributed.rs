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
    operator::{CountResult, GetResult, KnnBatchResult, SearchResult},
    plan::{Count, Get, Knn, Search},
    ExecutorError,
};

use rand::distributions::Distribution;
use tonic::Request;

// Convenience type alias for the gRPC query client used by the DistributedExecutor
type QueryClient =
    QueryExecutorClient<chroma_tracing::GrpcClientTraceService<tonic::transport::Channel>>;

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
    client_selection_config: ClientSelectionConfig,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ClientSelectionConfig {
    pub first_attempt_weights: Vec<f64>,
    pub uniform_on_retry: bool,
}

impl Default for ClientSelectionConfig {
    fn default() -> Self {
        Self {
            first_attempt_weights: vec![1.0, 1.0, 1.0],
            uniform_on_retry: true,
        }
    }
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
            config.port,
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
        let client_selection_config = config.client_selection_config.clone();

        Ok(Self {
            client_assigner,
            replication_factor: config.replication_factor,
            backoff,
            client_selection_config,
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
        let attempt_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let config = self.client_selection_config.clone();
        let res = {
            let attempt_count = attempt_count.clone();
            (|| async {
                let current_attempt =
                    attempt_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let is_retry = current_attempt > 0;
                choose_query_client_weighted(&clients, &config, is_retry)?
                    .count(Request::new(plan.clone()))
                    .await
            })
            .retry(self.backoff)
            .when(is_retryable_error)
            .await?
        };
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
        let attempt_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let config = self.client_selection_config.clone();
        let res = {
            let attempt_count = attempt_count.clone();
            (|| async {
                let current_attempt =
                    attempt_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let is_retry = current_attempt > 0;
                choose_query_client_weighted(&clients, &config, is_retry)?
                    .get(Request::new(plan.clone().try_into()?))
                    .await
            })
            .retry(self.backoff)
            .when(is_retryable_error)
            .await?
        };
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
        let attempt_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let config = self.client_selection_config.clone();
        let res = {
            let attempt_count = attempt_count.clone();
            (|| async {
                let current_attempt =
                    attempt_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let is_retry = current_attempt > 0;
                choose_query_client_weighted(&clients, &config, is_retry)?
                    .knn(Request::new(plan.clone().try_into()?))
                    .await
            })
            .retry(self.backoff)
            .when(is_retryable_error)
            .await?
        };
        Ok(res.into_inner().try_into()?)
    }

    pub async fn search(&mut self, plan: Search) -> Result<SearchResult, ExecutorError> {
        // Get the collection ID from the plan
        let collection_id = &plan
            .scan
            .collection_and_segments
            .collection
            .collection_id
            .to_string();

        let clients = self
            .client_assigner
            .clients(collection_id)
            .map_err(|e| ExecutorError::Internal(e.boxed()))?;

        // Convert plan to proto
        let request: chroma_types::chroma_proto::SearchPlan = plan.try_into()?;

        let attempt_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let config = self.client_selection_config.clone();
        let res = {
            let attempt_count = attempt_count.clone();
            (|| async {
                let current_attempt =
                    attempt_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let is_retry = current_attempt > 0;
                choose_query_client_weighted(&clients, &config, is_retry)?
                    .search(Request::new(request.clone()))
                    .await
            })
            .retry(self.backoff)
            .when(is_retryable_error)
            .await?
        };
        Ok(res.into_inner().try_into()?)
    }

    pub async fn is_ready(&self) -> bool {
        !self.client_assigner.is_empty()
    }
}

fn choose_client_weighted<T: Clone>(
    clients: &[T],
    config: &ClientSelectionConfig,
    is_retry: bool,
) -> Result<T, tonic::Status> {
    if clients.is_empty() {
        return Err(no_clients_found_status());
    }

    if clients.len() == 1 {
        return Ok(clients[0].clone());
    }

    let mut rng = rand::thread_rng();

    let selection_weights = if is_retry && config.uniform_on_retry {
        vec![1.0; clients.len()]
    } else {
        let mut res = config.first_attempt_weights.clone();
        if clients.len() < res.len() {
            tracing::warn!(
                "Client selection weights ({}) exceed available clients ({}), truncating",
                res.len(),
                clients.len()
            );
        }

        res.truncate(clients.len());
        res
    };

    let weight_sum: f64 = selection_weights.iter().sum();
    let normalized_weights: Vec<f64> = if weight_sum > 0.0 {
        selection_weights.iter().map(|w| w / weight_sum).collect()
    } else {
        vec![1.0 / clients.len() as f64; clients.len()]
    };

    let dist = rand::distributions::WeightedIndex::new(&normalized_weights).map_err(|e| {
        tracing::error!(
            "Failed to create weighted index for client selection: {}",
            e
        );
        no_clients_found_status()
    })?;
    let idx = dist.sample(&mut rng);
    Ok(clients[idx].clone())
}

fn choose_query_client_weighted(
    clients: &[QueryClient],
    config: &ClientSelectionConfig,
    is_retry: bool,
) -> Result<QueryClient, tonic::Status> {
    choose_client_weighted(clients, config, is_retry)
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

#[cfg(test)]
mod tests {
    use super::*;

    // Simple test client type for easier testing
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestClient {
        id: usize,
    }

    impl TestClient {
        fn new(id: usize) -> Self {
            Self { id }
        }
    }

    // Helper function to create test clients
    fn create_test_clients(count: usize) -> Vec<TestClient> {
        (0..count).map(TestClient::new).collect()
    }

    #[test]
    fn test_weighted_routing_all_to_first_client() {
        // Test that with weights [1.0, 0.0], all queries route to the first client
        let clients = create_test_clients(2);
        let config = ClientSelectionConfig {
            first_attempt_weights: vec![1.0, 0.0],
            uniform_on_retry: false,
        };

        let mut first_client_count = 0;
        let mut second_client_count = 0;
        let total_attempts = 1000;

        for _ in 0..total_attempts {
            let selected_client = choose_client_weighted(&clients, &config, false)
                .expect("Should successfully select a client");

            // Now we can directly compare the clients!
            if selected_client == clients[0] {
                first_client_count += 1;
            } else if selected_client == clients[1] {
                second_client_count += 1;
            }
        }

        // With weights [1.0, 0.0], ALL queries should go to the first client
        assert_eq!(
            first_client_count, total_attempts,
            "All queries should route to client A (weight 1.0)"
        );
        assert_eq!(
            second_client_count, 0,
            "No queries should route to client B (weight 0.0)"
        );
    }

    #[test]
    fn test_biased_client_selection() {
        let clients = create_test_clients(2);
        let config = ClientSelectionConfig {
            first_attempt_weights: vec![0.1, 0.9],
            uniform_on_retry: false,
        };

        let mut first_client_count = 0;
        let mut second_client_count = 0;
        let total_attempts = 1000;

        for _ in 0..total_attempts {
            let selected_client = choose_client_weighted(&clients, &config, false)
                .expect("Should successfully select a client");

            if selected_client == clients[0] {
                first_client_count += 1;
            } else if selected_client == clients[1] {
                second_client_count += 1;
            }
        }

        assert!(
            second_client_count > total_attempts * 2 / 3,
            "Most queries should route to client B (weight 0.9), got {}/{}",
            second_client_count,
            total_attempts
        );
        assert!(
            first_client_count < total_attempts / 3,
            "Few queries should route to client A (weight 0.1), got {}/{}",
            first_client_count,
            total_attempts
        );
    }
}
