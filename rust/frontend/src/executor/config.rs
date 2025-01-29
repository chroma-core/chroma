use super::{distributed::DistributedExecutor, Executor};
use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_system::System;
use serde::{Deserialize, Serialize};

/// Configuration for the distributed executor.
/// # Fields
/// - `connections_per_node` - The number of connections to maintain per node
/// - `replication_factor` - The target replication factor for the request
/// - `connect_timeout_ms` - The timeout for connecting to a node
/// - `request_timeout_ms` - The timeout for the request
/// - `assignment` - The assignment policy to use for routing requests
/// - `memberlist_provider` - The memberlist provider to use for getting the list of nodes
#[derive(Deserialize, Clone, Serialize)]
pub struct DistributedExecutorConfig {
    pub connections_per_node: usize,
    pub replication_factor: usize,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub assignment: chroma_config::assignment::config::AssignmentPolicyConfig,
    pub memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig,
}

#[derive(Deserialize, Clone, Serialize)]
pub enum ExecutorConfig {
    Distributed(DistributedExecutorConfig),
}

#[async_trait]
impl Configurable<(ExecutorConfig, System)> for Executor {
    async fn try_from_config(
        (config, system): &(ExecutorConfig, System),
    ) -> Result<Self, Box<dyn ChromaError>> {
        match config {
            ExecutorConfig::Distributed(distributed_config) => {
                let distributed_executor = DistributedExecutor::try_from_config(&(
                    distributed_config.clone(),
                    system.clone(),
                ))
                .await?;
                Ok(Executor::Distributed(distributed_executor))
            }
        }
    }
}
