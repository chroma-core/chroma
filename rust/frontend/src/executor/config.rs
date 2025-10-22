use std::time::Duration;

use super::{distributed::DistributedExecutor, local::LocalExecutor, Executor};
use crate::executor::distributed::ClientSelectionConfig;
use async_trait::async_trait;
use backon::ExponentialBuilder;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_system::System;
use serde::{Deserialize, Serialize};

// 32 MB
fn default_max_query_service_response_size_bytes() -> usize {
    1024 * 1024 * 32
}

fn default_query_service_port() -> u16 {
    50051
}

/// Configuration for the distributed executor.
/// # Fields
/// - `connections_per_node` - The number of connections to maintain per node
/// - `replication_factor` - The target replication factor for the request
/// - `connect_timeout_ms` - The timeout for connecting to a node
/// - `request_timeout_ms` - The timeout for the request
/// - `assignment` - The assignment policy to use for routing requests
/// - `memberlist_provider` - The memberlist provider to use for getting the list of nodes
/// - `port` - The port the query service listens on. Defaults to 50051.
#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct DistributedExecutorConfig {
    pub connections_per_node: usize,
    pub replication_factor: usize,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    #[serde(default = "RetryConfig::default")]
    pub retry: RetryConfig,
    pub assignment: chroma_config::assignment::config::AssignmentPolicyConfig,
    pub memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig,
    #[serde(default = "default_max_query_service_response_size_bytes")]
    pub max_query_service_response_size_bytes: usize,
    #[serde(default = "ClientSelectionConfig::default")]
    pub client_selection_config: ClientSelectionConfig,
    #[serde(default = "default_query_service_port")]
    pub port: u16,
}

#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct LocalExecutorConfig {}

#[derive(Deserialize, Clone, Serialize, Debug)]
pub enum ExecutorConfig {
    #[serde(alias = "distributed")]
    Distributed(DistributedExecutorConfig),
    #[serde(alias = "local")]
    Local(LocalExecutorConfig),
}

#[async_trait]
impl Configurable<(ExecutorConfig, System)> for Executor {
    async fn try_from_config(
        (config, system): &(ExecutorConfig, System),
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match config {
            ExecutorConfig::Distributed(distributed_config) => {
                let distributed_executor = DistributedExecutor::try_from_config(
                    &(distributed_config.clone(), system.clone()),
                    registry,
                )
                .await?;
                Ok(Executor::Distributed(distributed_executor))
            }
            // TODO(hammadb): WE cannot use this since we cannot inject the sysdb into the local executor
            // use ::new() instead for now
            ExecutorConfig::Local(local_config) => {
                let local_executor = LocalExecutor::try_from_config(local_config, registry).await?;
                Ok(Executor::Local(local_executor))
            }
        }
    }
}

//////////////////////// Retry Config ////////////////////////
/// Configuration for the retry policy.
/// # Fields
/// - `factor` - The factor to multiply the delay by
/// - `min_delay_ms` - The minimum delay in milliseconds
/// - `max_delay_ms` - The maximum delay in milliseconds
/// - `max_attempts` - The maximum number of attempts
#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct RetryConfig {
    pub factor: f32,
    pub min_delay_ms: u64,
    pub max_delay_ms: u64,
    pub max_attempts: usize,
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        RetryConfig {
            factor: 2.0,
            min_delay_ms: 100,
            max_delay_ms: 5000,
            max_attempts: 5,
            jitter: true,
        }
    }
}

impl From<&RetryConfig> for ExponentialBuilder {
    fn from(config: &RetryConfig) -> Self {
        let b = ExponentialBuilder::default()
            .with_factor(config.factor)
            .with_min_delay(Duration::from_millis(config.min_delay_ms))
            .with_max_delay(Duration::from_millis(config.max_delay_ms))
            .with_max_times(config.max_attempts);
        if config.jitter {
            b.with_jitter()
        } else {
            b
        }
    }
}
