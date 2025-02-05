use std::time::Duration;

use super::{distributed::DistributedExecutor, local::LocalExecutor, Executor};
use async_trait::async_trait;
use backon::ExponentialBuilder;
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
    #[serde(default = "RetryConfig::default")]
    pub retry: RetryConfig,
    pub assignment: chroma_config::assignment::config::AssignmentPolicyConfig,
    pub memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig,
}

#[derive(Deserialize, Clone, Serialize)]
pub enum ExecutorConfig {
    Distributed(DistributedExecutorConfig),
    Local,
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
            ExecutorConfig::Local => {
                Ok(Executor::Local(LocalExecutor::try_from_config(&()).await?))
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
#[derive(Deserialize, Clone, Serialize)]
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
