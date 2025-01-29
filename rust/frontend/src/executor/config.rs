use super::{distributed::DistributedExecutor, Executor};
use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_system::System;
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub(crate) struct DistributedExecutorConfig {
    pub(crate) connections_per_node: usize,
    pub(crate) replication_factor: usize,
    pub(crate) assignment: chroma_config::assignment::config::AssignmentPolicyConfig,
    pub(crate) memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig,
}

#[derive(Deserialize, Clone)]
pub(crate) enum ExecutorConfig {
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
