pub mod assignment_policy;
pub mod config;
pub mod rendezvous_hash;
use crate::{registry::Registry, Configurable};

use self::{assignment_policy::AssignmentPolicy, config::AssignmentPolicyConfig};
use async_trait::async_trait;
use chroma_error::ChromaError;

#[async_trait]
impl Configurable<AssignmentPolicyConfig> for Box<dyn AssignmentPolicy> {
    async fn try_from_config(
        config: &AssignmentPolicyConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            crate::assignment::config::AssignmentPolicyConfig::RendezvousHashing(_) => {
                Ok(Box::new(
                    assignment_policy::RendezvousHashingAssignmentPolicy::try_from_config(
                        config, registry,
                    )
                    .await?,
                ))
            }
        }
    }
}
