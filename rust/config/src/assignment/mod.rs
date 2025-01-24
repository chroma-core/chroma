pub mod assignment_policy;
pub mod config;
pub mod rendezvous_hash;
use crate::Configurable;

use self::{assignment_policy::AssignmentPolicy, config::AssignmentPolicyConfig};
use chroma_error::ChromaError;

pub async fn from_config(
    config: &AssignmentPolicyConfig,
) -> Result<Box<dyn AssignmentPolicy>, Box<dyn ChromaError>> {
    match &config {
        crate::assignment::config::AssignmentPolicyConfig::RendezvousHashing(_) => Ok(Box::new(
            assignment_policy::RendezvousHashingAssignmentPolicy::try_from_config(config).await?,
        )),
    }
}
