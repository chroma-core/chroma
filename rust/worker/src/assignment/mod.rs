pub(crate) mod assignment_policy;
pub(crate) mod config;
pub(crate) mod rendezvous_hash;
use self::{assignment_policy::AssignmentPolicy, config::AssignmentPolicyConfig};
use chroma_config::Configurable;
use chroma_error::ChromaError;

pub(crate) async fn from_config(
    config: &AssignmentPolicyConfig,
) -> Result<Box<dyn AssignmentPolicy>, Box<dyn ChromaError>> {
    match &config {
        crate::assignment::config::AssignmentPolicyConfig::RendezvousHashing(_) => Ok(Box::new(
            assignment_policy::RendezvousHashingAssignmentPolicy::try_from_config(config).await?,
        )),
    }
}
