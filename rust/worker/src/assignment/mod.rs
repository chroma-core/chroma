pub(crate) mod assignment_policy;
pub(crate) mod config;
pub(crate) mod rendezvous_hash;

use crate::{config::Configurable, errors::ChromaError};

use self::{assignment_policy::AssignmentPolicy, config::AssignmentPolicyConfig};

pub(crate) async fn from_config(
    config: &AssignmentPolicyConfig,
) -> Result<Box<dyn AssignmentPolicy>, Box<dyn ChromaError>> {
    match &config {
        crate::assignment::config::AssignmentPolicyConfig::RendezvousHashing(_) => Ok(Box::new(
            assignment_policy::RendezvousHashingAssignmentPolicy::try_from_config(config).await?,
        )),
    }
}
