use crate::{config::Configurable, errors::ChromaError};

use super::{
    config::{AssignmentPolicyConfig, HasherType},
    rendezvous_hash::{assign, AssignmentError, Murmur3Hasher},
};
use async_trait::async_trait;

/*
===========================================
Interfaces
===========================================
*/

/// AssignmentPolicy is a trait that defines how to assign a key to a set of members.
/// # Notes
/// This trait mirrors the go and python versions of the assignment policy
/// interface.
/// # Methods
/// - assign: Assign a key to a member.
/// - get_members: Get the members that can be assigned to.
/// - set_members: Set the members that can be assigned to.
pub(crate) trait AssignmentPolicy: Send + Sync {
    fn assign(&self, key: &str) -> Result<String, AssignmentError>;
    fn get_members(&self) -> Vec<String>;
    fn set_members(&mut self, members: Vec<String>);
}

/*
===========================================
Implementation
===========================================
*/

pub(crate) struct RendezvousHashingAssignmentPolicy {
    hasher: Murmur3Hasher,
    members: Vec<String>,
}

impl RendezvousHashingAssignmentPolicy {
    pub(crate) fn new() -> RendezvousHashingAssignmentPolicy {
        return RendezvousHashingAssignmentPolicy {
            hasher: Murmur3Hasher {},
            members: vec![],
        };
    }
}

#[async_trait]
impl Configurable<AssignmentPolicyConfig> for RendezvousHashingAssignmentPolicy {
    async fn try_from_config(
        config: &AssignmentPolicyConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let assignment_policy_config = match config {
            AssignmentPolicyConfig::RendezvousHashing(config) => config,
        };
        let hasher = match assignment_policy_config.hasher {
            HasherType::Murmur3 => Murmur3Hasher {},
        };
        return Ok(RendezvousHashingAssignmentPolicy {
            hasher: hasher,
            members: vec![],
        });
    }
}

impl AssignmentPolicy for RendezvousHashingAssignmentPolicy {
    fn assign(&self, key: &str) -> Result<String, AssignmentError> {
        let members = self.get_members();
        assign(key, members, &self.hasher)
    }

    fn get_members(&self) -> Vec<String> {
        // This is not designed to be used frequently for now, nor is the number of members
        // expected to be large, so we can just clone the members
        return self.members.clone();
    }

    fn set_members(&mut self, members: Vec<String>) {
        self.members = members;
    }
}
