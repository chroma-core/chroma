use crate::{
    config::{Configurable, WorkerConfig},
    errors::ChromaError,
};

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
/// - assign: Assign a key to a topic.
/// - get_members: Get the members that can be assigned to.
/// - set_members: Set the members that can be assigned to.
/// # Notes
/// An assignment policy is not responsible for creating the topics it assigns to.
/// It is the responsibility of the caller to ensure that the topics exist.
/// An assignment policy must be Send.
pub(crate) trait AssignmentPolicy: Send {
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
    // Rust beginners note
    // The reason we take String and not &str is because we need to put the strings into our
    // struct, and we can't do that with references so rather than clone the strings, we just
    // take ownership of them and put the responsibility on the caller to clone them if they
    // need to. This is the general pattern we should follow in rust - put the burden of cloning
    // on the caller, and if they don't need to clone, they can pass ownership.
    pub(crate) fn new(
        pulsar_tenant: String,
        pulsar_namespace: String,
    ) -> RendezvousHashingAssignmentPolicy {
        return RendezvousHashingAssignmentPolicy {
            hasher: Murmur3Hasher {},
            members: vec![],
        };
    }

    pub(crate) fn set_members(&mut self, members: Vec<String>) {
        self.members = members;
    }
}

#[async_trait]
impl Configurable for RendezvousHashingAssignmentPolicy {
    async fn try_from_config(worker_config: &WorkerConfig) -> Result<Self, Box<dyn ChromaError>> {
        let assignment_policy_config = match &worker_config.assignment_policy {
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
        let topics = self.get_members();
        let topic = assign(key, topics, &self.hasher);
        return topic;
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
