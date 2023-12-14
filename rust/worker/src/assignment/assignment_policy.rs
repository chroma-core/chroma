use crate::{
    config::{Configurable, WorkerConfig},
    errors::ChromaError,
};

use super::{
    config::{AssignmentPolicyConfig, HasherType},
    rendezvous_hash::{assign, AssignmentError, Murmur3Hasher},
};
use async_trait::async_trait;
use uuid::Uuid;

/*
===========================================
Interfaces
===========================================
*/

/// AssignmentPolicy is a trait that defines how to assign a collection to a topic.
/// # Notes
/// This trait mirrors the go and python versions of the assignment policy
/// interface.
/// # Methods
/// - assign: Assign a collection to a topic.
/// - get_topics: Get the topics that can be assigned to.
/// # Notes
/// An assignment policy is not responsible for creating the topics it assigns to.
/// It is the responsibility of the caller to ensure that the topics exist.
/// An assignment policy must be Send.
pub(crate) trait AssignmentPolicy: Send {
    fn assign(&self, collection_id: Uuid) -> Result<String, AssignmentError>;
    fn get_topics(&self) -> Vec<String>;
}

/*
===========================================
Implementation
===========================================
*/

pub(crate) struct RendezvousHashingAssignmentPolicy {
    // The pulsar tenant and namespace being in this implementation of the assignment policy
    // is purely a temporary measure while the topic propagation is being worked on.
    // TODO: Remove pulsar_tenant and pulsar_namespace from this struct once topic propagation
    // is implemented.
    pulsar_tenant: String,
    pulsar_namespace: String,
    hasher: Murmur3Hasher,
}

impl RendezvousHashingAssignmentPolicy {
    // Rust beginners note
    // The reason we take String and not &str is because we need to put the strings into our
    // struct, and we can't do that with references so rather than clone the strings, we just
    // take ownership of them and put the responsibility on the caller to clone them if they
    // need to. This is the general pattern we should follow in rust - put the burden of cloning
    // on the caller, and if they don't need to clone, they can pass ownership.
    pub fn new(
        pulsar_tenant: String,
        pulsar_namespace: String,
    ) -> RendezvousHashingAssignmentPolicy {
        return RendezvousHashingAssignmentPolicy {
            pulsar_tenant: pulsar_tenant,
            pulsar_namespace: pulsar_namespace,
            hasher: Murmur3Hasher {},
        };
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
            pulsar_tenant: worker_config.pulsar_tenant.clone(),
            pulsar_namespace: worker_config.pulsar_namespace.clone(),
            hasher: hasher,
        });
    }
}

impl AssignmentPolicy for RendezvousHashingAssignmentPolicy {
    fn assign(&self, collection_id: Uuid) -> Result<String, AssignmentError> {
        let collection_id = collection_id.to_string();
        let topics = self.get_topics();
        let topic = assign(&collection_id, topics, &self.hasher);
        return topic;
    }

    fn get_topics(&self) -> Vec<String> {
        // This mirrors the current python and go code, which assumes a fixed set of topics
        let mut topics = Vec::with_capacity(16);
        for i in 0..16 {
            let topic = format!(
                "persistent://{}/{}/chroma_log_{}",
                self.pulsar_tenant, self.pulsar_namespace, i
            );
            topics.push(topic);
        }
        return topics;
    }
}
