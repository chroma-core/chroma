// mirrors the go and python versions of the assignment policy
use crate::rendezvous_hash::{self, Murmur3Hasher};
use uuid::Uuid;

pub(crate) trait AssignmentPolicy: Send {
    fn assign(&self, collection_id: Uuid) -> String;
    fn get_topics(&self) -> Vec<String>;
}

pub(crate) struct RendezvousHashingAssignmentPolicy {
    pulsar_tenant: String,
    pulsar_namespace: String,
    hasher: Murmur3Hasher, // TODO: shiould this be generic?
}

impl RendezvousHashingAssignmentPolicy {
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

impl AssignmentPolicy for RendezvousHashingAssignmentPolicy {
    fn assign(&self, collection_id: Uuid) -> String {
        let collection_id = collection_id.to_string();
        let topics = self.get_topics();
        let topic = rendezvous_hash::assign(&collection_id, topics, &self.hasher).unwrap();
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
