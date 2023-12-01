// mirrors the go and python versions of the assignment policy
use uuid::Uuid;

trait AssignmentPolicy {
    fn assign(&self, collection_id: Uuid) -> String;
    fn get_topics(&self) -> Vec<String>;
}

struct RendezvousHashingAssignmentPolicy {
    pulsar_tenant: String,
    pulsar_namespace: String,
}

impl RendezvousHashingAssignmentPolicy {
    pub fn new(pulsar_tenant: &str, pulsar_namespace: &str) -> RendezvousHashingAssignmentPolicy {
        return RendezvousHashingAssignmentPolicy {
            pulsar_tenant: pulsar_tenant,
            pulsar_namespace: pulsar_namespace,
        };
    }
}

impl AssignmentPolicy for RendezvousHashingAssignmentPolicy {
    fn assign(&self, collection_id: Uuid) -> String {
        // let mut hasher = DefaultHasher::new();
        // collection_id.hash(&mut hasher);
        // let hash = hasher.finish();
        // let mut max_hash = 0;
        // let mut max_topic = String::new();
        // for topic in self.topics.iter() {
        //     let mut topic_hasher = DefaultHasher::new();
        //     topic.hash(&mut topic_hasher);
        //     let topic_hash = topic_hasher.finish();
        //     let combined_hash = topic_hash ^ hash;
        //     if combined_hash > max_hash {
        //         max_hash = combined_hash;
        //         max_topic = topic.clone();
        //     }
        // }
        // return max_topic;
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
    }
}
