use chroma_memberlist::memberlist_provider::Member;
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use rendezvous_hash::{DefaultNodeHasher, RendezvousNodes};
use std::collections::HashMap;

pub struct WorkDistributor {
    nodes: RendezvousNodes<DefaultNodeHasher>,
    member_map: HashMap<String, Member>,
}

impl WorkDistributor {
    pub fn new(members: Vec<Member>) -> Self {
        let mut nodes = RendezvousNodes::default();
        let mut member_map = HashMap::new();

        for member in members {
            nodes.insert(member.id.to_string());
            member_map.insert(member.id.to_string(), member);
        }

        Self { nodes, member_map }
    }

    pub fn is_my_work(
        &self,
        fn_id: &AttachedFunctionUuid,
        input_coll_id: &CollectionUuid,
        my_shard_id: &str,
    ) -> bool {
        let key = format!("{}-{}", fn_id, input_coll_id);
        match self.nodes.calc_candidates(&key).first() {
            Some(assigned_node) => assigned_node == my_shard_id,
            None => false,
        }
    }
}
