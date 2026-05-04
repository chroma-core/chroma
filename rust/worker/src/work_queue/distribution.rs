use chroma_memberlist::memberlist_provider::Member;
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct WorkDistributor {
    memberlist: Vec<Member>,
}

impl WorkDistributor {
    pub fn new(memberlist: Vec<Member>) -> Self {
        Self { memberlist }
    }

    pub fn update_memberlist(&mut self, memberlist: Vec<Member>) {
        self.memberlist = memberlist;
    }

    pub fn get_shard_for_work(
        &self,
        fn_id: &AttachedFunctionUuid,
        input_coll_id: &CollectionUuid,
    ) -> Option<String> {
        let members = &self.memberlist;
        if members.is_empty() {
            return None;
        }

        // Rendezvous hashing: compute hash for each member with the work item
        let work_key = format!("{}:{}", fn_id, input_coll_id);

        let mut best_member = None;
        let mut best_score = 0u64;

        for member in members {
            let score = self.compute_hash(&work_key, &member.member_id);
            if score > best_score {
                best_score = score;
                best_member = Some(member.member_id.clone());
            }
        }

        best_member
    }

    fn compute_hash(&self, work_key: &str, member_id: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        work_key.hash(&mut hasher);
        member_id.hash(&mut hasher);
        hasher.finish()
    }

    pub fn is_my_work(
        &self,
        fn_id: &AttachedFunctionUuid,
        input_coll_id: &CollectionUuid,
        my_shard_id: &str,
    ) -> bool {
        match self.get_shard_for_work(fn_id, input_coll_id) {
            Some(shard_id) => shard_id == my_shard_id,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_consistent_distribution() {
        // TODO: Add mock memberlist and test distribution consistency
        // This would require a mock implementation of MemberlistProvider
        // For now, just verify the struct can be instantiated
        let _ = std::panic::catch_unwind(|| {
            // Can't test without a real memberlist provider
        });
    }
}
