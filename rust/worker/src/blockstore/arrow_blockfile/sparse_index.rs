use crate::blockstore::types::Key;
use crate::blockstore::types::{Blockfile, BlockfileKey};
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

// A sentinel blockfilekey wrapper to represent the start blocks range
#[derive(Clone, Debug)]
pub enum SparseIndexEntry {
    Start,
    Key(BlockfileKey),
}

impl PartialEq for SparseIndexEntry {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SparseIndexEntry::Start, SparseIndexEntry::Start) => true,
            (SparseIndexEntry::Key(k1), SparseIndexEntry::Key(k2)) => k1 == k2,
            _ => false,
        }
    }
}

impl Eq for SparseIndexEntry {}

impl PartialOrd for SparseIndexEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (SparseIndexEntry::Start, SparseIndexEntry::Start) => Some(std::cmp::Ordering::Equal),
            (SparseIndexEntry::Start, SparseIndexEntry::Key(_)) => Some(std::cmp::Ordering::Less),
            (SparseIndexEntry::Key(_), SparseIndexEntry::Start) => {
                Some(std::cmp::Ordering::Greater)
            }
            (SparseIndexEntry::Key(k1), SparseIndexEntry::Key(k2)) => k1.partial_cmp(k2),
        }
    }
}

impl Ord for SparseIndexEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (SparseIndexEntry::Start, SparseIndexEntry::Start) => std::cmp::Ordering::Equal,
            (SparseIndexEntry::Start, SparseIndexEntry::Key(_)) => std::cmp::Ordering::Less,
            (SparseIndexEntry::Key(_), SparseIndexEntry::Start) => std::cmp::Ordering::Greater,
            (SparseIndexEntry::Key(k1), SparseIndexEntry::Key(k2)) => k1.cmp(k2),
        }
    }
}

pub(super) struct SparseIndex {
    forward: BTreeMap<SparseIndexEntry, Uuid>,
    reverse: HashMap<Uuid, SparseIndexEntry>,
}

impl SparseIndex {
    pub(super) fn new(initial_block_id: Uuid) -> Self {
        let mut forward = BTreeMap::new();
        forward.insert(SparseIndexEntry::Start, initial_block_id);
        let mut reverse = HashMap::new();
        reverse.insert(initial_block_id, SparseIndexEntry::Start);
        Self { forward, reverse }
    }

    pub(super) fn from(old_sparse_index: SparseIndex) -> Self {
        Self {
            forward: old_sparse_index.forward.clone(),
            reverse: old_sparse_index.reverse.clone(),
        }
    }

    pub(super) fn get_target_block_id(&self, search_key: &BlockfileKey) -> Uuid {
        let mut iter_curr = self.forward.iter();
        let mut iter_next = self.forward.iter().skip(1);
        let search_key = SparseIndexEntry::Key(search_key.clone());
        while let Some((curr_key, curr_block_id)) = iter_curr.next() {
            if let Some((next_key, _)) = iter_next.next() {
                if search_key >= *curr_key && search_key < *next_key {
                    return *curr_block_id;
                }
            } else {
                return *curr_block_id;
            }
        }
        panic!("No blocks in the sparse index");
    }

    pub(super) fn add_block(&mut self, start_key: BlockfileKey, block_id: Uuid) {
        self.forward
            .insert(SparseIndexEntry::Key(start_key.clone()), block_id);
        self.reverse
            .insert(block_id, SparseIndexEntry::Key(start_key));
    }

    pub(super) fn get_size(&self) -> usize {
        self.forward.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockstore::types::Key;

    #[test]
    fn test_sparse_index() {
        let mut block_id_1 = uuid::Uuid::new_v4();
        let mut sparse_index = SparseIndex::new(block_id_1);
        let mut blockfile_key =
            BlockfileKey::new("prefix".to_string(), Key::String("a".to_string()));
        sparse_index.add_block(blockfile_key.clone(), block_id_1);
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_1);

        blockfile_key = BlockfileKey::new("prefix".to_string(), Key::String("b".to_string()));
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_1);

        // Split the range into two blocks (start, c), and (c, end)
        let block_id_2 = uuid::Uuid::new_v4();
        blockfile_key = BlockfileKey::new("prefix".to_string(), Key::String("c".to_string()));
        sparse_index.add_block(blockfile_key.clone(), block_id_2);
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_2);

        // d should fall into the second block
        blockfile_key = BlockfileKey::new("prefix".to_string(), Key::String("d".to_string()));
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_2);

        // Split the second block into (c, f) and (f, end)
        let block_id_3 = uuid::Uuid::new_v4();
        blockfile_key = BlockfileKey::new("prefix".to_string(), Key::String("f".to_string()));
        sparse_index.add_block(blockfile_key.clone(), block_id_3);
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_3);

        // g should fall into the third block
        blockfile_key = BlockfileKey::new("prefix".to_string(), Key::String("g".to_string()));
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_3);

        // b should fall into the first block
        blockfile_key = BlockfileKey::new("prefix".to_string(), Key::String("b".to_string()));
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_1);
    }
}
