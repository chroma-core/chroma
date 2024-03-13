use crate::blockstore::types::BlockfileKey;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use uuid::Uuid;

/// A sentinel blockfilekey wrapper to represent the start blocks range
/// # Note
/// The start key is used to represent the first block in the sparse index, this makes
/// it easier to handle the case where the first block is split into two and also makes
/// determining the target block for a given key easier
#[derive(Clone, Debug)]
enum SparseIndexDelimiter {
    Start,
    Key(BlockfileKey),
}

impl PartialEq for SparseIndexDelimiter {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SparseIndexDelimiter::Start, SparseIndexDelimiter::Start) => true,
            (SparseIndexDelimiter::Key(k1), SparseIndexDelimiter::Key(k2)) => k1 == k2,
            _ => false,
        }
    }
}

impl Eq for SparseIndexDelimiter {}

impl PartialOrd for SparseIndexDelimiter {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (SparseIndexDelimiter::Start, SparseIndexDelimiter::Start) => {
                Some(std::cmp::Ordering::Equal)
            }
            (SparseIndexDelimiter::Start, SparseIndexDelimiter::Key(_)) => {
                Some(std::cmp::Ordering::Less)
            }
            (SparseIndexDelimiter::Key(_), SparseIndexDelimiter::Start) => {
                Some(std::cmp::Ordering::Greater)
            }
            (SparseIndexDelimiter::Key(k1), SparseIndexDelimiter::Key(k2)) => k1.partial_cmp(k2),
        }
    }
}

impl Ord for SparseIndexDelimiter {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (SparseIndexDelimiter::Start, SparseIndexDelimiter::Start) => std::cmp::Ordering::Equal,
            (SparseIndexDelimiter::Start, SparseIndexDelimiter::Key(_)) => std::cmp::Ordering::Less,
            (SparseIndexDelimiter::Key(_), SparseIndexDelimiter::Start) => {
                std::cmp::Ordering::Greater
            }
            (SparseIndexDelimiter::Key(k1), SparseIndexDelimiter::Key(k2)) => k1.cmp(k2),
        }
    }
}

/// A sparse index is used by a Blockfile to map a range of keys to a block id
/// # Methods
/// - `new` - Create a new sparse index with a single block
/// - `from` - Create a new sparse index from an existing sparse index
/// - `get_target_block_id` - Get the block id for a given key
/// - `add_block` - Add a new block to the sparse index
/// - `replace_block` - Replace an existing block with a new one
/// - `len` - Get the number of blocks in the sparse index
/// - `is_valid` - Check if the sparse index is valid, useful for debugging and testing
pub(super) struct SparseIndex {
    forward: BTreeMap<SparseIndexDelimiter, Uuid>,
    reverse: HashMap<Uuid, SparseIndexDelimiter>,
}

impl SparseIndex {
    pub(super) fn new(initial_block_id: Uuid) -> Self {
        let mut forward = BTreeMap::new();
        forward.insert(SparseIndexDelimiter::Start, initial_block_id);
        let mut reverse = HashMap::new();
        reverse.insert(initial_block_id, SparseIndexDelimiter::Start);
        Self { forward, reverse }
    }

    pub(super) fn from(old_sparse_index: &SparseIndex) -> Self {
        Self {
            forward: old_sparse_index.forward.clone(),
            reverse: old_sparse_index.reverse.clone(),
        }
    }

    pub(super) fn get_target_block_id(&self, search_key: &BlockfileKey) -> Uuid {
        let mut iter_curr = self.forward.iter();
        let mut iter_next = self.forward.iter().skip(1);
        let search_key = SparseIndexDelimiter::Key(search_key.clone());
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
            .insert(SparseIndexDelimiter::Key(start_key.clone()), block_id);
        self.reverse
            .insert(block_id, SparseIndexDelimiter::Key(start_key));
    }

    pub(super) fn replace_block(
        &mut self,
        old_block_id: Uuid,
        new_block_id: Uuid,
        new_start_key: BlockfileKey,
    ) {
        if let Some(old_start_key) = self.reverse.remove(&old_block_id) {
            self.forward.remove(&old_start_key);
            if old_start_key == SparseIndexDelimiter::Start {
                self.forward
                    .insert(SparseIndexDelimiter::Start, new_block_id);
            } else {
                self.forward
                    .insert(SparseIndexDelimiter::Key(new_start_key), new_block_id);
            }
        }
    }

    pub(super) fn len(&self) -> usize {
        self.forward.len()
    }

    /// Check if the sparse index is valid by ensuring that the keys are in order
    pub(super) fn is_valid(&self) -> bool {
        let mut first = true;
        // Two pointer traversal to check if the keys are in order and that the start key is first
        let mut iter_slow = self.forward.iter();
        let mut iter_fast = self.forward.iter().skip(1);
        while let Some((curr_key, _)) = iter_slow.next() {
            if first {
                if curr_key != &SparseIndexDelimiter::Start {
                    return false;
                }
                first = false;
            }
            if let Some((next_key, _)) = iter_fast.next() {
                if curr_key >= next_key {
                    return false;
                }
            }
        }
        true
    }

    /// An iterator over the block uuids in the sparse index
    pub(super) fn block_ids(&self) -> impl Iterator<Item = &Uuid> {
        self.forward.values()
    }
}

impl Debug for SparseIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SparseIndex {{")?;
        for (k, v) in self.forward.iter() {
            write!(f, "\n  {:?} -> {:?}", k, v)?;
        }
        write!(f, "\n}}")
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
