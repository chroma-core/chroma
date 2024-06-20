use crate::blockstore::key::{CompositeKey, KeyWrapper};
use crate::errors::ChromaError;
use core::panic;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::Arc;
use uuid::Uuid;

use super::block::delta::BlockDelta;
use super::block::{self, Block};
use super::provider::BlockManager;
use super::types::{ArrowReadableKey, ArrowWriteableKey, ArrowWriteableValue};

/// A sentinel blockfilekey wrapper to represent the start blocks range
/// # Note
/// The start key is used to represent the first block in the sparse index, this makes
/// it easier to handle the case where the first block is split into two and also makes
/// determining the target block for a given key easier
#[derive(Clone, Debug)]
pub(super) enum SparseIndexDelimiter {
    Start,
    Key(CompositeKey),
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
#[derive(Clone)]
pub(super) struct SparseIndex {
    pub(super) forward: Arc<Mutex<BTreeMap<SparseIndexDelimiter, Uuid>>>,
    reverse: Arc<Mutex<HashMap<Uuid, SparseIndexDelimiter>>>,
    pub(super) id: Uuid,
}

impl SparseIndex {
    pub(super) fn new(id: Uuid) -> Self {
        let mut forward = Arc::new(Mutex::new(BTreeMap::new()));
        let mut reverse = Arc::new(Mutex::new(HashMap::new()));
        Self {
            forward,
            reverse,
            id,
        }
    }

    // TOOD: Add state to ensure that we add this first and only once
    pub(super) fn add_initial_block(&self, block_id: Uuid) {
        let mut forward = self.forward.lock();
        forward.insert(SparseIndexDelimiter::Start, block_id);
        let mut reverse = self.reverse.lock();
        reverse.insert(block_id, SparseIndexDelimiter::Start);
    }

    pub(super) fn get_target_block_id(&self, search_key: &CompositeKey) -> Uuid {
        let forward = self.forward.lock();
        let mut iter_curr = forward.iter();
        let mut iter_next = forward.iter().skip(1);
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

    pub(super) fn get_block_ids_prefix(&self, prefix: &str) -> Vec<Uuid> {
        let lock_guard = self.forward.lock();
        let mut curr_iter = lock_guard.iter();
        let mut next_iter = lock_guard.iter().skip(1);
        let mut block_ids = vec![];
        while let Some((curr_key, curr_uuid)) = curr_iter.next() {
            let non_start_curr_key: Option<&CompositeKey>;
            match curr_key {
                SparseIndexDelimiter::Start => non_start_curr_key = None,
                SparseIndexDelimiter::Key(k) => non_start_curr_key = Some(k),
            }
            if let Some((next_key, _)) = next_iter.next() {
                // This can't be a start key but we still need to extract it.
                let non_start_next_key: Option<&CompositeKey>;
                match next_key {
                    SparseIndexDelimiter::Start => {
                        panic!("Invariant violation. Sparse index is not valid.");
                    }
                    SparseIndexDelimiter::Key(k) => non_start_next_key = Some(k),
                }
                // If delimeter starts with the same prefix then there will be keys inside the
                // block with this prefix.
                if non_start_curr_key.is_some()
                    && prefix == non_start_curr_key.unwrap().prefix.as_str()
                {
                    block_ids.push(*curr_uuid);
                }
                // If prefix is between the current delim and next delim then there could
                // be keys in this block that have this prefix.
                if (non_start_curr_key.is_none()
                    || prefix > non_start_curr_key.unwrap().prefix.as_str())
                    && (prefix <= non_start_next_key.unwrap().prefix.as_str())
                {
                    block_ids.push(*curr_uuid);
                }
            } else {
                // Last block.
                if non_start_curr_key.is_none()
                    || prefix >= non_start_curr_key.unwrap().prefix.as_str()
                {
                    block_ids.push(*curr_uuid);
                }
            }
        }
        block_ids
    }

    pub(super) fn get_block_ids_gt<'a, K: ArrowReadableKey<'a> + Into<KeyWrapper>>(
        &self,
        prefix: &str,
        key: K,
    ) -> Vec<Uuid> {
        let lock_guard = self.forward.lock();
        let mut curr_iter = lock_guard.iter();
        let mut next_iter = lock_guard.iter().skip(1);
        let mut block_ids = vec![];
        while let Some((curr_delim, curr_uuid)) = curr_iter.next() {
            let curr_key = match curr_delim {
                SparseIndexDelimiter::Start => None,
                SparseIndexDelimiter::Key(k) => Some(k),
            };
            let mut next_key: Option<&CompositeKey> = None;
            if let Some((next_delim, _)) = next_iter.next() {
                next_key = match next_delim {
                    SparseIndexDelimiter::Start => {
                        panic!("Invariant violation. Sparse index is not valid.")
                    }
                    SparseIndexDelimiter::Key(k) => Some(k),
                };
            }
            if curr_key.is_none() || curr_key.unwrap().prefix.as_str() < prefix {
                if next_key.is_none() || next_key.unwrap().prefix.as_str() >= prefix {
                    block_ids.push(*curr_uuid);
                }
            }
            if curr_key.is_some() && curr_key.unwrap().prefix.as_str() == prefix {
                if curr_key.unwrap().key > key.clone().into() {
                    block_ids.push(*curr_uuid);
                } else {
                    if next_key.is_none() || next_key.unwrap().key > key.clone().into() {
                        block_ids.push(*curr_uuid);
                    }
                }
            }
        }
        block_ids
    }

    pub(super) fn get_block_ids_lt<'a, K: ArrowReadableKey<'a> + Into<KeyWrapper>>(
        &self,
        prefix: &str,
        key: K,
    ) -> Vec<Uuid> {
        let lock_guard = self.forward.lock();
        let mut curr_iter = lock_guard.iter();
        let mut next_iter = lock_guard.iter().skip(1);
        let mut block_ids = vec![];
        while let Some((curr_delim, curr_uuid)) = curr_iter.next() {
            let curr_key = match curr_delim {
                SparseIndexDelimiter::Start => None,
                SparseIndexDelimiter::Key(k) => Some(k),
            };
            let mut next_key: Option<&CompositeKey> = None;
            if let Some((next_delim, _)) = next_iter.next() {
                next_key = match next_delim {
                    SparseIndexDelimiter::Start => {
                        panic!("Invariant violation. Sparse index is not valid.")
                    }
                    SparseIndexDelimiter::Key(k) => Some(k),
                };
            }
            if curr_key.is_none() || curr_key.unwrap().prefix.as_str() < prefix {
                if next_key.is_none() || next_key.unwrap().prefix.as_str() >= prefix {
                    block_ids.push(*curr_uuid);
                }
            }
            if curr_key.is_some() && curr_key.unwrap().prefix.as_str() == prefix {
                if curr_key.unwrap().key < key.clone().into() {
                    block_ids.push(*curr_uuid);
                }
            }
        }
        block_ids
    }

    pub(super) fn get_block_ids_gte<'a, K: ArrowReadableKey<'a> + Into<KeyWrapper>>(
        &self,
        prefix: &str,
        key: K,
    ) -> Vec<Uuid> {
        let lock_guard = self.forward.lock();
        let mut curr_iter = lock_guard.iter();
        let mut next_iter = lock_guard.iter().skip(1);
        let mut block_ids = vec![];
        while let Some((curr_delim, curr_uuid)) = curr_iter.next() {
            let curr_key = match curr_delim {
                SparseIndexDelimiter::Start => None,
                SparseIndexDelimiter::Key(k) => Some(k),
            };
            let mut next_key: Option<&CompositeKey> = None;
            if let Some((next_delim, _)) = next_iter.next() {
                next_key = match next_delim {
                    SparseIndexDelimiter::Start => {
                        panic!("Invariant violation. Sparse index is not valid.")
                    }
                    SparseIndexDelimiter::Key(k) => Some(k),
                };
            }
            if curr_key.is_none() || curr_key.unwrap().prefix.as_str() < prefix {
                if next_key.is_none() || next_key.unwrap().prefix.as_str() >= prefix {
                    block_ids.push(*curr_uuid);
                }
            }
            if curr_key.is_some() && curr_key.unwrap().prefix.as_str() == prefix {
                if curr_key.unwrap().key >= key.clone().into() {
                    block_ids.push(*curr_uuid);
                } else {
                    if next_key.is_none() || next_key.unwrap().key >= key.clone().into() {
                        block_ids.push(*curr_uuid);
                    }
                }
            }
        }
        block_ids
    }

    pub(super) fn get_block_ids_lte<'a, K: ArrowReadableKey<'a> + Into<KeyWrapper>>(
        &self,
        prefix: &str,
        key: K,
    ) -> Vec<Uuid> {
        let lock_guard = self.forward.lock();
        let mut curr_iter = lock_guard.iter();
        let mut next_iter = lock_guard.iter().skip(1);
        let mut block_ids = vec![];
        while let Some((curr_delim, curr_uuid)) = curr_iter.next() {
            let curr_key = match curr_delim {
                SparseIndexDelimiter::Start => None,
                SparseIndexDelimiter::Key(k) => Some(k),
            };
            let mut next_key: Option<&CompositeKey> = None;
            if let Some((next_delim, _)) = next_iter.next() {
                next_key = match next_delim {
                    SparseIndexDelimiter::Start => {
                        panic!("Invariant violation. Sparse index is not valid.")
                    }
                    SparseIndexDelimiter::Key(k) => Some(k),
                };
            }
            if curr_key.is_none() || curr_key.unwrap().prefix.as_str() < prefix {
                if next_key.is_none() || next_key.unwrap().prefix.as_str() >= prefix {
                    block_ids.push(*curr_uuid);
                }
            }
            if curr_key.is_some() && curr_key.unwrap().prefix.as_str() == prefix {
                if curr_key.unwrap().key <= key.clone().into() {
                    block_ids.push(*curr_uuid);
                }
            }
        }
        block_ids
    }

    pub(super) fn add_block(&self, start_key: CompositeKey, block_id: Uuid) {
        self.forward
            .lock()
            .insert(SparseIndexDelimiter::Key(start_key.clone()), block_id);
        self.reverse
            .lock()
            .insert(block_id, SparseIndexDelimiter::Key(start_key));
    }

    pub(super) fn replace_block(
        &self,
        old_block_id: Uuid,
        new_block_id: Uuid,
        new_start_key: CompositeKey,
    ) {
        let mut forward = self.forward.lock();
        let mut reverse = self.reverse.lock();
        if let Some(old_start_key) = reverse.remove(&old_block_id) {
            forward.remove(&old_start_key);
            if old_start_key == SparseIndexDelimiter::Start {
                forward.insert(SparseIndexDelimiter::Start, new_block_id);
            } else {
                forward.insert(SparseIndexDelimiter::Key(new_start_key), new_block_id);
            }
        }
    }

    pub(super) fn len(&self) -> usize {
        self.forward.lock().len()
    }

    pub(super) fn fork(&self, new_id: Uuid) -> Self {
        let mut new_forward = BTreeMap::new();
        let mut new_reverse = HashMap::new();
        let old_forward = self.forward.lock();
        for (key, block_id) in old_forward.iter() {
            new_forward.insert(key.clone(), *block_id);
            new_reverse.insert(*block_id, key.clone());
        }
        Self {
            forward: Arc::new(Mutex::new(new_forward)),
            reverse: Arc::new(Mutex::new(new_reverse)),
            id: new_id,
        }
    }

    /// Check if the sparse index is valid by ensuring that the keys are in order
    pub(super) fn is_valid(&self) -> bool {
        let forward = self.forward.lock();
        let mut first = true;
        // Two pointer traversal to check if the keys are in order and that the start key is first
        let mut iter_slow = forward.iter();
        let mut iter_fast = forward.iter().skip(1);
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

    pub(super) fn to_block<K: ArrowWriteableKey>(&self) -> Result<Block, Box<dyn ChromaError>> {
        let forward = self.forward.lock();
        if forward.is_empty() {
            // TODO: error here
            panic!("No blocks in the sparse index");
        }

        // TODO: we could save the uuid not as a string to be more space efficient
        // but given the scale is relatively small, this is fine for now
        let delta = BlockDelta::new::<K, &str>(self.id);
        for (key, block_id) in forward.iter() {
            match key {
                SparseIndexDelimiter::Start => {
                    delta.add("START", K::default(), block_id.to_string().as_str());
                }
                SparseIndexDelimiter::Key(k) => match &k.key {
                    KeyWrapper::String(s) => {
                        delta.add(&k.prefix, s.as_str(), block_id.to_string().as_str());
                    }
                    KeyWrapper::Float32(f) => {
                        delta.add(&k.prefix, *f, block_id.to_string().as_str());
                    }
                    KeyWrapper::Bool(b) => {
                        unimplemented!();
                        // delta.add("KEY", b, block_id.to_string().as_str());
                    }
                    KeyWrapper::Uint32(u) => {
                        delta.add(&k.prefix, *u, block_id.to_string().as_str());
                    }
                },
            }
        }

        let record_batch = delta.finish::<K, &str>();
        Ok(Block::from_record_batch(delta.id, record_batch))
    }

    pub(super) fn from_block<'block, K: ArrowReadableKey<'block> + 'block>(
        block: &'block Block,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let mut forward = BTreeMap::new();
        let mut reverse = HashMap::new();
        let id = block.id;
        let mut i = 0;
        while let Some((prefix, key, value)) = block.get_at_index::<K, &str>(i) {
            let (delimiter, block_id) = match prefix {
                "START" => {
                    let block_id = Uuid::parse_str(value);
                    match block_id {
                        Ok(block_id) => (SparseIndexDelimiter::Start, block_id),
                        Err(e) => panic!("Failed to parse block id: {}", e), // TODO: error here
                    }
                }
                _ => {
                    let block_id = Uuid::parse_str(value);
                    match block_id {
                        Ok(block_id) => (
                            SparseIndexDelimiter::Key(CompositeKey::new(prefix.to_string(), key)),
                            block_id,
                        ),
                        Err(e) => panic!("Failed to parse block id: {}", e), // TODO: error here
                    }
                }
            };
            forward.insert(delimiter.clone(), block_id);
            reverse.insert(block_id, delimiter);
            i += 1;
        }
        Ok(Self {
            forward: Arc::new(Mutex::new(forward)),
            reverse: Arc::new(Mutex::new(reverse)),
            id,
        })
    }
}

impl Debug for SparseIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let forward = self.forward.lock();
        let reverse = self.reverse.lock();
        write!(
            f,
            "SparseIndex {{ id: {}, forward: {:?}, reverse: {:?} }}",
            self.id, forward, reverse
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sparse_index() {
        let file_id = uuid::Uuid::new_v4();
        let block_id_1 = uuid::Uuid::new_v4();
        let mut sparse_index = SparseIndex::new(file_id);
        sparse_index.add_initial_block(block_id_1);
        let mut blockfile_key = CompositeKey::new("prefix".to_string(), "a");
        sparse_index.add_block(blockfile_key.clone(), block_id_1);
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_1);

        blockfile_key = CompositeKey::new("prefix".to_string(), "b");
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_1);

        // Split the range into two blocks (start, c), and (c, end)
        let block_id_2 = uuid::Uuid::new_v4();
        blockfile_key = CompositeKey::new("prefix".to_string(), "c");
        sparse_index.add_block(blockfile_key.clone(), block_id_2);
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_2);

        // d should fall into the second block
        blockfile_key = CompositeKey::new("prefix".to_string(), "d");
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_2);

        // Split the second block into (c, f) and (f, end)
        let block_id_3 = uuid::Uuid::new_v4();
        blockfile_key = CompositeKey::new("prefix".to_string(), "f");
        sparse_index.add_block(blockfile_key.clone(), block_id_3);
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_3);

        // g should fall into the third block
        blockfile_key = CompositeKey::new("prefix".to_string(), "g");
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_3);

        // b should fall into the first block
        blockfile_key = CompositeKey::new("prefix".to_string(), "b");
        assert_eq!(sparse_index.get_target_block_id(&blockfile_key), block_id_1);
    }

    #[test]
    fn test_to_from_block() {
        let file_id = uuid::Uuid::new_v4();
        let block_id_0 = uuid::Uuid::new_v4();

        // Add an initial block to the sparse index
        let sparse_index = SparseIndex::new(file_id);
        sparse_index.add_initial_block(block_id_0);

        // Add some more blocks
        let blockfile_key = CompositeKey::new("prefix".to_string(), "a");
        let block_id_1 = uuid::Uuid::new_v4();
        sparse_index.add_block(blockfile_key.clone(), block_id_1);

        let blockfile_key = CompositeKey::new("prefix".to_string(), "c");
        let block_id_2 = uuid::Uuid::new_v4();
        sparse_index.add_block(blockfile_key.clone(), block_id_2);

        let block = sparse_index.to_block::<&str>().unwrap();
        let new_sparse_index = SparseIndex::from_block::<&str>(&block).unwrap();

        let old_forward = sparse_index.forward.lock();
        let new_forward = new_sparse_index.forward.lock();

        assert_eq!(old_forward.len(), new_forward.len());
        for (old_key, old_block_id) in old_forward.iter() {
            let new_block_id = new_forward.get(old_key).unwrap();
            assert_eq!(old_block_id, new_block_id);
        }

        let old_reverse = sparse_index.reverse.lock();
        let new_reverse = new_sparse_index.reverse.lock();

        assert_eq!(old_reverse.len(), new_reverse.len());
        for (old_block_id, old_key) in old_reverse.iter() {
            let new_key = new_reverse.get(old_block_id).unwrap();
            assert_eq!(old_key, new_key);
        }
    }
}
