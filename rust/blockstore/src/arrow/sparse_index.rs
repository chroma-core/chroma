use crate::key::{CompositeKey, KeyWrapper};
use chroma_error::ChromaError;
use core::panic;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::Arc;
use uuid::Uuid;

use super::block::delta::BlockDelta;
use super::block::Block;
use super::types::{ArrowReadableKey, ArrowWriteableKey};

/// A sentinel blockfilekey wrapper to represent the start blocks range
/// # Note
/// The start key is used to represent the first block in the sparse index, this makes
/// it easier to handle the case where the first block is split into two and also makes
/// determining the target block for a given key easier
#[derive(Clone, Debug, Serialize, Deserialize)]
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
        Some(self.cmp(other))
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
#[derive(Clone, Serialize, Deserialize)]
pub struct SparseIndex {
    pub(super) data: Arc<Mutex<SparseIndexData>>,
    pub(super) id: Uuid,
}

/// The data structures that hold the sparse index
/// in memory.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct SparseIndexData {
    pub(super) forward: BTreeMap<SparseIndexDelimiter, Uuid>,
    reverse: HashMap<Uuid, SparseIndexDelimiter>,
}

impl SparseIndexData {
    pub(super) fn len(&self) -> usize {
        self.forward.len()
    }
}

impl SparseIndex {
    pub(super) fn new(id: Uuid) -> Self {
        let forward = BTreeMap::new();
        let reverse = HashMap::new();
        let data = SparseIndexData { forward, reverse };
        Self {
            data: Arc::new(Mutex::new(data)),
            id,
        }
    }

    // TOOD: Add state to ensure that we add this first and only once
    pub(super) fn add_initial_block(&self, block_id: Uuid) {
        let mut data = self.data.lock();
        let forward = &mut data.forward;
        forward.insert(SparseIndexDelimiter::Start, block_id);
        let reverse = &mut data.reverse;
        reverse.insert(block_id, SparseIndexDelimiter::Start);
    }

    pub(super) fn get_all_target_block_ids(&self, mut search_keys: Vec<CompositeKey>) -> Vec<Uuid> {
        // Sort so that we can search in one iteration.
        let data = self.data.lock();
        let forward = &data.forward;
        search_keys.sort();
        let mut result_uuids = Vec::new();
        let curr_iter = forward.iter();
        let mut next_iter = forward.iter().skip(1);
        let mut search_iter = search_keys.iter().peekable();
        for (curr_key, curr_block_id) in curr_iter {
            let search_key = match search_iter.peek() {
                Some(key) => SparseIndexDelimiter::Key((**key).clone()),
                None => {
                    break;
                }
            };
            if let Some((next_key, _)) = next_iter.next() {
                if search_key >= *curr_key && search_key < *next_key {
                    result_uuids.push(*curr_block_id);
                    // Move forward all search keys that match this block.
                    search_iter.next();
                    while let Some(key) = search_iter.peek() {
                        let search_key = SparseIndexDelimiter::Key((**key).clone());
                        if search_key >= *curr_key && search_key < *next_key {
                            search_iter.next();
                        } else {
                            break;
                        }
                    }
                }
            } else {
                // last block. All the remaining keys should be satisfied by this.
                result_uuids.push(*curr_block_id);
                break;
            }
        }
        result_uuids
    }

    pub(super) fn get_target_block_id(&self, search_key: &CompositeKey) -> Uuid {
        let data = self.data.lock();
        let forward = &data.forward;

        match forward
            .range(..=SparseIndexDelimiter::Key(search_key.clone()))
            .next_back()
        {
            Some((_, block_id)) => *block_id,
            None => {
                panic!("No blocks in the sparse index");
            }
        }
    }

    pub(super) fn get_block_ids_prefix(&self, prefix: &str) -> Vec<Uuid> {
        let data = self.data.lock();
        let forward = &data.forward;
        let curr_iter = forward.iter();
        let mut next_iter = forward.iter().skip(1);
        let mut block_ids = vec![];
        for (curr_key, curr_uuid) in curr_iter {
            let non_start_curr_key: Option<&CompositeKey> = match curr_key {
                SparseIndexDelimiter::Start => None,
                SparseIndexDelimiter::Key(k) => Some(k),
            };
            if let Some((next_key, _)) = next_iter.next() {
                // This can't be a start key but we still need to extract it.
                let non_start_next_key: Option<&CompositeKey> = match next_key {
                    SparseIndexDelimiter::Start => {
                        panic!("Invariant violation. Sparse index is not valid.");
                    }
                    SparseIndexDelimiter::Key(k) => Some(k),
                };
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
        let data = self.data.lock();
        let forward = &data.forward;
        let curr_iter = forward.iter();
        let mut next_iter = forward.iter().skip(1);
        let mut block_ids = vec![];
        for (curr_delim, curr_uuid) in curr_iter {
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
            if (curr_key.is_none() || curr_key.unwrap().prefix.as_str() < prefix)
                && (next_key.is_none() || next_key.unwrap().prefix.as_str() >= prefix)
            {
                block_ids.push(*curr_uuid);
            }
            if let Some(curr_key) = curr_key {
                if (curr_key.key > key.clone().into())
                    || next_key.is_none()
                    || next_key.unwrap().key > key.clone().into()
                {
                    block_ids.push(*curr_uuid);
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
        let data = self.data.lock();
        let forward = &data.forward;
        let curr_iter = forward.iter();
        let mut next_iter = forward.iter().skip(1);
        let mut block_ids = vec![];
        for (curr_delim, curr_uuid) in curr_iter {
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
            if (curr_key.is_none() || curr_key.unwrap().prefix.as_str() < prefix)
                && (next_key.is_none() || next_key.unwrap().prefix.as_str() >= prefix)
            {
                block_ids.push(*curr_uuid);
            }
            if let Some(curr_key) = curr_key {
                if curr_key.prefix.as_str() == prefix && curr_key.key < key.clone().into() {
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
        let data = self.data.lock();
        let forward = &data.forward;
        let curr_iter = forward.iter();
        let mut next_iter = forward.iter().skip(1);
        let mut block_ids = vec![];
        for (curr_delim, curr_uuid) in curr_iter {
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
            if (curr_key.is_none() || curr_key.unwrap().prefix.as_str() < prefix)
                && (next_key.is_none() || next_key.unwrap().prefix.as_str() >= prefix)
            {
                block_ids.push(*curr_uuid);
            }
            if let Some(curr_key) = curr_key {
                if curr_key.key >= key.clone().into()
                    || next_key.is_none()
                    || next_key.unwrap().key >= key.clone().into()
                {
                    block_ids.push(*curr_uuid);
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
        let data = self.data.lock();
        let forward = &data.forward;
        let curr_iter = forward.iter();
        let mut next_iter = forward.iter().skip(1);
        let mut block_ids = vec![];
        for (curr_delim, curr_uuid) in curr_iter {
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
            if (curr_key.is_none() || curr_key.unwrap().prefix.as_str() < prefix)
                && (next_key.is_none() || next_key.unwrap().prefix.as_str() >= prefix)
            {
                block_ids.push(*curr_uuid);
            }
            if let Some(curr_key) = curr_key {
                if curr_key.prefix.as_str() == prefix && curr_key.key <= key.clone().into() {
                    block_ids.push(*curr_uuid);
                }
            }
        }
        block_ids
    }

    pub(super) fn add_block(&self, start_key: CompositeKey, block_id: Uuid) {
        let mut data = self.data.lock();
        data.forward
            .insert(SparseIndexDelimiter::Key(start_key.clone()), block_id);
        data.reverse
            .insert(block_id, SparseIndexDelimiter::Key(start_key));
    }

    pub(super) fn replace_block(&self, old_block_id: Uuid, new_block_id: Uuid) {
        let mut data = self.data.lock();
        if let Some(old_start_key) = data.reverse.remove(&old_block_id) {
            data.forward.remove(&old_start_key);
            data.forward.insert(old_start_key.clone(), new_block_id);
            data.reverse.insert(new_block_id, old_start_key);
        }
    }

    fn correct_start_key(&self, data: &mut SparseIndexData) {
        if data.len() == 0 {
            return;
        }
        let key_copy;
        {
            let mut curr_iter = data.forward.iter();
            let (key, _) = curr_iter.nth(0).unwrap();
            if key == &SparseIndexDelimiter::Start {
                return;
            }
            key_copy = key.clone();
        }
        tracing::info!("Correcting start key of sparse index {:?}", self.id);
        if let Some(id) = data.forward.remove(&key_copy) {
            data.reverse.remove(&id);
            data.forward.insert(SparseIndexDelimiter::Start, id);
            data.reverse.insert(id, SparseIndexDelimiter::Start);
        }
    }

    pub(super) fn remove_block(&self, block_id: &Uuid) -> bool {
        // We commit and flush an empty dummy block if the blockfile is empty.
        // It can happen that other indexes of the segment are not empty. In this case,
        // our segment open() logic breaks down since we only handle either
        // all indexes initialized or none at all but not other combinations.
        // We could argue that we should fix the readers to handle these cases
        // but this is simpler, easier and less error prone to do.
        let mut data = self.data.lock();
        let mut removed = false;
        if data.len() > 1 {
            if let Some(start_key) = data.reverse.remove(block_id) {
                data.forward.remove(&start_key);
            }
            removed = true;
        }
        // It can happen that the sparse index does not contain
        // the start key after this sequence of operations,
        // for e.g. consider the following:
        // sparse_index: {start_key: block_id1, some_key: block_id2, some_other_key: block_id3}
        // If we delete block_id1 from the sparse index then it becomes
        // {some_key: block_id2, some_other_key: block_id3}
        // This should be changed to {start_key: block_id2, some_other_key: block_id3}
        self.correct_start_key(&mut data);
        removed
    }

    pub(super) fn len(&self) -> usize {
        let data = self.data.lock();
        data.forward.len()
    }

    pub(super) fn fork(&self, new_id: Uuid) -> Self {
        let mut new_forward = BTreeMap::new();
        let mut new_reverse = HashMap::new();
        let old_data = self.data.lock();
        let old_forward = &old_data.forward;
        for (key, block_id) in old_forward.iter() {
            new_forward.insert(key.clone(), *block_id);
            new_reverse.insert(*block_id, key.clone());
        }
        Self {
            data: Arc::new(Mutex::new(SparseIndexData {
                forward: new_forward,
                reverse: new_reverse,
            })),
            id: new_id,
        }
    }

    #[cfg(test)]
    /// Check if the sparse index is valid by ensuring that the keys are in order
    pub(super) fn is_valid(&self) -> bool {
        let data = self.data.lock();
        let mut first = true;
        // Two pointer traversal to check if the keys are in order and that the start key is first
        let iter_slow = data.forward.iter();
        let mut iter_fast = data.forward.iter().skip(1);
        for (curr_key, _) in iter_slow {
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

    pub(super) fn to_delta<K: ArrowWriteableKey>(
        &self,
    ) -> Result<BlockDelta, Box<dyn ChromaError>> {
        let data = self.data.lock();
        if data.forward.is_empty() {
            panic!("Invariant violation. No blocks in the sparse index");
        }

        // TODO: we could save the uuid not as a string to be more space efficient
        // but given the scale is relatively small, this is fine for now
        let delta = BlockDelta::new::<K, String>(self.id);
        for (key, block_id) in data.forward.iter() {
            match key {
                SparseIndexDelimiter::Start => {
                    delta.add("START", K::default(), block_id.to_string());
                }
                SparseIndexDelimiter::Key(k) => match &k.key {
                    KeyWrapper::String(s) => {
                        delta.add(&k.prefix, s.as_str(), block_id.to_string());
                    }
                    KeyWrapper::Float32(f) => {
                        delta.add(&k.prefix, *f, block_id.to_string());
                    }
                    KeyWrapper::Bool(b) => {
                        delta.add(&k.prefix, *b, block_id.to_string());
                    }
                    KeyWrapper::Uint32(u) => {
                        delta.add(&k.prefix, *u, block_id.to_string());
                    }
                },
            }
        }
        Ok(delta)
    }

    pub(super) fn from_block<'block, K: ArrowReadableKey<'block> + 'block>(
        block: &'block Block,
    ) -> Result<Self, uuid::Error> {
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
                        Err(e) => return Err(e),
                    }
                }
                _ => {
                    let block_id = Uuid::parse_str(value);
                    match block_id {
                        Ok(block_id) => (
                            SparseIndexDelimiter::Key(CompositeKey::new(prefix.to_string(), key)),
                            block_id,
                        ),
                        Err(e) => return Err(e),
                    }
                }
            };
            forward.insert(delimiter.clone(), block_id);
            reverse.insert(block_id, delimiter);
            i += 1;
        }
        Ok(Self {
            data: Arc::new(Mutex::new(SparseIndexData { forward, reverse })),
            id,
        })
    }
}

impl Debug for SparseIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self.data.lock();
        f.debug_struct("SparseIndex")
            .field("id", &self.id)
            .field("data", &data)
            .finish()
    }
}

impl chroma_cache::Weighted for SparseIndex {
    fn weight(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sparse_index() {
        let file_id = uuid::Uuid::new_v4();
        let block_id_1 = uuid::Uuid::new_v4();
        let sparse_index = SparseIndex::new(file_id);
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

        let block_delta = sparse_index.to_delta::<&str>().unwrap();
        let block =
            Block::from_record_batch(block_delta.id, block_delta.finish::<&str, String>(None));
        let new_sparse_index = SparseIndex::from_block::<&str>(&block).unwrap();

        let old_data = sparse_index.data.lock();

        assert_eq!(old_data.forward.len(), old_data.reverse.len());
        for (old_key, old_block_id) in old_data.forward.iter() {
            let new_block_id = old_data.forward.get(old_key).unwrap();
            assert_eq!(old_block_id, new_block_id);
        }

        let old_reverse = &old_data.reverse;
        let new_data = new_sparse_index.data.lock();

        assert_eq!(old_reverse.len(), new_data.reverse.len());
        for (old_block_id, old_key) in old_reverse.iter() {
            let new_key = new_data.reverse.get(old_block_id).unwrap();
            assert_eq!(old_key, new_key);
        }
    }

    #[test]
    fn test_get_all_block_ids() {
        let file_id = uuid::Uuid::new_v4();
        let block_id_1 = uuid::Uuid::new_v4();
        let sparse_index = SparseIndex::new(file_id);
        sparse_index.add_initial_block(block_id_1);
        let mut blockfile_key = CompositeKey::new("prefix".to_string(), "a");
        sparse_index.add_block(blockfile_key.clone(), block_id_1);

        // Split the range into two blocks (start, c), and (c, end)
        let block_id_2 = uuid::Uuid::new_v4();
        blockfile_key = CompositeKey::new("prefix".to_string(), "d");
        sparse_index.add_block(blockfile_key.clone(), block_id_2);

        // Split the second block into (c, f) and (f, end)
        let block_id_3 = uuid::Uuid::new_v4();
        blockfile_key = CompositeKey::new("prefix".to_string(), "f");
        sparse_index.add_block(blockfile_key.clone(), block_id_3);
        let composite_keys = vec![
            CompositeKey::new("prefix".to_string(), "b"),
            CompositeKey::new("prefix".to_string(), "c"),
            CompositeKey::new("prefix".to_string(), "d"),
            CompositeKey::new("prefix".to_string(), "e"),
        ];
        let blocks = sparse_index.get_all_target_block_ids(composite_keys);
        assert_eq!(blocks.len(), 2);
        assert!(blocks.contains(&block_id_1));
        assert!(blocks.contains(&block_id_2));
        let composite_keys = vec![
            CompositeKey::new("prefix".to_string(), "f"),
            CompositeKey::new("prefix".to_string(), "g"),
            CompositeKey::new("prefix".to_string(), "h"),
            CompositeKey::new("prefix".to_string(), "i"),
        ];
        let blocks = sparse_index.get_all_target_block_ids(composite_keys);
        assert_eq!(blocks.len(), 1);
        assert!(blocks.contains(&block_id_3));
    }

    #[test]
    fn test_serde() {
        let sparse_index_id = uuid::Uuid::new_v4();
        let block_id_1 = uuid::Uuid::new_v4();
        let sparse_index = SparseIndex::new(sparse_index_id);
        sparse_index.add_initial_block(block_id_1);
        let mut blockfile_key = CompositeKey::new("prefix".to_string(), "a");
        sparse_index.add_block(blockfile_key.clone(), block_id_1);

        // Split the range into two blocks (start, c), and (c, end)
        let block_id_2 = uuid::Uuid::new_v4();
        blockfile_key = CompositeKey::new("prefix".to_string(), "c");
        sparse_index.add_block(blockfile_key.clone(), block_id_2);

        let serialized = bincode::serialize(&sparse_index).unwrap();
        let deserialized: SparseIndex = bincode::deserialize(&serialized).unwrap();

        let old_data = sparse_index.data.lock();
        let new_data = deserialized.data.lock();
        for (key, block_id) in old_data.forward.iter() {
            assert_eq!(new_data.forward.get(key).unwrap(), block_id);
        }
    }
}
