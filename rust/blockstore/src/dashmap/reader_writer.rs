use std::ops::RangeBounds;
use std::sync::Arc;

use ahash::RandomState;
use chroma_error::ChromaError;
use dashmap::DashMap;
use uuid::Uuid;

use crate::key::{CompositeKey, InvalidKeyConversion, KeyWrapper};
use crate::Key;
use crate::Value;

use super::storage::{FromStoredValue, StorageManager, StoredValue, ToStoredValue};

// Type alias for DashMap with ahash for faster hashing
type FastDashMap<K, V> = DashMap<K, V, RandomState>;

// ============ Writer ============

#[derive(Clone, Debug)]
pub struct DashMapBlockfileWriter {
    storage: Arc<FastDashMap<CompositeKey, StoredValue>>,
    storage_manager: StorageManager,
    id: Uuid,
}

impl DashMapBlockfileWriter {
    pub fn new(storage_manager: StorageManager) -> Self {
        Self {
            storage: Arc::new(DashMap::with_hasher(RandomState::new())),
            storage_manager,
            id: Uuid::new_v4(),
        }
    }

    /// Create a new writer that forks from an existing blockfile.
    /// Copies all data from the existing blockfile into the new writer's DashMap.
    pub fn fork_from(storage_manager: StorageManager, fork_from: &Uuid) -> Option<Self> {
        let existing_data = storage_manager.get(fork_from)?;

        let storage = DashMap::with_hasher(RandomState::new());
        for (key, value) in existing_data.iter() {
            storage.insert(key.clone(), value.clone());
        }

        Some(Self {
            storage: Arc::new(storage),
            storage_manager,
            id: Uuid::new_v4(),
        })
    }

    pub fn set<K: Key + Into<KeyWrapper>, V: Value + ToStoredValue>(
        &self,
        prefix: &str,
        key: K,
        value: V,
    ) -> Result<(), Box<dyn ChromaError>> {
        let composite_key = CompositeKey::new(prefix.to_string(), key);
        self.storage.insert(composite_key, value.to_stored_value());
        Ok(())
    }

    pub fn delete<K: Key + Into<KeyWrapper>, V: Value>(
        &self,
        prefix: &str,
        key: K,
    ) -> Result<(), Box<dyn ChromaError>> {
        let composite_key = CompositeKey::new(prefix.to_string(), key);
        self.storage.remove(&composite_key);
        Ok(())
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Get a value from the writer, returning the PreparedValue form.
    /// This is used during SPANN index construction to read back posting lists.
    pub fn get_owned<K: Key + Into<KeyWrapper>, P: super::storage::PreparedValueFromStoredValue>(
        &self,
        prefix: &str,
        key: K,
    ) -> Result<Option<P>, Box<dyn ChromaError>> {
        let composite_key = CompositeKey::new(prefix.to_string(), key);
        Ok(self
            .storage
            .get(&composite_key)
            .and_then(|entry| P::prepared_from_stored_value(entry.value())))
    }

    /// Commit the writer - drains DashMap into sorted Vec and stores in StorageManager
    pub fn commit(self) -> Result<DashMapBlockfileFlusher, Box<dyn ChromaError>> {
        // Take ownership of Arc and unwrap the DashMap
        let storage = Arc::try_unwrap(self.storage).unwrap_or_else(|arc| {
            // Fallback: clone if other references exist
            arc.iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect::<Vec<_>>()
                .into_iter()
                .collect()
        });

        // Drain DashMap into Vec (no cloning needed)
        let mut data: Vec<(CompositeKey, StoredValue)> = storage.into_iter().collect();

        // Sort by CompositeKey
        data.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        // Store in manager
        self.storage_manager.commit(self.id, data);

        Ok(DashMapBlockfileFlusher { id: self.id })
    }
}

// ============ Reader ============

#[derive(Clone)]
pub struct DashMapBlockfileReader<K: Key, V: Value> {
    storage: Arc<Vec<(CompositeKey, StoredValue)>>,
    id: Uuid,
    _marker: std::marker::PhantomData<(K, V)>,
}

// Methods that don't require FromStoredValue
impl<K: Key, V: Value> DashMapBlockfileReader<K, V> {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn count(&self) -> Result<usize, Box<dyn ChromaError>> {
        Ok(self.storage.len())
    }
}

impl<
        'referred_data,
        K: Key
            + Into<KeyWrapper>
            + TryFrom<&'referred_data KeyWrapper, Error = InvalidKeyConversion>
            + PartialOrd,
        V: Value + FromStoredValue<'referred_data>,
    > DashMapBlockfileReader<K, V>
{
    pub fn open(id: Uuid, storage_manager: StorageManager) -> Option<Self> {
        let storage = storage_manager.get(&id)?;
        Some(Self {
            storage,
            id,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn get(
        &'referred_data self,
        prefix: &str,
        key: K,
    ) -> Result<Option<V>, Box<dyn ChromaError>> {
        let search_key = CompositeKey::new(prefix.to_string(), key);

        // Binary search for the key
        match self.storage.binary_search_by(|(k, _)| k.cmp(&search_key)) {
            Ok(idx) => {
                let (_, stored_value) = &self.storage[idx];
                Ok(V::from_stored_value(stored_value))
            }
            Err(_) => Ok(None),
        }
    }

    pub fn contains(&'referred_data self, prefix: &str, key: K) -> bool {
        let search_key = CompositeKey::new(prefix.to_string(), key);
        self.storage
            .binary_search_by(|(k, _)| k.cmp(&search_key))
            .is_ok()
    }

    /// Get the rank (0-based index) of a key in the sorted storage
    pub fn rank(&'referred_data self, prefix: &str, key: K) -> usize {
        let search_key = CompositeKey::new(prefix.to_string(), key);
        match self.storage.binary_search_by(|(k, _)| k.cmp(&search_key)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        }
    }

    pub fn get_range_iter<'prefix, PrefixRange, KeyRange>(
        &'referred_data self,
        prefix_range: PrefixRange,
        key_range: KeyRange,
    ) -> Result<
        impl Iterator<Item = (&'referred_data str, K, V)> + 'referred_data,
        Box<dyn ChromaError>,
    >
    where
        PrefixRange: RangeBounds<&'prefix str> + 'referred_data,
        KeyRange: RangeBounds<K> + 'referred_data,
    {
        // Find start index using binary search based on prefix range start bound
        let start_idx = match prefix_range.start_bound() {
            std::ops::Bound::Included(prefix) => {
                // Find first entry with prefix >= start_prefix
                self.storage
                    .partition_point(|(k, _)| k.prefix.as_str() < *prefix)
            }
            std::ops::Bound::Excluded(prefix) => {
                // Find first entry with prefix > start_prefix
                self.storage
                    .partition_point(|(k, _)| k.prefix.as_str() <= *prefix)
            }
            std::ops::Bound::Unbounded => 0,
        };

        // Find end index using binary search based on prefix range end bound
        let end_idx = match prefix_range.end_bound() {
            std::ops::Bound::Included(prefix) => {
                // Find first entry with prefix > end_prefix
                self.storage
                    .partition_point(|(k, _)| k.prefix.as_str() <= *prefix)
            }
            std::ops::Bound::Excluded(prefix) => {
                // Find first entry with prefix >= end_prefix
                self.storage
                    .partition_point(|(k, _)| k.prefix.as_str() < *prefix)
            }
            std::ops::Bound::Unbounded => self.storage.len(),
        };

        // Iterate only over the relevant slice and filter by key range
        let iter = self.storage[start_idx..end_idx]
            .iter()
            .filter(move |(composite_key, _)| {
                // Check key range - need to convert KeyWrapper back to K
                if let Ok(k) = K::try_from(&composite_key.key) {
                    key_range.contains(&k)
                } else {
                    false
                }
            })
            .filter_map(|(composite_key, stored_value)| {
                let k = K::try_from(&composite_key.key).ok()?;
                let v = V::from_stored_value(stored_value)?;
                Some((composite_key.prefix.as_str(), k, v))
            });

        Ok(iter)
    }
}

// ============ Flusher ============

pub struct DashMapBlockfileFlusher {
    id: Uuid,
}

impl DashMapBlockfileFlusher {
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// No-op for in-memory implementation
    pub fn flush(self) -> Result<(), Box<dyn ChromaError>> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashmap::storage::StorageManager;
    use std::ops::Bound;

    #[test]
    fn test_string_key_string_value() {
        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();
        writer.set("prefix", "key1", "value1".to_string()).unwrap();
        writer.set("prefix", "key2", "value2".to_string()).unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<&str, &str> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        assert_eq!(reader.get("prefix", "key1").unwrap(), Some("value1"));
        assert_eq!(reader.get("prefix", "key2").unwrap(), Some("value2"));
        assert_eq!(reader.get("prefix", "key3").unwrap(), None);
    }

    #[test]
    fn test_u32_key() {
        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();
        writer.set("prefix", 1u32, "value1".to_string()).unwrap();
        writer.set("prefix", 2u32, "value2".to_string()).unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<u32, &str> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        assert_eq!(reader.get("prefix", 1u32).unwrap(), Some("value1"));
        assert_eq!(reader.get("prefix", 2u32).unwrap(), Some("value2"));
    }

    #[test]
    fn test_f32_key() {
        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();
        writer.set("prefix", 1.0f32, "value1".to_string()).unwrap();
        writer.set("prefix", 2.0f32, "value2".to_string()).unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<f32, &str> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        assert_eq!(reader.get("prefix", 1.0f32).unwrap(), Some("value1"));
        assert_eq!(reader.get("prefix", 2.0f32).unwrap(), Some("value2"));
    }

    #[test]
    fn test_bool_key() {
        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();
        writer
            .set("prefix", true, "value_true".to_string())
            .unwrap();
        writer
            .set("prefix", false, "value_false".to_string())
            .unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<bool, &str> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        assert_eq!(reader.get("prefix", true).unwrap(), Some("value_true"));
        assert_eq!(reader.get("prefix", false).unwrap(), Some("value_false"));
    }

    #[test]
    fn test_contains() {
        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();
        writer.set("prefix", "key1", "value1".to_string()).unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<&str, &str> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        assert!(reader.contains("prefix", "key1"));
        assert!(!reader.contains("prefix", "key2"));
    }

    #[test]
    fn test_count() {
        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();
        writer.set("prefix", "key1", "value1".to_string()).unwrap();
        writer.set("prefix", "key2", "value2".to_string()).unwrap();
        writer.set("other", "key3", "value3".to_string()).unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<&str, &str> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        assert_eq!(reader.count().unwrap(), 3);
    }

    #[test]
    fn test_delete() {
        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();
        writer.set("prefix", "key1", "value1".to_string()).unwrap();
        writer.set("prefix", "key2", "value2".to_string()).unwrap();
        writer.delete::<&str, String>("prefix", "key1").unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<&str, &str> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        assert_eq!(reader.get("prefix", "key1").unwrap(), None);
        assert_eq!(reader.get("prefix", "key2").unwrap(), Some("value2"));
        assert_eq!(reader.count().unwrap(), 1);
    }

    #[test]
    fn test_get_range_by_prefix() {
        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();
        writer
            .set("prefix_a", "key1", "value1".to_string())
            .unwrap();
        writer
            .set("prefix_a", "key2", "value2".to_string())
            .unwrap();
        writer
            .set("prefix_b", "key3", "value3".to_string())
            .unwrap();
        writer
            .set("prefix_c", "key4", "value4".to_string())
            .unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<&str, &str> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        // Get only prefix_a
        let results: Vec<_> = reader
            .get_range_iter("prefix_a"..="prefix_a", ..)
            .unwrap()
            .collect();
        assert_eq!(results.len(), 2);
        assert!(results
            .iter()
            .any(|(_, k, v)| *k == "key1" && *v == "value1"));
        assert!(results
            .iter()
            .any(|(_, k, v)| *k == "key2" && *v == "value2"));
    }

    #[test]
    fn test_get_range_by_key() {
        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();
        writer.set("prefix", 1u32, "value1".to_string()).unwrap();
        writer.set("prefix", 2u32, "value2".to_string()).unwrap();
        writer.set("prefix", 3u32, "value3".to_string()).unwrap();
        writer.set("prefix", 4u32, "value4".to_string()).unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<u32, &str> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        // Get keys >= 2 and < 4
        let results: Vec<_> = reader
            .get_range_iter("prefix"..="prefix", 2u32..4u32)
            .unwrap()
            .collect();
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(_, k, _)| *k == 2));
        assert!(results.iter().any(|(_, k, _)| *k == 3));
    }

    #[test]
    fn test_get_range_gt() {
        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();
        writer.set("prefix", 1u32, "value1".to_string()).unwrap();
        writer.set("prefix", 2u32, "value2".to_string()).unwrap();
        writer.set("prefix", 3u32, "value3".to_string()).unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<u32, &str> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        // Get keys > 1
        let results: Vec<_> = reader
            .get_range_iter(
                "prefix"..="prefix",
                (Bound::Excluded(1u32), Bound::Unbounded),
            )
            .unwrap()
            .collect();
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(_, k, _)| *k == 2));
        assert!(results.iter().any(|(_, k, _)| *k == 3));
    }

    #[test]
    fn test_get_range_lte() {
        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();
        writer.set("prefix", 1u32, "value1".to_string()).unwrap();
        writer.set("prefix", 2u32, "value2".to_string()).unwrap();
        writer.set("prefix", 3u32, "value3".to_string()).unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<u32, &str> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        // Get keys <= 2
        let results: Vec<_> = reader
            .get_range_iter("prefix"..="prefix", ..=2u32)
            .unwrap()
            .collect();
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(_, k, _)| *k == 1));
        assert!(results.iter().any(|(_, k, _)| *k == 2));
    }

    #[test]
    fn test_fork_from() {
        let storage_manager = StorageManager::new();

        // Create initial blockfile
        let writer1 = DashMapBlockfileWriter::new(storage_manager.clone());
        let id1 = writer1.id();
        writer1.set("prefix", "key1", "value1".to_string()).unwrap();
        writer1.set("prefix", "key2", "value2".to_string()).unwrap();
        writer1.commit().unwrap();

        // Fork from first blockfile
        let writer2 = DashMapBlockfileWriter::fork_from(storage_manager.clone(), &id1).unwrap();
        let id2 = writer2.id();

        // Add new key and overwrite existing
        writer2.set("prefix", "key3", "value3".to_string()).unwrap();
        writer2
            .set("prefix", "key1", "updated".to_string())
            .unwrap();
        writer2.commit().unwrap();

        // Verify original is unchanged
        let reader1: DashMapBlockfileReader<&str, &str> =
            DashMapBlockfileReader::open(id1, storage_manager.clone()).unwrap();
        assert_eq!(reader1.get("prefix", "key1").unwrap(), Some("value1"));
        assert_eq!(reader1.get("prefix", "key2").unwrap(), Some("value2"));
        assert_eq!(reader1.get("prefix", "key3").unwrap(), None);
        assert_eq!(reader1.count().unwrap(), 2);

        // Verify forked has all changes
        let reader2: DashMapBlockfileReader<&str, &str> =
            DashMapBlockfileReader::open(id2, storage_manager).unwrap();
        assert_eq!(reader2.get("prefix", "key1").unwrap(), Some("updated"));
        assert_eq!(reader2.get("prefix", "key2").unwrap(), Some("value2"));
        assert_eq!(reader2.get("prefix", "key3").unwrap(), Some("value3"));
        assert_eq!(reader2.count().unwrap(), 3);
    }

    #[test]
    fn test_rank() {
        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();
        writer.set("prefix", "a", "value1".to_string()).unwrap();
        writer.set("prefix", "b", "value2".to_string()).unwrap();
        writer.set("prefix", "c", "value3".to_string()).unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<&str, &str> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        assert_eq!(reader.rank("prefix", "a"), 0);
        assert_eq!(reader.rank("prefix", "b"), 1);
        assert_eq!(reader.rank("prefix", "c"), 2);
        // Non-existent key returns insertion point
        assert_eq!(reader.rank("prefix", "ab"), 1);
    }

    #[test]
    fn test_roaring_bitmap_value() {
        use roaring::RoaringBitmap;

        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(2);
        bitmap.insert(100);

        writer.set("prefix", "key1", bitmap.clone()).unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<&str, RoaringBitmap> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        let result = reader.get("prefix", "key1").unwrap().unwrap();
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(100));
        assert!(!result.contains(3));
    }

    #[test]
    fn test_vec_u32_value() {
        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();

        let data: &[u32] = &[1, 2, 3, 4, 5];
        writer.set("prefix", "key1", data).unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<&str, &[u32]> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        let result = reader.get("prefix", "key1").unwrap().unwrap();
        assert_eq!(result, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_data_record() {
        use chroma_types::DataRecord;

        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();

        let record = DataRecord {
            id: "test_id",
            embedding: &[1.0, 2.0, 3.0],
            metadata: None,
            document: Some("test document"),
        };
        writer.set("prefix", "key1", &record).unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<&str, DataRecord> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        let result = reader.get("prefix", "key1").unwrap().unwrap();
        assert_eq!(result.id, "test_id");
        assert_eq!(result.embedding, &[1.0, 2.0, 3.0]);
        assert_eq!(result.document, Some("test document"));
    }

    #[test]
    fn test_spann_posting_list() {
        use chroma_types::SpannPostingList;

        let storage_manager = StorageManager::new();
        let writer = DashMapBlockfileWriter::new(storage_manager.clone());
        let id = writer.id();

        let posting_list = SpannPostingList {
            doc_offset_ids: &[1, 2, 3],
            doc_versions: &[1, 1, 2],
            doc_embeddings: &[0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
        };
        writer.set("prefix", 1u32, &posting_list).unwrap();
        writer.commit().unwrap();

        let reader: DashMapBlockfileReader<u32, SpannPostingList> =
            DashMapBlockfileReader::open(id, storage_manager).unwrap();

        let result = reader.get("prefix", 1u32).unwrap().unwrap();
        assert_eq!(result.doc_offset_ids, &[1, 2, 3]);
        assert_eq!(result.doc_versions, &[1, 1, 2]);
        assert_eq!(result.doc_embeddings, &[0.1, 0.2, 0.3, 0.4, 0.5, 0.6]);
    }
}
