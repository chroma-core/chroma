use crate::{
    blockstore::key::{CompositeKey, KeyWrapper},
    errors::ChromaError,
    segment::DataRecord,
};
use arrow::array::Int32Array;
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use std::{collections::HashMap, sync::Arc};

pub(crate) trait Writeable {
    fn write_to_storage(prefix: &str, key: KeyWrapper, value: Self, storage: &StorageBuilder);
    fn remove_from_storage(prefix: &str, key: KeyWrapper, storage: &StorageBuilder);
}

pub(crate) trait Readable<'referred_data>: Sized {
    fn read_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Option<Self>;

    fn get_by_prefix_from_storage(
        prefix: &str,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)>;

    fn read_gt_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)>;

    fn read_gte_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)>;

    fn read_lt_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)>;

    fn read_lte_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)>;

    fn count(storage: &'referred_data Storage) -> Result<usize, Box<dyn ChromaError>>;
}

impl Writeable for &str {
    fn write_to_storage(prefix: &str, key: KeyWrapper, value: Self, storage: &StorageBuilder) {
        storage
            .string_value_storage
            .write()
            .as_mut()
            .unwrap()
            .insert(
                CompositeKey {
                    prefix: prefix.to_string(),
                    key,
                },
                value.to_string(),
            );
    }

    fn remove_from_storage(prefix: &str, key: KeyWrapper, storage: &StorageBuilder) {
        storage
            .string_value_storage
            .write()
            .as_mut()
            .unwrap()
            .remove(&CompositeKey {
                prefix: prefix.to_string(),
                key,
            });
    }
}

impl<'referred_data> Readable<'referred_data> for &'referred_data str {
    fn read_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Option<Self> {
        storage
            .string_value_storage
            .get(&CompositeKey {
                prefix: prefix.to_string(),
                key,
            })
            .map(|s| s.as_str())
    }

    fn get_by_prefix_from_storage(
        prefix: &str,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .string_value_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix)
            .map(|(k, v)| (k, v.as_str()))
            .collect()
    }

    fn read_gt_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .string_value_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key > key)
            .map(|(k, v)| (k, v.as_str()))
            .collect()
    }

    fn read_gte_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .string_value_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key >= key)
            .map(|(k, v)| (k, v.as_str()))
            .collect()
    }

    fn read_lt_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .string_value_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key < key)
            .map(|(k, v)| (k, v.as_str()))
            .collect()
    }

    fn read_lte_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .string_value_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key <= key)
            .map(|(k, v)| (k, v.as_str()))
            .collect()
    }

    fn count(storage: &'referred_data Storage) -> Result<usize, Box<dyn ChromaError>> {
        Ok(storage.string_value_storage.iter().len())
    }
}

// TODO: remove this and make this all use a unified storage so we don't have two impls
impl Writeable for &Int32Array {
    fn write_to_storage(prefix: &str, key: KeyWrapper, value: Self, storage: &StorageBuilder) {
        storage
            .int32_array_storage
            .write()
            .as_mut()
            .unwrap()
            .insert(
                CompositeKey {
                    prefix: prefix.to_string(),
                    key: key.clone(),
                },
                value.clone(),
            );
    }

    fn remove_from_storage(prefix: &str, key: KeyWrapper, storage: &StorageBuilder) {
        storage
            .int32_array_storage
            .write()
            .as_mut()
            .unwrap()
            .remove(&CompositeKey {
                prefix: prefix.to_string(),
                key,
            });
    }
}

impl<'referred_data> Readable<'referred_data> for Int32Array {
    fn read_from_storage(prefix: &str, key: KeyWrapper, storage: &Storage) -> Option<Self> {
        storage
            .int32_array_storage
            .get(&CompositeKey {
                prefix: prefix.to_string(),
                key,
            })
            .map(|a| a.clone())
    }

    fn get_by_prefix_from_storage(
        prefix: &str,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .int32_array_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn read_gt_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .int32_array_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key > key)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn read_gte_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .int32_array_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key >= key)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn read_lt_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .int32_array_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key < key)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn read_lte_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .int32_array_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key <= key)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn count(storage: &'referred_data Storage) -> Result<usize, Box<dyn ChromaError>> {
        Ok(storage.int32_array_storage.iter().len())
    }
}

impl Writeable for &RoaringBitmap {
    fn write_to_storage(prefix: &str, key: KeyWrapper, value: Self, storage: &StorageBuilder) {
        storage
            .roaring_bitmap_storage
            .write()
            .as_mut()
            .unwrap()
            .insert(
                CompositeKey {
                    prefix: prefix.to_string(),
                    key: key.clone(),
                },
                value.clone(),
            );
    }

    fn remove_from_storage(prefix: &str, key: KeyWrapper, storage: &StorageBuilder) {
        storage
            .roaring_bitmap_storage
            .write()
            .as_mut()
            .unwrap()
            .remove(&CompositeKey {
                prefix: prefix.to_string(),
                key,
            });
    }
}

impl<'referred_data> Readable<'referred_data> for RoaringBitmap {
    fn read_from_storage(prefix: &str, key: KeyWrapper, storage: &Storage) -> Option<Self> {
        storage
            .roaring_bitmap_storage
            .get(&CompositeKey {
                prefix: prefix.to_string(),
                key,
            })
            .map(|a| a.clone())
    }

    fn get_by_prefix_from_storage(
        prefix: &str,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .roaring_bitmap_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn read_gt_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .roaring_bitmap_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key > key)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn read_gte_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .roaring_bitmap_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key >= key)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn read_lt_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .roaring_bitmap_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key < key)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn read_lte_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .roaring_bitmap_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key <= key)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn count(storage: &'referred_data Storage) -> Result<usize, Box<dyn ChromaError>> {
        Ok(storage.roaring_bitmap_storage.iter().len())
    }
}

impl Writeable for u32 {
    fn write_to_storage(prefix: &str, key: KeyWrapper, value: Self, storage: &StorageBuilder) {
        storage.u32_storage.write().as_mut().unwrap().insert(
            CompositeKey {
                prefix: prefix.to_string(),
                key: key.clone(),
            },
            value,
        );
    }

    fn remove_from_storage(prefix: &str, key: KeyWrapper, storage: &StorageBuilder) {
        storage
            .u32_storage
            .write()
            .as_mut()
            .unwrap()
            .remove(&CompositeKey {
                prefix: prefix.to_string(),
                key,
            });
    }
}

impl<'referred_data> Readable<'referred_data> for u32 {
    fn read_from_storage(prefix: &str, key: KeyWrapper, storage: &Storage) -> Option<Self> {
        storage
            .u32_storage
            .get(&CompositeKey {
                prefix: prefix.to_string(),
                key,
            })
            .map(|a| *a)
    }

    fn get_by_prefix_from_storage(
        prefix: &str,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .u32_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix)
            .map(|(k, v)| (k, *v))
            .collect()
    }

    fn read_gt_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .u32_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key > key)
            .map(|(k, v)| (k, *v))
            .collect()
    }

    fn read_gte_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .u32_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key >= key)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn read_lt_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .u32_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key < key)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn read_lte_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .u32_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key <= key)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn count(storage: &'referred_data Storage) -> Result<usize, Box<dyn ChromaError>> {
        Ok(storage.u32_storage.iter().len())
    }
}

impl Writeable for &DataRecord<'_> {
    fn write_to_storage(prefix: &str, key: KeyWrapper, value: Self, storage: &StorageBuilder) {
        storage
            .data_record_id_storage
            .write()
            .as_mut()
            .unwrap()
            .insert(
                CompositeKey {
                    prefix: prefix.to_string(),
                    key: key.clone(),
                },
                value.id.to_string(),
            );
        storage
            .data_record_embedding_storage
            .write()
            .as_mut()
            .unwrap()
            .insert(
                CompositeKey {
                    prefix: prefix.to_string(),
                    key,
                },
                value.embedding.to_vec(),
            );
    }

    fn remove_from_storage(prefix: &str, key: KeyWrapper, storage: &StorageBuilder) {
        storage
            .data_record_id_storage
            .write()
            .as_mut()
            .unwrap()
            .remove(&CompositeKey {
                prefix: prefix.to_string(),
                key: key.clone(),
            });
        storage
            .data_record_embedding_storage
            .write()
            .as_mut()
            .unwrap()
            .remove(&CompositeKey {
                prefix: prefix.to_string(),
                key,
            });
    }
}

impl<'referred_data> Readable<'referred_data> for DataRecord<'referred_data> {
    fn read_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Option<Self> {
        let id = storage.data_record_id_storage.get(&CompositeKey {
            prefix: prefix.to_string(),
            key: key.clone(),
        });
        let embedding = storage.data_record_embedding_storage.get(&CompositeKey {
            prefix: prefix.to_string(),
            key,
        });
        // TODO: don't unwrap
        Some(DataRecord {
            id: &id.unwrap(),
            embedding: &embedding.unwrap(),
            metadata: None,
            document: None,
        })
    }

    fn get_by_prefix_from_storage(
        prefix: &str,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .data_record_id_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix)
            .map(|(k, v)| {
                let embedding = storage.data_record_embedding_storage.get(k).unwrap();
                let id = v;
                (
                    k,
                    DataRecord {
                        id,
                        embedding,
                        metadata: None,
                        document: None,
                    },
                )
            })
            .collect()
    }

    fn read_gt_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .data_record_id_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key > key)
            .map(|(k, v)| {
                let embedding = storage.data_record_embedding_storage.get(k).unwrap();
                let id = v;
                (
                    k,
                    DataRecord {
                        id,
                        embedding,
                        metadata: None,
                        document: None,
                    },
                )
            })
            .collect()
    }

    fn read_gte_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .data_record_id_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key >= key)
            .map(|(k, v)| {
                let embedding = storage.data_record_embedding_storage.get(k).unwrap();
                let id = v;
                (
                    k,
                    DataRecord {
                        id,
                        embedding,
                        metadata: None,
                        document: None,
                    },
                )
            })
            .collect()
    }

    fn read_lt_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .data_record_id_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key < key)
            .map(|(k, v)| {
                let embedding = storage.data_record_embedding_storage.get(k).unwrap();
                let id = v;
                (
                    k,
                    DataRecord {
                        id,
                        embedding,
                        metadata: None,
                        document: None,
                    },
                )
            })
            .collect()
    }

    fn read_lte_from_storage(
        prefix: &str,
        key: KeyWrapper,
        storage: &'referred_data Storage,
    ) -> Vec<(&'referred_data CompositeKey, Self)> {
        storage
            .data_record_id_storage
            .iter()
            .filter(|(k, _)| k.prefix == prefix && k.key <= key)
            .map(|(k, v)| {
                let embedding = storage.data_record_embedding_storage.get(k).unwrap();
                let id = v;
                (
                    k,
                    DataRecord {
                        id,
                        embedding,
                        metadata: None,
                        document: None,
                    },
                )
            })
            .collect()
    }

    fn count(storage: &'referred_data Storage) -> Result<usize, Box<dyn ChromaError>> {
        Ok(storage.data_record_id_storage.iter().len())
    }
}

#[derive(Clone)]
pub(crate) struct StorageBuilder {
    // String Value
    string_value_storage: Arc<RwLock<Option<HashMap<CompositeKey, String>>>>,
    // u32 Value
    u32_storage: Arc<RwLock<Option<HashMap<CompositeKey, u32>>>>,
    // Roaring Bitmap Value
    roaring_bitmap_storage: Arc<RwLock<Option<HashMap<CompositeKey, RoaringBitmap>>>>,
    // Int32 Array Value
    int32_array_storage: Arc<RwLock<Option<HashMap<CompositeKey, Int32Array>>>>,
    // Data Record Fields
    data_record_id_storage: Arc<RwLock<Option<HashMap<CompositeKey, String>>>>,
    data_record_embedding_storage: Arc<RwLock<Option<HashMap<CompositeKey, Vec<f32>>>>>,
    pub(super) id: uuid::Uuid,
}

#[derive(Clone)]
pub(crate) struct Storage {
    // String Value
    string_value_storage: Arc<HashMap<CompositeKey, String>>,
    // // u32 Value
    u32_storage: Arc<HashMap<CompositeKey, u32>>,
    // // Roaring Bitmap Value
    roaring_bitmap_storage: Arc<HashMap<CompositeKey, RoaringBitmap>>,
    // // Int32 Array Value
    int32_array_storage: Arc<HashMap<CompositeKey, Int32Array>>,
    // Data Record Fields
    data_record_id_storage: Arc<HashMap<CompositeKey, String>>,
    data_record_embedding_storage: Arc<HashMap<CompositeKey, Vec<f32>>>,
    pub(super) id: uuid::Uuid,
}

#[derive(Clone)]
pub(crate) struct StorageManager {
    read_cache: Arc<RwLock<HashMap<uuid::Uuid, Storage>>>,
    write_cache: Arc<RwLock<HashMap<uuid::Uuid, StorageBuilder>>>,
}

impl StorageManager {
    pub(super) fn new() -> Self {
        Self {
            read_cache: Arc::new(RwLock::new(HashMap::new())),
            write_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(super) fn get(&self, id: uuid::Uuid) -> Option<Storage> {
        let cache_guard = self.read_cache.read();
        let storage = cache_guard.get(&id)?.clone();
        Some(storage)
    }

    pub(super) fn create(&self) -> StorageBuilder {
        let id = uuid::Uuid::new_v4();
        let builder = StorageBuilder {
            string_value_storage: Arc::new(RwLock::new(Some(HashMap::new()))),
            u32_storage: Arc::new(RwLock::new(Some(HashMap::new()))),
            roaring_bitmap_storage: Arc::new(RwLock::new(Some(HashMap::new()))),
            int32_array_storage: Arc::new(RwLock::new(Some(HashMap::new()))),
            data_record_id_storage: Arc::new(RwLock::new(Some(HashMap::new()))),
            data_record_embedding_storage: Arc::new(RwLock::new(Some(HashMap::new()))),
            id,
        };
        let mut cache_guard = self.write_cache.write();
        cache_guard.insert(id, builder.clone());
        builder
    }

    pub(super) fn commit(&self, id: uuid::Uuid) -> Storage {
        let mut write_cache_guard = self.write_cache.write();
        let builder = write_cache_guard.remove(&id).unwrap();
        let storage = Storage {
            string_value_storage: builder.string_value_storage.write().take().unwrap().into(),
            int32_array_storage: builder.int32_array_storage.write().take().unwrap().into(),
            roaring_bitmap_storage: builder
                .roaring_bitmap_storage
                .write()
                .take()
                .unwrap()
                .into(),
            u32_storage: builder.u32_storage.write().take().unwrap().into(),
            data_record_id_storage: builder
                .data_record_id_storage
                .write()
                .take()
                .unwrap()
                .into(),
            data_record_embedding_storage: builder
                .data_record_embedding_storage
                .write()
                .take()
                .unwrap()
                .into(),
            id,
        };
        let mut read_cache_guard = self.read_cache.write();
        read_cache_guard.insert(id, storage.clone());
        storage
    }
}
