use crate::key::CompositeKey;
use chroma_types::{chroma_proto::UpdateMetadata, DataRecord, SpannPostingList};
use parking_lot::RwLock;
use prost::Message;
use roaring::RoaringBitmap;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

/// Unified storage for all value types
#[derive(Clone, Debug)]
pub enum StoredValue {
    String(String),
    U32(u32),
    F32(f32),
    Bool(bool),
    VecU32(Vec<u32>),
    RoaringBitmap(RoaringBitmap),
    DataRecord {
        id: String,
        embedding: Vec<f32>,
        metadata: Option<Vec<u8>>,
        document: Option<String>,
    },
    SpannPostingList {
        doc_offset_ids: Vec<u32>,
        doc_versions: Vec<u32>,
        doc_embeddings: Vec<f32>,
    },
}

/// Trait for converting writable values to StoredValue
pub trait ToStoredValue {
    fn to_stored_value(self) -> StoredValue;
}

/// Trait for converting StoredValue back to readable values
pub trait FromStoredValue<'a>: Sized {
    fn from_stored_value(value: &'a StoredValue) -> Option<Self>;
}

// ============ ToStoredValue implementations ============

impl ToStoredValue for String {
    fn to_stored_value(self) -> StoredValue {
        StoredValue::String(self)
    }
}

impl ToStoredValue for &str {
    fn to_stored_value(self) -> StoredValue {
        StoredValue::String(self.to_string())
    }
}

impl ToStoredValue for u32 {
    fn to_stored_value(self) -> StoredValue {
        StoredValue::U32(self)
    }
}

impl ToStoredValue for f32 {
    fn to_stored_value(self) -> StoredValue {
        StoredValue::F32(self)
    }
}

impl ToStoredValue for bool {
    fn to_stored_value(self) -> StoredValue {
        StoredValue::Bool(self)
    }
}

impl ToStoredValue for Vec<u32> {
    fn to_stored_value(self) -> StoredValue {
        StoredValue::VecU32(self)
    }
}

impl ToStoredValue for &[u32] {
    fn to_stored_value(self) -> StoredValue {
        StoredValue::VecU32(self.to_vec())
    }
}

impl ToStoredValue for RoaringBitmap {
    fn to_stored_value(self) -> StoredValue {
        StoredValue::RoaringBitmap(self)
    }
}

impl ToStoredValue for &RoaringBitmap {
    fn to_stored_value(self) -> StoredValue {
        StoredValue::RoaringBitmap(self.clone())
    }
}

impl ToStoredValue for &DataRecord<'_> {
    fn to_stored_value(self) -> StoredValue {
        let metadata = self.metadata.as_ref().map(|m| {
            let metadata_proto = Into::<UpdateMetadata>::into(m.clone());
            metadata_proto.encode_to_vec()
        });
        StoredValue::DataRecord {
            id: self.id.to_string(),
            embedding: self.embedding.to_vec(),
            metadata,
            document: self.document.map(|s| s.to_string()),
        }
    }
}

impl ToStoredValue for &SpannPostingList<'_> {
    fn to_stored_value(self) -> StoredValue {
        StoredValue::SpannPostingList {
            doc_offset_ids: self.doc_offset_ids.to_vec(),
            doc_versions: self.doc_versions.to_vec(),
            doc_embeddings: self.doc_embeddings.to_vec(),
        }
    }
}

// ============ FromStoredValue implementations ============

impl<'a> FromStoredValue<'a> for &'a str {
    fn from_stored_value(value: &'a StoredValue) -> Option<Self> {
        match value {
            StoredValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

impl<'a> FromStoredValue<'a> for u32 {
    fn from_stored_value(value: &'a StoredValue) -> Option<Self> {
        match value {
            StoredValue::U32(v) => Some(*v),
            _ => None,
        }
    }
}

impl<'a> FromStoredValue<'a> for f32 {
    fn from_stored_value(value: &'a StoredValue) -> Option<Self> {
        match value {
            StoredValue::F32(v) => Some(*v),
            _ => None,
        }
    }
}

impl<'a> FromStoredValue<'a> for bool {
    fn from_stored_value(value: &'a StoredValue) -> Option<Self> {
        match value {
            StoredValue::Bool(v) => Some(*v),
            _ => None,
        }
    }
}

impl<'a> FromStoredValue<'a> for &'a [u32] {
    fn from_stored_value(value: &'a StoredValue) -> Option<Self> {
        match value {
            StoredValue::VecU32(v) => Some(v.as_slice()),
            _ => None,
        }
    }
}

impl<'a> FromStoredValue<'a> for RoaringBitmap {
    fn from_stored_value(value: &'a StoredValue) -> Option<Self> {
        match value {
            StoredValue::RoaringBitmap(v) => Some(v.clone()),
            _ => None,
        }
    }
}

impl<'a> FromStoredValue<'a> for DataRecord<'a> {
    fn from_stored_value(value: &'a StoredValue) -> Option<Self> {
        match value {
            StoredValue::DataRecord {
                id,
                embedding,
                metadata,
                document,
            } => {
                let metadata = metadata.as_ref().and_then(|bytes| {
                    if bytes.is_empty() {
                        None
                    } else {
                        UpdateMetadata::decode(bytes.as_slice())
                            .ok()
                            .and_then(|m| m.try_into().ok())
                    }
                });
                Some(DataRecord {
                    id: id.as_str(),
                    embedding: embedding.as_slice(),
                    metadata,
                    document: document.as_deref(),
                })
            }
            _ => None,
        }
    }
}

impl<'a> FromStoredValue<'a> for SpannPostingList<'a> {
    fn from_stored_value(value: &'a StoredValue) -> Option<Self> {
        match value {
            StoredValue::SpannPostingList {
                doc_offset_ids,
                doc_versions,
                doc_embeddings,
            } => Some(SpannPostingList {
                doc_offset_ids: doc_offset_ids.as_slice(),
                doc_versions: doc_versions.as_slice(),
                doc_embeddings: doc_embeddings.as_slice(),
            }),
            _ => None,
        }
    }
}

// ============ StorageManager ============

/// Holds committed (frozen) data by id for readers to access
#[derive(Clone, Default)]
pub struct StorageManager {
    committed: Arc<RwLock<HashMap<Uuid, Arc<Vec<(CompositeKey, StoredValue)>>>>>,
}

impl StorageManager {
    pub fn new() -> Self {
        Self {
            committed: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Store committed data (sorted vec)
    pub fn commit(&self, id: Uuid, data: Vec<(CompositeKey, StoredValue)>) {
        let mut guard = self.committed.write();
        guard.insert(id, Arc::new(data));
    }

    /// Retrieve committed data for reader
    pub fn get(&self, id: &Uuid) -> Option<Arc<Vec<(CompositeKey, StoredValue)>>> {
        let guard = self.committed.read();
        guard.get(id).cloned()
    }

    /// Clear all committed data
    pub fn clear(&self) {
        let mut guard = self.committed.write();
        guard.clear();
    }
}
