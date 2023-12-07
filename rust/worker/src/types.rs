use std::collections::HashMap;

use num_bigint::BigInt;
use num_traits::float::Float;
use uuid::Uuid;

pub(crate) type SeqId = BigInt;

pub(crate) enum Operation {
    Add,
    Update,
    Upsert,
    Delete,
}

pub(crate) enum ScalarEncoding {
    FLOAT32,
    INT32,
}

pub(crate) enum MetadataValue {
    Int(i32),
    Float(f64),
    Str(String),
    Bool(bool),
    None,
}

// Type alias for the UpdateMetadata
pub(crate) type UpdateMetadata = HashMap<String, MetadataValue>;

pub(crate) struct EmbeddingRecord {
    pub(crate) id: Uuid,
    pub(crate) seq_id: SeqId,
    pub(crate) embedding: Option<Vec<f32>>, // NOTE: we only support float32 embeddings for now
    pub(crate) encoding: Option<ScalarEncoding>,
    pub(crate) metadata: Option<UpdateMetadata>,
    pub(crate) operation: Operation,
    pub(crate) collection_id: Uuid,
}
