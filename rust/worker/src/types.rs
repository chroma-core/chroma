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
    id: Uuid,
    seq_id: SeqId,
    embedding: Option<Vec<f32>>, // NOTE: we only support float32 embeddings for now
    encoding: Option<ScalarEncoding>,
    metadata: Option<UpdateMetadata>,
    operation: Operation,
    collection_id: Option<Uuid>,
}
