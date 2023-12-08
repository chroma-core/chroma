use std::collections::HashMap;

use num_bigint::BigInt;
use uuid::Uuid;

pub(crate) type SeqId = BigInt;

#[derive(Debug)]
pub(crate) enum Operation {
    Add,
    Update,
    Upsert,
    Delete,
}

#[derive(Debug)]
pub(crate) enum ScalarEncoding {
    FLOAT32,
    INT32,
}

#[derive(Debug)]
pub(crate) enum SegmentScope {
    VECTOR,
    METADATA,
}

#[derive(Debug)]
pub(crate) enum MetadataValue {
    Int(i32),
    Float(f64),
    Str(String),
    Bool(bool),
    None,
}

#[derive(Debug)]
pub(crate) struct Collection {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) topic: String,
    pub(crate) metadata: Option<UpdateMetadata>,
    pub(crate) dimension: Option<i32>,
    pub(crate) tenant: String,
    pub(crate) database: String,
}

#[derive(Debug)]
pub(crate) struct Segment {
    pub(crate) id: Uuid,
    pub(crate) r#type: String,
    pub(crate) scope: SegmentScope,
    pub(crate) topic: Option<String>,
    pub(crate) collection: Option<Uuid>,
    pub(crate) metadata: Option<UpdateMetadata>,
}

// Type alias for the UpdateMetadata
pub(crate) type UpdateMetadata = HashMap<String, MetadataValue>;

#[derive(Debug)]
pub(crate) struct EmbeddingRecord {
    pub(crate) id: String,
    pub(crate) seq_id: SeqId,
    pub(crate) embedding: Option<Vec<f32>>, // NOTE: we only support float32 embeddings for now
    pub(crate) encoding: Option<ScalarEncoding>,
    pub(crate) metadata: Option<UpdateMetadata>,
    pub(crate) operation: Operation,
    pub(crate) collection_id: Uuid,
}
