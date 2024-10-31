use std::collections::HashMap;

use chroma_types::{Chunk, LogRecord, Operation, OperationRecord, UpdateMetadataValue};
use rand::{
    distributions::{Alphanumeric, Open01},
    thread_rng, Rng,
};

pub struct LogGenerator<G>
where
    G: Fn(usize) -> OperationRecord,
{
    pub generator: G,
}

impl<G> LogGenerator<G>
where
    G: Fn(usize) -> OperationRecord,
{
    pub fn generate_vec<O>(&self, offsets: O) -> Vec<LogRecord>
    where
        O: Iterator<Item = usize>,
    {
        offsets
            .map(|log_offset| LogRecord {
                log_offset: log_offset as i64,
                record: (self.generator)(log_offset),
            })
            .collect()
    }

    pub fn generate_chunk<O>(&self, offsets: O) -> Chunk<LogRecord>
    where
        O: Iterator<Item = usize>,
    {
        Chunk::new(self.generate_vec(offsets).into())
    }
}

pub fn offset_as_id(offset: usize) -> String {
    format!("offset_id_{offset}")
}

pub fn random_embedding(dim: usize) -> Vec<f32> {
    thread_rng().sample_iter(&Open01).take(dim).collect()
}

pub fn modulo_metadata(offset: usize) -> HashMap<String, UpdateMetadataValue> {
    vec![
        (
            "offset".to_string(),
            UpdateMetadataValue::Int(offset as i64),
        ),
        (
            "is_even".to_string(),
            UpdateMetadataValue::Bool(offset % 2 == 0),
        ),
        (
            "modulo_3".to_string(),
            UpdateMetadataValue::Int((offset % 3) as i64),
        ),
    ]
    .into_iter()
    .collect()
}

pub fn random_document(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .map(char::from)
        .take(len)
        .collect()
}

pub fn add_generator_0(offset: usize) -> OperationRecord {
    OperationRecord {
        id: offset_as_id(offset),
        embedding: Some(random_embedding(6)),
        encoding: None,
        metadata: Some(modulo_metadata(offset)),
        document: Some(random_document(6)),
        operation: Operation::Add,
    }
}
