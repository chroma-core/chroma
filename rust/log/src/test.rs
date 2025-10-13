use std::collections::HashMap;

use async_trait::async_trait;
use chroma_segment::test::TestDistributedSegment;
use chroma_types::{Chunk, LogRecord, Operation, OperationRecord, UpdateMetadataValue};
use rand::{
    distributions::{Alphanumeric, Open01},
    thread_rng, Rng,
};

pub const TEST_EMBEDDING_DIMENSION: usize = 6;

pub trait LogGenerator {
    fn generate_vec<O>(&self, offsets: O) -> Vec<LogRecord>
    where
        O: Iterator<Item = usize>;
    fn generate_chunk<O>(&self, offsets: O) -> Chunk<LogRecord>
    where
        O: Iterator<Item = usize>,
    {
        Chunk::new(self.generate_vec(offsets).into())
    }
}

impl<G> LogGenerator for G
where
    G: Fn(usize) -> OperationRecord,
{
    fn generate_vec<O>(&self, offsets: O) -> Vec<LogRecord>
    where
        O: Iterator<Item = usize>,
    {
        offsets
            .map(|log_offset| LogRecord {
                log_offset: log_offset as i64,
                record: self(log_offset),
            })
            .collect()
    }
}

pub fn int_as_id(value: usize) -> String {
    format!("id_{value}")
}

pub fn random_embedding(dim: usize) -> Vec<f32> {
    thread_rng().sample_iter(&Open01).take(dim).collect()
}

pub fn modulo_metadata(value: usize) -> HashMap<String, UpdateMetadataValue> {
    vec![
        ("id".to_string(), UpdateMetadataValue::Int(value as i64)),
        (
            "is_even".to_string(),
            UpdateMetadataValue::Bool(value % 2 == 0),
        ),
        (
            "modulo_3".to_string(),
            UpdateMetadataValue::Int((value % 3) as i64),
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

pub fn modulo_document(value: usize) -> String {
    let cat = if value % 3 == 0 { "<cat>" } else { "" };
    let dog = if value % 5 == 0 { "<dog>" } else { "" };
    format!("{cat}{dog}")
}

pub fn upsert_generator(offset: usize) -> OperationRecord {
    OperationRecord {
        id: int_as_id(offset),
        embedding: Some(random_embedding(TEST_EMBEDDING_DIMENSION)),
        encoding: None,
        metadata: Some(modulo_metadata(offset)),
        document: Some(random_document(6)),
        operation: Operation::Upsert,
    }
}

/// Adds new record and deletes from the start every 6 records`
///
/// # Illustration for head of log
/// [Add 1], [Add 2], [Add 3], [Add 4], [Add 5], [Del 1], [Add 6] ...
pub fn add_delete_generator(offset: usize) -> OperationRecord {
    if offset % 6 == 0 {
        OperationRecord {
            id: int_as_id(offset / 6),
            embedding: None,
            encoding: None,
            metadata: None,
            document: None,
            operation: Operation::Delete,
        }
    } else {
        let int_id = offset - offset / 6;
        OperationRecord {
            id: int_as_id(int_id),
            embedding: Some(random_embedding(TEST_EMBEDDING_DIMENSION)),
            encoding: None,
            metadata: Some(modulo_metadata(int_id)),
            document: Some(modulo_document(int_id)),
            operation: Operation::Add,
        }
    }
}

#[async_trait]
pub trait LoadFromGenerator<L: LogGenerator> {
    async fn populate_with_generator(&mut self, log_count: usize, generator: L);
}

#[async_trait]
impl<L: LogGenerator + Send + 'static> LoadFromGenerator<L> for TestDistributedSegment {
    async fn populate_with_generator(&mut self, log_count: usize, generator: L) {
        let ids: Vec<_> = (1..=log_count).collect();
        for chunk in ids.chunks(10_000) {
            Box::pin(
                self.compact_log(
                    generator.generate_chunk(chunk.iter().copied()),
                    chunk
                        .first()
                        .copied()
                        .expect("The chunk of offset ids to generate should not be empty."),
                ),
            )
            .await;
        }
    }
}
