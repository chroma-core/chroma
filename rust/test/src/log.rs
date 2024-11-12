use chroma_types::{Chunk, LogRecord, OperationRecord};
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

pub fn random_document(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .map(char::from)
        .take(len)
        .collect()
}
