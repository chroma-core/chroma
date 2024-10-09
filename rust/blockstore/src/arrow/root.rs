use super::{
    block::{delta::BlockDelta, Block},
    sparse_index::{SparseIndex, SparseIndexDelimiter},
    types::ArrowWriteableKey,
};
use crate::key::KeyWrapper;
use chroma_error::ChromaError;
use std::{collections::HashMap, fmt::Display};
use uuid::Uuid;

const CURRENT_VERSION: Version = Version::V1_1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Version {
    V1,
    V1_1,
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Version::V1 => write!(f, "v1"),
            Version::V1_1 => write!(f, "v1.1"),
        }
    }
}

impl From<&str> for Version {
    fn from(s: &str) -> Self {
        match s {
            "v1" => Version::V1,
            "v1.1" => Version::V1_1,
            _ => panic!("Unknown version: {}", s),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct RootWriter {
    // TODO: Replace with writer
    sparse_index: SparseIndex,
    // Metadata
    id: Uuid,
    version: Version,
}

#[derive(Debug, Clone)]
pub(super) struct RootReader {
    // TODO: Replace with reader
    sparse_index: SparseIndex,
    // Metadata
    id: Uuid,
    version: Version,
}

impl RootWriter {
    pub(super) fn new(id: Uuid, sparse_index: SparseIndex) -> Self {
        Self {
            version: CURRENT_VERSION,
            sparse_index,
            id,
        }
    }

    fn to_block<K: ArrowWriteableKey>(&self) -> Result<Block, Box<dyn ChromaError>> {
        let data = self.sparse_index.data.lock();
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
                    KeyWrapper::Bool(_b) => {
                        unimplemented!();
                        // delta.add("KEY", b, block_id.to_string().as_str());
                    }
                    KeyWrapper::Uint32(u) => {
                        delta.add(&k.prefix, *u, block_id.to_string());
                    }
                },
            }
        }

        let delta_id = delta.id;
        let metadata = HashMap::from_iter(vec![
            ("version".to_string(), self.version.to_string()),
            ("id".to_string(), self.id.to_string()),
        ]);
        let record_batch = delta.finish::<K, String>(Some(metadata));
        Ok(Block::from_record_batch(delta_id, record_batch))
    }
}
