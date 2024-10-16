use super::{
    block::{Block, BlockToBytesError},
    sparse_index::{SparseIndexReader, SparseIndexValue, SparseIndexWriter, SparseIndexWriterData},
    types::{ArrowReadableKey, ArrowWriteableKey},
};
use crate::{arrow::sparse_index::SparseIndexDelimiter, key::CompositeKey};
use arrow::{
    array::{
        Array, BinaryArray, BinaryBuilder, RecordBatch, StringArray, UInt32Array, UInt32Builder,
    },
    datatypes::{DataType, Field, Schema},
};
use chroma_error::ChromaError;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    sync::Arc,
};
use thiserror::Error;
use uuid::Uuid;

pub(super) const CURRENT_VERSION: Version = Version::V1_1;

// ================
// Version
// ================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub(super) enum Version {
    V1 = 1,
    V1_1 = 2,
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Version::V1 => write!(f, "v1"),
            Version::V1_1 => write!(f, "v1.1"),
        }
    }
}

#[derive(Error, Debug)]
pub(super) enum VersionError {
    #[error("Unknown version: {0}")]
    UnknownVersion(String),
}

impl ChromaError for VersionError {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::InvalidArgument
    }
}

impl TryFrom<&str> for Version {
    type Error = VersionError;
    fn try_from(s: &str) -> Result<Self, VersionError> {
        match s {
            "v1" => Ok(Version::V1),
            "v1.1" => Ok(Version::V1_1),
            _ => Err(VersionError::UnknownVersion(s.to_string())),
        }
    }
}

// ================
// RootWriter
// ================

#[derive(Debug, Clone)]
pub(super) struct RootWriter {
    pub(super) sparse_index: SparseIndexWriter,
    // Metadata
    pub(super) id: Uuid,
    version: Version,
}

impl RootWriter {
    pub(super) fn new(version: Version, id: Uuid, sparse_index: SparseIndexWriter) -> Self {
        Self {
            version,
            sparse_index,
            id,
        }
    }

    fn ids_as_arrow(&self, sparse_index_data: &SparseIndexWriterData) -> (BinaryArray, Field) {
        let mut ids_builder = BinaryBuilder::new();
        for (_, value) in sparse_index_data.forward.iter() {
            ids_builder.append_value(value.into_bytes());
        }
        (
            ids_builder.finish(),
            Field::new("id", DataType::Binary, false),
        )
    }

    fn counts_as_arrow(&self, sparse_index_data: &SparseIndexWriterData) -> (UInt32Array, Field) {
        let mut count_builder = UInt32Builder::new();
        // Version 1.0 does not have counts, so we default to 0
        // Since we don't currently write a blockfile to a new version, we don't need to worry about
        // the case where the version is 1.1 but the count is not set
        if self.version < Version::V1_1 {
            for _ in 0..sparse_index_data.forward.len() {
                count_builder.append_value(0);
            }
        } else {
            for (_, value) in sparse_index_data.counts.iter() {
                count_builder.append_value(*value);
            }
        }
        (
            count_builder.finish(),
            Field::new("count", DataType::UInt32, false),
        )
    }

    pub(super) fn to_bytes<K: ArrowWriteableKey>(&self) -> Result<Vec<u8>, Box<dyn ChromaError>> {
        // Serialize the sparse index as an arrow record batch
        // TODO(hammadb): Note that this should ideally use the Block API to serialize the sparse
        // index, but we are currently using the arrow API directly because the block api
        // does not support multiple columns with different types. When we add support for this
        // we can switch to using the block API.
        let sparse_index_data = self.sparse_index.data.lock();
        let mut prefix_cap = 0;
        let mut key_cap = 0;
        for (key, _) in sparse_index_data.forward.iter() {
            match key {
                SparseIndexDelimiter::Start => {}
                SparseIndexDelimiter::Key(k) => {
                    prefix_cap += k.prefix.len();
                    key_cap += k.key.get_size();
                }
            }
        }
        let mut key_builder =
            K::get_arrow_builder(sparse_index_data.forward.len(), prefix_cap, key_cap);

        for (key, _) in sparse_index_data.forward.iter() {
            match key {
                SparseIndexDelimiter::Start => key_builder.add_key(CompositeKey {
                    prefix: "START".to_string(),
                    key: K::default().into(),
                }),
                SparseIndexDelimiter::Key(k) => {
                    key_builder.add_key(k.clone());
                }
            };
        }

        // NOTE(hammadb) This could be done as one pass over the sparse index but
        // this is simpler to write and this it not performance critical / impact is minimal
        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.as_arrow();
        let (built_ids, id_field) = self.ids_as_arrow(&sparse_index_data);
        let (built_counts, count_field) = self.counts_as_arrow(&sparse_index_data);

        let metadata = HashMap::from_iter(vec![
            ("version".to_string(), self.version.to_string()),
            ("id".to_string(), self.id.to_string()),
        ]);

        let schema = Arc::new(Schema::new_with_metadata(
            vec![prefix_field, key_field, id_field, count_field],
            metadata,
        ));

        let record_batch = match RecordBatch::try_new(
            schema,
            vec![
                Arc::new(prefix_arr),
                Arc::new(key_arr),
                Arc::new(built_ids),
                Arc::new(built_counts),
            ],
        ) {
            Ok(record_batch) => record_batch,
            Err(e) => return Err(Box::new(ToBytesError::ArrowError(e))),
        };

        match Block::from_record_batch(self.id, record_batch).to_bytes() {
            Ok(bytes) => Ok(bytes),
            Err(e) => Err(Box::new(ToBytesError::BlockToBytesError(e))),
        }
    }
}

// ================
// Writer Errors
// ================

#[derive(Error, Debug)]
pub enum ToBytesError {
    #[error(transparent)]
    BlockToBytesError(#[from] BlockToBytesError),
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),
}

impl ChromaError for ToBytesError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            ToBytesError::BlockToBytesError(_) => chroma_error::ErrorCodes::Internal,
            ToBytesError::ArrowError(_) => chroma_error::ErrorCodes::Internal,
        }
    }
}

// ================
// RootReader
// ================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootReader {
    pub(super) sparse_index: SparseIndexReader,
    // Metadata
    pub(super) id: Uuid,
    pub(super) version: Version,
}

impl chroma_cache::Weighted for RootReader {
    fn weight(&self) -> usize {
        1
    }
}

#[derive(Error, Debug)]
pub(super) enum FromBytesError {
    #[error("Error parsing UUID: {0}")]
    UuidParseError(#[from] uuid::Error),
    #[error("Error parsing version: {0}")]
    VersionParseError(#[from] std::fmt::Error),
    #[error("Missing metadata: {0}")]
    MissingMetadata(String),
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("No data")]
    NoDataError,
    #[error("Stored id does not match provided id")]
    IdMismatch,
    #[error(transparent)]
    VersionError(#[from] VersionError),
}

impl ChromaError for FromBytesError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            FromBytesError::UuidParseError(_) => chroma_error::ErrorCodes::InvalidArgument,
            FromBytesError::VersionParseError(_) => chroma_error::ErrorCodes::InvalidArgument,
            FromBytesError::MissingMetadata(_) => chroma_error::ErrorCodes::InvalidArgument,
            FromBytesError::ArrowError(_) => chroma_error::ErrorCodes::Internal,
            FromBytesError::NoDataError => chroma_error::ErrorCodes::Internal,
            FromBytesError::IdMismatch => chroma_error::ErrorCodes::InvalidArgument,
            FromBytesError::VersionError(e) => e.code(),
        }
    }
}

impl RootReader {
    pub(super) fn from_bytes<'data, K: ArrowReadableKey<'data>>(
        bytes: &[u8],
        id: Uuid,
    ) -> Result<Self, FromBytesError> {
        let mut cursor = std::io::Cursor::new(bytes);
        let arrow_reader = arrow::ipc::reader::FileReader::try_new(&mut cursor, None);

        let record_batch = match arrow_reader {
            Ok(mut reader) => match reader.next() {
                Some(Ok(batch)) => batch,
                Some(Err(e)) => return Err(FromBytesError::ArrowError(e)),
                None => {
                    return Err(FromBytesError::NoDataError);
                }
            },
            Err(e) => return Err(FromBytesError::ArrowError(e)),
        };

        let metadata = &record_batch.schema_ref().metadata;
        let (version, read_id) = match (metadata.get("version"), metadata.get("id")) {
            (Some(version), Some(read_id)) => (
                Version::try_from(version.as_str())?,
                Uuid::parse_str(read_id)?,
            ),
            (Some(_), None) => return Err(FromBytesError::MissingMetadata("id".to_string())),
            (None, Some(_)) => {
                return Err(FromBytesError::MissingMetadata("version".to_string()));
            }
            // We default to the current version in the absence of metadata for these fields for
            // backwards compatibility
            (None, None) => (Version::V1, id),
        };

        if read_id != id {
            return Err(FromBytesError::IdMismatch);
        }

        let prefix_arr = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Prefix array to be a StringArray");
        // Use unsafe to promote the liftimes using unsafe, we know record batch lives as long as it needs to.
        // It only needs to live as long as the sparse index is being constructed.
        // The sparse index copies the data so it can live as long as it needs to independently
        let record_batch: &'data RecordBatch = unsafe { std::mem::transmute(&record_batch) };
        let key_arr = record_batch.column(1);
        let id_arr = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("ID array to be a BinaryArray");

        // Version 1.1 is the first version to have a count column
        let mut counts = None;
        if version >= Version::V1_1 {
            let count_arr = record_batch
                .column(3)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("Count array to be a UInt32Array");
            counts = Some(count_arr);
        }

        let sparse_index_len = prefix_arr.len();
        let mut forward = BTreeMap::new();
        for i in 0..sparse_index_len {
            let prefix = prefix_arr.value(i);
            let key = K::get(key_arr, i);
            let block_id = match Uuid::from_slice(id_arr.value(i)) {
                Ok(block_id) => block_id,
                Err(e) => return Err(FromBytesError::UuidParseError(e)),
            };

            let count = match counts {
                Some(count_arr) => count_arr.value(i),
                None => 0,
            };

            match prefix {
                "START" => {
                    forward.insert(
                        SparseIndexDelimiter::Start,
                        SparseIndexValue::new(block_id, count),
                    );
                }
                _ => {
                    forward.insert(
                        SparseIndexDelimiter::Key(CompositeKey::new(prefix.to_string(), key)),
                        SparseIndexValue::new(block_id, count),
                    );
                }
            }
        }

        let sparse_index_reader = SparseIndexReader::new(forward);
        Ok(Self {
            version,
            sparse_index: sparse_index_reader,
            id,
        })
    }

    pub(super) fn fork(&self, new_id: Uuid) -> RootWriter {
        let new_sparse_index = self.sparse_index.fork();
        RootWriter {
            version: self.version,
            sparse_index: new_sparse_index,
            id: new_id,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_to_from_bytes() {
        let block_ids = [
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        ];
        let sparse_index = SparseIndexWriter::new(block_ids[0]);

        let bf_id = Uuid::new_v4();
        let root_writer = RootWriter::new(CURRENT_VERSION, bf_id, sparse_index);

        root_writer
            .sparse_index
            .add_block(CompositeKey::new("prefix".to_string(), "a"), block_ids[1])
            .expect("No error");
        root_writer
            .sparse_index
            .add_block(CompositeKey::new("prefix".to_string(), "b"), block_ids[2])
            .expect("No error");
        root_writer
            .sparse_index
            .add_block(CompositeKey::new("prefix".to_string(), "c"), block_ids[3])
            .expect("No error");

        root_writer
            .sparse_index
            .set_count(block_ids[0], 1)
            .expect("Set count should succeed");
        root_writer
            .sparse_index
            .set_count(block_ids[1], 2)
            .expect("Set count should succeed");
        root_writer
            .sparse_index
            .set_count(block_ids[2], 3)
            .expect("Set count should succeed");
        root_writer
            .sparse_index
            .set_count(block_ids[3], 4)
            .expect("Set count should succeed");

        let bytes = root_writer
            .to_bytes::<&str>()
            .expect("To be able to serialize");
        let root_reader =
            RootReader::from_bytes::<&str>(&bytes, bf_id).expect("To be able to deserialize");

        // Check that the sparse index is the same
        assert_eq!(
            root_writer.sparse_index.len(),
            root_reader.sparse_index.len()
        );

        // Check that the block mapping is the same
        for (key, value) in root_writer.sparse_index.data.lock().forward.iter() {
            assert_eq!(
                root_reader.sparse_index.data.forward.get(key).unwrap().id,
                *value
            );
        }

        // Check that counts are the same
        let writer_data = &root_writer.sparse_index.data.lock();
        for (key, _) in writer_data.forward.iter() {
            assert_eq!(
                root_reader
                    .sparse_index
                    .data
                    .forward
                    .get(key)
                    .unwrap()
                    .count,
                *writer_data.counts.get(key).unwrap()
            );
        }

        assert_eq!(root_writer.version, root_reader.version);
        assert_eq!(root_writer.id, root_reader.id);
    }

    #[test]
    fn test_from_v1() {
        let block_ids = [
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        ];
        let counts = [1, 2, 3, 4];
        let composite_keys = [
            CompositeKey::new("prefix".to_string(), "a"),
            CompositeKey::new("prefix".to_string(), "b"),
            CompositeKey::new("prefix".to_string(), "c"),
            CompositeKey::new("prefix".to_string(), "d"),
        ];
        let sparse_index = SparseIndexWriter::new(block_ids[0]);

        let bf_id = Uuid::new_v4();
        let root_writer = RootWriter::new(Version::V1, bf_id, sparse_index);
        root_writer
            .sparse_index
            .set_count(block_ids[0], counts[0])
            .expect("Set count should succeed");
        for i in 1..block_ids.len() {
            root_writer
                .sparse_index
                .add_block(composite_keys[i].clone(), block_ids[i])
                .expect("No error");
            root_writer
                .sparse_index
                .set_count(block_ids[i], counts[i])
                .expect("Set count should succeed");
        }

        let bytes = root_writer
            .to_bytes::<&str>()
            .expect("To be able to serialize");

        let root_reader =
            RootReader::from_bytes::<&str>(&bytes, bf_id).expect("To be able to deserialize");

        // Check the version is still v1
        assert_eq!(root_reader.version, Version::V1);
        // Check that the counts map is just 0
        for (key, _) in root_reader.sparse_index.data.forward.iter() {
            assert_eq!(
                root_reader
                    .sparse_index
                    .data
                    .forward
                    .get(key)
                    .unwrap()
                    .count,
                0
            );
        }
    }
}
