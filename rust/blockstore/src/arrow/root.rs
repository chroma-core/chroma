use super::{
    block::Block,
    sparse_index::{SparseIndexReader, SparseIndexWriter},
    types::{ArrowReadableKey, ArrowWriteableKey},
};
use chroma_error::ChromaError;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};
use thiserror::Error;
use uuid::Uuid;

pub(super) const CURRENT_VERSION: Version = Version::V1_1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Version {
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

    pub(super) fn to_block<K: ArrowWriteableKey>(&self) -> Result<Block, Box<dyn ChromaError>> {
        let delta = self.sparse_index.to_delta::<K>()?;
        let metadata = HashMap::from_iter(vec![
            ("version".to_string(), self.version.to_string()),
            ("id".to_string(), self.id.to_string()),
        ]);
        let record_batch = delta.finish::<K, String>(Some(metadata));
        Ok(Block::from_record_batch(self.id, record_batch))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootReader {
    pub(super) sparse_index: SparseIndexReader,
    // Metadata
    pub(super) id: Uuid,
    version: Version,
}

impl chroma_cache::Weighted for RootReader {
    fn weight(&self) -> usize {
        1
    }
}

#[derive(Error, Debug)]
pub(super) enum FromBlockError {
    #[error("Error parsing UUID: {0}")]
    UuidParseError(#[from] uuid::Error),
    #[error("Error parsing version: {0}")]
    VersionParseError(#[from] std::fmt::Error),
    #[error("Missing metadata: {0}")]
    MissingMetadata(String),
    #[error(transparent)]
    VersionError(#[from] VersionError),
}

impl ChromaError for FromBlockError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            FromBlockError::UuidParseError(_) => chroma_error::ErrorCodes::InvalidArgument,
            FromBlockError::VersionParseError(_) => chroma_error::ErrorCodes::InvalidArgument,
            FromBlockError::MissingMetadata(_) => chroma_error::ErrorCodes::InvalidArgument,
            FromBlockError::VersionError(e) => e.code(),
        }
    }
}

impl RootReader {
    pub(super) fn from_block<'block, K: ArrowReadableKey<'block> + 'block>(
        block: &'block Block,
    ) -> Result<Self, FromBlockError> {
        // Parse metadata
        let block_metadata = block.metadata();
        let (version, id) = match (block_metadata.get("version"), block_metadata.get("id")) {
            (Some(version), Some(id)) => {
                (Version::try_from(version.as_str())?, Uuid::parse_str(id)?)
            }
            (Some(_), None) => return Err(FromBlockError::MissingMetadata("id".to_string())),
            (None, Some(_)) => {
                return Err(FromBlockError::MissingMetadata("version".to_string()));
            }
            // We default to the current version in the absence of metadata for these fields for
            // backwards compatibility
            (None, None) => (Version::V1, block.id),
        };

        let sparse_index = match SparseIndexReader::from_block::<K>(block) {
            Ok(sparse_index) => sparse_index,
            Err(e) => return Err(FromBlockError::UuidParseError(e)),
        };

        Ok(Self {
            version,
            sparse_index,
            id,
        })
    }

    pub(super) fn fork(&self, new_id: Uuid) -> RootWriter {
        let new_sparse_index = self.sparse_index.fork(new_id);
        RootWriter {
            version: self.version,
            sparse_index: new_sparse_index,
            id: new_id,
        }
    }
}
