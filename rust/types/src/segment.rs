use super::{
    CollectionUuid, Metadata, MetadataValueConversionError, SegmentScope,
    SegmentScopeConversionError,
};
use crate::collection_schema::Schema;
use crate::{chroma_proto, DatabaseUuid};
use chroma_error::{ChromaError, ErrorCodes};
use std::{collections::HashMap, str::FromStr};
use thiserror::Error;
use tonic::Status;
use uuid::Uuid;

pub const USER_ID_TO_OFFSET_ID: &str = "user_id_to_offset_id";
pub const OFFSET_ID_TO_USER_ID: &str = "offset_id_to_user_id";
pub const OFFSET_ID_TO_DATA: &str = "offset_id_to_data";
pub const MAX_OFFSET_ID: &str = "max_offset_id";
pub const USER_ID_BLOOM_FILTER: &str = "user_id_bloom_filter";

pub const FULL_TEXT_PLS: &str = "full_text_pls";
pub const STRING_METADATA: &str = "string_metadata";
pub const BOOL_METADATA: &str = "bool_metadata";
pub const F32_METADATA: &str = "f32_metadata";
pub const U32_METADATA: &str = "u32_metadata";

pub const SPARSE_MAX: &str = "sparse_max";
pub const SPARSE_OFFSET_VALUE: &str = "sparse_offset_value";
pub const SPARSE_POSTING: &str = "sparse_posting";

pub const HNSW_PATH: &str = "hnsw_path";
pub const VERSION_MAP_PATH: &str = "version_map_path";
pub const POSTING_LIST_PATH: &str = "posting_list_path";
pub const MAX_HEAD_ID_BF_PATH: &str = "max_head_id_path";

pub const QUANTIZED_SPANN_CLUSTER: &str = "quantized_spann_cluster";
pub const QUANTIZED_SPANN_SCALAR_METADATA: &str = "quantized_spann_scalar_metadata";
pub const QUANTIZED_SPANN_EMBEDDING_METADATA: &str = "quantized_spann_embedding_metadata";
pub const QUANTIZED_SPANN_RAW_CENTROID: &str = "quantized_spann_raw_centroid";
pub const QUANTIZED_SPANN_QUANTIZED_CENTROID: &str = "quantized_spann_quantized_centroid";

/// SegmentUuid is a wrapper around Uuid to provide a type for the segment id.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SegmentUuid(pub Uuid);

impl SegmentUuid {
    pub fn new() -> Self {
        SegmentUuid(Uuid::new_v4())
    }
}

impl FromStr for SegmentUuid {
    type Err = SegmentConversionError;

    fn from_str(s: &str) -> Result<Self, SegmentConversionError> {
        match Uuid::parse_str(s) {
            Ok(uuid) => Ok(SegmentUuid(uuid)),
            Err(_) => Err(SegmentConversionError::InvalidUuid),
        }
    }
}

impl std::fmt::Display for SegmentUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SegmentType {
    BlockfileMetadata,
    BlockfileRecord,
    HnswDistributed,
    HnswLocalMemory,
    HnswLocalPersisted,
    Sqlite,
    Spann,
    QuantizedSpann,
}

impl From<SegmentType> for String {
    fn from(segment_type: SegmentType) -> String {
        match segment_type {
            SegmentType::BlockfileMetadata => "urn:chroma:segment/metadata/blockfile".to_string(),
            SegmentType::BlockfileRecord => "urn:chroma:segment/record/blockfile".to_string(),
            SegmentType::HnswDistributed => {
                "urn:chroma:segment/vector/hnsw-distributed".to_string()
            }
            SegmentType::HnswLocalMemory => {
                "urn:chroma:segment/vector/hnsw-local-memory".to_string()
            }
            SegmentType::HnswLocalPersisted => {
                "urn:chroma:segment/vector/hnsw-local-persisted".to_string()
            }
            SegmentType::Spann => "urn:chroma:segment/vector/spann".to_string(),
            SegmentType::QuantizedSpann => "urn:chroma:segment/vector/quantized-spann".to_string(),
            SegmentType::Sqlite => "urn:chroma:segment/metadata/sqlite".to_string(),
        }
    }
}

impl TryFrom<&str> for SegmentType {
    type Error = SegmentConversionError;

    fn try_from(segment_type: &str) -> Result<Self, Self::Error> {
        match segment_type {
            "urn:chroma:segment/metadata/blockfile" => Ok(SegmentType::BlockfileMetadata),
            "urn:chroma:segment/record/blockfile" => Ok(SegmentType::BlockfileRecord),
            "urn:chroma:segment/vector/hnsw-distributed" => Ok(SegmentType::HnswDistributed),
            "urn:chroma:segment/vector/hnsw-local-memory" => Ok(SegmentType::HnswLocalMemory),
            "urn:chroma:segment/vector/hnsw-local-persisted" => Ok(Self::HnswLocalPersisted),
            "urn:chroma:segment/vector/spann" => Ok(SegmentType::Spann),
            "urn:chroma:segment/vector/quantized-spann" => Ok(SegmentType::QuantizedSpann),
            "urn:chroma:segment/metadata/sqlite" => Ok(SegmentType::Sqlite),
            _ => Err(SegmentConversionError::InvalidSegmentType),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Segment {
    pub id: SegmentUuid,
    pub r#type: SegmentType,
    pub scope: SegmentScope,
    pub collection: CollectionUuid,
    pub metadata: Option<Metadata>,
    pub file_path: HashMap<String, Vec<String>>,
}

impl Segment {
    // INVARIANT: THIS ALWAYS RETURNS AT LEAST ONE SHARD
    pub fn get_shards(&self) -> Result<Vec<SegmentShard>, SegmentShardError> {
        let num_shards = self.num_shards()?;

        // Create a SegmentShard for each shard index, propagating any errors
        let shards: Result<Vec<SegmentShard>, SegmentShardError> = (0..num_shards)
            .map(|shard_index| SegmentShard::try_from((self, shard_index as u32)))
            .collect();

        shards
    }

    pub fn prefetch_supported(&self) -> bool {
        matches!(
            self.r#type,
            SegmentType::BlockfileMetadata
                | SegmentType::BlockfileRecord
                | SegmentType::QuantizedSpann
                | SegmentType::Spann
        )
    }

    /// Returns the file paths that should be prefetched for this segment.
    /// If shard_index is None, returns the active shard's file paths. If shard_index is Some, returns
    /// only the file paths for that shard.
    pub fn filepaths_to_prefetch(&self, shard_index: Option<u32>) -> Vec<String> {
        let mut res = Vec::new();
        match self.r#type {
            SegmentType::QuantizedSpann => {
                for key in [
                    QUANTIZED_SPANN_CLUSTER,
                    QUANTIZED_SPANN_EMBEDDING_METADATA,
                    QUANTIZED_SPANN_SCALAR_METADATA,
                ] {
                    if let Some(paths) = self.file_path.get(key) {
                        if let Some(path) = match shard_index {
                            Some(index) => paths.get(index as usize),
                            None => paths.last(),
                        } {
                            res.push(path.clone());
                        }
                    }
                }
            }
            SegmentType::Spann => {
                if let Some(pl_path) = self.file_path.get(POSTING_LIST_PATH) {
                    if let Some(path) = match shard_index {
                        Some(index) => pl_path.get(index as usize),
                        None => pl_path.last(),
                    } {
                        res.push(path.clone());
                    }
                }
            }
            SegmentType::BlockfileMetadata | SegmentType::BlockfileRecord => {
                for (key, paths) in &self.file_path {
                    if key == USER_ID_BLOOM_FILTER {
                        continue;
                    }
                    if let Some(path) = match shard_index {
                        Some(index) => paths.get(index as usize),
                        None => paths.last(),
                    } {
                        res.push(path.clone());
                    }
                }
            }
            _ => {}
        }
        res
    }

    /// Returns the number of shards for this segment.
    /// Derives from the length of file_path Vecs (all must be equal per the
    /// shard count invariant). Returns 1 if no file paths are present.
    pub fn num_shards(&self) -> Result<usize, SegmentShardError> {
        let mut values = self.file_path.values();
        let num_shards = match values.next() {
            Some(paths) => paths.len().max(1),
            None => return Ok(1),
        };
        for (key, paths) in &self.file_path {
            if paths.len() != num_shards {
                return Err(SegmentShardError::MismatchedShardCounts {
                    key: key.clone(),
                    actual: paths.len(),
                    expected: num_shards,
                });
            }
        }
        Ok(num_shards)
    }

    /// Clears file paths for a specific shard index.
    /// This is useful during shard-specific rebuilds where we want to regenerate
    /// only one shard's files while preserving others.
    pub fn clear_shard_file_paths(&mut self, shard_index: u32) {
        let shard_idx = shard_index as usize;
        for (_, paths) in self.file_path.iter_mut() {
            if paths.len() > shard_idx {
                paths[shard_idx].clear();
            }
        }
        if self
            .file_path
            .values()
            .all(|paths| paths.iter().all(|p| p.is_empty()))
        {
            self.file_path.clear();
        }
    }

    /// Check that the on-disk file_path shape matches what the collection schema
    /// implies. Returns `Ok(())` when they agree or the segment is uninitialized
    /// (empty file_path). Returns `Err(SchemaMismatchError)` on the first
    /// mismatch detected.
    ///
    /// Validates that all file_path keys have the same shard count and that the
    /// set of keys present is consistent with the schema.
    pub fn matches_schema(&self, schema: &Schema) -> Result<(), SchemaMismatchError> {
        if self.file_path.is_empty() {
            return Ok(());
        }
        // Validate all keys have the same number of shards.
        self.num_shards().map_err(SchemaMismatchError::ShardError)?;

        match self.scope {
            SegmentScope::VECTOR => self.check_vector_consistency(schema),
            SegmentScope::METADATA => self.check_metadata_consistency(schema),
            SegmentScope::RECORD => self.check_record_consistency(),
            SegmentScope::SQLITE => Ok(()),
        }
    }

    fn mismatch(&self, detail: String) -> SchemaMismatchError {
        SchemaMismatchError::Mismatch {
            segment_id: self.id,
            scope: self.scope.clone(),
            detail,
        }
    }

    /// Verify that all `required` keys exist in file_path and no `denied` keys
    /// exist. Uninitialized segments (no relevant keys at all) pass.
    fn require_keys(&self, required: &[&str], denied: &[&str]) -> Result<(), SchemaMismatchError> {
        let has_required: Vec<&str> = required
            .iter()
            .filter(|k| self.file_path.contains_key(**k))
            .copied()
            .collect();
        let has_denied: Vec<&str> = denied
            .iter()
            .filter(|k| self.file_path.contains_key(**k))
            .copied()
            .collect();

        // No relevant keys at all — uninitialized, OK.
        if has_required.is_empty() && has_denied.is_empty() {
            return Ok(());
        }

        // Denied keys must not be present.
        if !has_denied.is_empty() {
            return Err(self.mismatch(format!(
                "unexpected keys present: {}",
                has_denied.join(", ")
            )));
        }

        // All required keys must be present — no partial state.
        if has_required.len() != required.len() {
            let missing: Vec<&str> = required
                .iter()
                .filter(|k| !self.file_path.contains_key(**k))
                .copied()
                .collect();
            return Err(self.mismatch(format!("missing required keys: {}", missing.join(", "))));
        }

        Ok(())
    }

    fn check_vector_consistency(&self, schema: &Schema) -> Result<(), SchemaMismatchError> {
        if schema.get_spann_config().is_none() {
            return Ok(());
        }

        let spann_keys: &[&str] = &[
            HNSW_PATH,
            VERSION_MAP_PATH,
            POSTING_LIST_PATH,
            MAX_HEAD_ID_BF_PATH,
        ];
        let qspann_keys: &[&str] = &[
            QUANTIZED_SPANN_CLUSTER,
            QUANTIZED_SPANN_SCALAR_METADATA,
            QUANTIZED_SPANN_EMBEDDING_METADATA,
            QUANTIZED_SPANN_RAW_CENTROID,
            QUANTIZED_SPANN_QUANTIZED_CENTROID,
        ];

        if schema.is_quantization_enabled() {
            self.require_keys(qspann_keys, spann_keys)
        } else {
            self.require_keys(spann_keys, qspann_keys)
        }
    }

    fn check_metadata_consistency(&self, schema: &Schema) -> Result<(), SchemaMismatchError> {
        let base_keys: &[&str] = &[
            FULL_TEXT_PLS,
            STRING_METADATA,
            BOOL_METADATA,
            F32_METADATA,
            U32_METADATA,
        ];
        self.require_keys(base_keys, &[])?;

        if schema.is_sparse_index_enabled() {
            let wand_keys: &[&str] = &[SPARSE_MAX, SPARSE_OFFSET_VALUE];
            let maxscore_keys: &[&str] = &[SPARSE_POSTING];

            if schema.is_maxscore_enabled() {
                self.require_keys(maxscore_keys, wand_keys)
            } else {
                self.require_keys(wand_keys, maxscore_keys)
            }
        } else {
            Ok(())
        }
    }

    fn check_record_consistency(&self) -> Result<(), SchemaMismatchError> {
        let required: &[&str] = &[
            USER_ID_TO_OFFSET_ID,
            OFFSET_ID_TO_USER_ID,
            OFFSET_ID_TO_DATA,
            MAX_OFFSET_ID,
        ];
        self.require_keys(required, &[])
    }

    pub fn extract_prefix_and_id(path: &str) -> Result<(&str, uuid::Uuid), uuid::Error> {
        let (prefix, id) = match path.rfind('/') {
            Some(pos) => (&path[..pos], &path[pos + 1..]),
            None => ("", path),
        };
        match Uuid::try_parse(id) {
            Ok(uid) => Ok((prefix, uid)),
            Err(e) => Err(e),
        }
    }

    pub fn construct_prefix_path(&self, tenant: &str, database_id: &DatabaseUuid) -> String {
        Self::construct_prefix_path_impl(tenant, database_id, &self.collection, &self.id)
    }

    fn construct_prefix_path_impl(
        tenant: &str,
        database_id: &DatabaseUuid,
        collection: &CollectionUuid,
        segment_id: &SegmentUuid,
    ) -> String {
        format!(
            "tenant/{}/database/{}/collection/{}/segment/{}",
            tenant, database_id, collection, segment_id
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SegmentShard {
    pub id: SegmentUuid,
    pub r#type: SegmentType,
    pub scope: SegmentScope,
    pub collection: CollectionUuid,
    pub metadata: Option<Metadata>,
    pub file_path: HashMap<String, String>,
}

impl SegmentShard {
    pub fn construct_prefix_path(&self, tenant: &str, database_id: &DatabaseUuid) -> String {
        Segment::construct_prefix_path_impl(tenant, database_id, &self.collection, &self.id)
    }
}

#[derive(Error, Debug)]
pub enum SegmentShardError {
    #[error("Empty path vector for key '{0}'")]
    EmptyPathVector(String),
    #[error("Empty path string for key '{0}'")]
    EmptyPathString(String),
    #[error("Shard index {index} out of bounds for key '{key}' (len {len})")]
    ShardIndexOutOfBounds { key: String, index: u32, len: usize },
    #[error("Mismatched shard counts: key '{key}' has {actual} entries, expected {expected}")]
    MismatchedShardCounts {
        key: String,
        actual: usize,
        expected: usize,
    },
    #[error("No shards found")]
    EmptyShards,
}

impl ChromaError for SegmentShardError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

/// Error returned by [`Segment::matches_schema`] when the on-disk file_path
/// shape does not match what the collection schema implies.
#[derive(Error, Debug)]
pub enum SchemaMismatchError {
    #[error("Schema/file_path mismatch on segment {segment_id} (scope {scope:?}): {detail}")]
    Mismatch {
        segment_id: SegmentUuid,
        scope: SegmentScope,
        detail: String,
    },
    #[error("Error inspecting shards: {0}")]
    ShardError(SegmentShardError),
}

impl ChromaError for SchemaMismatchError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::Mismatch { .. } => ErrorCodes::FailedPrecondition,
            Self::ShardError(e) => e.code(),
        }
    }
}

impl TryFrom<(&Segment, u32)> for SegmentShard {
    type Error = SegmentShardError;

    fn try_from((segment, shard_index): (&Segment, u32)) -> Result<Self, Self::Error> {
        let mut file_path = HashMap::new();
        // If there are no shards in the filepaths this for loop won't
        // run and this function will return an empty SegmentShard.
        for (key, paths) in &segment.file_path {
            match paths.get(shard_index as usize) {
                Some(path) => {
                    // During rebuild, clear_shard_file_paths sets paths to empty strings
                    // to signal "create new". Skip these so writers see missing keys instead.
                    if !path.is_empty() {
                        file_path.insert(key.clone(), path.clone());
                    }
                }
                None if paths.is_empty() => {
                    return Err(SegmentShardError::EmptyPathVector(key.clone()));
                }
                None => {
                    return Err(SegmentShardError::ShardIndexOutOfBounds {
                        key: key.clone(),
                        index: shard_index,
                        len: paths.len(),
                    });
                }
            }
        }

        Ok(SegmentShard {
            id: segment.id,
            r#type: segment.r#type,
            scope: segment.scope.clone(),
            collection: segment.collection,
            metadata: segment.metadata.clone(),
            file_path,
        })
    }
}

impl Segment {
    pub fn new_shard(&self) -> SegmentShard {
        SegmentShard {
            id: self.id,
            r#type: self.r#type,
            scope: self.scope.clone(),
            collection: self.collection,
            metadata: self.metadata.clone(),
            file_path: HashMap::new(),
        }
    }
}

#[derive(Error, Debug)]
pub enum SegmentConversionError {
    #[error("Invalid UUID")]
    InvalidUuid,
    #[error(transparent)]
    MetadataValueConversionError(#[from] MetadataValueConversionError),
    #[error(transparent)]
    SegmentScopeConversionError(#[from] SegmentScopeConversionError),
    #[error("Invalid segment type")]
    InvalidSegmentType,
}

impl ChromaError for SegmentConversionError {
    fn code(&self) -> ErrorCodes {
        match self {
            SegmentConversionError::InvalidUuid => ErrorCodes::InvalidArgument,
            SegmentConversionError::InvalidSegmentType => ErrorCodes::InvalidArgument,
            SegmentConversionError::SegmentScopeConversionError(e) => e.code(),
            SegmentConversionError::MetadataValueConversionError(e) => e.code(),
        }
    }
}

impl From<SegmentConversionError> for Status {
    fn from(value: SegmentConversionError) -> Self {
        Status::invalid_argument(value.to_string())
    }
}

impl TryFrom<chroma_proto::Segment> for Segment {
    type Error = SegmentConversionError;

    fn try_from(proto_segment: chroma_proto::Segment) -> Result<Self, Self::Error> {
        let mut proto_segment = proto_segment;

        let segment_uuid = match SegmentUuid::from_str(&proto_segment.id) {
            Ok(uuid) => uuid,
            Err(_) => return Err(SegmentConversionError::InvalidUuid),
        };
        let collection_uuid = match Uuid::try_parse(&proto_segment.collection) {
            Ok(uuid) => uuid,
            Err(_) => return Err(SegmentConversionError::InvalidUuid),
        };
        let collection_uuid = CollectionUuid(collection_uuid);
        let segment_metadata: Option<Metadata> = match proto_segment.metadata {
            Some(proto_metadata) => match proto_metadata.try_into() {
                Ok(metadata) => Some(metadata),
                Err(e) => return Err(SegmentConversionError::MetadataValueConversionError(e)),
            },
            None => None,
        };
        let scope: SegmentScope = match proto_segment.scope.try_into() {
            Ok(scope) => scope,
            Err(e) => return Err(SegmentConversionError::SegmentScopeConversionError(e)),
        };

        let segment_type: SegmentType = proto_segment.r#type.as_str().try_into()?;

        let mut file_paths = HashMap::new();
        let drain = proto_segment.file_paths.drain();
        for (key, value) in drain {
            file_paths.insert(key, value.paths);
        }

        Ok(Segment {
            id: segment_uuid,
            r#type: segment_type,
            scope,
            collection: collection_uuid,
            metadata: segment_metadata,
            file_path: file_paths,
        })
    }
}

impl From<Segment> for chroma_proto::Segment {
    fn from(value: Segment) -> Self {
        Self {
            id: value.id.0.to_string(),
            r#type: value.r#type.into(),
            scope: chroma_proto::SegmentScope::from(value.scope) as i32,
            collection: value.collection.0.to_string(),
            metadata: value.metadata.map(Into::into),
            file_paths: value
                .file_path
                .into_iter()
                .map(|(name, paths)| (name, chroma_proto::FilePaths { paths }))
                .collect(),
        }
    }
}

pub fn test_segment(collection_uuid: CollectionUuid, scope: SegmentScope) -> Segment {
    let r#type = match scope {
        SegmentScope::METADATA => SegmentType::BlockfileMetadata,
        SegmentScope::RECORD => SegmentType::BlockfileRecord,
        SegmentScope::VECTOR => SegmentType::HnswDistributed,
        SegmentScope::SQLITE => unimplemented!("Sqlite segment is not implemented"),
    };
    Segment {
        id: SegmentUuid::new(),
        r#type,
        scope,
        collection: collection_uuid,
        metadata: None,
        file_path: HashMap::new(),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::MetadataValue;

    #[test]
    fn test_segment_try_from() {
        let mut metadata = chroma_proto::UpdateMetadata {
            metadata: HashMap::new(),
        };
        metadata.metadata.insert(
            "foo".to_string(),
            chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::IntValue(42)),
            },
        );
        let proto_segment = chroma_proto::Segment {
            id: "00000000-0000-0000-0000-000000000000".to_string(),
            r#type: "urn:chroma:segment/vector/hnsw-distributed".to_string(),
            scope: chroma_proto::SegmentScope::Vector as i32,
            collection: "00000000-0000-0000-0000-000000000000".to_string(),
            metadata: Some(metadata),
            file_paths: HashMap::new(),
        };
        let converted_segment: Segment = proto_segment.try_into().unwrap();
        assert_eq!(converted_segment.id, SegmentUuid(Uuid::nil()));
        assert_eq!(converted_segment.r#type, SegmentType::HnswDistributed);
        assert_eq!(converted_segment.scope, SegmentScope::VECTOR);
        assert_eq!(converted_segment.collection, CollectionUuid(Uuid::nil()));
        let metadata = converted_segment.metadata.unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata.get("foo").unwrap(), &MetadataValue::Int(42));
    }

    #[test]
    fn test_segment_construct_prefix_path() {
        let segment = Segment {
            id: SegmentUuid(Uuid::nil()),
            r#type: SegmentType::BlockfileMetadata,
            scope: SegmentScope::METADATA,
            collection: CollectionUuid(Uuid::nil()),
            metadata: None,
            file_path: HashMap::new(),
        };
        let tenant = "test_tenant";
        let database_id = &DatabaseUuid(Uuid::nil());
        let prefix_path = segment.construct_prefix_path(tenant, database_id);
        assert_eq!(
            prefix_path,
            "tenant/test_tenant/database/00000000-0000-0000-0000-000000000000/collection/00000000-0000-0000-0000-000000000000/segment/00000000-0000-0000-0000-000000000000"
        );
    }

    #[test]
    fn test_segment_extract_prefix_and_id() {
        let path = "tenant/test_tenant/database/00000000-0000-0000-0000-000000000000/collection/00000000-0000-0000-0000-000000000000/segment/00000000-0000-0000-0000-000000000000/00000000-0000-0000-0000-000000000001";
        let (prefix, id) =
            Segment::extract_prefix_and_id(path).expect("Failed to extract prefix and id");
        assert_eq!(
            prefix,
            "tenant/test_tenant/database/00000000-0000-0000-0000-000000000000/collection/00000000-0000-0000-0000-000000000000/segment/00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(
            id,
            Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("Cannot happen")
        );
    }

    #[test]
    fn test_segment_extract_prefix_and_id_legacy() {
        let path = "00000000-0000-0000-0000-000000000001";
        let (prefix, id) =
            Segment::extract_prefix_and_id(path).expect("Failed to extract prefix and id");
        assert_eq!(prefix, "");
        assert_eq!(
            id,
            Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("Cannot happen")
        );
    }

    #[test]
    fn test_num_shards_empty_file_path() {
        let segment = Segment {
            id: SegmentUuid(Uuid::nil()),
            r#type: SegmentType::BlockfileRecord,
            scope: SegmentScope::RECORD,
            collection: CollectionUuid(Uuid::nil()),
            metadata: None,
            file_path: HashMap::new(),
        };
        assert_eq!(segment.num_shards().unwrap(), 1);
    }

    #[test]
    fn test_num_shards_single_shard() {
        let mut file_path = HashMap::new();
        file_path.insert("key_a".to_string(), vec!["path_a_0".to_string()]);
        file_path.insert("key_b".to_string(), vec!["path_b_0".to_string()]);
        let segment = Segment {
            id: SegmentUuid(Uuid::nil()),
            r#type: SegmentType::BlockfileRecord,
            scope: SegmentScope::RECORD,
            collection: CollectionUuid(Uuid::nil()),
            metadata: None,
            file_path,
        };
        assert_eq!(segment.num_shards().unwrap(), 1);
    }

    #[test]
    fn test_num_shards_multi_shard() {
        let mut file_path = HashMap::new();
        file_path.insert(
            "key_a".to_string(),
            vec!["a0".to_string(), "a1".to_string(), "a2".to_string()],
        );
        file_path.insert(
            "key_b".to_string(),
            vec!["b0".to_string(), "b1".to_string(), "b2".to_string()],
        );
        let segment = Segment {
            id: SegmentUuid(Uuid::nil()),
            r#type: SegmentType::BlockfileRecord,
            scope: SegmentScope::RECORD,
            collection: CollectionUuid(Uuid::nil()),
            metadata: None,
            file_path,
        };
        assert_eq!(segment.num_shards().unwrap(), 3);
    }

    #[test]
    fn test_num_shards_mismatched_lengths() {
        let mut file_path = HashMap::new();
        file_path.insert(
            "key_a".to_string(),
            vec!["a0".to_string(), "a1".to_string()],
        );
        file_path.insert(
            "key_b".to_string(),
            vec!["b0".to_string(), "b1".to_string(), "b2".to_string()],
        );
        let segment = Segment {
            id: SegmentUuid(Uuid::nil()),
            r#type: SegmentType::BlockfileRecord,
            scope: SegmentScope::RECORD,
            collection: CollectionUuid(Uuid::nil()),
            metadata: None,
            file_path,
        };
        let err = segment.num_shards().unwrap_err();
        assert!(matches!(
            err,
            SegmentShardError::MismatchedShardCounts { .. }
        ));
    }

    #[test]
    fn test_clear_shard_file_paths() {
        // Create a segment with 3 shards
        let mut file_path = HashMap::new();
        file_path.insert(
            "metadata".to_string(),
            vec![
                "/path/to/shard0/metadata".to_string(),
                "/path/to/shard1/metadata".to_string(),
                "/path/to/shard2/metadata".to_string(),
            ],
        );
        file_path.insert(
            "index".to_string(),
            vec![
                "/path/to/shard0/index".to_string(),
                "/path/to/shard1/index".to_string(),
                "/path/to/shard2/index".to_string(),
            ],
        );
        file_path.insert(
            "data".to_string(),
            vec![
                "/path/to/shard0/data".to_string(),
                "/path/to/shard1/data".to_string(),
                "/path/to/shard2/data".to_string(),
            ],
        );

        let mut segment = Segment {
            id: SegmentUuid(Uuid::nil()),
            r#type: SegmentType::BlockfileRecord,
            scope: SegmentScope::RECORD,
            collection: CollectionUuid(Uuid::nil()),
            metadata: None,
            file_path,
        };

        // Clear only shard 1's file paths
        segment.clear_shard_file_paths(1);

        // Verify shard 0 and shard 2 are untouched
        assert_eq!(
            segment.file_path.get("metadata").unwrap()[0],
            "/path/to/shard0/metadata"
        );
        assert_eq!(
            segment.file_path.get("metadata").unwrap()[2],
            "/path/to/shard2/metadata"
        );
        assert_eq!(
            segment.file_path.get("index").unwrap()[0],
            "/path/to/shard0/index"
        );
        assert_eq!(
            segment.file_path.get("index").unwrap()[2],
            "/path/to/shard2/index"
        );
        assert_eq!(
            segment.file_path.get("data").unwrap()[0],
            "/path/to/shard0/data"
        );
        assert_eq!(
            segment.file_path.get("data").unwrap()[2],
            "/path/to/shard2/data"
        );

        // Verify shard 1 is cleared (empty string)
        assert_eq!(segment.file_path.get("metadata").unwrap()[1], "");
        assert_eq!(segment.file_path.get("index").unwrap()[1], "");
        assert_eq!(segment.file_path.get("data").unwrap()[1], "");

        // Test clearing a shard index that's out of bounds (should not panic)
        segment.clear_shard_file_paths(5);

        // Verify nothing changed for the out-of-bounds case
        assert_eq!(
            segment.file_path.get("metadata").unwrap()[0],
            "/path/to/shard0/metadata"
        );
    }

    // ── matches_schema tests ────────────────────────────────────────────────

    use crate::collection_configuration::KnnIndex;
    use crate::collection_schema::{
        Quantization, Schema, SparseIndexAlgorithm, SparseVectorIndexConfig, SparseVectorIndexType,
        SparseVectorValueType,
    };

    /// Helper: create a Spann schema (full precision, no quantization).
    fn spann_schema() -> Schema {
        Schema::new_default(KnnIndex::Spann)
    }

    /// Helper: create a Spann schema with quantization enabled.
    fn quantized_spann_schema() -> Schema {
        let mut schema = Schema::new_default(KnnIndex::Spann);
        if let Some(spann_config) = schema.get_spann_config_mut() {
            spann_config.quantize = Quantization::FourBitRabitQWithUSearch;
        }
        schema
    }

    /// Helper: create an HNSW schema.
    fn hnsw_schema() -> Schema {
        Schema::new_default(KnnIndex::Hnsw)
    }

    /// Helper: create a schema with sparse index enabled (Wand).
    fn sparse_wand_schema() -> Schema {
        let mut schema = Schema::new_default(KnnIndex::Hnsw);
        schema
            .keys
            .entry("sparse_key".to_string())
            .or_default()
            .sparse_vector = Some(SparseVectorValueType {
            sparse_vector_index: Some(SparseVectorIndexType {
                enabled: true,
                config: SparseVectorIndexConfig {
                    embedding_function: None,
                    source_key: None,
                    bm25: None,
                    algorithm: SparseIndexAlgorithm::Wand,
                },
            }),
        });
        schema
    }

    /// Helper: create a schema with sparse index enabled (MaxScore).
    fn sparse_maxscore_schema() -> Schema {
        let mut schema = sparse_wand_schema();
        schema.set_sparse_algorithm(SparseIndexAlgorithm::MaxScore);
        schema
    }

    fn make_vector_segment(file_path: HashMap<String, Vec<String>>) -> Segment {
        Segment {
            id: SegmentUuid(Uuid::nil()),
            r#type: SegmentType::Spann,
            scope: SegmentScope::VECTOR,
            collection: CollectionUuid(Uuid::nil()),
            metadata: None,
            file_path,
        }
    }

    fn make_metadata_segment(file_path: HashMap<String, Vec<String>>) -> Segment {
        Segment {
            id: SegmentUuid(Uuid::nil()),
            r#type: SegmentType::BlockfileMetadata,
            scope: SegmentScope::METADATA,
            collection: CollectionUuid(Uuid::nil()),
            metadata: None,
            file_path,
        }
    }

    #[test]
    fn test_matches_schema_empty_file_path() {
        let seg = make_vector_segment(HashMap::new());
        assert!(seg.matches_schema(&spann_schema()).is_ok());
        assert!(seg.matches_schema(&quantized_spann_schema()).is_ok());
        assert!(seg.matches_schema(&hnsw_schema()).is_ok());
    }

    #[test]
    fn test_matches_schema_spann_matches_spann() {
        let mut fp = HashMap::new();
        fp.insert(HNSW_PATH.to_string(), vec!["path/a".to_string()]);
        fp.insert(VERSION_MAP_PATH.to_string(), vec!["path/b".to_string()]);
        fp.insert(POSTING_LIST_PATH.to_string(), vec!["path/c".to_string()]);
        fp.insert(MAX_HEAD_ID_BF_PATH.to_string(), vec!["path/d".to_string()]);
        let seg = make_vector_segment(fp);
        assert!(seg.matches_schema(&spann_schema()).is_ok());
    }

    #[test]
    fn test_matches_schema_qspann_matches_qspann() {
        let mut fp = HashMap::new();
        fp.insert(
            QUANTIZED_SPANN_CLUSTER.to_string(),
            vec!["path/a".to_string()],
        );
        fp.insert(
            QUANTIZED_SPANN_SCALAR_METADATA.to_string(),
            vec!["path/b".to_string()],
        );
        fp.insert(
            QUANTIZED_SPANN_EMBEDDING_METADATA.to_string(),
            vec!["path/c".to_string()],
        );
        fp.insert(
            QUANTIZED_SPANN_RAW_CENTROID.to_string(),
            vec!["path/d".to_string()],
        );
        fp.insert(
            QUANTIZED_SPANN_QUANTIZED_CENTROID.to_string(),
            vec!["path/e".to_string()],
        );
        let seg = make_vector_segment(fp);
        assert!(seg.matches_schema(&quantized_spann_schema()).is_ok());
    }

    #[test]
    fn test_matches_schema_spann_on_disk_qspann_in_schema() {
        let mut fp = HashMap::new();
        fp.insert(HNSW_PATH.to_string(), vec!["path/a".to_string()]);
        fp.insert(VERSION_MAP_PATH.to_string(), vec!["path/b".to_string()]);
        fp.insert(POSTING_LIST_PATH.to_string(), vec!["path/c".to_string()]);
        fp.insert(MAX_HEAD_ID_BF_PATH.to_string(), vec!["path/d".to_string()]);
        let seg = make_vector_segment(fp);
        let err = seg.matches_schema(&quantized_spann_schema()).unwrap_err();
        assert!(matches!(err, SchemaMismatchError::Mismatch { .. }));
    }

    #[test]
    fn test_matches_schema_qspann_on_disk_spann_in_schema() {
        let mut fp = HashMap::new();
        fp.insert(
            QUANTIZED_SPANN_CLUSTER.to_string(),
            vec!["path/a".to_string()],
        );
        fp.insert(
            QUANTIZED_SPANN_SCALAR_METADATA.to_string(),
            vec!["path/b".to_string()],
        );
        fp.insert(
            QUANTIZED_SPANN_EMBEDDING_METADATA.to_string(),
            vec!["path/c".to_string()],
        );
        fp.insert(
            QUANTIZED_SPANN_RAW_CENTROID.to_string(),
            vec!["path/d".to_string()],
        );
        fp.insert(
            QUANTIZED_SPANN_QUANTIZED_CENTROID.to_string(),
            vec!["path/e".to_string()],
        );
        let seg = make_vector_segment(fp);
        let err = seg.matches_schema(&spann_schema()).unwrap_err();
        assert!(matches!(err, SchemaMismatchError::Mismatch { .. }));
    }

    #[test]
    fn test_matches_schema_hnsw_schema_always_ok() {
        // HNSW schema does not check file_path — no migration variants.
        let mut fp = HashMap::new();
        fp.insert(HNSW_PATH.to_string(), vec!["path/a".to_string()]);
        let seg = make_vector_segment(fp);
        assert!(seg.matches_schema(&hnsw_schema()).is_ok());
    }

    fn make_record_segment(file_path: HashMap<String, Vec<String>>) -> Segment {
        Segment {
            id: SegmentUuid(Uuid::nil()),
            r#type: SegmentType::BlockfileRecord,
            scope: SegmentScope::RECORD,
            collection: CollectionUuid(Uuid::nil()),
            metadata: None,
            file_path,
        }
    }

    #[test]
    fn test_matches_schema_record_empty() {
        let seg = make_record_segment(HashMap::new());
        assert!(seg.matches_schema(&spann_schema()).is_ok());
    }

    #[test]
    fn test_matches_schema_record_complete() {
        let mut fp = HashMap::new();
        fp.insert(USER_ID_TO_OFFSET_ID.to_string(), vec!["a".to_string()]);
        fp.insert(OFFSET_ID_TO_USER_ID.to_string(), vec!["b".to_string()]);
        fp.insert(OFFSET_ID_TO_DATA.to_string(), vec!["c".to_string()]);
        fp.insert(MAX_OFFSET_ID.to_string(), vec!["d".to_string()]);
        let seg = make_record_segment(fp);
        assert!(seg.matches_schema(&spann_schema()).is_ok());
    }

    #[test]
    fn test_matches_schema_record_missing_key() {
        let mut fp = HashMap::new();
        fp.insert(USER_ID_TO_OFFSET_ID.to_string(), vec!["a".to_string()]);
        fp.insert(OFFSET_ID_TO_USER_ID.to_string(), vec!["b".to_string()]);
        fp.insert(OFFSET_ID_TO_DATA.to_string(), vec!["c".to_string()]);
        // MAX_OFFSET_ID intentionally omitted
        let seg = make_record_segment(fp);
        let err = seg.matches_schema(&spann_schema()).unwrap_err();
        assert!(matches!(err, SchemaMismatchError::Mismatch { .. }));
    }

    #[test]
    fn test_matches_schema_sparse_wand_matches_wand() {
        let mut fp = HashMap::new();
        fp.insert(SPARSE_MAX.to_string(), vec!["path/a".to_string()]);
        fp.insert(SPARSE_OFFSET_VALUE.to_string(), vec!["path/b".to_string()]);
        let seg = make_metadata_segment(fp);
        assert!(seg.matches_schema(&sparse_wand_schema()).is_ok());
    }

    #[test]
    fn test_matches_schema_sparse_maxscore_matches_maxscore() {
        let mut fp = HashMap::new();
        fp.insert(SPARSE_POSTING.to_string(), vec!["path/a".to_string()]);
        let seg = make_metadata_segment(fp);
        assert!(seg.matches_schema(&sparse_maxscore_schema()).is_ok());
    }

    #[test]
    fn test_matches_schema_sparse_wand_on_disk_maxscore_in_schema() {
        let mut fp = HashMap::new();
        fp.insert(SPARSE_MAX.to_string(), vec!["path/a".to_string()]);
        fp.insert(SPARSE_OFFSET_VALUE.to_string(), vec!["path/b".to_string()]);
        let seg = make_metadata_segment(fp);
        let err = seg.matches_schema(&sparse_maxscore_schema()).unwrap_err();
        assert!(matches!(err, SchemaMismatchError::Mismatch { .. }));
    }

    #[test]
    fn test_matches_schema_sparse_maxscore_on_disk_wand_in_schema() {
        let mut fp = HashMap::new();
        fp.insert(SPARSE_POSTING.to_string(), vec!["path/a".to_string()]);
        let seg = make_metadata_segment(fp);
        let err = seg.matches_schema(&sparse_wand_schema()).unwrap_err();
        assert!(matches!(err, SchemaMismatchError::Mismatch { .. }));
    }

    #[test]
    fn test_matches_schema_sparse_empty_file_path() {
        let seg = make_metadata_segment(HashMap::new());
        assert!(seg.matches_schema(&sparse_maxscore_schema()).is_ok());
        assert!(seg.matches_schema(&sparse_wand_schema()).is_ok());
    }

    #[test]
    fn test_matches_schema_sparse_disabled_index_no_check() {
        // Schema with no sparse index enabled — should not check.
        let schema = hnsw_schema();
        assert!(!schema.is_sparse_index_enabled());
        let mut fp = HashMap::new();
        fp.insert(SPARSE_POSTING.to_string(), vec!["path/a".to_string()]);
        let seg = make_metadata_segment(fp);
        assert!(seg.matches_schema(&schema).is_ok());
    }

    #[test]
    fn test_matches_schema_sparse_both_keys_present() {
        // Both Wand and MaxScore keys present — invalid state.
        let mut fp = HashMap::new();
        fp.insert(SPARSE_POSTING.to_string(), vec!["path/a".to_string()]);
        fp.insert(SPARSE_MAX.to_string(), vec!["path/b".to_string()]);
        fp.insert(SPARSE_OFFSET_VALUE.to_string(), vec!["path/c".to_string()]);
        let seg = make_metadata_segment(fp);
        let err = seg.matches_schema(&sparse_maxscore_schema()).unwrap_err();
        assert!(matches!(err, SchemaMismatchError::Mismatch { .. }));
    }
}
