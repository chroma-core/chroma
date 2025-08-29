use super::{
    CollectionUuid, Metadata, MetadataValueConversionError, SegmentScope,
    SegmentScopeConversionError,
};
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

pub const FULL_TEXT_PLS: &str = "full_text_pls";
pub const STRING_METADATA: &str = "string_metadata";
pub const BOOL_METADATA: &str = "bool_metadata";
pub const F32_METADATA: &str = "f32_metadata";
pub const U32_METADATA: &str = "u32_metadata";

pub const SPARSE_MAX: &str = "sparse_max";
pub const SPARSE_OFFSET_VALUE: &str = "sparse_offset_value";

pub const HNSW_PATH: &str = "hnsw_path";
pub const VERSION_MAP_PATH: &str = "version_map_path";
pub const POSTING_LIST_PATH: &str = "posting_list_path";
pub const MAX_HEAD_ID_BF_PATH: &str = "max_head_id_path";

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
    pub fn prefetch_supported(&self) -> bool {
        matches!(
            self.r#type,
            SegmentType::BlockfileMetadata | SegmentType::BlockfileRecord | SegmentType::Spann
        )
    }

    pub fn filepaths_to_prefetch(&self) -> Vec<String> {
        let mut res = Vec::new();
        match self.r#type {
            SegmentType::Spann => {
                if let Some(pl_path) = self.file_path.get(POSTING_LIST_PATH) {
                    res.extend(pl_path.iter().cloned());
                }
            }
            SegmentType::BlockfileMetadata | SegmentType::BlockfileRecord => {
                for paths in self.file_path.values() {
                    res.extend(paths.iter().cloned());
                }
            }
            _ => {}
        }
        res
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
        format!(
            "tenant/{}/database/{}/collection/{}/segment/{}",
            tenant, database_id, self.collection, self.id
        )
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
}
