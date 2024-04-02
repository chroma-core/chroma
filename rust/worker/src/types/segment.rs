use super::{Metadata, MetadataValueConversionError, SegmentScope, SegmentScopeConversionError};
use crate::{
    chroma_proto,
    errors::{ChromaError, ErrorCodes},
};
use std::collections::HashMap;
use std::vec::Vec;
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SegmentType {
    HnswDistributed,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Segment {
    pub(crate) id: Uuid,
    pub(crate) r#type: SegmentType,
    pub(crate) scope: SegmentScope,
    pub(crate) collection: Option<Uuid>,
    pub(crate) metadata: Option<Metadata>,
    pub(crate) file_path: HashMap<String, Vec<String>>,
}

#[derive(Error, Debug)]
pub(crate) enum SegmentConversionError {
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
    fn code(&self) -> crate::errors::ErrorCodes {
        match self {
            SegmentConversionError::InvalidUuid => ErrorCodes::InvalidArgument,
            SegmentConversionError::InvalidSegmentType => ErrorCodes::InvalidArgument,
            SegmentConversionError::SegmentScopeConversionError(e) => e.code(),
            SegmentConversionError::MetadataValueConversionError(e) => e.code(),
        }
    }
}

impl TryFrom<chroma_proto::Segment> for Segment {
    type Error = SegmentConversionError;

    fn try_from(proto_segment: chroma_proto::Segment) -> Result<Self, Self::Error> {
        let mut proto_segment = proto_segment;

        let segment_uuid = match Uuid::try_parse(&proto_segment.id) {
            Ok(uuid) => uuid,
            Err(_) => return Err(SegmentConversionError::InvalidUuid),
        };
        let collection_uuid = match proto_segment.collection {
            Some(collection_id) => match Uuid::try_parse(&collection_id) {
                Ok(uuid) => Some(uuid),
                Err(_) => return Err(SegmentConversionError::InvalidUuid),
            },
            // The UUID can be none in the local version of chroma but not distributed
            None => return Err(SegmentConversionError::InvalidUuid),
        };
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

        let segment_type = match proto_segment.r#type.as_str() {
            "urn:chroma:segment/vector/hnsw-distributed" => SegmentType::HnswDistributed,
            _ => {
                return Err(SegmentConversionError::InvalidUuid);
            }
        };

        let mut file_paths = HashMap::new();
        let drain = proto_segment.file_paths.drain();
        for (key, mut value) in drain {
            file_paths.insert(key, value.paths);
        }

        Ok(Segment {
            id: segment_uuid,
            r#type: segment_type,
            scope: scope,
            collection: collection_uuid,
            metadata: segment_metadata,
            file_path: file_paths,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::types::MetadataValue;

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
            collection: Some("00000000-0000-0000-0000-000000000000".to_string()),
            metadata: Some(metadata),
            file_paths: HashMap::new(),
        };
        let converted_segment: Segment = proto_segment.try_into().unwrap();
        assert_eq!(converted_segment.id, Uuid::nil());
        assert_eq!(converted_segment.r#type, SegmentType::HnswDistributed);
        assert_eq!(converted_segment.scope, SegmentScope::VECTOR);
        assert_eq!(converted_segment.collection, Some(Uuid::nil()));
        let metadata = converted_segment.metadata.unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata.get("foo").unwrap(), &MetadataValue::Int(42));
    }
}
