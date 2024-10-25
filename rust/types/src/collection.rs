use super::{Metadata, MetadataValueConversionError};
use crate::chroma_proto;
use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;
use uuid::Uuid;

/// CollectionUuid is a wrapper around Uuid to provide a type for the collection id.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CollectionUuid(pub Uuid);

impl CollectionUuid {
    pub fn new() -> Self {
        CollectionUuid(Uuid::new_v4())
    }
}

impl std::str::FromStr for CollectionUuid {
    type Err = CollectionConversionError;

    fn from_str(s: &str) -> Result<Self, CollectionConversionError> {
        match Uuid::parse_str(s) {
            Ok(uuid) => Ok(CollectionUuid(uuid)),
            Err(_) => Err(CollectionConversionError::InvalidUuid),
        }
    }
}

impl std::fmt::Display for CollectionUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "collection-{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Collection {
    pub collection_id: CollectionUuid,
    pub name: String,
    pub metadata: Option<Metadata>,
    pub dimension: Option<i32>,
    pub tenant: String,
    pub database: String,
    pub log_position: i64,
    pub version: i32,
}

#[derive(Error, Debug)]
pub enum CollectionConversionError {
    #[error("Invalid UUID")]
    InvalidUuid,
    #[error(transparent)]
    MetadataValueConversionError(#[from] MetadataValueConversionError),
}

impl ChromaError for CollectionConversionError {
    fn code(&self) -> ErrorCodes {
        match self {
            CollectionConversionError::InvalidUuid => ErrorCodes::InvalidArgument,
            CollectionConversionError::MetadataValueConversionError(e) => e.code(),
        }
    }
}

impl TryFrom<chroma_proto::Collection> for Collection {
    type Error = CollectionConversionError;

    fn try_from(proto_collection: chroma_proto::Collection) -> Result<Self, Self::Error> {
        let collection_uuid = match Uuid::try_parse(&proto_collection.id) {
            Ok(uuid) => uuid,
            Err(_) => return Err(CollectionConversionError::InvalidUuid),
        };
        let collection_id = CollectionUuid(collection_uuid);
        let collection_metadata: Option<Metadata> = match proto_collection.metadata {
            Some(proto_metadata) => match proto_metadata.try_into() {
                Ok(metadata) => Some(metadata),
                Err(e) => return Err(CollectionConversionError::MetadataValueConversionError(e)),
            },
            None => None,
        };
        Ok(Collection {
            collection_id,
            name: proto_collection.name,
            metadata: collection_metadata,
            dimension: proto_collection.dimension,
            tenant: proto_collection.tenant,
            database: proto_collection.database,
            log_position: proto_collection.log_position,
            version: proto_collection.version,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_collection_try_from() {
        let proto_collection = chroma_proto::Collection {
            id: "00000000-0000-0000-0000-000000000000".to_string(),
            name: "foo".to_string(),
            configuration_json_str: "{\"a\": \"param\", \"b\": \"param2\", \"3\": true}"
                .to_string(),
            metadata: None,
            dimension: None,
            tenant: "baz".to_string(),
            database: "qux".to_string(),
            log_position: 0,
            version: 0,
        };
        let converted_collection: Collection = proto_collection.try_into().unwrap();
        assert_eq!(
            converted_collection.collection_id,
            CollectionUuid(Uuid::nil())
        );
        assert_eq!(converted_collection.name, "foo".to_string());
        assert_eq!(converted_collection.metadata, None);
        assert_eq!(converted_collection.dimension, None);
        assert_eq!(converted_collection.tenant, "baz".to_string());
        assert_eq!(converted_collection.database, "qux".to_string());
    }
}
