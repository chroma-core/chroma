use super::{Metadata, MetadataValueConversionError};
use crate::{
    chroma_proto,
    errors::{ChromaError, ErrorCodes},
};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Collection {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) topic: String,
    pub(crate) metadata: Option<Metadata>,
    pub(crate) dimension: Option<i32>,
    pub(crate) tenant: String,
    pub(crate) database: String,
    pub(crate) log_position: i64,
    pub(crate) version: i32,
}

#[derive(Error, Debug)]
pub(crate) enum CollectionConversionError {
    #[error("Invalid UUID")]
    InvalidUuid,
    #[error(transparent)]
    MetadataValueConversionError(#[from] MetadataValueConversionError),
}

impl ChromaError for CollectionConversionError {
    fn code(&self) -> crate::errors::ErrorCodes {
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
        let collection_metadata: Option<Metadata> = match proto_collection.metadata {
            Some(proto_metadata) => match proto_metadata.try_into() {
                Ok(metadata) => Some(metadata),
                Err(e) => return Err(CollectionConversionError::MetadataValueConversionError(e)),
            },
            None => None,
        };
        Ok(Collection {
            id: collection_uuid,
            name: proto_collection.name,
            topic: proto_collection.topic,
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
            topic: "bar".to_string(),
            metadata: None,
            dimension: None,
            tenant: "baz".to_string(),
            database: "qux".to_string(),
            log_position: 0,
            version: 0,
        };
        let converted_collection: Collection = proto_collection.try_into().unwrap();
        assert_eq!(converted_collection.id, Uuid::nil());
        assert_eq!(converted_collection.name, "foo".to_string());
        assert_eq!(converted_collection.topic, "bar".to_string());
        assert_eq!(converted_collection.metadata, None);
        assert_eq!(converted_collection.dimension, None);
        assert_eq!(converted_collection.tenant, "baz".to_string());
        assert_eq!(converted_collection.database, "qux".to_string());
    }
}
