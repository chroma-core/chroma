use super::{Metadata, MetadataValueConversionError};
use crate::{
    chroma_proto, test_segment, CollectionConfiguration, InternalCollectionConfiguration, Segment,
    SegmentScope,
};
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;
use uuid::Uuid;

#[cfg(feature = "pyo3")]
use pyo3::types::PyAnyMethods;

/// CollectionUuid is a wrapper around Uuid to provide a type for the collection id.
#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    Deserialize,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    ToSchema,
)]
pub struct CollectionUuid(pub Uuid);

impl CollectionUuid {
    pub fn new() -> Self {
        CollectionUuid(Uuid::new_v4())
    }
}

impl std::str::FromStr for CollectionUuid {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Uuid::parse_str(s) {
            Ok(uuid) => Ok(CollectionUuid(uuid)),
            Err(err) => Err(err),
        }
    }
}

impl std::fmt::Display for CollectionUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

fn serialize_internal_collection_configuration<S: serde::Serializer>(
    config: &InternalCollectionConfiguration,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let collection_config: CollectionConfiguration = config.clone().into();
    collection_config.serialize(serializer)
}

fn deserialize_internal_collection_configuration<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<InternalCollectionConfiguration, D::Error> {
    let collection_config = CollectionConfiguration::deserialize(deserializer)?;
    collection_config
        .try_into()
        .map_err(serde::de::Error::custom)
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct Collection {
    #[serde(rename = "id")]
    pub collection_id: CollectionUuid,
    pub name: String,
    #[serde(
        serialize_with = "serialize_internal_collection_configuration",
        deserialize_with = "deserialize_internal_collection_configuration",
        rename = "configuration_json"
    )]
    #[schema(value_type = CollectionConfiguration)]
    pub config: InternalCollectionConfiguration,
    pub metadata: Option<Metadata>,
    pub dimension: Option<i32>,
    pub tenant: String,
    pub database: String,
    pub log_position: i64,
    pub version: i32,
    #[serde(skip)]
    pub total_records_post_compaction: u64,
    #[serde(skip)]
    pub size_bytes_post_compaction: u64,
    #[serde(skip)]
    pub last_compaction_time_secs: u64,
    #[serde(skip)]
    pub version_file_path: Option<String>,
    #[serde(skip)]
    pub root_collection_id: Option<CollectionUuid>,
    #[serde(skip)]
    pub lineage_file_path: Option<String>,
}

impl Default for Collection {
    fn default() -> Self {
        Self {
            collection_id: CollectionUuid::new(),
            name: "".to_string(),
            config: InternalCollectionConfiguration::default_hnsw(),
            metadata: None,
            dimension: None,
            tenant: "".to_string(),
            database: "".to_string(),
            log_position: 0,
            version: 0,
            total_records_post_compaction: 0,
            size_bytes_post_compaction: 0,
            last_compaction_time_secs: 0,
            version_file_path: None,
            root_collection_id: None,
            lineage_file_path: None,
        }
    }
}

#[cfg(feature = "pyo3")]
#[pyo3::pymethods]
impl Collection {
    #[getter]
    fn id<'py>(&self, py: pyo3::Python<'py>) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let res = pyo3::prelude::PyModule::import(py, "uuid")?
            .getattr("UUID")?
            .call1((self.collection_id.to_string(),))?;
        Ok(res)
    }

    #[getter]
    fn configuration<'py>(
        &self,
        py: pyo3::Python<'py>,
    ) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let config: crate::CollectionConfiguration = self.config.clone().into();
        let config_json_str = serde_json::to_string(&config).unwrap();
        let res = pyo3::prelude::PyModule::import(py, "json")?
            .getattr("loads")?
            .call1((config_json_str,))?;
        Ok(res)
    }

    #[getter]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    pub fn metadata(&self) -> Option<Metadata> {
        self.metadata.clone()
    }

    #[getter]
    pub fn dimension(&self) -> Option<i32> {
        self.dimension
    }

    #[getter]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    #[getter]
    pub fn database(&self) -> &str {
        &self.database
    }
}

impl Collection {
    pub fn test_collection(dim: i32) -> Self {
        Collection {
            name: "test_collection".to_string(),
            dimension: Some(dim),
            tenant: "default_tenant".to_string(),
            database: "default_database".to_string(),
            ..Default::default()
        }
    }
}

#[derive(Error, Debug)]
pub enum CollectionConversionError {
    #[error("Invalid config: {0}")]
    InvalidConfig(#[from] serde_json::Error),
    #[error("Invalid UUID")]
    InvalidUuid,
    #[error(transparent)]
    MetadataValueConversionError(#[from] MetadataValueConversionError),
}

impl ChromaError for CollectionConversionError {
    fn code(&self) -> ErrorCodes {
        match self {
            CollectionConversionError::InvalidConfig(_) => ErrorCodes::InvalidArgument,
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
            config: serde_json::from_str(&proto_collection.configuration_json_str)?,
            metadata: collection_metadata,
            dimension: proto_collection.dimension,
            tenant: proto_collection.tenant,
            database: proto_collection.database,
            log_position: proto_collection.log_position,
            version: proto_collection.version,
            total_records_post_compaction: proto_collection.total_records_post_compaction,
            size_bytes_post_compaction: proto_collection.size_bytes_post_compaction,
            last_compaction_time_secs: proto_collection.last_compaction_time_secs,
            version_file_path: proto_collection.version_file_path,
            root_collection_id: proto_collection
                .root_collection_id
                .map(|uuid| CollectionUuid(Uuid::try_parse(&uuid).unwrap())),
            lineage_file_path: proto_collection.lineage_file_path,
        })
    }
}

#[derive(Error, Debug)]
pub enum CollectionToProtoError {
    #[error("Could not serialize config: {0}")]
    ConfigSerialization(#[from] serde_json::Error),
}

impl ChromaError for CollectionToProtoError {
    fn code(&self) -> ErrorCodes {
        match self {
            CollectionToProtoError::ConfigSerialization(_) => ErrorCodes::Internal,
        }
    }
}

impl TryFrom<Collection> for chroma_proto::Collection {
    type Error = CollectionToProtoError;

    fn try_from(value: Collection) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.collection_id.0.to_string(),
            name: value.name,
            configuration_json_str: serde_json::to_string(&value.config)?,
            metadata: value.metadata.map(Into::into),
            dimension: value.dimension,
            tenant: value.tenant,
            database: value.database,
            log_position: value.log_position,
            version: value.version,
            total_records_post_compaction: value.total_records_post_compaction,
            size_bytes_post_compaction: value.size_bytes_post_compaction,
            last_compaction_time_secs: value.last_compaction_time_secs,
            version_file_path: value.version_file_path,
            root_collection_id: value.root_collection_id.map(|uuid| uuid.0.to_string()),
            lineage_file_path: value.lineage_file_path,
        })
    }
}

#[derive(Clone, Debug)]
pub struct CollectionAndSegments {
    pub collection: Collection,
    pub metadata_segment: Segment,
    pub record_segment: Segment,
    pub vector_segment: Segment,
}

impl CollectionAndSegments {
    pub fn test(dim: i32) -> Self {
        let collection = Collection::test_collection(dim);
        let collection_uuid = collection.collection_id;
        Self {
            collection,
            metadata_segment: test_segment(collection_uuid, SegmentScope::METADATA),
            record_segment: test_segment(collection_uuid, SegmentScope::RECORD),
            vector_segment: test_segment(collection_uuid, SegmentScope::VECTOR),
        }
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
            total_records_post_compaction: 0,
            size_bytes_post_compaction: 0,
            last_compaction_time_secs: 0,
            version_file_path: Some("version_file_path".to_string()),
            root_collection_id: Some("00000000-0000-0000-0000-000000000000".to_string()),
            lineage_file_path: Some("lineage_file_path".to_string()),
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
        assert_eq!(converted_collection.total_records_post_compaction, 0);
        assert_eq!(converted_collection.size_bytes_post_compaction, 0);
        assert_eq!(converted_collection.last_compaction_time_secs, 0);
        assert_eq!(
            converted_collection.version_file_path,
            Some("version_file_path".to_string())
        );
        assert_eq!(
            converted_collection.root_collection_id,
            Some(CollectionUuid(Uuid::nil()))
        );
        assert_eq!(
            converted_collection.lineage_file_path,
            Some("lineage_file_path".to_string())
        );
    }
}
