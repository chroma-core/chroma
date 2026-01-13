use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use google_cloud_spanner::row::Row;

use super::{Metadata, MetadataValue, MetadataValueConversionError};
use crate::{
    chroma_proto, sysdb_errors::SysDbError, test_segment, CollectionConfiguration,
    InternalCollectionConfiguration, Schema, SchemaError, Segment, SegmentScope,
    UpdateCollectionConfiguration, UpdateMetadata,
};
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[cfg(feature = "pyo3")]
use pyo3::{exceptions::PyValueError, types::PyAnyMethods};

/// CollectionUuid is a wrapper around Uuid to provide a type for the collection id.
#[derive(
    Copy, Clone, Debug, Default, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CollectionUuid(pub Uuid);

/// DatabaseUuid is a wrapper around Uuid to provide a type for the database id.
#[derive(
    Copy, Clone, Debug, Default, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DatabaseUuid(pub Uuid);

impl DatabaseUuid {
    pub fn new() -> Self {
        DatabaseUuid(Uuid::new_v4())
    }
}

impl CollectionUuid {
    pub fn new() -> Self {
        CollectionUuid(Uuid::new_v4())
    }

    pub fn storage_prefix_for_log(&self) -> String {
        format!("logs/{}", self)
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

impl std::str::FromStr for DatabaseUuid {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Uuid::parse_str(s) {
            Ok(uuid) => Ok(DatabaseUuid(uuid)),
            Err(err) => Err(err),
        }
    }
}

impl std::fmt::Display for DatabaseUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
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
    #[cfg_attr(feature = "utoipa", schema(value_type = CollectionConfiguration))]
    pub config: InternalCollectionConfiguration,
    pub schema: Option<Schema>,
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
    #[serde(skip, default = "SystemTime::now")]
    pub updated_at: SystemTime,
    #[serde(skip)]
    pub database_id: DatabaseUuid,
    /// Number of consecutive compaction failures for this collection.
    /// Used by the scheduler to track and skip collections that repeatedly fail compaction.
    #[serde(skip)]
    pub compaction_failure_count: i32,
}

impl Default for Collection {
    fn default() -> Self {
        Self {
            collection_id: CollectionUuid::new(),
            name: "".to_string(),
            config: InternalCollectionConfiguration::default_hnsw(),
            schema: None,
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
            updated_at: SystemTime::now(),
            database_id: DatabaseUuid::new(),
            compaction_failure_count: 0,
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
    fn schema<'py>(
        &self,
        py: pyo3::Python<'py>,
    ) -> pyo3::PyResult<Option<pyo3::Bound<'py, pyo3::PyAny>>> {
        match self.schema.as_ref() {
            Some(schema) => {
                let schema_json = serde_json::to_string(schema)
                    .map_err(|err| PyValueError::new_err(err.to_string()))?;
                let res = pyo3::prelude::PyModule::import(py, "json")?
                    .getattr("loads")?
                    .call1((schema_json,))?;
                Ok(Some(res))
            }
            None => Ok(None),
        }
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
    /// Reconcile the collection schema and configuration when serving read requests.
    ///
    /// The read path needs to tolerate collections that only have a configuration persisted.
    /// This helper hydrates `schema` from the stored configuration when needed, or regenerates
    /// the configuration from the existing schema to keep both representations consistent.
    pub fn reconcile_schema_for_read(&mut self) -> Result<(), SchemaError> {
        if let Some(schema) = self.schema.as_ref() {
            self.config = InternalCollectionConfiguration::try_from(schema)
                .map_err(|reason| SchemaError::InvalidSchema { reason })?;
        } else {
            self.schema = Some(Schema::try_from(&self.config)?);
        }

        Ok(())
    }

    pub fn test_collection(dim: i32) -> Self {
        Collection {
            name: "test_collection".to_string(),
            dimension: Some(dim),
            tenant: "default_tenant".to_string(),
            database: "default_database".to_string(),
            database_id: DatabaseUuid::new(),
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
    #[error("Missing Database Id")]
    MissingDatabaseId,
}

impl ChromaError for CollectionConversionError {
    fn code(&self) -> ErrorCodes {
        match self {
            CollectionConversionError::InvalidConfig(_) => ErrorCodes::InvalidArgument,
            CollectionConversionError::InvalidUuid => ErrorCodes::InvalidArgument,
            CollectionConversionError::MetadataValueConversionError(e) => e.code(),
            CollectionConversionError::MissingDatabaseId => ErrorCodes::Internal,
        }
    }
}

impl TryFrom<chroma_proto::Collection> for Collection {
    type Error = CollectionConversionError;

    fn try_from(proto_collection: chroma_proto::Collection) -> Result<Self, Self::Error> {
        let collection_id = CollectionUuid::from_str(&proto_collection.id)
            .map_err(|_| CollectionConversionError::InvalidUuid)?;
        let collection_metadata: Option<Metadata> = match proto_collection.metadata {
            Some(proto_metadata) => match proto_metadata.try_into() {
                Ok(metadata) => Some(metadata),
                Err(e) => return Err(CollectionConversionError::MetadataValueConversionError(e)),
            },
            None => None,
        };
        // TODO(@codetheweb): this be updated to error with "missing field" once all SysDb deployments are up-to-date
        let updated_at = match proto_collection.updated_at {
            Some(updated_at) => {
                SystemTime::UNIX_EPOCH
                    + Duration::new(updated_at.seconds as u64, updated_at.nanos as u32)
            }
            None => SystemTime::now(),
        };
        let database_id = match proto_collection.database_id {
            Some(db_id) => DatabaseUuid::from_str(&db_id)
                .map_err(|_| CollectionConversionError::InvalidUuid)?,
            None => {
                return Err(CollectionConversionError::MissingDatabaseId);
            }
        };
        let schema = match proto_collection.schema_str {
            Some(schema_str) if !schema_str.is_empty() => Some(serde_json::from_str(&schema_str)?),
            _ => None,
        };

        Ok(Collection {
            collection_id,
            name: proto_collection.name,
            config: serde_json::from_str(&proto_collection.configuration_json_str)?,
            schema,
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
            updated_at,
            database_id,
            compaction_failure_count: proto_collection.compaction_failure_count,
        })
    }
}

// ============================================================================
// Row Conversion Implementation (Spanner DAO layer)
// ============================================================================

/// Convert a vector of Spanner rows (from a JOIN query) into a Collection.
///
/// The rows are expected to come from a query that JOINs:
/// - collections table
/// - collection_metadata table (LEFT JOIN, may have multiple rows per metadata key)
/// - collection_compaction_cursors table (LEFT JOIN, single row for a specific region)
///
/// Column names expected:
/// - collection_id, name, dimension, database_id, database_name, tenant_id, updated_at
/// - metadata_key, metadata_str_value, metadata_int_value, metadata_float_value, metadata_bool_value
/// - last_compacted_offset, version, total_records_post_compaction, size_bytes_post_compaction,
///   last_compaction_time_secs, version_file_name, index_schema
impl TryFrom<Vec<Row>> for Collection {
    type Error = SysDbError;

    fn try_from(rows: Vec<Row>) -> Result<Self, Self::Error> {
        if rows.is_empty() {
            return Err(SysDbError::NotFound("no rows returned".to_string()));
        }

        // Extract collection fields from the first row (same for all rows)
        let first_row = &rows[0];

        let collection_id_str: String = first_row
            .column_by_name("collection_id")
            .map_err(SysDbError::FailedToReadColumn)?;
        let name: String = first_row
            .column_by_name("name")
            .map_err(SysDbError::FailedToReadColumn)?;
        let dimension: Option<i64> = first_row
            .column_by_name("dimension")
            .map_err(SysDbError::FailedToReadColumn)?;
        let database_id_str: String = first_row
            .column_by_name("database_id")
            .map_err(SysDbError::FailedToReadColumn)?;
        let database_name: String = first_row
            .column_by_name("database_name")
            .map_err(SysDbError::FailedToReadColumn)?;
        let tenant_id: String = first_row
            .column_by_name("tenant_id")
            .map_err(SysDbError::FailedToReadColumn)?;

        // Spanner returns TIMESTAMP as i64 microseconds since Unix epoch
        let updated_at_us: i64 = first_row
            .column_by_name("updated_at")
            .map_err(SysDbError::FailedToReadColumn)?;

        let last_compacted_offset: Option<i64> = first_row
            .column_by_name("last_compacted_offset")
            .map_err(SysDbError::FailedToReadColumn)?;
        let version: Option<i64> = first_row
            .column_by_name("version")
            .map_err(SysDbError::FailedToReadColumn)?;
        let last_compaction_time_us: Option<i64> = first_row
            .column_by_name("last_compaction_time_secs")
            .map_err(SysDbError::FailedToReadColumn)?;
        let version_file_name: Option<String> = first_row
            .column_by_name("version_file_name")
            .map_err(SysDbError::FailedToReadColumn)?;
        let total_records_post_compaction: i64 = first_row
            .column_by_name("total_records_post_compaction")
            .map_err(SysDbError::FailedToReadColumn)?;
        let size_bytes_post_compaction: i64 = first_row
            .column_by_name("size_bytes_post_compaction")
            .map_err(SysDbError::FailedToReadColumn)?;
        let compaction_failure_count: i64 = first_row
            .column_by_name("compaction_failure_count")
            .map_err(SysDbError::FailedToReadColumn)?;
        let schema_json: String = first_row
            .column_by_name("index_schema")
            .map_err(SysDbError::FailedToReadColumn)?;

        // Aggregate metadata from all rows
        let mut collection_metadata = Metadata::new();
        for row in &rows {
            // metadata_key may be NULL if there's no metadata (LEFT JOIN)
            if let Ok(Some(key)) = row.column_by_name::<Option<String>>("metadata_key") {
                let str_val: Option<String> = row
                    .column_by_name("metadata_str_value")
                    .map_err(SysDbError::FailedToReadColumn)?;
                let int_val: Option<i64> = row
                    .column_by_name("metadata_int_value")
                    .map_err(SysDbError::FailedToReadColumn)?;
                let float_val: Option<f64> = row
                    .column_by_name("metadata_float_value")
                    .map_err(SysDbError::FailedToReadColumn)?;
                let bool_val: Option<bool> = row
                    .column_by_name("metadata_bool_value")
                    .map_err(SysDbError::FailedToReadColumn)?;

                if let Some(s) = str_val {
                    collection_metadata.insert(key, MetadataValue::Str(s));
                } else if let Some(i) = int_val {
                    collection_metadata.insert(key, MetadataValue::Int(i));
                } else if let Some(f) = float_val {
                    collection_metadata.insert(key, MetadataValue::Float(f));
                } else if let Some(b) = bool_val {
                    collection_metadata.insert(key, MetadataValue::Bool(b));
                }
            }
        }

        // Parse schema JSON (index_schema is NOT NULL, so parsing should succeed)
        let parsed_schema: Schema =
            serde_json::from_str(&schema_json).map_err(SysDbError::InvalidSchemaJson)?;

        // Convert microseconds to SystemTime
        let updated_at_system_time = UNIX_EPOCH + Duration::from_micros(updated_at_us as u64);

        // Convert last_compaction_time from microseconds to seconds
        let last_compaction_time_secs_u64 = last_compaction_time_us
            .map(|us| (us / 1_000_000) as u64)
            .unwrap_or(0);

        Ok(Collection {
            collection_id: CollectionUuid(
                Uuid::parse_str(&collection_id_str).map_err(SysDbError::InvalidUuid)?,
            ),
            name,
            config: InternalCollectionConfiguration::default_hnsw(),
            schema: Some(parsed_schema),
            metadata: if collection_metadata.is_empty() {
                None
            } else {
                Some(collection_metadata)
            },
            dimension: dimension.map(|d| d as i32),
            tenant: tenant_id,
            database: database_name,
            log_position: last_compacted_offset.unwrap_or(0),
            version: version.map(|v| v as i32).unwrap_or(0),
            total_records_post_compaction: total_records_post_compaction as u64,
            size_bytes_post_compaction: size_bytes_post_compaction as u64,
            last_compaction_time_secs: last_compaction_time_secs_u64,
            version_file_path: version_file_name,
            root_collection_id: None,
            lineage_file_path: None,
            updated_at: updated_at_system_time,
            database_id: DatabaseUuid(
                Uuid::parse_str(&database_id_str).map_err(SysDbError::InvalidUuid)?,
            ),
            compaction_failure_count: compaction_failure_count as i32,
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
            schema_str: value
                .schema
                .map(|s| serde_json::to_string(&s))
                .transpose()?,
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
            updated_at: Some(value.updated_at.into()),
            database_id: Some(value.database_id.0.to_string()),
            compaction_failure_count: value.compaction_failure_count,
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
    // If dimension is not set and vector segment has no files,
    // we assume this is an uninitialized collection
    pub fn is_uninitialized(&self) -> bool {
        self.collection.dimension.is_none() && self.vector_segment.file_path.is_empty()
    }

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

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CreateCollectionPayload {
    pub name: String,
    pub schema: Option<Schema>,
    pub configuration: Option<CollectionConfiguration>,
    pub metadata: Option<Metadata>,
    #[serde(default)]
    pub get_or_create: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct UpdateCollectionPayload {
    pub new_name: Option<String>,
    pub new_metadata: Option<UpdateMetadata>,
    pub new_configuration: Option<UpdateCollectionConfiguration>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_collection_try_from() {
        // Create a valid Schema and serialize it
        let schema = Schema::new_default(crate::KnnIndex::Spann);
        let schema_str = serde_json::to_string(&schema).unwrap();

        let proto_collection = chroma_proto::Collection {
            id: "00000000-0000-0000-0000-000000000000".to_string(),
            name: "foo".to_string(),
            configuration_json_str: "{\"a\": \"param\", \"b\": \"param2\", \"3\": true}"
                .to_string(),
            schema_str: Some(schema_str),
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
            updated_at: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 1,
            }),
            database_id: Some("00000000-0000-0000-0000-000000000000".to_string()),
            compaction_failure_count: 0,
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
        assert_eq!(
            converted_collection.updated_at,
            SystemTime::UNIX_EPOCH + Duration::new(1, 1)
        );
        assert_eq!(converted_collection.database_id, DatabaseUuid(Uuid::nil()));
    }

    #[test]
    fn storage_prefix_for_log_format() {
        let collection_id = Uuid::parse_str("34e72052-5e60-47cb-be88-19a9715b7026")
            .map(CollectionUuid)
            .unwrap();
        let prefix = collection_id.storage_prefix_for_log();
        assert_eq!("logs/34e72052-5e60-47cb-be88-19a9715b7026", prefix);
    }
}
