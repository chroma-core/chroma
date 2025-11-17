use crate::collection_configuration::InternalCollectionConfiguration;
use crate::collection_configuration::InternalUpdateCollectionConfiguration;
use crate::error::QueryConversionError;
use crate::operator::GetResult;
use crate::operator::Key;
use crate::operator::KnnBatchResult;
use crate::operator::KnnProjectionRecord;
use crate::operator::ProjectionRecord;
use crate::operator::SearchResult;
use crate::plan::PlanToProtoError;
use crate::plan::SearchPayload;
use crate::validators::{
    validate_metadata_vec, validate_name, validate_non_empty_collection_update_metadata,
    validate_optional_metadata, validate_schema, validate_update_metadata_vec,
};
use crate::Collection;
use crate::CollectionConfigurationToInternalConfigurationError;
use crate::CollectionConversionError;
use crate::CollectionUuid;
use crate::DistributedSpannParametersFromSegmentError;
use crate::EmbeddingsPayload;
use crate::HnswParametersFromSegmentError;
use crate::Metadata;
use crate::RawWhereFields;
use crate::Schema;
use crate::SchemaError;
use crate::SegmentConversionError;
use crate::SegmentScopeConversionError;
use crate::UpdateEmbeddingsPayload;
use crate::UpdateMetadata;
use crate::Where;
use crate::WhereValidationError;
use chroma_error::ChromaValidationError;
use chroma_error::{ChromaError, ErrorCodes};
use serde::Deserialize;
use serde::Serialize;
use std::time::SystemTimeError;
use thiserror::Error;
use tonic::Status;
use uuid::Uuid;
use validator::Validate;
use validator::ValidationError;

#[cfg(feature = "pyo3")]
use pyo3::types::PyAnyMethods;

#[derive(Debug, Error)]
pub enum GetSegmentsError {
    #[error("Could not parse segment")]
    SegmentConversion(#[from] SegmentConversionError),
    #[error("Unknown segment scope")]
    UnknownScope(#[from] SegmentScopeConversionError),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for GetSegmentsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetSegmentsError::SegmentConversion(_) => ErrorCodes::Internal,
            GetSegmentsError::UnknownScope(_) => ErrorCodes::Internal,
            GetSegmentsError::Internal(err) => err.code(),
        }
    }
}

#[derive(Debug, Error)]
pub enum GetCollectionWithSegmentsError {
    #[error("Failed to convert proto collection")]
    CollectionConversionError(#[from] CollectionConversionError),
    #[error("Duplicate segment")]
    DuplicateSegment,
    #[error("Missing field: [{0}]")]
    Field(String),
    #[error("Failed to convert proto segment")]
    SegmentConversionError(#[from] SegmentConversionError),
    #[error("Failed to get segments")]
    GetSegmentsError(#[from] GetSegmentsError),
    #[error("Grpc error: {0}")]
    Grpc(#[from] Status),
    #[error("Collection [{0}] does not exist.")]
    NotFound(String),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for GetCollectionWithSegmentsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionWithSegmentsError::CollectionConversionError(
                collection_conversion_error,
            ) => collection_conversion_error.code(),
            GetCollectionWithSegmentsError::DuplicateSegment => ErrorCodes::Internal,
            GetCollectionWithSegmentsError::Field(_) => ErrorCodes::FailedPrecondition,
            GetCollectionWithSegmentsError::SegmentConversionError(segment_conversion_error) => {
                segment_conversion_error.code()
            }
            GetCollectionWithSegmentsError::Grpc(status) => status.code().into(),
            GetCollectionWithSegmentsError::GetSegmentsError(get_segments_error) => {
                get_segments_error.code()
            }
            GetCollectionWithSegmentsError::NotFound(_) => ErrorCodes::NotFound,
            GetCollectionWithSegmentsError::Internal(err) => err.code(),
        }
    }

    fn should_trace_error(&self) -> bool {
        if let Self::Grpc(status) = self {
            status.code() != ErrorCodes::NotFound.into()
        } else {
            true
        }
    }
}

#[derive(Debug, Error)]
pub enum BatchGetCollectionVersionFilePathsError {
    #[error("Grpc error: {0}")]
    Grpc(#[from] Status),
    #[error("Could not parse UUID from string {1}: {0}")]
    Uuid(uuid::Error, String),
}

impl ChromaError for BatchGetCollectionVersionFilePathsError {
    fn code(&self) -> ErrorCodes {
        match self {
            BatchGetCollectionVersionFilePathsError::Grpc(status) => status.code().into(),
            BatchGetCollectionVersionFilePathsError::Uuid(_, _) => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Debug, Error)]
pub enum BatchGetCollectionSoftDeleteStatusError {
    #[error("Grpc error: {0}")]
    Grpc(#[from] Status),
    #[error("Could not parse UUID from string {1}: {0}")]
    Uuid(uuid::Error, String),
}

impl ChromaError for BatchGetCollectionSoftDeleteStatusError {
    fn code(&self) -> ErrorCodes {
        match self {
            BatchGetCollectionSoftDeleteStatusError::Grpc(status) => status.code().into(),
            BatchGetCollectionSoftDeleteStatusError::Uuid(_, _) => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ResetResponse {}

#[derive(Debug, Error)]
pub enum ResetError {
    #[error(transparent)]
    Cache(Box<dyn ChromaError>),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Reset is disabled by config")]
    NotAllowed,
}

impl ChromaError for ResetError {
    fn code(&self) -> ErrorCodes {
        match self {
            ResetError::Cache(err) => err.code(),
            ResetError::Internal(err) => err.code(),
            ResetError::NotAllowed => ErrorCodes::PermissionDenied,
        }
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ChecklistResponse {
    pub max_batch_size: u32,
    pub supports_base64_encoding: bool,
}

#[derive(Debug, Error)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum HeartbeatError {
    #[error("system time error: {0}")]
    CouldNotGetTime(String),
}

impl From<SystemTimeError> for HeartbeatError {
    fn from(err: SystemTimeError) -> Self {
        HeartbeatError::CouldNotGetTime(err.to_string())
    }
}

impl ChromaError for HeartbeatError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[non_exhaustive]
#[derive(Serialize, Validate, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CreateTenantRequest {
    #[validate(length(min = 3))]
    pub name: String,
}

impl CreateTenantRequest {
    pub fn try_new(name: String) -> Result<Self, ChromaValidationError> {
        let request = Self { name };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CreateTenantResponse {}

#[derive(Debug, Error)]
pub enum CreateTenantError {
    #[error("Tenant [{0}] already exists")]
    AlreadyExists(String),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for CreateTenantError {
    fn code(&self) -> ErrorCodes {
        match self {
            CreateTenantError::AlreadyExists(_) => ErrorCodes::AlreadyExists,
            CreateTenantError::Internal(err) => err.code(),
        }
    }
}

#[non_exhaustive]
#[derive(Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct GetTenantRequest {
    pub name: String,
}

impl GetTenantRequest {
    pub fn try_new(name: String) -> Result<Self, ChromaValidationError> {
        let request = Self { name };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct GetTenantResponse {
    pub name: String,
    pub resource_name: Option<String>,
}

#[cfg(feature = "pyo3")]
#[pyo3::pymethods]
impl GetTenantResponse {
    #[getter]
    pub fn name(&self) -> &String {
        &self.name
    }

    #[getter]
    pub fn resource_name(&self) -> Option<String> {
        self.resource_name.clone()
    }
}

#[derive(Debug, Error)]
pub enum GetTenantError {
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Tenant [{0}] not found")]
    NotFound(String),
}

impl ChromaError for GetTenantError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetTenantError::Internal(err) => err.code(),
            GetTenantError::NotFound(_) => ErrorCodes::NotFound,
        }
    }
}

#[non_exhaustive]
#[derive(Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct UpdateTenantRequest {
    pub tenant_id: String,
    pub resource_name: String,
}

impl UpdateTenantRequest {
    pub fn try_new(
        tenant_id: String,
        resource_name: String,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            resource_name,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct UpdateTenantResponse {}

#[cfg(feature = "pyo3")]
#[pyo3::pymethods]
impl UpdateTenantResponse {}

#[derive(Error, Debug)]
pub enum UpdateTenantError {
    #[error("Failed to set resource name")]
    FailedToSetResourceName(#[from] tonic::Status),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Tenant [{0}] not found")]
    NotFound(String),
}

impl ChromaError for UpdateTenantError {
    fn code(&self) -> ErrorCodes {
        match self {
            UpdateTenantError::FailedToSetResourceName(_) => ErrorCodes::AlreadyExists,
            UpdateTenantError::Internal(err) => err.code(),
            UpdateTenantError::NotFound(_) => ErrorCodes::NotFound,
        }
    }
}

#[non_exhaustive]
#[derive(Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CreateDatabaseRequest {
    pub database_id: Uuid,
    pub tenant_id: String,
    #[validate(length(min = 3))]
    pub database_name: String,
}

impl CreateDatabaseRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
    ) -> Result<Self, ChromaValidationError> {
        let database_id = Uuid::new_v4();
        let request = Self {
            database_id,
            tenant_id,
            database_name,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CreateDatabaseResponse {}

#[derive(Error, Debug)]
pub enum CreateDatabaseError {
    #[error("Database [{0}] already exists")]
    AlreadyExists(String),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for CreateDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            CreateDatabaseError::AlreadyExists(_) => ErrorCodes::AlreadyExists,
            CreateDatabaseError::Internal(status) => status.code(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct Database {
    pub id: Uuid,
    pub name: String,
    pub tenant: String,
}

#[cfg(feature = "pyo3")]
#[pyo3::pymethods]
impl Database {
    #[getter]
    fn id<'py>(&self, py: pyo3::Python<'py>) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let res = pyo3::prelude::PyModule::import(py, "uuid")?
            .getattr("UUID")?
            .call1((self.id.to_string(),))?;
        Ok(res)
    }

    #[getter]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
}

#[non_exhaustive]
#[derive(Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ListDatabasesRequest {
    pub tenant_id: String,
    pub limit: Option<u32>,
    pub offset: u32,
}

impl ListDatabasesRequest {
    pub fn try_new(
        tenant_id: String,
        limit: Option<u32>,
        offset: u32,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            limit,
            offset,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

pub type ListDatabasesResponse = Vec<Database>;

#[derive(Debug, Error)]
pub enum ListDatabasesError {
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Invalid database id [{0}]")]
    InvalidID(String),
}

impl ChromaError for ListDatabasesError {
    fn code(&self) -> ErrorCodes {
        match self {
            ListDatabasesError::Internal(status) => status.code(),
            ListDatabasesError::InvalidID(_) => ErrorCodes::InvalidArgument,
        }
    }
}

#[non_exhaustive]
#[derive(Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct GetDatabaseRequest {
    pub tenant_id: String,
    pub database_name: String,
}

impl GetDatabaseRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

pub type GetDatabaseResponse = Database;

#[derive(Error, Debug)]
pub enum GetDatabaseError {
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Invalid database id [{0}]")]
    InvalidID(String),
    #[error("Database [{0}] not found. Are you sure it exists?")]
    NotFound(String),
}

impl ChromaError for GetDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetDatabaseError::Internal(err) => err.code(),
            GetDatabaseError::InvalidID(_) => ErrorCodes::InvalidArgument,
            GetDatabaseError::NotFound(_) => ErrorCodes::NotFound,
        }
    }
}

#[non_exhaustive]
#[derive(Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DeleteDatabaseRequest {
    pub tenant_id: String,
    pub database_name: String,
}

impl DeleteDatabaseRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DeleteDatabaseResponse {}

#[derive(Debug, Error)]
pub enum DeleteDatabaseError {
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Invalid database id [{0}]")]
    InvalidID(String),
    #[error("Database [{0}] not found")]
    NotFound(String),
}

impl ChromaError for DeleteDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            DeleteDatabaseError::Internal(err) => err.code(),
            DeleteDatabaseError::InvalidID(_) => ErrorCodes::InvalidArgument,
            DeleteDatabaseError::NotFound(_) => ErrorCodes::NotFound,
        }
    }
}

#[derive(Debug, Error)]
pub enum FinishDatabaseDeletionError {
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for FinishDatabaseDeletionError {
    fn code(&self) -> ErrorCodes {
        match self {
            FinishDatabaseDeletionError::Internal(err) => err.code(),
        }
    }
}

#[non_exhaustive]
#[derive(Validate, Debug, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ListCollectionsRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub limit: Option<u32>,
    pub offset: u32,
}

impl ListCollectionsRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        limit: Option<u32>,
        offset: u32,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
            limit,
            offset,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

pub type ListCollectionsResponse = Vec<Collection>;

#[non_exhaustive]
#[derive(Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CountCollectionsRequest {
    pub tenant_id: String,
    pub database_name: String,
}

impl CountCollectionsRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

pub type CountCollectionsResponse = u32;

#[non_exhaustive]
#[derive(Validate, Clone, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct GetCollectionRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_name: String,
}

impl GetCollectionRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        collection_name: String,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
            collection_name,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

pub type GetCollectionResponse = Collection;

#[derive(Debug, Error)]
pub enum GetCollectionError {
    #[error("Failed to reconcile schema: {0}")]
    InvalidSchema(#[from] SchemaError),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Collection [{0}] does not exist")]
    NotFound(String),
}

impl ChromaError for GetCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionError::InvalidSchema(e) => e.code(),
            GetCollectionError::Internal(err) => err.code(),
            GetCollectionError::NotFound(_) => ErrorCodes::NotFound,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CreateCollectionRequest {
    pub tenant_id: String,
    pub database_name: String,
    #[validate(custom(function = "validate_name"))]
    pub name: String,
    #[validate(custom(function = "validate_optional_metadata"))]
    pub metadata: Option<Metadata>,
    pub configuration: Option<InternalCollectionConfiguration>,
    #[validate(custom(function = "validate_schema"))]
    pub schema: Option<Schema>,
    pub get_or_create: bool,
}

impl CreateCollectionRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        name: String,
        metadata: Option<Metadata>,
        configuration: Option<InternalCollectionConfiguration>,
        schema: Option<Schema>,
        get_or_create: bool,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
            name,
            metadata,
            configuration,
            schema,
            get_or_create,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

pub type CreateCollectionResponse = Collection;

#[derive(Debug, Error)]
pub enum CreateCollectionError {
    #[error("Invalid HNSW parameters: {0}")]
    InvalidHnswParameters(#[from] HnswParametersFromSegmentError),
    #[error("Could not parse config: {0}")]
    InvalidConfig(#[from] CollectionConfigurationToInternalConfigurationError),
    #[error("Invalid Spann parameters: {0}")]
    InvalidSpannParameters(#[from] DistributedSpannParametersFromSegmentError),
    #[error("Collection [{0}] already exists")]
    AlreadyExists(String),
    #[error("Database [{0}] does not exist")]
    DatabaseNotFound(String),
    #[error("Could not fetch collections: {0}")]
    Get(#[from] GetCollectionsError),
    #[error("Could not deserialize configuration: {0}")]
    Configuration(serde_json::Error),
    #[error("Could not serialize schema: {0}")]
    Schema(#[source] SchemaError),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("The operation was aborted, {0}")]
    Aborted(String),
    #[error("SPANN is still in development. Not allowed to created spann indexes")]
    SpannNotImplemented,
    #[error("HNSW is not supported on this platform")]
    HnswNotSupported,
    #[error("Failed to parse db id")]
    DatabaseIdParseError,
    #[error("Failed to reconcile schema: {0}")]
    InvalidSchema(#[source] SchemaError),
}

impl ChromaError for CreateCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            CreateCollectionError::InvalidHnswParameters(_) => ErrorCodes::InvalidArgument,
            CreateCollectionError::InvalidConfig(_) => ErrorCodes::InvalidArgument,
            CreateCollectionError::InvalidSpannParameters(_) => ErrorCodes::InvalidArgument,
            CreateCollectionError::AlreadyExists(_) => ErrorCodes::AlreadyExists,
            CreateCollectionError::DatabaseNotFound(_) => ErrorCodes::InvalidArgument,
            CreateCollectionError::Get(err) => err.code(),
            CreateCollectionError::Configuration(_) => ErrorCodes::Internal,
            CreateCollectionError::Internal(err) => err.code(),
            CreateCollectionError::Aborted(_) => ErrorCodes::Aborted,
            CreateCollectionError::SpannNotImplemented => ErrorCodes::InvalidArgument,
            CreateCollectionError::HnswNotSupported => ErrorCodes::InvalidArgument,
            CreateCollectionError::DatabaseIdParseError => ErrorCodes::Internal,
            CreateCollectionError::InvalidSchema(e) => e.code(),
            CreateCollectionError::Schema(e) => e.code(),
        }
    }
}

#[derive(Debug, Error)]
pub enum CountCollectionsError {
    #[error("Internal error in getting count")]
    Internal,
}

impl ChromaError for CountCollectionsError {
    fn code(&self) -> ErrorCodes {
        match self {
            CountCollectionsError::Internal => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug, Error)]
pub enum GetCollectionsError {
    #[error("Failed to reconcile schema: {0}")]
    InvalidSchema(#[from] SchemaError),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Could not deserialize configuration")]
    Configuration(#[source] serde_json::Error),
    #[error("Could not deserialize collection ID")]
    CollectionId(#[from] uuid::Error),
    #[error("Could not deserialize database ID")]
    DatabaseId,
    #[error("Could not deserialize schema")]
    Schema(#[source] serde_json::Error),
}

impl ChromaError for GetCollectionsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionsError::InvalidSchema(e) => e.code(),
            GetCollectionsError::Internal(err) => err.code(),
            GetCollectionsError::Configuration(_) => ErrorCodes::Internal,
            GetCollectionsError::CollectionId(_) => ErrorCodes::Internal,
            GetCollectionsError::DatabaseId => ErrorCodes::Internal,
            GetCollectionsError::Schema(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ChromaResourceName {
    pub tenant_resource_name: String,
    pub database_name: String,
    pub collection_name: String,
}
#[non_exhaustive]
#[derive(Clone, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct GetCollectionByCrnRequest {
    pub parsed_crn: ChromaResourceName,
}

impl GetCollectionByCrnRequest {
    pub fn try_new(crn: String) -> Result<Self, ChromaValidationError> {
        let parsed_crn = parse_and_validate_crn(&crn)?;
        Ok(Self { parsed_crn })
    }
}

fn parse_and_validate_crn(crn: &str) -> Result<ChromaResourceName, ChromaValidationError> {
    let mut parts = crn.splitn(4, ':');
    if let (Some(p1), Some(p2), Some(p3), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    {
        if !p1.is_empty() && !p2.is_empty() && !p3.is_empty() {
            return Ok(ChromaResourceName {
                tenant_resource_name: p1.to_string(),
                database_name: p2.to_string(),
                collection_name: p3.to_string(),
            });
        }
    }
    let mut err = ValidationError::new("invalid_crn_format");
    err.message = Some(
        "CRN must be in the format <tenant_resource_name>:<database_name>:<collection_name> with non-empty parts"
            .into(),
    );
    Err(ChromaValidationError::from(("crn", err)))
}

pub type GetCollectionByCrnResponse = Collection;

#[derive(Debug, Error)]
pub enum GetCollectionByCrnError {
    #[error("Failed to reconcile schema: {0}")]
    InvalidSchema(#[from] SchemaError),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Collection [{0}] does not exist")]
    NotFound(String),
}

impl ChromaError for GetCollectionByCrnError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionByCrnError::InvalidSchema(e) => e.code(),
            GetCollectionByCrnError::Internal(err) => err.code(),
            GetCollectionByCrnError::NotFound(_) => ErrorCodes::NotFound,
        }
    }
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum CollectionMetadataUpdate {
    ResetMetadata,
    UpdateMetadata(UpdateMetadata),
}

#[non_exhaustive]
#[derive(Clone, Validate, Debug, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct UpdateCollectionRequest {
    pub collection_id: CollectionUuid,
    #[validate(custom(function = "validate_name"))]
    pub new_name: Option<String>,
    #[validate(custom(function = "validate_non_empty_collection_update_metadata"))]
    pub new_metadata: Option<CollectionMetadataUpdate>,
    pub new_configuration: Option<InternalUpdateCollectionConfiguration>,
}

impl UpdateCollectionRequest {
    pub fn try_new(
        collection_id: CollectionUuid,
        new_name: Option<String>,
        new_metadata: Option<CollectionMetadataUpdate>,
        new_configuration: Option<InternalUpdateCollectionConfiguration>,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            collection_id,
            new_name,
            new_metadata,
            new_configuration,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct UpdateCollectionResponse {}

#[derive(Error, Debug)]
pub enum UpdateCollectionError {
    #[error("Collection [{0}] does not exist")]
    NotFound(String),
    #[error("Metadata reset unsupported")]
    MetadataResetUnsupported,
    #[error("Could not serialize configuration")]
    Configuration(#[source] serde_json::Error),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Could not parse config: {0}")]
    InvalidConfig(#[from] CollectionConfigurationToInternalConfigurationError),
    #[error("SPANN is still in development. Not allowed to created spann indexes")]
    SpannNotImplemented,
    #[error("Could not serialize schema: {0}")]
    Schema(#[source] serde_json::Error),
}

impl ChromaError for UpdateCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            UpdateCollectionError::NotFound(_) => ErrorCodes::NotFound,
            UpdateCollectionError::MetadataResetUnsupported => ErrorCodes::InvalidArgument,
            UpdateCollectionError::Configuration(_) => ErrorCodes::Internal,
            UpdateCollectionError::Internal(err) => err.code(),
            UpdateCollectionError::InvalidConfig(_) => ErrorCodes::InvalidArgument,
            UpdateCollectionError::SpannNotImplemented => ErrorCodes::InvalidArgument,
            UpdateCollectionError::Schema(_) => ErrorCodes::Internal,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DeleteCollectionRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_name: String,
}

impl DeleteCollectionRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        collection_name: String,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
            collection_name,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DeleteCollectionResponse {}

#[derive(Error, Debug)]
pub enum DeleteCollectionError {
    #[error("Collection [{0}] does not exist")]
    NotFound(String),
    #[error(transparent)]
    Validation(#[from] ChromaValidationError),
    #[error(transparent)]
    Get(#[from] GetCollectionError),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for DeleteCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            DeleteCollectionError::Validation(err) => err.code(),
            DeleteCollectionError::NotFound(_) => ErrorCodes::NotFound,
            DeleteCollectionError::Get(err) => err.code(),
            DeleteCollectionError::Internal(err) => err.code(),
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ForkCollectionRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub source_collection_id: CollectionUuid,
    pub target_collection_name: String,
}

impl ForkCollectionRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        source_collection_id: CollectionUuid,
        target_collection_name: String,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
            source_collection_id,
            target_collection_name,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

pub type ForkCollectionResponse = Collection;

#[derive(Clone, Debug)]
pub struct ForkLogsResponse {
    pub compaction_offset: u64,
    pub enumeration_offset: u64,
}

#[derive(Error, Debug)]
pub enum ForkCollectionError {
    #[error("Collection [{0}] already exists")]
    AlreadyExists(String),
    #[error("Failed to convert proto collection")]
    CollectionConversionError(#[from] CollectionConversionError),
    #[error("Duplicate segment")]
    DuplicateSegment,
    #[error("Missing field: [{0}]")]
    Field(String),
    #[error("Collection forking is unsupported for local chroma")]
    Local,
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Collection [{0}] does not exist")]
    NotFound(String),
    #[error("Failed to convert proto segment")]
    SegmentConversionError(#[from] SegmentConversionError),
    #[error("Failed to reconcile schema: {0}")]
    InvalidSchema(#[from] SchemaError),
}

impl ChromaError for ForkCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            ForkCollectionError::NotFound(_) => ErrorCodes::NotFound,
            ForkCollectionError::AlreadyExists(_) => ErrorCodes::AlreadyExists,
            ForkCollectionError::CollectionConversionError(e) => e.code(),
            ForkCollectionError::DuplicateSegment => ErrorCodes::Internal,
            ForkCollectionError::Field(_) => ErrorCodes::FailedPrecondition,
            ForkCollectionError::Local => ErrorCodes::Unimplemented,
            ForkCollectionError::Internal(e) => e.code(),
            ForkCollectionError::SegmentConversionError(e) => e.code(),
            ForkCollectionError::InvalidSchema(e) => e.code(),
        }
    }
}

#[derive(Debug, Error)]
pub enum CountForksError {
    #[error("Collection [{0}] does not exist")]
    NotFound(String),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Count forks is unsupported for local chroma")]
    Local,
}

impl ChromaError for CountForksError {
    fn code(&self) -> ErrorCodes {
        match self {
            CountForksError::NotFound(_) => ErrorCodes::NotFound,
            CountForksError::Internal(chroma_error) => chroma_error.code(),
            CountForksError::Local => ErrorCodes::Unimplemented,
        }
    }
}

#[derive(Debug, Error)]
pub enum ListAttachedFunctionsError {
    #[error("Collection [{0}] does not exist")]
    NotFound(String),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("List attached functions is not implemented")]
    NotImplemented,
}

impl ChromaError for ListAttachedFunctionsError {
    fn code(&self) -> ErrorCodes {
        match self {
            ListAttachedFunctionsError::NotFound(_) => ErrorCodes::NotFound,
            ListAttachedFunctionsError::Internal(chroma_error) => chroma_error.code(),
            ListAttachedFunctionsError::NotImplemented => ErrorCodes::Unimplemented,
        }
    }
}

#[derive(Debug, Error)]
pub enum GetCollectionSizeError {
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Collection [{0}] does not exist")]
    NotFound(String),
}

impl ChromaError for GetCollectionSizeError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionSizeError::Internal(err) => err.code(),
            GetCollectionSizeError::NotFound(_) => ErrorCodes::NotFound,
        }
    }
}

#[derive(Error, Debug)]
pub enum ListCollectionVersionsError {
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Collection [{0}] does not exist")]
    NotFound(String),
}

impl ChromaError for ListCollectionVersionsError {
    fn code(&self) -> ErrorCodes {
        match self {
            ListCollectionVersionsError::Internal(err) => err.code(),
            ListCollectionVersionsError::NotFound(_) => ErrorCodes::NotFound,
        }
    }
}

////////////////////////// Metadata Key Constants //////////////////////////

pub const CHROMA_KEY: &str = "chroma:";
pub const CHROMA_DOCUMENT_KEY: &str = "chroma:document";
pub const CHROMA_URI_KEY: &str = "chroma:uri";

////////////////////////// AddCollectionRecords //////////////////////////

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct AddCollectionRecordsPayload {
    pub ids: Vec<String>,
    pub embeddings: EmbeddingsPayload,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
    pub metadatas: Option<Vec<Option<Metadata>>>,
}

impl AddCollectionRecordsPayload {
    pub fn new(
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<Metadata>>>,
    ) -> Self {
        Self {
            ids,
            embeddings: EmbeddingsPayload::JsonArrays(embeddings),
            documents,
            uris,
            metadatas,
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct AddCollectionRecordsRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Vec<String>,
    #[validate(custom(function = "validate_embeddings"))]
    pub embeddings: Vec<Vec<f32>>,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
    #[validate(custom(function = "validate_metadata_vec"))]
    pub metadatas: Option<Vec<Option<Metadata>>>,
}

impl AddCollectionRecordsRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        collection_id: CollectionUuid,
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<Metadata>>>,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }

    pub fn into_payload(self) -> AddCollectionRecordsPayload {
        AddCollectionRecordsPayload {
            ids: self.ids,
            embeddings: EmbeddingsPayload::JsonArrays(self.embeddings),
            documents: self.documents,
            uris: self.uris,
            metadatas: self.metadatas,
        }
    }
}

fn validate_embeddings(embeddings: &[Vec<f32>]) -> Result<(), ValidationError> {
    if embeddings.iter().any(|e| e.is_empty()) {
        return Err(ValidationError::new("embedding_minimum_dimensions")
            .with_message("Each embedding must have at least 1 dimension".into()));
    }
    Ok(())
}

#[derive(Serialize, Default, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct AddCollectionRecordsResponse {}

#[derive(Error, Debug)]
pub enum AddCollectionRecordsError {
    #[error("Failed to get collection: {0}")]
    Collection(#[from] GetCollectionError),
    #[error("Backoff and retry")]
    Backoff,
    #[error(transparent)]
    Other(#[from] Box<dyn ChromaError>),
}

impl ChromaError for AddCollectionRecordsError {
    fn code(&self) -> ErrorCodes {
        match self {
            AddCollectionRecordsError::Collection(err) => err.code(),
            AddCollectionRecordsError::Backoff => ErrorCodes::ResourceExhausted,
            AddCollectionRecordsError::Other(err) => err.code(),
        }
    }
}

////////////////////////// UpdateCollectionRecords //////////////////////////

#[derive(Deserialize, Debug, Clone, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct UpdateCollectionRecordsPayload {
    pub ids: Vec<String>,
    pub embeddings: Option<UpdateEmbeddingsPayload>,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
    pub metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

#[non_exhaustive]
#[derive(Debug, Clone, Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct UpdateCollectionRecordsRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Vec<String>,
    pub embeddings: Option<Vec<Option<Vec<f32>>>>,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
    #[validate(custom(function = "validate_update_metadata_vec"))]
    pub metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

impl UpdateCollectionRecordsRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        collection_id: CollectionUuid,
        ids: Vec<String>,
        embeddings: Option<Vec<Option<Vec<f32>>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }

    pub fn into_payload(self) -> UpdateCollectionRecordsPayload {
        UpdateCollectionRecordsPayload {
            ids: self.ids,
            embeddings: self.embeddings.map(UpdateEmbeddingsPayload::JsonArrays),
            documents: self.documents,
            uris: self.uris,
            metadatas: self.metadatas,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct UpdateCollectionRecordsResponse {}

#[derive(Error, Debug)]
pub enum UpdateCollectionRecordsError {
    #[error("Backoff and retry")]
    Backoff,
    #[error(transparent)]
    Other(#[from] Box<dyn ChromaError>),
}

impl ChromaError for UpdateCollectionRecordsError {
    fn code(&self) -> ErrorCodes {
        match self {
            UpdateCollectionRecordsError::Backoff => ErrorCodes::ResourceExhausted,
            UpdateCollectionRecordsError::Other(err) => err.code(),
        }
    }
}

////////////////////////// UpsertCollectionRecords //////////////////////////

#[derive(Deserialize, Debug, Clone, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct UpsertCollectionRecordsPayload {
    pub ids: Vec<String>,
    pub embeddings: EmbeddingsPayload,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
    pub metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

#[non_exhaustive]
#[derive(Debug, Clone, Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct UpsertCollectionRecordsRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Vec<String>,
    #[validate(custom(function = "validate_embeddings"))]
    pub embeddings: Vec<Vec<f32>>,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
    #[validate(custom(function = "validate_update_metadata_vec"))]
    pub metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

impl UpsertCollectionRecordsRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        collection_id: CollectionUuid,
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }

    pub fn into_payload(self) -> UpsertCollectionRecordsPayload {
        UpsertCollectionRecordsPayload {
            ids: self.ids.clone(),
            embeddings: EmbeddingsPayload::JsonArrays(self.embeddings.clone()),
            documents: self.documents.clone(),
            uris: self.uris.clone(),
            metadatas: self.metadatas.clone(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct UpsertCollectionRecordsResponse {}

#[derive(Error, Debug)]
pub enum UpsertCollectionRecordsError {
    #[error("Backoff and retry")]
    Backoff,
    #[error(transparent)]
    Other(#[from] Box<dyn ChromaError>),
}

impl ChromaError for UpsertCollectionRecordsError {
    fn code(&self) -> ErrorCodes {
        match self {
            UpsertCollectionRecordsError::Backoff => ErrorCodes::ResourceExhausted,
            UpsertCollectionRecordsError::Other(err) => err.code(),
        }
    }
}

////////////////////////// DeleteCollectionRecords //////////////////////////

#[derive(Deserialize, Debug, Clone, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DeleteCollectionRecordsPayload {
    pub ids: Option<Vec<String>>,
    #[serde(flatten)]
    pub where_fields: RawWhereFields,
}

#[non_exhaustive]
#[derive(Debug, Clone, Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DeleteCollectionRecordsRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Option<Vec<String>>,
    pub r#where: Option<Where>,
}

impl DeleteCollectionRecordsRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        collection_id: CollectionUuid,
        ids: Option<Vec<String>>,
        r#where: Option<Where>,
    ) -> Result<Self, ChromaValidationError> {
        if ids.as_ref().map(|ids| ids.is_empty()).unwrap_or(false) && r#where.is_none() {
            return Err(ChromaValidationError::from((
                ("ids, where"),
                ValidationError::new("filter")
                    .with_message("Either ids or where must be specified".into()),
            )));
        }

        let request = Self {
            tenant_id,
            database_name,
            collection_id,
            ids,
            r#where,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }

    pub fn into_payload(self) -> Result<DeleteCollectionRecordsPayload, WhereError> {
        let where_fields = if let Some(r#where) = self.r#where.as_ref() {
            RawWhereFields::from_json_str(Some(&serde_json::to_string(r#where)?), None)?
        } else {
            RawWhereFields::default()
        };
        Ok(DeleteCollectionRecordsPayload {
            ids: self.ids.clone(),
            where_fields,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DeleteCollectionRecordsResponse {}

#[derive(Error, Debug)]
pub enum DeleteCollectionRecordsError {
    #[error("Failed to resolve records for deletion: {0}")]
    Get(#[from] ExecutorError),
    #[error("Backoff and retry")]
    Backoff,
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for DeleteCollectionRecordsError {
    fn code(&self) -> ErrorCodes {
        match self {
            DeleteCollectionRecordsError::Get(err) => err.code(),
            DeleteCollectionRecordsError::Backoff => ErrorCodes::ResourceExhausted,
            DeleteCollectionRecordsError::Internal(err) => err.code(),
        }
    }
}

////////////////////////// Include //////////////////////////

#[derive(Error, Debug)]
#[error("Invalid include value: {0}")]
pub struct IncludeParsingError(String);

impl ChromaError for IncludeParsingError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum Include {
    #[serde(rename = "distances")]
    Distance,
    #[serde(rename = "documents")]
    Document,
    #[serde(rename = "embeddings")]
    Embedding,
    #[serde(rename = "metadatas")]
    Metadata,
    #[serde(rename = "uris")]
    Uri,
}

impl TryFrom<&str> for Include {
    type Error = IncludeParsingError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "distances" => Ok(Include::Distance),
            "documents" => Ok(Include::Document),
            "embeddings" => Ok(Include::Embedding),
            "metadatas" => Ok(Include::Metadata),
            "uris" => Ok(Include::Uri),
            _ => Err(IncludeParsingError(value.to_string())),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct IncludeList(pub Vec<Include>);

impl IncludeList {
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    pub fn default_query() -> Self {
        Self(vec![
            Include::Document,
            Include::Metadata,
            Include::Distance,
        ])
    }
    pub fn default_get() -> Self {
        Self(vec![Include::Document, Include::Metadata])
    }
    pub fn all() -> Self {
        Self(vec![
            Include::Document,
            Include::Metadata,
            Include::Distance,
            Include::Embedding,
            Include::Uri,
        ])
    }
}

impl TryFrom<Vec<String>> for IncludeList {
    type Error = IncludeParsingError;

    fn try_from(value: Vec<String>) -> Result<Self, Self::Error> {
        let mut includes = Vec::new();
        for v in value {
            // "data" is only used by single node Chroma
            if v == "data" {
                includes.push(Include::Metadata);
                continue;
            }

            includes.push(Include::try_from(v.as_str())?);
        }
        Ok(IncludeList(includes))
    }
}

////////////////////////// Count //////////////////////////

#[non_exhaustive]
#[derive(Clone, Deserialize, Serialize, Validate)]
pub struct CountRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
}

impl CountRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        collection_id: CollectionUuid,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
            collection_id,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

pub type CountResponse = u32;

//////////////////////// Payload Err ////////////////////

#[derive(Debug, thiserror::Error)]
pub enum WhereError {
    #[error("serialization: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("validation: {0}")]
    Validation(#[from] WhereValidationError),
}

////////////////////////// Get //////////////////////////

#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct GetRequestPayload {
    pub ids: Option<Vec<String>>,
    #[serde(flatten)]
    pub where_fields: RawWhereFields,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    #[serde(default = "IncludeList::default_get")]
    pub include: IncludeList,
}

#[non_exhaustive]
#[derive(Debug, Clone, Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct GetRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Option<Vec<String>>,
    pub r#where: Option<Where>,
    pub limit: Option<u32>,
    pub offset: u32,
    pub include: IncludeList,
}

impl GetRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        collection_id: CollectionUuid,
        ids: Option<Vec<String>>,
        r#where: Option<Where>,
        limit: Option<u32>,
        offset: u32,
        include: IncludeList,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
            collection_id,
            ids,
            r#where,
            limit,
            offset,
            include,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }

    pub fn into_payload(self) -> Result<GetRequestPayload, WhereError> {
        let where_fields = if let Some(r#where) = self.r#where.as_ref() {
            RawWhereFields::from_json_str(Some(&serde_json::to_string(r#where)?), None)?
        } else {
            RawWhereFields::default()
        };
        Ok(GetRequestPayload {
            ids: self.ids,
            where_fields,
            limit: self.limit,
            offset: Some(self.offset),
            include: self.include,
        })
    }
}

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct GetResponse {
    pub ids: Vec<String>,
    pub embeddings: Option<Vec<Vec<f32>>>,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
    // TODO(hammadb): Add metadata & include to the response
    pub metadatas: Option<Vec<Option<Metadata>>>,
    pub include: Vec<Include>,
}

impl GetResponse {
    pub fn sort_by_ids(&mut self) {
        let mut indices: Vec<usize> = (0..self.ids.len()).collect();
        indices.sort_by(|&a, &b| self.ids[a].cmp(&self.ids[b]));

        let sorted_ids = indices.iter().map(|&i| self.ids[i].clone()).collect();
        self.ids = sorted_ids;

        if let Some(ref mut embeddings) = self.embeddings {
            let sorted_embeddings = indices.iter().map(|&i| embeddings[i].clone()).collect();
            *embeddings = sorted_embeddings;
        }

        if let Some(ref mut documents) = self.documents {
            let sorted_docs = indices.iter().map(|&i| documents[i].clone()).collect();
            *documents = sorted_docs;
        }

        if let Some(ref mut uris) = self.uris {
            let sorted_uris = indices.iter().map(|&i| uris[i].clone()).collect();
            *uris = sorted_uris;
        }

        if let Some(ref mut metadatas) = self.metadatas {
            let sorted_metas = indices.iter().map(|&i| metadatas[i].clone()).collect();
            *metadatas = sorted_metas;
        }
    }
}

#[cfg(feature = "pyo3")]
#[pyo3::pymethods]
impl GetResponse {
    #[getter]
    pub fn ids(&self) -> &Vec<String> {
        &self.ids
    }

    #[getter]
    pub fn embeddings(&self) -> Option<Vec<Vec<f32>>> {
        self.embeddings.clone()
    }

    #[getter]
    pub fn documents(&self) -> Option<Vec<Option<String>>> {
        self.documents.clone()
    }

    #[getter]
    pub fn uris(&self) -> Option<Vec<Option<String>>> {
        self.uris.clone()
    }

    #[getter]
    pub fn metadatas(&self) -> Option<Vec<Option<Metadata>>> {
        self.metadatas.clone()
    }
}

impl From<(GetResult, IncludeList)> for GetResponse {
    fn from((result, IncludeList(include_vec)): (GetResult, IncludeList)) -> Self {
        let mut res = Self {
            ids: Vec::new(),
            embeddings: include_vec
                .contains(&Include::Embedding)
                .then_some(Vec::new()),
            documents: include_vec
                .contains(&Include::Document)
                .then_some(Vec::new()),
            uris: include_vec.contains(&Include::Uri).then_some(Vec::new()),
            metadatas: include_vec
                .contains(&Include::Metadata)
                .then_some(Vec::new()),
            include: include_vec,
        };
        for ProjectionRecord {
            id,
            document,
            embedding,
            mut metadata,
        } in result.result.records
        {
            res.ids.push(id);
            if let (Some(emb), Some(embeddings)) = (embedding, res.embeddings.as_mut()) {
                embeddings.push(emb);
            }
            if let Some(documents) = res.documents.as_mut() {
                documents.push(document);
            }
            let uri = metadata.as_mut().and_then(|meta| {
                meta.remove(CHROMA_URI_KEY).and_then(|v| {
                    if let crate::MetadataValue::Str(uri) = v {
                        Some(uri)
                    } else {
                        None
                    }
                })
            });
            if let Some(uris) = res.uris.as_mut() {
                uris.push(uri);
            }

            let metadata = metadata.map(|m| {
                m.into_iter()
                    .filter(|(k, _)| !k.starts_with(CHROMA_KEY))
                    .collect()
            });
            if let Some(metadatas) = res.metadatas.as_mut() {
                metadatas.push(metadata);
            }
        }
        res
    }
}

////////////////////////// Query //////////////////////////

#[derive(Deserialize, Debug, Clone, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct QueryRequestPayload {
    pub ids: Option<Vec<String>>,
    #[serde(flatten)]
    pub where_fields: RawWhereFields,
    pub query_embeddings: Vec<Vec<f32>>,
    pub n_results: Option<u32>,
    #[serde(default = "IncludeList::default_query")]
    pub include: IncludeList,
}

#[non_exhaustive]
#[derive(Debug, Clone, Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct QueryRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Option<Vec<String>>,
    pub r#where: Option<Where>,
    pub embeddings: Vec<Vec<f32>>,
    pub n_results: u32,
    pub include: IncludeList,
}

impl QueryRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        collection_id: CollectionUuid,
        ids: Option<Vec<String>>,
        r#where: Option<Where>,
        embeddings: Vec<Vec<f32>>,
        n_results: u32,
        include: IncludeList,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
            collection_id,
            ids,
            r#where,
            embeddings,
            n_results,
            include,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }

    pub fn into_payload(self) -> Result<QueryRequestPayload, WhereError> {
        let where_fields = if let Some(r#where) = self.r#where.as_ref() {
            RawWhereFields::from_json_str(Some(&serde_json::to_string(r#where)?), None)?
        } else {
            RawWhereFields::default()
        };
        Ok(QueryRequestPayload {
            ids: self.ids,
            where_fields,
            query_embeddings: self.embeddings,
            n_results: Some(self.n_results),
            include: self.include,
        })
    }
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct QueryResponse {
    pub ids: Vec<Vec<String>>,
    pub embeddings: Option<Vec<Vec<Option<Vec<f32>>>>>,
    pub documents: Option<Vec<Vec<Option<String>>>>,
    pub uris: Option<Vec<Vec<Option<String>>>>,
    pub metadatas: Option<Vec<Vec<Option<Metadata>>>>,
    pub distances: Option<Vec<Vec<Option<f32>>>>,
    pub include: Vec<Include>,
}

impl QueryResponse {
    pub fn sort_by_ids(&mut self) {
        fn reorder<T: Clone>(v: &mut [T], indices: &[usize]) {
            let old = v.to_owned();
            for (new_pos, &i) in indices.iter().enumerate() {
                v[new_pos] = old[i].clone();
            }
        }

        for i in 0..self.ids.len() {
            let mut indices: Vec<usize> = (0..self.ids[i].len()).collect();

            indices.sort_unstable_by(|&a, &b| self.ids[i][a].cmp(&self.ids[i][b]));

            reorder(&mut self.ids[i], &indices);

            if let Some(embeddings) = &mut self.embeddings {
                reorder(&mut embeddings[i], &indices);
            }

            if let Some(documents) = &mut self.documents {
                reorder(&mut documents[i], &indices);
            }

            if let Some(uris) = &mut self.uris {
                reorder(&mut uris[i], &indices);
            }

            if let Some(metadatas) = &mut self.metadatas {
                reorder(&mut metadatas[i], &indices);
            }

            if let Some(distances) = &mut self.distances {
                reorder(&mut distances[i], &indices);
            }
        }
    }
}

#[cfg(feature = "pyo3")]
#[pyo3::pymethods]
impl QueryResponse {
    #[getter]
    pub fn ids(&self) -> &Vec<Vec<String>> {
        &self.ids
    }

    #[getter]
    pub fn embeddings(&self) -> Option<Vec<Vec<Option<Vec<f32>>>>> {
        self.embeddings.clone()
    }

    #[getter]
    pub fn documents(&self) -> Option<Vec<Vec<Option<String>>>> {
        self.documents.clone()
    }

    #[getter]
    pub fn uris(&self) -> Option<Vec<Vec<Option<String>>>> {
        self.uris.clone()
    }

    #[getter]
    pub fn metadatas(&self) -> Option<Vec<Vec<Option<Metadata>>>> {
        self.metadatas.clone()
    }

    #[getter]
    pub fn distances(&self) -> Option<Vec<Vec<Option<f32>>>> {
        self.distances.clone()
    }
}

impl From<(KnnBatchResult, IncludeList)> for QueryResponse {
    fn from((result, IncludeList(include_vec)): (KnnBatchResult, IncludeList)) -> Self {
        let mut res = Self {
            ids: Vec::new(),
            embeddings: include_vec
                .contains(&Include::Embedding)
                .then_some(Vec::new()),
            documents: include_vec
                .contains(&Include::Document)
                .then_some(Vec::new()),
            uris: include_vec.contains(&Include::Uri).then_some(Vec::new()),
            metadatas: include_vec
                .contains(&Include::Metadata)
                .then_some(Vec::new()),
            distances: include_vec
                .contains(&Include::Distance)
                .then_some(Vec::new()),
            include: include_vec,
        };
        for query_result in result.results {
            let mut ids = Vec::new();
            let mut embeddings = Vec::new();
            let mut documents = Vec::new();
            let mut uris = Vec::new();
            let mut metadatas = Vec::new();
            let mut distances = Vec::new();
            for KnnProjectionRecord {
                record:
                    ProjectionRecord {
                        id,
                        document,
                        embedding,
                        mut metadata,
                    },
                distance,
            } in query_result.records
            {
                ids.push(id);
                embeddings.push(embedding);
                documents.push(document);

                let uri = metadata.as_mut().and_then(|meta| {
                    meta.remove(CHROMA_URI_KEY).and_then(|v| {
                        if let crate::MetadataValue::Str(uri) = v {
                            Some(uri)
                        } else {
                            None
                        }
                    })
                });
                uris.push(uri);

                let metadata = metadata.map(|m| {
                    m.into_iter()
                        .filter(|(k, _)| !k.starts_with(CHROMA_KEY))
                        .collect()
                });
                metadatas.push(metadata);

                distances.push(distance);
            }
            res.ids.push(ids);

            if let Some(res_embs) = res.embeddings.as_mut() {
                res_embs.push(embeddings);
            }
            if let Some(res_docs) = res.documents.as_mut() {
                res_docs.push(documents);
            }
            if let Some(res_uri) = res.uris.as_mut() {
                res_uri.push(uris);
            }
            if let Some(res_metas) = res.metadatas.as_mut() {
                res_metas.push(metadatas);
            }
            if let Some(res_dists) = res.distances.as_mut() {
                res_dists.push(distances);
            }
        }
        res
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct SearchRequestPayload {
    pub searches: Vec<SearchPayload>,
}

#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Validate)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct SearchRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub searches: Vec<SearchPayload>,
}

impl SearchRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        collection_id: CollectionUuid,
        searches: Vec<SearchPayload>,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
            collection_id,
            searches,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }

    pub fn into_payload(self) -> SearchRequestPayload {
        SearchRequestPayload {
            searches: self.searches,
        }
    }
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct SearchResponse {
    pub ids: Vec<Vec<String>>,
    pub documents: Vec<Option<Vec<Option<String>>>>,
    pub embeddings: Vec<Option<Vec<Option<Vec<f32>>>>>,
    pub metadatas: Vec<Option<Vec<Option<Metadata>>>>,
    pub scores: Vec<Option<Vec<Option<f32>>>>,
    pub select: Vec<Vec<Key>>,
}

impl From<(SearchResult, Vec<SearchPayload>)> for SearchResponse {
    fn from((result, payloads): (SearchResult, Vec<SearchPayload>)) -> Self {
        let num_payloads = payloads.len();
        let mut res = Self {
            ids: Vec::with_capacity(num_payloads),
            documents: Vec::with_capacity(num_payloads),
            embeddings: Vec::with_capacity(num_payloads),
            metadatas: Vec::with_capacity(num_payloads),
            scores: Vec::with_capacity(num_payloads),
            select: Vec::with_capacity(num_payloads),
        };

        for (payload_result, payload) in result.results.into_iter().zip(payloads) {
            // Get the sorted keys for this payload
            let mut payload_select = Vec::from_iter(payload.select.keys.iter().cloned());
            payload_select.sort();

            let num_records = payload_result.records.len();
            let mut ids = Vec::with_capacity(num_records);
            let mut documents = Vec::with_capacity(num_records);
            let mut embeddings = Vec::with_capacity(num_records);
            let mut metadatas = Vec::with_capacity(num_records);
            let mut scores = Vec::with_capacity(num_records);

            for record in payload_result.records {
                ids.push(record.id);
                documents.push(record.document);
                embeddings.push(record.embedding);
                metadatas.push(record.metadata);
                scores.push(record.score);
            }

            res.ids.push(ids);
            res.select.push(payload_select.clone());

            // Push documents if requested by this payload, otherwise None
            res.documents.push(
                payload_select
                    .binary_search(&Key::Document)
                    .is_ok()
                    .then_some(documents),
            );

            // Push embeddings if requested by this payload, otherwise None
            res.embeddings.push(
                payload_select
                    .binary_search(&Key::Embedding)
                    .is_ok()
                    .then_some(embeddings),
            );

            // Push metadatas if requested by this payload, otherwise None
            // Include if either Key::Metadata is present or any Key::MetadataField(_)
            let has_metadata = payload_select.binary_search(&Key::Metadata).is_ok()
                || payload_select
                    .last()
                    .is_some_and(|field| matches!(field, Key::MetadataField(_)));
            res.metadatas.push(has_metadata.then_some(metadatas));

            // Push scores if requested by this payload, otherwise None
            res.scores.push(
                payload_select
                    .binary_search(&Key::Score)
                    .is_ok()
                    .then_some(scores),
            );
        }

        res
    }
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Error executing plan: {0}")]
    Executor(#[from] ExecutorError),
    #[error(transparent)]
    Other(#[from] Box<dyn ChromaError>),
}

impl ChromaError for QueryError {
    fn code(&self) -> ErrorCodes {
        match self {
            QueryError::Executor(e) => e.code(),
            QueryError::Other(err) => err.code(),
        }
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct HealthCheckResponse {
    pub is_executor_ready: bool,
    pub is_log_client_ready: bool,
}

impl HealthCheckResponse {
    pub fn get_status_code(&self) -> tonic::Code {
        if self.is_executor_ready && self.is_log_client_ready {
            tonic::Code::Ok
        } else {
            tonic::Code::Unavailable
        }
    }
}

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Error converting: {0}")]
    Conversion(#[from] QueryConversionError),
    #[error("Error converting plan to proto: {0}")]
    PlanToProto(#[from] PlanToProtoError),
    #[error(transparent)]
    Grpc(#[from] Status),
    #[error("Inconsistent data")]
    InconsistentData,
    #[error("Collection is missing HNSW configuration")]
    CollectionMissingHnswConfiguration,
    #[error("Internal error: {0}")]
    Internal(Box<dyn ChromaError>),
    #[error("Error sending backfill request to compactor: {0}")]
    BackfillError(Box<dyn ChromaError>),
    #[error("Not implemented: {0}")]
    NotImplemented(String),
}

impl ChromaError for ExecutorError {
    fn code(&self) -> ErrorCodes {
        match self {
            ExecutorError::Conversion(_) => ErrorCodes::InvalidArgument,
            ExecutorError::PlanToProto(_) => ErrorCodes::Internal,
            ExecutorError::Grpc(e) => e.code().into(),
            ExecutorError::InconsistentData => ErrorCodes::Internal,
            ExecutorError::CollectionMissingHnswConfiguration => ErrorCodes::Internal,
            ExecutorError::Internal(e) => e.code(),
            ExecutorError::BackfillError(e) => e.code(),
            ExecutorError::NotImplemented(_) => ErrorCodes::Unimplemented,
        }
    }
}

//////////////////////////  Attached Function Operations //////////////////////////

#[non_exhaustive]
#[derive(Clone, Debug, Deserialize, Serialize, Validate)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct AttachFunctionRequest {
    #[validate(length(min = 1))]
    pub name: String,
    pub function_id: String,
    pub output_collection: String,
    #[serde(default = "default_empty_json_object")]
    pub params: serde_json::Value,
}

fn default_empty_json_object() -> serde_json::Value {
    serde_json::json!({})
}

impl AttachFunctionRequest {
    pub fn try_new(
        name: String,
        function_id: String,
        output_collection: String,
        params: serde_json::Value,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            name,
            function_id,
            output_collection,
            params,
        };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct AttachedFunctionInfo {
    pub id: String,
    pub name: String,
    pub function_id: String,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct AttachFunctionResponse {
    pub attached_function: AttachedFunctionInfo,
}

#[derive(Error, Debug)]
pub enum AttachFunctionError {
    #[error(" Attached Function with name [{0}] already exists")]
    AlreadyExists(String),
    #[error("Input collection [{0}] does not exist")]
    InputCollectionNotFound(String),
    #[error("Output collection [{0}] already exists")]
    OutputCollectionExists(String),
    #[error(transparent)]
    Validation(#[from] ChromaValidationError),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for AttachFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            AttachFunctionError::AlreadyExists(_) => ErrorCodes::AlreadyExists,
            AttachFunctionError::InputCollectionNotFound(_) => ErrorCodes::NotFound,
            AttachFunctionError::OutputCollectionExists(_) => ErrorCodes::AlreadyExists,
            AttachFunctionError::Validation(err) => err.code(),
            AttachFunctionError::Internal(err) => err.code(),
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Deserialize, Validate, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DetachFunctionRequest {
    /// Whether to delete the output collection as well
    #[serde(default)]
    pub delete_output: bool,
}

impl DetachFunctionRequest {
    pub fn try_new(delete_output: bool) -> Result<Self, ChromaValidationError> {
        let request = Self { delete_output };
        request.validate().map_err(ChromaValidationError::from)?;
        Ok(request)
    }
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DetachFunctionResponse {
    pub success: bool,
}

#[derive(Error, Debug)]
pub enum DetachFunctionError {
    #[error(" Attached Function with ID [{0}] does not exist")]
    NotFound(String),
    #[error(transparent)]
    Validation(#[from] ChromaValidationError),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for DetachFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            DetachFunctionError::NotFound(_) => ErrorCodes::NotFound,
            DetachFunctionError::Validation(err) => err.code(),
            DetachFunctionError::Internal(err) => err.code(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{MetadataValue, SparseVector, UpdateMetadataValue};
    use std::collections::HashMap;

    #[test]
    fn test_create_database_min_length() {
        let request = CreateDatabaseRequest::try_new("default_tenant".to_string(), "a".to_string());
        assert!(request.is_err());
    }

    #[test]
    fn test_create_tenant_min_length() {
        let request = CreateTenantRequest::try_new("a".to_string());
        assert!(request.is_err());
    }

    #[test]
    fn test_add_request_validates_sparse_vectors() {
        let mut metadata = HashMap::new();
        // Add unsorted sparse vector - should fail validation
        metadata.insert(
            "sparse".to_string(),
            MetadataValue::SparseVector(SparseVector::new(vec![3, 1, 2], vec![0.3, 0.1, 0.2])),
        );

        let result = AddCollectionRecordsRequest::try_new(
            "tenant".to_string(),
            "database".to_string(),
            CollectionUuid(uuid::Uuid::new_v4()),
            vec!["id1".to_string()],
            vec![vec![0.1, 0.2]],
            None,
            None,
            Some(vec![Some(metadata)]),
        );

        // Should fail because sparse vector is not sorted
        assert!(result.is_err());
    }

    #[test]
    fn test_update_request_validates_sparse_vectors() {
        let mut metadata = HashMap::new();
        // Add unsorted sparse vector - should fail validation
        metadata.insert(
            "sparse".to_string(),
            UpdateMetadataValue::SparseVector(SparseVector::new(
                vec![3, 1, 2],
                vec![0.3, 0.1, 0.2],
            )),
        );

        let result = UpdateCollectionRecordsRequest::try_new(
            "tenant".to_string(),
            "database".to_string(),
            CollectionUuid(uuid::Uuid::new_v4()),
            vec!["id1".to_string()],
            None,
            None,
            None,
            Some(vec![Some(metadata)]),
        );

        // Should fail because sparse vector is not sorted
        assert!(result.is_err());
    }

    #[test]
    fn test_upsert_request_validates_sparse_vectors() {
        let mut metadata = HashMap::new();
        // Add unsorted sparse vector - should fail validation
        metadata.insert(
            "sparse".to_string(),
            UpdateMetadataValue::SparseVector(SparseVector::new(
                vec![3, 1, 2],
                vec![0.3, 0.1, 0.2],
            )),
        );

        let result = UpsertCollectionRecordsRequest::try_new(
            "tenant".to_string(),
            "database".to_string(),
            CollectionUuid(uuid::Uuid::new_v4()),
            vec!["id1".to_string()],
            vec![vec![0.1, 0.2]],
            None,
            None,
            Some(vec![Some(metadata)]),
        );

        // Should fail because sparse vector is not sorted
        assert!(result.is_err());
    }
}
