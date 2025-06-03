use crate::collection_configuration::InternalCollectionConfiguration;
use crate::collection_configuration::UpdateCollectionConfiguration;
use crate::error::QueryConversionError;
use crate::operator::GetResult;
use crate::operator::KnnBatchResult;
use crate::operator::KnnProjectionRecord;
use crate::operator::ProjectionRecord;
use crate::plan::PlanToProtoError;
use crate::validators::{
    validate_name, validate_non_empty_collection_update_metadata, validate_non_empty_metadata,
};
use crate::Collection;
use crate::CollectionConfigurationToInternalConfigurationError;
use crate::CollectionConversionError;
use crate::CollectionUuid;
use crate::DistributedSpannParametersFromSegmentError;
use crate::HnswParametersFromSegmentError;
use crate::Metadata;
use crate::SegmentConversionError;
use crate::SegmentScopeConversionError;
use crate::UpdateMetadata;
use crate::Where;
use chroma_error::ChromaValidationError;
use chroma_error::{ChromaError, ErrorCodes};
use serde::Deserialize;
use serde::Serialize;
use std::time::SystemTimeError;
use thiserror::Error;
use tonic::Status;
use utoipa::ToSchema;
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
    #[error("Collection [{0}] does not exists.")]
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

#[derive(Serialize, ToSchema)]
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

#[derive(Serialize, ToSchema)]
pub struct ChecklistResponse {
    pub max_batch_size: u32,
}

#[derive(Serialize, ToSchema)]
pub struct HeartbeatResponse {
    #[serde(rename(serialize = "nanosecond heartbeat"))]
    #[schema(rename = "nanosecond heartbeat")]
    pub nanosecond_heartbeat: u128,
}

#[derive(Debug, Error, ToSchema)]
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

#[derive(Serialize, Deserialize, ToSchema, Default)]
pub struct GetUserIdentityResponse {
    pub user_id: String,
    pub tenant: String,
    pub databases: Vec<String>,
}

#[non_exhaustive]
#[derive(Serialize, Validate, Deserialize, ToSchema)]
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

#[derive(Serialize, Deserialize, ToSchema)]
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
#[derive(Validate, Serialize, ToSchema)]
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

#[derive(Serialize, ToSchema)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct GetTenantResponse {
    pub name: String,
}

#[cfg(feature = "pyo3")]
#[pyo3::pymethods]
impl GetTenantResponse {
    #[getter]
    pub fn name(&self) -> &String {
        &self.name
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
#[derive(Validate, Serialize, ToSchema)]
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

#[derive(ToSchema, Serialize)]
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

#[derive(Serialize, Deserialize, Debug, ToSchema, Clone, Default)]
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
#[derive(Validate, Serialize, ToSchema)]
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
#[derive(Validate, ToSchema, Serialize)]
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
#[derive(Validate, Serialize, ToSchema)]
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

#[derive(Serialize, ToSchema)]
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
#[derive(Validate, Debug, Serialize, ToSchema)]
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
#[derive(Validate, Serialize, ToSchema)]
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
#[derive(Validate, Clone, Serialize, ToSchema)]
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
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Collection [{0}] does not exists")]
    NotFound(String),
}

impl ChromaError for GetCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionError::Internal(err) => err.code(),
            GetCollectionError::NotFound(_) => ErrorCodes::NotFound,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Validate, Serialize, ToSchema)]
pub struct CreateCollectionRequest {
    pub tenant_id: String,
    pub database_name: String,
    #[validate(custom(function = "validate_name"))]
    pub name: String,
    #[validate(custom(function = "validate_non_empty_metadata"))]
    pub metadata: Option<Metadata>,
    pub configuration: Option<InternalCollectionConfiguration>,
    pub get_or_create: bool,
}

impl CreateCollectionRequest {
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        name: String,
        metadata: Option<Metadata>,
        configuration: Option<InternalCollectionConfiguration>,
        get_or_create: bool,
    ) -> Result<Self, ChromaValidationError> {
        let request = Self {
            tenant_id,
            database_name,
            name,
            metadata,
            configuration,
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
    Configuration(#[from] serde_json::Error),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("The operation was aborted, {0}")]
    Aborted(String),
    #[error("SPANN is still in development. Not allowed to created spann indexes")]
    SpannNotImplemented,
    #[error("HNSW is not supported on this platform")]
    HnswNotSupported,
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
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Could not deserialize configuration")]
    Configuration(#[from] serde_json::Error),
    #[error("Could not deserialize collection ID")]
    CollectionId(#[from] uuid::Error),
}

impl ChromaError for GetCollectionsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionsError::Internal(err) => err.code(),
            GetCollectionsError::Configuration(_) => ErrorCodes::Internal,
            GetCollectionsError::CollectionId(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Clone, Deserialize, Serialize, Debug, ToSchema)]
pub enum CollectionMetadataUpdate {
    ResetMetadata,
    UpdateMetadata(UpdateMetadata),
}

#[non_exhaustive]
#[derive(Clone, Validate, Debug, Serialize, ToSchema)]
pub struct UpdateCollectionRequest {
    pub collection_id: CollectionUuid,
    #[validate(custom(function = "validate_name"))]
    pub new_name: Option<String>,
    #[validate(custom(function = "validate_non_empty_collection_update_metadata"))]
    pub new_metadata: Option<CollectionMetadataUpdate>,
    pub new_configuration: Option<UpdateCollectionConfiguration>,
}

impl UpdateCollectionRequest {
    pub fn try_new(
        collection_id: CollectionUuid,
        new_name: Option<String>,
        new_metadata: Option<CollectionMetadataUpdate>,
        new_configuration: Option<UpdateCollectionConfiguration>,
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

#[derive(Serialize, ToSchema)]
pub struct UpdateCollectionResponse {}

#[derive(Error, Debug)]
pub enum UpdateCollectionError {
    #[error("Collection [{0}] does not exists")]
    NotFound(String),
    #[error("Metadata reset unsupported")]
    MetadataResetUnsupported,
    #[error("Could not serialize configuration")]
    Configuration(#[from] serde_json::Error),
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Could not parse config: {0}")]
    InvalidConfig(#[from] CollectionConfigurationToInternalConfigurationError),
    #[error("SPANN is still in development. Not allowed to created spann indexes")]
    SpannNotImplemented,
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
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Validate, Serialize, ToSchema)]
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

#[derive(Serialize, ToSchema)]
pub struct DeleteCollectionResponse {}

#[derive(Error, Debug)]
pub enum DeleteCollectionError {
    #[error("Collection [{0}] does not exists")]
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
#[derive(Clone, Validate, Serialize, ToSchema)]
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
    #[error("Collection [{0}] does not exists")]
    NotFound(String),
    #[error("Failed to convert proto segment")]
    SegmentConversionError(#[from] SegmentConversionError),
}

impl ChromaError for ForkCollectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            ForkCollectionError::AlreadyExists(_) => ErrorCodes::AlreadyExists,
            ForkCollectionError::CollectionConversionError(collection_conversion_error) => {
                collection_conversion_error.code()
            }
            ForkCollectionError::DuplicateSegment => ErrorCodes::Internal,
            ForkCollectionError::Field(_) => ErrorCodes::FailedPrecondition,
            ForkCollectionError::Local => ErrorCodes::Unimplemented,
            ForkCollectionError::Internal(chroma_error) => chroma_error.code(),
            ForkCollectionError::NotFound(_) => ErrorCodes::NotFound,
            ForkCollectionError::SegmentConversionError(segment_conversion_error) => {
                segment_conversion_error.code()
            }
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
pub enum GetCollectionSizeError {
    #[error(transparent)]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Collection [{0}] does not exists")]
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
    #[error("Collection [{0}] does not exists")]
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

#[non_exhaustive]
#[derive(Debug, Clone, Validate, Serialize, ToSchema)]
pub struct AddCollectionRecordsRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Vec<String>,
    pub embeddings: Option<Vec<Vec<f32>>>,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
    pub metadatas: Option<Vec<Option<Metadata>>>,
}

impl AddCollectionRecordsRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        collection_id: CollectionUuid,
        ids: Vec<String>,
        embeddings: Option<Vec<Vec<f32>>>,
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
}

#[derive(Serialize, ToSchema, Default, Deserialize)]
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
            AddCollectionRecordsError::Backoff => ErrorCodes::Unavailable,
            AddCollectionRecordsError::Other(err) => err.code(),
        }
    }
}

////////////////////////// UpdateCollectionRecords //////////////////////////

#[non_exhaustive]
#[derive(Debug, Clone, Validate, Serialize, ToSchema)]
pub struct UpdateCollectionRecordsRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Vec<String>,
    pub embeddings: Option<Vec<Option<Vec<f32>>>>,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
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
}

#[derive(Serialize, ToSchema)]
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
            UpdateCollectionRecordsError::Backoff => ErrorCodes::Unavailable,
            UpdateCollectionRecordsError::Other(err) => err.code(),
        }
    }
}

////////////////////////// UpsertCollectionRecords //////////////////////////

#[non_exhaustive]
#[derive(Debug, Clone, Validate, Serialize, ToSchema)]
pub struct UpsertCollectionRecordsRequest {
    pub tenant_id: String,
    pub database_name: String,
    pub collection_id: CollectionUuid,
    pub ids: Vec<String>,
    pub embeddings: Option<Vec<Vec<f32>>>,
    pub documents: Option<Vec<Option<String>>>,
    pub uris: Option<Vec<Option<String>>>,
    pub metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

impl UpsertCollectionRecordsRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        tenant_id: String,
        database_name: String,
        collection_id: CollectionUuid,
        ids: Vec<String>,
        embeddings: Option<Vec<Vec<f32>>>,
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
}

#[derive(Serialize, ToSchema)]
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
            UpsertCollectionRecordsError::Backoff => ErrorCodes::Unavailable,
            UpsertCollectionRecordsError::Other(err) => err.code(),
        }
    }
}

////////////////////////// DeleteCollectionRecords //////////////////////////

#[non_exhaustive]
#[derive(Debug, Clone, Validate, Serialize, ToSchema)]
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
}

#[derive(Serialize, ToSchema)]
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
            DeleteCollectionRecordsError::Backoff => ErrorCodes::Unavailable,
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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
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

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema, PartialEq)]
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

////////////////////////// Get //////////////////////////

#[non_exhaustive]
#[derive(Debug, Clone, Validate, Serialize, ToSchema)]
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
}

#[derive(Clone, Deserialize, Serialize, Debug, ToSchema, Default)]
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

#[non_exhaustive]
#[derive(Debug, Clone, Validate, Serialize, ToSchema)]
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
}

#[derive(Clone, Deserialize, Serialize, ToSchema, Debug)]
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

#[derive(Serialize, ToSchema)]
pub struct HealthCheckResponse {
    pub is_executor_ready: bool,
}

impl HealthCheckResponse {
    pub fn get_status_code(&self) -> tonic::Code {
        if self.is_executor_ready {
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
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
}
