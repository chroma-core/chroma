use crate::types::FlushCompactionRequest;
use crate::types::SysDbError;
use crate::types::{self as internal, validate_uuid};
use crate::{
    backend::{Assignable, BackendFactory, Runnable},
    config::RootConfig,
};
use backon::{ConstantBuilder, Retryable};
use chroma_config::{registry::Registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::version_file::{VersionFileManager, VersionFileType};
use chroma_storage::Storage;
use chroma_types::chroma_proto::collection_version_info::VersionChangeReason;
use chroma_types::chroma_proto::{
    sys_db_server::{SysDb, SysDbServer},
    AttachFunctionRequest, AttachFunctionResponse, BatchGetCollectionSoftDeleteStatusRequest,
    BatchGetCollectionSoftDeleteStatusResponse, BatchGetCollectionVersionFilePathsRequest,
    BatchGetCollectionVersionFilePathsResponse, CheckCollectionsRequest, CheckCollectionsResponse,
    CleanupExpiredPartialAttachedFunctionsRequest, CleanupExpiredPartialAttachedFunctionsResponse,
    CountCollectionsRequest, CountCollectionsResponse, CountForksRequest, CountForksResponse,
    CreateCollectionRequest, CreateCollectionResponse, CreateDatabaseRequest,
    CreateDatabaseResponse, CreateSegmentRequest, CreateSegmentResponse, CreateTenantRequest,
    CreateTenantResponse, DeleteCollectionRequest, DeleteCollectionResponse,
    DeleteCollectionVersionRequest, DeleteCollectionVersionResponse, DeleteDatabaseRequest,
    DeleteDatabaseResponse, DeleteSegmentRequest, DeleteSegmentResponse, DetachFunctionRequest,
    DetachFunctionResponse, FinishAttachedFunctionDeletionRequest,
    FinishAttachedFunctionDeletionResponse, FinishCollectionDeletionRequest,
    FinishCollectionDeletionResponse, FinishCreateAttachedFunctionRequest,
    FinishCreateAttachedFunctionResponse, FinishDatabaseDeletionRequest,
    FinishDatabaseDeletionResponse, FlushCollectionCompactionAndAttachedFunctionRequest,
    FlushCollectionCompactionAndAttachedFunctionResponse, FlushCollectionCompactionRequest,
    FlushCollectionCompactionResponse, ForkCollectionRequest, ForkCollectionResponse,
    GetAttachedFunctionsRequest, GetAttachedFunctionsResponse, GetAttachedFunctionsToGcRequest,
    GetAttachedFunctionsToGcResponse, GetCollectionByResourceNameRequest, GetCollectionRequest,
    GetCollectionResponse, GetCollectionSizeRequest, GetCollectionSizeResponse,
    GetCollectionWithSegmentsRequest, GetCollectionWithSegmentsResponse, GetCollectionsRequest,
    GetCollectionsResponse, GetDatabaseRequest, GetDatabaseResponse, GetFunctionsRequest,
    GetFunctionsResponse, GetLastCompactionTimeForTenantRequest,
    GetLastCompactionTimeForTenantResponse, GetSegmentsRequest, GetSegmentsResponse,
    GetTenantRequest, GetTenantResponse, IncrementCompactionFailureCountRequest,
    IncrementCompactionFailureCountResponse, ListCollectionVersionsRequest,
    ListCollectionVersionsResponse, ListCollectionsToGcRequest, ListCollectionsToGcResponse,
    ListDatabasesRequest, ListDatabasesResponse, MarkVersionForDeletionRequest,
    MarkVersionForDeletionResponse, ResetStateResponse, RestoreCollectionRequest,
    RestoreCollectionResponse, SetLastCompactionTimeForTenantRequest, SetTenantResourceNameRequest,
    SetTenantResourceNameResponse, UpdateCollectionRequest, UpdateCollectionResponse,
    UpdateSegmentRequest, UpdateSegmentResponse,
};
use chroma_types::{Collection, CollectionUuid, DatabaseName};
use std::collections::HashMap;
use thiserror::Error;
use tokio::{
    select,
    signal::unix::{signal, SignalKind},
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::instrument;

pub struct SysdbService {
    port: u16,
    #[allow(dead_code)]
    local_region_object_storage: Storage,
    backends: BackendFactory,
}

impl SysdbService {
    pub fn new(port: u16, local_region_object_storage: Storage, backends: BackendFactory) -> Self {
        Self {
            port,
            local_region_object_storage,
            backends,
        }
    }

    pub async fn run(self) {
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to create SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to create SIGINT handler");

        let addr = format!("[::]:{}", self.port)
            .parse()
            .expect("Failed to parse address");

        tracing::info!("Sysdb service listening on {}", addr);

        let (health_reporter, health_service) = tonic_health::server::health_reporter();

        // TODO(Sanket): More sophisticated is_ready logic.
        health_reporter
            .set_serving::<SysDbServer<SysdbService>>()
            .await;

        let backends = self.backends.clone();
        Box::pin(
            Server::builder()
                .layer(chroma_tracing::GrpcServerTraceLayer)
                .add_service(health_service)
                .add_service(SysDbServer::new(self))
                .serve_with_shutdown(addr, async move {
                    // TODO(Sanket): Drain existing requests before shutting down.
                    select! {
                        _ = sigterm.recv() => {
                            backends.close().await;
                            tracing::info!("Received SIGTERM, shutting down server");
                        }
                        _ = sigint.recv() => {
                            backends.close().await;
                            tracing::info!("Received SIGINT, shutting down server");
                        }
                    }
                }),
        )
        .await
        .expect("Server failed");
    }
}

#[derive(Error, Debug)]
pub enum SysdbServiceError {
    #[error("Config validation error: {0}")]
    ConfigValidation(String),
}

impl ChromaError for SysdbServiceError {
    fn code(&self) -> ErrorCodes {
        match self {
            SysdbServiceError::ConfigValidation(_) => ErrorCodes::InvalidArgument,
        }
    }
}

#[async_trait::async_trait]
impl Configurable<RootConfig> for SysdbService {
    async fn try_from_config(
        config: &RootConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb_config = &config.sysdb_service;
        sysdb_config
            .regions_and_topologies
            .validate()
            .map_err(|e| e.boxed())?;
        let backends =
            BackendFactory::try_from_config(&sysdb_config.regions_and_topologies, registry).await?;
        let local_region_config = sysdb_config
            .regions_and_topologies
            .preferred_region_config()
            .ok_or_else(|| -> Box<dyn ChromaError> {
                Box::new(SysdbServiceError::ConfigValidation(
                    "local region config not found".to_string(),
                ))
            })?;
        let storage = Storage::try_from_config(&local_region_config.storage, registry).await?;
        Ok(SysdbService::new(sysdb_config.port, storage, backends))
    }
}

#[async_trait::async_trait]
impl SysDb for SysdbService {
    async fn create_database(
        &self,
        request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        let proto_req = request.into_inner();
        let internal_req: internal::CreateDatabaseRequest = proto_req
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        let backend = internal_req.assign(&self.backends);
        let resp = internal_req.run(backend).await?;

        Ok(Response::new(resp.into()))
    }

    async fn get_database(
        &self,
        request: Request<GetDatabaseRequest>,
    ) -> Result<Response<GetDatabaseResponse>, Status> {
        let proto_req = request.into_inner();
        let internal_req: internal::GetDatabaseRequest = proto_req
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        let backend = internal_req.assign(&self.backends);
        let internal_resp = internal_req.run(backend).await?;

        let proto_resp = GetDatabaseResponse {
            database: Some(internal_resp.database.into()),
        };

        Ok(Response::new(proto_resp))
    }

    async fn list_databases(
        &self,
        request: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        let proto_req = request.into_inner();
        let internal_req: internal::ListDatabasesRequest = proto_req
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        let backends = internal_req.assign(&self.backends);
        let internal_resp = internal_req.run(backends).await?;

        // Convert internal Database to proto Database
        // Names are already prefixed with topology (e.g., "tilt-spanning+my_db")
        let databases: Vec<chroma_types::chroma_proto::Database> = internal_resp
            .into_iter()
            .map(|db| chroma_types::chroma_proto::Database {
                id: db.id.to_string(),
                name: db.name,
                tenant: db.tenant,
            })
            .collect();

        Ok(Response::new(ListDatabasesResponse { databases }))
    }

    async fn delete_database(
        &self,
        _request: Request<DeleteDatabaseRequest>,
    ) -> Result<Response<DeleteDatabaseResponse>, Status> {
        Err(Status::unimplemented("delete_database is not supported"))
    }

    async fn finish_database_deletion(
        &self,
        _request: Request<FinishDatabaseDeletionRequest>,
    ) -> Result<Response<FinishDatabaseDeletionResponse>, Status> {
        Err(Status::unimplemented(
            "finish_database_deletion is not supported",
        ))
    }

    async fn create_tenant(
        &self,
        request: Request<CreateTenantRequest>,
    ) -> Result<Response<CreateTenantResponse>, Status> {
        let proto_req = request.into_inner();
        let internal_req: internal::CreateTenantRequest = proto_req
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        let backends = internal_req.assign(&self.backends);
        let resp = internal_req.run(backends).await?;

        Ok(Response::new(resp.into()))
    }

    async fn get_tenant(
        &self,
        request: Request<GetTenantRequest>,
    ) -> Result<Response<GetTenantResponse>, Status> {
        let proto_req = request.into_inner();
        let internal_req: internal::GetTenantsRequest = proto_req
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        let backend = internal_req.assign(&self.backends);
        let internal_resp = internal_req.run(backend).await?;

        Ok(Response::new(internal_resp.try_into()?))
    }

    async fn set_tenant_resource_name(
        &self,
        _request: Request<SetTenantResourceNameRequest>,
    ) -> Result<Response<SetTenantResourceNameResponse>, Status> {
        Err(Status::unimplemented(
            "set_tenant_resource_name is not supported",
        ))
    }

    async fn create_segment(
        &self,
        _request: Request<CreateSegmentRequest>,
    ) -> Result<Response<CreateSegmentResponse>, Status> {
        Err(Status::unimplemented("create_segment is not supported"))
    }

    async fn delete_segment(
        &self,
        _request: Request<DeleteSegmentRequest>,
    ) -> Result<Response<DeleteSegmentResponse>, Status> {
        Err(Status::unimplemented("delete_segment is not supported"))
    }

    async fn get_segments(
        &self,
        _request: Request<GetSegmentsRequest>,
    ) -> Result<Response<GetSegmentsResponse>, Status> {
        Err(Status::unimplemented("get_segments is not supported"))
    }

    async fn update_segment(
        &self,
        _request: Request<UpdateSegmentRequest>,
    ) -> Result<Response<UpdateSegmentResponse>, Status> {
        Err(Status::unimplemented("update_segment is not supported"))
    }

    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        let proto_req = request.into_inner();
        let internal_req: internal::CreateCollectionRequest = proto_req
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        let backend = internal_req.assign(&self.backends);
        let internal_resp = internal_req.run(backend).await?;

        let proto_resp: CreateCollectionResponse = internal_resp
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        Ok(Response::new(proto_resp))
    }

    async fn delete_collection(
        &self,
        request: Request<DeleteCollectionRequest>,
    ) -> Result<Response<DeleteCollectionResponse>, Status> {
        let proto_req = request.into_inner();

        // Convert DeleteCollectionRequest to UpdateCollectionRequest for soft delete
        let collection_id = validate_uuid(&proto_req.id)
            .map_err(|e| Status::invalid_argument(format!("Invalid collection ID: {}", e)))?;

        let database_name = DatabaseName::new(&proto_req.database)
            .ok_or_else(|| Status::invalid_argument("database name is required"))?;

        // First, get the current collection to retrieve its name
        let backend_for_get = internal::GetCollectionWithSegmentsRequest {
            database_name: database_name.clone(),
            id: CollectionUuid(collection_id),
        }
        .assign(&self.backends);

        let current_collection = internal::GetCollectionWithSegmentsRequest {
            database_name: database_name.clone(),
            id: CollectionUuid(collection_id),
        }
        .run(backend_for_get)
        .await
        .map_err(|e: SysDbError| Status::from(e))?;

        // Generate new name with "_deleted_" prefix and collection ID
        let deleted_new_name = format!(
            "_deleted_{}_{}",
            current_collection.collection.name, collection_id
        );

        // NOTE: This is not a TOCTOU because the transaction below checks to make sure
        // the concerned collection is not soft deleted before proceeding.

        // Create an UpdateCollectionRequest that marks the collection as deleted with the new name
        let internal_req = internal::UpdateCollectionRequest {
            database_name,
            id: CollectionUuid(collection_id),
            name: Some(deleted_new_name),
            dimension: None,
            metadata: None,
            reset_metadata: false,
            new_configuration: None,
            cursor_updates: None,
            is_deleted: Some(true), // Mark as soft deleted
        };

        let backend = internal_req.assign(&self.backends);
        let internal_resp = internal_req
            .run(backend)
            .await
            .map_err(|e: SysDbError| Status::from(e))?;

        let proto_resp: DeleteCollectionResponse = internal_resp
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        Ok(Response::new(proto_resp))
    }

    async fn finish_collection_deletion(
        &self,
        _request: Request<FinishCollectionDeletionRequest>,
    ) -> Result<Response<FinishCollectionDeletionResponse>, Status> {
        Err(Status::unimplemented(
            "finish_collection_deletion is not supported",
        ))
    }

    async fn get_collection(
        &self,
        _request: Request<GetCollectionRequest>,
    ) -> Result<Response<GetCollectionResponse>, Status> {
        Err(Status::unimplemented("get_collection is not supported"))
    }

    async fn get_collections(
        &self,
        request: Request<GetCollectionsRequest>,
    ) -> Result<Response<GetCollectionsResponse>, Status> {
        let proto_req = request.into_inner();
        let internal_req: internal::GetCollectionsRequest = proto_req
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        let backend = internal_req.assign(&self.backends);
        let internal_resp = internal_req.run(backend).await?;

        let proto_resp: GetCollectionsResponse = internal_resp
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        Ok(Response::new(proto_resp))
    }

    async fn get_collection_by_resource_name(
        &self,
        _request: Request<GetCollectionByResourceNameRequest>,
    ) -> Result<Response<GetCollectionResponse>, Status> {
        Err(Status::unimplemented(
            "get_collection_by_resource_name is not supported",
        ))
    }

    async fn count_collections(
        &self,
        request: Request<CountCollectionsRequest>,
    ) -> Result<Response<CountCollectionsResponse>, Status> {
        let proto_req = request.into_inner();
        let internal_req: internal::CountCollectionsRequest = proto_req
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        let backends = internal_req.assign(&self.backends);
        let internal_resp = internal_req.run(backends).await?;

        let proto_resp: CountCollectionsResponse = internal_resp.into();

        Ok(Response::new(proto_resp))
    }

    async fn get_collection_with_segments(
        &self,
        request: Request<GetCollectionWithSegmentsRequest>,
    ) -> Result<Response<GetCollectionWithSegmentsResponse>, Status> {
        let proto_req = request.into_inner();
        let internal_req: internal::GetCollectionWithSegmentsRequest = proto_req
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        let backend = internal_req.assign(&self.backends);
        let internal_resp = internal_req.run(backend).await?;

        let proto_resp: GetCollectionWithSegmentsResponse = internal_resp
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        Ok(Response::new(proto_resp))
    }

    async fn check_collections(
        &self,
        _request: Request<CheckCollectionsRequest>,
    ) -> Result<Response<CheckCollectionsResponse>, Status> {
        Err(Status::unimplemented("check_collections is not supported"))
    }

    async fn update_collection(
        &self,
        request: Request<UpdateCollectionRequest>,
    ) -> Result<Response<UpdateCollectionResponse>, Status> {
        let proto_req = request.into_inner();
        let internal_req: internal::UpdateCollectionRequest = proto_req
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        let backend = internal_req.assign(&self.backends);
        let internal_resp = internal_req.run(backend).await?;

        let proto_resp: UpdateCollectionResponse = internal_resp
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        Ok(Response::new(proto_resp))
    }

    async fn fork_collection(
        &self,
        _request: Request<ForkCollectionRequest>,
    ) -> Result<Response<ForkCollectionResponse>, Status> {
        Err(Status::unimplemented("fork_collection is not supported"))
    }

    async fn count_forks(
        &self,
        _request: Request<CountForksRequest>,
    ) -> Result<Response<CountForksResponse>, Status> {
        Err(Status::unimplemented("count_forks is not supported"))
    }

    async fn reset_state(
        &self,
        _request: Request<()>,
    ) -> Result<Response<ResetStateResponse>, Status> {
        let internal_req = internal::ResetStateRequest {};

        let backends = internal_req.assign(&self.backends);
        let internal_resp = internal_req.run(backends).await.map_err(Status::from)?;

        let proto_resp: ResetStateResponse = internal_resp.try_into().map_err(Status::from)?;

        Ok(Response::new(proto_resp))
    }

    async fn get_last_compaction_time_for_tenant(
        &self,
        request: Request<GetLastCompactionTimeForTenantRequest>,
    ) -> Result<Response<GetLastCompactionTimeForTenantResponse>, Status> {
        let proto_req = request.into_inner();
        // Create a GetTenantsRequest with the same tenant IDs
        let get_tenants_req = internal::GetTenantsRequest {
            ids: proto_req.tenant_id.clone(),
        };
        let backend = get_tenants_req.assign(&self.backends);
        let tenants_response = get_tenants_req
            .run(backend)
            .await
            .map_err(|e: SysDbError| Status::from(e))?;
        // Extract last compaction times from the tenants
        let tenant_last_compaction_times: Vec<
            chroma_types::chroma_proto::TenantLastCompactionTime,
        > = tenants_response
            .tenants
            .into_iter()
            .map(
                |tenant| chroma_types::chroma_proto::TenantLastCompactionTime {
                    tenant_id: tenant.id,
                    last_compaction_time: tenant.last_compaction_time,
                },
            )
            .collect();
        Ok(Response::new(GetLastCompactionTimeForTenantResponse {
            tenant_last_compaction_time: tenant_last_compaction_times,
        }))
    }

    async fn set_last_compaction_time_for_tenant(
        &self,
        _request: Request<SetLastCompactionTimeForTenantRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "set_last_compaction_time_for_tenant is not supported",
        ))
    }

    async fn restore_collection(
        &self,
        _request: Request<RestoreCollectionRequest>,
    ) -> Result<Response<RestoreCollectionResponse>, Status> {
        Err(Status::unimplemented("restore_collection is not supported"))
    }

    async fn list_collection_versions(
        &self,
        _request: Request<ListCollectionVersionsRequest>,
    ) -> Result<Response<ListCollectionVersionsResponse>, Status> {
        Err(Status::unimplemented(
            "list_collection_versions is not supported",
        ))
    }

    async fn get_collection_size(
        &self,
        _request: Request<GetCollectionSizeRequest>,
    ) -> Result<Response<GetCollectionSizeResponse>, Status> {
        Err(Status::unimplemented(
            "get_collection_size is not supported",
        ))
    }

    async fn list_collections_to_gc(
        &self,
        request: Request<ListCollectionsToGcRequest>,
    ) -> Result<Response<ListCollectionsToGcResponse>, Status> {
        let proto_req = request.into_inner();
        let internal_req: internal::ListCollectionsToGcRequest = proto_req
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        let backends = internal_req.assign(&self.backends);
        let internal_resp = internal_req.run(backends).await?;

        let proto_resp: ListCollectionsToGcResponse = internal_resp.into();

        Ok(Response::new(proto_resp))
    }
    async fn mark_version_for_deletion(
        &self,
        request: Request<MarkVersionForDeletionRequest>,
    ) -> Result<Response<MarkVersionForDeletionResponse>, Status> {
        let proto_req = request.into_inner();
        let database_name = proto_req
            .database_name
            .as_ref()
            .and_then(DatabaseName::new)
            .ok_or_else(|| Status::invalid_argument("valid database_name is required"))?;

        let mut collection_id_to_success = HashMap::new();
        for version_list in &proto_req.versions {
            let collection_id_str = version_list.collection_id.clone();
            let success = match self
                .update_version_file_single_collection(
                    version_list,
                    &database_name,
                    VersionFileOperation::MarkForDeletion,
                )
                .await
            {
                Ok(_) => true,
                Err(e) => {
                    tracing::error!(
                        collection_id = %collection_id_str,
                        error = ?e,
                        "Failed to mark versions for deletion after retries"
                    );
                    false
                }
            };
            collection_id_to_success.insert(collection_id_str, success);
        }

        Ok(Response::new(MarkVersionForDeletionResponse {
            collection_id_to_success,
        }))
    }

    async fn delete_collection_version(
        &self,
        request: Request<DeleteCollectionVersionRequest>,
    ) -> Result<Response<DeleteCollectionVersionResponse>, Status> {
        let proto_req = request.into_inner();
        let database_name = proto_req
            .database_name
            .as_ref()
            .and_then(DatabaseName::new)
            .ok_or_else(|| Status::invalid_argument("database_name is required"))?;

        let mut collection_id_to_success = std::collections::HashMap::new();
        for version_list in &proto_req.versions {
            let collection_id_str = version_list.collection_id.clone();
            let success = self
                .update_version_file_single_collection(
                    version_list,
                    &database_name,
                    VersionFileOperation::DeleteVersions,
                )
                .await
                .is_ok();
            collection_id_to_success.insert(collection_id_str, success);
        }

        Ok(Response::new(DeleteCollectionVersionResponse {
            collection_id_to_success,
        }))
    }

    async fn batch_get_collection_version_file_paths(
        &self,
        request: Request<BatchGetCollectionVersionFilePathsRequest>,
    ) -> Result<Response<BatchGetCollectionVersionFilePathsResponse>, Status> {
        // TODO(tanujnay112): This is inefficient. Augment the GetCollections spanner API
        // to specify SELECT columns.
        let proto_req = request.into_inner();
        let internal_req: internal::GetCollectionsRequest = proto_req
            .try_into()
            .map_err(|e: internal::SysDbError| Status::from(e))?;

        let backend = internal_req.assign(&self.backends);
        let collections_resp = internal_req.run(backend).await?;
        let mut collection_id_to_version_file_path = HashMap::new();
        let mut missing_paths = Vec::new();

        for collection in collections_resp.collections {
            let collection_id_str = collection.collection_id.to_string();
            if let Some(version_file_path) = collection.version_file_path {
                collection_id_to_version_file_path.insert(collection_id_str, version_file_path);
            } else {
                missing_paths.push(collection_id_str);
            }
        }

        if !missing_paths.is_empty() {
            return Err(Status::not_found(format!(
                "Version file paths not found for collections: {}",
                missing_paths.join(", ")
            )));
        }

        Ok(Response::new(BatchGetCollectionVersionFilePathsResponse {
            collection_id_to_version_file_path,
        }))
    }

    async fn batch_get_collection_soft_delete_status(
        &self,
        _request: Request<BatchGetCollectionSoftDeleteStatusRequest>,
    ) -> Result<Response<BatchGetCollectionSoftDeleteStatusResponse>, Status> {
        Err(Status::unimplemented(
            "batch_get_collection_soft_delete_status is not supported",
        ))
    }

    async fn cleanup_expired_partial_attached_functions(
        &self,
        _request: Request<CleanupExpiredPartialAttachedFunctionsRequest>,
    ) -> Result<Response<CleanupExpiredPartialAttachedFunctionsResponse>, Status> {
        Err(Status::unimplemented(
            "cleanup_expired_partial_attached_functions is not supported",
        ))
    }

    async fn get_functions(
        &self,
        _request: Request<GetFunctionsRequest>,
    ) -> Result<Response<GetFunctionsResponse>, Status> {
        Err(Status::unimplemented("get_functions is not supported"))
    }

    async fn get_attached_functions_to_gc(
        &self,
        _request: Request<GetAttachedFunctionsToGcRequest>,
    ) -> Result<Response<GetAttachedFunctionsToGcResponse>, Status> {
        Err(Status::unimplemented(
            "get_attached_functions_to_gc is not supported",
        ))
    }

    async fn finish_attached_function_deletion(
        &self,
        _request: Request<FinishAttachedFunctionDeletionRequest>,
    ) -> Result<Response<FinishAttachedFunctionDeletionResponse>, Status> {
        Err(Status::unimplemented(
            "finish_attached_function_deletion is not supported",
        ))
    }

    async fn flush_collection_compaction(
        &self,
        request: Request<FlushCollectionCompactionRequest>,
    ) -> Result<Response<FlushCollectionCompactionResponse>, Status> {
        let proto_req = request.into_inner();

        let get_collections_req = internal::GetCollectionsRequest::try_from(proto_req.clone())?;
        let backend = get_collections_req.assign(&self.backends);

        let collection_id = get_collections_req
            .filter
            .ids
            .as_ref()
            .and_then(|ids| ids.first())
            .copied()
            .ok_or_else(|| Status::invalid_argument("Collection ID is required"))?;
        let database_name = get_collections_req
            .filter
            .database_name
            .clone()
            .ok_or_else(|| Status::invalid_argument("Database name is required"))?;

        // Create the version file in object storage and flush compaction with retry on stale entry
        let backoff = ConstantBuilder::default()
            .with_delay(std::time::Duration::ZERO)
            .with_max_times(3);

        let internal_resp = (|| async {
            let get_collections_req = get_collections_req.clone();
            let collection_response = get_collections_req.run(backend.clone()).await?;

            let collection: Collection = collection_response
                .collections
                .first()
                .ok_or_else(|| {
                    SysDbError::NotFound(format!("Collection {} not found", collection_id))
                })?
                .clone();
            let old_version_file_path = collection.version_file_path.clone().unwrap_or_default();
            let existing_version = proto_req.collection_version;
            let new_version = existing_version + 1;
            if collection.version != proto_req.collection_version {
                return Err(SysDbError::CollectionVersionStale {
                    current_version: collection.version,
                    compaction_version: proto_req.collection_version,
                });
            }

            let (new_version_file, version_file_path) = self
                .create_new_version_file(
                    &self.local_region_object_storage,
                    &collection,
                    proto_req.segment_compaction_info.clone(),
                    new_version as i64,
                    VersionFileType::Compaction,
                )
                .await
                .map_err(|e: SysDbError| {
                    tracing::error!("Failed to create new version file: {}", e);
                    e
                })?;

            // Construct the internal request with version file data
            let internal_req = FlushCompactionRequest {
                collection_id,
                tenant_id: proto_req.tenant_id.clone(),
                log_position: proto_req.log_position,
                current_collection_version: proto_req.collection_version,
                flush_segment_compaction_infos: proto_req.segment_compaction_info.clone(),
                total_records_post_compaction: proto_req.total_records_post_compaction,
                size_bytes_post_compaction: proto_req.size_bytes_post_compaction,
                schema_str: proto_req.schema_str.clone(),
                old_version_file_path: old_version_file_path.clone(),
                new_version_file,
                version_file_path,
                new_version,
                database_name: database_name.clone().into_string(),
            };

            // Execute the compaction flush
            // Use the same backend assigned to the get_collections request from the
            // beginning of this method.
            internal_req.run(backend.clone()).await
        })
        .retry(backoff)
        .when(|e: &SysDbError| {
            if matches!(e, SysDbError::CollectionEntryIsStale) {
                tracing::warn!(
                    "Collection entry is stale, retrying flush collection compaction for collection_id: {}",
                    collection_id
                );
                true
            } else {
                false
            }
        })
        .await?;

        Ok(Response::new(
            internal_resp
                .try_into()
                .map_err(|e: SysDbError| Status::from(e))?,
        ))
    }

    async fn attach_function(
        &self,
        _request: Request<AttachFunctionRequest>,
    ) -> Result<Response<AttachFunctionResponse>, Status> {
        Err(Status::unimplemented("attach_function is not supported"))
    }

    async fn get_attached_functions(
        &self,
        _request: Request<GetAttachedFunctionsRequest>,
    ) -> Result<Response<GetAttachedFunctionsResponse>, Status> {
        Err(Status::unimplemented(
            "get_attached_functions is not supported",
        ))
    }

    async fn detach_function(
        &self,
        _request: Request<DetachFunctionRequest>,
    ) -> Result<Response<DetachFunctionResponse>, Status> {
        Err(Status::unimplemented("detach_function is not supported"))
    }

    async fn finish_create_attached_function(
        &self,
        _request: Request<FinishCreateAttachedFunctionRequest>,
    ) -> Result<Response<FinishCreateAttachedFunctionResponse>, Status> {
        Err(Status::unimplemented(
            "finish_create_attached_function is not supported",
        ))
    }

    async fn flush_collection_compaction_and_attached_function(
        &self,
        _request: Request<FlushCollectionCompactionAndAttachedFunctionRequest>,
    ) -> Result<Response<FlushCollectionCompactionAndAttachedFunctionResponse>, Status> {
        Err(Status::unimplemented(
            "flush_collection_compaction_and_attached_function is not supported",
        ))
    }

    async fn increment_compaction_failure_count(
        &self,
        request: Request<IncrementCompactionFailureCountRequest>,
    ) -> Result<Response<IncrementCompactionFailureCountResponse>, Status> {
        let proto_req = request.into_inner();

        let internal_req = internal::UpdateCollectionRequest {
            id: CollectionUuid(validate_uuid(&proto_req.collection_id)?),
            database_name: proto_req
                .database_name
                .ok_or_else(|| Status::internal("database_name is required"))
                .and_then(|db_name| {
                    DatabaseName::new(db_name)
                        .ok_or_else(|| Status::internal("a valid database_name is required"))
                })?,
            name: None,
            dimension: None,
            metadata: None,
            reset_metadata: false,
            new_configuration: None,
            cursor_updates: Some(internal::UpdateCollectionCursor {
                compaction_failure_count_increment: Some(1),
            }),
            is_deleted: None,
        };

        // Execute the update
        let backend = internal_req.assign(&self.backends);
        let _internal_resp = internal_req.run(backend).await?;

        Ok(Response::new(IncrementCompactionFailureCountResponse {}))
    }
}

impl SysdbService {
    /// Create a new version file in object storage
    #[instrument(skip(self, storage, segments), level = "info", fields(collection_id = %collection.collection_id))]
    async fn create_new_version_file(
        &self,
        storage: &Storage,
        collection: &Collection,
        segments: Vec<chroma_types::chroma_proto::FlushSegmentCompactionInfo>,
        new_version: i64,
        version_file_type: VersionFileType,
    ) -> Result<(chroma_types::chroma_proto::CollectionVersionFile, String), SysDbError> {
        let version_file_manager = VersionFileManager::new(storage.clone());

        let mut version_file_pb = match &collection.version_file_path {
            Some(_) => {
                // Load existing version file and update it
                version_file_manager.fetch(collection).await?
            }
            None => chroma_types::chroma_proto::CollectionVersionFile {
                collection_info_immutable: Some(
                    chroma_types::chroma_proto::CollectionInfoImmutable {
                        tenant_id: collection.tenant.clone(),
                        database_id: collection.database.clone(),
                        collection_id: collection.collection_id.to_string(),
                        collection_name: collection.name.clone(),
                        collection_creation_secs: chrono::Utc::now().timestamp(),
                        database_name: collection.database.clone(),
                        ..Default::default()
                    },
                ),
                version_history: Some(chroma_types::chroma_proto::CollectionVersionHistory {
                    versions: vec![],
                }),
            },
        };

        // Handle empty segments like the Go sysdb - preserve existing segment info
        let segments_to_use = if segments.is_empty() {
            // If no new segments, preserve existing segment info from the latest version
            version_file_pb
                .version_history
                .as_ref()
                .and_then(|version_history| {
                    version_history
                        .versions
                        .last()
                        .and_then(|last_version| last_version.segment_info.as_ref())
                        .map(|segment_info| segment_info.segment_compaction_info.clone())
                })
                .unwrap_or_else(|| segments.clone())
        } else {
            segments
        };

        let new_version_file_path = version_file_manager.generate_file_path(
            collection,
            new_version,
            version_file_type.clone(),
        );
        let ts_secs = chrono::Utc::now().timestamp();
        let new_version_info = chroma_types::chroma_proto::CollectionVersionInfo {
            version: new_version,
            segment_info: Some(chroma_types::chroma_proto::CollectionSegmentInfo {
                segment_compaction_info: segments_to_use,
            }),
            collection_info_mutable: Some(chroma_types::chroma_proto::CollectionInfoMutable {
                current_log_position: collection.log_position,
                current_collection_version: collection.version as i64,
                updated_at_secs: ts_secs,
                dimension: 0, // Default value - not used in Go version
                last_compaction_time_secs: collection.last_compaction_time_secs as i64,
            }),
            created_at_secs: ts_secs,
            version_change_reason: VersionChangeReason::DataCompaction as i32,
            version_file_name: new_version_file_path.clone(),
            marked_for_deletion: false,
        };
        if let Some(ref mut version_history) = version_file_pb.version_history {
            version_history.versions.push(new_version_info);
        } else {
            version_file_pb.version_history =
                Some(chroma_types::chroma_proto::CollectionVersionHistory {
                    versions: vec![new_version_info],
                });
        }
        version_file_manager
            .upload(
                &new_version_file_path,
                &version_file_pb,
                collection,
                new_version,
            )
            .await
            .map_err(|e| {
                tracing::error!("Failed to upload version file: {}", e);
                e
            })?;

        Ok((version_file_pb, new_version_file_path))
    }

    /// Update a single collection's version file using a CAS retry loop.
    /// Shared by both mark_version_for_deletion and delete_collection_version.
    async fn update_version_file_single_collection(
        &self,
        version_list: &chroma_types::chroma_proto::VersionListForCollection,
        database_name: &DatabaseName,
        operation: VersionFileOperation,
    ) -> Result<(), SysDbError> {
        let collection_id =
            CollectionUuid(crate::types::validate_uuid(&version_list.collection_id)?);
        let target_versions: std::collections::HashSet<i64> =
            version_list.versions.iter().copied().collect();

        let backoff = ConstantBuilder::default()
            .with_delay(std::time::Duration::ZERO)
            .with_max_times(10);

        (|| async {
            // 1. Get collection to obtain current version_file_path
            let get_req = internal::GetCollectionsRequest::for_collection(
                collection_id,
                database_name.clone(),
                true,
            );
            let backend = get_req.assign(&self.backends);
            let collection_response = get_req.run(backend.clone()).await?;
            let collection = collection_response
                .collections
                .first()
                .ok_or_else(|| {
                    SysDbError::NotFound(format!("Collection {} not found", collection_id))
                })?
                .clone();

            let old_version_file_path = collection.version_file_path.clone().unwrap_or_default();

            // 2. Fetch the existing version file from storage
            let version_file_manager =
                VersionFileManager::new(self.local_region_object_storage.clone());
            let mut version_file = version_file_manager.fetch(&collection).await.map_err(|e| {
                tracing::error!(
                    collection_id = %collection_id,
                    error = ?e,
                    "Failed to fetch version file for {}",
                    operation.name()
                );
                SysDbError::Internal(format!("Failed to fetch version file: {}", e))
            })?;

            // 3. Apply the operation to the version file
            let version_history = version_file
                .version_history
                .as_mut()
                .ok_or_else(|| SysDbError::Internal("Version history not found".to_string()))?;

            match operation {
                VersionFileOperation::MarkForDeletion => {
                    let mut versions_found = 0;
                    for version_info in version_history.versions.iter_mut() {
                        if target_versions.contains(&version_info.version) {
                            version_info.marked_for_deletion = true;
                            versions_found += 1;
                        }
                    }
                    if versions_found != target_versions.len() {
                        return Err(SysDbError::Internal(format!(
                            "Requested versions not found in version file: found {}, expected {}",
                            versions_found,
                            target_versions.len()
                        )));
                    }
                }
                VersionFileOperation::DeleteVersions => {
                    version_history
                        .versions
                        .retain(|v| !target_versions.contains(&v.version));
                }
            }

            // Calculate oldest version timestamp and active version count
            let oldest_version_ts = version_history.versions.first().map(|v| v.created_at_secs);
            let num_active_versions = version_history
                .versions
                .iter()
                .filter(|v| !v.marked_for_deletion)
                .count() as i32;

            tracing::debug!(
                collection_id = %collection_id,
                oldest_version_ts = ?oldest_version_ts,
                num_active_versions = num_active_versions,
                total_versions = version_history.versions.len(),
                "Calculated version metadata for update"
            );

            let new_version_file_path = version_file_manager.generate_file_path(
                &collection,
                collection.version as i64,
                operation.version_file_type(),
            );

            // 4. Upload the new version file
            version_file_manager
                .upload(
                    &new_version_file_path,
                    &version_file,
                    &collection,
                    collection.version as i64,
                )
                .await
                .map_err(|e| {
                    tracing::error!(
                        collection_id = %collection_id,
                        error = ?e,
                        "Failed to upload {} version file",
                        operation.name()
                    );
                    SysDbError::Internal(format!(
                        "Failed to upload {} version file: {}",
                        operation.name(),
                        e
                    ))
                })?;

            // 5. CAS update version_file_name and related fields in the DB
            tracing::debug!(
                collection_id = %collection_id,
                old_version_file_path = %old_version_file_path,
                new_version_file_path = %new_version_file_path,
                "Calling update_version_related_fields"
            );

            let updated = backend
                .update_version_related_fields(
                    &collection_id.0.to_string(),
                    &old_version_file_path,
                    &new_version_file_path,
                    oldest_version_ts,
                    num_active_versions,
                )
                .await
                .map_err(|e| {
                    tracing::error!(
                        collection_id = %collection_id,
                        error = ?e,
                        "Failed to update version related fields"
                    );
                    e
                })?;

            tracing::debug!(
                collection_id = %collection_id,
                updated = updated,
                "Database update completed"
            );

            if !updated {
                tracing::info!(
                    collection_id = %collection_id,
                    "CAS failed for {}, retrying",
                    operation.name()
                );
                // TODO: delete the orphaned version file from storage
                return Err(SysDbError::CollectionEntryIsStale);
            }

            tracing::info!(
                collection_id = %collection_id,
                versions = ?version_list.versions,
                new_version_file_path = %new_version_file_path,
                "Successfully completed {}",
                operation.name()
            );

            Ok(())
        })
        .retry(backoff)
        .when(|e: &SysDbError| matches!(e, SysDbError::CollectionEntryIsStale))
        .await
    }
}

#[derive(Debug, Clone, Copy)]
enum VersionFileOperation {
    MarkForDeletion,
    DeleteVersions,
}

impl VersionFileOperation {
    fn name(&self) -> &'static str {
        match self {
            VersionFileOperation::MarkForDeletion => "mark_version_for_deletion",
            VersionFileOperation::DeleteVersions => "delete_collection_version",
        }
    }

    fn version_file_type(&self) -> VersionFileType {
        match self {
            VersionFileOperation::MarkForDeletion => VersionFileType::GarbageCollectionMark,
            VersionFileOperation::DeleteVersions => VersionFileType::GarbageCollectionDelete,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{Assignable, Backend, BackendFactory};
    use crate::spanner::SpannerBackend;
    use crate::types::{
        CollectionFilter, CreateCollectionRequest, CreateDatabaseRequest, CreateTenantRequest,
        GetCollectionsRequest,
    };
    use chroma_config::Configurable;
    use chroma_storage::config::{LocalStorageConfig, StorageConfig};
    use chroma_storage::Storage;
    use chroma_types::chroma_proto::{
        sys_db_server::SysDb, FilePaths, FlushCollectionCompactionRequest,
        FlushSegmentCompactionInfo,
    };
    use chroma_types::{
        CollectionUuid, Database, DatabaseName, Schema, Segment, SegmentScope, SegmentType,
        SegmentUuid, TopologyName,
    };
    use std::collections::HashMap;
    use tempfile::TempDir;
    use tonic::{Request, Response};
    use uuid::Uuid;

    async fn setup_test_backend() -> Option<SpannerBackend> {
        crate::spanner::tests::setup_test_backend().await
    }

    async fn setup_test_backend_with_region(region: &str) -> Option<SpannerBackend> {
        crate::spanner::tests::setup_test_backend_with_region(region).await
    }

    async fn setup_tenant_and_database(backend: &SpannerBackend) -> (String, DatabaseName) {
        let tenant_id = Uuid::new_v4().to_string();
        let create_tenant_req = CreateTenantRequest {
            id: tenant_id.clone(),
        };
        let _: crate::types::CreateTenantResponse = backend
            .create_tenant(create_tenant_req)
            .await
            .expect("Failed to create tenant");

        let database_name = DatabaseName::new("test_database").expect("Invalid database name");
        let create_db_req = CreateDatabaseRequest {
            id: Uuid::new_v4(),
            name: database_name.clone(),
            tenant_id: tenant_id.clone(),
        };
        let _: crate::types::CreateDatabaseResponse = backend
            .create_database(create_db_req)
            .await
            .expect("Failed to create database");

        (tenant_id, database_name)
    }

    fn create_test_segment_compaction_info() -> Vec<FlushSegmentCompactionInfo> {
        let mut file_paths = HashMap::new();
        file_paths.insert(
            "data".to_string(),
            FilePaths {
                paths: vec!["new/path1.bin".to_string()],
            },
        );

        // Create segment info for segments that will actually exist
        // Create all three required segments (metadata, record, vector)
        let metadata_uuid = Uuid::new_v4();
        let record_uuid = Uuid::new_v4();
        let vector_uuid = Uuid::new_v4();

        vec![
            FlushSegmentCompactionInfo {
                segment_id: metadata_uuid.to_string(),
                file_paths: file_paths.clone(),
            },
            FlushSegmentCompactionInfo {
                segment_id: record_uuid.to_string(),
                file_paths: file_paths.clone(),
            },
            FlushSegmentCompactionInfo {
                segment_id: vector_uuid.to_string(),
                file_paths,
            },
        ]
    }

    async fn setup_test_service(backend: SpannerBackend) -> (SysdbService, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage_config = StorageConfig::Local(LocalStorageConfig {
            root: temp_dir.path().to_string_lossy().to_string(),
        });
        let registry = chroma_config::registry::Registry::new();
        let storage: Storage = Storage::try_from_config(&storage_config, &registry)
            .await
            .expect("Failed to create local storage");

        let mut topology_to_backend = std::collections::HashMap::new();
        topology_to_backend.insert(TopologyName::new("us").unwrap(), backend);
        let backends = BackendFactory::new(topology_to_backend);
        let service = SysdbService::new(50051, storage, backends);

        (service, temp_dir)
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_flush_collection_compaction() {
        let Some(backend): Option<SpannerBackend> = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (service, _temp_dir) = setup_test_service(backend.clone()).await;

        // Create test data
        let (tenant_id, database_name) = setup_tenant_and_database(&backend).await;
        let collection_id = CollectionUuid(Uuid::new_v4());

        // Create collection with segments
        let metadata_segment_id = SegmentUuid(Uuid::new_v4());
        let record_segment_id = SegmentUuid(Uuid::new_v4());
        let vector_segment_id = SegmentUuid(Uuid::new_v4());

        // Create segment_compaction_info using the same IDs
        let segment_compaction_info = vec![
            FlushSegmentCompactionInfo {
                segment_id: metadata_segment_id.0.to_string(),
                file_paths: {
                    let mut file_paths = HashMap::new();
                    file_paths.insert(
                        "data".to_string(),
                        FilePaths {
                            paths: vec!["new/path1.bin".to_string()],
                        },
                    );
                    file_paths
                },
            },
            FlushSegmentCompactionInfo {
                segment_id: record_segment_id.0.to_string(),
                file_paths: {
                    let mut file_paths = HashMap::new();
                    file_paths.insert(
                        "data".to_string(),
                        FilePaths {
                            paths: vec!["new/path2.bin".to_string()],
                        },
                    );
                    file_paths
                },
            },
            FlushSegmentCompactionInfo {
                segment_id: vector_segment_id.0.to_string(),
                file_paths: {
                    let mut file_paths = HashMap::new();
                    file_paths.insert(
                        "data".to_string(),
                        FilePaths {
                            paths: vec!["new/path3.bin".to_string()],
                        },
                    );
                    file_paths
                },
            },
        ];

        let create_collection_req = CreateCollectionRequest {
            id: collection_id,
            tenant_id: tenant_id.clone(),
            database_name: database_name.clone(),
            name: "test_collection".to_string(),
            dimension: Some(128),
            metadata: Some(HashMap::new()),
            segments: vec![
                Segment {
                    id: metadata_segment_id,
                    r#type: SegmentType::BlockfileMetadata,
                    scope: SegmentScope::METADATA,
                    collection: collection_id,
                    file_path: HashMap::new(),
                    metadata: None,
                },
                Segment {
                    id: record_segment_id,
                    r#type: SegmentType::BlockfileRecord,
                    scope: SegmentScope::RECORD,
                    collection: collection_id,
                    file_path: HashMap::new(),
                    metadata: None,
                },
                Segment {
                    id: vector_segment_id,
                    r#type: SegmentType::HnswDistributed,
                    scope: SegmentScope::VECTOR,
                    collection: collection_id,
                    file_path: HashMap::new(),
                    metadata: None,
                },
            ],
            get_or_create: false,
            index_schema: Schema::default(),
        };
        let create_resp: crate::types::CreateCollectionResponse = backend
            .create_collection(create_collection_req)
            .await
            .expect("Failed to create collection");

        let collection_id = create_resp.collection.collection_id;

        // Get the current collection version
        let get_collection_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(vec![collection_id]),
        };
        let get_resp: crate::types::GetCollectionsResponse = backend
            .get_collections(get_collection_req)
            .await
            .expect("Failed to get collection");

        let current_version = get_resp.collections.first().unwrap().version;

        // Prepare flush compaction request
        let proto_req = FlushCollectionCompactionRequest {
            tenant_id: tenant_id.clone(),
            collection_id: collection_id.0.to_string(),
            segment_compaction_info,
            total_records_post_compaction: 500,
            size_bytes_post_compaction: 512000,
            schema_str: Some("{\"defaults\": {\"test\": \"schema\"}, \"keys\": {}}".to_string()),
            collection_version: current_version,
            log_position: 0,
            database_name: Some(database_name.clone().into_string()),
        };

        // Execute the flush
        let request = Request::new(proto_req);
        let response: Result<
            Response<chroma_types::chroma_proto::FlushCollectionCompactionResponse>,
            tonic::Status,
        > = service.flush_collection_compaction(request).await;

        // Verify success
        assert!(
            response.is_ok(),
            "Failed to flush collection compaction: {:?}",
            response.err()
        );

        let proto_resp = response.unwrap().into_inner();
        assert_eq!(proto_resp.collection_id, collection_id.0.to_string());
        assert!(proto_resp.collection_version > 0);

        // Verify collection version was incremented
        let get_collection_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(vec![collection_id]),
        };
        let get_resp: crate::types::GetCollectionsResponse = backend
            .get_collections(get_collection_req)
            .await
            .expect("Failed to get collection");

        let collection = get_resp.collections.first().expect("Collection not found");
        assert_eq!(collection.version, 1); // Should be incremented from 0 to 1

        // Verify version file path was set (this was the main issue before the fix)
        let version_file_path = collection
            .version_file_path
            .as_ref()
            .expect("Collection should have a version file path after flush");

        // Verify the version file path format is correct
        assert!(version_file_path.contains("versionfiles/"));
        assert!(version_file_path.contains(&format!("/{:06}", collection.version)));
        assert!(version_file_path.contains("_flush"));

        // Read and validate the version file contents
        let version_file_manager =
            VersionFileManager::new(service.local_region_object_storage.clone());
        let version_file = version_file_manager
            .fetch(collection)
            .await
            .expect("Failed to fetch version file after flush");

        // Verify version file structure
        assert!(
            version_file.collection_info_immutable.is_some(),
            "Collection immutable info should be set"
        );
        let collection_info_immutable = version_file.collection_info_immutable.as_ref().unwrap();
        assert_eq!(
            collection_info_immutable.collection_id,
            collection_id.0.to_string()
        );
        assert_eq!(collection_info_immutable.collection_name, "test_collection");
        assert_eq!(collection_info_immutable.tenant_id, tenant_id);
        assert_eq!(
            collection_info_immutable.database_id,
            database_name.as_ref().to_string()
        );

        // Verify version history
        assert!(
            version_file.version_history.is_some(),
            "Version history should be set"
        );
        let version_history = version_file.version_history.as_ref().unwrap();
        assert_eq!(
            version_history.versions.len(),
            1,
            "Should have exactly one version after flush"
        );

        // Verify the version details
        let version_info = &version_history.versions[0];
        assert_eq!(version_info.version, 1, "Version should be 1");
        assert!(
            !version_info.marked_for_deletion,
            "Version should not be marked for deletion"
        );
        assert_eq!(
            version_info.version_change_reason,
            VersionChangeReason::DataCompaction as i32
        );

        // Verify segment info is preserved
        assert!(
            version_info.segment_info.is_some(),
            "Segment info should be set"
        );
        let segment_info = version_info.segment_info.as_ref().unwrap();
        assert_eq!(
            segment_info.segment_compaction_info.len(),
            3,
            "Should have 3 segments"
        );

        // Verify collection mutable info
        assert!(
            version_info.collection_info_mutable.is_some(),
            "Collection mutable info should be set"
        );
        let mutable_info = version_info.collection_info_mutable.as_ref().unwrap();
        assert_eq!(
            mutable_info.current_collection_version,
            current_version as i64
        );
        assert_eq!(mutable_info.current_log_position, 0);
        assert!(
            mutable_info.updated_at_secs > 0,
            "Updated timestamp should be set"
        );
        assert_eq!(
            mutable_info.last_compaction_time_secs, 0,
            "Last compaction time should be default value"
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_flush_collection_compaction_invalid_collection_id() {
        let Some(backend): Option<SpannerBackend> = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (service, _temp_dir) = setup_test_service(backend).await;

        // Test with invalid UUID
        let invalid_collection_id = "invalid-uuid";
        let segment_compaction_info = create_test_segment_compaction_info();
        let proto_req = FlushCollectionCompactionRequest {
            tenant_id: "test_tenant".to_string(),
            collection_id: invalid_collection_id.to_string(),
            segment_compaction_info,
            total_records_post_compaction: 100,
            size_bytes_post_compaction: 1024,
            schema_str: None,
            collection_version: 0,
            log_position: 0,
            database_name: Some("test_database".to_string()),
        };

        let request = Request::new(proto_req);
        let response: Result<
            Response<chroma_types::chroma_proto::FlushCollectionCompactionResponse>,
            tonic::Status,
        > = service.flush_collection_compaction(request).await;

        // Should fail with InvalidArgument
        assert!(response.is_err());
        let status = response.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Invalid UUID"));
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_flush_collection_compaction_nonexistent_collection() {
        let Some(backend): Option<SpannerBackend> = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (service, _temp_dir) = setup_test_service(backend).await;

        // Test with non-existent collection (valid UUID but doesn't exist)
        let nonexistent_collection_id = CollectionUuid(Uuid::new_v4());
        let segment_compaction_info = create_test_segment_compaction_info();
        let proto_req = FlushCollectionCompactionRequest {
            tenant_id: "test_tenant".to_string(),
            collection_id: nonexistent_collection_id.0.to_string(),
            segment_compaction_info,
            total_records_post_compaction: 100,
            size_bytes_post_compaction: 1024,
            schema_str: None,
            collection_version: 0,
            log_position: 0,
            database_name: Some("test_database".to_string()),
        };

        let request = Request::new(proto_req);
        let response: Result<
            Response<chroma_types::chroma_proto::FlushCollectionCompactionResponse>,
            tonic::Status,
        > = service.flush_collection_compaction(request).await;

        // Should fail with NotFound
        assert!(response.is_err());
        let status = response.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert!(status.message().contains("not found"));
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_flush_collection_compaction_cross_region_version_consistency(
    ) {
        // Create three separate backends to simulate independent regions
        let Some(backend_us): Option<SpannerBackend> = setup_test_backend_with_region("us").await
        else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };
        let Some(backend_eu): Option<SpannerBackend> =
            setup_test_backend_with_region("europe").await
        else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };
        let Some(backend_ap): Option<SpannerBackend> = setup_test_backend_with_region("asia").await
        else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // Set up test infrastructure for each region
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage_config = StorageConfig::Local(LocalStorageConfig {
            root: temp_dir.path().to_string_lossy().to_string(),
        });
        let registry = chroma_config::registry::Registry::new();
        let storage: Storage = Storage::try_from_config(&storage_config, &registry)
            .await
            .expect("Failed to create local storage");

        // Create separate BackendFactories for each region to simulate independent regions
        let mut topology_to_backend_us = std::collections::HashMap::new();
        topology_to_backend_us.insert(TopologyName::new("us").unwrap(), backend_us.clone());
        let backends_us = BackendFactory::new(topology_to_backend_us);
        let service_us = SysdbService::new(50051, storage.clone(), backends_us);

        let mut topology_to_backend_eu = std::collections::HashMap::new();
        topology_to_backend_eu.insert(TopologyName::new("europe").unwrap(), backend_eu.clone());
        let backends_eu = BackendFactory::new(topology_to_backend_eu);
        let service_eu = SysdbService::new(50052, storage.clone(), backends_eu);

        let mut topology_to_backend_ap = std::collections::HashMap::new();
        topology_to_backend_ap.insert(TopologyName::new("asia").unwrap(), backend_ap.clone());
        let backends_ap = BackendFactory::new(topology_to_backend_ap);
        let _service_ap = SysdbService::new(50053, storage.clone(), backends_ap);

        // Create test data in US region
        let (tenant_id, database_name) = setup_tenant_and_database(&backend_us).await;
        let collection_id = CollectionUuid(Uuid::new_v4());

        // Create collection with segments in US region
        let segment_compaction_info = create_test_segment_compaction_info();
        let metadata_uuid = Uuid::parse_str(&segment_compaction_info[0].segment_id).unwrap();
        let record_uuid = Uuid::parse_str(&segment_compaction_info[1].segment_id).unwrap();
        let vector_uuid = Uuid::parse_str(&segment_compaction_info[2].segment_id).unwrap();

        let create_collection_req = CreateCollectionRequest {
            id: collection_id,
            tenant_id: tenant_id.clone(),
            database_name: database_name.clone(),
            name: "test_collection".to_string(),
            dimension: Some(128),
            metadata: Some(HashMap::new()),
            segments: vec![
                Segment {
                    id: SegmentUuid(metadata_uuid),
                    r#type: SegmentType::BlockfileMetadata,
                    scope: SegmentScope::METADATA,
                    collection: collection_id,
                    file_path: HashMap::new(),
                    metadata: None,
                },
                Segment {
                    id: SegmentUuid(record_uuid),
                    r#type: SegmentType::BlockfileRecord,
                    scope: SegmentScope::RECORD,
                    collection: collection_id,
                    file_path: HashMap::new(),
                    metadata: None,
                },
                Segment {
                    id: SegmentUuid(vector_uuid),
                    r#type: SegmentType::HnswDistributed,
                    scope: SegmentScope::VECTOR,
                    collection: collection_id,
                    file_path: HashMap::new(),
                    metadata: None,
                },
            ],
            get_or_create: false,
            index_schema: Schema::default(),
        };
        let create_resp: crate::types::CreateCollectionResponse = backend_us
            .create_collection(create_collection_req)
            .await
            .expect("Failed to create collection");

        let collection_id = create_resp.collection.collection_id;

        // Get the current collection version from US region
        let get_collection_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(vec![collection_id]),
        };
        let get_resp_initial_us: crate::types::GetCollectionsResponse = backend_us
            .get_collections(get_collection_req.clone())
            .await
            .expect("Failed to get collection from US region");

        let initial_version_us = get_resp_initial_us.collections.first().unwrap().version;
        assert_eq!(
            initial_version_us, 0,
            "Initial collection version should be 0 in US region"
        );

        // Check that EU and AP regions see the same collection (shared database)
        let get_resp_eu: crate::types::GetCollectionsResponse = backend_eu
            .get_collections(get_collection_req.clone())
            .await
            .expect("Failed to get collection from EU region");

        let get_resp_ap: crate::types::GetCollectionsResponse = backend_ap
            .get_collections(get_collection_req.clone())
            .await
            .expect("Failed to get collection from AP region");

        // EU and AP should see the collection since they share the same database
        assert!(
            !get_resp_eu.collections.is_empty(),
            "EU region should see the collection (shared database)"
        );
        assert!(
            !get_resp_ap.collections.is_empty(),
            "AP region should see the collection (shared database)"
        );

        // All regions should see the same initial version
        let eu_version_initial = get_resp_eu.collections.first().unwrap().version;
        let ap_version_initial = get_resp_ap.collections.first().unwrap().version;
        assert_eq!(eu_version_initial, 0, "EU region should see version 0");
        assert_eq!(ap_version_initial, 0, "AP region should see version 0");

        // Prepare flush compaction request for US region
        let proto_req_us = FlushCollectionCompactionRequest {
            tenant_id: tenant_id.clone(),
            collection_id: collection_id.0.to_string(),
            segment_compaction_info: segment_compaction_info.clone(),
            total_records_post_compaction: 500,
            size_bytes_post_compaction: 512000,
            schema_str: Some("{\"defaults\": {\"test\": \"schema\"}, \"keys\": {}}".to_string()),
            collection_version: initial_version_us,
            log_position: 0,
            database_name: Some("test_database".to_string()),
        };

        // Execute the flush in US region
        let request_us = Request::new(proto_req_us);
        let response_us: Result<
            Response<chroma_types::chroma_proto::FlushCollectionCompactionResponse>,
            tonic::Status,
        > = service_us.flush_collection_compaction(request_us).await;

        // Verify success
        assert!(
            response_us.is_ok(),
            "Failed to flush collection compaction in US region: {:?}",
            response_us.err()
        );

        let proto_resp_us = response_us.unwrap().into_inner();
        assert_eq!(proto_resp_us.collection_id, collection_id.0.to_string());
        assert!(proto_resp_us.collection_version > 0);

        // Verify US region now has version 1 and version file path
        let get_resp_after_flush_us: crate::types::GetCollectionsResponse = backend_us
            .get_collections(get_collection_req.clone())
            .await
            .expect("Failed to get collection from US region after flush");

        let us_version_after_flush = get_resp_after_flush_us.collections.first().unwrap().version;
        let us_version_file_path_after = get_resp_after_flush_us
            .collections
            .first()
            .unwrap()
            .version_file_path
            .as_ref();

        assert_eq!(
            us_version_after_flush, 1,
            "US region should see version 1 after flush"
        );
        assert!(
            us_version_file_path_after.is_some(),
            "US region should have version file path after flush"
        );

        // EU and AP regions should still see version 0 since they are isolated from US region
        let get_resp_eu_after: crate::types::GetCollectionsResponse = backend_eu
            .get_collections(get_collection_req.clone())
            .await
            .expect("Failed to get collection from EU region after US flush");

        let get_resp_ap_after: crate::types::GetCollectionsResponse = backend_ap
            .get_collections(get_collection_req.clone())
            .await
            .expect("Failed to get collection from AP region after US flush");

        let eu_version_after_flush = get_resp_eu_after.collections.first().unwrap().version;
        let ap_version_after_flush = get_resp_ap_after.collections.first().unwrap().version;

        assert_eq!(
            eu_version_after_flush, 0,
            "EU region should still see version 0 (isolated from US flush)"
        );
        assert_eq!(
            ap_version_after_flush, 0,
            "AP region should still see version 0 (isolated from US flush)"
        );

        // Only US region should have version file path after its flush
        assert!(
            us_version_file_path_after.is_some(),
            "US region should have version file path after flush"
        );
        assert!(
            get_resp_eu_after
                .collections
                .first()
                .unwrap()
                .version_file_path
                .is_none(),
            "EU region should not have version file path (didn't flush)"
        );
        assert!(
            get_resp_ap_after
                .collections
                .first()
                .unwrap()
                .version_file_path
                .is_none(),
            "AP region should not have version file path (didn't flush)"
        );

        // Now flush the collection in EU region (independent operation)
        let proto_req_eu = FlushCollectionCompactionRequest {
            tenant_id: tenant_id.clone(),
            collection_id: collection_id.0.to_string(),
            segment_compaction_info: segment_compaction_info.clone(),
            total_records_post_compaction: 600,
            size_bytes_post_compaction: 614400,
            schema_str: Some("{\"defaults\": {\"test\": \"schema_eu\"}, \"keys\": {}}".to_string()),
            collection_version: eu_version_after_flush, // Version 0 (EU didn't see US flush)
            log_position: 0,
            database_name: Some("test_database".to_string()),
        };

        let request_eu = Request::new(proto_req_eu);
        let response_eu: Result<
            Response<chroma_types::chroma_proto::FlushCollectionCompactionResponse>,
            tonic::Status,
        > = service_eu.flush_collection_compaction(request_eu).await;

        // Verify EU flush success
        assert!(
            response_eu.is_ok(),
            "Failed to flush collection compaction in EU region: {:?}",
            response_eu.err()
        );

        let proto_resp_eu = response_eu.unwrap().into_inner();
        assert_eq!(proto_resp_eu.collection_id, collection_id.0.to_string());
        assert!(proto_resp_eu.collection_version > 0);

        // After EU flush, verify that regions remain isolated:
        // - US should still see version 1 (unchanged by EU flush)
        // - EU should see version 1 (its own flush)
        // - AP should still see version 0 (no flush in AP region)
        let get_resp_after_eu_flush_us: crate::types::GetCollectionsResponse = backend_us
            .get_collections(get_collection_req.clone())
            .await
            .expect("Failed to get collection from US region after EU flush");

        let get_resp_after_eu_flush_eu: crate::types::GetCollectionsResponse = backend_eu
            .get_collections(get_collection_req.clone())
            .await
            .expect("Failed to get collection from EU region after EU flush");

        let get_resp_after_eu_flush_ap: crate::types::GetCollectionsResponse = backend_ap
            .get_collections(get_collection_req.clone())
            .await
            .expect("Failed to get collection from AP region after EU flush");

        let us_version_final = get_resp_after_eu_flush_us
            .collections
            .first()
            .unwrap()
            .version;
        let eu_version_final = get_resp_after_eu_flush_eu
            .collections
            .first()
            .unwrap()
            .version;
        let ap_version_final = get_resp_after_eu_flush_ap
            .collections
            .first()
            .unwrap()
            .version;

        assert_eq!(
            us_version_final, 1,
            "US region should still see version 1 (unchanged by EU flush)"
        );
        assert_eq!(
            eu_version_final, 1,
            "EU region should see version 1 after its own flush"
        );
        assert_eq!(
            ap_version_final, 0,
            "AP region should still see version 0 (no flush in AP region)"
        );

        // Verify version file paths are isolated per region
        let us_version_file_path_final = get_resp_after_eu_flush_us
            .collections
            .first()
            .unwrap()
            .version_file_path
            .as_ref()
            .unwrap();

        let eu_version_file_path_final = get_resp_after_eu_flush_eu
            .collections
            .first()
            .unwrap()
            .version_file_path
            .as_ref()
            .unwrap();

        assert!(
            get_resp_after_eu_flush_ap
                .collections
                .first()
                .unwrap()
                .version_file_path
                .is_none(),
            "AP region should not have version file path (no flush)"
        );

        // US and EU should have the same version number in their paths (shared database)
        // Extract version number from the filename part (e.g., "000001_..._flush")
        let us_filename = us_version_file_path_final.split('/').next_back().unwrap();
        let eu_filename = eu_version_file_path_final.split('/').next_back().unwrap();

        let us_version = us_filename.split('_').next().unwrap();
        let eu_version = eu_filename.split('_').next().unwrap();

        assert_eq!(
            us_version, eu_version,
            "US and EU should have the same version number (shared database)"
        );

        // Verify the version file path formats are correct
        assert!(us_version_file_path_final.contains("versionfiles/"));
        assert!(us_version_file_path_final.contains(&format!("/{:06}", us_version_final)));
        assert!(us_version_file_path_final.contains("_flush"));

        assert!(eu_version_file_path_final.contains("versionfiles/"));
        assert!(eu_version_file_path_final.contains(&format!("/{:06}", eu_version_final)));
        assert!(eu_version_file_path_final.contains("_flush"));
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_flush_collection_compaction_version_stale() {
        let Some(backend): Option<SpannerBackend> = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (service, _temp_dir) = setup_test_service(backend.clone()).await;

        // Create test data
        let (tenant_id, database_name) = setup_tenant_and_database(&backend).await;
        let collection_id = CollectionUuid(Uuid::new_v4());

        // Create collection with segments
        let segment_compaction_info = create_test_segment_compaction_info();
        let metadata_uuid = Uuid::parse_str(&segment_compaction_info[0].segment_id).unwrap();
        let record_uuid = Uuid::parse_str(&segment_compaction_info[1].segment_id).unwrap();
        let vector_uuid = Uuid::parse_str(&segment_compaction_info[2].segment_id).unwrap();

        let create_collection_req = CreateCollectionRequest {
            id: collection_id,
            tenant_id: tenant_id.clone(),
            database_name: database_name.clone(),
            name: "test_collection".to_string(),
            dimension: Some(128),
            metadata: Some(HashMap::new()),
            segments: vec![
                Segment {
                    id: SegmentUuid(metadata_uuid),
                    r#type: SegmentType::BlockfileMetadata,
                    scope: SegmentScope::METADATA,
                    collection: collection_id,
                    file_path: HashMap::new(),
                    metadata: None,
                },
                Segment {
                    id: SegmentUuid(record_uuid),
                    r#type: SegmentType::BlockfileRecord,
                    scope: SegmentScope::RECORD,
                    collection: collection_id,
                    file_path: HashMap::new(),
                    metadata: None,
                },
                Segment {
                    id: SegmentUuid(vector_uuid),
                    r#type: SegmentType::HnswDistributed,
                    scope: SegmentScope::VECTOR,
                    collection: collection_id,
                    file_path: HashMap::new(),
                    metadata: None,
                },
            ],
            get_or_create: false,
            index_schema: Schema::default(),
        };
        let create_resp: crate::types::CreateCollectionResponse = backend
            .create_collection(create_collection_req)
            .await
            .expect("Failed to create collection");

        let collection_id = create_resp.collection.collection_id;

        // Get the current collection version (should be 0)
        let get_collection_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(vec![collection_id]),
        };
        let get_resp: crate::types::GetCollectionsResponse = backend
            .get_collections(get_collection_req.clone())
            .await
            .expect("Failed to get collection");

        let initial_version = get_resp.collections.first().unwrap().version;
        assert_eq!(initial_version, 0, "Initial collection version should be 0");

        // First flush compaction succeeds (version 0 -> 1)
        let proto_req_first = FlushCollectionCompactionRequest {
            tenant_id: tenant_id.clone(),
            collection_id: collection_id.0.to_string(),
            segment_compaction_info: segment_compaction_info.clone(),
            total_records_post_compaction: 500,
            size_bytes_post_compaction: 512000,
            schema_str: Some("{\"defaults\": {\"test\": \"schema\"}, \"keys\": {}}".to_string()),
            collection_version: initial_version,
            log_position: 0,
            database_name: Some(database_name.clone().into_string()),
        };

        let request_first = Request::new(proto_req_first);
        let response_first: Result<
            Response<chroma_types::chroma_proto::FlushCollectionCompactionResponse>,
            tonic::Status,
        > = service.flush_collection_compaction(request_first).await;

        assert!(
            response_first.is_ok(),
            "First flush should succeed: {:?}",
            response_first.err()
        );

        // Verify version was incremented to 1
        let get_resp_after_first: crate::types::GetCollectionsResponse = backend
            .get_collections(get_collection_req.clone())
            .await
            .expect("Failed to get collection after first flush");

        let version_after_first = get_resp_after_first.collections.first().unwrap().version;
        assert_eq!(
            version_after_first, 1,
            "Version should be 1 after first flush"
        );

        // Second flush compaction with stale version (0) should fail with CollectionVersionStale
        let proto_req_stale = FlushCollectionCompactionRequest {
            tenant_id: tenant_id.clone(),
            collection_id: collection_id.0.to_string(),
            segment_compaction_info: segment_compaction_info.clone(),
            total_records_post_compaction: 600,
            size_bytes_post_compaction: 614400,
            schema_str: Some("{\"defaults\": {\"stale\": \"schema\"}, \"keys\": {}}".to_string()),
            collection_version: initial_version, // Stale version 0 (current is 1)
            log_position: 0,
            database_name: Some(database_name.clone().into_string()),
        };

        let request_stale = Request::new(proto_req_stale);
        let response_stale: Result<
            Response<chroma_types::chroma_proto::FlushCollectionCompactionResponse>,
            tonic::Status,
        > = service.flush_collection_compaction(request_stale).await;

        // Should fail with CollectionVersionStale error
        assert!(
            response_stale.is_err(),
            "Flush with stale version should fail"
        );

        let status = response_stale.unwrap_err();
        // CollectionVersionStale maps to Internal error code
        assert_eq!(
            status.code(),
            tonic::Code::Internal,
            "Expected Internal error code for CollectionVersionStale"
        );
        assert!(
            status.message().contains("stale")
                || status.message().contains("version")
                || status.message().contains("Collection version"),
            "Error message should indicate version staleness: {}",
            status.message()
        );

        // Verify collection version is still 1 (stale flush didn't change anything)
        let get_resp_final: crate::types::GetCollectionsResponse = backend
            .get_collections(get_collection_req)
            .await
            .expect("Failed to get collection after stale flush attempt");

        let final_version = get_resp_final.collections.first().unwrap().version;
        assert_eq!(
            final_version, 1,
            "Version should still be 1 after failed stale flush"
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_flush_collection_compaction_empty_segments_preserves_existing(
    ) {
        let Some(backend): Option<SpannerBackend> = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (service, _temp_dir) = setup_test_service(backend.clone()).await;

        // Create test data
        let (tenant_id, database_name) = setup_tenant_and_database(&backend).await;
        let collection_id = CollectionUuid(Uuid::new_v4());

        // Create collection with initial segments
        let initial_segment_compaction_info = create_test_segment_compaction_info();

        let create_collection_req = CreateCollectionRequest {
            id: collection_id,
            tenant_id: tenant_id.clone(),
            database_name: database_name.clone(),
            name: "test_collection".to_string(),
            dimension: Some(128),
            metadata: Some(HashMap::new()),
            segments: vec![
                Segment {
                    id: SegmentUuid(
                        initial_segment_compaction_info[0]
                            .segment_id
                            .parse()
                            .unwrap(),
                    ),
                    r#type: SegmentType::BlockfileMetadata,
                    scope: SegmentScope::METADATA,
                    collection: collection_id,
                    file_path: HashMap::new(),
                    metadata: None,
                },
                Segment {
                    id: SegmentUuid(
                        initial_segment_compaction_info[1]
                            .segment_id
                            .parse()
                            .unwrap(),
                    ),
                    r#type: SegmentType::BlockfileRecord,
                    scope: SegmentScope::RECORD,
                    collection: collection_id,
                    file_path: HashMap::new(),
                    metadata: None,
                },
                Segment {
                    id: SegmentUuid(
                        initial_segment_compaction_info[2]
                            .segment_id
                            .parse()
                            .unwrap(),
                    ),
                    r#type: SegmentType::HnswDistributed,
                    scope: SegmentScope::VECTOR,
                    collection: collection_id,
                    file_path: HashMap::new(),
                    metadata: None,
                },
            ],
            index_schema: Default::default(),
            get_or_create: false,
        };

        let _: crate::types::CreateCollectionResponse = backend
            .create_collection(create_collection_req)
            .await
            .expect("Failed to create collection");

        // First flush with segments to establish version history
        let proto_req_first = FlushCollectionCompactionRequest {
            tenant_id: tenant_id.clone(),
            collection_id: collection_id.0.to_string(),
            segment_compaction_info: vec![
                initial_segment_compaction_info[0].clone(),
                initial_segment_compaction_info[1].clone(),
                initial_segment_compaction_info[2].clone(),
            ],
            log_position: 100,
            collection_version: 0,
            total_records_post_compaction: 1000,
            size_bytes_post_compaction: 5000,
            schema_str: None,
            database_name: Some(database_name.as_ref().to_string()),
        };

        let request_first = Request::new(proto_req_first);
        let response_first: Result<
            Response<chroma_types::chroma_proto::FlushCollectionCompactionResponse>,
            tonic::Status,
        > = service.flush_collection_compaction(request_first).await;

        assert!(
            response_first.is_ok(),
            "First flush should succeed: {:?}",
            response_first.err()
        );

        // Second flush with EMPTY segments - should preserve existing segment info
        let proto_req_empty = FlushCollectionCompactionRequest {
            tenant_id: tenant_id.clone(),
            collection_id: collection_id.0.to_string(),
            segment_compaction_info: vec![], // Empty segments!
            log_position: 200,
            collection_version: 1, // Updated version
            total_records_post_compaction: 1500,
            size_bytes_post_compaction: 6000,
            schema_str: None,
            database_name: Some(database_name.as_ref().to_string()),
        };

        let request_empty = Request::new(proto_req_empty);
        let response_empty: Result<
            Response<chroma_types::chroma_proto::FlushCollectionCompactionResponse>,
            tonic::Status,
        > = service.flush_collection_compaction(request_empty).await;

        assert!(
            response_empty.is_ok(),
            "Empty segments flush should succeed: {:?}",
            response_empty.err()
        );

        // Verify the version file was created and preserves existing segment info
        let version_file_manager =
            VersionFileManager::new(service.local_region_object_storage.clone());

        // Get the updated collection to fetch the correct version file path
        let get_collection_req = GetCollectionsRequest {
            filter: CollectionFilter {
                ids: Some(vec![collection_id]),
                database_name: Some(database_name.clone()),
                name: None,
                tenant_id: None,
                include_soft_deleted: false,
                limit: None,
                offset: None,
                topology_name: None,
            },
        };
        let backend = Backend::Spanner(backend.clone());
        let collection_response = get_collection_req
            .run(backend)
            .await
            .expect("Failed to get collection after flush");
        let updated_collection = collection_response.collections.first().unwrap();

        let version_file = version_file_manager
            .fetch(updated_collection)
            .await
            .expect("Failed to fetch version file");

        // Verify the latest version contains the preserved segment info
        let latest_version = version_file
            .version_history
            .as_ref()
            .unwrap()
            .versions
            .last()
            .unwrap();

        assert_eq!(latest_version.version, 2, "Latest version should be 2");

        let segment_info = latest_version.segment_info.as_ref().unwrap();
        assert_eq!(
            segment_info.segment_compaction_info.len(),
            3,
            "Should preserve the original segment info even with empty flush"
        );

        for (i, segment_compaction_info) in segment_info
            .segment_compaction_info
            .iter()
            .enumerate()
            .take(3)
        {
            assert_eq!(
                segment_compaction_info.segment_id, initial_segment_compaction_info[i].segment_id,
                "Should preserve the original segment ID"
            );
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_flush_collection_compaction_empty_segments_new_collection() {
        let Some(backend): Option<SpannerBackend> = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (service, _temp_dir) = setup_test_service(backend.clone()).await;

        // Create test data
        let (tenant_id, database_name) = setup_tenant_and_database(&backend).await;
        let collection_id = CollectionUuid(Uuid::new_v4());

        // Create collection WITHOUT initial version history
        let create_collection_req = CreateCollectionRequest {
            id: collection_id,
            tenant_id: tenant_id.clone(),
            database_name: database_name.clone(),
            name: "test_collection".to_string(),
            dimension: Some(128),
            metadata: Some(HashMap::new()),
            segments: vec![],
            index_schema: Default::default(),
            get_or_create: false,
        };

        let _: crate::types::CreateCollectionResponse = backend
            .create_collection(create_collection_req)
            .await
            .expect("Failed to create collection");

        // Flush with EMPTY segments on new collection - should handle correctly by not updating segments
        // in new version.
        let proto_req_empty = FlushCollectionCompactionRequest {
            tenant_id: tenant_id.clone(),
            collection_id: collection_id.0.to_string(),
            segment_compaction_info: vec![],
            log_position: 100,
            collection_version: 0,
            total_records_post_compaction: 1000,
            size_bytes_post_compaction: 5000,
            schema_str: None,
            database_name: Some(database_name.as_ref().to_string()),
        };

        let request_empty = Request::new(proto_req_empty);
        let response_empty: Result<
            Response<chroma_types::chroma_proto::FlushCollectionCompactionResponse>,
            tonic::Status,
        > = service.flush_collection_compaction(request_empty).await;

        assert!(
            response_empty.is_ok(),
            "Empty segments flush on new collection should succeed: {:?}",
            response_empty.err()
        );

        // Verify the version file was created with empty segment info (no history to preserve)
        let version_file_manager =
            VersionFileManager::new(service.local_region_object_storage.clone());

        // Get the updated collection to fetch the correct version file path
        let get_collection_req = GetCollectionsRequest {
            filter: CollectionFilter {
                ids: Some(vec![collection_id]),
                database_name: Some(database_name.clone()),
                name: None,
                tenant_id: None,
                include_soft_deleted: false,
                limit: None,
                offset: None,
                topology_name: None,
            },
        };
        let backend = Backend::Spanner(backend.clone());
        let collection_response = get_collection_req
            .run(backend)
            .await
            .expect("Failed to get collection after flush");
        let updated_collection = collection_response.collections.first().unwrap();

        let version_file = version_file_manager
            .fetch(updated_collection)
            .await
            .expect("Failed to fetch version file");

        // Verify the latest version contains empty segment info
        let latest_version = version_file
            .version_history
            .as_ref()
            .unwrap()
            .versions
            .last()
            .unwrap();

        assert_eq!(latest_version.version, 1, "Latest version should be 1");

        let segment_info = latest_version.segment_info.as_ref().unwrap();
        assert_eq!(
            segment_info.segment_compaction_info.len(),
            0,
            "Should have empty segment info for new collection with empty flush"
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_topology_name_routing_priority() {
        // Create a mock backend for testing
        let Some(mock_backend_us) = setup_test_backend_with_region("us").await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let Some(mock_backend_eu) = setup_test_backend_with_region("eu").await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // Create BackendFactory with topology mapping
        let mut topology_to_backend = HashMap::new();
        topology_to_backend.insert(
            TopologyName::new("us-east").unwrap(),
            mock_backend_us.clone(),
        );
        topology_to_backend.insert(
            TopologyName::new("eu-west").unwrap(),
            mock_backend_eu.clone(),
        );
        let factory = BackendFactory::new(topology_to_backend);

        // Test 1: Explicit topology_name takes priority over database-derived topology
        // This doesn't make sense as a use case but just testing the semantics.
        let request_with_explicit_topology = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .database_name(DatabaseName::new("eu-west+mydb").unwrap())
                .topology_name("us-east"), // Explicit topology should win
        };
        let backend = request_with_explicit_topology.assign(&factory);
        // Should route to us-east (explicit topology), not eu-west (from database)
        let Backend::Spanner(spanner_backend) = backend;
        assert_eq!(
            spanner_backend.local_region(),
            mock_backend_us.local_region()
        );

        // Test 2: Database with topology prefix (no explicit topology)
        let request_with_database_topology = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .database_name(DatabaseName::new("eu-west+mydb").unwrap()),
            // No explicit topology_name
        };

        let backend = request_with_database_topology.assign(&factory);
        // Should route based on database-derived topology
        let Backend::Spanner(spanner_backend) = backend;
        assert_eq!(
            spanner_backend.local_region(),
            mock_backend_eu.local_region()
        );

        // Test 3: Database without topology prefix (no explicit topology)
        let request_with_plain_database = GetCollectionsRequest {
            filter: CollectionFilter::default().database_name(DatabaseName::new("mydb").unwrap()),
            // No topology_name and no topology in database
        };

        let backend = request_with_plain_database.assign(&factory);
        // Should route to default Spanner backend
        assert!(
            matches!(backend, Backend::Spanner(_)),
            "Should route to default Spanner backend"
        );

        // Test 4: No database or topology (fallback to default)
        let request_no_filter = GetCollectionsRequest {
            filter: CollectionFilter::default(),
        };

        let backend = request_no_filter.assign(&factory);
        // Should route to default Spanner backend
        assert!(
            matches!(backend, Backend::Spanner(_)),
            "Should route to default Spanner backend"
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_mcmr_list_databases() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // Setup test data - create multiple databases
        let (tenant_id, database_name1) = setup_tenant_and_database(&backend).await;
        let database_name2 = DatabaseName::new("test_database_2").expect("Invalid database name");
        let create_db_req2 = CreateDatabaseRequest {
            id: Uuid::new_v4(),
            name: database_name2.clone(),
            tenant_id: tenant_id.clone(),
        };
        let _: crate::types::CreateDatabaseResponse = backend
            .create_database(create_db_req2)
            .await
            .expect("Failed to create second database");

        // Test the internal ListDatabasesRequest
        let request = internal::ListDatabasesRequest {
            tenant_id: tenant_id.clone(),
        };

        // Test the Runnable implementation
        let backends = vec![Backend::Spanner(backend.clone())];
        let result = request.run(backends).await;

        assert!(
            result.is_ok(),
            "Failed to list databases: {:?}",
            result.err()
        );

        let databases = result.unwrap();
        assert!(databases.len() == 2, "Should return exactly two databases");

        // Verify both databases we created are in the results
        let db_names: Vec<String> = databases.iter().map(|db| db.name.clone()).collect();
        assert!(
            db_names.contains(&database_name1.as_ref().to_string()),
            "Should contain the first test database: {}",
            database_name1.as_ref()
        );
        assert!(
            db_names.contains(&database_name2.as_ref().to_string()),
            "Should contain the second test database: {}",
            database_name2.as_ref()
        );

        // Verify all databases belong to the correct tenant
        for db in &databases {
            assert_eq!(
                db.tenant, tenant_id,
                "Database should belong to test tenant"
            );
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_mcmr_list_databases_empty_tenant() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // Use a non-existent tenant
        let tenant_id = Uuid::new_v4().to_string();
        let request = internal::ListDatabasesRequest { tenant_id };

        let backends = vec![Backend::Spanner(backend)];
        let result = request.run(backends).await;

        assert!(result.is_ok(), "Should succeed even for empty tenant");

        let databases = result.unwrap();
        assert!(
            databases.is_empty(),
            "Should return empty list for non-existent tenant"
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_mcmr_list_databases_with_factory() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // Setup test data - create multiple databases
        let (tenant_id, database_name1) = setup_tenant_and_database(&backend).await;
        let database_name2 = DatabaseName::new("test_database_2").expect("Invalid database name");
        let create_db_req2 = CreateDatabaseRequest {
            id: Uuid::new_v4(),
            name: database_name2.clone(),
            tenant_id: tenant_id.clone(),
        };
        let _: crate::types::CreateDatabaseResponse = backend
            .create_database(create_db_req2)
            .await
            .expect("Failed to create second database");

        // Create a backend factory
        let mut topology_to_backend = std::collections::HashMap::new();
        topology_to_backend.insert(TopologyName::new("us").unwrap(), backend.clone());
        let factory = BackendFactory::new(topology_to_backend);

        // Test the Assignable implementation
        let request = internal::ListDatabasesRequest {
            tenant_id: tenant_id.clone(),
        };
        let backends = request.assign(&factory);

        assert!(!backends.is_empty(), "Should assign at least one backend");

        // Test the Runnable implementation with factory-assigned backends
        let result = request.run(backends).await;

        assert!(
            result.is_ok(),
            "Failed to list databases: {:?}",
            result.err()
        );

        let databases = result.unwrap();
        assert!(databases.len() >= 2, "Should return at least two databases");

        // Verify both databases we created are in the results
        let db_names: Vec<String> = databases.iter().map(|db| db.name.clone()).collect();
        assert!(
            db_names.contains(&database_name1.as_ref().to_string()),
            "Should contain the first test database: {}",
            database_name1.as_ref()
        );
        assert!(
            db_names.contains(&database_name2.as_ref().to_string()),
            "Should contain the second test database: {}",
            database_name2.as_ref()
        );

        // Verify all databases belong to the correct tenant
        for db in &databases {
            assert_eq!(
                db.tenant, tenant_id,
                "Database should belong to test tenant"
            );
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_mcmr_list_databases_single_and_mcmr() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // Setup test data - create single-region databases
        let (tenant_id, single_region_db1) = setup_tenant_and_database(&backend).await;
        let single_region_db2 =
            DatabaseName::new("single_region_db_2").expect("Invalid database name");
        let create_sr_req2 = CreateDatabaseRequest {
            id: Uuid::new_v4(),
            name: single_region_db2.clone(),
            tenant_id: tenant_id.clone(),
        };
        let _: crate::types::CreateDatabaseResponse = backend
            .create_database(create_sr_req2)
            .await
            .expect("Failed to create second single-region database");

        // Create MCMR databases (simulate by creating databases with topology prefixes)
        let mcmr_db1 = DatabaseName::new("us+mcmr_db_1").expect("Invalid database name");
        let create_mcmr_req1 = CreateDatabaseRequest {
            id: Uuid::new_v4(),
            name: mcmr_db1.clone(),
            tenant_id: tenant_id.clone(),
        };
        let _: crate::types::CreateDatabaseResponse = backend
            .create_database(create_mcmr_req1)
            .await
            .expect("Failed to create first MCMR database");

        let mcmr_db2 = DatabaseName::new("eu+mcmr_db_2").expect("Invalid database name");
        let create_mcmr_req2 = CreateDatabaseRequest {
            id: Uuid::new_v4(),
            name: mcmr_db2.clone(),
            tenant_id: tenant_id.clone(),
        };
        let _: crate::types::CreateDatabaseResponse = backend
            .create_database(create_mcmr_req2)
            .await
            .expect("Failed to create second MCMR database");

        // Test the internal ListDatabasesRequest
        let request = internal::ListDatabasesRequest {
            tenant_id: tenant_id.clone(),
        };

        // Test the Runnable implementation with single backend (simulates MCMR scenario)
        let backends = vec![Backend::Spanner(backend.clone())];
        let result = request.run(backends).await;

        assert!(
            result.is_ok(),
            "Failed to list databases with single and MCMR: {:?}",
            result.err()
        );

        let databases = result.unwrap();
        assert!(
            databases.len() >= 4,
            "Should return at least four databases"
        );

        // Verify all databases we created are in the results
        let db_names: Vec<String> = databases.iter().map(|db| db.name.clone()).collect();
        assert!(
            db_names.contains(&single_region_db1.as_ref().to_string()),
            "Should contain the first single-region database: {}",
            single_region_db1.as_ref()
        );
        assert!(
            db_names.contains(&single_region_db2.as_ref().to_string()),
            "Should contain the second single-region database: {}",
            single_region_db2.as_ref()
        );
        assert!(
            db_names.contains(&mcmr_db1.as_ref().to_string()),
            "Should contain the first MCMR database: {}",
            mcmr_db1.as_ref()
        );
        assert!(
            db_names.contains(&mcmr_db2.as_ref().to_string()),
            "Should contain the second MCMR database: {}",
            mcmr_db2.as_ref()
        );

        // Verify all databases belong to the correct tenant
        for db in &databases {
            assert_eq!(
                db.tenant, tenant_id,
                "Database should belong to test tenant"
            );
        }

        // Test stable sorting by topology - single-region databases first, then MCMR sorted by topology
        let single_region_dbs: Vec<&Database> = databases
            .iter()
            .filter(|db| !db.name.contains('+'))
            .collect();
        let mcmr_dbs: Vec<&Database> = databases
            .iter()
            .filter(|db| db.name.contains('+'))
            .collect();

        // Verify single-region databases come first
        if !single_region_dbs.is_empty() && !mcmr_dbs.is_empty() {
            let first_mcmr_index = databases
                .iter()
                .position(|db| db.name.contains('+'))
                .unwrap();

            // All databases before the first MCMR database should be single-region
            for db in &databases[..first_mcmr_index] {
                assert!(
                    !db.name.contains('+'),
                    "Database before first MCMR should be single-region, got: {}",
                    db.name
                );
            }

            // All databases after the first MCMR database should be MCMR
            for db in &databases[first_mcmr_index..] {
                assert!(
                    db.name.contains('+'),
                    "Database after first MCMR should be MCMR, got: {}",
                    db.name
                );
            }
        }

        // Verify MCMR databases are sorted by topology (eu before us)
        assert!(
            mcmr_dbs.len() == 2,
            "Should return exactly two MCMR databases"
        );
        assert!(
            mcmr_dbs[0].name.starts_with("eu+"),
            "First MCMR database should be from 'eu' topology, got: {}",
            mcmr_dbs[0].name
        );
        assert!(
            mcmr_dbs[1].name.starts_with("us+"),
            "Second MCMR database should be from 'us' topology, got: {}",
            mcmr_dbs[1].name
        );

        // Test that we have both single-region and MCMR databases
        let single_region_count = databases.iter().filter(|db| !db.name.contains('+')).count();
        let mcmr_count = databases.iter().filter(|db| db.name.contains('+')).count();

        assert!(
            single_region_count >= 2,
            "Should have at least 2 single-region databases, got: {}",
            single_region_count
        );
        assert!(
            mcmr_count >= 2,
            "Should have at least 2 MCMR databases, got: {}",
            mcmr_count
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_mcmr_delete_collection_soft_delete() {
        let Some(backend): Option<SpannerBackend> = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (service, _temp_dir) = setup_test_service(backend.clone()).await;

        // Create test data
        let (tenant_id, database_name) = setup_tenant_and_database(&backend).await;

        // Create collection using the internal type like other tests
        let collection_id = CollectionUuid(Uuid::new_v4());
        let create_collection_req = CreateCollectionRequest {
            id: collection_id,
            tenant_id: tenant_id.clone(),
            database_name: database_name.clone(),
            name: "test_collection".to_string(),
            dimension: Some(128),
            metadata: Some(HashMap::new()),
            segments: vec![Segment {
                id: SegmentUuid(Uuid::new_v4()),
                r#type: SegmentType::BlockfileMetadata,
                scope: SegmentScope::METADATA,
                collection: collection_id,
                file_path: HashMap::new(),
                metadata: None,
            }],
            index_schema: chroma_types::Schema::default(),
            get_or_create: false,
        };

        let backend_for_create = create_collection_req.assign(&service.backends);
        let _create_response = create_collection_req.run(backend_for_create).await.unwrap();

        // Test: Delete the collection (soft delete)
        let delete_req = chroma_types::chroma_proto::DeleteCollectionRequest {
            tenant: tenant_id.clone(),
            database: database_name.as_ref().to_string(),
            id: collection_id.0.to_string(),
            segment_ids: vec![],
        };
        let delete_response = service
            .delete_collection(Request::new(delete_req))
            .await
            .unwrap();

        // Verify the response
        assert_eq!(
            delete_response.into_inner(),
            chroma_types::chroma_proto::DeleteCollectionResponse {}
        );

        // Verify the collection is soft deleted by trying to get it
        let get_req = chroma_types::chroma_proto::GetCollectionWithSegmentsRequest {
            database: Some(database_name.as_ref().to_string()),
            id: collection_id.0.to_string(),
        };
        let get_result = service
            .get_collection_with_segments(Request::new(get_req))
            .await;

        // Should fail because collection is soft deleted
        assert!(get_result.is_err());
        let status = get_result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);

        // Verify the collection exists in soft-deleted form using backend directly
        let get_collections_req = internal::GetCollectionsRequest {
            filter: internal::CollectionFilter {
                ids: Some(vec![collection_id]),
                database_name: Some(database_name.clone()),
                name: None,
                tenant_id: None,
                include_soft_deleted: true,
                limit: None,
                offset: None,
                topology_name: None,
            },
        };
        let backend_for_get = get_collections_req.assign(&service.backends);
        let get_collections_result = get_collections_req.run(backend_for_get).await;

        // Should succeed and return the soft-deleted collection
        assert!(get_collections_result.is_ok());
        let response = get_collections_result.unwrap();
        assert_eq!(response.collections.len(), 1);
        // The collection should be returned when include_soft_deleted=true
        // (even though chroma_types::Collection doesn't expose is_deleted field)
        // because the proto structure is different. The main test is that
        // soft delete works and the collection is not found when trying to get it.
    }
}
