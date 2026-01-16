use crate::types as internal;
use crate::types::SysDbError;
use crate::{
    backend::{Assignable, BackendFactory, Runnable},
    config::RootConfig,
};
use chroma_config::{registry::Registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
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
use thiserror::Error;
use tokio::{
    select,
    signal::unix::{signal, SignalKind},
};
use tonic::{transport::Server, Request, Response, Status};

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
        _request: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        todo!()
    }

    async fn delete_database(
        &self,
        _request: Request<DeleteDatabaseRequest>,
    ) -> Result<Response<DeleteDatabaseResponse>, Status> {
        todo!()
    }

    async fn finish_database_deletion(
        &self,
        _request: Request<FinishDatabaseDeletionRequest>,
    ) -> Result<Response<FinishDatabaseDeletionResponse>, Status> {
        todo!()
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
        request: Request<SetTenantResourceNameRequest>,
    ) -> Result<Response<SetTenantResourceNameResponse>, Status> {
        let proto_req = request.into_inner();
        let internal_req: internal::SetTenantResourceNameRequest = proto_req
            .try_into()
            .map_err(|e: SysDbError| Status::from(e))?;

        let backends = internal_req.assign(&self.backends);
        let resp = internal_req.run(backends).await?;

        Ok(Response::new(resp.into()))
    }

    async fn create_segment(
        &self,
        _request: Request<CreateSegmentRequest>,
    ) -> Result<Response<CreateSegmentResponse>, Status> {
        todo!()
    }

    async fn delete_segment(
        &self,
        _request: Request<DeleteSegmentRequest>,
    ) -> Result<Response<DeleteSegmentResponse>, Status> {
        todo!()
    }

    async fn get_segments(
        &self,
        _request: Request<GetSegmentsRequest>,
    ) -> Result<Response<GetSegmentsResponse>, Status> {
        todo!()
    }

    async fn update_segment(
        &self,
        _request: Request<UpdateSegmentRequest>,
    ) -> Result<Response<UpdateSegmentResponse>, Status> {
        todo!()
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
        _request: Request<DeleteCollectionRequest>,
    ) -> Result<Response<DeleteCollectionResponse>, Status> {
        todo!()
    }

    async fn finish_collection_deletion(
        &self,
        _request: Request<FinishCollectionDeletionRequest>,
    ) -> Result<Response<FinishCollectionDeletionResponse>, Status> {
        todo!()
    }

    async fn get_collection(
        &self,
        _request: Request<GetCollectionRequest>,
    ) -> Result<Response<GetCollectionResponse>, Status> {
        todo!()
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
        todo!()
    }

    async fn count_collections(
        &self,
        _request: Request<CountCollectionsRequest>,
    ) -> Result<Response<CountCollectionsResponse>, Status> {
        todo!()
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
        todo!()
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
        todo!()
    }

    async fn count_forks(
        &self,
        _request: Request<CountForksRequest>,
    ) -> Result<Response<CountForksResponse>, Status> {
        todo!()
    }

    async fn reset_state(
        &self,
        _request: Request<()>,
    ) -> Result<Response<ResetStateResponse>, Status> {
        todo!()
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
        // Use the existing get_tenants infrastructure
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
        // TODO: Remove this method
        todo!()
    }

    async fn restore_collection(
        &self,
        _request: Request<RestoreCollectionRequest>,
    ) -> Result<Response<RestoreCollectionResponse>, Status> {
        todo!()
    }

    async fn list_collection_versions(
        &self,
        _request: Request<ListCollectionVersionsRequest>,
    ) -> Result<Response<ListCollectionVersionsResponse>, Status> {
        todo!()
    }

    async fn get_collection_size(
        &self,
        _request: Request<GetCollectionSizeRequest>,
    ) -> Result<Response<GetCollectionSizeResponse>, Status> {
        todo!()
    }

    async fn list_collections_to_gc(
        &self,
        _request: Request<ListCollectionsToGcRequest>,
    ) -> Result<Response<ListCollectionsToGcResponse>, Status> {
        todo!()
    }

    async fn mark_version_for_deletion(
        &self,
        _request: Request<MarkVersionForDeletionRequest>,
    ) -> Result<Response<MarkVersionForDeletionResponse>, Status> {
        todo!()
    }

    async fn delete_collection_version(
        &self,
        _request: Request<DeleteCollectionVersionRequest>,
    ) -> Result<Response<DeleteCollectionVersionResponse>, Status> {
        todo!()
    }

    async fn batch_get_collection_version_file_paths(
        &self,
        _request: Request<BatchGetCollectionVersionFilePathsRequest>,
    ) -> Result<Response<BatchGetCollectionVersionFilePathsResponse>, Status> {
        todo!()
    }

    async fn batch_get_collection_soft_delete_status(
        &self,
        _request: Request<BatchGetCollectionSoftDeleteStatusRequest>,
    ) -> Result<Response<BatchGetCollectionSoftDeleteStatusResponse>, Status> {
        todo!()
    }

    async fn flush_collection_compaction(
        &self,
        _request: Request<FlushCollectionCompactionRequest>,
    ) -> Result<Response<FlushCollectionCompactionResponse>, Status> {
        todo!()
    }

    async fn attach_function(
        &self,
        _request: Request<AttachFunctionRequest>,
    ) -> Result<Response<AttachFunctionResponse>, Status> {
        todo!()
    }

    async fn get_attached_functions(
        &self,
        _request: Request<GetAttachedFunctionsRequest>,
    ) -> Result<Response<GetAttachedFunctionsResponse>, Status> {
        todo!()
    }

    async fn detach_function(
        &self,
        _request: Request<DetachFunctionRequest>,
    ) -> Result<Response<DetachFunctionResponse>, Status> {
        todo!()
    }

    async fn finish_create_attached_function(
        &self,
        _request: Request<FinishCreateAttachedFunctionRequest>,
    ) -> Result<Response<FinishCreateAttachedFunctionResponse>, Status> {
        todo!()
    }

    async fn cleanup_expired_partial_attached_functions(
        &self,
        _request: Request<CleanupExpiredPartialAttachedFunctionsRequest>,
    ) -> Result<Response<CleanupExpiredPartialAttachedFunctionsResponse>, Status> {
        todo!()
    }

    async fn get_functions(
        &self,
        _request: Request<GetFunctionsRequest>,
    ) -> Result<Response<GetFunctionsResponse>, Status> {
        todo!()
    }

    async fn get_attached_functions_to_gc(
        &self,
        _request: Request<GetAttachedFunctionsToGcRequest>,
    ) -> Result<Response<GetAttachedFunctionsToGcResponse>, Status> {
        todo!()
    }

    async fn finish_attached_function_deletion(
        &self,
        _request: Request<FinishAttachedFunctionDeletionRequest>,
    ) -> Result<Response<FinishAttachedFunctionDeletionResponse>, Status> {
        todo!()
    }

    async fn flush_collection_compaction_and_attached_function(
        &self,
        _request: Request<FlushCollectionCompactionAndAttachedFunctionRequest>,
    ) -> Result<Response<FlushCollectionCompactionAndAttachedFunctionResponse>, Status> {
        todo!()
    }

    async fn increment_compaction_failure_count(
        &self,
        _request: Request<IncrementCompactionFailureCountRequest>,
    ) -> Result<Response<IncrementCompactionFailureCountResponse>, Status> {
        todo!()
    }
}
