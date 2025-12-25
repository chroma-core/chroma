use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
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
    GetTenantRequest, GetTenantResponse, ListCollectionVersionsRequest,
    ListCollectionVersionsResponse, ListCollectionsToGcRequest, ListCollectionsToGcResponse,
    ListDatabasesRequest, ListDatabasesResponse, MarkVersionForDeletionRequest,
    MarkVersionForDeletionResponse, ResetStateResponse, RestoreCollectionRequest,
    RestoreCollectionResponse, SetLastCompactionTimeForTenantRequest, SetTenantResourceNameRequest,
    SetTenantResourceNameResponse, UpdateCollectionRequest, UpdateCollectionResponse,
    UpdateSegmentRequest, UpdateSegmentResponse,
};
use tokio::{
    select,
    signal::unix::{signal, SignalKind},
};
use tonic::{transport::Server, Request, Response, Status};

use crate::config::SysDbServiceConfig;
use crate::router::{Operation, RouteRequest, Router};
use crate::spanner::SpannerBackend;
use crate::types as internal;

pub struct SysdbService {
    port: u16,
    #[allow(dead_code)]
    storage: Storage,
    #[allow(dead_code)]
    router: Router,
}

impl SysdbService {
    pub fn new(port: u16, storage: Storage, router: Router) -> Self {
        Self {
            port,
            storage,
            router,
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

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

        // TODO(Sanket): More sophisticated is_ready logic.
        health_reporter
            .set_serving::<SysDbServer<SysdbService>>()
            .await;

        let router = self.router.clone();
        Server::builder()
            .layer(chroma_tracing::GrpcServerTraceLayer)
            .add_service(health_service)
            .add_service(SysDbServer::new(self))
            .serve_with_shutdown(addr, async move {
                // TODO(Sanket): Drain existing requests before shutting down.
                select! {
                    _ = sigterm.recv() => {
                        router.close().await;
                        tracing::info!("Received SIGTERM, shutting down server");
                    }
                    _ = sigint.recv() => {
                        router.close().await;
                        tracing::info!("Received SIGINT, shutting down server");
                    }
                }
            })
            .await
            .expect("Server failed");
    }
}

#[async_trait::async_trait]
impl Configurable<SysDbServiceConfig> for SysdbService {
    async fn try_from_config(
        config: &SysDbServiceConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let storage = Storage::try_from_config(&config.storage, registry).await?;
        let spanner = SpannerBackend::try_from_config(&config.spanner, registry).await?;
        let router = Router::new(spanner);
        Ok(SysdbService::new(config.port, storage, router))
    }
}

#[async_trait::async_trait]
impl SysDb for SysdbService {
    async fn create_database(
        &self,
        _request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        todo!()
    }

    async fn get_database(
        &self,
        _request: Request<GetDatabaseRequest>,
    ) -> Result<Response<GetDatabaseResponse>, Status> {
        todo!()
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
        // Convert proto -> internal request type
        let internal_req: internal::CreateTenantRequest = proto_req.into();

        let backends = self.router.route(&RouteRequest {
            op: Operation::CreateTenant,
            db_name: None,
            tenant: Some(&internal_req.name),
        })?;

        // Fan out to all backends
        let mut internal_resp = None;
        for backend in backends {
            internal_resp = Some(backend.create_tenant(&internal_req).await?);
        }

        // Convert internal -> proto response type
        let resp = internal_resp.ok_or_else(|| Status::internal("no backend for request"))?;
        Ok(Response::new(resp.into()))
    }

    async fn get_tenant(
        &self,
        request: Request<GetTenantRequest>,
    ) -> Result<Response<GetTenantResponse>, Status> {
        let proto_req = request.into_inner();
        // Convert proto -> internal request type
        let internal_req: internal::GetTenantRequest = proto_req.into();

        let backends = self.router.route(&RouteRequest {
            op: Operation::GetTenant,
            db_name: None,
            tenant: Some(&internal_req.name),
        })?;

        // Should be exactly one backend for GetTenant
        let backend = backends
            .into_iter()
            .next()
            .ok_or_else(|| Status::internal("no backend for request"))?;

        let internal_resp = backend.get_tenant(&internal_req).await?.ok_or_else(|| {
            Status::not_found(format!("tenant '{}' not found", internal_req.name))
        })?;

        // Convert internal -> proto response type
        Ok(Response::new(internal_resp.into()))
    }

    async fn set_tenant_resource_name(
        &self,
        request: Request<SetTenantResourceNameRequest>,
    ) -> Result<Response<SetTenantResourceNameResponse>, Status> {
        let proto_req = request.into_inner();
        // Convert proto -> internal request type
        let internal_req: internal::SetTenantResourceNameRequest = proto_req.into();

        let backends = self.router.route(&RouteRequest {
            op: Operation::SetTenantResourceName,
            db_name: None,
            tenant: Some(&internal_req.id),
        })?;

        // Fan out to all backends
        let mut internal_resp = None;
        for backend in backends {
            internal_resp = Some(backend.set_tenant_resource_name(&internal_req).await?);
        }

        // Convert internal -> proto response type
        let resp = internal_resp.ok_or_else(|| Status::internal("no backend for request"))?;
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
        _request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        todo!()
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
        _request: Request<GetCollectionsRequest>,
    ) -> Result<Response<GetCollectionsResponse>, Status> {
        todo!()
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
        _request: Request<GetCollectionWithSegmentsRequest>,
    ) -> Result<Response<GetCollectionWithSegmentsResponse>, Status> {
        todo!()
    }

    async fn check_collections(
        &self,
        _request: Request<CheckCollectionsRequest>,
    ) -> Result<Response<CheckCollectionsResponse>, Status> {
        todo!()
    }

    async fn update_collection(
        &self,
        _request: Request<UpdateCollectionRequest>,
    ) -> Result<Response<UpdateCollectionResponse>, Status> {
        todo!()
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
        _request: Request<GetLastCompactionTimeForTenantRequest>,
    ) -> Result<Response<GetLastCompactionTimeForTenantResponse>, Status> {
        todo!()
    }

    async fn set_last_compaction_time_for_tenant(
        &self,
        _request: Request<SetLastCompactionTimeForTenantRequest>,
    ) -> Result<Response<()>, Status> {
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
}
