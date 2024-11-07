use std::{iter::once, str::FromStr};

use chroma_blockstore::provider::BlockfileProvider;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_types::{
    chroma_proto::{
        self, query_executor_server::QueryExecutor, CountPlan, CountResult, GetPlan, GetResult,
        KnnBatchResult, KnnPlan,
    },
    CollectionUuid, SegmentUuid,
};
use futures::future::try_join_all;
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{trace_span, Instrument};

use crate::{
    config::QueryServiceConfig,
    execution::{
        dispatcher::Dispatcher,
        operators::{
            fetch_log::FetchLogOperator, fetch_segment::FetchSegmentOperator,
            knn_projection::KnnProjectionOperator,
        },
        orchestration::{
            get::GetOrchestrator,
            knn::{KnnError, KnnFilterOrchestrator, KnnOrchestrator},
            CountQueryOrchestrator,
        },
    },
    log::log::Log,
    sysdb::sysdb::SysDb,
    system::{ComponentHandle, System},
    tracing::util::wrap_span_with_parent_context,
    utils::convert::{from_proto_knn, to_proto_knn_batch_result},
};

#[derive(Clone)]
pub struct WorkerServer {
    // System
    system: Option<System>,
    // Component dependencies
    dispatcher: Option<ComponentHandle<Dispatcher>>,
    // Service dependencies
    log: Box<Log>,
    sysdb: Box<SysDb>,
    hnsw_index_provider: HnswIndexProvider,
    blockfile_provider: BlockfileProvider,
    port: u16,
}

#[async_trait::async_trait]
impl Configurable<QueryServiceConfig> for WorkerServer {
    async fn try_from_config(config: &QueryServiceConfig) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb_config = &config.sysdb;
        let sysdb = match crate::sysdb::from_config(sysdb_config).await {
            Ok(sysdb) => sysdb,
            Err(err) => {
                tracing::error!("Failed to create sysdb component: {:?}", err);
                return Err(err);
            }
        };
        let log_config = &config.log;
        let log = match crate::log::from_config(log_config).await {
            Ok(log) => log,
            Err(err) => {
                tracing::error!("Failed to create log component: {:?}", err);
                return Err(err);
            }
        };
        let storage = match chroma_storage::from_config(&config.storage).await {
            Ok(storage) => storage,
            Err(err) => {
                tracing::error!("Failed to create storage component: {:?}", err);
                return Err(err);
            }
        };

        let blockfile_provider = BlockfileProvider::try_from_config(&(
            config.blockfile_provider.clone(),
            storage.clone(),
        ))
        .await?;
        let hnsw_index_provider =
            HnswIndexProvider::try_from_config(&(config.hnsw_provider.clone(), storage.clone()))
                .await?;
        Ok(WorkerServer {
            dispatcher: None,
            system: None,
            sysdb,
            log,
            hnsw_index_provider,
            blockfile_provider,
            port: config.my_port,
        })
    }
}

impl WorkerServer {
    pub(crate) async fn run(worker: WorkerServer) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("[::]:{}", worker.port).parse().unwrap();
        println!("Worker listening on {}", addr);
        let server = Server::builder().add_service(
            chroma_proto::query_executor_server::QueryExecutorServer::new(worker.clone()),
        );

        #[cfg(debug_assertions)]
        let server =
            server.add_service(chroma_proto::debug_server::DebugServer::new(worker.clone()));

        let server = server.serve_with_shutdown(addr, async {
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(sigterm) => sigterm,
                Err(e) => {
                    tracing::error!("Failed to create signal handler: {:?}", e);
                    return;
                }
            };
            sigterm.recv().await;
            tracing::info!("Received SIGTERM, shutting down");
        });

        server.await?;

        Ok(())
    }

    pub(crate) fn set_dispatcher(&mut self, dispatcher: ComponentHandle<Dispatcher>) {
        self.dispatcher = Some(dispatcher);
    }

    pub(crate) fn set_system(&mut self, system: System) {
        self.system = Some(system);
    }

    fn decompose_proto_scan(
        &self,
        scan: chroma_proto::ScanOperator,
    ) -> Result<(FetchLogOperator, FetchSegmentOperator), Status> {
        let collection = scan
            .collection
            .ok_or(Status::invalid_argument("Invalid Collection"))?;
        let collection_uuid = CollectionUuid::from_str(&collection.id)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let vector_uuid = SegmentUuid::from_str(&scan.knn_id)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let metadata_uuid = SegmentUuid::from_str(&scan.metadata_id)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let record_uuid = SegmentUuid::from_str(&scan.record_id)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        Ok((
            FetchLogOperator {
                log_client: self.log.clone(),
                // TODO: Make this configurable
                batch_size: 100,
                start_log_offset_id: collection.log_position as u32 + 1,
                maximum_fetch_count: None,
                collection_uuid,
            },
            FetchSegmentOperator {
                sysdb: self.sysdb.clone(),
                collection_uuid,
                collection_version: collection.version as u32,
                metadata_uuid,
                record_uuid,
                vector_uuid,
            },
        ))
    }

    async fn orchestrate_count(
        &self,
        count: Request<CountPlan>,
    ) -> Result<Response<CountResult>, Status> {
        let scan = count
            .into_inner()
            .scan
            .ok_or(Status::invalid_argument("Invalid Scan Operator"))?;
        let (fetch_log_operator, fetch_segment_operator) = self.decompose_proto_scan(scan)?;

        // TODO: Update Count Orchestractor
        let counter = CountQueryOrchestrator::new(
            self.clone_system()?,
            &fetch_segment_operator.metadata_uuid.0,
            &fetch_log_operator.collection_uuid,
            fetch_log_operator.log_client,
            fetch_segment_operator.sysdb,
            self.clone_dispatcher()?,
            self.blockfile_provider.clone(),
            fetch_segment_operator.collection_version,
            fetch_log_operator.start_log_offset_id as u64 - 1,
        );

        match counter.run().await {
            Ok(count) => Ok(Response::new(CountResult {
                count: count as u32,
            })),
            Err(err) => Err(Status::new(err.code().into(), err.to_string())),
        }
    }

    async fn orchestrate_get(&self, get: Request<GetPlan>) -> Result<Response<GetResult>, Status> {
        let get_inner = get.into_inner();
        let scan = get_inner
            .scan
            .ok_or(Status::invalid_argument("Invalid Scan Operator"))?;
        let (fetch_log_operator, fetch_segment_operator) = self.decompose_proto_scan(scan)?;
        let filter = get_inner
            .filter
            .ok_or(Status::invalid_argument("Invalid Filter Operator"))?;
        let limit = get_inner
            .limit
            .ok_or(Status::invalid_argument("Invalid Scan Operator"))?;
        let projection = get_inner
            .projection
            .ok_or(Status::invalid_argument("Invalid Projection Operator"))?;

        let getter = GetOrchestrator::new(
            self.blockfile_provider.clone(),
            self.clone_dispatcher()?,
            // TODO: Make this configurable
            1000,
            fetch_log_operator,
            fetch_segment_operator,
            filter.try_into()?,
            limit.into(),
            projection.into(),
        );

        match getter.run(self.clone_system()?).await {
            Ok(result) => Ok(Response::new(result.try_into()?)),
            Err(err) => Err(Status::new(err.code().into(), err.to_string())),
        }
    }

    async fn orchestrate_knn(
        &self,
        knn: Request<KnnPlan>,
    ) -> Result<Response<KnnBatchResult>, Status> {
        let dispatcher = self.clone_dispatcher()?;
        let system = self.clone_system()?;
        let knn_inner = knn.into_inner();
        let scan = knn_inner
            .scan
            .ok_or(Status::invalid_argument("Invalid Scan Operator"))?;
        let (fetch_log_operator, fetch_segment_operator) = self.decompose_proto_scan(scan)?;
        let filter = knn_inner
            .filter
            .ok_or(Status::invalid_argument("Invalid Filter Operator"))?;
        let knn = knn_inner
            .knn
            .ok_or(Status::invalid_argument("Invalid Scan Operator"))?;

        let sieve = KnnFilterOrchestrator::new(
            self.blockfile_provider.clone(),
            dispatcher.clone(),
            // TODO: Make this configurable
            1000,
            fetch_log_operator,
            fetch_segment_operator,
            filter.try_into()?,
        );

        let matching_records = match sieve.run(system.clone()).await {
            Ok(output) => output,
            Err(KnnError::EmptyCollection) => {
                return Ok(Response::new(to_proto_knn_batch_result(
                    once(Default::default())
                        .cycle()
                        .take(knn.embeddings.len())
                        .collect(),
                )?));
            }
            Err(e) => {
                return Err(Status::new(e.code().into(), e.to_string()));
            }
        };

        let projection = knn_inner
            .projection
            .ok_or(Status::invalid_argument("Invalid Projection Operator"))?;
        let knn_projection = KnnProjectionOperator::try_from(projection)?;

        match try_join_all(
            from_proto_knn(knn)?
                .into_iter()
                .map(|knn| {
                    KnnOrchestrator::new(
                        self.blockfile_provider.clone(),
                        dispatcher.clone(),
                        self.hnsw_index_provider.clone(),
                        // TODO: Make this configurable
                        1000,
                        matching_records.clone(),
                        knn,
                        knn_projection.clone(),
                    )
                })
                .map(|knner| knner.run(system.clone())),
        )
        .await
        {
            Ok(results) => Ok(Response::new(to_proto_knn_batch_result(results)?)),
            Err(err) => Err(Status::new(err.code().into(), err.to_string())),
        }
    }

    fn clone_dispatcher(&self) -> Result<ComponentHandle<Dispatcher>, Status> {
        self.dispatcher
            .as_ref()
            .ok_or(Status::internal("Dispatcher is not initialized"))
            .cloned()
    }

    fn clone_system(&self) -> Result<System, Status> {
        self.system
            .as_ref()
            .ok_or(Status::internal("System is not initialized"))
            .cloned()
    }
}

#[tonic::async_trait]
impl QueryExecutor for WorkerServer {
    async fn count(&self, count: Request<CountPlan>) -> Result<Response<CountResult>, Status> {
        // Note: We cannot write a middleware that instruments every service rpc
        // with a span because of https://github.com/hyperium/tonic/pull/1202.
        let count_span = trace_span!(
            "CountPlan",
            count = ?count
        );
        let instrumented_span = wrap_span_with_parent_context(count_span, count.metadata());
        self.orchestrate_count(count)
            .instrument(instrumented_span)
            .await
    }

    async fn get(&self, get: Request<GetPlan>) -> Result<Response<GetResult>, Status> {
        // Note: We cannot write a middleware that instruments every service rpc
        // with a span because of https://github.com/hyperium/tonic/pull/1202.
        let get_span = trace_span!(
            "GetPlan",
            get = ?get
        );
        let instrumented_span = wrap_span_with_parent_context(get_span, get.metadata());
        self.orchestrate_get(get)
            .instrument(instrumented_span)
            .await
    }

    async fn knn(&self, knn: Request<KnnPlan>) -> Result<Response<KnnBatchResult>, Status> {
        // Note: We cannot write a middleware that instruments every service rpc
        // with a span because of https://github.com/hyperium/tonic/pull/1202.
        let knn_span = trace_span!(
            "KnnPlan",
            knn = ?knn
        );
        let instrumented_span = wrap_span_with_parent_context(knn_span, knn.metadata());
        self.orchestrate_knn(knn)
            .instrument(instrumented_span)
            .await
    }
}

#[cfg(debug_assertions)]
#[tonic::async_trait]
impl chroma_proto::debug_server::Debug for WorkerServer {
    async fn get_info(
        &self,
        request: Request<()>,
    ) -> Result<Response<chroma_proto::GetInfoResponse>, Status> {
        // Note: We cannot write a middleware that instruments every service rpc
        // with a span because of https://github.com/hyperium/tonic/pull/1202.
        let request_span = trace_span!("Get info");

        wrap_span_with_parent_context(request_span, request.metadata()).in_scope(|| {
            let response = chroma_proto::GetInfoResponse {
                version: option_env!("CARGO_PKG_VERSION")
                    .unwrap_or("unknown")
                    .to_string(),
            };
            Ok(Response::new(response))
        })
    }

    async fn trigger_panic(&self, request: Request<()>) -> Result<Response<()>, Status> {
        // Note: We cannot write a middleware that instruments every service rpc
        // with a span because of https://github.com/hyperium/tonic/pull/1202.
        let request_span = trace_span!("Trigger panic");

        wrap_span_with_parent_context(request_span, request.metadata()).in_scope(|| {
            panic!("Intentional panic triggered");
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::dispatcher;
    use crate::log::log::InMemoryLog;
    use crate::segment::test::TestSegment;
    use crate::sysdb::test_sysdb::TestSysDb;
    use crate::system;
    #[cfg(debug_assertions)]
    use chroma_proto::debug_client::DebugClient;
    use chroma_proto::query_executor_client::QueryExecutorClient;
    use uuid::Uuid;

    fn run_server() -> String {
        let sysdb = TestSysDb::new();
        let log = InMemoryLog::new();
        let segments = TestSegment::default();
        let port = random_port::PortPicker::new().random(true).pick().unwrap();

        let mut server = WorkerServer {
            dispatcher: None,
            system: None,
            sysdb: Box::new(SysDb::Test(sysdb)),
            log: Box::new(Log::InMemory(log)),
            hnsw_index_provider: segments.hnsw_provider,
            blockfile_provider: segments.blockfile_provider,
            port,
        };

        let system: system::System = system::System::new();
        let dispatcher = dispatcher::Dispatcher::new(4, 10, 10);
        let dispatcher_handle = system.start_component(dispatcher);

        server.set_system(system);
        server.set_dispatcher(dispatcher_handle);

        tokio::spawn(async move {
            let _ = crate::server::WorkerServer::run(server).await;
        });

        format!("http://localhost:{}", port)
    }

    fn scan() -> chroma_proto::ScanOperator {
        chroma_proto::ScanOperator {
            collection: Some(chroma_proto::Collection {
                id: Uuid::new_v4().to_string(),
                name: "Test-Collection".to_string(),
                configuration_json_str: String::new(),
                metadata: None,
                dimension: None,
                tenant: "Test-Tenant".to_string(),
                database: "Test-Database".to_string(),
                log_position: 0,
                version: 0,
            }),
            knn_id: Uuid::new_v4().to_string(),
            metadata_id: Uuid::new_v4().to_string(),
            record_id: Uuid::new_v4().to_string(),
        }
    }

    #[tokio::test]
    #[cfg(debug_assertions)]
    async fn gracefully_handles_panics() {
        let mut client = DebugClient::connect(run_server()).await.unwrap();

        // Test response when handler panics
        let err_response = client.trigger_panic(Request::new(())).await.unwrap_err();
        assert_eq!(err_response.code(), tonic::Code::Cancelled);

        // The server should still work, even after a panic was thrown
        let response = client.get_info(Request::new(())).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn validate_count_plan() {
        let mut reader = QueryExecutorClient::connect(run_server()).await.unwrap();
        let mut scan_operator = scan();
        let request = chroma_proto::CountPlan {
            scan: Some(scan_operator.clone()),
        };

        // segment or collection not found
        let response = reader.count(request).await;
        assert_eq!(response.unwrap_err().code(), tonic::Code::NotFound);

        // invalid segment uuid
        scan_operator.record_id = "invalid_segment_id".to_string();
        let request = chroma_proto::CountPlan {
            scan: Some(scan_operator.clone()),
        };
        let response = reader.count(request).await;

        assert!(response.is_err());
        assert_eq!(response.unwrap_err(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn validate_get_plan() {
        let mut reader = QueryExecutorClient::connect(run_server()).await.unwrap();
        let request = chroma_proto::GetPlan {
            scan: Some(scan()),
            // error parsing filter
            filter: None,
            limit: Some(chroma_proto::LimitOperator {
                skip: 0,
                fetch: None,
            }),
            projection: Some(chroma_proto::ProjectionOperator {
                document: false,
                embedding: false,
                metadata: false,
            }),
        };
        let response = reader.get(request.clone()).await;

        assert!(response.is_err());
        assert_eq!(response.unwrap_err().code(), tonic::Code::InvalidArgument);
    }
}
