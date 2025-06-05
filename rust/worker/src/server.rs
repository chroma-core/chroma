use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_index::{hnsw_provider::HnswIndexProvider, spann::types::SpannMetrics};
use chroma_log::Log;
use chroma_segment::spann_provider::SpannProvider;
use chroma_storage::Storage;
use chroma_sysdb::SysDb;
use chroma_system::{ComponentHandle, Dispatcher, Orchestrator, System};
use chroma_tracing::util::wrap_span_with_parent_context;
use chroma_types::{
    chroma_proto::{
        query_executor_server::{QueryExecutor, QueryExecutorServer},
        CountPlan, CountResult, GetPlan, GetResult, KnnBatchResult, KnnPlan,
    },
    operator::Scan,
    CollectionAndSegments, SegmentType,
};
use futures::{stream, StreamExt, TryStreamExt};
use std::iter::once;
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{trace_span, Instrument};

use crate::{
    config::QueryServiceConfig,
    execution::{
        operators::{fetch_log::FetchLogOperator, knn_projection::KnnProjectionOperator},
        orchestration::{
            get::GetOrchestrator, knn::KnnOrchestrator, knn_filter::KnnFilterOrchestrator,
            spann_knn::SpannKnnOrchestrator, CountOrchestrator,
        },
    },
    utils::convert::{from_proto_knn, to_proto_knn_batch_result},
};

#[derive(Clone)]
pub struct WorkerServer {
    // System
    system: System,
    // Component dependencies
    dispatcher: Option<ComponentHandle<Dispatcher>>,
    // Service dependencies
    log: Log,
    _sysdb: SysDb,
    hnsw_index_provider: HnswIndexProvider,
    blockfile_provider: BlockfileProvider,
    port: u16,
    // config
    fetch_log_batch_size: u32,
}

#[async_trait]
impl Configurable<(QueryServiceConfig, System)> for WorkerServer {
    async fn try_from_config(
        config: &(QueryServiceConfig, System),
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (config, system) = config;
        let sysdb = SysDb::try_from_config(&config.sysdb, registry).await?;
        let log = Log::try_from_config(&(config.log.clone(), system.clone()), registry).await?;
        let storage = Storage::try_from_config(&config.storage, registry).await?;
        let blockfile_provider = BlockfileProvider::try_from_config(
            &(config.blockfile_provider.clone(), storage.clone()),
            registry,
        )
        .await?;
        let hnsw_index_provider = HnswIndexProvider::try_from_config(
            &(config.hnsw_provider.clone(), storage.clone()),
            registry,
        )
        .await?;
        Ok(WorkerServer {
            dispatcher: None,
            system: system.clone(),
            _sysdb: sysdb,
            log,
            hnsw_index_provider,
            blockfile_provider,
            port: config.my_port,
            fetch_log_batch_size: config.fetch_log_batch_size,
        })
    }
}

impl WorkerServer {
    pub(crate) async fn run(worker: WorkerServer) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("[::]:{}", worker.port).parse().unwrap();
        println!("Worker listening on {}", addr);

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<QueryExecutorServer<Self>>()
            .await;

        let server = Server::builder()
            .add_service(health_service)
            .add_service(QueryExecutorServer::new(worker.clone()));

        #[cfg(debug_assertions)]
        let server = server.add_service(
            chroma_types::chroma_proto::debug_server::DebugServer::new(worker.clone()),
        );

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

    fn fetch_log(
        &self,
        collection_and_segments: &CollectionAndSegments,
        batch_size: u32,
    ) -> FetchLogOperator {
        FetchLogOperator {
            log_client: self.log.clone(),
            batch_size,
            // The collection log position is inclusive, and we want to start from the next log
            // Note that we query using the incoming log position this is critical for correctness
            start_log_offset_id: u64::try_from(collection_and_segments.collection.log_position + 1)
                .unwrap_or_default(),
            maximum_fetch_count: None,
            collection_uuid: collection_and_segments.collection.collection_id,
            tenant: collection_and_segments.collection.tenant.clone(),
        }
    }

    async fn orchestrate_count(
        &self,
        count: Request<CountPlan>,
    ) -> Result<Response<CountResult>, Status> {
        let scan = count
            .into_inner()
            .scan
            .ok_or(Status::invalid_argument("Invalid Scan Operator"))?;

        let collection_and_segments = Scan::try_from(scan)?.collection_and_segments;
        let fetch_log = self.fetch_log(&collection_and_segments, self.fetch_log_batch_size);

        let count_orchestrator = CountOrchestrator::new(
            self.blockfile_provider.clone(),
            self.clone_dispatcher()?,
            // TODO: Make this configurable
            1000,
            collection_and_segments,
            fetch_log,
        );

        match count_orchestrator.run(self.system.clone()).await {
            Ok((count, pulled_log_bytes)) => Ok(Response::new(CountResult {
                count,
                pulled_log_bytes,
            })),
            Err(err) => Err(Status::new(err.code().into(), err.to_string())),
        }
    }

    async fn orchestrate_get(&self, get: Request<GetPlan>) -> Result<Response<GetResult>, Status> {
        let get_inner = get.into_inner();
        let scan = get_inner
            .scan
            .ok_or(Status::invalid_argument("Invalid Scan Operator"))?;

        let collection_and_segments = Scan::try_from(scan)?.collection_and_segments;
        let fetch_log = self.fetch_log(&collection_and_segments, self.fetch_log_batch_size);

        let filter = get_inner
            .filter
            .ok_or(Status::invalid_argument("Invalid Filter Operator"))?;

        let limit = get_inner
            .limit
            .ok_or(Status::invalid_argument("Invalid Scan Operator"))?;

        let projection = get_inner
            .projection
            .ok_or(Status::invalid_argument("Invalid Projection Operator"))?;

        let get_orchestrator = GetOrchestrator::new(
            self.blockfile_provider.clone(),
            self.clone_dispatcher()?,
            // TODO: Make this configurable
            1000,
            collection_and_segments,
            fetch_log,
            filter.try_into()?,
            limit.into(),
            projection.into(),
        );

        match get_orchestrator.run(self.system.clone()).await {
            Ok((result, pulled_log_bytes)) => Ok(Response::new(GetResult {
                records: result
                    .records
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
                pulled_log_bytes,
            })),
            Err(err) => Err(Status::new(err.code().into(), err.to_string())),
        }
    }

    async fn orchestrate_knn(
        &self,
        knn: Request<KnnPlan>,
    ) -> Result<Response<KnnBatchResult>, Status> {
        let dispatcher = self.clone_dispatcher()?;
        let system = self.system.clone();

        let knn_inner = knn.into_inner();

        let scan = knn_inner
            .scan
            .ok_or(Status::invalid_argument("Invalid Scan Operator"))?;

        let collection_and_segments = Scan::try_from(scan)?.collection_and_segments;

        let fetch_log = self.fetch_log(&collection_and_segments, self.fetch_log_batch_size);

        let filter = knn_inner
            .filter
            .ok_or(Status::invalid_argument("Invalid Filter Operator"))?;

        let knn = knn_inner
            .knn
            .ok_or(Status::invalid_argument("Invalid Knn Operator"))?;

        let projection = knn_inner
            .projection
            .ok_or(Status::invalid_argument("Invalid Projection Operator"))?;
        let knn_projection = KnnProjectionOperator::try_from(projection)
            .map_err(|e| Status::invalid_argument(format!("Invalid Projection Operator: {}", e)))?;

        if knn.embeddings.is_empty() {
            return Ok(Response::new(to_proto_knn_batch_result(0, Vec::new())?));
        }

        // If dimension is not set and segment is uninitialized, we assume
        // this is a query on empty collection, so we return early here
        if collection_and_segments.collection.dimension.is_none()
            && collection_and_segments.vector_segment.file_path.is_empty()
        {
            return Ok(Response::new(to_proto_knn_batch_result(
                0,
                once(Default::default())
                    .cycle()
                    .take(knn.embeddings.len())
                    .collect(),
            )?));
        }

        let vector_segment_type = collection_and_segments.vector_segment.r#type;
        let knn_filter_orchestrator = KnnFilterOrchestrator::new(
            self.blockfile_provider.clone(),
            dispatcher.clone(),
            self.hnsw_index_provider.clone(),
            // TODO: Make this configurable
            1000,
            collection_and_segments.clone(),
            fetch_log,
            filter.try_into()?,
        );

        let matching_records = match knn_filter_orchestrator.run(system.clone()).await {
            Ok(output) => output,
            Err(e) => {
                return Err(Status::new(e.code().into(), e.to_string()));
            }
        };

        let pulled_log_bytes = matching_records.fetch_log_bytes;

        if vector_segment_type == SegmentType::Spann {
            tracing::debug!("Running KNN on SPANN segment");
            let spann_provider = SpannProvider {
                hnsw_provider: self.hnsw_index_provider.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                garbage_collection_context: None,
                metrics: SpannMetrics::default(),
            };
            let knn_orchestrator_futures = from_proto_knn(knn)?
                .into_iter()
                .map(|knn| {
                    SpannKnnOrchestrator::new(
                        spann_provider.clone(),
                        dispatcher.clone(),
                        1000,
                        collection_and_segments.collection.clone(),
                        matching_records.clone(),
                        knn.fetch as usize,
                        knn.embedding,
                        knn_projection.clone(),
                    )
                })
                .map(|knner| knner.run(system.clone()));
            match stream::iter(knn_orchestrator_futures)
                .buffered(32)
                .try_collect::<Vec<_>>()
                .await
            {
                Ok(results) => Ok(Response::new(to_proto_knn_batch_result(
                    pulled_log_bytes,
                    results,
                )?)),
                Err(err) => Err(Status::new(err.code().into(), err.to_string())),
            }
        } else {
            let knn_orchestrator_futures = from_proto_knn(knn)?
                .into_iter()
                .map(|knn| {
                    KnnOrchestrator::new(
                        self.blockfile_provider.clone(),
                        dispatcher.clone(),
                        // TODO: Make this configurable
                        1000,
                        matching_records.clone(),
                        knn,
                        knn_projection.clone(),
                    )
                })
                .map(|knner| knner.run(system.clone()));

            match stream::iter(knn_orchestrator_futures)
                .buffered(32)
                .try_collect::<Vec<_>>()
                .await
            {
                Ok(results) => Ok(Response::new(to_proto_knn_batch_result(
                    pulled_log_bytes,
                    results,
                )?)),
                Err(err) => Err(Status::new(err.code().into(), err.to_string())),
            }
        }
    }

    fn clone_dispatcher(&self) -> Result<ComponentHandle<Dispatcher>, Status> {
        self.dispatcher
            .as_ref()
            .ok_or(Status::internal("Dispatcher is not initialized"))
            .cloned()
    }
}

#[async_trait]
impl QueryExecutor for WorkerServer {
    async fn count(&self, count: Request<CountPlan>) -> Result<Response<CountResult>, Status> {
        // Note: We cannot write a middleware that instruments every service rpc
        // with a span because of https://github.com/hyperium/tonic/pull/1202.
        let count_span = trace_span!("CountPlan",);
        let instrumented_span = wrap_span_with_parent_context(count_span, count.metadata());
        self.orchestrate_count(count)
            .instrument(instrumented_span)
            .await
    }

    async fn get(&self, get: Request<GetPlan>) -> Result<Response<GetResult>, Status> {
        // Note: We cannot write a middleware that instruments every service rpc
        // with a span because of https://github.com/hyperium/tonic/pull/1202.
        let get_span = trace_span!("GetPlan",);
        let instrumented_span = wrap_span_with_parent_context(get_span, get.metadata());
        self.orchestrate_get(get)
            .instrument(instrumented_span)
            .await
    }

    async fn knn(&self, knn: Request<KnnPlan>) -> Result<Response<KnnBatchResult>, Status> {
        // Note: We cannot write a middleware that instruments every service rpc
        // with a span because of https://github.com/hyperium/tonic/pull/1202.
        let knn_span = trace_span!("KnnPlan",);
        let instrumented_span = wrap_span_with_parent_context(knn_span, knn.metadata());
        self.orchestrate_knn(knn)
            .instrument(instrumented_span)
            .await
    }
}

#[cfg(debug_assertions)]
#[async_trait]
impl chroma_types::chroma_proto::debug_server::Debug for WorkerServer {
    async fn get_info(
        &self,
        request: Request<()>,
    ) -> Result<Response<chroma_types::chroma_proto::GetInfoResponse>, Status> {
        // Note: We cannot write a middleware that instruments every service rpc
        // with a span because of https://github.com/hyperium/tonic/pull/1202.
        let request_span = trace_span!("Get info");

        wrap_span_with_parent_context(request_span, request.metadata()).in_scope(|| {
            let response = chroma_types::chroma_proto::GetInfoResponse {
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
    use std::collections::HashMap;

    use super::*;
    use chroma_index::test_hnsw_index_provider;
    use chroma_log::in_memory_log::InMemoryLog;
    use chroma_segment::test::TestDistributedSegment;
    use chroma_sysdb::TestSysDb;
    use chroma_system::DispatcherConfig;
    use chroma_types::chroma_proto;
    #[cfg(debug_assertions)]
    use chroma_types::chroma_proto::debug_client::DebugClient;
    use chroma_types::chroma_proto::query_executor_client::QueryExecutorClient;
    use uuid::Uuid;

    fn run_server() -> String {
        let sysdb = TestSysDb::new();
        let system = System::new();
        let log = InMemoryLog::new();
        let segments = TestDistributedSegment::default();
        let port = random_port::PortPicker::new().random(true).pick().unwrap();

        let mut server = WorkerServer {
            dispatcher: None,
            system: system.clone(),
            _sysdb: SysDb::Test(sysdb),
            log: Log::InMemory(log),
            hnsw_index_provider: test_hnsw_index_provider(),
            blockfile_provider: segments.blockfile_provider,
            port,
            fetch_log_batch_size: 100,
        };

        let dispatcher = Dispatcher::new(DispatcherConfig {
            num_worker_threads: 4,
            task_queue_limit: 10,
            dispatcher_queue_size: 10,
            worker_queue_size: 10,
            active_io_tasks: 10,
        });
        let dispatcher_handle = system.start_component(dispatcher);
        server.set_dispatcher(dispatcher_handle);

        tokio::spawn(async move {
            let _ = crate::server::WorkerServer::run(server).await;
        });

        format!("http://localhost:{}", port)
    }

    fn scan() -> chroma_proto::ScanOperator {
        let collection_id = Uuid::new_v4().to_string();
        chroma_proto::ScanOperator {
            collection: Some(chroma_proto::Collection {
                id: collection_id.clone(),
                name: "test-collection".to_string(),
                configuration_json_str: "{}".to_string(),
                metadata: None,
                dimension: None,
                tenant: "test-tenant".to_string(),
                database: "test-database".to_string(),
                ..Default::default()
            }),
            knn: Some(chroma_proto::Segment {
                id: Uuid::new_v4().to_string(),
                r#type: "urn:chroma:segment/vector/hnsw-distributed".to_string(),
                scope: 0,
                collection: collection_id.clone(),
                metadata: None,
                file_paths: HashMap::new(),
            }),
            metadata: Some(chroma_proto::Segment {
                id: Uuid::new_v4().to_string(),
                r#type: "urn:chroma:segment/metadata/blockfile".to_string(),
                scope: 1,
                collection: collection_id.clone(),
                metadata: None,
                file_paths: HashMap::new(),
            }),
            record: Some(chroma_proto::Segment {
                id: Uuid::new_v4().to_string(),
                r#type: "urn:chroma:segment/record/blockfile".to_string(),
                scope: 2,
                collection: collection_id.clone(),
                metadata: None,
                file_paths: HashMap::new(),
            }),
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
        let mut executor = QueryExecutorClient::connect(run_server()).await.unwrap();
        let mut scan_operator = scan();
        scan_operator.metadata = Some(chroma_proto::Segment {
            id: "invalid-metadata-segment-id".to_string(),
            r#type: "urn:chroma:segment/metadata/blockfile".to_string(),
            scope: 1,
            collection: scan_operator
                .collection
                .as_ref()
                .expect("The collection should exist")
                .id
                .clone(),
            metadata: None,
            file_paths: HashMap::new(),
        });
        let request = chroma_proto::CountPlan {
            scan: Some(scan_operator.clone()),
        };

        // invalid segment uuid
        let response = executor.count(request).await;
        assert!(response.is_err());
        assert_eq!(response.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn validate_get_plan() {
        let mut executor = QueryExecutorClient::connect(run_server()).await.unwrap();
        let mut scan_operator = scan();
        let request = chroma_proto::GetPlan {
            scan: Some(scan_operator.clone()),
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

        // error parsing filter
        let response = executor.get(request.clone()).await;
        assert!(response.is_err());
        assert_eq!(response.unwrap_err().code(), tonic::Code::InvalidArgument);

        scan_operator.collection = Some(chroma_proto::Collection {
            id: "invalid-collection-iD".to_string(),
            name: "broken-collection".to_string(),
            configuration_json_str: "{}".to_string(),
            metadata: None,
            dimension: None,
            tenant: "test-tenant".to_string(),
            database: "test-database".to_string(),
            ..Default::default()
        });
        let request = chroma_proto::GetPlan {
            scan: Some(scan_operator.clone()),
            filter: Some(chroma_proto::FilterOperator {
                ids: None,
                r#where: None,
                where_document: None,
            }),
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

        // invalid collection uuid
        let response = executor.get(request.clone()).await;
        assert!(response.is_err());
        assert_eq!(response.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    fn gen_knn_request(
        mut scan_operator: Option<chroma_proto::ScanOperator>,
    ) -> chroma_proto::KnnPlan {
        if scan_operator.is_none() {
            scan_operator = Some(scan());
        }
        chroma_proto::KnnPlan {
            scan: scan_operator,
            filter: Some(chroma_proto::FilterOperator {
                ids: None,
                r#where: None,
                where_document: None,
            }),
            knn: Some(chroma_proto::KnnOperator {
                embeddings: vec![],
                fetch: 0,
            }),
            projection: Some(chroma_proto::KnnProjectionOperator {
                projection: Some(chroma_proto::ProjectionOperator {
                    document: false,
                    embedding: false,
                    metadata: false,
                }),
                distance: false,
            }),
        }
    }

    #[tokio::test]
    async fn validate_knn_plan_empty_embeddings() {
        let mut executor = QueryExecutorClient::connect(run_server()).await.unwrap();
        let response = executor.knn(gen_knn_request(None)).await;
        assert!(response.is_ok());
        assert_eq!(response.unwrap().into_inner().results.len(), 0);
    }

    #[tokio::test]
    async fn validate_knn_plan_filter() {
        let mut executor = QueryExecutorClient::connect(run_server()).await.unwrap();
        let mut request = gen_knn_request(None);
        request.filter = None;
        let response = executor.knn(request).await;
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().to_lowercase().contains("filter operator"),
            "{}",
            err.message()
        );
    }

    #[tokio::test]
    async fn validate_knn_plan_knn() {
        let mut executor = QueryExecutorClient::connect(run_server()).await.unwrap();
        let mut request = gen_knn_request(None);
        request.knn = None;
        let response = executor.knn(request).await;
        assert!(response.is_err());
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().to_lowercase().contains("knn operator"),
            "{}",
            err.message()
        );
    }

    #[tokio::test]
    async fn validate_knn_plan_projection() {
        let mut executor = QueryExecutorClient::connect(run_server()).await.unwrap();
        let mut request = gen_knn_request(None);
        request.projection = None;
        let response = executor.knn(request).await;
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().to_lowercase().contains("projection operator"),
            "{}",
            err.message()
        );

        let mut request = gen_knn_request(None);
        request.projection = Some(chroma_proto::KnnProjectionOperator {
            projection: None,
            distance: false,
        });
        let response = executor.knn(request).await;
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message()
                .to_lowercase()
                .contains("projection operator: "),
            "{}",
            err.message()
        );
    }

    #[tokio::test]
    async fn validate_knn_plan_scan() {
        let mut executor = QueryExecutorClient::connect(run_server()).await.unwrap();
        let mut request = gen_knn_request(None);
        request.scan = None;
        let response = executor.knn(request).await;
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().to_lowercase().contains("scan operator"),
            "{}",
            err.message()
        );
    }

    #[tokio::test]
    async fn validate_knn_plan_scan_collection() {
        let mut executor = QueryExecutorClient::connect(run_server()).await.unwrap();
        let mut scan = scan();
        scan.collection.as_mut().unwrap().id = "invalid-collection-id".to_string();
        let response = executor.knn(gen_knn_request(Some(scan))).await;
        assert!(response.is_err());
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn validate_knn_plan_scan_vector() {
        let mut executor = QueryExecutorClient::connect(run_server()).await.unwrap();
        // invalid vector uuid
        let mut scan_operator = scan();
        scan_operator.knn = Some(chroma_proto::Segment {
            id: "invalid-knn-segment-id".to_string(),
            r#type: "urn:chroma:segment/vector/hnsw-distributed".to_string(),
            scope: 0,
            collection: scan_operator
                .collection
                .as_ref()
                .expect("The collection should exist")
                .id
                .clone(),
            metadata: None,
            file_paths: HashMap::new(),
        });
        let response = executor.knn(gen_knn_request(Some(scan_operator))).await;
        assert!(response.is_err());
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn validate_knn_plan_scan_record() {
        let mut executor = QueryExecutorClient::connect(run_server()).await.unwrap();
        let mut scan_operator = scan();
        scan_operator.record = Some(chroma_proto::Segment {
            id: "invalid-record-segment-id".to_string(),
            r#type: "urn:chroma:segment/record/blockfile".to_string(),
            scope: 2,
            collection: scan_operator
                .collection
                .as_ref()
                .expect("The collection should exist")
                .id
                .clone(),
            metadata: None,
            file_paths: HashMap::new(),
        });
        let response = executor.knn(gen_knn_request(Some(scan_operator))).await;
        assert!(response.is_err());
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn validate_knn_plan_scan_metadata() {
        let mut executor = QueryExecutorClient::connect(run_server()).await.unwrap();
        let mut scan_operator = scan();
        scan_operator.metadata = Some(chroma_proto::Segment {
            id: "invalid-metadata-segment-id".to_string(),
            r#type: "urn:chroma:segment/metadata/blockfile".to_string(),
            scope: 1,
            collection: scan_operator
                .collection
                .as_ref()
                .expect("The collection should exist")
                .id
                .clone(),
            metadata: None,
            file_paths: HashMap::new(),
        });
        let response = executor.knn(gen_knn_request(Some(scan_operator))).await;
        assert!(response.is_err());
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
