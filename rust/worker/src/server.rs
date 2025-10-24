use std::{collections::HashSet, time::Duration};

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_jemalloc_pprof_server::spawn_pprof_server;
use chroma_log::Log;
use chroma_segment::spann_provider::SpannProvider;
use chroma_storage::Storage;
use chroma_sysdb::SysDb;
use chroma_system::{ComponentHandle, Dispatcher, Orchestrator, System};
use chroma_types::{
    chroma_proto::{
        self,
        query_executor_server::{QueryExecutor, QueryExecutorServer},
    },
    operator::{GetResult, Knn, KnnBatch, KnnBatchResult, KnnProjection, QueryVector, Scan},
    plan::SearchPayload,
    CollectionAndSegments, SegmentType,
};
use futures::{stream, StreamExt, TryStreamExt};
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Request, Response, Status};

use crate::{
    config::QueryServiceConfig,
    execution::{
        operators::fetch_log::FetchLogOperator,
        orchestration::{
            get::GetOrchestrator,
            knn::KnnOrchestrator,
            knn_filter::KnnFilterOrchestrator,
            projection::ProjectionOrchestrator,
            rank::{RankOrchestrator, RankOrchestratorOutput},
            spann_knn::SpannKnnOrchestrator,
            sparse_knn::SparseKnnOrchestrator,
            CountOrchestrator,
        },
    },
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
    spann_provider: SpannProvider,
    port: u16,
    jemalloc_pprof_server_port: Option<u16>,
    // config
    fetch_log_batch_size: u32,
    shutdown_grace_period: Duration,
    bm25_tenant: HashSet<String>,
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
        let spann_provider = SpannProvider::try_from_config(
            &(
                hnsw_index_provider.clone(),
                blockfile_provider.clone(),
                config.spann_provider.clone(),
            ),
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
            spann_provider,
            port: config.my_port,
            jemalloc_pprof_server_port: config.jemalloc_pprof_server_port,
            fetch_log_batch_size: config.fetch_log_batch_size,
            shutdown_grace_period: config.grpc_shutdown_grace_period,
            bm25_tenant: config.bm25_tenant.clone(),
        })
    }
}

impl WorkerServer {
    pub(crate) async fn run(worker: WorkerServer) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("[::]:{}", worker.port).parse().unwrap();
        println!("Worker listening on {}", addr);

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

        let server = Server::builder()
            .layer(chroma_tracing::GrpcServerTraceLayer)
            .add_service(health_service)
            .add_service(QueryExecutorServer::new(worker.clone()));

        // Start pprof server
        let mut pprof_shutdown_tx = None;
        if let Some(port) = worker.jemalloc_pprof_server_port {
            tracing::info!("Starting jemalloc pprof server on port {}", port);
            let shutdown_channel = tokio::sync::oneshot::channel();
            pprof_shutdown_tx = Some(shutdown_channel.0);
            spawn_pprof_server(port, shutdown_channel.1).await;
        }

        #[cfg(debug_assertions)]
        let server = server.add_service(
            chroma_types::chroma_proto::debug_server::DebugServer::new(worker.clone()),
        );

        let shutdown_grace_period = worker.shutdown_grace_period;
        let server = server.serve_with_shutdown(addr, async {
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(sigterm) => sigterm,
                Err(e) => {
                    tracing::error!("Failed to create signal handler: {:?}", e);
                    return;
                }
            };
            sigterm.recv().await;
            tracing::info!("Received SIGTERM, waiting for grace period...");
            // Note: gRPC calls can still be successfully made during this period. We rely on the memberlist updating to stop clients from sending new requests. Ideally there would be a Tower layer that rejected new requests during this period with UNAVAILABLE or similar.
            tokio::time::sleep(shutdown_grace_period).await;
            tracing::info!("Grace period ended, shutting down server...");
        });

        tokio::spawn(async move {
            // Poll is-ready every ms until the server is ready
            // We don't timeout here because we assume some upstream daemon like
            // system will kill/restart us if we don't become ready in a reasonable time
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(1));
            loop {
                interval.tick().await;
                if worker.is_ready() {
                    break;
                }
            }
            health_reporter
                .set_serving::<QueryExecutorServer<WorkerServer>>()
                .await;
        });

        server.await?;

        // Shutdown pprof server after server is finished shutting down
        if let Some(shutdown_tx) = pprof_shutdown_tx {
            let _ = shutdown_tx.send(());
        }

        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.log.is_ready()
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
        count: Request<chroma_proto::CountPlan>,
    ) -> Result<Response<chroma_proto::CountResult>, Status> {
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
            Ok((count, pulled_log_bytes)) => Ok(Response::new(chroma_proto::CountResult {
                count,
                pulled_log_bytes,
            })),
            Err(err) => Err(Status::new(err.code().into(), err.to_string())),
        }
    }

    async fn orchestrate_get(
        &self,
        get: Request<chroma_proto::GetPlan>,
    ) -> Result<Response<chroma_proto::GetResult>, Status> {
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
            Ok(GetResult {
                pulled_log_bytes,
                result,
            }) => Ok(Response::new(chroma_proto::GetResult {
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
        knn: Request<chroma_proto::KnnPlan>,
    ) -> Result<Response<chroma_proto::KnnBatchResult>, Status> {
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
        let knn_projection = KnnProjection::try_from(projection)
            .map_err(|e| Status::invalid_argument(format!("Invalid Projection Operator: {}", e)))?;

        if knn.embeddings.is_empty() {
            return Ok(Response::new(KnnBatchResult::default().try_into()?));
        }

        // We return early on uninitialized collection, otherwise
        // the downstream will error due to missing dimension
        if collection_and_segments.is_uninitialized() {
            return Ok(Response::new(
                KnnBatchResult {
                    pulled_log_bytes: 0,
                    results: vec![Default::default(); knn.embeddings.len()],
                }
                .try_into()?,
            ));
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
            // Create unified futures that run KNN then projection
            let knn_with_projection_futures =
                Vec::from(KnnBatch::try_from(knn)?).into_iter().map(|knn| {
                    let spann_provider = self.spann_provider.clone();
                    let dispatcher = dispatcher.clone();
                    let collection_and_segments = collection_and_segments.clone();
                    let matching_records = matching_records.clone();
                    let system = system.clone();
                    let blockfile_provider = self.blockfile_provider.clone();
                    let knn_projection = knn_projection.clone();

                    async move {
                        // Run KNN orchestrator
                        let knn_orchestrator = SpannKnnOrchestrator::new(
                            spann_provider,
                            dispatcher.clone(),
                            1000,
                            collection_and_segments.clone(),
                            matching_records.clone(),
                            knn.fetch as usize,
                            knn.embedding,
                        );
                        let record_distances = knn_orchestrator
                            .run(system.clone())
                            .await
                            .map_err(|e| Status::new(e.code().into(), e.to_string()))?;

                        // Run projection orchestrator
                        let projection_orchestrator = ProjectionOrchestrator::new(
                            dispatcher,
                            1000,
                            blockfile_provider,
                            matching_records.logs.clone(),
                            collection_and_segments.record_segment.clone(),
                            record_distances,
                            knn_projection,
                        );
                        projection_orchestrator
                            .run(system)
                            .await
                            .map_err(|e| Status::new(e.code().into(), e.to_string()))
                    }
                });

            match stream::iter(knn_with_projection_futures)
                .buffered(32)
                .try_collect::<Vec<_>>()
                .await
            {
                Ok(results) => Ok(Response::new(
                    KnnBatchResult {
                        pulled_log_bytes,
                        results,
                    }
                    .try_into()?,
                )),
                Err(err) => Err(err),
            }
        } else {
            // Create unified futures that run KNN then projection
            let knn_with_projection_futures =
                Vec::from(KnnBatch::try_from(knn)?).into_iter().map(|knn| {
                    let blockfile_provider = self.blockfile_provider.clone();
                    let dispatcher = dispatcher.clone();
                    let collection_and_segments = collection_and_segments.clone();
                    let matching_records = matching_records.clone();
                    let system = system.clone();
                    let knn_projection = knn_projection.clone();

                    async move {
                        // Run KNN orchestrator
                        let knn_orchestrator = KnnOrchestrator::new(
                            blockfile_provider.clone(),
                            dispatcher.clone(),
                            // TODO: Make this configurable
                            1000,
                            collection_and_segments.clone(),
                            matching_records.clone(),
                            knn,
                        );
                        let record_distances = knn_orchestrator
                            .run(system.clone())
                            .await
                            .map_err(|e| Status::new(e.code().into(), e.to_string()))?;

                        // Run projection orchestrator
                        let projection_orchestrator = ProjectionOrchestrator::new(
                            dispatcher,
                            1000,
                            blockfile_provider,
                            matching_records.logs.clone(),
                            collection_and_segments.record_segment.clone(),
                            record_distances,
                            knn_projection,
                        );
                        projection_orchestrator
                            .run(system)
                            .await
                            .map_err(|e| Status::new(e.code().into(), e.to_string()))
                    }
                });

            match stream::iter(knn_with_projection_futures)
                .buffered(32)
                .try_collect::<Vec<_>>()
                .await
            {
                Ok(results) => Ok(Response::new(
                    KnnBatchResult {
                        pulled_log_bytes,
                        results,
                    }
                    .try_into()?,
                )),
                Err(err) => Err(err),
            }
        }
    }

    async fn orchestrate_search(
        &self,
        scan: chroma_proto::ScanOperator,
        payload: chroma_proto::SearchPayload,
    ) -> Result<RankOrchestratorOutput, Status> {
        let collection_and_segments = Scan::try_from(scan)?.collection_and_segments;
        let search_payload = SearchPayload::try_from(payload)?;
        let fetch_log = self.fetch_log(&collection_and_segments, self.fetch_log_batch_size);

        // We return early on uninitialized collection, otherwise
        // the downstream will error due to missing dimension
        if collection_and_segments.is_uninitialized() {
            return Ok(RankOrchestratorOutput::default());
        }

        let knn_filter_orchestrator = KnnFilterOrchestrator::new(
            self.blockfile_provider.clone(),
            self.clone_dispatcher()?,
            self.hnsw_index_provider.clone(),
            1000, // TODO: Make this configurable
            collection_and_segments.clone(),
            fetch_log,
            search_payload.filter.clone(),
        );

        let knn_filter_output = match knn_filter_orchestrator.run(self.system.clone()).await {
            Ok(output) => output,
            Err(e) => {
                return Err(Status::new(e.code().into(), e.to_string()));
            }
        };

        let knn_queries = search_payload.rank.knn_queries();
        let mut knn_futures = Vec::with_capacity(knn_queries.len());

        for knn_query in knn_queries {
            let knn_filter_output_clone = knn_filter_output.clone();
            let collection_and_segments_clone = collection_and_segments.clone();
            let system_clone = self.system.clone();
            let dispatcher = self.clone_dispatcher()?;
            let blockfile_provider = self.blockfile_provider.clone();
            let spann_provider = self.spann_provider.clone();

            knn_futures.push(async move {
                let result = match knn_query.query {
                    QueryVector::Dense(query) => {
                        // Check segment type to decide between HNSW and SPANN
                        let vector_segment_type =
                            collection_and_segments_clone.vector_segment.r#type;

                        if vector_segment_type == SegmentType::Spann {
                            // Use SPANN KNN orchestrator
                            let spann_orchestrator = SpannKnnOrchestrator::new(
                                spann_provider,
                                dispatcher,
                                1000,
                                collection_and_segments_clone,
                                knn_filter_output_clone,
                                knn_query.limit as usize,
                                query,
                            );

                            spann_orchestrator
                                .run(system_clone)
                                .await
                                .map_err(|e| Status::new(e.code().into(), e.to_string()))?
                        } else {
                            // Use HNSW KNN orchestrator
                            let knn = Knn {
                                embedding: query,
                                fetch: knn_query.limit,
                            };

                            let knn_orchestrator = KnnOrchestrator::new(
                                blockfile_provider,
                                dispatcher,
                                1000,
                                collection_and_segments_clone,
                                knn_filter_output_clone,
                                knn,
                            );

                            knn_orchestrator
                                .run(system_clone)
                                .await
                                .map_err(|e| Status::new(e.code().into(), e.to_string()))?
                        }
                    }
                    QueryVector::Sparse(query) => {
                        // Use Sparse KNN orchestrator
                        let tenant = collection_and_segments_clone.collection.tenant.clone();
                        let sparse_orchestrator = SparseKnnOrchestrator::new(
                            blockfile_provider,
                            dispatcher,
                            1000,
                            collection_and_segments_clone,
                            self.bm25_tenant.contains(&tenant),
                            knn_filter_output_clone,
                            query,
                            knn_query.key.to_string(),
                            knn_query.limit,
                        );

                        sparse_orchestrator
                            .run(system_clone)
                            .await
                            .map_err(|e| Status::new(e.code().into(), e.to_string()))?
                    }
                };

                Ok::<_, Status>(result)
            });
        }

        let knn_results = stream::iter(knn_futures)
            .buffered(32)
            .try_collect::<Vec<_>>()
            .await?;

        // Run RankOrchestrator to evaluate ranks and select results
        let rank_orchestrator = RankOrchestrator::new(
            self.blockfile_provider.clone(),
            self.clone_dispatcher()?,
            1000, // TODO: Make this configurable
            knn_filter_output,
            knn_results,
            search_payload.rank,
            search_payload.limit,
            search_payload.select,
            collection_and_segments,
        );

        rank_orchestrator
            .run(self.system.clone())
            .await
            .map_err(|err| Status::new(err.code().into(), err.to_string()))
    }

    async fn orchestrate_search_batch(
        &self,
        search: Request<chroma_proto::SearchPlan>,
    ) -> Result<Response<chroma_proto::SearchResult>, Status> {
        let search_plan = search.into_inner();
        let scan = search_plan
            .scan
            .ok_or(Status::invalid_argument("Invalid Scan Operator"))?;

        let futures = search_plan
            .payloads
            .into_iter()
            .map(|payload| self.orchestrate_search(scan.clone(), payload));

        let orchestrator_results = stream::iter(futures)
            .buffered(32) // Process up to 32 payloads concurrently
            .try_collect::<Vec<_>>()
            .await?;
        let (results, pulled_log_bytes) = orchestrator_results
            .into_iter()
            .map(|output| (output.result, output.pulled_log_bytes))
            .unzip::<_, _, Vec<_>, Vec<_>>();

        Ok(Response::new(chroma_proto::SearchResult {
            results: results
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            pulled_log_bytes: pulled_log_bytes.into_iter().max().unwrap_or_default(),
        }))
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
    async fn count(
        &self,
        count: Request<chroma_proto::CountPlan>,
    ) -> Result<Response<chroma_proto::CountResult>, Status> {
        self.orchestrate_count(count).await
    }

    async fn get(
        &self,
        get: Request<chroma_proto::GetPlan>,
    ) -> Result<Response<chroma_proto::GetResult>, Status> {
        self.orchestrate_get(get).await
    }

    async fn knn(
        &self,
        knn: Request<chroma_proto::KnnPlan>,
    ) -> Result<Response<chroma_proto::KnnBatchResult>, Status> {
        self.orchestrate_knn(knn).await
    }

    async fn search(
        &self,
        request: Request<chroma_proto::SearchPlan>,
    ) -> Result<Response<chroma_proto::SearchResult>, Status> {
        self.orchestrate_search_batch(request).await
    }
}

#[cfg(debug_assertions)]
#[async_trait]
impl chroma_types::chroma_proto::debug_server::Debug for WorkerServer {
    async fn get_info(
        &self,
        _request: Request<()>,
    ) -> Result<Response<chroma_types::chroma_proto::GetInfoResponse>, Status> {
        let response = chroma_types::chroma_proto::GetInfoResponse {
            version: option_env!("CARGO_PKG_VERSION")
                .unwrap_or("unknown")
                .to_string(),
        };
        Ok(Response::new(response))
    }

    async fn trigger_panic(&self, _request: Request<()>) -> Result<Response<()>, Status> {
        panic!("Intentional panic triggered");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use chroma_log::in_memory_log::InMemoryLog;
    use chroma_segment::test::TestDistributedSegment;
    use chroma_sysdb::TestSysDb;
    use chroma_system::DispatcherConfig;
    use chroma_types::chroma_proto;
    #[cfg(debug_assertions)]
    use chroma_types::chroma_proto::debug_client::DebugClient;
    use chroma_types::chroma_proto::query_executor_client::QueryExecutorClient;
    use uuid::Uuid;

    async fn run_server() -> String {
        let sysdb = TestSysDb::new();
        let system = System::new();
        let log = InMemoryLog::new();
        let segments = TestDistributedSegment::new().await;
        let port = random_port::PortPicker::new().random(true).pick().unwrap();

        let mut server = WorkerServer {
            dispatcher: None,
            system: system.clone(),
            _sysdb: SysDb::Test(sysdb),
            log: Log::InMemory(log),
            hnsw_index_provider: segments.hnsw_provider,
            blockfile_provider: segments.blockfile_provider,
            spann_provider: segments.spann_provider,
            port,
            jemalloc_pprof_server_port: None,
            fetch_log_batch_size: 100,
            shutdown_grace_period: Duration::from_secs(1),
            bm25_tenant: HashSet::new(),
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
        let database_id = Uuid::new_v4().to_string();
        chroma_proto::ScanOperator {
            collection: Some(chroma_proto::Collection {
                id: collection_id.clone(),
                name: "test-collection".to_string(),
                configuration_json_str: "{}".to_string(),
                metadata: None,
                dimension: None,
                tenant: "test-tenant".to_string(),
                database: "test-database".to_string(),
                database_id: Some(database_id.clone()),
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
        let mut client = DebugClient::connect(run_server().await).await.unwrap();

        // Test response when handler panics
        let err_response = client.trigger_panic(Request::new(())).await.unwrap_err();
        assert_eq!(err_response.code(), tonic::Code::Cancelled);

        // The server should still work, even after a panic was thrown
        let response = client.get_info(Request::new(())).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn validate_count_plan() {
        let mut executor = QueryExecutorClient::connect(run_server().await)
            .await
            .unwrap();
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
        let mut executor = QueryExecutorClient::connect(run_server().await)
            .await
            .unwrap();
        let mut scan_operator = scan();
        let request = chroma_proto::GetPlan {
            scan: Some(scan_operator.clone()),
            filter: None,
            limit: Some(chroma_proto::LimitOperator {
                offset: 0,
                limit: None,
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
                offset: 0,
                limit: None,
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
        let mut executor = QueryExecutorClient::connect(run_server().await)
            .await
            .unwrap();
        let response = executor.knn(gen_knn_request(None)).await;
        assert!(response.is_ok());
        assert_eq!(response.unwrap().into_inner().results.len(), 0);
    }

    #[tokio::test]
    async fn validate_knn_plan_filter() {
        let mut executor = QueryExecutorClient::connect(run_server().await)
            .await
            .unwrap();
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
        let mut executor = QueryExecutorClient::connect(run_server().await)
            .await
            .unwrap();
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
        let mut executor = QueryExecutorClient::connect(run_server().await)
            .await
            .unwrap();
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
        let mut executor = QueryExecutorClient::connect(run_server().await)
            .await
            .unwrap();
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
        let mut executor = QueryExecutorClient::connect(run_server().await)
            .await
            .unwrap();
        let mut scan = scan();
        scan.collection.as_mut().unwrap().id = "invalid-collection-id".to_string();
        let response = executor.knn(gen_knn_request(Some(scan))).await;
        assert!(response.is_err());
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn validate_knn_plan_scan_vector() {
        let mut executor = QueryExecutorClient::connect(run_server().await)
            .await
            .unwrap();
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
        let mut executor = QueryExecutorClient::connect(run_server().await)
            .await
            .unwrap();
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
        let mut executor = QueryExecutorClient::connect(run_server().await)
            .await
            .unwrap();
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
