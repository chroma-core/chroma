use std::iter::once;

use crate::config::QueryServiceConfig;
use crate::execution::dispatcher::Dispatcher;
use crate::execution::operators::fetch_log::FetchLogOperator;
use crate::execution::operators::fetch_segment::FetchSegmentOperator;
use crate::execution::operators::filter::FilterOperator;
use crate::execution::operators::knn::KnnOperator;
use crate::execution::operators::knn_projection::KnnProjectionOperator;
use crate::execution::operators::limit::LimitOperator;
use crate::execution::operators::projection::ProjectionOperator;
use crate::execution::orchestration::get::GetOrchestrator;
use crate::execution::orchestration::knn::{KnnError, KnnFilterOrchestrator, KnnOrchestrator};
use crate::execution::orchestration::{CountQueryOrchestrator, GetVectorsOrchestrator};
use crate::log::log::Log;
use crate::sysdb::sysdb::SysDb;
use crate::system::{ComponentHandle, System};
use crate::tracing::util::wrap_span_with_parent_context;
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_types::chroma_proto::{
    self, CountRecordsRequest, CountRecordsResponse, QueryMetadataRequest, QueryMetadataResponse,
};
use chroma_types::chroma_proto::{
    GetVectorsRequest, GetVectorsResponse, QueryVectorsRequest, QueryVectorsResponse,
};
use chroma_types::{CollectionUuid, MetadataValue, ScalarEncoding, SegmentUuid, Where};
use futures::future::try_join_all;
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{trace_span, Instrument};
use uuid::Uuid;

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

#[async_trait]
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
        let server = Server::builder()
            .add_service(chroma_proto::vector_reader_server::VectorReaderServer::new(
                worker.clone(),
            ))
            .add_service(
                chroma_proto::metadata_reader_server::MetadataReaderServer::new(worker.clone()),
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

    pub(crate) async fn query_vectors_instrumented(
        &self,
        request: Request<QueryVectorsRequest>,
    ) -> Result<Response<QueryVectorsResponse>, Status> {
        let request = request.into_inner();
        let segment_uuid = match Uuid::parse_str(&request.segment_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return Err(Status::invalid_argument("Invalid Segment UUID"));
            }
        };

        let collection_uuid = match Uuid::parse_str(&request.collection_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return Err(Status::invalid_argument("Invalid Collection UUID"));
            }
        };
        let collection_uuid = CollectionUuid(collection_uuid);

        let (collection_version, log_position) = match request.version_context {
            Some(version_context) => (
                version_context.collection_version,
                version_context.log_position,
            ),
            None => {
                return Err(Status::invalid_argument("No version context provided"));
            }
        };

        let mut proto_results_for_all = Vec::new();

        let mut query_vectors = Vec::new();
        for proto_query_vector in request.vectors {
            let (query_vector, _encoding) = match proto_query_vector.try_into() {
                Ok((vector, encoding)) => (vector, encoding),
                Err(e) => {
                    return Err(Status::internal(format!("Error converting vector: {}", e)));
                }
            };
            query_vectors.push(query_vector);
        }

        let dispatcher = match self.dispatcher {
            Some(ref dispatcher) => dispatcher.clone(),
            None => {
                return Err(Status::internal("No dispatcher found"));
            }
        };

        let system = match self.system {
            Some(ref system) => system,
            None => {
                tracing::error!("No system found");
                return Err(Status::internal("No system found"));
            }
        };

        let knn_filter_orchestrator = KnnFilterOrchestrator::new(
            self.blockfile_provider.clone(),
            dispatcher.clone(),
            // TODO: Load the configuration for this
            1000,
            FetchLogOperator {
                log_client: self.log.clone(),
                batch_size: 100,
                start_log_offset_id: log_position as u32 + 1,
                maximum_fetch_count: None,
                collection_uuid,
            },
            FetchSegmentOperator {
                sysdb: self.sysdb.clone(),
                collection_uuid,
                collection_version,
                metadata_uuid: None,
                record_uuid: None,
                vector_uuid: Some(SegmentUuid(segment_uuid)),
            },
            FilterOperator {
                query_ids: (!request.allowed_ids.is_empty()).then_some(request.allowed_ids),
                where_clause: None,
            },
        );

        let knn_filter_output = match knn_filter_orchestrator.run(system.clone()).await {
            Ok(output) => output,
            Err(KnnError::EmptyCollection) => {
                return Ok(Response::new(chroma_proto::QueryVectorsResponse {
                    results: once(chroma_proto::VectorQueryResults {
                        results: Vec::new(),
                    })
                    .cycle()
                    .take(query_vectors.len())
                    .collect(),
                }));
            }
            Err(e) => {
                tracing::error!("Error running orchestrator: {}", e);
                return Err(Status::new(
                    e.code().into(),
                    format!("Error running orchestrator: {}", e),
                ));
            }
        };

        let embedding_dim = query_vectors[0].len();

        let knn_orchestrators: Vec<_> = query_vectors
            .into_iter()
            .map(|embedding| {
                KnnOrchestrator::new(
                    self.blockfile_provider.clone(),
                    dispatcher.clone(),
                    self.hnsw_index_provider.clone(),
                    // TODO: Load the configuration for this
                    1000,
                    knn_filter_output.clone(),
                    KnnOperator {
                        embedding,
                        fetch: request.k as u32,
                    },
                    KnnProjectionOperator {
                        projection: ProjectionOperator {
                            document: false,
                            embedding: request.include_embeddings,
                            metadata: false,
                        },
                        distance: true,
                    },
                )
            })
            .collect();

        let result = try_join_all(
            knn_orchestrators
                .into_iter()
                .map(|knn| knn.run(system.clone())),
        )
        .await;

        let result = match result {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("Error running orchestrator: {}", e);
                return Err(Status::new(
                    e.code().into(),
                    format!("Error running orchestrator: {}", e),
                ));
            }
        };

        for result_set in result {
            let mut proto_results = Vec::new();
            for query_result in result_set.records {
                let proto_result = chroma_proto::VectorQueryResult {
                    id: query_result.record.id,
                    distance: query_result
                        .distance
                        .expect("The distance should be present"),
                    vector: match query_result.record.embedding {
                        Some(vector) => {
                            match (vector, ScalarEncoding::FLOAT32, embedding_dim).try_into() {
                                Ok(proto_vector) => Some(proto_vector),
                                Err(e) => {
                                    return Err(Status::internal(format!(
                                        "Error converting vector: {}",
                                        e
                                    )));
                                }
                            }
                        }
                        None => None,
                    },
                };
                proto_results.push(proto_result);
            }
            proto_results_for_all.push(chroma_proto::VectorQueryResults {
                results: proto_results,
            });
        }

        let resp = chroma_proto::QueryVectorsResponse {
            results: proto_results_for_all,
        };

        Ok(Response::new(resp))
    }

    async fn get_vectors_instrumented(
        &self,
        request: Request<GetVectorsRequest>,
    ) -> Result<Response<GetVectorsResponse>, Status> {
        let request = request.into_inner();
        let segment_uuid = match Uuid::parse_str(&request.segment_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return Err(Status::invalid_argument("Invalid Segment UUID"));
            }
        };

        let collection_uuid = match Uuid::parse_str(&request.collection_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return Err(Status::invalid_argument("Invalid Collection UUID"));
            }
        };
        let collection_uuid = CollectionUuid(collection_uuid);

        let (collection_version, log_position) = match request.version_context {
            Some(version_context) => (
                version_context.collection_version,
                version_context.log_position,
            ),
            None => {
                return Err(Status::invalid_argument("No version context provided"));
            }
        };

        let dispatcher = match self.dispatcher {
            Some(ref dispatcher) => dispatcher.clone(),
            None => {
                return Err(Status::internal("No dispatcher found"));
            }
        };

        let system = match self.system {
            Some(ref system) => system,
            None => {
                return Err(Status::internal("No system found"));
            }
        };

        let orchestrator = GetVectorsOrchestrator::new(
            system.clone(),
            request.ids,
            segment_uuid,
            collection_uuid,
            self.log.clone(),
            self.sysdb.clone(),
            dispatcher,
            self.blockfile_provider.clone(),
            collection_version,
            log_position,
        );
        let result = orchestrator.run().await;
        let mut result = match result {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("Error running orchestrator: {}", e);
                return Err(Status::new(
                    e.code().into(),
                    format!("Error running orchestrator: {}", e),
                ));
            }
        };

        let mut output = Vec::new();
        let id_drain = result.ids.drain(..);
        let vector_drain = result.vectors.drain(..);

        for (id, vector) in id_drain.zip(vector_drain) {
            let vector_len = vector.len();
            let proto_vector = match (vector, ScalarEncoding::FLOAT32, vector_len).try_into() {
                Ok(vector) => vector,
                Err(_) => {
                    return Err(Status::internal("Error converting vector"));
                }
            };

            let proto_vector_record = chroma_proto::VectorEmbeddingRecord {
                id,
                vector: Some(proto_vector),
            };
            output.push(proto_vector_record);
        }

        let response = chroma_proto::GetVectorsResponse { records: output };
        Ok(Response::new(response))
    }

    async fn query_metadata_instrumented(
        &self,
        request: Request<QueryMetadataRequest>,
    ) -> Result<Response<QueryMetadataResponse>, Status> {
        let request = request.into_inner();
        let segment_uuid = match Uuid::parse_str(&request.segment_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                tracing::error!("Invalid Segment UUID");
                return Err(Status::invalid_argument("Invalid Segment UUID"));
            }
        };

        let collection_uuid = match Uuid::parse_str(&request.collection_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return Err(Status::invalid_argument("Invalid Collection UUID"));
            }
        };
        let collection_uuid = CollectionUuid(collection_uuid);

        let (collection_version, log_position) = match request.version_context {
            Some(version_context) => (
                version_context.collection_version,
                version_context.log_position,
            ),
            None => {
                return Err(Status::invalid_argument("No version context provided"));
            }
        };

        let dispatcher = match self.dispatcher {
            Some(ref dispatcher) => dispatcher,
            None => {
                tracing::error!("No dispatcher found");
                return Err(Status::internal("No dispatcher found"));
            }
        };

        let system = match self.system {
            Some(ref system) => system,
            None => {
                tracing::error!("No system found");
                return Err(Status::internal("No system found"));
            }
        };

        // If no ids are provided, pass None to the orchestrator
        let query_ids = request.ids.map(|uids| uids.ids);

        let where_clause = match request.r#where {
            Some(where_clause) => match where_clause.try_into() {
                Ok(where_clause) => Some(where_clause),
                Err(_) => {
                    tracing::error!("Error converting where clause");
                    return Err(Status::internal(
                        "Error converting where clause".to_string(),
                    ));
                }
            },
            None => None,
        };

        let where_document_clause = match request.where_document {
            Some(where_document_clause) => match where_document_clause.try_into() {
                Ok(where_document_clause) => Some(where_document_clause),
                Err(_) => {
                    tracing::error!("Error converting where document clause");
                    return Err(Status::internal(
                        "Error converting where document clause".to_string(),
                    ));
                }
            },
            None => None,
        };

        let clause = match (where_clause, where_document_clause) {
            (Some(wc), Some(wdc)) => Some(Where::conjunction(vec![wc, wdc])),
            (Some(c), None) | (None, Some(c)) => Some(c),
            _ => None,
        };

        let orchestrator = GetOrchestrator::new(
            self.blockfile_provider.clone(),
            dispatcher.clone(),
            // TODO: Load the configuration for this
            1000,
            FetchLogOperator {
                log_client: self.log.clone(),
                batch_size: 100,
                start_log_offset_id: log_position as u32 + 1,
                maximum_fetch_count: None,
                collection_uuid,
            },
            FetchSegmentOperator {
                sysdb: self.sysdb.clone(),
                vector_uuid: None,
                metadata_uuid: Some(SegmentUuid(segment_uuid)),
                record_uuid: None,
                collection_uuid,
                collection_version,
            },
            FilterOperator {
                query_ids,
                where_clause: clause,
            },
            LimitOperator {
                skip: request.offset.unwrap_or_default(),
                fetch: request.limit,
            },
            ProjectionOperator {
                document: request.include_metadata,
                embedding: request.include_metadata,
                metadata: request.include_metadata,
            },
        );

        let result = orchestrator.run(system.clone()).await;
        let result = match result {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("Error running orchestrator: {}", e);
                return Err(Status::new(
                    e.code().into(),
                    format!("Error running orchestrator: {}", e),
                ));
            }
        };

        let mut output = Vec::new();
        for record in result.records {
            let metadata = if request.include_metadata {
                let mut meta = record.metadata.unwrap_or_default();

                // The transport layer assumes the document exists in the metadata
                // with the special key "chroma:document"
                if let Some(doc) = record.document {
                    meta.insert("chroma:document".to_string(), MetadataValue::Str(doc));
                }
                Some(chroma_proto::UpdateMetadata::from(meta))
            } else {
                None
            };

            output.push(chroma_proto::MetadataEmbeddingRecord {
                id: record.id,
                metadata,
            });
        }

        // This is an implementation stub
        let response = chroma_proto::QueryMetadataResponse { records: output };
        Ok(Response::new(response))
    }
}

#[tonic::async_trait]
impl chroma_proto::vector_reader_server::VectorReader for WorkerServer {
    async fn get_vectors(
        &self,
        request: Request<GetVectorsRequest>,
    ) -> Result<Response<GetVectorsResponse>, Status> {
        // Note: We cannot write a middleware that instruments every service rpc
        // with a span because of https://github.com/hyperium/tonic/pull/1202.
        let request_span = trace_span!(
            "Get vectors",
            segment_id = request.get_ref().segment_id,
            ids = ?request.get_ref().ids
        );

        let instrumented_span = wrap_span_with_parent_context(request_span, request.metadata());
        self.get_vectors_instrumented(request)
            .instrument(instrumented_span)
            .await
    }

    async fn query_vectors(
        &self,
        request: Request<QueryVectorsRequest>,
    ) -> Result<Response<QueryVectorsResponse>, Status> {
        // Note: We cannot write a middleware that instruments every service rpc
        // with a span because of https://github.com/hyperium/tonic/pull/1202.
        let query_span = trace_span!(
            "Query vectors",
            k = request.get_ref().k,
            segment_id = request.get_ref().segment_id,
            include_embeddings = request.get_ref().include_embeddings,
            allowed_ids = ?request.get_ref().allowed_ids
        );
        let instrumented_span = wrap_span_with_parent_context(query_span, request.metadata());
        self.query_vectors_instrumented(request)
            .instrument(instrumented_span)
            .await
    }
}

#[tonic::async_trait]
impl chroma_proto::metadata_reader_server::MetadataReader for WorkerServer {
    async fn count_records(
        &self,
        request: Request<CountRecordsRequest>,
    ) -> Result<Response<CountRecordsResponse>, Status> {
        let request = request.into_inner();
        let segment_uuid = match Uuid::parse_str(&request.segment_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return Err(Status::invalid_argument("Invalid Segment UUID"));
            }
        };
        let collection_uuid = match Uuid::parse_str(&request.collection_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return Err(Status::invalid_argument("Invalid Collection UUID"));
            }
        };
        let collection_uuid = CollectionUuid(collection_uuid);

        let (collection_version, log_position) = match request.version_context {
            Some(version_context) => (
                version_context.collection_version,
                version_context.log_position,
            ),
            None => {
                return Err(Status::invalid_argument("No version context provided"));
            }
        };

        let dispatcher = match self.dispatcher {
            Some(ref dispatcher) => dispatcher,
            None => {
                return Err(Status::internal("No dispatcher found"));
            }
        };

        let system = match self.system {
            Some(ref system) => system,
            None => {
                return Err(Status::internal("No system found"));
            }
        };

        let orchestrator = CountQueryOrchestrator::new(
            system.clone(),
            &segment_uuid,
            &collection_uuid,
            self.log.clone(),
            self.sysdb.clone(),
            dispatcher.clone(),
            self.blockfile_provider.clone(),
            collection_version,
            log_position,
        );

        let result = orchestrator.run().await;
        let c = match result {
            Ok(r) => {
                println!("Count value {}", r);
                r
            }
            Err(e) => {
                println!("Error! {:?}", e);
                // TODO: Return 0 for now but should return an error at some point.
                0
            }
        };
        let response = CountRecordsResponse { count: c as u32 };
        Ok(Response::new(response))
    }

    async fn query_metadata(
        &self,
        request: Request<QueryMetadataRequest>,
    ) -> Result<Response<QueryMetadataResponse>, Status> {
        let query_span = trace_span!("Query metadata", segment_id = request.get_ref().segment_id);
        let instrumented_span = wrap_span_with_parent_context(query_span, request.metadata());
        self.query_metadata_instrumented(request)
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
    #[cfg(debug_assertions)]
    use super::*;
    #[cfg(debug_assertions)]
    use crate::execution::dispatcher;
    #[cfg(debug_assertions)]
    use crate::log::log::InMemoryLog;
    #[cfg(debug_assertions)]
    use crate::sysdb::test_sysdb::TestSysDb;
    #[cfg(debug_assertions)]
    use crate::system;
    #[cfg(debug_assertions)]
    use chroma_blockstore::arrow::config::TEST_MAX_BLOCK_SIZE_BYTES;
    #[cfg(debug_assertions)]
    use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
    #[cfg(debug_assertions)]
    use chroma_proto::debug_client::DebugClient;
    #[cfg(debug_assertions)]
    use chroma_storage::{local::LocalStorage, Storage};
    #[cfg(debug_assertions)]
    use tempfile::tempdir;

    #[tokio::test]
    #[cfg(debug_assertions)]
    async fn gracefully_handles_panics() {
        let sysdb = TestSysDb::new();
        let log = InMemoryLog::new();
        let tmp_dir = tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let hnsw_index_cache = new_non_persistent_cache_for_test();
        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let port = random_port::PortPicker::new().pick().unwrap();
        let mut server = WorkerServer {
            dispatcher: None,
            system: None,
            sysdb: Box::new(SysDb::Test(sysdb)),
            log: Box::new(Log::InMemory(log)),
            hnsw_index_provider: HnswIndexProvider::new(
                storage.clone(),
                tmp_dir.path().to_path_buf(),
                hnsw_index_cache,
                rx,
            ),
            blockfile_provider: BlockfileProvider::new_arrow(
                storage,
                TEST_MAX_BLOCK_SIZE_BYTES,
                block_cache,
                sparse_index_cache,
            ),
            port,
        };

        let system: system::System = system::System::new();
        let dispatcher = dispatcher::Dispatcher::new(4, 10, 10);
        let dispatcher_handle = system.start_component(dispatcher);

        server.set_system(system.clone());
        server.set_dispatcher(dispatcher_handle);

        tokio::spawn(async move {
            let _ = crate::server::WorkerServer::run(server).await;
        });

        let mut client = DebugClient::connect(format!("http://localhost:{}", port))
            .await
            .unwrap();

        // Test response when handler panics
        let err_response = client.trigger_panic(Request::new(())).await.unwrap_err();
        assert_eq!(err_response.code(), tonic::Code::Cancelled);

        // The server should still work, even after a panic was thrown
        let response = client.get_info(Request::new(())).await;
        assert!(response.is_ok());
    }
}
