use std::collections::HashMap;
use std::path::PathBuf;

use crate::blockstore::provider::BlockfileProvider;
use crate::chroma_proto::{
    self, CountRecordsRequest, CountRecordsResponse, QueryMetadataRequest, QueryMetadataResponse,
};
use crate::chroma_proto::{
    GetVectorsRequest, GetVectorsResponse, QueryVectorsRequest, QueryVectorsResponse,
};
use crate::config::{Configurable, QueryServiceConfig};
use crate::errors::ChromaError;
use crate::execution::operator::TaskMessage;
use crate::execution::orchestration::{
    CountQueryOrchestrator, GetVectorsOrchestrator, HnswQueryOrchestrator,
    MetadataQueryOrchestrator,
};
use crate::index::hnsw_provider::HnswIndexProvider;
use crate::log::log::Log;
use crate::sysdb::sysdb::SysDb;
use crate::system::{Receiver, System};
use crate::tracing::util::wrap_span_with_parent_context;
use crate::types::MetadataValue;
use crate::types::ScalarEncoding;
use async_trait::async_trait;
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{trace, trace_span, Instrument};
use uuid::Uuid;

#[derive(Clone)]
pub struct WorkerServer {
    // System
    system: Option<System>,
    // Component dependencies
    dispatcher: Option<Box<dyn Receiver<TaskMessage>>>,
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
                println!("Failed to create sysdb component: {:?}", err);
                return Err(err);
            }
        };
        let log_config = &config.log;
        let log = match crate::log::from_config(log_config).await {
            Ok(log) => log,
            Err(err) => {
                println!("Failed to create log component: {:?}", err);
                return Err(err);
            }
        };
        let storage = match crate::storage::from_config(&config.storage).await {
            Ok(storage) => storage,
            Err(err) => {
                println!("Failed to create storage component: {:?}", err);
                return Err(err);
            }
        };
        // TODO: inject hnsw index provider somehow
        // TODO: inject blockfile provider somehow
        // TODO: real path
        let path = PathBuf::from("~/tmp");
        Ok(WorkerServer {
            dispatcher: None,
            system: None,
            sysdb,
            log,
            hnsw_index_provider: HnswIndexProvider::new(storage.clone(), path),
            blockfile_provider: BlockfileProvider::new_arrow(storage),
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

    pub(crate) fn set_dispatcher(&mut self, dispatcher: Box<dyn Receiver<TaskMessage>>) {
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

        let mut proto_results_for_all = Vec::new();

        let parse_vectors_span = trace_span!("Input vectors parsing");
        let mut query_vectors = Vec::new();
        let _ = parse_vectors_span.in_scope(|| {
            for proto_query_vector in request.vectors {
                let (query_vector, _encoding) = match proto_query_vector.try_into() {
                    Ok((vector, encoding)) => (vector, encoding),
                    Err(e) => {
                        return Err(Status::internal(format!("Error converting vector: {}", e)));
                    }
                };
                query_vectors.push(query_vector);
            }
            trace!("Parsed vectors {:?}", query_vectors);
            Ok(())
        });

        let dispatcher = match self.dispatcher {
            Some(ref dispatcher) => dispatcher,
            None => {
                return Err(Status::internal("No dispatcher found"));
            }
        };

        let result = match self.system {
            Some(ref system) => {
                let orchestrator = HnswQueryOrchestrator::new(
                    // TODO: Should not have to clone query vectors here
                    system.clone(),
                    query_vectors.clone(),
                    request.k,
                    request.allowed_ids,
                    request.include_embeddings,
                    segment_uuid,
                    self.log.clone(),
                    self.sysdb.clone(),
                    self.hnsw_index_provider.clone(),
                    self.blockfile_provider.clone(),
                    dispatcher.clone(),
                );
                orchestrator.run().await
            }
            None => {
                return Err(Status::internal("No system found"));
            }
        };

        let result = match result {
            Ok(result) => result,
            Err(e) => {
                return Err(Status::internal(format!(
                    "Error running orchestrator: {}",
                    e
                )));
            }
        };

        for result_set in result {
            let mut proto_results = Vec::new();
            for query_result in result_set {
                let proto_result = chroma_proto::VectorQueryResult {
                    id: query_result.id,
                    distance: query_result.distance,
                    vector: match query_result.vector {
                        Some(vector) => {
                            match (vector, ScalarEncoding::FLOAT32, query_vectors[0].len())
                                .try_into()
                            {
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

        return Ok(Response::new(resp));
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

        let orchestrator = GetVectorsOrchestrator::new(
            system.clone(),
            request.ids,
            segment_uuid,
            self.log.clone(),
            self.sysdb.clone(),
            dispatcher.clone(),
            self.blockfile_provider.clone(),
        );
        let result = orchestrator.run().await;
        let mut result = match result {
            Ok(result) => result,
            Err(e) => {
                return Err(Status::internal(format!(
                    "Error running orchestrator: {}",
                    e
                )));
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

        // For now we don't support limit/offset/where/where document
        if request.limit.is_some() || request.offset.is_some() {
            tracing::error!("Limit and offset not supported");
            return Err(Status::unimplemented("Limit and offset not supported"));
        }

        // If no ids are provided, pass None to the orchestrator
        let query_ids = match request.ids.len() {
            0 => None,
            _ => Some(request.ids),
        };

        let where_clause = match request.r#where {
            Some(where_clause) => match where_clause.try_into() {
                Ok(where_clause) => Some(where_clause),
                Err(_) => {
                    tracing::error!("Error converting where clause");
                    return Err(Status::internal(format!("Error converting where clause",)));
                }
            },
            None => None,
        };

        let where_document_clause = match request.where_document {
            Some(where_document_clause) => match where_document_clause.try_into() {
                Ok(where_document_clause) => Some(where_document_clause),
                Err(_) => {
                    tracing::error!("Error converting where document clause");
                    return Err(Status::internal(format!(
                        "Error converting where document clause",
                    )));
                }
            },
            None => None,
        };

        let orchestrator = MetadataQueryOrchestrator::new(
            system.clone(),
            &segment_uuid,
            query_ids,
            self.log.clone(),
            self.sysdb.clone(),
            dispatcher.clone(),
            self.blockfile_provider.clone(),
            where_clause,
            where_document_clause,
        );

        let result = orchestrator.run().await;
        let result = match result {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("Error running orchestrator: {}", e);
                return Err(Status::internal(format!(
                    "Error running orchestrator: {}",
                    e
                )));
            }
        };

        let mut output = Vec::new();
        let (ids, metadatas, documents) = result;
        for ((id, metadata), document) in ids
            .into_iter()
            .zip(metadatas.into_iter())
            .zip(documents.into_iter())
        {
            // The transport layer assumes the document exists in the metadata
            // with the special key "chroma:document"
            let mut output_metadata = match metadata {
                Some(metadata) => metadata,
                None => HashMap::new(),
            };
            match document {
                Some(document) => {
                    output_metadata
                        .insert("chroma:document".to_string(), MetadataValue::Str(document));
                }
                None => {}
            }
            let record = chroma_proto::MetadataEmbeddingRecord {
                id,
                metadata: Some(chroma_proto::UpdateMetadata::from(output_metadata)),
            };
            output.push(record);
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
        println!("Querying count for segment {}", segment_uuid);
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
            self.log.clone(),
            self.sysdb.clone(),
            dispatcher.clone(),
            self.blockfile_provider.clone(),
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
    use crate::execution::dispatcher;
    use crate::log::log::InMemoryLog;
    use crate::storage::local::LocalStorage;
    use crate::storage::Storage;
    use crate::sysdb::test_sysdb::TestSysDb;
    use crate::system;

    use super::*;
    use chroma_proto::debug_client::DebugClient;
    use tempfile::tempdir;

    #[tokio::test]
    async fn gracefully_handles_panics() {
        let sysdb = TestSysDb::new();
        let log = InMemoryLog::new();
        let tmp_dir = tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

        let port = random_port::PortPicker::new().pick().unwrap();
        let mut server = WorkerServer {
            dispatcher: None,
            system: None,
            sysdb: Box::new(SysDb::Test(sysdb)),
            log: Box::new(Log::InMemory(log)),
            hnsw_index_provider: HnswIndexProvider::new(
                storage.clone(),
                tmp_dir.path().to_path_buf(),
            ),
            blockfile_provider: BlockfileProvider::new_arrow(storage),
            port,
        };

        let system: system::System = system::System::new();
        let dispatcher = dispatcher::Dispatcher::new(4, 10, 10);
        let dispatcher_handle = system.start_component(dispatcher);

        server.set_system(system.clone());
        server.set_dispatcher(dispatcher_handle.receiver());

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
