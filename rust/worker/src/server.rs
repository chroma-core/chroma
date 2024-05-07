use std::path::PathBuf;

use crate::blockstore::provider::BlockfileProvider;
use crate::chroma_proto;
use crate::chroma_proto::{
    GetVectorsRequest, GetVectorsResponse, QueryVectorsRequest, QueryVectorsResponse,
};
use crate::config::{Configurable, QueryServiceConfig};
use crate::errors::ChromaError;
use crate::execution::operator::TaskMessage;
use crate::execution::orchestration::HnswQueryOrchestrator;
use crate::index::hnsw_provider::HnswIndexProvider;
use crate::log::log::Log;
use crate::sysdb::sysdb::SysDb;
use crate::system::{Receiver, System};
use crate::types::ScalarEncoding;
use async_trait::async_trait;
use opentelemetry::trace::{
    FutureExt, SpanContext, SpanId, SpanRef, TraceContextExt, TraceFlags, TraceId, TraceState,
};
use opentelemetry::Context;
use tonic::{metadata::MetadataMap, transport::Server, Request, Response, Status};
use tracing::{debug, trace, trace_span, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

pub struct WorkerServer {
    // System
    system: Option<System>,
    // Component dependencies
    dispatcher: Option<Box<dyn Receiver<TaskMessage>>>,
    // Service dependencies
    log: Box<dyn Log>,
    sysdb: Box<dyn SysDb>,
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
        let _server = Server::builder()
            .add_service(chroma_proto::vector_reader_server::VectorReaderServer::new(
                worker,
            ))
            .serve(addr)
            .await?;
        println!("Worker shutting down");

        Ok(())
    }

    pub(crate) fn set_dispatcher(&mut self, dispatcher: Box<dyn Receiver<TaskMessage>>) {
        self.dispatcher = Some(dispatcher);
    }

    pub(crate) fn set_system(&mut self, system: System) {
        self.system = Some(system);
    }

    pub(crate) fn try_parse_tracecontext(
        &self,
        metadata: &MetadataMap,
    ) -> (Option<TraceId>, Option<SpanId>) {
        let mut traceid: Option<TraceId> = None;
        let mut spanid: Option<SpanId> = None;
        if metadata.contains_key("chroma-traceid") {
            let id_res = metadata.get("chroma-traceid").unwrap().to_str();
            // Failure is not fatal.
            match id_res {
                Ok(id) => {
                    let trace_id = TraceId::from_hex(id);
                    match trace_id {
                        Ok(id) => traceid = Some(id),
                        Err(_) => traceid = None,
                    }
                }
                Err(_) => traceid = None,
            }
        }
        if metadata.contains_key("chroma-spanid") {
            let id_res = metadata.get("chroma-spanid").unwrap().to_str();
            // Failure is not fatal.
            match id_res {
                Ok(id) => {
                    let span_id = SpanId::from_hex(id);
                    match span_id {
                        Ok(id) => spanid = Some(id),
                        Err(_) => spanid = None,
                    }
                }
                Err(_) => spanid = None,
            }
        }
        (traceid, spanid)
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
}

#[tonic::async_trait]
impl chroma_proto::vector_reader_server::VectorReader for WorkerServer {
    async fn get_vectors(
        &self,
        request: Request<GetVectorsRequest>,
    ) -> Result<Response<GetVectorsResponse>, Status> {
        let request = request.into_inner();
        let _segment_uuid = match Uuid::parse_str(&request.segment_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return Err(Status::invalid_argument("Invalid UUID"));
            }
        };

        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn query_vectors(
        &self,
        request: Request<QueryVectorsRequest>,
    ) -> Result<Response<QueryVectorsResponse>, Status> {
        let (traceid, spanid) = self.try_parse_tracecontext(request.metadata());
        let query_span = trace_span!(
            "Query vectors",
            k = request.get_ref().k,
            segment_id = request.get_ref().segment_id,
            include_embeddings = request.get_ref().include_embeddings,
            allowed_ids = ?request.get_ref().allowed_ids
        );
        // Attach context passed by FE as parent.
        if traceid.is_some() && spanid.is_some() {
            let span_context = SpanContext::new(
                traceid.unwrap(),
                spanid.unwrap(),
                TraceFlags::new(1),
                true,
                TraceState::default(),
            );
            let context = query_span
                .context()
                .with_remote_span_context(span_context)
                .clone();
            query_span.set_parent(context);
        }
        self.query_vectors_instrumented(request)
            .instrument(query_span)
            .await
    }
}
