use crate::chroma_proto;
use crate::chroma_proto::{
    GetVectorsRequest, GetVectorsResponse, QueryVectorsRequest, QueryVectorsResponse,
};
use crate::config::{Configurable, WorkerConfig};
use crate::errors::ChromaError;
use crate::execution::operator::TaskMessage;
use crate::execution::orchestration::HnswQueryOrchestrator;
use crate::log::log::Log;
use crate::segment::SegmentManager;
use crate::sysdb::sysdb::SysDb;
use crate::system::{Receiver, System};
use crate::types::ScalarEncoding;
use async_trait::async_trait;
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;

pub struct WorkerServer {
    // System
    system: Option<System>,
    // Component dependencies
    segment_manager: Option<SegmentManager>,
    dispatcher: Option<Box<dyn Receiver<TaskMessage>>>,
    // Service dependencies
    log: Box<dyn Log>,
    sysdb: Box<dyn SysDb>,
    port: u16,
}

#[async_trait]
impl Configurable for WorkerServer {
    async fn try_from_config(config: &WorkerConfig) -> Result<Self, Box<dyn ChromaError>> {
        println!("Creating worker server from config");
        println!("Creating sysdb from config for worker server");
        let sysdb = match crate::sysdb::from_config(&config).await {
            Ok(sysdb) => sysdb,
            Err(err) => {
                return Err(err);
            }
        };
        println!("Creating log from config for worker server");
        let log = match crate::log::from_config(&config).await {
            Ok(log) => log,
            Err(err) => {
                return Err(err);
            }
        };
        Ok(WorkerServer {
            segment_manager: None,
            dispatcher: None,
            system: None,
            sysdb,
            log,
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
                worker,
            ))
            .serve(addr)
            .await?;
        println!("Worker shutting down");

        Ok(())
    }

    pub(crate) fn set_segment_manager(&mut self, segment_manager: SegmentManager) {
        self.segment_manager = Some(segment_manager);
    }

    pub(crate) fn set_dispatcher(&mut self, dispatcher: Box<dyn Receiver<TaskMessage>>) {
        self.dispatcher = Some(dispatcher);
    }

    pub(crate) fn set_system(&mut self, system: System) {
        self.system = Some(system);
    }
}

#[tonic::async_trait]
impl chroma_proto::vector_reader_server::VectorReader for WorkerServer {
    async fn get_vectors(
        &self,
        request: Request<GetVectorsRequest>,
    ) -> Result<Response<GetVectorsResponse>, Status> {
        let request = request.into_inner();
        let segment_uuid = match Uuid::parse_str(&request.segment_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return Err(Status::invalid_argument("Invalid UUID"));
            }
        };

        let segment_manager = match self.segment_manager {
            Some(ref segment_manager) => segment_manager,
            None => {
                return Err(Status::internal("No segment manager found"));
            }
        };

        let records = match segment_manager
            .get_records(&segment_uuid, request.ids)
            .await
        {
            Ok(records) => records,
            Err(e) => {
                return Err(Status::internal(format!("Error getting records: {}", e)));
            }
        };

        let mut proto_records = Vec::new();
        for record in records {
            let sed_id_bytes = record.seq_id.to_bytes_le();
            let dim = record.vector.len();
            let proto_vector = (record.vector, ScalarEncoding::FLOAT32, dim).try_into();
            match proto_vector {
                Ok(proto_vector) => {
                    let proto_record = chroma_proto::VectorEmbeddingRecord {
                        id: record.id,
                        seq_id: sed_id_bytes.1,
                        vector: Some(proto_vector),
                    };
                    proto_records.push(proto_record);
                }
                Err(e) => {
                    return Err(Status::internal(format!("Error converting vector: {}", e)));
                }
            }
        }

        let resp = chroma_proto::GetVectorsResponse {
            records: proto_records,
        };

        Ok(Response::new(resp))
    }

    async fn query_vectors(
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

        let segment_manager = match self.segment_manager {
            Some(ref segment_manager) => segment_manager,
            None => {
                return Err(Status::internal("No segment manager found"));
            }
        };

        let mut proto_results_for_all = Vec::new();

        let mut query_vectors = Vec::new();
        for proto_query_vector in request.vectors {
            let (query_vector, encoding) = match proto_query_vector.try_into() {
                Ok((vector, encoding)) => (vector, encoding),
                Err(e) => {
                    return Err(Status::internal(format!("Error converting vector: {}", e)));
                }
            };
            query_vectors.push(query_vector);
        }

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
                    dispatcher.clone(),
                );
                orchestrator.run().await
            }
            None => {
                return Err(Status::internal("No system found"));
            }
        };
        println!("Server recieved result: {:?}", result);

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
                    seq_id: query_result.seq_id.to_bytes_le().1,
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

        println!("Server sending response: {:?}", resp);
        return Ok(Response::new(resp));
    }
}
