use std::f32::consts::E;

use crate::chroma_proto;
use crate::chroma_proto::{
    GetVectorsRequest, GetVectorsResponse, QueryVectorsRequest, QueryVectorsResponse,
};
use crate::config::{Configurable, WorkerConfig};
use crate::errors::ChromaError;
use crate::segment::SegmentManager;
use crate::types::ScalarEncoding;
use async_trait::async_trait;
use kube::core::request;
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;

pub struct WorkerServer {
    segment_manager: Option<SegmentManager>,
    port: u16,
}

#[async_trait]
impl Configurable for WorkerServer {
    async fn try_from_config(config: &WorkerConfig) -> Result<Self, Box<dyn ChromaError>> {
        Ok(WorkerServer {
            segment_manager: None,
            port: config.my_port,
        })
    }
}

impl WorkerServer {
    pub(crate) async fn run(worker: WorkerServer) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("[::1]:{}", worker.port).parse().unwrap();
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
        for proto_query_vector in request.vectors {
            let (query_vector, encoding) = match proto_query_vector.try_into() {
                Ok((vector, encoding)) => (vector, encoding),
                Err(e) => {
                    return Err(Status::internal(format!("Error converting vector: {}", e)));
                }
            };

            let results = match segment_manager
                .query_vector(
                    &segment_uuid,
                    &query_vector,
                    request.k as usize,
                    request.include_embeddings,
                )
                .await
            {
                Ok(results) => results,
                Err(e) => {
                    return Err(Status::internal(format!("Error querying segment: {}", e)));
                }
            };

            let mut proto_results = Vec::new();
            for query_result in results {
                let proto_result = chroma_proto::VectorQueryResult {
                    id: query_result.id,
                    seq_id: query_result.seq_id.to_bytes_le().1,
                    distance: query_result.distance,
                    vector: match query_result.vector {
                        Some(vector) => {
                            match (vector, ScalarEncoding::FLOAT32, query_vector.len()).try_into() {
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

            let vector_query_results = chroma_proto::VectorQueryResults {
                results: proto_results,
            };
            proto_results_for_all.push(vector_query_results);
        }

        let resp = chroma_proto::QueryVectorsResponse {
            results: proto_results_for_all,
        };

        return Ok(Response::new(resp));
    }
}
