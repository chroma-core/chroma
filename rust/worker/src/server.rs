use crate::chroma_proto;
use crate::chroma_proto::{
    GetVectorsRequest, GetVectorsResponse, QueryVectorsRequest, QueryVectorsResponse,
};
use crate::config::{Configurable, WorkerConfig};
use crate::errors::ChromaError;
use crate::segment::SegmentManager;
use async_trait::async_trait;
use tonic::{transport::Server, Request, Response, Status};

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
        println!("Got a request: {:?}", request);

        let id = "a";
        let records = vec![chroma_proto::VectorEmbeddingRecord {
            id: id.into(),
            seq_id: vec![1u8],
            vector: None,
        }];

        let reply = chroma_proto::GetVectorsResponse { records: records };

        Ok(Response::new(reply))
    }

    async fn query_vectors(
        &self,
        request: Request<QueryVectorsRequest>,
    ) -> Result<Response<QueryVectorsResponse>, Status> {
        println!("Got a request: {:?}", request);
        let results = chroma_proto::VectorQueryResults {
            results: vec![chroma_proto::VectorQueryResult {
                id: ("a").into(),
                seq_id: vec![1u8],
                distance: 0.1,
                vector: None,
            }],
        };
        let many_results = vec![results];
        let resp = chroma_proto::QueryVectorsResponse {
            results: many_results,
        };

        Ok(Response::new(resp))
    }
}
