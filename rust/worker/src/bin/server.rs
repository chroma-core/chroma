use std::vec;

use chroma::vector_reader_server::{VectorReader, VectorReaderServer};
use chroma::{GetVectorsRequest, GetVectorsResponse, QueryVectorsRequest, QueryVectorsResponse};
use tonic::{transport::Server, Request, Response, Status};

pub mod chroma {
    tonic::include_proto!("chroma"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct WorkerServer {}

#[tonic::async_trait]
impl VectorReader for WorkerServer {
    async fn get_vectors(
        &self,
        request: Request<GetVectorsRequest>,
    ) -> Result<Response<GetVectorsResponse>, Status> {
        println!("Got a request: {:?}", request);

        let id = "a";
        let records = vec![chroma::VectorEmbeddingRecord {
            id: id.into(),
            seq_id: vec![1u8],
            vector: None,
        }];

        let reply = chroma::GetVectorsResponse { records: records };

        Ok(Response::new(reply))
    }

    async fn query_vectors(
        &self,
        request: Request<QueryVectorsRequest>,
    ) -> Result<Response<QueryVectorsResponse>, Status> {
        println!("Got a request: {:?}", request);
        let results = chroma::VectorQueryResults {
            results: vec![chroma::VectorQueryResult {
                id: ("a").into(),
                seq_id: vec![1u8],
                distance: 0.1,
                vector: None,
            }],
        };
        let many_results = vec![results];
        let resp = chroma::QueryVectorsResponse {
            results: many_results,
        };

        Ok(Response::new(resp))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let worker = WorkerServer::default();
    let server = Server::builder()
        .add_service(VectorReaderServer::new(worker))
        .serve(addr)
        .await?;

    Ok(())
}
