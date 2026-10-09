use async_trait::async_trait;
use chroma_system::ComponentHandle;
use chroma_types::chroma_proto::{
    fn_consumer_server::{FnConsumer, FnConsumerServer},
    FnConsumerInProgressJobInfo, ListFnConsumerInProgressJobsRequest,
    ListFnConsumerInProgressJobsResponse,
};
use tonic::{Request, Response, Status};

use super::fn_consumer_manager::{FnConsumerManager, ListInProgressJobsMessage};

pub struct FnConsumerGrpcServer {
    manager: ComponentHandle<FnConsumerManager>,
}

impl FnConsumerGrpcServer {
    pub fn new(manager: ComponentHandle<FnConsumerManager>) -> Self {
        Self { manager }
    }

    pub fn into_service(self) -> FnConsumerServer<Self> {
        FnConsumerServer::new(self)
    }
}

#[async_trait]
impl FnConsumer for FnConsumerGrpcServer {
    async fn list_in_progress_jobs(
        &self,
        _request: Request<ListFnConsumerInProgressJobsRequest>,
    ) -> Result<Response<ListFnConsumerInProgressJobsResponse>, Status> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.manager
            .receiver()
            .send(ListInProgressJobsMessage { response_tx }, None)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        let jobs = response_rx
            .await
            .map_err(|error| Status::internal(format!("Failed to receive response: {error}")))?
            .into_iter()
            .map(|entry| FnConsumerInProgressJobInfo {
                fn_id: entry.fn_id.to_string(),
                expires_at_epoch_secs: entry.expires_at_epoch_secs,
                collection_ids: entry
                    .collection_ids
                    .into_iter()
                    .map(|collection_id| collection_id.to_string())
                    .collect(),
            })
            .collect();

        Ok(Response::new(ListFnConsumerInProgressJobsResponse { jobs }))
    }
}
