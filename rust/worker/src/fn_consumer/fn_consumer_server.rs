use chroma_system::ComponentHandle;
use chroma_types::chroma_proto::{
    fn_consumer_service_server::{FnConsumerService, FnConsumerServiceServer},
    FinishWorkRequest,
};
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use std::str::FromStr;
use tonic::{Request, Response, Status};

use crate::fn_consumer::fn_consumer_manager::{FnConsumerManager, RemoveInProgressMessage};
use crate::work_queue::work_queue_client::WorkQueueClient;

#[allow(dead_code)]
pub struct FnConsumerServer {
    manager: ComponentHandle<FnConsumerManager>,
    work_queue_client: WorkQueueClient,
}

#[allow(dead_code)]
impl FnConsumerServer {
    pub fn new(
        manager: ComponentHandle<FnConsumerManager>,
        work_queue_client: WorkQueueClient,
    ) -> Self {
        Self {
            manager,
            work_queue_client,
        }
    }

    pub fn into_service(self) -> FnConsumerServiceServer<Self> {
        FnConsumerServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl FnConsumerService for FnConsumerServer {
    async fn finish_work(
        &self,
        request: Request<FinishWorkRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();

        let fn_id = AttachedFunctionUuid::from_str(&req.fn_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid fn_id: {}", e)))?;
        let input_coll_id = CollectionUuid::from_str(&req.input_coll_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid input_coll_id: {}", e)))?;

        // Forward to the work queue synchronously. If this fails we surface
        // the error to the caller and leave the manager's in-progress entry
        // alone so the job-expiry safety net can reclaim it.
        let mut work_queue_client = self.work_queue_client.clone();
        work_queue_client
            .finish_work(req.fn_id, req.input_coll_id, req.completion_offset)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to forward FinishWork to work queue: {}", e))
            })?;

        // Drop the in-progress entry so the next poll can pick up new work
        // for this (fn_id, input_coll_id).
        self.manager
            .receiver()
            .send(
                RemoveInProgressMessage {
                    fn_id,
                    input_coll_id,
                },
                None,
            )
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to notify manager of completion: {}", e))
            })?;

        Ok(Response::new(()))
    }
}
