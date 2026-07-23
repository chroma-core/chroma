use crate::work_queue::types::WorkQueueError;
use crate::work_queue::work_queue_manager::{
    FinishWorkMessage, GetWorkMessage, PushWorkMessage, WorkQueueManager,
};
use chroma_system::ComponentHandle;
use chroma_types::chroma_proto::{
    work_queue_service_server::{WorkQueueService, WorkQueueServiceServer},
    FinishWorkRequest, GetWorkRequest, GetWorkResponse, PushWorkRequest, WorkItemResult,
};
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use std::str::FromStr;
use tonic::{Request, Response, Status};

pub struct WorkQueueServer {
    manager: ComponentHandle<WorkQueueManager>,
}

impl WorkQueueServer {
    pub fn new(manager: ComponentHandle<WorkQueueManager>) -> Self {
        Self { manager }
    }

    pub fn into_service(self) -> WorkQueueServiceServer<Self> {
        WorkQueueServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl WorkQueueService for WorkQueueServer {
    async fn push_work(&self, request: Request<PushWorkRequest>) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let fn_id = AttachedFunctionUuid::from_str(&req.fn_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid fn_id: {}", e)))?;
        let input_coll_id = CollectionUuid::from_str(&req.input_coll_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid collection_id: {}", e)))?;

        let msg = PushWorkMessage {
            fn_id,
            input_coll_id,
            completion_offset: req.completion_offset,
            compaction_offset: req.compaction_offset,
            response_tx,
        };

        self.manager
            .receiver()
            .send(msg, None)
            .await
            .map_err(|e| Status::internal(format!("Failed to send message: {}", e)))?;

        response_rx
            .await
            .map_err(|e| Status::internal(format!("Failed to receive response: {}", e)))?
            .map_err(|e: WorkQueueError| Status::internal(e.to_string()))?;

        Ok(Response::new(()))
    }

    async fn finish_work(
        &self,
        request: Request<FinishWorkRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let msg = FinishWorkMessage {
            fn_id: AttachedFunctionUuid::from_str(&req.fn_id)
                .map_err(|e| Status::invalid_argument(format!("Invalid fn_id: {}", e)))?,
            input_coll_id: CollectionUuid::from_str(&req.input_coll_id)
                .map_err(|e| Status::invalid_argument(format!("Invalid collection_id: {}", e)))?,
            new_completion_offset: req.completion_offset,
            response_tx,
        };

        self.manager
            .receiver()
            .send(msg, None)
            .await
            .map_err(|e| Status::internal(format!("Failed to send message: {}", e)))?;

        response_rx
            .await
            .map_err(|e| Status::internal(format!("Failed to receive response: {}", e)))?
            .map_err(|e: WorkQueueError| Status::internal(e.to_string()))?;
        Ok(Response::new(()))
    }

    async fn get_work(
        &self,
        request: Request<GetWorkRequest>,
    ) -> Result<Response<GetWorkResponse>, Status> {
        let req = request.into_inner();
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let msg = GetWorkMessage {
            shard_id: req.shard_id,
            limit: req.limit as usize,
            response_tx,
        };

        self.manager
            .receiver()
            .send(msg, None)
            .await
            .map_err(|e| Status::internal(format!("Failed to send message: {}", e)))?;

        let items = response_rx
            .await
            .map_err(|e| Status::internal(format!("Failed to receive response: {}", e)))?
            .map_err(|e: WorkQueueError| Status::internal(e.to_string()))?;

        let results: Vec<WorkItemResult> = items
            .into_iter()
            .map(|record| WorkItemResult {
                fn_id: record.fn_id.to_string(),
                input_coll_id: record.input_coll_id.to_string(),
                completion_offset: record.completion_offset,
                compaction_offset: Some(record.compaction_offset),
            })
            .collect();

        Ok(Response::new(GetWorkResponse { items: results }))
    }
}
