use crate::work_queue::types::FinishResult;
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

#[allow(dead_code)]
pub struct WorkQueueServer {
    manager: ComponentHandle<WorkQueueManager>,
}

#[allow(dead_code)]
impl WorkQueueServer {
    pub fn new(manager: ComponentHandle<WorkQueueManager>) -> Self {
        Self { manager }
    }

    #[allow(dead_code)]
    pub fn into_service(self) -> WorkQueueServiceServer<Self> {
        WorkQueueServiceServer::new(self)
    }

    // Stub for repair handling - will be replaced with actual sysdb integration
    #[allow(dead_code)]
    async fn handle_repair_stub(&self, fn_id: &AttachedFunctionUuid) {
        // TODO: Implement actual repair logic once sysdb integration is ready
        // This should:
        // 1. Call get_attached_functions() to get latest offset
        // 2. Re-push work with new offset
        // 3. Call FinalizeAsyncAttachedFunctionRepair()
        tracing::info!("STUB: Handling repair for function {:?}", fn_id);
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
            .map_err(|e| Status::internal(format!("Operation failed: {}", e)))?;

        Ok(Response::new(()))
    }

    async fn finish_work(
        &self,
        request: Request<FinishWorkRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let fn_id = AttachedFunctionUuid::from_str(&req.fn_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid fn_id: {}", e)))?;
        let input_coll_id = CollectionUuid::from_str(&req.input_coll_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid collection_id: {}", e)))?;

        let msg = FinishWorkMessage {
            fn_id,
            input_coll_id,
            new_completion_offset: req.completion_offset,
            response_tx,
        };

        self.manager
            .receiver()
            .send(msg, None)
            .await
            .map_err(|e| Status::internal(format!("Failed to send message: {}", e)))?;

        let result = response_rx
            .await
            .map_err(|e| Status::internal(format!("Failed to receive response: {}", e)))?
            .map_err(|e| Status::internal(format!("Operation failed: {}", e)))?;

        // Handle the result
        match result {
            FinishResult::Success => {
                // Success case - just return ok
                Ok(Response::new(()))
            }
            FinishResult::NeedsRepair => {
                // NeedsRepair case - handle repair here
                // TODO: Replace with actual repair handling once sysdb integration is ready
                // Also need to check for error from this call.
                self.handle_repair_stub(&fn_id).await;
                Ok(Response::new(()))
            }
        }
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
            .map_err(|e| Status::internal(format!("Operation failed: {}", e)))?;

        let results: Vec<WorkItemResult> = items
            .into_iter()
            .map(|record| WorkItemResult {
                fn_id: record.fn_id.to_string(),
                input_coll_id: record.input_coll_id.to_string(),
                completion_offset: record.completion_offset,
            })
            .collect();

        Ok(Response::new(GetWorkResponse { items: results }))
    }
}
