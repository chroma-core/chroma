use crate::work_queue::types::{FinishResult, WorkQueueError};
use crate::work_queue::work_queue_manager::{
    FinishWorkMessage, GetWorkMessage, PushWorkMessage, WorkQueueManager,
};
use chroma_sysdb::SysDb;
use chroma_system::ComponentHandle;
use chroma_types::chroma_proto::{
    work_queue_service_server::{WorkQueueService, WorkQueueServiceServer},
    FinalizeAsyncAttachedFunctionRepairRequest, FinishWorkRequest, GetWorkRequest, GetWorkResponse,
    PushWorkRequest, WorkItemResult,
};
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use std::str::FromStr;
use tonic::{Request, Response, Status};

pub struct WorkQueueServer {
    manager: ComponentHandle<WorkQueueManager>,
    sysdb: SysDb,
}

impl WorkQueueServer {
    pub fn new(manager: ComponentHandle<WorkQueueManager>, sysdb: SysDb) -> Self {
        Self { manager, sysdb }
    }

    pub fn into_service(self) -> WorkQueueServiceServer<Self> {
        WorkQueueServiceServer::new(self)
    }

    // Handle repair by finalizing the repair in sysdb
    async fn handle_repair(
        &self,
        fn_id: &AttachedFunctionUuid,
        input_coll_id: &CollectionUuid,
    ) -> Result<(), WorkQueueError> {
        // The work has already been re-pushed by WorkQueueManager
        // We just need to finalize the repair
        let repair_request = FinalizeAsyncAttachedFunctionRepairRequest {
            attached_function_id: fn_id.to_string(),
            collection_id: input_coll_id.to_string(),
        };

        let mut sysdb = self.sysdb.clone();
        sysdb
            .finalize_async_attached_function_repair(repair_request)
            .await
            .map_err(|e| WorkQueueError::RepairFailed(e.to_string()))?;

        tracing::info!(
            "Repair finalized for function {} and collection {}",
            fn_id,
            input_coll_id
        );

        Ok(())
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
            .map_err(|e: WorkQueueError| Status::internal(e.to_string()))?;

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
            .map_err(|e: WorkQueueError| Status::internal(e.to_string()))?;

        // Handle the result
        match result {
            FinishResult::Success => {
                // Success case - just return ok
                Ok(Response::new(()))
            }
            FinishResult::NeedsRepair => {
                // NeedsRepair case - handle repair
                self.handle_repair(&fn_id, &input_coll_id)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
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
            .map_err(|e: WorkQueueError| Status::internal(e.to_string()))?;

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
