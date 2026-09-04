use crate::work_queue::types::{FinishResult, WorkQueueError};
use crate::work_queue::work_queue_manager::{
    DeferWorkMessage, FinishWorkMessage, GetWorkMessage, PushWorkMessage,
    SetFunctionFailureCountMessage, UpdateFunctionFailureCountMessage, WorkQueueManager,
};
use chroma_sysdb::SysDb;
use chroma_system::ComponentHandle;
use chroma_types::chroma_proto::{
    work_queue_service_server::{WorkQueueService, WorkQueueServiceServer},
    DeferWorkRequest, FailAttachedFunctionRequest, FailFunctionRequest,
    FinalizeAsyncAttachedFunctionRepairRequest, FinishWorkRequest, GetWorkRequest, GetWorkResponse,
    PushWorkRequest, SetAttachedFunctionFailureCountRequest, SetFunctionFailureCountRequest,
    WorkItemResult,
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

    async fn fail_function(
        &self,
        request: Request<FailFunctionRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let fn_id = AttachedFunctionUuid::from_str(&req.fn_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid fn_id: {}", e)))?;
        let input_coll_id = CollectionUuid::from_str(&req.input_coll_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid collection_id: {}", e)))?;

        let mut sysdb = self.sysdb.clone();
        let failure_count = sysdb
            .fail_attached_function(FailAttachedFunctionRequest {
                attached_function_id: fn_id.to_string(),
                collection_id: input_coll_id.to_string(),
            })
            .await
            .map_err(|e| Status::internal(format!("Failed to record function failure: {}", e)))?;

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.manager
            .receiver()
            .send(
                UpdateFunctionFailureCountMessage {
                    fn_id,
                    input_coll_id,
                    failure_count,
                    response_tx,
                },
                None,
            )
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to update function failure count: {}", e))
            })?;
        response_rx.await.map_err(|e| {
            Status::internal(format!("Failed to receive failure count update: {}", e))
        })?;

        Ok(Response::new(()))
    }

    async fn defer_work(&self, request: Request<DeferWorkRequest>) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let fn_id = AttachedFunctionUuid::from_str(&req.fn_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid fn_id: {}", e)))?;
        let input_coll_id = CollectionUuid::from_str(&req.input_coll_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid collection_id: {}", e)))?;
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        self.manager
            .receiver()
            .send(
                DeferWorkMessage {
                    fn_id,
                    input_coll_id,
                    response_tx,
                },
                None,
            )
            .await
            .map_err(|e| Status::internal(format!("Failed to defer work: {}", e)))?;
        response_rx
            .await
            .map_err(|e| Status::internal(format!("Failed to receive defer response: {}", e)))?;

        Ok(Response::new(()))
    }

    async fn set_function_failure_count(
        &self,
        request: Request<SetFunctionFailureCountRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        if req.failure_count < 0 {
            return Err(Status::invalid_argument(
                "failure_count must be non-negative",
            ));
        }
        let fn_id = AttachedFunctionUuid::from_str(&req.fn_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid fn_id: {}", e)))?;
        let input_coll_id = CollectionUuid::from_str(&req.input_coll_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid collection_id: {}", e)))?;

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.manager
            .receiver()
            .send(
                SetFunctionFailureCountMessage {
                    fn_id,
                    input_coll_id,
                    failure_count: req.failure_count,
                    response_tx,
                },
                None,
            )
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to mirror failure count to WQS: {}", e))
            })?;

        match response_rx
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to receive WQS failure count update: {}", e))
            })?
            .map_err(|e| Status::internal(e.to_string()))?
        {
            true => {
                let mut sysdb = self.sysdb.clone();
                sysdb
                    .set_attached_function_failure_count(SetAttachedFunctionFailureCountRequest {
                        attached_function_id: req.fn_id,
                        collection_id: req.input_coll_id,
                        failure_count: req.failure_count,
                    })
                    .await
                    .map_err(|e| {
                        Status::internal(format!("Failed to set function failure count: {}", e))
                    })?;
                Ok(Response::new(()))
            }
            false => Err(Status::not_found("Work queue entry not found")),
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
            max_failure_count: req.max_failure_count,
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
