use crate::work_queue::types::{FinishResult, WorkQueueError};
use crate::work_queue::work_queue_manager::{
    DeferWorkMessage, FinishWorkMessage, GetWorkMessage, GetWorkResult, PushWorkMessage,
    SetFunctionFailureCountMessage, UpdateFunctionFailureCountMessage, WorkQueueManager,
};
use crate::work_queue::{GET_WORK_RETRY_PUSHBACK_MS_METADATA, GRPC_MAX_DECODING_MESSAGE_SIZE};
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
use std::collections::HashSet;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};

fn retry_after_ms(retry_after: Duration) -> u64 {
    let retry_after_ms = retry_after.as_nanos().saturating_add(999_999) / 1_000_000;
    retry_after_ms.max(1).min(u128::from(u64::MAX)) as u64
}

fn retry_at_unix_ms(retry_after: Duration, now: SystemTime) -> u64 {
    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    retry_after_ms(since_epoch.saturating_add(retry_after))
}

fn get_work_item_limit(limit: u32, max_items: u32) -> usize {
    if max_items == 0 {
        limit as usize
    } else {
        max_items as usize
    }
}

fn resource_exhausted_status(retry_after_ms: u64) -> Status {
    let mut status = Status::resource_exhausted("GetWork rate limit exhausted");
    status.metadata_mut().insert(
        GET_WORK_RETRY_PUSHBACK_MS_METADATA,
        retry_after_ms
            .to_string()
            .parse()
            .expect("retry delay is valid ASCII metadata"),
    );
    status
}

fn get_work_response(result: GetWorkResult) -> Result<Response<GetWorkResponse>, Status> {
    get_work_response_at(result, SystemTime::now())
}

fn get_work_response_at(
    result: GetWorkResult,
    now: SystemTime,
) -> Result<Response<GetWorkResponse>, Status> {
    let retry_after_ms = result.retry_after.map(retry_after_ms);
    if result.items.is_empty() {
        if let Some(retry_after_ms) = retry_after_ms {
            return Err(resource_exhausted_status(retry_after_ms));
        }
    }

    let items = result
        .items
        .into_iter()
        .map(|record| WorkItemResult {
            fn_id: record.fn_id.to_string(),
            input_coll_id: record.input_coll_id.to_string(),
            completion_offset: record.completion_offset,
            compaction_offset: Some(record.compaction_offset),
        })
        .collect();

    Ok(Response::new(GetWorkResponse {
        items,
        retry_at_unix_ms: result
            .retry_after
            .map(|retry_after| retry_at_unix_ms(retry_after, now)),
    }))
}

pub struct WorkQueueServer {
    manager: ComponentHandle<WorkQueueManager>,
    sysdb: SysDb,
}

impl WorkQueueServer {
    pub fn new(manager: ComponentHandle<WorkQueueManager>, sysdb: SysDb) -> Self {
        Self { manager, sysdb }
    }

    pub fn into_service(self) -> WorkQueueServiceServer<Self> {
        WorkQueueServiceServer::new(self).max_decoding_message_size(GRPC_MAX_DECODING_MESSAGE_SIZE)
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
        let excluded_fn_ids = req
            .excluded_fn_ids
            .iter()
            .map(|fn_id| {
                fn_id.parse::<AttachedFunctionUuid>().map_err(|error| {
                    Status::invalid_argument(format!(
                        "Invalid excluded attached function ID {fn_id:?}: {error}"
                    ))
                })
            })
            .collect::<Result<HashSet<_>, _>>()?;
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let msg = GetWorkMessage {
            shard_id: req.shard_id,
            limit: req.limit as usize,
            max_items: get_work_item_limit(req.limit, req.max_items),
            max_failure_count: req.max_failure_count,
            excluded_fn_ids,
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

        get_work_response(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::work_queue::types::WorkQueueRecord;

    #[test]
    fn resource_exhausted_status_includes_rounded_up_retry_delay() {
        let status = resource_exhausted_status(retry_after_ms(Duration::from_micros(100)));

        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            status
                .metadata()
                .get(GET_WORK_RETRY_PUSHBACK_MS_METADATA)
                .unwrap(),
            "1"
        );

        let status = resource_exhausted_status(retry_after_ms(Duration::from_millis(125)));
        assert_eq!(
            status
                .metadata()
                .get(GET_WORK_RETRY_PUSHBACK_MS_METADATA)
                .unwrap(),
            "125"
        );
    }

    #[test]
    fn retry_after_milliseconds_round_up() {
        assert_eq!(retry_after_ms(Duration::ZERO), 1);
        assert_eq!(retry_after_ms(Duration::from_micros(100)), 1);
        assert_eq!(retry_after_ms(Duration::from_millis(125)), 125);
    }

    #[test]
    fn missing_item_limit_preserves_legacy_request_semantics() {
        assert_eq!(get_work_item_limit(10, 0), 10);
        assert_eq!(get_work_item_limit(10, 25), 25);
    }

    #[test]
    fn partial_response_preserves_retry_deadline() {
        let response = get_work_response_at(
            GetWorkResult {
                items: vec![WorkQueueRecord {
                    fn_id: AttachedFunctionUuid::new(),
                    input_coll_id: CollectionUuid::new(),
                    completion_offset: 1,
                    compaction_offset: 2,
                    insertion_order: 3,
                    failure_count: 0,
                }],
                retry_after: Some(Duration::from_millis(125)),
            },
            UNIX_EPOCH + Duration::from_secs(1_000),
        )
        .unwrap()
        .into_inner();

        assert_eq!(response.items.len(), 1);
        assert_eq!(response.retry_at_unix_ms, Some(1_000_125));
    }

    #[test]
    fn empty_rate_limited_response_uses_resource_exhausted() {
        let status = get_work_response(GetWorkResult {
            items: Vec::new(),
            retry_after: Some(Duration::from_millis(125)),
        })
        .unwrap_err();

        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            status
                .metadata()
                .get(GET_WORK_RETRY_PUSHBACK_MS_METADATA)
                .unwrap(),
            "125"
        );
    }
}
