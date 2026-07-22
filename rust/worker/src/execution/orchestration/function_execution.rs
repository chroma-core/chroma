use chroma_error::{source_chain_contains, ChromaError};
use chroma_log::grpc_log::GrpcPullLogsError;
<<<<<<< HEAD
use chroma_system::{Operator, System};
use chroma_types::{
    AttachedFunction, AttachedFunctionUuid, CollectionUuid, DatabaseName,
    GetCollectionWithSegmentsError,
};
use std::error::Error;
use tonic::Code;
use uuid::Uuid;

use crate::execution::operators::{
    fetch_log::FetchLogError,
    finish_async_work::{FinishAsyncWorkItem, FinishAsyncWorkOperator},
    materialize_logs::MaterializeLogOutput,
};

use super::{
    compact::{CollectionCompactInfo, CompactionContext, CompactionError, CompactionResponse},
    log_fetch_orchestrator::LogFetchOrchestratorResponse,
};

#[derive(Debug, Clone)]
pub struct FunctionExecutionInput {
    pub collection_id: CollectionUuid,
    pub queue_compaction_offset: i64,
}

#[derive(Debug, Clone)]
pub struct FunctionInputCollectionData {
    pub collection_info: CollectionCompactInfo,
    pub materialized_log_data: Vec<MaterializeLogOutput>,
    pub resolved_attached_functions: Vec<AttachedFunction>,
}

#[derive(Debug, Clone)]
pub struct FunctionExecutionProgress {
    pub input_collection_id: CollectionUuid,
    pub updated_completion_offset: u64,
}

#[derive(Debug, Clone)]
pub struct FunctionContext {
    pub attached_function_id: AttachedFunctionUuid,
    pub function_id: Uuid,
    pub input_progress: Vec<FunctionExecutionProgress>,
    pub is_async: bool,
    pub attached_function: AttachedFunction,
}

#[derive(Debug)]
pub struct FunctionExecutionContext {
    compaction_context: CompactionContext,
}

fn has_reached_queue_frontier(completion_offset: i64, queue_compaction_offset: i64) -> bool {
    completion_offset >= queue_compaction_offset
}

impl FunctionExecutionContext {
    pub fn new(compaction_context: &CompactionContext) -> Self {
        Self {
            compaction_context: compaction_context.clone(),
        }
    }

    async fn fetch_function_input_logs(
        mut log_fetch_context: CompactionContext,
        collection_id: CollectionUuid,
        database_name: chroma_types::DatabaseName,
        system: System,
        use_compacted_logs: bool,
        attached_function_id: AttachedFunctionUuid,
    ) -> Result<LogFetchOrchestratorResponse, CompactionError> {
        Ok(log_fetch_context
            .run_get_logs(
                collection_id,
                database_name.clone(),
                system.clone(),
                use_compacted_logs,
                Some(attached_function_id),
            )
            .await?)
    }

    async fn fetch_backfilled_function_input_collection_data(
        log_fetch_context: CompactionContext,
        collection_id: CollectionUuid,
        attached_function_id: AttachedFunctionUuid,
        database_name: DatabaseName,
        system: System,
    ) -> Result<FunctionInputCollectionData, CompactionError> {
        match Self::fetch_function_input_logs(
            log_fetch_context,
            collection_id,
            database_name,
            system,
            true,
            attached_function_id,
        )
        .await?
        {
            LogFetchOrchestratorResponse::Success(success) => Ok(FunctionInputCollectionData {
                collection_info: success.collection_info,
                materialized_log_data: success.materialized,
                resolved_attached_functions: success.resolved_attached_functions,
            }),
            LogFetchOrchestratorResponse::RequireCompactionOffsetRepair(_)
            | LogFetchOrchestratorResponse::RequireFunctionBackfill(_) => {
                Err(CompactionError::InvariantViolation(
                    "Function execution backfill fetch should only return success",
                ))
            }
        }
    }

    async fn fetch_function_input_collection_data(
        compaction_context: CompactionContext,
        collection_id: CollectionUuid,
        attached_function_id: AttachedFunctionUuid,
        database_name: DatabaseName,
        system: System,
    ) -> Result<FunctionInputCollectionData, CompactionError> {
        let log_fetch_context = compaction_context;
        let result = match Self::fetch_function_input_logs(
            log_fetch_context.clone(),
            collection_id,
            database_name.clone(),
            system.clone(),
            false,
            attached_function_id,
        )
        .await
        {
            Ok(result) => result,
            Err(err) if Self::should_backfill_on_fetch_error(&err) => {
                return Box::pin(Self::fetch_backfilled_function_input_collection_data(
                    log_fetch_context,
                    collection_id,
                    attached_function_id,
                    database_name,
                    system,
                ))
                .await;
            }
            Err(err) => return Err(err),
        };

        match result {
            LogFetchOrchestratorResponse::Success(success) => Ok(FunctionInputCollectionData {
                collection_info: success.collection_info,
                materialized_log_data: success.materialized,
                resolved_attached_functions: success.resolved_attached_functions,
            }),
            LogFetchOrchestratorResponse::RequireFunctionBackfill(_) => {
                Box::pin(Self::fetch_backfilled_function_input_collection_data(
                    log_fetch_context,
                    collection_id,
                    attached_function_id,
                    database_name,
                    system,
                ))
                .await
            }
            LogFetchOrchestratorResponse::RequireCompactionOffsetRepair(_) => {
                Err(CompactionError::InvariantViolation(
                    "Function execution does not support compaction offset repair",
                ))
            }
        }
    }

    fn should_backfill_on_fetch_error(error: &CompactionError) -> bool {
        source_chain_contains(error, |source| {
            let Some(FetchLogError::PullLog(pull_error)) = source.downcast_ref::<FetchLogError>()
            else {
                return false;
            };

            Self::is_purged_pull_error(pull_error.as_ref())
        })
    }

    fn is_purged_pull_error(pull_error: &(dyn ChromaError + 'static)) -> bool {
        let pull_error = pull_error as &(dyn Error + 'static);

        if matches!(
            pull_error.downcast_ref::<GrpcPullLogsError>(),
            Some(GrpcPullLogsError::Purged)
        ) {
            return true;
        }

        pull_error
            .downcast_ref::<Box<dyn ChromaError>>()
            .is_some_and(|pull_error| Self::is_purged_pull_error(pull_error.as_ref()))
    }

    fn is_stale_input_collection_error(error: &GetCollectionWithSegmentsError) -> bool {
        match error {
            GetCollectionWithSegmentsError::NotFound(_) => true,
            GetCollectionWithSegmentsError::Grpc(status) => {
                status.code() == Code::NotFound
                    || (status.code() == Code::FailedPrecondition
                        && status.message().contains("soft deleted"))
            }
            _ => false,
        }
    }

    async fn purge_deleted(
        compaction_context: CompactionContext,
        attached_function_id: AttachedFunctionUuid,
        work_items: Vec<FinishAsyncWorkItem>,
    ) -> Result<(), CompactionError> {
        if work_items.is_empty() {
            return Ok(());
        }

        let Some(work_queue_client) = compaction_context.work_queue_client.clone() else {
            return Err(CompactionError::InvariantViolation(
                "Work queue client not available for async function",
            ));
        };

        FinishAsyncWorkOperator::new()
            .run(
                &crate::execution::operators::finish_async_work::FinishAsyncWorkInput::new(
                    attached_function_id,
                    work_items,
                    work_queue_client,
                ),
            )
            .await
            .map_err(|_| {
                CompactionError::InvariantViolation("Failed to purge deleted fn-consumer work item")
            })?;

        Ok(())
    }

    async fn partition_live_and_stale_inputs(
        compaction_context: CompactionContext,
        attached_function_id: AttachedFunctionUuid,
        fn_inputs: &[FunctionExecutionInput],
    ) -> Result<(Option<DatabaseName>, Vec<FunctionExecutionInput>), CompactionError> {
        if fn_inputs.is_empty() {
            return Err(CompactionError::InvariantViolation(
                "Function execution requires at least one input collection",
            ));
        }

        let mut sysdb = compaction_context.sysdb.clone();
        let mut live_inputs = Vec::with_capacity(fn_inputs.len());
        let mut stale_work_items = Vec::new();
        let mut shared_database_name = None;

        for input in fn_inputs.iter().cloned() {
            // TODO(tanujnay112): This does not support MCMR yet because work queue records
            // do not carry the database name. Pass the database name from the work queue
            // service and remove this unscoped lookup once that metadata is available.
            match sysdb
                .get_collection_with_segments(None, input.collection_id)
                .await
            {
                Ok(collection_info) => {
                    if shared_database_name.is_none() {
                        shared_database_name = Some(
                            DatabaseName::new(&collection_info.collection.database).ok_or(
                                CompactionError::InvariantViolation(
                                    "Invalid function input collection database name",
                                ),
                            )?,
                        );
                    }
                    live_inputs.push(input);
                }
                Err(error) if Self::is_stale_input_collection_error(&error) => {
                    tracing::info!(
                        collection_id = %input.collection_id,
                        attached_function_id = %attached_function_id,
                        error = %error,
                        "Finishing stale fn-consumer work for deleted input collection"
                    );
                    stale_work_items.push(FinishAsyncWorkItem {
                        input_collection_id: input.collection_id,
                        completion_offset: input.queue_compaction_offset,
                    });
                }
                Err(_) => {
                    return Err(CompactionError::InvariantViolation(
                        "Failed to resolve function input collection database",
                    ));
                }
            }
        }

        Self::purge_deleted(
            compaction_context,
            attached_function_id,
            stale_work_items,
        )
        .await?;

        Ok((shared_database_name, live_inputs))
    }

    #[tracing::instrument(skip(self, system))]
    pub async fn run(
        self,
        attached_function_id: AttachedFunctionUuid,
        fn_inputs: Vec<FunctionExecutionInput>,
        system: System,
    ) -> Result<CompactionResponse, CompactionError> {
        if fn_inputs.is_empty() {
            return Err(CompactionError::InvariantViolation(
                "Function execution requires at least one input collection",
            ));
        }

        let base_context = self.compaction_context;
        let (shared_database_name, live_inputs) = Self::partition_live_and_stale_inputs(
            base_context.clone(),
            attached_function_id,
            &fn_inputs,
        )
        .await?;
        if live_inputs.is_empty() {
            return Ok(CompactionResponse::Success {
                job_id: attached_function_id.into(),
            });
        }
        let shared_database_name =
            shared_database_name.ok_or(CompactionError::InvariantViolation(
                "Function execution requires at least one live input collection",
            ))?;
        let mut input_collection_data = Vec::with_capacity(fn_inputs.len());
        for input in live_inputs {
            let collection_data = Box::pin(Self::fetch_function_input_collection_data(
                base_context.clone(),
                input.collection_id,
                attached_function_id,
                shared_database_name.clone(),
                system.clone(),
            ))
            .await?;

            let completion_offset = collection_data
                .resolved_attached_functions
                .iter()
                .find(|attached_function| attached_function.id == attached_function_id)
                .map(|attached_function| attached_function.completion_offset as i64)
                .ok_or(CompactionError::InvariantViolation(
                    "Missing resolved attached function state for fn-consumer input collection",
                ))?;

            if has_reached_queue_frontier(completion_offset, input.queue_compaction_offset) {
                tracing::info!(
                    collection_id = %input.collection_id,
                    completion_offset,
                    queue_compaction_offset = input.queue_compaction_offset,
                    "Skipping stale fn-consumer work item because attached function is already at or beyond the queued frontier"
                );
                continue;
            }

            input_collection_data.push(collection_data);
        }

        if input_collection_data.is_empty() {
            return Ok(CompactionResponse::Success {
                job_id: attached_function_id.into(),
            });
        }

        let mut compaction_context = base_context;

        if let Some((function_context, collection_register_info)) = compaction_context
            .run_attached_function_workflow(
                input_collection_data,
                system.clone(),
                false,
                Some(attached_function_id),
            )
            .await?
        {
            compaction_context
                .run_register(
                    vec![collection_register_info],
                    Some(function_context),
                    system,
                )
                .await?;
        }

        Ok(CompactionResponse::Success {
            job_id: attached_function_id.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{has_reached_queue_frontier, FunctionExecutionContext};
    use crate::execution::{
        operators::fetch_log::FetchLogError,
        orchestration::{
            compact::CompactionError, log_fetch_orchestrator::LogFetchOrchestratorError,
        },
    };
    use chroma_error::ChromaError;
    use chroma_log::grpc_log::GrpcPullLogsError;
    use chroma_types::GetCollectionWithSegmentsError;
    use tonic::Status;

    #[test]
    fn purged_pull_logs_error_triggers_backfill() {
        let pull_error: Box<dyn ChromaError> = Box::new(GrpcPullLogsError::Purged);
        let err = CompactionError::DataFetchError(LogFetchOrchestratorError::FetchLog(
            FetchLogError::PullLog(Box::new(pull_error)),
        ));

        assert!(FunctionExecutionContext::should_backfill_on_fetch_error(
            &err
        ));
    }

    #[test]
    fn directly_boxed_purged_error_triggers_backfill() {
        let err = CompactionError::DataFetchError(LogFetchOrchestratorError::FetchLog(
            FetchLogError::PullLog(Box::new(GrpcPullLogsError::Purged)),
        ));

        assert!(FunctionExecutionContext::should_backfill_on_fetch_error(
            &err
        ));
    }

    #[test]
    fn zero_queue_frontier_treats_equality_as_complete() {
        assert!(has_reached_queue_frontier(0, 0));
    }

    #[test]
    fn generic_not_found_does_not_trigger_backfill() {
        let pull_error: Box<dyn ChromaError> = Box::new(GrpcPullLogsError::FailedToPullLogs(
            Status::not_found("unrelated not found"),
        ));
        let err = CompactionError::DataFetchError(LogFetchOrchestratorError::FetchLog(
            FetchLogError::PullLog(Box::new(pull_error)),
        ));

        assert!(!FunctionExecutionContext::should_backfill_on_fetch_error(
            &err
        ));
    }

    #[test]
    fn deleted_collection_not_found_is_treated_as_stale_input() {
        assert!(FunctionExecutionContext::is_stale_input_collection_error(
            &GetCollectionWithSegmentsError::NotFound("missing".to_string())
        ));
    }

    #[test]
    fn soft_deleted_collection_is_treated_as_stale_input() {
        assert!(FunctionExecutionContext::is_stale_input_collection_error(
            &GetCollectionWithSegmentsError::Grpc(Status::failed_precondition(
                "collection soft deleted",
            ))
        ));
    }

    #[test]
    fn unrelated_failed_precondition_is_not_treated_as_stale_input() {
        assert!(!FunctionExecutionContext::is_stale_input_collection_error(
            &GetCollectionWithSegmentsError::Grpc(Status::failed_precondition(
                "different precondition failure",
            ))
        ));
    }
}
