use chroma_error::source_chain_contains;
use chroma_log::grpc_log::GrpcPullLogsError;
use chroma_system::System;
use chroma_types::{AttachedFunction, AttachedFunctionUuid, CollectionUuid, DatabaseName};
use uuid::Uuid;

use crate::execution::operators::{
    fetch_log::FetchLogError, materialize_logs::MaterializeLogOutput,
};

use super::{
    compact::{CollectionCompactInfo, CompactionContext, CompactionError, CompactionResponse},
    log_fetch_orchestrator::{LogFetchOrchestratorError, LogFetchOrchestratorResponse},
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
        match error {
            CompactionError::DataFetchError(LogFetchOrchestratorError::FetchLog(
                FetchLogError::PullLog(err),
            )) => source_chain_contains(err.as_ref(), |source| {
                source
                    .downcast_ref::<GrpcPullLogsError>()
                    .map(|pull_err| matches!(pull_err, GrpcPullLogsError::Purged))
                    .unwrap_or(false)
            }),
            _ => false,
        }
    }

    async fn resolve_shared_input_database_name(
        compaction_context: CompactionContext,
        fn_inputs: &[FunctionExecutionInput],
    ) -> Result<DatabaseName, CompactionError> {
        let Some(first_input) = fn_inputs.first() else {
            return Err(CompactionError::InvariantViolation(
                "Function execution requires at least one input collection",
            ));
        };

        let mut sysdb = compaction_context.sysdb.clone();
        // TODO(tanujnay112): This does not support MCMR yet because work queue records
        // do not carry the database name. Pass the database name from the work queue
        // service and remove this unscoped lookup once that metadata is available.
        let collection_info = sysdb
            .get_collection_with_segments(None, first_input.collection_id)
            .await
            .map_err(|_| {
                CompactionError::InvariantViolation(
                    "Failed to resolve function input collection database",
                )
            })?;

        DatabaseName::new(&collection_info.collection.database).ok_or(
            CompactionError::InvariantViolation("Invalid function input collection database name"),
        )
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
        let shared_database_name =
            Self::resolve_shared_input_database_name(base_context.clone(), &fn_inputs).await?;
        let mut input_collection_data = Vec::with_capacity(fn_inputs.len());
        for input in fn_inputs {
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
    use chroma_log::grpc_log::GrpcPullLogsError;
    use tonic::Status;

    #[test]
    fn purged_pull_logs_error_triggers_backfill() {
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
        let err = CompactionError::DataFetchError(LogFetchOrchestratorError::FetchLog(
            FetchLogError::PullLog(Box::new(GrpcPullLogsError::FailedToPullLogs(
                Status::not_found("unrelated not found"),
            ))),
        ));

        assert!(!FunctionExecutionContext::should_backfill_on_fetch_error(
            &err
        ));
    }
}
