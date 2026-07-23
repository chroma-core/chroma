use std::cell::OnceCell;

use chroma_error::source_chain_contains;
use chroma_log::grpc_log::GrpcPullLogsError;
use chroma_sysdb::GetCollectionsOptions;
use chroma_system::{Operator, System};
use chroma_types::{AttachedFunction, AttachedFunctionUuid, CollectionUuid, DatabaseName};
<<<<<<< HEAD
use uuid::Uuid;

use crate::execution::operators::materialize_logs::MaterializeLogOutput;
=======
use std::collections::HashSet;
use std::error::Error;
use uuid::Uuid;

use crate::execution::operators::{
    fetch_log::FetchLogError,
    finish_async_work::{FinishAsyncWorkInput, FinishAsyncWorkItem, FinishAsyncWorkOperator},
    materialize_logs::MaterializeLogOutput,
};
>>>>>>> 68efa222a ([BUG](worker): Purge deleted fn work (#7489))

use super::{
    compact::{CollectionCompactInfo, CompactionContext, CompactionError, CompactionResponse},
    log_fetch_orchestrator::{LogFetchOrchestratorError, LogFetchOrchestratorResponse},
};
use crate::execution::operators::fetch_log::FetchLogError;

#[derive(Debug, Clone)]
pub struct FunctionInputCollectionData {
    pub collection_info: CollectionCompactInfo,
    pub materialized_log_data: Vec<MaterializeLogOutput>,
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

impl FunctionExecutionContext {
    pub fn new(compaction_context: &CompactionContext) -> Self {
        Self {
            compaction_context: compaction_context.clone(),
        }
    }

    fn build_log_fetch_context(
        mut compaction_context: CompactionContext,
        log_start_offset: i64,
    ) -> CompactionContext {
        compaction_context.collection_info = OnceCell::new();
        compaction_context.log_start_offset = Some(log_start_offset);
        compaction_context
    }

    async fn fetch_function_input_logs(
        mut log_fetch_context: CompactionContext,
        collection_id: CollectionUuid,
        database_name: chroma_types::DatabaseName,
        system: System,
        use_compacted_logs: bool,
    ) -> Result<LogFetchOrchestratorResponse, CompactionError> {
        Ok(log_fetch_context
            .run_get_logs(
                collection_id,
                database_name.clone(),
                system.clone(),
                use_compacted_logs,
            )
            .await?)
    }

    async fn fetch_function_input_collection_data(
        compaction_context: CompactionContext,
        collection_id: CollectionUuid,
        completion_offset: i64,
        database_name: DatabaseName,
        system: System,
    ) -> Result<FunctionInputCollectionData, CompactionError> {
        let log_fetch_context =
            Self::build_log_fetch_context(compaction_context, completion_offset);
        let result = match Self::fetch_function_input_logs(
            log_fetch_context.clone(),
            collection_id,
            database_name.clone(),
            system.clone(),
            false,
        )
        .await
        {
            Ok(result) => result,
            Err(err) if Self::should_backfill_on_fetch_error(&err) => {
                match Self::fetch_function_input_logs(
                    log_fetch_context,
                    collection_id,
                    database_name,
                    system,
                    true,
                )
                .await?
                {
                    LogFetchOrchestratorResponse::Success(success) => {
                        return Ok(FunctionInputCollectionData {
                            collection_info: success.collection_info,
                            materialized_log_data: success.materialized,
                        });
                    }
                    LogFetchOrchestratorResponse::RequireCompactionOffsetRepair(_)
                    | LogFetchOrchestratorResponse::RequireFunctionBackfill(_) => {
                        return Err(CompactionError::InvariantViolation(
                            "Function execution backfill fetch should only return success",
                        ));
                    }
                }
            }
            Err(err) => return Err(err),
        };

        let (materialized_log_data, collection_info) = match result {
            LogFetchOrchestratorResponse::Success(success) => {
                (success.materialized, success.collection_info)
            }
            LogFetchOrchestratorResponse::RequireFunctionBackfill(backfill) => {
                // BackfillFn forces compaction and schedules async work. Fn-consumers
                // only backfill when their incremental log offset has been purged.
                (backfill.materialized, backfill.collection_info)
            }
            LogFetchOrchestratorResponse::RequireCompactionOffsetRepair(_) => {
                return Err(CompactionError::InvariantViolation(
                    "Function execution does not support compaction offset repair",
                ));
            }
        };

        Ok(FunctionInputCollectionData {
            collection_info,
            materialized_log_data,
        })
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

    async fn purge_deleted(
        compaction_context: CompactionContext,
<<<<<<< HEAD
        fn_inputs: &[(CollectionUuid, i64)],
    ) -> Result<DatabaseName, CompactionError> {
        let Some((first_input_collection_id, _)) = fn_inputs.first() else {
=======
        attached_function_id: AttachedFunctionUuid,
        work_items: Vec<FinishAsyncWorkItem>,
    ) -> Result<(), CompactionError> {
        if work_items.is_empty() {
            return Ok(());
        }

        let Some(work_queue_client) = compaction_context.work_queue_client.clone() else {
>>>>>>> 68efa222a ([BUG](worker): Purge deleted fn work (#7489))
            return Err(CompactionError::InvariantViolation(
                "Work queue client not available for async function",
            ));
        };

<<<<<<< HEAD
        let mut sysdb = compaction_context.sysdb.clone();
        // TODO(tanujnay112): This does not support MCMR yet because work queue records
        // do not carry the database name. Pass the database name from the work queue
        // service and remove this unscoped lookup once that metadata is available.
        let collection_info = sysdb
            .get_collection_with_segments(None, *first_input_collection_id)
=======
        FinishAsyncWorkOperator::new()
            .run(&FinishAsyncWorkInput::new(
                attached_function_id,
                work_items,
                work_queue_client,
            ))
>>>>>>> 68efa222a ([BUG](worker): Purge deleted fn work (#7489))
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
        let collections = sysdb
            .get_collections(GetCollectionsOptions {
                collection_ids: Some(fn_inputs.iter().map(|input| input.collection_id).collect()),
                include_soft_deleted: false,
                limit: Some(fn_inputs.len() as u32),
                ..Default::default()
            })
            .await
            .map_err(|_| {
                CompactionError::InvariantViolation("Failed to resolve function input collections")
            })?;
        let live_collection_ids: HashSet<_> = collections
            .iter()
            .map(|collection| collection.collection_id)
            .collect();
        let shared_database_name = collections
            .first()
            .map(|collection| {
                DatabaseName::new(&collection.database).ok_or(CompactionError::InvariantViolation(
                    "Invalid function input collection database name",
                ))
            })
            .transpose()?;
        let mut live_inputs = Vec::with_capacity(fn_inputs.len());
        let mut stale_work_items = Vec::new();

        for input in fn_inputs.iter().cloned() {
            if live_collection_ids.contains(&input.collection_id) {
                live_inputs.push(input);
            } else {
                tracing::info!(
                    collection_id = %input.collection_id,
                    attached_function_id = %attached_function_id,
                    "Finishing stale fn-consumer work for deleted input collection"
                );
                stale_work_items.push(FinishAsyncWorkItem {
                    input_collection_id: input.collection_id,
                    completion_offset: input.queue_compaction_offset,
                });
            }
        }

        Self::purge_deleted(compaction_context, attached_function_id, stale_work_items).await?;

        Ok((shared_database_name, live_inputs))
    }

    #[tracing::instrument(skip(self, system))]
    pub async fn run(
        self,
        attached_function_id: AttachedFunctionUuid,
        fn_inputs: Vec<(CollectionUuid, i64)>,
        system: System,
    ) -> Result<CompactionResponse, CompactionError> {
        if fn_inputs.is_empty() {
            return Err(CompactionError::InvariantViolation(
                "Function execution requires at least one input collection",
            ));
        }

        let base_context = self.compaction_context;
        let (shared_database_name, live_inputs) = Box::pin(Self::partition_live_and_stale_inputs(
            base_context.clone(),
            attached_function_id,
            &fn_inputs,
        ))
        .await?;
        if live_inputs.is_empty() {
            return Ok(CompactionResponse::Success {
                job_id: attached_function_id.into(),
            });
        }
        let shared_database_name =
<<<<<<< HEAD
            Self::resolve_shared_input_database_name(base_context.clone(), &fn_inputs).await?;
        let mut input_collection_data = Vec::with_capacity(fn_inputs.len());
        for (collection_id, completion_offset) in fn_inputs {
            input_collection_data.push(
                Box::pin(Self::fetch_function_input_collection_data(
                    base_context.clone(),
                    collection_id,
=======
            shared_database_name.ok_or(CompactionError::InvariantViolation(
                "Function execution requires at least one live input collection",
            ))?;
        let mut input_collection_data = Vec::with_capacity(live_inputs.len());
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
>>>>>>> 68efa222a ([BUG](worker): Purge deleted fn work (#7489))
                    completion_offset,
                    shared_database_name.clone(),
                    system.clone(),
                ))
                .await?,
            );
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
    use super::FunctionExecutionContext;
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
