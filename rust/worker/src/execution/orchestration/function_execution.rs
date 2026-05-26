use std::cell::OnceCell;

use chroma_system::System;
use chroma_types::{AttachedFunction, AttachedFunctionUuid, CollectionUuid};
use uuid::Uuid;

use crate::execution::operators::materialize_logs::MaterializeLogOutput;

use super::{
    compact::{CollectionCompactInfo, CompactionContext, CompactionError, CompactionResponse},
    log_fetch_orchestrator::LogFetchOrchestratorResponse,
};

#[derive(Debug, Clone)]
pub struct FunctionExecutionBatch {
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

    async fn fetch_function_input_batch(
        compaction_context: CompactionContext,
        collection_id: CollectionUuid,
        completion_offset: i64,
        database_name: chroma_types::DatabaseName,
        system: System,
    ) -> Result<FunctionExecutionBatch, CompactionError> {
        let log_fetch_context =
            Self::build_log_fetch_context(compaction_context, completion_offset);
        let result = Self::fetch_function_input_logs(
            log_fetch_context.clone(),
            collection_id,
            database_name.clone(),
            system.clone(),
            false,
        )
        .await?;

        let (materialized_log_data, collection_info) = match result {
            LogFetchOrchestratorResponse::Success(success) => {
                (success.materialized, success.collection_info)
            }
            LogFetchOrchestratorResponse::RequireFunctionBackfill(_) => {
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
                        (success.materialized, success.collection_info)
                    }
                    LogFetchOrchestratorResponse::RequireCompactionOffsetRepair(_)
                    | LogFetchOrchestratorResponse::RequireFunctionBackfill(_) => {
                        return Err(CompactionError::InvariantViolation(
                            "Function execution backfill fetch should only return success",
                        ));
                    }
                }
            }
            LogFetchOrchestratorResponse::RequireCompactionOffsetRepair(_) => {
                return Err(CompactionError::InvariantViolation(
                    "Function execution does not support compaction offset repair",
                ));
            }
        };

        Ok(FunctionExecutionBatch {
            collection_info,
            materialized_log_data,
        })
    }

    pub async fn run(
        self,
        fn_inputs: Vec<(CollectionUuid, i64)>,
        database_name: chroma_types::DatabaseName,
        system: System,
    ) -> Result<CompactionResponse, CompactionError> {
        if fn_inputs.is_empty() {
            return Err(CompactionError::InvariantViolation(
                "Function execution requires at least one input collection",
            ));
        }

        let base_context = self.compaction_context;
        let mut input_batches = Vec::with_capacity(fn_inputs.len());
        for (collection_id, completion_offset) in fn_inputs {
            input_batches.push(
                Box::pin(Self::fetch_function_input_batch(
                    base_context.clone(),
                    collection_id,
                    completion_offset,
                    database_name.clone(),
                    system.clone(),
                ))
                .await?,
            );
        }

        let first_collection_id = input_batches[0].collection_info.collection_id;
        let mut compaction_context = base_context;

        if let Some((function_context, collection_register_info)) = compaction_context
            .run_attached_function_workflow(input_batches, system.clone(), false)
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
            job_id: first_collection_id.into(),
        })
    }
}
