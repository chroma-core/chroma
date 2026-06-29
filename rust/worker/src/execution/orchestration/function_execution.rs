use std::cell::OnceCell;

use chroma_system::System;
use chroma_types::{AttachedFunction, AttachedFunctionUuid, CollectionUuid, DatabaseName};
use uuid::Uuid;

use crate::execution::operators::materialize_logs::MaterializeLogOutput;

use super::{
    compact::{CollectionCompactInfo, CompactionContext, CompactionError, CompactionResponse},
    log_fetch_orchestrator::LogFetchOrchestratorResponse,
};

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

        Ok(FunctionInputCollectionData {
            collection_info,
            materialized_log_data,
        })
    }

    async fn resolve_shared_input_database_name(
        compaction_context: CompactionContext,
        fn_inputs: &[(CollectionUuid, i64)],
    ) -> Result<DatabaseName, CompactionError> {
        let Some((first_input_collection_id, _)) = fn_inputs.first() else {
            return Err(CompactionError::InvariantViolation(
                "Function execution requires at least one input collection",
            ));
        };

        let mut sysdb = compaction_context.sysdb.clone();
        // TODO(tanujnay112): This does not support MCMR yet because work queue records
        // do not carry the database name. Pass the database name from the work queue
        // service and remove this unscoped lookup once that metadata is available.
        let collection_info = sysdb
            .get_collection_with_segments(None, *first_input_collection_id)
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

    #[tracing::instrument(skip(system))]
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
        let shared_database_name =
            Self::resolve_shared_input_database_name(base_context.clone(), &fn_inputs).await?;
        let mut input_collection_data = Vec::with_capacity(fn_inputs.len());
        for (collection_id, completion_offset) in fn_inputs {
            input_collection_data.push(
                Box::pin(Self::fetch_function_input_collection_data(
                    base_context.clone(),
                    collection_id,
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
