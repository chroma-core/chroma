use std::{cell::OnceCell, collections::HashSet};

use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{hnsw_provider::HnswIndexProvider, IndexUuid};
use chroma_log::Log;
use chroma_segment::{
    blockfile_metadata::MetadataSegmentWriter,
    blockfile_record::{RecordSegmentReader, RecordSegmentWriter},
    spann_provider::SpannProvider,
    types::{ChromaSegmentWriter, VectorSegmentWriter},
};
use chroma_sysdb::SysDb;
use chroma_system::{
    wrap, ComponentHandle, Dispatcher, Orchestrator, OrchestratorContext, PanicError, System,
    TaskError,
};
use chroma_types::{Collection, CollectionUuid, JobId, Schema, SegmentUuid};
use opentelemetry::metrics::Counter;
use thiserror::Error;

use super::apply_logs_orchestrator::{ApplyLogsOrchestrator, ApplyLogsOrchestratorError};
use super::attached_function_orchestrator::{
    AttachedFunctionOrchestrator, AttachedFunctionOrchestratorError,
    AttachedFunctionOrchestratorResponse,
};
use super::log_fetch_orchestrator::{
    LogFetchOrchestrator, LogFetchOrchestratorResponse, RequireCompactionOffsetRepair, Success,
};
use super::register_orchestrator::{CollectionRegisterInfo, RegisterOrchestrator};

use crate::execution::{
    operators::{
        get_attached_function::{GetAttachedFunctionInput, GetAttachedFunctionOperator},
        materialize_logs::MaterializeLogOutput,
    },
    orchestration::{
        apply_logs_orchestrator::ApplyLogsOrchestratorResponse,
        attached_function_orchestrator::FunctionContext,
        log_fetch_orchestrator::LogFetchOrchestratorError,
        register_orchestrator::{RegisterOrchestratorError, RegisterOrchestratorResponse},
    },
};

/**  The state of the orchestrator.
In chroma, we have a relatively fixed number of query plans that we can execute. Rather
than a flexible state machine abstraction, we just manually define the states that we
expect to encounter for a given query plan. This is a bit more rigid, but it's also simpler and easier to
understand. We can always add more abstraction later if we need it.

```plaintext
                                                ┌────────────────────────────┐
                                                ├─► Apply logs to segment #1 │
                                                │                            ├──► Commit segment #1 ──► Flush segment #1 ┐
                                                ├─► Apply logs to segment #1 │                                           │
Pending ──► PullLogs/SourceRecord ──► Partition │                            │                                           ├──► Register ─► Finished
                                                ├─► Apply logs to segment #2 │                                           │
                                                │                            ├──► Commit segment #2 ──► Flush segment #2 ┘
                                                ├─► Apply logs to segment #2 │
                                                └────────────────────────────┘
```
*/

#[derive(Debug)]
pub struct CompactionMetrics {
    pub total_logs_applied_flushed: Counter<u64>,
}

impl Default for CompactionMetrics {
    fn default() -> Self {
        let meter = opentelemetry::global::meter("chroma.compactor");
        CompactionMetrics {
            total_logs_applied_flushed: meter
                .u64_counter("total_logs_applied_flushed")
                .with_description(
                    "The total number of log records applied and flushed during compaction",
                )
                .build(),
        }
    }
}

#[derive(Debug)]
pub enum ExecutionState {
    Pending,
    Partition,
    MaterializeApplyCommitFlush,
    Register,
}

#[derive(Clone, Debug)]
pub struct CompactWriters {
    pub(crate) record_reader: Option<RecordSegmentReader<'static>>,
    pub(crate) metadata_writer: MetadataSegmentWriter<'static>,
    pub(crate) record_writer: RecordSegmentWriter,
    pub(crate) vector_writer: VectorSegmentWriter,
}

#[derive(Debug, Clone)]
pub struct CollectionCompactInfo {
    pub collection_id: CollectionUuid,
    pub collection: Collection,
    pub(crate) writers: Option<CompactWriters>,
    pub pulled_log_offset: i64,
    pub hnsw_index_uuid: Option<IndexUuid>,
    pub schema: Option<Schema>,
}

#[derive(Debug)]
pub enum BackfillResult {
    BackfillCompleted {
        function_context: FunctionContext,
        collection_register_info: CollectionRegisterInfo,
    },
    NoBackfillRequired,
}

#[derive(Debug)]
pub struct CompactionContext {
    pub collection_info: OnceCell<CollectionCompactInfo>,
    pub log: Log,
    pub sysdb: SysDb,
    pub blockfile_provider: BlockfileProvider,
    pub hnsw_provider: HnswIndexProvider,
    pub spann_provider: SpannProvider,
    pub dispatcher: ComponentHandle<Dispatcher>,
    pub orchestrator_context: OrchestratorContext,
    pub is_rebuild: bool,
    pub fetch_log_batch_size: u32,
    pub max_compaction_size: usize,
    pub max_partition_size: usize,
    pub hnsw_index_uuids: HashSet<IndexUuid>, // TODO(tanujnay112): Remove after direct hnsw is solidified
    pub is_function_disabled: bool,
    #[cfg(test)]
    pub poison_offset: Option<u32>,
}

impl Clone for CompactionContext {
    fn clone(&self) -> Self {
        let orchestrator_context = OrchestratorContext::new(self.dispatcher.clone());
        Self {
            collection_info: self.collection_info.clone(),
            log: self.log.clone(),
            sysdb: self.sysdb.clone(),
            blockfile_provider: self.blockfile_provider.clone(),
            hnsw_provider: self.hnsw_provider.clone(),
            spann_provider: self.spann_provider.clone(),
            dispatcher: self.dispatcher.clone(),
            orchestrator_context,
            is_rebuild: self.is_rebuild,
            fetch_log_batch_size: self.fetch_log_batch_size,
            max_compaction_size: self.max_compaction_size,
            max_partition_size: self.max_partition_size,
            hnsw_index_uuids: self.hnsw_index_uuids.clone(),
            is_function_disabled: self.is_function_disabled,
            #[cfg(test)]
            poison_offset: self.poison_offset,
        }
    }
}

impl CompactionContext {
    /// Create an empty output context for attached function orchestrator
    /// This creates a new context with an empty collection_info OnceCell
    fn clone_for_new_collection(&self) -> Self {
        let orchestrator_context = OrchestratorContext::new(self.dispatcher.clone());
        Self {
            collection_info: OnceCell::new(), // Start empty for output context
            log: self.log.clone(),
            sysdb: self.sysdb.clone(),
            blockfile_provider: self.blockfile_provider.clone(),
            hnsw_provider: self.hnsw_provider.clone(),
            spann_provider: self.spann_provider.clone(),
            dispatcher: self.dispatcher.clone(),
            orchestrator_context,
            is_rebuild: self.is_rebuild,
            fetch_log_batch_size: self.fetch_log_batch_size,
            max_compaction_size: self.max_compaction_size,
            max_partition_size: self.max_partition_size,
            hnsw_index_uuids: self.hnsw_index_uuids.clone(),
            is_function_disabled: self.is_function_disabled,
            #[cfg(test)]
            poison_offset: self.poison_offset,
        }
    }
}

#[derive(Error, Debug)]
pub enum CompactionError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error applying data to segment writers: {0}")]
    ApplyDataError(#[from] ApplyLogsOrchestratorError),
    #[error("Error executing attached function: {0}")]
    AttachedFunction(#[from] AttachedFunctionOrchestratorError),
    #[error("Error fetching compaction context: {0}")]
    CompactionContextError(#[from] CompactionContextError),
    #[error("Error fetching logs: {0}")]
    DataFetchError(#[from] LogFetchOrchestratorError),
    #[error("Error registering collection: {0}")]
    RegisterError(#[from] RegisterOrchestratorError),
    #[error("Panic during compaction: {0}")]
    PanicError(#[from] PanicError),
    #[error("Invariant violation: {}", .0)]
    InvariantViolation(&'static str),
}

impl<E> From<TaskError<E>> for CompactionError
where
    E: Into<CompactionError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Aborted => CompactionError::Aborted,
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

impl ChromaError for CompactionError {
    fn code(&self) -> ErrorCodes {
        match self {
            CompactionError::Aborted => ErrorCodes::Aborted,
            CompactionError::ApplyDataError(e) => e.code(),
            CompactionError::AttachedFunction(e) => e.code(),
            CompactionError::CompactionContextError(e) => e.code(),
            CompactionError::DataFetchError(e) => e.code(),
            CompactionError::RegisterError(e) => e.code(),
            CompactionError::PanicError(e) => e.code(),
            CompactionError::InvariantViolation(_) => ErrorCodes::Internal,
        }
    }

    fn should_trace_error(&self) -> bool {
        match self {
            Self::Aborted => true,
            Self::ApplyDataError(e) => e.should_trace_error(),
            Self::AttachedFunction(e) => e.should_trace_error(),
            Self::CompactionContextError(e) => e.should_trace_error(),
            Self::DataFetchError(e) => e.should_trace_error(),
            Self::PanicError(e) => e.should_trace_error(),
            Self::RegisterError(e) => e.should_trace_error(),
            Self::InvariantViolation(_) => true,
        }
    }
}

#[derive(Error, Debug)]
pub enum CompactionContextError {
    #[error("Invariant violation: {0}")]
    InvariantViolation(&'static str),
}

impl ChromaError for CompactionContextError {
    fn code(&self) -> ErrorCodes {
        match self {
            CompactionContextError::InvariantViolation(_) => ErrorCodes::Internal,
        }
    }

    fn should_trace_error(&self) -> bool {
        match self {
            CompactionContextError::InvariantViolation(_) => true,
        }
    }
}

impl CompactionContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        is_rebuild: bool,
        fetch_log_batch_size: u32,
        max_compaction_size: usize,
        max_partition_size: usize,
        log: Log,
        sysdb: SysDb,
        blockfile_provider: BlockfileProvider,
        hnsw_provider: HnswIndexProvider,
        spann_provider: SpannProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        is_function_disabled: bool,
    ) -> Self {
        let orchestrator_context = OrchestratorContext::new(dispatcher.clone());
        CompactionContext {
            collection_info: OnceCell::new(),
            is_rebuild,
            fetch_log_batch_size,
            max_compaction_size,
            max_partition_size,
            log,
            sysdb,
            blockfile_provider,
            hnsw_provider,
            spann_provider,
            dispatcher,
            orchestrator_context,
            hnsw_index_uuids: HashSet::new(),
            is_function_disabled,
            #[cfg(test)]
            poison_offset: None,
        }
    }

    #[cfg(test)]
    pub fn set_poison_offset(&mut self, offset: u32) {
        self.poison_offset = Some(offset);
    }

    pub fn get_collection_info(&self) -> Result<&CollectionCompactInfo, CompactionContextError> {
        self.collection_info
            .get()
            .ok_or(CompactionContextError::InvariantViolation(
                "Collection info should have been set",
            ))
    }

    pub fn get_segment_writers(&self) -> Result<CompactWriters, CompactionContextError> {
        self.get_collection_info()?.writers.clone().ok_or(
            CompactionContextError::InvariantViolation("Segment writers should have been set"),
        )
    }

    pub fn get_collection_info_mut(
        &mut self,
    ) -> Result<&mut CollectionCompactInfo, CompactionContextError> {
        self.collection_info
            .get_mut()
            .ok_or(CompactionContextError::InvariantViolation(
                "Collection info mut should have been set",
            ))
    }

    pub fn get_segment_writer_by_id(
        &self,
        segment_id: SegmentUuid,
    ) -> Result<ChromaSegmentWriter<'static>, CompactionContextError> {
        let writers = self.get_segment_writers()?;

        if writers.metadata_writer.id == segment_id {
            return Ok(ChromaSegmentWriter::MetadataSegment(
                writers.metadata_writer,
            ));
        }

        if writers.record_writer.id == segment_id {
            return Ok(ChromaSegmentWriter::RecordSegment(writers.record_writer));
        }

        if writers.vector_writer.get_id() == segment_id {
            return Ok(ChromaSegmentWriter::VectorSegment(writers.vector_writer));
        }

        Err(CompactionContextError::InvariantViolation(
            "Segment id should match one of the writer segment id",
        ))
    }

    pub(crate) async fn run_get_logs(
        &mut self,
        collection_id: CollectionUuid,
        system: System,
        is_getting_compacted_logs: bool,
    ) -> Result<LogFetchOrchestratorResponse, LogFetchOrchestratorError> {
        // TODO(tanujnay112): This is awful, we need to find a better way to pass
        // the active collection info around.
        self.collection_info = OnceCell::new();
        let log_fetch_orchestrator = LogFetchOrchestrator::new(
            collection_id,
            self.is_rebuild || is_getting_compacted_logs,
            self.fetch_log_batch_size,
            self.max_compaction_size,
            self.max_partition_size,
            self.log.clone(),
            self.sysdb.clone(),
            self.blockfile_provider.clone(),
            self.hnsw_provider.clone(),
            self.spann_provider.clone(),
            self.dispatcher.clone(),
        );

        let log_fetch_response = match log_fetch_orchestrator.run(system.clone()).await {
            Ok(response) => response,
            Err(e) => {
                if e.should_trace_error() {
                    tracing::error!("Data fetch phase failed: {e}");
                }
                return Err(e);
            }
        };

        match log_fetch_response {
            LogFetchOrchestratorResponse::Success(success) => {
                let materialized = success.materialized;
                let collection_info = success.collection_info;

                self.collection_info
                    .set(collection_info.clone())
                    .map_err(|_| {
                        CompactionContextError::InvariantViolation("Collection info already set")
                    })?;

                if let Some(hnsw_index_uuid) = collection_info.hnsw_index_uuid {
                    self.hnsw_index_uuids.insert(hnsw_index_uuid);
                }

                Ok(Success::new(materialized, collection_info.clone()).into())
            }
            LogFetchOrchestratorResponse::RequireCompactionOffsetRepair(repair) => Ok(
                RequireCompactionOffsetRepair::new(repair.job_id, repair.witnessed_offset_in_sysdb)
                    .into(),
            ),
            LogFetchOrchestratorResponse::RequireFunctionBackfill(backfill) => {
                if let Some(hnsw_index_uuid) = backfill.collection_info.hnsw_index_uuid {
                    self.hnsw_index_uuids.insert(hnsw_index_uuid);
                };

                tracing::info!(
                    "Backfilling collection {}",
                    backfill.collection_info.collection_id
                );

                self.collection_info
                    .set(backfill.collection_info.clone())
                    .map_err(|_| {
                        CompactionContextError::InvariantViolation("Collection info already set")
                    })?;

                Ok(LogFetchOrchestratorResponse::RequireFunctionBackfill(
                    backfill,
                ))
            }
        }
    }

    pub(crate) async fn run_apply_logs(
        &mut self,
        log_fetch_records: Vec<MaterializeLogOutput>,
        system: System,
    ) -> Result<ApplyLogsOrchestratorResponse, ApplyLogsOrchestratorError> {
        let collection_info = self.get_collection_info()?;
        if log_fetch_records.is_empty() {
            return Ok(ApplyLogsOrchestratorResponse::new_with_empty_results(
                collection_info.collection_id.into(),
                collection_info,
            ));
        }

        if self.get_collection_info().is_err() {
            return Err(ApplyLogsOrchestratorError::InvariantViolation(
                "Output collection info should have been set before running apply logs",
            ));
        }

        // INVARIANT: Every element of log_fetch_records should be non-empty
        for mat_logs in log_fetch_records.iter() {
            if mat_logs.result.is_empty() {
                return Err(ApplyLogsOrchestratorError::InvariantViolation(
                    "Every element of log_fetch_records should be non-empty",
                ));
            }
        }

        let apply_logs_orchestrator = ApplyLogsOrchestrator::new(self, Some(log_fetch_records));

        let apply_logs_response = match apply_logs_orchestrator.run(system).await {
            Ok(response) => response,
            Err(e) => {
                if e.should_trace_error() {
                    tracing::error!("Apply data phase failed: {e}");
                }
                return Err(e);
            }
        };

        let collection_info = self.get_collection_info_mut()?;
        collection_info.schema = apply_logs_response.schema.clone();
        collection_info.collection.total_records_post_compaction =
            apply_logs_response.total_records_post_compaction;

        Ok(apply_logs_response)
    }

    // Should be invoked on output collection context
    pub(crate) async fn run_attached_function(
        &mut self,
        data_fetch_records: Vec<MaterializeLogOutput>,
        system: System,
        is_backfill: bool,
    ) -> Result<AttachedFunctionOrchestratorResponse, AttachedFunctionOrchestratorError> {
        let collection_info = self.get_collection_info()?.clone();
        let attached_function_orchestrator = AttachedFunctionOrchestrator::new(
            collection_info,
            self.clone_for_new_collection(),
            self.dispatcher.clone(),
            data_fetch_records,
            is_backfill,
        );

        let attached_function_response =
            match Box::pin(attached_function_orchestrator.run(system)).await {
                Ok(response) => response,
                Err(e) => {
                    if e.should_trace_error() {
                        tracing::error!("Attached function phase failed: {e}");
                    }
                    return Err(e);
                }
            };

        // Set the output collection info based on the response
        match &attached_function_response {
            AttachedFunctionOrchestratorResponse::NoAttachedFunction { .. } => {}
            AttachedFunctionOrchestratorResponse::Success {
                output_collection_info,
                ..
            } => {
                // We are replacing the output collection info with the attached function output
                self.collection_info = OnceCell::from(output_collection_info.clone());

                if let Some(hnsw_index_uuid) = output_collection_info.hnsw_index_uuid {
                    self.hnsw_index_uuids.insert(hnsw_index_uuid);
                }
            }
        }

        Ok(attached_function_response)
    }

    async fn run_regular_compaction_workflow(
        &mut self,
        log_fetch_records: Vec<MaterializeLogOutput>,
        system: System,
    ) -> Result<CollectionRegisterInfo, CompactionError> {
        let apply_logs_response = self.run_apply_logs(log_fetch_records, system).await?;

        // Build CollectionRegisterInfo from the updated context
        let collection_info = self
            .get_collection_info()
            .map_err(CompactionError::CompactionContextError)?
            .clone();

        Ok(CollectionRegisterInfo {
            collection_info,
            flush_results: apply_logs_response.flush_results,
            collection_logical_size_bytes: apply_logs_response.collection_logical_size_bytes,
        })
    }

    async fn needs_backfill(&mut self) -> Result<bool, CompactionError> {
        let collection_info = self.get_collection_info()?;
        let collection_id = collection_info.collection_id;
        let log_position = collection_info.collection.log_position;

        // Create the operator and wrap it as a task
        let operator = Box::new(GetAttachedFunctionOperator::new(
            self.sysdb.clone(),
            collection_id,
        ));
        let input = GetAttachedFunctionInput { collection_id };

        // Create a receiver for the task
        let (receiver, rx) = chroma_system::OneshotMessageReceiver::new();

        // Wrap the operator as a task
        let task = wrap(
            operator,
            input,
            Box::new(receiver),
            self.orchestrator_context.task_cancellation_token.clone(),
        );

        // Send the task to the dispatcher
        self.dispatcher
            .send(task, Some(tracing::Span::current()))
            .await
            .map_err(|_| {
                CompactionError::InvariantViolation(
                    "Failed to send GetAttachedFunction task to dispatcher",
                )
            })?;

        // Wait for the result
        let task_result = rx.await.map_err(|_| {
            CompactionError::InvariantViolation("Failed to receive GetAttachedFunction task result")
        })?;

        let output = task_result
            .into_inner()
            .map_err(|_| CompactionError::InvariantViolation("GetAttachedFunction task failed"))?;

        // Check if we have an attached function
        match output.attached_function {
            Some(function) => {
                // Check if backfill is needed by comparing offsets
                // log_position is i64, completion_offset is u64
                let log_position_u64 = log_position.max(0) as u64;
                if log_position_u64 < function.completion_offset {
                    return Err(CompactionError::InvariantViolation(
                        "Log position is less than completion offset",
                    ));
                }
                Ok(function.completion_offset < log_position_u64)
            }
            None => Ok(false), // No attached function means no backfill needed
        }
    }

    async fn run_backfill_attached_function_workflow(
        &mut self,
        system: System,
    ) -> Result<BackfillResult, CompactionError> {
        // See if we need backfill
        if !self.needs_backfill().await? {
            tracing::debug!("No backfill needed");
            return Ok(BackfillResult::NoBackfillRequired);
        }

        tracing::debug!("Backfill needed");

        let log_fetch_records = match self
            .run_get_logs(
                self.get_collection_info().map_err(CompactionError::CompactionContextError)?.collection_id,
                system.clone(),
                true,
            )
            .await?
        {
            LogFetchOrchestratorResponse::Success(success) => success.materialized,
            LogFetchOrchestratorResponse::RequireCompactionOffsetRepair(_)
            | LogFetchOrchestratorResponse::RequireFunctionBackfill(_) => {
                return Err(CompactionError::InvariantViolation(
                    "Attached function backfill log fetch should not return compaction offset repair or function backfill",
                ))
            }
        };

        let result =
            Box::pin(self.run_attached_function_workflow(log_fetch_records, system, true)).await?;

        match result {
            Some((function_context, collection_register_info)) => {
                Ok(BackfillResult::BackfillCompleted {
                    function_context,
                    collection_register_info,
                })
            }
            None => Ok(BackfillResult::NoBackfillRequired),
        }
    }

    async fn run_attached_function_workflow(
        &mut self,
        log_fetch_records: Vec<MaterializeLogOutput>,
        system: System,
        is_backfill: bool,
    ) -> Result<Option<(FunctionContext, CollectionRegisterInfo)>, CompactionError> {
        let attached_function_result =
            Box::pin(self.run_attached_function(log_fetch_records, system.clone(), is_backfill))
                .await?;

        match attached_function_result {
            AttachedFunctionOrchestratorResponse::NoAttachedFunction { .. } => Ok(None),
            AttachedFunctionOrchestratorResponse::Success {
                job_id: _,
                output_collection_info,
                materialized_output,
                function_context,
            } => {
                // Update self to use the output collection for apply_logs
                self.collection_info = OnceCell::from(output_collection_info.clone());

                // Apply materialized output to output collection
                let apply_logs_response = self
                    .run_apply_logs(materialized_output, system.clone())
                    .await?;

                // Get updated collection info after running apply logs.
                let output_collection_info = self.get_collection_info()?;

                let collection_register_info = CollectionRegisterInfo {
                    collection_info: output_collection_info.clone(),
                    flush_results: apply_logs_response.flush_results,
                    collection_logical_size_bytes: apply_logs_response
                        .collection_logical_size_bytes,
                };

                Ok(Some((function_context, collection_register_info)))
            }
        }
    }

    pub(crate) async fn run_register(
        &mut self,
        collection_register_infos: Vec<CollectionRegisterInfo>,
        function_register_info: Option<FunctionContext>,
        system: System,
    ) -> Result<RegisterOrchestratorResponse, RegisterOrchestratorError> {
        let dispatcher = self.dispatcher.clone();

        if collection_register_infos.is_empty() || collection_register_infos.len() > 2 {
            return Err(RegisterOrchestratorError::InvariantViolation(
                "Invalid number of collection register infos",
            ));
        }

        let register_orchestrator = RegisterOrchestrator::new(
            self,
            dispatcher,
            collection_register_infos,
            function_register_info,
        );

        match register_orchestrator.run(system).await {
            Ok(response) => Ok(response),
            Err(e) => {
                if e.should_trace_error() {
                    tracing::error!("Register phase failed: {e}");
                }
                Err(e)
            }
        }
    }

    pub(crate) async fn run_compaction(
        &mut self,
        collection_id: CollectionUuid,
        system: System,
    ) -> Result<CompactionResponse, CompactionError> {
        let result = self
            .run_get_logs(collection_id, system.clone(), false)
            .await?;

        let (log_fetch_records, _) = match result {
            LogFetchOrchestratorResponse::Success(success) => {
                (success.materialized, success.collection_info)
            }
            LogFetchOrchestratorResponse::RequireCompactionOffsetRepair(repair) => {
                return Ok(CompactionResponse::RequireCompactionOffsetRepair {
                    job_id: repair.job_id,
                    witnessed_offset_in_sysdb: repair.witnessed_offset_in_sysdb,
                });
            }
            LogFetchOrchestratorResponse::RequireFunctionBackfill(backfill) => {
                // Skip backfill workflow if function is disabled for this collection
                if self.is_function_disabled {
                    tracing::info!(
                        collection_id = %collection_id,
                        "Skipping function backfill workflow for disabled collection"
                    );
                    (backfill.materialized, backfill.collection_info)
                } else {
                    // Try to run backfill workflow
                    let fn_result =
                        Box::pin(self.run_backfill_attached_function_workflow(system.clone()))
                            .await?;

                    match fn_result {
                        BackfillResult::BackfillCompleted {
                            function_context,
                            collection_register_info,
                        } => {
                            // Backfill was needed and completed - register and return
                            let results = vec![collection_register_info];
                            Box::pin(self.run_register(
                                results,
                                Some(function_context),
                                system.clone(),
                            ))
                            .await?;

                            // TODO(tanujnay112): Should we look into just doing the rest of the compaction workflow
                            // instead of exiting here?

                            return Ok(CompactionResponse::Success {
                                job_id: collection_id.into(),
                            });
                        }
                        BackfillResult::NoBackfillRequired => {
                            // No backfill was needed - reuse the already-fetched logs
                            (backfill.materialized, backfill.collection_info)
                        }
                    }
                }
            }
        };

        // Wrap in Arc to avoid cloning large MaterializeLogOutput data
        let log_fetch_records_clone = log_fetch_records.clone();

        let mut self_clone_fn = self.clone();
        // TODO(tanujnay112): Think about a better way to pass mutable state to these futures
        let mut self_clone_compact = self.clone();
        let system_clone_fn = system.clone();
        let system_clone_compact = system.clone();

        // 1. Attached function execution + apply output to output collection
        // 2. Apply input logs to input collection
        // Box the futures to avoid stack overflow with large state machines
        let is_function_disabled = self.is_function_disabled;
        let fn_future = async move {
            if is_function_disabled {
                tracing::info!("Skipping attached function workflow for disabled collection");
                Ok(None)
            } else {
                Box::pin(self_clone_fn.run_attached_function_workflow(
                    log_fetch_records_clone,
                    system_clone_fn,
                    false,
                ))
                .await
            }
        };

        let compact_future = Box::pin(async move {
            self_clone_compact
                .run_regular_compaction_workflow(log_fetch_records, system_clone_compact)
                .await
        });

        let (fn_result, compact_result) = tokio::try_join!(fn_future, compact_future)?;

        // Collect results
        let mut attached_function_context = None;
        let mut results: Vec<CollectionRegisterInfo> = Vec::new();

        if let Some((function_context, collection_register_info)) = fn_result {
            attached_function_context = Some(function_context);
            results.push(collection_register_info);
        }
        // Otherwise there was no attached function

        // Process input collection result
        // Invariant: flush_results is empty => collection_logical_size_bytes == collection_info.collection.size_bytes_post_compaction
        if compact_result.flush_results.is_empty()
            && compact_result.collection_logical_size_bytes
                != compact_result
                    .collection_info
                    .collection
                    .size_bytes_post_compaction
        {
            return Err(CompactionError::InvariantViolation(
                "Collection logical size bytes should be equal to whatever it started with",
            ));
        }

        results.push(compact_result);

        let _ =
            Box::pin(self.run_register(results, attached_function_context, system.clone())).await?;

        Ok(CompactionResponse::Success {
            job_id: collection_id.into(),
        })
    }

    pub(crate) async fn cleanup(self) {
        if self.hnsw_provider.use_direct_hnsw {
            return;
        }

        // TODO(tanujnay112): Remove when use_direct_hnsw is fully deprecated
        for hnsw_index_uuid in self.hnsw_index_uuids {
            let _ = HnswIndexProvider::purge_one_id(
                self.hnsw_provider.temporary_storage_path.as_path(),
                hnsw_index_uuid,
            )
            .await;
        }
    }
}

// ============== Component Implementation ==============
#[derive(Debug)]
pub enum CompactionResponse {
    Success {
        job_id: JobId,
    },
    RequireCompactionOffsetRepair {
        job_id: JobId,
        witnessed_offset_in_sysdb: i64,
    },
}

#[allow(clippy::too_many_arguments)]
pub async fn compact(
    system: System,
    collection_id: CollectionUuid,
    is_rebuild: bool,
    fetch_log_batch_size: u32,
    max_compaction_size: usize,
    max_partition_size: usize,
    log: Log,
    sysdb: SysDb,
    blockfile_provider: BlockfileProvider,
    hnsw_index_provider: HnswIndexProvider,
    spann_provider: SpannProvider,
    dispatcher: ComponentHandle<Dispatcher>,
    is_function_disabled: bool,
    #[cfg(test)] poison_offset: Option<u32>,
) -> Result<CompactionResponse, CompactionError> {
    let mut compaction_context = CompactionContext::new(
        is_rebuild,
        fetch_log_batch_size,
        max_compaction_size,
        max_partition_size,
        log.clone(),
        sysdb.clone(),
        blockfile_provider.clone(),
        hnsw_index_provider.clone(),
        spann_provider.clone(),
        dispatcher.clone(),
        is_function_disabled,
    );

    #[cfg(test)]
    if let Some(poison_offset) = poison_offset {
        compaction_context.set_poison_offset(poison_offset);
    }

    let result = Box::pin(compaction_context.run_compaction(collection_id, system)).await;
    Box::pin(compaction_context.cleanup()).await;
    result
}

#[cfg(test)]
mod tests {
    use chroma_log::test::{
        add_delete_net_zero_generator, upsert_generator, TEST_EMBEDDING_DIMENSION,
    };
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use tokio::fs;

    use chroma_blockstore::arrow::config::{BlockManagerConfig, TEST_MAX_BLOCK_SIZE_BYTES};
    use chroma_blockstore::provider::BlockfileProvider;
    use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
    use chroma_config::{registry::Registry, Configurable};
    use chroma_index::config::{HnswGarbageCollectionConfig, PlGarbageCollectionConfig};
    use chroma_index::spann::types::SpannMetrics;
    use chroma_index::{hnsw_provider::HnswIndexProvider, spann::types::GarbageCollectionContext};
    use chroma_log::{
        in_memory_log::{InMemoryLog, InternalLogRecord},
        test::{add_delete_generator, LogGenerator},
        Log,
    };
    use chroma_segment::{spann_provider::SpannProvider, test::TestDistributedSegment};
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_sysdb::{SysDb, TestSysDb};
    use chroma_system::{ComponentHandle, Dispatcher, DispatcherConfig, Orchestrator, System};
    use chroma_types::{
        operator::{Filter, Limit, Projection, ProjectionRecord},
        Collection, DocumentExpression, DocumentOperator, MetadataExpression, PrimitiveOperator,
        Segment, SegmentUuid, Where,
    };
    use regex::Regex;
    use tempfile;

    use crate::{
        config::RootConfig,
        execution::{operators::fetch_log::FetchLogOperator, orchestration::get::GetOrchestrator},
    };

    use super::{compact, CompactionContext, CompactionResponse, LogFetchOrchestratorResponse};
    use crate::execution::orchestration::register_orchestrator::CollectionRegisterInfo;

    async fn get_all_records(
        system: &System,
        dispatcher_handle: &ComponentHandle<Dispatcher>,
        blockfile_provider: BlockfileProvider,
        log: Log,
        cas: chroma_types::CollectionAndSegments,
    ) -> HashMap<String, ProjectionRecord> {
        let fetch_log = FetchLogOperator {
            log_client: log,
            batch_size: 50,
            start_log_offset_id: u64::try_from(cas.collection.log_position + 1).unwrap_or_default(),
            maximum_fetch_count: None,
            collection_uuid: cas.collection.collection_id,
            tenant: cas.collection.tenant.clone(),
        };

        let filter = Filter {
            query_ids: None,
            where_clause: None,
        };

        let limit = Limit {
            offset: 0,
            limit: None,
        };

        let project = Projection {
            document: true,
            embedding: true,
            metadata: true,
        };

        let get_orchestrator = GetOrchestrator::new(
            blockfile_provider,
            dispatcher_handle.clone(),
            1000,
            cas,
            fetch_log,
            filter,
            limit,
            project,
        );

        let result = get_orchestrator
            .run(system.clone())
            .await
            .expect("Get orchestrator should not fail");

        result
            .result
            .records
            .into_iter()
            .map(|record| (record.id.clone(), record))
            .collect()
    }

    #[tokio::test]
    async fn test_rebuild() {
        let config = RootConfig::default();
        let system = System::default();
        let registry = Registry::new();
        let dispatcher = Dispatcher::try_from_config(&config.query_service.dispatcher, &registry)
            .await
            .expect("Should be able to initialize dispatcher");
        let dispatcher_handle = system.start_component(dispatcher);
        let mut sysdb = SysDb::Test(TestSysDb::new());
        let test_segments = TestDistributedSegment::new().await;
        let collection_id = test_segments.collection.collection_id;
        sysdb
            .create_collection(
                test_segments.collection.tenant,
                test_segments.collection.database,
                collection_id,
                test_segments.collection.name,
                vec![
                    test_segments.record_segment.clone(),
                    test_segments.metadata_segment.clone(),
                    test_segments.vector_segment.clone(),
                ],
                None,
                None,
                None,
                test_segments.collection.dimension,
                false,
            )
            .await
            .expect("Colleciton create should be successful");
        let mut in_memory_log = InMemoryLog::new();
        add_delete_generator
            .generate_vec(1..=120)
            .into_iter()
            .for_each(|log| {
                in_memory_log.add_log(
                    collection_id,
                    InternalLogRecord {
                        collection_id,
                        log_offset: log.log_offset - 1,
                        log_ts: log.log_offset,
                        record: log,
                    },
                )
            });
        let log = Log::InMemory(in_memory_log);

        let compact_result = Box::pin(compact(
            system.clone(),
            collection_id,
            false,
            50,
            1000,
            50,
            log.clone(),
            sysdb.clone(),
            test_segments.blockfile_provider.clone(),
            test_segments.hnsw_provider.clone(),
            test_segments.spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
            None,
        ))
        .await;
        assert!(compact_result.is_ok());

        let old_cas = sysdb
            .get_collection_with_segments(collection_id)
            .await
            .expect("Collection and segment information should be present");

        let fetch_log = FetchLogOperator {
            log_client: log.clone(),
            batch_size: 50,
            start_log_offset_id: u64::try_from(old_cas.collection.log_position + 1)
                .unwrap_or_default(),
            maximum_fetch_count: None,
            collection_uuid: collection_id,
            tenant: old_cas.collection.tenant.clone(),
        };
        let filter = Filter {
            query_ids: None,
            where_clause: Some(Where::disjunction(vec![
                Where::Metadata(MetadataExpression {
                    key: "is_even".to_string(),
                    comparison: chroma_types::MetadataComparison::Primitive(
                        PrimitiveOperator::Equal,
                        chroma_types::MetadataValue::Bool(true),
                    ),
                }),
                Where::Document(DocumentExpression {
                    operator: DocumentOperator::Contains,
                    pattern: "<cat>".to_string(),
                }),
            ])),
        };
        let limit = Limit {
            offset: 0,
            limit: None,
        };
        let project = Projection {
            document: true,
            embedding: true,
            metadata: true,
        };
        let get_orchestrator = GetOrchestrator::new(
            test_segments.blockfile_provider.clone(),
            dispatcher_handle.clone(),
            1000,
            old_cas.clone(),
            fetch_log.clone(),
            filter.clone(),
            limit.clone(),
            project.clone(),
        );

        let old_vals = get_orchestrator
            .run(system.clone())
            .await
            .expect("Get orchestrator should not fail");

        assert!(!old_vals.result.records.is_empty());

        let rebuild_result = Box::pin(compact(
            system.clone(),
            collection_id,
            true,
            5000,
            10000,
            1000,
            log,
            sysdb.clone(),
            test_segments.blockfile_provider.clone(),
            test_segments.hnsw_provider.clone(),
            test_segments.spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
            None,
        ))
        .await;
        assert!(rebuild_result.is_ok());

        let new_cas = sysdb
            .get_collection_with_segments(collection_id)
            .await
            .expect("Collection and segment information should be present");

        let mut expected_new_collection = old_cas.collection.clone();
        expected_new_collection.version += 1;

        let version_suffix_re = Regex::new(r"/\d+$").unwrap();

        expected_new_collection.version_file_path = Some(
            version_suffix_re
                .replace(&old_cas.collection.version_file_path.clone().unwrap(), "/2")
                .to_string(),
        );
        assert_eq!(new_cas.collection, expected_new_collection);
        assert_eq!(new_cas.metadata_segment.id, old_cas.metadata_segment.id);
        assert_eq!(new_cas.record_segment.id, old_cas.record_segment.id);
        assert_eq!(new_cas.vector_segment.id, old_cas.vector_segment.id);
        assert_ne!(
            new_cas.metadata_segment.file_path,
            old_cas.metadata_segment.file_path
        );
        assert_ne!(
            new_cas.record_segment.file_path,
            old_cas.record_segment.file_path
        );
        assert_ne!(
            new_cas.vector_segment.file_path,
            old_cas.vector_segment.file_path
        );

        let get_orchestrator = GetOrchestrator::new(
            test_segments.blockfile_provider.clone(),
            dispatcher_handle,
            1000,
            new_cas,
            fetch_log,
            filter,
            limit,
            project,
        );

        let new_vals = get_orchestrator
            .run(system)
            .await
            .expect("Get orchestrator should not fail");

        assert_eq!(new_vals, old_vals);
    }

    #[tokio::test]
    async fn test_rebuild_empty_filepath() {
        let config = RootConfig::default();
        let system = System::default();
        let registry = Registry::new();
        let dispatcher = Dispatcher::try_from_config(&config.query_service.dispatcher, &registry)
            .await
            .expect("Should be able to initialize dispatcher");
        let dispatcher_handle = system.start_component(dispatcher);
        let mut sysdb = SysDb::Test(TestSysDb::new());
        let test_segments = TestDistributedSegment::new().await;
        let collection_id = test_segments.collection.collection_id;
        sysdb
            .create_collection(
                test_segments.collection.tenant,
                test_segments.collection.database,
                collection_id,
                test_segments.collection.name,
                vec![
                    test_segments.record_segment.clone(),
                    test_segments.metadata_segment.clone(),
                    test_segments.vector_segment.clone(),
                ],
                None,
                None,
                None,
                test_segments.collection.dimension,
                false,
            )
            .await
            .expect("Colleciton create should be successful");
        let in_memory_log = InMemoryLog::new();
        let log = Log::InMemory(in_memory_log);

        let rebuild_result = Box::pin(compact(
            system.clone(),
            collection_id,
            true,
            5000,
            10000,
            1000,
            log,
            sysdb.clone(),
            test_segments.blockfile_provider.clone(),
            test_segments.hnsw_provider.clone(),
            test_segments.spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
            None,
        ))
        .await;
        assert!(rebuild_result.is_ok());

        let new_cas = sysdb
            .get_collection_with_segments(collection_id)
            .await
            .expect("Collection and segment information should be present");

        assert!(new_cas.metadata_segment.file_path.is_empty());
        assert!(new_cas.record_segment.file_path.is_empty());
        assert!(new_cas.vector_segment.file_path.is_empty());
    }

    #[tokio::test]
    async fn test_some_empty_partitions() {
        let mut log = Log::InMemory(InMemoryLog::new());
        let in_memory_log = match log {
            Log::InMemory(ref mut log) => log,
            _ => panic!("Expected InMemoryLog"),
        };

        let tmpdir = tempfile::tempdir().unwrap();
        tokio::fs::remove_dir_all(tmpdir.path())
            .await
            .expect("Failed to remove temp dir");
        let storage = Storage::Local(LocalStorage::new(tmpdir.path().to_str().unwrap()));

        let tenant = "tenant_log_repair".to_string();
        let collection = Collection {
            name: "collection_log_repair".to_string(),
            dimension: Some(TEST_EMBEDDING_DIMENSION.try_into().unwrap()),
            tenant: tenant.clone(),
            database: "database_log_repair".to_string(),
            log_position: -1,
            ..Default::default()
        };

        let collection_uuid = collection.collection_id;

        // Add some log records
        add_delete_generator
            .generate_vec(1..=60)
            .into_iter()
            .for_each(|log| {
                in_memory_log.add_log(
                    collection_uuid,
                    InternalLogRecord {
                        collection_id: collection_uuid,
                        log_offset: log.log_offset - 1,
                        log_ts: log.log_offset,
                        record: log,
                    },
                )
            });

        let mut sysdb = SysDb::Test(TestSysDb::new());
        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_collection(collection);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let record_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        let hnsw_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        let metadata_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_segment(record_segment);
                sysdb.add_segment(hnsw_segment);
                sysdb.add_segment(metadata_segment);
                sysdb.add_tenant_last_compaction_time(tenant, 1);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let hnsw_cache = new_non_persistent_cache_for_test();
        let gc_context = GarbageCollectionContext::try_from_config(
            &(
                PlGarbageCollectionConfig::default(),
                HnswGarbageCollectionConfig::default(),
            ),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");
        let blockfile_provider = BlockfileProvider::new_arrow(
            storage.clone(),
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmpdir.path()),
            hnsw_cache,
            16,
            false,
        );
        let spann_provider = SpannProvider {
            hnsw_provider: hnsw_provider.clone(),
            blockfile_provider: blockfile_provider.clone(),
            garbage_collection_context: gc_context,
            metrics: SpannMetrics::default(),
            pl_block_size: 5 * 1024 * 1024,
            adaptive_search_nprobe: true,
        };

        let config = RootConfig::default();
        let system = System::default();
        let registry = Registry::new();
        let dispatcher = Dispatcher::try_from_config(&config.query_service.dispatcher, &registry)
            .await
            .expect("Should be able to initialize dispatcher");
        let dispatcher_handle = system.start_component(dispatcher);

        let old_cas = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap();

        let old_records = get_all_records(
            &system,
            &dispatcher_handle,
            blockfile_provider.clone(),
            log.clone(),
            old_cas,
        )
        .await;

        let first_compaction_result = Box::pin(compact(
            system.clone(),
            collection_uuid,
            false,
            5000,
            10000,
            1,
            log.clone(),
            sysdb.clone(),
            blockfile_provider.clone(),
            hnsw_provider.clone(),
            spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
            None,
        ))
        .await;

        first_compaction_result.expect("Should succeed");

        let collection = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap()
            .collection;
        assert_eq!(collection.log_position, 60);
        assert_eq!(collection.version, 1);

        let new_cas = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap();
        let new_records = get_all_records(
            &system,
            &dispatcher_handle,
            blockfile_provider.clone(),
            log.clone(),
            new_cas,
        )
        .await;
        assert_eq!(old_records, new_records);
    }

    #[tokio::test]
    async fn test_broken_apply() {
        // Setup: Create a log that will fail on update_collection_log_offset
        let mut log = Log::InMemory(InMemoryLog::new());
        let in_memory_log = match log {
            Log::InMemory(ref mut log) => log,
            _ => panic!("Expected InMemoryLog"),
        };

        let tmpdir = tempfile::tempdir().unwrap();
        tokio::fs::remove_dir_all(tmpdir.path())
            .await
            .expect("Failed to remove temp dir");
        let storage = Storage::Local(LocalStorage::new(tmpdir.path().to_str().unwrap()));

        let tenant = "tenant_log_repair".to_string();
        let collection = Collection {
            name: "collection_log_repair".to_string(),
            dimension: Some(TEST_EMBEDDING_DIMENSION.try_into().unwrap()),
            tenant: tenant.clone(),
            database: "database_log_repair".to_string(),
            log_position: -1,
            ..Default::default()
        };

        let collection_uuid = collection.collection_id;

        // Add some log records
        upsert_generator
            .generate_vec(1..=60)
            .into_iter()
            .for_each(|log| {
                in_memory_log.add_log(
                    collection_uuid,
                    InternalLogRecord {
                        collection_id: collection_uuid,
                        log_offset: log.log_offset - 1,
                        log_ts: log.log_offset,
                        record: log,
                    },
                )
            });

        let mut sysdb = SysDb::Test(TestSysDb::new());
        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_collection(collection);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let record_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        let hnsw_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        let metadata_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_segment(record_segment);
                sysdb.add_segment(hnsw_segment);
                sysdb.add_segment(metadata_segment);
                sysdb.add_tenant_last_compaction_time(tenant, 1);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let hnsw_cache = new_non_persistent_cache_for_test();
        let gc_context = GarbageCollectionContext::try_from_config(
            &(
                PlGarbageCollectionConfig::default(),
                HnswGarbageCollectionConfig::default(),
            ),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");
        let blockfile_provider = BlockfileProvider::new_arrow(
            storage.clone(),
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmpdir.path()),
            hnsw_cache,
            16,
            false,
        );
        let spann_provider = SpannProvider {
            hnsw_provider: hnsw_provider.clone(),
            blockfile_provider: blockfile_provider.clone(),
            garbage_collection_context: gc_context,
            metrics: SpannMetrics::default(),
            pl_block_size: 5 * 1024 * 1024,
            adaptive_search_nprobe: true,
        };

        let config = RootConfig::default();
        let system = System::default();
        let registry = Registry::new();
        let dispatcher = Dispatcher::try_from_config(&config.query_service.dispatcher, &registry)
            .await
            .expect("Should be able to initialize dispatcher");
        let dispatcher_handle = system.start_component(dispatcher);
        let old_cas = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap();

        let old_records = get_all_records(
            &system,
            &dispatcher_handle,
            blockfile_provider.clone(),
            log.clone(),
            old_cas,
        )
        .await;

        let first_compaction_result = Box::pin(compact(
            system.clone(),
            collection_uuid,
            false,
            5000,
            10000,
            1, // Important to make sure each partition is one key.
            log.clone(),
            sysdb.clone(),
            blockfile_provider.clone(),
            hnsw_provider.clone(),
            spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
            Some(2), // The apply operator processing this offset will fail.
        ))
        .await;

        first_compaction_result.expect_err("Should fail");

        let new_cas = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap();
        let new_records = get_all_records(
            &system,
            &dispatcher_handle,
            blockfile_provider.clone(),
            log.clone(),
            new_cas.clone(),
        )
        .await;
        assert_eq!(new_cas.collection.log_position, -1);
        assert_eq!(new_cas.collection.version, 0);
        assert_eq!(old_records, new_records);
        assert_eq!(new_cas.record_segment.file_path.len(), 0);
        assert_eq!(new_cas.vector_segment.file_path.len(), 0);
        assert_eq!(new_cas.metadata_segment.file_path.len(), 0);
    }

    #[tokio::test]
    async fn test_log_repair() {
        // Setup: Create a log that will fail on update_collection_log_offset
        let mut log = Log::InMemory(InMemoryLog::new());
        let in_memory_log = match log {
            Log::InMemory(ref mut log) => log,
            _ => panic!("Expected InMemoryLog"),
        };

        let tmpdir = tempfile::tempdir().unwrap();
        tokio::fs::remove_dir_all(tmpdir.path())
            .await
            .expect("Failed to remove temp dir");
        let storage = Storage::Local(LocalStorage::new(tmpdir.path().to_str().unwrap()));

        let tenant = "tenant_log_repair".to_string();
        let collection = Collection {
            name: "collection_log_repair".to_string(),
            dimension: Some(TEST_EMBEDDING_DIMENSION.try_into().unwrap()),
            tenant: tenant.clone(),
            database: "database_log_repair".to_string(),
            log_position: -1,
            ..Default::default()
        };

        let collection_uuid = collection.collection_id;

        // Add some log records
        add_delete_generator
            .generate_vec(1..=10)
            .into_iter()
            .for_each(|log| {
                in_memory_log.add_log(
                    collection_uuid,
                    InternalLogRecord {
                        collection_id: collection_uuid,
                        log_offset: log.log_offset - 1,
                        log_ts: log.log_offset,
                        record: log,
                    },
                )
            });

        // Configure InMemoryLog to fail on update_collection_log_offset
        in_memory_log.set_fail_update_offset(true);

        let mut sysdb = SysDb::Test(TestSysDb::new());
        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_collection(collection);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let record_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        let hnsw_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        let metadata_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_segment(record_segment);
                sysdb.add_segment(hnsw_segment);
                sysdb.add_segment(metadata_segment);
                sysdb.add_tenant_last_compaction_time(tenant, 1);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let hnsw_cache = new_non_persistent_cache_for_test();
        let gc_context = GarbageCollectionContext::try_from_config(
            &(
                PlGarbageCollectionConfig::default(),
                HnswGarbageCollectionConfig::default(),
            ),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");
        let blockfile_provider = BlockfileProvider::new_arrow(
            storage.clone(),
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmpdir.path()),
            hnsw_cache,
            16,
            false,
        );
        let spann_provider = SpannProvider {
            hnsw_provider: hnsw_provider.clone(),
            blockfile_provider: blockfile_provider.clone(),
            garbage_collection_context: gc_context,
            metrics: SpannMetrics::default(),
            pl_block_size: 5 * 1024 * 1024,
            adaptive_search_nprobe: true,
        };

        let config = RootConfig::default();
        let system = System::default();
        let registry = Registry::new();
        let dispatcher = Dispatcher::try_from_config(&config.query_service.dispatcher, &registry)
            .await
            .expect("Should be able to initialize dispatcher");
        let dispatcher_handle = system.start_component(dispatcher);

        let old_cas = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap();

        let old_records = get_all_records(
            &system,
            &dispatcher_handle,
            blockfile_provider.clone(),
            log.clone(),
            old_cas,
        )
        .await;

        // Run first compaction - this should fail to update the log offset
        let first_compaction_result = Box::pin(compact(
            system.clone(),
            collection_uuid,
            false,
            5000,
            10000,
            1000,
            log.clone(),
            sysdb.clone(),
            blockfile_provider.clone(),
            hnsw_provider.clone(),
            spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
            None,
        ))
        .await;

        // First compaction should fail because update_collection_log_offset fails
        assert!(
            first_compaction_result.is_err(),
            "First compaction should fail due to update_collection_log_offset failure"
        );

        // Now fix the log to allow updates
        match log {
            Log::InMemory(ref mut log) => {
                log.set_fail_update_offset(false);
            }
            _ => panic!("Expected InMemoryLog"),
        }

        // Run second compaction - this should detect the repair is needed
        // because the offset wasn't updated in the first compaction
        let second_compaction_result = Box::pin(compact(
            system.clone(),
            collection_uuid,
            false,
            5000,
            10000,
            1000,
            log.clone(),
            sysdb.clone(),
            blockfile_provider.clone(),
            hnsw_provider.clone(),
            spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
            None,
        ))
        .await;

        // Second compaction should return RequireCompactionOffsetRepair
        match second_compaction_result {
            Ok(CompactionResponse::RequireCompactionOffsetRepair {
                job_id,
                witnessed_offset_in_sysdb,
            }) => {
                println!("Got expected RequireCompactionOffsetRepair response");
                println!("Job ID: {:?}", job_id);
                println!("Witnessed offset: {}", witnessed_offset_in_sysdb);
                assert_eq!(
                    witnessed_offset_in_sysdb, 10,
                    "Expected witnessed offset to be 10"
                );
            }
            Ok(CompactionResponse::Success { .. }) => {
                panic!("Expected RequireCompactionOffsetRepair but got Success");
            }
            Err(e) => {
                panic!(
                    "Expected RequireCompactionOffsetRepair but got error: {:?}",
                    e
                );
            }
        }

        // Manually repair the log position in sysdb (simulating external repair)
        // The segments were actually flushed with data up to offset 60, so update the collection
        let mut collection = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap()
            .collection;
        collection.log_position = 60;
        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_collection(collection);
            }
            _ => panic!("Expected TestSysDb"),
        }

        // Now verify we can get records successfully after repair
        let new_cas = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap();
        let new_records = get_all_records(
            &system,
            &dispatcher_handle,
            blockfile_provider.clone(),
            log.clone(),
            new_cas,
        )
        .await;
        assert_eq!(old_records, new_records);
    }

    #[tokio::test]
    async fn test_compaction_with_empty_logs_from_inserts_and_deletes() {
        let mut log = Log::InMemory(InMemoryLog::new());
        let in_memory_log = match log {
            Log::InMemory(ref mut log) => log,
            _ => panic!("Expected InMemoryLog"),
        };
        let tmpdir = tempfile::tempdir().unwrap();
        // Clear temp dir.
        tokio::fs::remove_dir_all(tmpdir.path())
            .await
            .expect("Failed to remove temp dir");
        let storage = Storage::Local(LocalStorage::new(tmpdir.path().to_str().unwrap()));

        let tenant = "tenant_empty_logs".to_string();
        let collection = Collection {
            name: "collection_empty_logs".to_string(),
            dimension: Some(TEST_EMBEDDING_DIMENSION.try_into().unwrap()),
            tenant: tenant.clone(),
            database: "database_empty_logs".to_string(),
            log_position: -1,
            ..Default::default()
        };

        let collection_uuid = collection.collection_id;

        // Add logs that represent inserts and deletes that net out to 0
        // Use the add_delete_generator to create 250 records (125 pairs of insert+delete)

        add_delete_net_zero_generator
            .generate_vec(1..=251) // This creates 100 log entries that net out to empty
            .into_iter()
            .for_each(|log| {
                in_memory_log.add_log(
                    collection_uuid,
                    InternalLogRecord {
                        collection_id: collection_uuid,
                        log_offset: log.log_offset - 1,
                        log_ts: log.log_offset,
                        record: log,
                    },
                )
            });

        let mut sysdb = SysDb::Test(TestSysDb::new());
        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_collection(collection);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let record_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        let hnsw_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        let metadata_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_segment(record_segment);
                sysdb.add_segment(hnsw_segment);
                sysdb.add_segment(metadata_segment);
                sysdb.add_tenant_last_compaction_time(tenant, 1);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let hnsw_cache = new_non_persistent_cache_for_test();
        let gc_context = GarbageCollectionContext::try_from_config(
            &(
                PlGarbageCollectionConfig::default(),
                HnswGarbageCollectionConfig::default(),
            ),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");
        let blockfile_provider = BlockfileProvider::new_arrow(
            storage.clone(),
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmpdir.path().to_str().unwrap()),
            hnsw_cache,
            16,
            false,
        );
        let spann_provider = SpannProvider {
            hnsw_provider: hnsw_provider.clone(),
            blockfile_provider: blockfile_provider.clone(),
            garbage_collection_context: gc_context,
            metrics: SpannMetrics::default(),
            pl_block_size: 5 * 1024 * 1024,
            adaptive_search_nprobe: true,
        };
        let system = System::new();

        let dispatcher = Dispatcher::new(DispatcherConfig {
            num_worker_threads: 10,
            task_queue_limit: 100,
            dispatcher_queue_size: 100,
            worker_queue_size: 100,
            active_io_tasks: 100,
        });
        let dispatcher_handle = system.start_component(dispatcher);

        let old_cas = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap();

        let old_records = get_all_records(
            &system,
            &dispatcher_handle,
            blockfile_provider.clone(),
            log.clone(),
            old_cas,
        )
        .await;

        let compact_result = Box::pin(compact(
            system.clone(),
            collection_uuid,
            false, // walrus_enabled
            50,    // min_compaction_size
            1000,  // max_compaction_size
            50,    // max_partition_size
            log.clone(),
            sysdb.clone(),
            blockfile_provider.clone(),
            hnsw_provider.clone(),
            spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
            None,
        ))
        .await;

        // Verify compaction completed successfully
        assert!(
            compact_result.is_ok(),
            "Compaction should succeed when logs net out to empty, but got error: {:?}",
            compact_result.err()
        );

        // Verify that the collection has 0 bytes post-compaction since all operations net out to empty
        let new_cas = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap();
        let collection_after_compaction = new_cas.clone().collection;

        println!(
            "Collection size post-compaction: {} bytes",
            collection_after_compaction.size_bytes_post_compaction
        );
        println!(
            "Collection log position: {}",
            collection_after_compaction.log_position
        );

        assert_eq!(
            collection_after_compaction.total_records_post_compaction, 0,
            "Collection should have 0 records post-compaction when all inserts/deletes net out to empty, but got {} records",
            collection_after_compaction.total_records_post_compaction
        );

        assert_eq!(
            collection_after_compaction.size_bytes_post_compaction, 0,
            "Collection should have 0 bytes post-compaction when all inserts/deletes net out to empty, but got {} bytes",
            collection_after_compaction.size_bytes_post_compaction
        );

        assert_eq!(
            collection_after_compaction.log_position, 251,
            "Collection log position is wrong"
        );
        check_purge_successful(tmpdir.path()).await;
        let new_records = get_all_records(
            &system,
            &dispatcher_handle,
            blockfile_provider.clone(),
            log.clone(),
            new_cas.clone(),
        )
        .await;
        assert_eq!(old_records, new_records);
        assert_eq!(new_cas.record_segment.file_path.len(), 0);
        assert_eq!(new_cas.vector_segment.file_path.len(), 0);
        assert_eq!(new_cas.metadata_segment.file_path.len(), 0);
    }

    #[tokio::test]
    async fn test_compaction_with_empty_logs_second_compaction() {
        let mut log = Log::InMemory(InMemoryLog::new());
        let tmpdir = tempfile::tempdir().unwrap();
        // Clear temp dir.
        tokio::fs::remove_dir_all(tmpdir.path())
            .await
            .expect("Failed to remove temp dir");
        let storage = Storage::Local(LocalStorage::new(tmpdir.path().to_str().unwrap()));

        let tenant = "tenant_empty_logs_second".to_string();
        let collection = Collection {
            name: "collection_empty_logs_second".to_string(),
            dimension: Some(TEST_EMBEDDING_DIMENSION.try_into().unwrap()),
            tenant: tenant.clone(),
            database: "database_empty_logs_second".to_string(),
            log_position: -1,
            ..Default::default()
        };

        let collection_uuid = collection.collection_id;

        // First, add some real data for the first compaction (50 records)
        {
            let in_memory_log = match log {
                Log::InMemory(ref mut log) => log,
                _ => panic!("Expected InMemoryLog"),
            };
            upsert_generator
                .generate_vec(1..=49)
                .into_iter()
                .for_each(|log| {
                    in_memory_log.add_log(
                        collection_uuid,
                        InternalLogRecord {
                            collection_id: collection_uuid,
                            log_offset: log.log_offset - 1,
                            log_ts: log.log_offset,
                            record: log,
                        },
                    )
                });
        }

        let mut sysdb = SysDb::Test(TestSysDb::new());
        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_collection(collection);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let record_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        let hnsw_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        let metadata_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_segment(record_segment);
                sysdb.add_segment(hnsw_segment);
                sysdb.add_segment(metadata_segment);
                sysdb.add_tenant_last_compaction_time(tenant.clone(), 1);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let hnsw_cache = new_non_persistent_cache_for_test();
        let gc_context = GarbageCollectionContext::try_from_config(
            &(
                PlGarbageCollectionConfig::default(),
                HnswGarbageCollectionConfig::default(),
            ),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");
        let blockfile_provider = BlockfileProvider::new_arrow(
            storage.clone(),
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmpdir.path().to_str().unwrap()),
            hnsw_cache,
            16,
            false,
        );
        let spann_provider = SpannProvider {
            hnsw_provider: hnsw_provider.clone(),
            blockfile_provider: blockfile_provider.clone(),
            garbage_collection_context: gc_context,
            metrics: SpannMetrics::default(),
            pl_block_size: 5 * 1024 * 1024,
            adaptive_search_nprobe: true,
        };
        let system = System::new();

        let dispatcher = Dispatcher::new(DispatcherConfig {
            num_worker_threads: 10,
            task_queue_limit: 100,
            dispatcher_queue_size: 100,
            worker_queue_size: 100,
            active_io_tasks: 100,
        });
        let dispatcher_handle = system.start_component(dispatcher);

        // Run first compaction with real data
        let first_compact_result = Box::pin(compact(
            system.clone(),
            collection_uuid,
            false, // walrus_enabled
            50,    // min_compaction_size
            1000,  // max_compaction_size
            50,    // max_partition_size
            log.clone(),
            sysdb.clone(),
            blockfile_provider.clone(),
            hnsw_provider.clone(),
            spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
            None,
        ))
        .await;

        assert!(
            first_compact_result.is_ok(),
            "First compaction should succeed, but got error: {:?}",
            first_compact_result.err()
        );

        // Verify first compaction created data
        let collection_after_first = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .expect("Collection should exist after first compaction");

        assert_eq!(
            collection_after_first
                .collection
                .total_records_post_compaction,
            49,
            "Collection should have 49 records after first compaction, but got {}",
            collection_after_first
                .collection
                .total_records_post_compaction
        );

        assert!(
            collection_after_first.collection.size_bytes_post_compaction > 0,
            "Collection should have non-zero size after first compaction, but got {} bytes",
            collection_after_first.collection.size_bytes_post_compaction
        );

        // Now add logs that net out to 0 for the second compaction
        {
            let in_memory_log = match log {
                Log::InMemory(ref mut log) => log,
                _ => panic!("Expected InMemoryLog"),
            };
            add_delete_net_zero_generator
                .generate_vec(100..=250) // Starting from 51 since we already have 50 logs
                .into_iter()
                .for_each(|log| {
                    in_memory_log.add_log(
                        collection_uuid,
                        InternalLogRecord {
                            collection_id: collection_uuid,
                            log_offset: log.log_offset - 1 - 50,
                            log_ts: log.log_offset - 50,
                            record: log,
                        },
                    )
                });
        }

        let old_cas = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap();

        let old_records = get_all_records(
            &system,
            &dispatcher_handle,
            blockfile_provider.clone(),
            log.clone(),
            old_cas,
        )
        .await;

        // Run second compaction with empty logs
        let second_compact_result = Box::pin(compact(
            system.clone(),
            collection_uuid,
            false, // walrus_enabled
            50,    // min_compaction_size
            1000,  // max_compaction_size
            50,    // max_partition_size
            log.clone(),
            sysdb.clone(),
            blockfile_provider.clone(),
            hnsw_provider.clone(),
            spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
            None,
        ))
        .await;

        // Verify second compaction completed successfully
        assert!(
            second_compact_result.is_ok(),
            "Second compaction should succeed when logs net out to empty, but got error: {:?}",
            second_compact_result.err()
        );

        // Verify that the collection still has the same data from the first compaction
        let collection_after_second = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .expect("Collection should exist after second compaction");

        println!(
            "Collection size after second compaction: {} bytes",
            collection_after_second
                .collection
                .size_bytes_post_compaction
        );
        println!(
            "Collection log position: {}",
            collection_after_second.collection.log_position
        );

        assert_eq!(
            collection_after_second.collection.total_records_post_compaction, 50,
            "Collection should still have 50 records after second compaction with empty logs, but got {} records",
            collection_after_second.collection.total_records_post_compaction
        );

        assert!(
            collection_after_second.collection.size_bytes_post_compaction > 0,
            "Collection should still have non-zero size after second compaction with empty logs, but got {} bytes",
            collection_after_second.collection.size_bytes_post_compaction
        );

        assert_eq!(
            collection_after_second.collection.log_position, 250,
            "Collection log position should be 250 after processing all logs"
        );

        check_purge_successful(tmpdir.path()).await;
        let new_cas = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap();
        let new_records = get_all_records(
            &system,
            &dispatcher_handle,
            blockfile_provider.clone(),
            log.clone(),
            new_cas,
        )
        .await;
        assert_eq!(old_records, new_records);
    }

    #[test]
    fn test_concurrent_compactions() {
        // Deep async call chains create large state machines that exceed default 2MB stack
        // Use larger stack to accommodate the nested futures
        std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024) // 8 MB stack
            .spawn(|| {
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(test_concurrent_compactions_impl())
            })
            .unwrap()
            .join()
            .unwrap();
    }

    async fn test_concurrent_compactions_impl() {
        // This test simulates the scenario where:
        // 1. Compaction 1 starts its log_fetch_orchestrator
        // 2. Compaction 2 starts and finishes everything
        // 3. Compaction 1 continues with the rest of its orchestrators and fails cleanly

        let mut log = Log::InMemory(InMemoryLog::new());
        let in_memory_log = match log {
            Log::InMemory(ref mut log) => log,
            _ => panic!("Expected InMemoryLog"),
        };
        let tmpdir = tempfile::tempdir().unwrap();
        // Clear temp dir.
        tokio::fs::remove_dir_all(tmpdir.path())
            .await
            .expect("Failed to remove temp dir");
        let storage = Storage::Local(LocalStorage::new(tmpdir.path().to_str().unwrap()));

        let tenant = "tenant_concurrent_log_fetch".to_string();

        // Create a collection for testing
        let collection = Collection {
            name: "collection_concurrent_log_fetch".to_string(),
            dimension: Some(TEST_EMBEDDING_DIMENSION.try_into().unwrap()),
            tenant: tenant.clone(),
            database: "database_concurrent_log_fetch".to_string(),
            log_position: -1,
            ..Default::default()
        };

        let collection_uuid = collection.collection_id;

        // Add logs for the collection
        add_delete_net_zero_generator
            .generate_vec(1..=101) // This creates 100 log entries that net out to empty
            .into_iter()
            .for_each(|log| {
                in_memory_log.add_log(
                    collection_uuid,
                    InternalLogRecord {
                        collection_id: collection_uuid,
                        log_offset: log.log_offset - 1,
                        log_ts: log.log_offset,
                        record: log,
                    },
                )
            });

        let mut sysdb = SysDb::Test(TestSysDb::new());
        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_collection(collection.clone());
            }
            _ => panic!("Invalid sysdb type"),
        }

        // Create segments for the collection
        let record_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        let hnsw_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        let metadata_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: collection_uuid,
            metadata: None,
            file_path: HashMap::new(),
        };

        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_segment(record_segment);
                sysdb.add_segment(hnsw_segment);
                sysdb.add_segment(metadata_segment);
                sysdb.add_tenant_last_compaction_time(tenant.clone(), 1);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let hnsw_cache = new_non_persistent_cache_for_test();
        let gc_context = GarbageCollectionContext::try_from_config(
            &(
                PlGarbageCollectionConfig::default(),
                HnswGarbageCollectionConfig::default(),
            ),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");
        let blockfile_provider = BlockfileProvider::new_arrow(
            storage.clone(),
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmpdir.path().to_str().unwrap()),
            hnsw_cache,
            16,
            false,
        );
        let spann_provider = SpannProvider {
            hnsw_provider: hnsw_provider.clone(),
            blockfile_provider: blockfile_provider.clone(),
            garbage_collection_context: gc_context,
            metrics: SpannMetrics::default(),
            pl_block_size: 5 * 1024 * 1024,
            adaptive_search_nprobe: true,
        };
        let system = System::new();

        let dispatcher = Dispatcher::new(DispatcherConfig {
            num_worker_threads: 10,
            task_queue_limit: 100,
            dispatcher_queue_size: 100,
            worker_queue_size: 100,
            active_io_tasks: 100,
        });
        let dispatcher_handle = system.start_component(dispatcher);

        let old_cas = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap();

        let old_records = get_all_records(
            &system,
            &dispatcher_handle,
            blockfile_provider.clone(),
            log.clone(),
            old_cas,
        )
        .await;

        // Test the actual compaction workflow by simulating the timing
        // Manually create compaction contexts to control each phase

        // Compaction 1: Start with run_get_logs only
        let mut compaction_context_1 = CompactionContext::new(
            false,
            50,
            1000,
            50,
            log.clone(),
            sysdb.clone(),
            blockfile_provider.clone(),
            hnsw_provider.clone(),
            spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
        );

        // Start compaction 1's log_fetch_orchestrator
        println!("Starting compaction 1's run_get_logs...");
        let compaction_1_logs_result = compaction_context_1
            .run_get_logs(collection_uuid, system.clone(), false)
            .await;

        // Store the logs for compaction 1 to use later
        let (compaction_1_log_records, _compaction_1_collection_info) =
            match compaction_1_logs_result {
                Ok(LogFetchOrchestratorResponse::Success(success)) => {
                    (success.materialized, success.collection_info)
                }
                Ok(LogFetchOrchestratorResponse::RequireCompactionOffsetRepair(_)) => {
                    panic!("Unexpected repair response in test");
                }
                Ok(LogFetchOrchestratorResponse::RequireFunctionBackfill(_)) => {
                    panic!("Unexpected function backfill response in test");
                }
                Err(e) => {
                    panic!("Compaction 1 run_get_logs failed: {:?}", e);
                }
            };

        println!(
            "Compaction 1's run_get_logs completed successfully, got {} log records",
            compaction_1_log_records.len()
        );

        // Create a NEW compaction context for compaction 2 to simulate a fresh compaction
        // This ensures both compactions work with the same initial state
        let _ = CompactionContext::new(
            false,
            50,
            1000,
            50,
            log.clone(),
            sysdb.clone(),
            blockfile_provider.clone(),
            hnsw_provider.clone(),
            spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
        );

        // Now start compaction 2 and let it run completely using the compact() function
        println!("Starting compaction 2 to completion...");
        let compaction_2 = Box::pin(compact(
            system.clone(),
            collection_uuid,
            false, // walrus_enabled
            50,    // min_compaction_size
            1000,  // max_compaction_size
            50,    // max_partition_size
            log.clone(),
            sysdb.clone(),
            blockfile_provider.clone(),
            hnsw_provider.clone(),
            spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
            None,
        ));

        let _compaction_2_result = compaction_2
            .await
            .expect("Compaction 2 should have succeeded.");

        assert_eq!(
            sysdb
                .get_collection_with_segments(collection_uuid)
                .await
                .unwrap()
                .collection
                .version,
            1
        );

        // Now try to continue compaction 1 with the rest of the phases
        // Compaction 1 should fail because compaction 2 already processed the same logs
        println!(
            "Continuing compaction 1 with run_apply_logs using {} log records...",
            compaction_1_log_records.len()
        );
        let compaction_1_apply_response = compaction_context_1
            .run_apply_logs(compaction_1_log_records, system.clone())
            .await
            .expect("Apply should have succeeded.");

        let register_info = vec![CollectionRegisterInfo {
            collection_info: compaction_context_1.get_collection_info().unwrap().clone(),
            flush_results: compaction_1_apply_response.flush_results,
            collection_logical_size_bytes: compaction_1_apply_response
                .collection_logical_size_bytes,
        }];

        let _register_result =
            Box::pin(compaction_context_1.run_register(register_info, None, system.clone()))
                .await
                .expect_err("Register should have failed.");

        // Verify that the collection was successfully compacted (by whichever succeeded)
        let collection_after_compaction = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .expect("Collection should exist after compaction");

        // The collection should be in a consistent state
        assert_eq!(
            collection_after_compaction.collection.version, 1,
            "Collection should have version 1"
        );

        let new_cas = sysdb
            .get_collection_with_segments(collection_uuid)
            .await
            .unwrap();
        let new_records = get_all_records(
            &system,
            &dispatcher_handle,
            blockfile_provider.clone(),
            log.clone(),
            new_cas,
        )
        .await;
        assert_eq!(old_records, new_records);
    }

    pub async fn check_purge_successful(path: impl AsRef<Path>) {
        let mut entries = fs::read_dir(&path).await.expect("Failed to read dir");

        while let Some(entry) = entries.next_entry().await.expect("Failed to read next dir") {
            let path = entry.path();
            let metadata = entry.metadata().await.expect("Failed to read metadata");

            if metadata.is_dir() {
                assert!(path.ends_with("tenant"));
            } else {
                panic!("Expected hnsw purge to be successful")
            }
        }
    }

    /// Test that rebuilding a collection also rebuilds its attached function's output collection.
    /// This test requires a running Tilt environment with GRPC sysdb.
    ///
    /// Steps:
    /// 1. Create collection with 10 records (6 red, 4 blue) and attach statistics function
    /// 2. First compaction (is_function_disabled=false) - function runs, completion_offset=9
    /// 3. Add 5 more records (green)
    /// 4. Second compaction (is_function_disabled=true) - function skipped, completion_offset stays 9
    /// 5. Rebuild (is_rebuild=true) - function catches up on all 15 records
    /// 6. Verify statistics: red=6, blue=4, green=5, total=15
    #[tokio::test]
    async fn test_k8s_integration_rebuild_with_attached_function() {
        use chroma_log::in_memory_log::{InMemoryLog, InternalLogRecord};
        use chroma_segment::blockfile_record::RecordSegmentReader;
        use chroma_types::{CollectionUuid, Operation, OperationRecord, UpdateMetadataValue};

        // Setup test environment
        let config = RootConfig::default();
        let system = System::default();
        let registry = Registry::new();
        let dispatcher = Dispatcher::try_from_config(&config.query_service.dispatcher, &registry)
            .await
            .expect("Should be able to initialize dispatcher");
        let dispatcher_handle = system.start_component(dispatcher);

        // Connect to Grpc SysDb (requires Tilt running)
        let grpc_sysdb = chroma_sysdb::GrpcSysDb::try_from_config(
            &chroma_sysdb::GrpcSysDbConfig {
                host: "localhost".to_string(),
                port: 50051,
                connect_timeout_ms: 5000,
                request_timeout_ms: 10000,
                num_channels: 4,
            },
            &registry,
        )
        .await
        .expect("Should connect to grpc sysdb");
        let mut sysdb = SysDb::Grpc(grpc_sysdb);

        let test_segments = TestDistributedSegment::new().await;
        let mut in_memory_log = InMemoryLog::new();

        // Create input collection
        let collection_name = format!("test_rebuild_fn_{}", uuid::Uuid::new_v4());
        let collection_id = CollectionUuid::new();

        sysdb
            .create_collection(
                test_segments.collection.tenant.clone(),
                test_segments.collection.database.clone(),
                collection_id,
                collection_name,
                vec![
                    test_segments.record_segment.clone(),
                    test_segments.metadata_segment.clone(),
                    test_segments.vector_segment.clone(),
                ],
                None,
                None,
                None,
                test_segments.collection.dimension,
                false,
            )
            .await
            .expect("Collection create should be successful");

        let tenant = "default_tenant".to_string();
        let db = "default_database".to_string();

        // Set initial log position
        sysdb
            .flush_compaction(
                tenant.clone(),
                collection_id,
                -1,
                0,
                std::sync::Arc::new([]),
                0,
                0,
                None,
            )
            .await
            .expect("Should be able to update log_position");

        // Add 10 records with metadata
        for i in 0..10 {
            let mut metadata = HashMap::new();
            let color = if i < 6 { "red" } else { "blue" };
            metadata.insert(
                "color".to_string(),
                UpdateMetadataValue::Str(color.to_string()),
            );

            let log_record = chroma_types::LogRecord {
                log_offset: i as i64,
                record: OperationRecord {
                    id: format!("record_{}", i),
                    embedding: Some(vec![
                        0.0;
                        test_segments.collection.dimension.unwrap_or(384)
                            as usize
                    ]),
                    encoding: None,
                    metadata: Some(metadata),
                    document: Some(format!("doc {}", i)),
                    operation: Operation::Upsert,
                },
            };

            in_memory_log.add_log(
                collection_id,
                InternalLogRecord {
                    collection_id,
                    log_offset: i as i64,
                    log_ts: i as i64,
                    record: log_record,
                },
            );
        }

        let log = Log::InMemory(in_memory_log);
        let test_run_id = uuid::Uuid::new_v4();
        let attached_function_name = format!("test_rebuild_fn_{}", test_run_id);
        let output_collection_name = format!("test_rebuild_output_{}", test_run_id);

        // Create statistics attached function via sysdb
        let (attached_function_id, _created) = sysdb
            .create_attached_function(
                attached_function_name.clone(),
                "statistics".to_string(),
                collection_id,
                output_collection_name.clone(),
                serde_json::Value::Null,
                tenant.clone(),
                db.clone(),
                10, // dimension
            )
            .await
            .expect("Attached function creation should succeed");
        sysdb
            .finish_create_attached_function(attached_function_id)
            .await
            .expect("Attached function creation finish should succeed");

        // First compaction - populates both input and output collections
        println!("Starting first compaction...");
        Box::pin(compact(
            system.clone(),
            collection_id,
            false, // not a rebuild
            50,
            1000,
            50,
            log.clone(),
            sysdb.clone(),
            test_segments.blockfile_provider.clone(),
            test_segments.hnsw_provider.clone(),
            test_segments.spann_provider.clone(),
            dispatcher_handle.clone(),
            false,
            None,
        ))
        .await
        .expect("First compaction should succeed");
        println!("First compaction completed");

        // Verify the attached function was executed
        let attached_function_after_compact = sysdb
            .get_attached_functions(chroma_sysdb::GetAttachedFunctionsOptions {
                name: Some(attached_function_name.clone()),
                input_collection_id: Some(collection_id),
                only_ready: true,
                ..Default::default()
            })
            .await
            .expect("Attached function query should succeed")
            .into_iter()
            .next()
            .expect("Attached function should be found");
        assert_eq!(
            attached_function_after_compact.completion_offset, 9,
            "Completion offset should be 9 after processing 10 records (0-9)"
        );

        let output_collection_id = attached_function_after_compact
            .output_collection_id
            .expect("Output collection should exist");

        // Add 5 more records (10-14) with color="green"
        let mut log = log; // Make log mutable
        if let Log::InMemory(ref mut in_memory_log) = log {
            for i in 10..15 {
                let mut metadata = HashMap::new();
                metadata.insert(
                    "color".to_string(),
                    UpdateMetadataValue::Str("green".to_string()),
                );

                let log_record = chroma_types::LogRecord {
                    log_offset: i as i64,
                    record: OperationRecord {
                        id: format!("record_{}", i),
                        embedding: Some(vec![
                            0.0;
                            test_segments.collection.dimension.unwrap_or(384)
                                as usize
                        ]),
                        encoding: None,
                        metadata: Some(metadata),
                        document: Some(format!("doc {}", i)),
                        operation: Operation::Upsert,
                    },
                };

                in_memory_log.add_log(
                    collection_id,
                    InternalLogRecord {
                        collection_id,
                        log_offset: i as i64,
                        log_ts: i as i64,
                        record: log_record,
                    },
                );
            }
        }
        println!("Added 5 more records (10-14) with color=green");

        // Second compaction with is_function_disabled=true - function should NOT run
        println!("Starting second compaction with is_function_disabled=true...");
        Box::pin(compact(
            system.clone(),
            collection_id,
            false, // not a rebuild
            50,
            1000,
            50,
            log.clone(),
            sysdb.clone(),
            test_segments.blockfile_provider.clone(),
            test_segments.hnsw_provider.clone(),
            test_segments.spann_provider.clone(),
            dispatcher_handle.clone(),
            true, // is_function_disabled = true
            None,
        ))
        .await
        .expect("Second compaction should succeed");
        println!("Second compaction completed");

        // Verify the attached function was NOT executed (completion_offset should still be 9)
        let attached_function_after_disabled = sysdb
            .get_attached_functions(chroma_sysdb::GetAttachedFunctionsOptions {
                name: Some(attached_function_name.clone()),
                input_collection_id: Some(collection_id),
                only_ready: true,
                ..Default::default()
            })
            .await
            .expect("Attached function query should succeed")
            .into_iter()
            .next()
            .expect("Attached function should be found");
        assert_eq!(
            attached_function_after_disabled.completion_offset, 9,
            "Completion offset should still be 9 - function was disabled"
        );
        println!(
            "After disabled compaction: completion_offset={} (should be 9)",
            attached_function_after_disabled.completion_offset
        );

        // Get output collection info before rebuild
        let output_before_rebuild = sysdb
            .get_collection_with_segments(output_collection_id)
            .await
            .expect("Should get output collection before rebuild");
        let output_version_before = output_before_rebuild.collection.version;
        let output_file_path_before = output_before_rebuild.record_segment.file_path.clone();

        println!(
            "Before rebuild: output_version={}, output_file_path={:?}",
            output_version_before, output_file_path_before
        );

        // Now REBUILD the input collection - function should catch up on ALL records
        println!("Starting rebuild of input collection (function should catch up)...");
        Box::pin(compact(
            system.clone(),
            collection_id,
            true, // is_rebuild = true
            5000,
            10000,
            1000,
            log.clone(),
            sysdb.clone(),
            test_segments.blockfile_provider.clone(),
            test_segments.hnsw_provider.clone(),
            test_segments.spann_provider.clone(),
            dispatcher_handle.clone(),
            false, // is_function_disabled = false for rebuild
            None,
        ))
        .await
        .expect("Rebuild should succeed");
        println!("Rebuild completed");

        // Verify the input collection was rebuilt (version incremented)
        let input_after_rebuild = sysdb
            .get_collection_with_segments(collection_id)
            .await
            .expect("Should get input collection after rebuild");
        println!(
            "After rebuild: input_version={}",
            input_after_rebuild.collection.version
        );
        assert!(
            input_after_rebuild.collection.version > 1,
            "Input collection version should be incremented after rebuild"
        );

        // Verify the output collection was also rebuilt
        let output_after_rebuild = sysdb
            .get_collection_with_segments(output_collection_id)
            .await
            .expect("Should get output collection after rebuild");

        println!(
            "After rebuild: output_version={}, output_file_path={:?}",
            output_after_rebuild.collection.version, output_after_rebuild.record_segment.file_path
        );

        // Check if output collection was actually rebuilt
        if output_after_rebuild.collection.version == output_version_before {
            println!("WARNING: Output collection version did NOT change - attached function may not have run during rebuild!");
        }

        assert!(
            output_after_rebuild.collection.version > output_version_before,
            "Output collection version should be incremented after rebuild. Before: {}, After: {}",
            output_version_before,
            output_after_rebuild.collection.version
        );

        // Verify the output collection has new file paths (was actually rebuilt)
        assert_ne!(
            output_after_rebuild.record_segment.file_path, output_file_path_before,
            "Output collection record segment file path should change after rebuild"
        );

        // Verify the output collection has the correct statistics data (not doubled!)
        // If record_reader is incorrectly passed during rebuild, the statistics would
        // accumulate instead of being recomputed from scratch.
        let reader = Box::pin(RecordSegmentReader::from_segment(
            &output_after_rebuild.record_segment,
            &test_segments.blockfile_provider,
        ))
        .await
        .expect("Should create reader for output collection");

        let max_offset_id = reader.get_max_offset_id();
        assert!(
            max_offset_id > 0,
            "Output collection should have records after rebuild"
        );

        // Read the statistics and verify counts are correct (not doubled)
        use futures::stream::StreamExt;
        let mut stream = reader.get_data_stream(0..=max_offset_id).await;
        let mut stats_by_key_value: HashMap<(String, String), i64> = HashMap::new();

        while let Some(result) = stream.next().await {
            let (_, record) = result.expect("Should read record");
            let metadata = record
                .metadata
                .expect("Statistics records should have metadata");

            let key = match metadata.get("key") {
                Some(chroma_types::MetadataValue::Str(k)) => k.clone(),
                _ => continue,
            };
            let value = match metadata.get("value") {
                Some(chroma_types::MetadataValue::Str(v)) => v.clone(),
                _ => continue,
            };
            let count = match metadata.get("count") {
                Some(chroma_types::MetadataValue::Int(c)) => *c,
                _ => continue,
            };

            stats_by_key_value.insert((key, value), count);
        }

        // After rebuild, statistics should include ALL 15 records:
        // - 6 records with color="red" (from first compaction)
        // - 4 records with color="blue" (from first compaction)
        // - 5 records with color="green" (added after first compaction, skipped by disabled second compaction)
        // The rebuild should catch up on the green records that were missed.
        let red_count = stats_by_key_value.get(&("color".to_string(), "red".to_string()));
        let blue_count = stats_by_key_value.get(&("color".to_string(), "blue".to_string()));
        let green_count = stats_by_key_value.get(&("color".to_string(), "green".to_string()));
        println!(
            "Statistics after rebuild: red={:?}, blue={:?}, green={:?}, all_stats={:?}",
            red_count, blue_count, green_count, stats_by_key_value
        );

        assert_eq!(red_count, Some(&6), "Should have 6 records with color=red");
        assert_eq!(
            blue_count,
            Some(&4),
            "Should have 4 records with color=blue"
        );
        assert_eq!(
            green_count,
            Some(&5),
            "Should have 5 records with color=green (caught up during rebuild)"
        );

        // Verify total_count includes all 15 records
        let total_count =
            stats_by_key_value.get(&("summary".to_string(), "total_count".to_string()));
        assert_eq!(
            total_count,
            Some(&15),
            "Total count should be 15 (all records processed during rebuild)"
        );

        tracing::info!(
            "Rebuild with attached function test completed successfully. \
            Input collection version: {}, Output collection version: {}, \
            Statistics: red={:?}, blue={:?}, green={:?}, total={:?}",
            input_after_rebuild.collection.version,
            output_after_rebuild.collection.version,
            red_count,
            blue_count,
            green_count,
            total_count
        );
    }
}
