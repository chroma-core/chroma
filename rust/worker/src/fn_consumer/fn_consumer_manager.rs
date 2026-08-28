use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_log::Log;
use chroma_segment::spann_provider::SpannProvider;
use chroma_sysdb::{GetCollectionsOptions, SysDb};
use chroma_system::{
    Component, ComponentContext, ComponentHandle, Dispatcher, Handler, Operator,
    ReceiverForMessage, System,
};
use chroma_types::{
    AttachedFunction, AttachedFunctionUuid, CollectionUuid, DatabaseName, FunctionWorkload,
};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use opentelemetry::metrics::{Counter, Gauge, Histogram};
use opentelemetry::KeyValue;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tokio::sync::{mpsc, OnceCell};
use tracing::{instrument, span};

use crate::compactor::config::CompactorConfig;
use crate::execution::orchestration::compact::CompactionContext;
use crate::execution::orchestration::function_execution::{
    FunctionExecutionContext, FunctionExecutionInput, FunctionExecutionPlan,
};
use crate::execution::{
    operators::get_async_fn_fetch_boundaries::{
        GetAsyncFnFetchBoundariesInput, GetAsyncFnFetchBoundariesOperator,
    },
    orchestration::async_function_boundary::BoundarySelection,
};
use crate::fn_consumer::config::FnConsumerConfig;
use crate::fn_consumer::memory::{AdmissionDecision, MemoryAdmission};
use crate::work_queue::work_queue_client::WorkQueueClient;

fn has_reached_frontier(completion_offset: i64, queue_compaction_offset: i64) -> bool {
    completion_offset >= queue_compaction_offset
}

fn unplanned_input(
    collection_id: CollectionUuid,
    queue_compaction_offset: i64,
) -> FunctionExecutionInput {
    FunctionExecutionInput {
        collection_id,
        queue_compaction_offset,
        plan: None,
    }
}

fn record_bypasses(
    bypass_counts: &mut HashMap<AttachedFunctionUuid, usize>,
    skipped: impl IntoIterator<Item = AttachedFunctionUuid>,
    max_bypass_count: usize,
) -> bool {
    let mut reached_barrier = false;
    for fn_id in skipped {
        let count = bypass_counts.entry(fn_id).or_default();
        *count = count.saturating_add(1);
        reached_barrier |= *count >= max_bypass_count;
    }
    reached_barrier
}

#[derive(Debug)]
pub struct InProgressFn {
    expires_at: SystemTime,
    expiry_logged: bool,
    reserved_bytes: u64,
}

impl InProgressFn {
    pub fn new(job_expiry_seconds: u64, reserved_bytes: u64) -> Self {
        Self {
            expires_at: SystemTime::now() + Duration::from_secs(job_expiry_seconds),
            expiry_logged: false,
            reserved_bytes,
        }
    }

    pub fn is_expired(&self) -> bool {
        SystemTime::now() >= self.expires_at
    }
}

#[derive(Error, Debug)]
pub enum DispatchError {
    #[error("Dispatcher not initialized")]
    DispatcherNotInitialized,

    #[error("Compaction workflow failed: {0}")]
    CompactionFailed(#[from] crate::execution::orchestration::compact::CompactionError),

    #[error("Function consumer dispatch task panicked")]
    DispatchPanicked,
}

impl ChromaError for DispatchError {
    fn code(&self) -> ErrorCodes {
        match self {
            DispatchError::DispatcherNotInitialized => ErrorCodes::Internal,
            DispatchError::CompactionFailed(_) => ErrorCodes::Internal,
            DispatchError::DispatchPanicked => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug)]
enum FnDispatchOutcome {
    Completed,
    Replan,
    ReplanBackfill(CollectionUuid),
}

type FnDispatchOutput = Result<FnDispatchOutcome, DispatchError>;
type FnDispatchFuture = Pin<Box<dyn Future<Output = FnDispatchOutput> + Send>>;

struct FnDispatchTask {
    fn_id: AttachedFunctionUuid,
    future: FnDispatchFuture,
    // Retained separately because the dispatch future may panic before it can
    // report failures to the work queue itself.
    work_queue_client: Option<WorkQueueClient>,
    batch: Vec<FunctionExecutionInput>,
    metrics: FnConsumerMetrics,
}

struct FnDispatchCompletion {
    fn_id: AttachedFunctionUuid,
    batch: Vec<FunctionExecutionInput>,
    result: FnDispatchOutput,
}

#[derive(Clone, Debug)]
struct OrderedWorkCandidate {
    fn_id: AttachedFunctionUuid,
    inputs: Vec<(CollectionUuid, i64)>,
}

#[derive(Debug)]
struct PlannedCandidate {
    fn_id: AttachedFunctionUuid,
    batch: Vec<FunctionExecutionInput>,
    estimate_bytes: Option<u64>,
    estimate_kind: &'static str,
}

enum PlanningError {
    Transient(String),
    Unaddressable(String),
}

#[derive(Clone, Debug)]
struct FnConsumerMetrics {
    reserved_bytes: Gauge<u64>,
    cgroup_current_bytes: Gauge<u64>,
    cgroup_peak_bytes: Gauge<u64>,
    admitted_estimate_bytes: Histogram<u64>,
    bypass_count: Counter<u64>,
    unaddressable_count: Counter<u64>,
    fallback_count: Counter<u64>,
    dlq_increment_count: Counter<u64>,
}

impl Default for FnConsumerMetrics {
    fn default() -> Self {
        let meter = opentelemetry::global::meter("chroma_fn_consumer");
        Self {
            reserved_bytes: meter
                .u64_gauge("fn_consumer_reserved_memory_bytes")
                .with_description("Total memory reserved for admitted function invocations")
                .build(),
            cgroup_current_bytes: meter
                .u64_gauge("fn_consumer_cgroup_current_memory_bytes")
                .with_description("Current pod cgroup memory usage at admission time")
                .build(),
            cgroup_peak_bytes: meter
                .u64_gauge("fn_consumer_cgroup_peak_memory_bytes")
                .with_description("Peak pod cgroup memory usage")
                .build(),
            admitted_estimate_bytes: meter
                .u64_histogram("fn_consumer_admitted_memory_estimate_bytes")
                .with_description("Predicted peak bytes for admitted function invocations")
                .build(),
            bypass_count: meter
                .u64_counter("fn_consumer_memory_bypass_count")
                .with_description("Older invocations bypassed by an admitted younger invocation")
                .build(),
            unaddressable_count: meter
                .u64_counter("fn_consumer_memory_unaddressable_count")
                .with_description("Invocations classified as unable to fit in this pod")
                .build(),
            fallback_count: meter
                .u64_counter("fn_consumer_memory_fallback_count")
                .with_description("Descriptor-less invocations admitted using exclusive fallback")
                .build(),
            dlq_increment_count: meter
                .u64_counter("fn_consumer_dlq_increment_count")
                .with_description("Work queue failure increments requested by fn-consumer")
                .build(),
        }
    }
}

#[derive(Clone, Debug)]
struct DispatchCompletedMessage;

#[derive(Clone)]
pub struct FnConsumerContext {
    pub system: System,
    pub dispatcher: Option<ComponentHandle<Dispatcher>>,
    pub poll_interval: Duration,
    pub max_concurrent_workers: usize,
    pub get_work_batch_size: u32,
    pub job_expiry_seconds: u64,
    pub max_failure_count: i32,
    pub my_member_id: String,
    pub log: Log,
    pub sysdb: SysDb,
    pub blockfile_provider: BlockfileProvider,
    pub hnsw_provider: HnswIndexProvider,
    pub spann_provider: SpannProvider,
    pub fetch_log_batch_size: u32,
    pub fetch_log_concurrency: usize,
    pub max_compaction_size: usize,
    pub max_partition_size: usize,
    metrics: FnConsumerMetrics,
}

impl std::fmt::Debug for FnConsumerContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnConsumerContext")
            .field("poll_interval", &self.poll_interval)
            .field("max_concurrent_workers", &self.max_concurrent_workers)
            .field("get_work_batch_size", &self.get_work_batch_size)
            .field("job_expiry_seconds", &self.job_expiry_seconds)
            .field("my_member_id", &self.my_member_id)
            .finish()
    }
}

pub struct FnConsumerManager {
    context: FnConsumerContext,
    in_progress: HashMap<AttachedFunctionUuid, InProgressFn>,
    work_queue_client: WorkQueueClient,
    dispatch_awaiter_channel: mpsc::Sender<FnDispatchTask>,
    dispatch_awaiter_completion_channel: mpsc::UnboundedReceiver<FnDispatchCompletion>,
    dispatch_awaiter: tokio::task::JoinHandle<()>,
    completion_notifier:
        std::sync::Arc<OnceCell<Box<dyn ReceiverForMessage<DispatchCompletedMessage>>>>,
    memory_admission: Option<MemoryAdmission>,
    reserved_bytes: u64,
    bypass_counts: HashMap<AttachedFunctionUuid, usize>,
    backfill_required: HashSet<(AttachedFunctionUuid, CollectionUuid)>,
    metrics: FnConsumerMetrics,
}

impl std::fmt::Debug for FnConsumerManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnConsumerManager")
            .field("context", &self.context)
            .field("in_progress_count", &self.in_progress.len())
            .field("reserved_bytes", &self.reserved_bytes)
            .finish()
    }
}

impl FnConsumerManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: FnConsumerConfig,
        compactor_config: CompactorConfig,
        my_member_id: String,
        system: System,
        work_queue_client: WorkQueueClient,
        log: Log,
        sysdb: SysDb,
        blockfile_provider: BlockfileProvider,
        hnsw_provider: HnswIndexProvider,
        spann_provider: SpannProvider,
    ) -> Result<Self, String> {
        let memory_admission =
            MemoryAdmission::from_config(&config.memory_admission, config.max_concurrent_workers)?;
        let metrics = FnConsumerMetrics::default();
        let context = FnConsumerContext {
            system,
            dispatcher: None,
            poll_interval: Duration::from_secs(config.poll_interval_sec),
            max_concurrent_workers: config.max_concurrent_workers,
            get_work_batch_size: config.get_work_batch_size,
            job_expiry_seconds: config.job_expiry_seconds,
            max_failure_count: config.max_failure_count,
            my_member_id,
            log,
            sysdb,
            blockfile_provider,
            hnsw_provider,
            spann_provider,
            fetch_log_batch_size: compactor_config.fetch_log_batch_size,
            fetch_log_concurrency: compactor_config.fetch_log_concurrency,
            max_compaction_size: compactor_config.max_compaction_size,
            max_partition_size: compactor_config.max_partition_size,
            metrics: metrics.clone(),
        };
        let (dispatch_awaiter_tx, dispatch_awaiter_rx) =
            mpsc::channel::<FnDispatchTask>(config.max_concurrent_workers.max(1));
        // Every dispatched function sends exactly one completion, and we retain its
        // in-progress slot until that completion is drained. Therefore, pending
        // completions are bounded by max_concurrent_workers and need no backpressure.
        let (completion_tx, completion_rx) = mpsc::unbounded_channel::<FnDispatchCompletion>();
        let completion_notifier = std::sync::Arc::new(OnceCell::new());
        let awaiter_notifier = completion_notifier.clone();
        let dispatch_awaiter = tokio::spawn(async move {
            fn_dispatch_awaiter_loop(dispatch_awaiter_rx, completion_tx, awaiter_notifier).await;
        });
        Ok(Self {
            context,
            in_progress: HashMap::new(),
            work_queue_client,
            dispatch_awaiter_channel: dispatch_awaiter_tx,
            dispatch_awaiter_completion_channel: completion_rx,
            dispatch_awaiter,
            completion_notifier,
            memory_admission,
            reserved_bytes: 0,
            bypass_counts: HashMap::new(),
            backfill_required: HashSet::new(),
            metrics,
        })
    }

    pub fn set_dispatcher(&mut self, dispatcher: ComponentHandle<Dispatcher>) {
        self.context.dispatcher = Some(dispatcher);
    }

    fn warn_expired(&mut self) {
        for (fn_id, job) in &mut self.in_progress {
            if job.is_expired() && !job.expiry_logged {
                tracing::warn!(
                    fn_id = %fn_id,
                    "Function consumer dispatch exceeded its expiry; retaining slot until completion"
                );
                job.expiry_logged = true;
            }
        }
    }

    fn compute_remaining_capacity(&self) -> usize {
        self.context
            .max_concurrent_workers
            .saturating_sub(self.in_progress.len())
    }

    fn fn_in_progress(&self, fn_id: AttachedFunctionUuid) -> bool {
        self.in_progress.contains_key(&fn_id)
    }

    async fn plan_input(
        &self,
        fn_id: AttachedFunctionUuid,
        collection_id: CollectionUuid,
        queue_compaction_offset: i64,
    ) -> Result<
        (
            FunctionExecutionInput,
            AttachedFunction,
            Option<FunctionWorkload>,
        ),
        PlanningError,
    > {
        let mut sysdb = self.context.sysdb.clone();
        let attached_function = sysdb
            .get_attached_functions(None, Some(collection_id), vec![], true)
            .await
            .map_err(|error| PlanningError::Transient(error.to_string()))?
            .into_iter()
            .find(|attached_function| attached_function.id == fn_id)
            .ok_or_else(|| {
                PlanningError::Transient(format!(
                    "attached function {fn_id} was not found for input {collection_id}"
                ))
            })?;
        let completion_offset =
            i64::try_from(attached_function.completion_offset).map_err(|_| {
                PlanningError::Unaddressable(format!(
                    "attached function completion offset {} exceeds i64",
                    attached_function.completion_offset
                ))
            })?;

        // Stale work is cheap but still dispatched so the existing finish path
        // can retire it from the work queue.
        if has_reached_frontier(completion_offset, queue_compaction_offset) {
            return Ok((
                unplanned_input(collection_id, queue_compaction_offset),
                attached_function,
                Some(FunctionWorkload::current()),
            ));
        }

        let collection = sysdb
            .get_collections(GetCollectionsOptions {
                collection_ids: Some(vec![collection_id]),
                include_soft_deleted: false,
                limit: Some(1),
                ..Default::default()
            })
            .await
            .map_err(|error| PlanningError::Transient(error.to_string()))?
            .into_iter()
            .next();
        let Some(collection) = collection else {
            return Ok((
                unplanned_input(collection_id, queue_compaction_offset),
                attached_function,
                Some(FunctionWorkload::current()),
            ));
        };
        let database_name = DatabaseName::new(&collection.database).ok_or_else(|| {
            PlanningError::Unaddressable(format!(
                "input collection {collection_id} has an invalid database name"
            ))
        })?;
        let collection_and_segments = sysdb
            .get_collection_with_segments(Some(database_name), collection_id)
            .await
            .map_err(|error| PlanningError::Transient(error.to_string()))?;

        if self.backfill_required.contains(&(fn_id, collection_id)) {
            if collection_and_segments.collection.log_position <= completion_offset {
                return Ok((
                    unplanned_input(collection_id, queue_compaction_offset),
                    attached_function,
                    Some(FunctionWorkload::current()),
                ));
            }
            let record_count = collection_and_segments
                .collection
                .total_records_post_compaction;
            let logical_bytes = collection_and_segments
                .collection
                .size_bytes_post_compaction;
            return Ok((
                FunctionExecutionInput {
                    collection_id,
                    queue_compaction_offset,
                    plan: Some(FunctionExecutionPlan::Backfill {
                        expected_completion_offset: completion_offset,
                        target_log_position: collection_and_segments.collection.log_position,
                    }),
                },
                attached_function,
                Some(FunctionWorkload {
                    format_version: chroma_types::FUNCTION_WORKLOAD_FORMAT_VERSION,
                    source_log_records: record_count,
                    source_log_bytes: logical_bytes,
                    materialized_records: record_count,
                    non_delete_records: record_count,
                    id_bytes: 0,
                    document_bytes: logical_bytes,
                    metadata_bytes: 0,
                    embedding_bytes: 0,
                    metadata_entries: record_count,
                    max_non_embedding_record_bytes: logical_bytes,
                }),
            ));
        }

        let boundary = GetAsyncFnFetchBoundariesOperator::new()
            .run(&GetAsyncFnFetchBoundariesInput {
                collection: collection_and_segments.collection,
                record_segment: collection_and_segments.record_segment,
                completion_offset,
                max_compaction_size: self.context.max_compaction_size,
                blockfile_provider: self.context.blockfile_provider.clone(),
                selection: BoundarySelection::NextLive,
            })
            .await
            .map_err(|error| {
                let message = error.to_string();
                if message.contains("exceeds max_compaction_size") {
                    PlanningError::Unaddressable(message)
                } else {
                    PlanningError::Transient(message)
                }
            })?;
        let workload = boundary.function_workload.clone();

        Ok((
            FunctionExecutionInput {
                collection_id,
                queue_compaction_offset,
                plan: Some(FunctionExecutionPlan::Boundary(Box::new(boundary))),
            },
            attached_function,
            workload,
        ))
    }

    async fn plan_candidate(
        &self,
        candidate: OrderedWorkCandidate,
    ) -> Result<PlannedCandidate, PlanningError> {
        let mut batch = Vec::with_capacity(candidate.inputs.len());
        let mut attached_function = None;
        let mut aggregate: Option<FunctionWorkload> = None;
        let mut has_legacy_workload = false;
        let mut has_backfill = false;

        for (collection_id, queue_compaction_offset) in candidate.inputs {
            let (input, function, workload) = self
                .plan_input(candidate.fn_id, collection_id, queue_compaction_offset)
                .await?;
            has_backfill |= input
                .plan
                .as_ref()
                .is_some_and(FunctionExecutionPlan::is_backfill);
            attached_function.get_or_insert(function);
            match workload {
                Some(workload) if workload.is_supported() => match &mut aggregate {
                    Some(aggregate) => aggregate.merge(&workload),
                    None => aggregate = Some(workload),
                },
                _ => has_legacy_workload = true,
            }
            batch.push(input);
        }

        let estimate_bytes = if has_legacy_workload {
            None
        } else {
            let function = attached_function.as_ref().ok_or_else(|| {
                PlanningError::Transient("candidate had no attached function state".to_string())
            })?;
            Some(
                self.memory_admission
                    .as_ref()
                    .expect("planning is only used with memory admission")
                    .estimate(
                        function,
                        &aggregate.unwrap_or_else(FunctionWorkload::current),
                    )
                    .map_err(PlanningError::Unaddressable)?,
            )
        };
        let estimate_kind = if estimate_bytes.is_none() {
            "legacy"
        } else if has_backfill {
            "backfill"
        } else if attached_function
            .as_ref()
            .is_some_and(|function| function.function_id == chroma_types::FUNCTION_HTTP_GENERATE_ID)
        {
            "http_generate"
        } else {
            "other"
        };

        Ok(PlannedCandidate {
            fn_id: candidate.fn_id,
            batch,
            estimate_bytes,
            estimate_kind,
        })
    }

    /// Runs the attached function workflow for the given function across a batch of input collections.
    #[instrument(
        name = "FnConsumerManager::dispatch_batch",
        parent = None,
        skip(context, work_queue_client),
        err
    )]
    async fn dispatch_batch(
        context: FnConsumerContext,
        mut work_queue_client: WorkQueueClient,
        fn_id: AttachedFunctionUuid,
        batch: Vec<FunctionExecutionInput>,
    ) -> FnDispatchOutput {
        let Some(dispatcher) = context.dispatcher.clone() else {
            tracing::error!("Dispatcher not set on FnConsumerManager");
            return Err(DispatchError::DispatcherNotInitialized);
        };

        if batch.is_empty() {
            return Err(DispatchError::CompactionFailed(
                crate::execution::orchestration::compact::CompactionError::InvariantViolation(
                    "Function consumer dispatch requires at least one input collection",
                ),
            ));
        }

        // Create CompactionContext with is_fn_consumer = true. The function
        // execution flow applies each input collection's completion offset when
        // fetching logs, so the shared base context should not carry one.
        let compaction_context = CompactionContext::new(
            None, // rebuild_info
            context.fetch_log_batch_size,
            context.fetch_log_concurrency,
            context.max_compaction_size,
            context.max_partition_size,
            context.log.clone(),
            context.sysdb.clone(),
            context.blockfile_provider.clone(),
            context.hnsw_provider.clone(),
            context.spann_provider.clone(),
            dispatcher,
            false,                           // is_function_disabled
            true,                            // is_fn_consumer
            None,                            // fragment_fetcher
            None,                            // bloom_filter_manager
            None,                            // shard_size
            Some(work_queue_client.clone()), // work_queue_client
        );

        let function_execution_context = FunctionExecutionContext::new(&compaction_context);
        let result =
            Box::pin(function_execution_context.run(fn_id, batch.clone(), context.system.clone()))
                .await;

        match result {
            Ok(_response) => {
                tracing::info!(
                    fn_id = %fn_id,
                    batch_size = batch.len(),
                    "Function consumer workflow completed successfully"
                );
                Ok(FnDispatchOutcome::Completed)
            }
            Err(
                crate::execution::orchestration::compact::CompactionError::FunctionBackfillRequired(
                    collection_id,
                ),
            ) => {
                tracing::info!(
                    fn_id = %fn_id,
                    collection_id = %collection_id,
                    "Function input requires an explicitly planned backfill"
                );
                Ok(FnDispatchOutcome::ReplanBackfill(collection_id))
            }
            Err(crate::execution::orchestration::compact::CompactionError::FunctionPlanStale(
                collection_id,
            )) => {
                tracing::info!(
                    fn_id = %fn_id,
                    collection_id = %collection_id,
                    "Function admission plan became stale; leaving work queued"
                );
                Ok(FnDispatchOutcome::Replan)
            }
            Err(e) => {
                tracing::error!(
                    fn_id = %fn_id,
                    batch_size = batch.len(),
                    "Function consumer workflow failed: {}",
                    e,
                );
                report_batch_failure(&mut work_queue_client, fn_id, &batch, &context.metrics).await;
                Err(e.into())
            }
        }
    }

    fn process_completions(&mut self) {
        while let Ok(completion) = self.dispatch_awaiter_completion_channel.try_recv() {
            if let Some(in_progress) = self.in_progress.remove(&completion.fn_id) {
                self.reserved_bytes = self
                    .reserved_bytes
                    .saturating_sub(in_progress.reserved_bytes);
                self.metrics.reserved_bytes.record(self.reserved_bytes, &[]);
            }

            match completion.result {
                Ok(FnDispatchOutcome::Completed) => {
                    for input in &completion.batch {
                        if input
                            .plan
                            .as_ref()
                            .is_some_and(FunctionExecutionPlan::is_backfill)
                        {
                            self.backfill_required
                                .remove(&(completion.fn_id, input.collection_id));
                        }
                    }
                    tracing::debug!(
                        fn_id = %completion.fn_id,
                        batch_size = completion.batch.len(),
                        "Successfully completed work batch"
                    );
                }
                Ok(FnDispatchOutcome::Replan) => {}
                Ok(FnDispatchOutcome::ReplanBackfill(collection_id)) => {
                    self.backfill_required
                        .insert((completion.fn_id, collection_id));
                }
                Err(e) => {
                    tracing::warn!(
                        fn_id = %completion.fn_id,
                        batch_size = completion.batch.len(),
                        error = %e,
                        "Failed to process work batch"
                    );
                }
            }
        }
    }

    async fn poll_and_dispatch(&mut self) {
        let span = tracing::debug_span!("FnConsumerManager::poll_and_dispatch");
        let _guard = span.enter();

        self.process_completions();
        self.warn_expired();
        let mut remaining_capacity = self.compute_remaining_capacity();
        if remaining_capacity == 0 {
            tracing::debug!("fn_consumer at capacity, skipping poll");
            return;
        }
        let limit = self
            .memory_admission
            .as_ref()
            .map_or(self.context.get_work_batch_size, |admission| {
                admission.lookahead_size
            });
        let resp = match self
            .work_queue_client
            .get_work_with_failure_limit(
                self.context.my_member_id.clone(),
                limit,
                self.context.max_failure_count,
            )
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                tracing::error!("GetWork failed: {}", e);
                return;
            }
        };
        let mut candidates = Vec::<OrderedWorkCandidate>::new();
        let mut candidate_indices = HashMap::<AttachedFunctionUuid, usize>::new();
        for item in resp.items {
            let Ok(fn_id) = item.fn_id.parse::<AttachedFunctionUuid>() else {
                tracing::error!(fn_id = item.fn_id, "skipping work item: invalid fn_id");
                continue;
            };
            let Ok(input_coll_id) = item.input_coll_id.parse::<CollectionUuid>() else {
                tracing::error!(
                    input_coll_id = item.input_coll_id,
                    "skipping work item: invalid input_coll_id"
                );
                continue;
            };
            let Some(compaction_offset) = item.compaction_offset else {
                tracing::error!(
                    fn_id = %fn_id,
                    input_coll_id = %input_coll_id,
                    completion_offset = item.completion_offset,
                    "skipping work item: missing required compaction_offset"
                );
                continue;
            };

            let candidate_index = *candidate_indices.entry(fn_id).or_insert_with(|| {
                candidates.push(OrderedWorkCandidate {
                    fn_id,
                    inputs: Vec::new(),
                });
                candidates.len() - 1
            });
            candidates[candidate_index]
                .inputs
                .push((input_coll_id, compaction_offset));
        }

        let mut batches_to_process =
            Vec::<(AttachedFunctionUuid, Vec<FunctionExecutionInput>, u64)>::new();
        if self.memory_admission.is_none() {
            for candidate in candidates {
                if remaining_capacity == 0 {
                    break;
                }
                if self.fn_in_progress(candidate.fn_id) {
                    continue;
                }
                let batch = candidate
                    .inputs
                    .into_iter()
                    .map(|(collection_id, queue_compaction_offset)| {
                        unplanned_input(collection_id, queue_compaction_offset)
                    })
                    .collect::<Vec<_>>();
                if !batch.is_empty() {
                    batches_to_process.push((candidate.fn_id, batch, 0));
                    remaining_capacity -= 1;
                }
            }
        } else {
            let admission = match self.memory_admission.clone() {
                Some(admission) => admission,
                None => return,
            };
            let mut skipped_for_memory = Vec::<AttachedFunctionUuid>::new();
            for candidate in candidates {
                if remaining_capacity == 0 {
                    break;
                }
                if self.fn_in_progress(candidate.fn_id) {
                    continue;
                }

                let candidate_fn_id = candidate.fn_id;
                let unaddressable_batch = candidate
                    .inputs
                    .iter()
                    .map(|(collection_id, queue_compaction_offset)| {
                        unplanned_input(*collection_id, *queue_compaction_offset)
                    })
                    .collect::<Vec<_>>();
                let planned = match self.plan_candidate(candidate).await {
                    Ok(planned) => planned,
                    Err(PlanningError::Transient(error)) => {
                        tracing::warn!(%error, "Function memory planning failed; leaving work queued");
                        continue;
                    }
                    Err(PlanningError::Unaddressable(error)) => {
                        tracing::error!(%error, "Function invocation is unaddressable");
                        self.metrics.unaddressable_count.add(1, &[]);
                        report_batch_failure(
                            &mut self.work_queue_client,
                            candidate_fn_id,
                            &unaddressable_batch,
                            &self.metrics,
                        )
                        .await;
                        continue;
                    }
                };
                let current_bytes = match admission.current_bytes() {
                    Ok(current_bytes) => current_bytes,
                    Err(error) => {
                        tracing::error!(%error, "Cannot read cgroup memory usage; failing admission closed");
                        break;
                    }
                };
                self.metrics.cgroup_current_bytes.record(current_bytes, &[]);
                if let Some(peak_bytes) = admission.peak_bytes() {
                    match peak_bytes {
                        Ok(peak_bytes) => self.metrics.cgroup_peak_bytes.record(peak_bytes, &[]),
                        Err(error) => {
                            tracing::warn!(%error, "Cannot read cgroup peak memory usage")
                        }
                    }
                }

                let (decision, reservation_bytes) = match planned.estimate_bytes {
                    Some(estimate_bytes) => (
                        admission.decide(self.reserved_bytes, current_bytes, estimate_bytes),
                        estimate_bytes,
                    ),
                    None => {
                        let exclusive = self.in_progress.is_empty()
                            && batches_to_process.is_empty()
                            && self.reserved_bytes == 0;
                        let reservation = admission.exclusive_fallback_reservation(current_bytes);
                        let decision = if exclusive && reservation > 0 {
                            AdmissionDecision::Admit
                        } else {
                            AdmissionDecision::Wait
                        };
                        (decision, reservation)
                    }
                };

                match decision {
                    AdmissionDecision::Unaddressable => {
                        self.metrics.unaddressable_count.add(1, &[]);
                        tracing::error!(
                            fn_id = %planned.fn_id,
                            estimate_bytes = reservation_bytes,
                            budget_bytes = admission.schedulable_budget_bytes(),
                            "Function invocation exceeds total schedulable memory"
                        );
                        report_batch_failure(
                            &mut self.work_queue_client,
                            planned.fn_id,
                            &planned.batch,
                            &self.metrics,
                        )
                        .await;
                        self.bypass_counts.remove(&planned.fn_id);
                    }
                    AdmissionDecision::Wait => {
                        let bypass_count = self
                            .bypass_counts
                            .get(&planned.fn_id)
                            .copied()
                            .unwrap_or_default();
                        if bypass_count >= admission.max_bypass_count {
                            tracing::debug!(
                                fn_id = %planned.fn_id,
                                bypass_count,
                                "Memory-bound invocation is an ordering barrier"
                            );
                            break;
                        }
                        skipped_for_memory.push(planned.fn_id);
                    }
                    AdmissionDecision::Admit => {
                        let bypasses = skipped_for_memory.len() as u64;
                        let reached_barrier = record_bypasses(
                            &mut self.bypass_counts,
                            skipped_for_memory.iter().copied(),
                            admission.max_bypass_count,
                        );
                        self.metrics.bypass_count.add(bypasses, &[]);
                        self.bypass_counts.remove(&planned.fn_id);
                        self.reserved_bytes = self.reserved_bytes.saturating_add(reservation_bytes);
                        self.metrics.reserved_bytes.record(self.reserved_bytes, &[]);
                        self.metrics.admitted_estimate_bytes.record(
                            reservation_bytes,
                            &[KeyValue::new("kind", planned.estimate_kind)],
                        );
                        if planned.estimate_bytes.is_none() {
                            self.metrics.fallback_count.add(1, &[]);
                        }
                        tracing::info!(
                            fn_id = %planned.fn_id,
                            estimate_bytes = reservation_bytes,
                            reserved_bytes = self.reserved_bytes,
                            current_bytes,
                            budget_bytes = admission.schedulable_budget_bytes(),
                            headroom_bytes = admission.headroom_bytes(),
                            estimate_kind = planned.estimate_kind,
                            "Admitted function invocation"
                        );
                        let is_exclusive_fallback = planned.estimate_bytes.is_none();
                        batches_to_process.push((planned.fn_id, planned.batch, reservation_bytes));
                        remaining_capacity -= 1;
                        if is_exclusive_fallback || reached_barrier {
                            break;
                        }
                    }
                }
            }
        }

        for (fn_id, batch, reservation_bytes) in batches_to_process {
            self.in_progress.insert(
                fn_id,
                InProgressFn::new(self.context.job_expiry_seconds, reservation_bytes),
            );
            let task = FnDispatchTask {
                fn_id,
                future: Box::pin(Self::dispatch_batch(
                    self.context.clone(),
                    self.work_queue_client.clone(),
                    fn_id,
                    batch.clone(),
                )),
                work_queue_client: Some(self.work_queue_client.clone()),
                batch,
                metrics: self.metrics.clone(),
            };
            if let Err(e) = self.dispatch_awaiter_channel.send(task).await {
                if let Some(in_progress) = self.in_progress.remove(&fn_id) {
                    self.reserved_bytes = self
                        .reserved_bytes
                        .saturating_sub(in_progress.reserved_bytes);
                    self.metrics.reserved_bytes.record(self.reserved_bytes, &[]);
                }
                tracing::error!(
                    fn_id = %fn_id,
                    error = ?e,
                    "Failed to enqueue function dispatch task"
                );
            }
        }
    }
}

async fn report_batch_failure(
    work_queue_client: &mut WorkQueueClient,
    fn_id: AttachedFunctionUuid,
    batch: &[FunctionExecutionInput],
    metrics: &FnConsumerMetrics,
) {
    for item in batch {
        if let Err(report_error) = work_queue_client
            .fail_function(fn_id.to_string(), item.collection_id.to_string())
            .await
        {
            tracing::error!(
                fn_id = %fn_id,
                input_coll_id = %item.collection_id,
                error = %report_error,
                "Failed to report attached function execution failure"
            );
        } else {
            metrics.dlq_increment_count.add(1, &[]);
        }
    }
}

fn panic_message(panic_payload: &(dyn Any + Send)) -> String {
    if let Some(message) = panic_payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic_payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

async fn fn_dispatch_awaiter_loop(
    mut task_rx: mpsc::Receiver<FnDispatchTask>,
    completion_tx: mpsc::UnboundedSender<FnDispatchCompletion>,
    completion_notifier: std::sync::Arc<
        OnceCell<Box<dyn ReceiverForMessage<DispatchCompletedMessage>>>,
    >,
) {
    let mut futures = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            Some(completion) = futures.next() => {
                if completion_tx.send(completion).is_err() {
                    tracing::error!("Failed to record function dispatch result");
                } else if let Some(receiver) = completion_notifier.get() {
                    if let Err(error) = receiver.send(DispatchCompletedMessage, None).await {
                        tracing::error!(%error, "Failed to trigger immediate fn-consumer refill");
                    }
                }
            }
            Some(task) = task_rx.recv() => {
                futures.push(async move {
                    let FnDispatchTask {
                        fn_id,
                        future,
                        mut work_queue_client,
                        batch,
                        metrics,
                    } = task;
                    let result = AssertUnwindSafe(future).catch_unwind().await;
                    let result = match result {
                        Ok(result) => result,
                        Err(panic_payload) => {
                            tracing::error!(
                                fn_id = %fn_id,
                                panic = %panic_message(&*panic_payload),
                                "Function consumer dispatch task panicked"
                            );
                            if let Some(work_queue_client) = work_queue_client.as_mut() {
                                report_batch_failure(
                                    work_queue_client,
                                    fn_id,
                                    &batch,
                                    &metrics,
                                )
                                .await;
                            }
                            Err(DispatchError::DispatchPanicked)
                        }
                    };
                    FnDispatchCompletion {
                        fn_id,
                        batch,
                        result,
                    }
                });
            }
            else => break,
        }
    }
}

impl Drop for FnConsumerManager {
    fn drop(&mut self) {
        self.dispatch_awaiter.abort();
    }
}

#[derive(Clone, Debug)]
pub struct ScheduledPollMessage;

#[async_trait]
impl Component for FnConsumerManager {
    fn get_name() -> &'static str {
        "Fn consumer manager"
    }

    fn queue_size(&self) -> usize {
        1000
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        tracing::info!("Starting FnConsumerManager");
        if self
            .completion_notifier
            .set(ctx.receiver::<DispatchCompletedMessage>())
            .is_err()
        {
            tracing::error!("Fn-consumer completion notifier was already initialized");
        }
        ctx.scheduler.schedule(
            ScheduledPollMessage,
            self.context.poll_interval,
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled fn-consumer poll")),
        );
    }
}

#[async_trait]
impl Handler<DispatchCompletedMessage> for FnConsumerManager {
    type Result = ();

    async fn handle(&mut self, _: DispatchCompletedMessage, _ctx: &ComponentContext<Self>) {
        Box::pin(self.poll_and_dispatch()).await;
    }
}

#[async_trait]
impl Handler<ScheduledPollMessage> for FnConsumerManager {
    type Result = ();

    async fn handle(&mut self, _: ScheduledPollMessage, ctx: &ComponentContext<Self>) {
        Box::pin(self.poll_and_dispatch()).await;
        ctx.scheduler.schedule(
            ScheduledPollMessage,
            self.context.poll_interval,
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled fn-consumer poll")),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;
    use tokio::time::{timeout, Duration};

    fn unused_notifier(
    ) -> std::sync::Arc<OnceCell<Box<dyn ReceiverForMessage<DispatchCompletedMessage>>>> {
        std::sync::Arc::new(OnceCell::new())
    }

    #[tokio::test]
    async fn dispatch_awaiter_completes_later_tasks_while_one_is_running() {
        let (task_tx, task_rx) = mpsc::channel(2);
        let (completion_tx, mut completion_rx) = mpsc::unbounded_channel();
        let awaiter = tokio::spawn(fn_dispatch_awaiter_loop(
            task_rx,
            completion_tx,
            unused_notifier(),
        ));
        let slow_fn_id = AttachedFunctionUuid::new();
        let fast_fn_id = AttachedFunctionUuid::new();
        let (slow_started_tx, slow_started_rx) = oneshot::channel();
        let (release_slow_tx, release_slow_rx) = oneshot::channel();

        task_tx
            .send(FnDispatchTask {
                fn_id: slow_fn_id,
                future: Box::pin(async move {
                    let _ = slow_started_tx.send(());
                    let _ = release_slow_rx.await;
                    Ok(FnDispatchOutcome::Completed)
                }),
                work_queue_client: None,
                batch: Vec::new(),
                metrics: FnConsumerMetrics::default(),
            })
            .await
            .unwrap();
        slow_started_rx.await.unwrap();

        task_tx
            .send(FnDispatchTask {
                fn_id: fast_fn_id,
                future: Box::pin(async { Ok(FnDispatchOutcome::Completed) }),
                work_queue_client: None,
                batch: Vec::new(),
                metrics: FnConsumerMetrics::default(),
            })
            .await
            .unwrap();

        let completion = timeout(Duration::from_secs(1), completion_rx.recv())
            .await
            .expect("fast task should complete while slow task is running")
            .expect("completion channel should remain open");
        assert_eq!(completion.fn_id, fast_fn_id);
        completion
            .result
            .expect("fast task should complete successfully");

        release_slow_tx.send(()).unwrap();
        let completion = timeout(Duration::from_secs(1), completion_rx.recv())
            .await
            .expect("slow task should complete after release")
            .expect("completion channel should remain open");
        assert_eq!(completion.fn_id, slow_fn_id);
        completion
            .result
            .expect("slow task should complete successfully");

        drop(task_tx);
        awaiter.await.unwrap();
    }

    #[tokio::test]
    async fn dispatch_awaiter_completes_panicked_tasks() {
        let (task_tx, task_rx) = mpsc::channel(1);
        let (completion_tx, mut completion_rx) = mpsc::unbounded_channel();
        let awaiter = tokio::spawn(fn_dispatch_awaiter_loop(
            task_rx,
            completion_tx,
            unused_notifier(),
        ));
        let fn_id = AttachedFunctionUuid::new();

        task_tx
            .send(FnDispatchTask {
                fn_id,
                future: Box::pin(async { panic!("expected test panic") }),
                work_queue_client: None,
                batch: Vec::new(),
                metrics: FnConsumerMetrics::default(),
            })
            .await
            .unwrap();

        let completion = timeout(Duration::from_secs(1), completion_rx.recv())
            .await
            .expect("panicked task should complete")
            .expect("completion channel should remain open");
        assert_eq!(completion.fn_id, fn_id);
        assert!(matches!(
            completion.result,
            Err(DispatchError::DispatchPanicked)
        ));

        drop(task_tx);
        awaiter.await.unwrap();
    }

    #[test]
    fn formats_panic_payloads_for_logging() {
        assert_eq!(panic_message(&"panic message"), "panic message");
        assert_eq!(panic_message(&"panic message".to_owned()), "panic message");
        assert_eq!(panic_message(&42_u32), "non-string panic payload");
    }

    #[test]
    fn bypasses_become_a_barrier_after_the_configured_bound() {
        let fn_id = AttachedFunctionUuid::new();
        let mut counts = HashMap::new();

        assert!(!record_bypasses(&mut counts, [fn_id], 2));
        assert_eq!(counts, HashMap::from([(fn_id, 1)]));
        assert!(record_bypasses(&mut counts, [fn_id], 2));
        assert_eq!(counts, HashMap::from([(fn_id, 2)]));
    }
}
