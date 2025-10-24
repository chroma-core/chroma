use std::{
    cell::OnceCell,
    collections::HashMap,
    path::Path,
    sync::{atomic::AtomicU32, Arc},
};

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{hnsw_provider::HnswIndexProvider, IndexUuid};
use chroma_log::Log;
use chroma_segment::{
    blockfile_metadata::{MetadataSegmentError, MetadataSegmentWriter},
    blockfile_record::{
        ApplyMaterializedLogError, RecordSegmentReader, RecordSegmentReaderCreationError,
        RecordSegmentWriter, RecordSegmentWriterCreationError,
    },
    distributed_hnsw::{DistributedHNSWSegmentFromSegmentError, DistributedHNSWSegmentWriter},
    distributed_spann::SpannSegmentWriterError,
    spann_provider::SpannProvider,
    types::{
        ChromaSegmentFlusher, ChromaSegmentWriter, MaterializeLogsResult, VectorSegmentWriter,
    },
};
use chroma_sysdb::SysDb;
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{
    Chunk, Collection, CollectionUuid, LogRecord, NonceUuid, Schema, SchemaError, Segment,
    SegmentFlushInfo, SegmentType, SegmentUuid, Task, TaskUuid,
};
use opentelemetry::trace::TraceContextExt;
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::execution::operators::{
    apply_log_to_segment_writer::{
        ApplyLogToSegmentWriterInput, ApplyLogToSegmentWriterOperator,
        ApplyLogToSegmentWriterOperatorError, ApplyLogToSegmentWriterOutput,
    },
    commit_segment_writer::{
        CommitSegmentWriterInput, CommitSegmentWriterOperator, CommitSegmentWriterOperatorError,
        CommitSegmentWriterOutput,
    },
    execute_task::{
        CountTask, ExecuteTaskError, ExecuteTaskInput, ExecuteTaskOperator, ExecuteTaskOutput,
    },
    fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
    finish_task::{FinishTaskError, FinishTaskInput, FinishTaskOperator, FinishTaskOutput},
    flush_segment_writer::{
        FlushSegmentWriterInput, FlushSegmentWriterOperator, FlushSegmentWriterOperatorError,
        FlushSegmentWriterOutput,
    },
    get_collection_and_segments::{
        GetCollectionAndSegmentsError, GetCollectionAndSegmentsOperator,
        GetCollectionAndSegmentsOutput,
    },
    materialize_logs::{
        MaterializeLogInput, MaterializeLogOperator, MaterializeLogOperatorError,
        MaterializeLogOutput,
    },
    partition_log::{PartitionError, PartitionInput, PartitionOperator, PartitionOutput},
    prefetch_segment::{
        PrefetchSegmentError, PrefetchSegmentInput, PrefetchSegmentOperator, PrefetchSegmentOutput,
    },
    prepare_task::{PrepareTaskError, PrepareTaskInput, PrepareTaskOperator, PrepareTaskOutput},
    register::{RegisterError, RegisterInput, RegisterOperator, RegisterOutput},
    source_record_segment::{
        SourceRecordSegmentError, SourceRecordSegmentInput, SourceRecordSegmentOperator,
        SourceRecordSegmentOutput,
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
struct CompactOrchestratorMetrics {
    total_logs_applied_flushed: opentelemetry::metrics::Counter<u64>,
}

impl Default for CompactOrchestratorMetrics {
    fn default() -> Self {
        let meter = opentelemetry::global::meter("chroma.compactor");
        CompactOrchestratorMetrics {
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
enum ExecutionState {
    Pending,
    Partition,
    MaterializeApplyCommitFlush,
    Register,
    FinishTask,
}

#[derive(Clone, Debug)]
pub(crate) struct TaskContext {
    pub(crate) task_id: TaskUuid,
    pub(crate) task: Option<Task>,
    pub(crate) execution_nonce: NonceUuid,
}

#[derive(Clone, Debug)]
pub(crate) struct CompactWriters {
    pub(crate) record_reader: Option<RecordSegmentReader<'static>>,
    pub(crate) metadata_writer: MetadataSegmentWriter<'static>,
    pub(crate) record_writer: RecordSegmentWriter,
    pub(crate) vector_writer: VectorSegmentWriter,
}

#[derive(Debug)]
pub struct CompactOrchestrator {
    // === Compaction Configuration ===
    hnsw_index_uuid: Option<IndexUuid>,
    rebuild: bool,
    fetch_log_batch_size: u32,
    max_compaction_size: usize,
    max_partition_size: usize,

    // === Shared Services & Providers ===
    context: OrchestratorContext,
    blockfile_provider: BlockfileProvider,
    log: Log,
    sysdb: SysDb,
    hnsw_provider: HnswIndexProvider,
    spann_provider: SpannProvider,

    // === Input Collection (read logs/segments from) ===
    /// Collection to read logs and segments from
    /// For regular compaction: input_collection_id == output_collection_id
    /// For task compaction: input_collection_id != output_collection_id
    input_collection_id: CollectionUuid,
    input_collection: OnceCell<Collection>,
    input_segments: OnceCell<Vec<Segment>>,
    /// How much to pull from fetch_logs for INPUT collection
    pulled_log_offset: i64,

    // === Output Collection (write compacted data to) ===
    /// Collection to write compacted segments to
    output_collection_id: OnceCell<CollectionUuid>,
    output_collection: OnceCell<Collection>,
    output_segments: OnceCell<Vec<Segment>>,

    // === Writers & Results ===
    writers: OnceCell<CompactWriters>,
    flush_results: Vec<SegmentFlushInfo>,
    result_channel: Option<Sender<Result<CompactionResponse, CompactionError>>>,

    // === State Tracking ===
    num_uncompleted_materialization_tasks: usize,
    num_uncompleted_tasks_by_segment: HashMap<SegmentUuid, usize>,
    collection_logical_size_delta_bytes: i64,
    state: ExecutionState,

    // Total number of records in the collection after the compaction
    total_records_post_compaction: u64,

    // Total number of materialized logs
    num_materialized_logs: u64,

    // We track a parent span for each segment type so we can group all the spans for a given segment type (makes the resulting trace much easier to read)
    segment_spans: HashMap<SegmentUuid, Span>,

    metrics: CompactOrchestratorMetrics,

    // schema after applying deltas
    schema: Option<Schema>,
    // === Task Context (optional) ===
    /// Available if this orchestrator is for a task
    task_context: Option<TaskContext>,
}

#[derive(Error, Debug)]
pub enum CompactionError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error applying logs to segment writers: {0}")]
    ApplyLog(#[from] ApplyLogToSegmentWriterOperatorError),
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error commiting segment writers: {0}")]
    Commit(#[from] CommitSegmentWriterOperatorError),
    #[error("Error executing task: {0}")]
    ExecuteTask(#[from] ExecuteTaskError),
    #[error("Error fetching logs: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error finishing task: {0}")]
    FinishTask(#[from] FinishTaskError),
    #[error("Error flushing segment writers: {0}")]
    Flush(#[from] FlushSegmentWriterOperatorError),
    #[error("Error getting collection and segments: {0}")]
    GetCollectionAndSegments(#[from] GetCollectionAndSegmentsError),
    #[error("Error creating hnsw writer: {0}")]
    HnswSegment(#[from] DistributedHNSWSegmentFromSegmentError),
    #[error("Invariant violation: {}", .0)]
    InvariantViolation(&'static str),
    #[error("Error materializing logs: {0}")]
    MaterializeLogs(#[from] MaterializeLogOperatorError),
    #[error("Error creating metadata writer: {0}")]
    MetadataSegment(#[from] MetadataSegmentError),
    #[error("Panic during compaction: {0}")]
    Panic(#[from] PanicError),
    #[error("Error partitioning logs: {0}")]
    Partition(#[from] PartitionError),
    #[error("Error prefetching segment: {0}")]
    PrefetchSegment(#[from] PrefetchSegmentError),
    #[error("Error preparing task: {0}")]
    PrepareTask(#[from] PrepareTaskError),
    #[error("Error creating record segment reader: {0}")]
    RecordSegmentReader(#[from] RecordSegmentReaderCreationError),
    #[error("Error creating record segment writer: {0}")]
    RecordSegmentWriter(#[from] RecordSegmentWriterCreationError),
    #[error("Error registering compaction result: {0}")]
    Register(#[from] RegisterError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Error creating spann writer: {0}")]
    SpannSegment(#[from] SpannSegmentWriterError),
    #[error("Error sourcing record segment: {0}")]
    SourceRecordSegment(#[from] SourceRecordSegmentError),
    #[error("Could not count current segment: {0}")]
    CountError(Box<dyn chroma_error::ChromaError>),
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
            _ => ErrorCodes::Internal,
        }
    }

    fn should_trace_error(&self) -> bool {
        if let CompactionError::FetchLog(FetchLogError::PullLog(e)) = self {
            e.code() != ErrorCodes::NotFound
        } else {
            match self {
                Self::Aborted => true,
                Self::ApplyLog(e) => e.should_trace_error(),
                Self::Channel(e) => e.should_trace_error(),
                Self::Commit(e) => e.should_trace_error(),
                Self::ExecuteTask(e) => e.should_trace_error(),
                Self::FetchLog(e) => e.should_trace_error(),
                Self::FinishTask(e) => e.should_trace_error(),
                Self::Flush(e) => e.should_trace_error(),
                Self::GetCollectionAndSegments(e) => e.should_trace_error(),
                Self::HnswSegment(e) => e.should_trace_error(),
                Self::InvariantViolation(_) => true,
                Self::MaterializeLogs(e) => e.should_trace_error(),
                Self::MetadataSegment(e) => e.should_trace_error(),
                Self::Panic(e) => e.should_trace_error(),
                Self::Partition(e) => e.should_trace_error(),
                Self::PrefetchSegment(e) => e.should_trace_error(),
                Self::PrepareTask(e) => e.should_trace_error(),
                Self::RecordSegmentReader(e) => e.should_trace_error(),
                Self::RecordSegmentWriter(e) => e.should_trace_error(),
                Self::Register(e) => e.should_trace_error(),
                Self::Result(_) => true,
                Self::SpannSegment(e) => e.should_trace_error(),
                Self::SourceRecordSegment(e) => e.should_trace_error(),
                Self::CountError(e) => e.should_trace_error(),
            }
        }
    }
}

#[derive(Debug)]
pub enum CompactionResponse {
    Success {
        job_id: Uuid,
    },
    RequireCompactionOffsetRepair {
        collection_id: CollectionUuid,
        witnessed_offset_in_sysdb: i64,
    },
}

impl CompactOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_collection_id: CollectionUuid,
        rebuild: bool,
        fetch_log_batch_size: u32,
        max_compaction_size: usize,
        max_partition_size: usize,
        log: Log,
        sysdb: SysDb,
        blockfile_provider: BlockfileProvider,
        hnsw_provider: HnswIndexProvider,
        spann_provider: SpannProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        result_channel: Option<Sender<Result<CompactionResponse, CompactionError>>>,
    ) -> Self {
        let context = OrchestratorContext::new(dispatcher);
        let output_collection_cell = OnceCell::new();
        // SAFETY(tanujnay112): We just created the OnceCell, so this should never fail
        output_collection_cell.set(input_collection_id).unwrap();
        CompactOrchestrator {
            hnsw_index_uuid: None,
            rebuild,
            fetch_log_batch_size,
            max_compaction_size,
            max_partition_size,
            context,
            blockfile_provider,
            log,
            sysdb,
            hnsw_provider,
            spann_provider,
            input_collection_id,
            input_collection: OnceCell::new(),
            input_segments: OnceCell::new(),
            pulled_log_offset: 0,
            output_collection_id: output_collection_cell,
            output_collection: OnceCell::new(),
            output_segments: OnceCell::new(),
            writers: OnceCell::new(),
            flush_results: Vec::new(),
            result_channel,
            num_uncompleted_materialization_tasks: 0,
            num_uncompleted_tasks_by_segment: HashMap::new(),
            collection_logical_size_delta_bytes: 0,
            state: ExecutionState::Pending,
            total_records_post_compaction: 0,
            num_materialized_logs: 0,
            segment_spans: HashMap::new(),
            metrics: CompactOrchestratorMetrics::default(),
            schema: None,
            task_context: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_for_task(
        input_collection_id: CollectionUuid,
        rebuild: bool,
        fetch_log_batch_size: u32,
        max_compaction_size: usize,
        max_partition_size: usize,
        log: Log,
        sysdb: SysDb,
        blockfile_provider: BlockfileProvider,
        hnsw_provider: HnswIndexProvider,
        spann_provider: SpannProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        result_channel: Option<Sender<Result<CompactionResponse, CompactionError>>>,
        task_uuid: TaskUuid,
        execution_nonce: NonceUuid,
    ) -> Self {
        let mut orchestrator = CompactOrchestrator::new(
            input_collection_id,
            rebuild,
            fetch_log_batch_size,
            max_compaction_size,
            max_partition_size,
            log,
            sysdb,
            blockfile_provider,
            hnsw_provider,
            spann_provider,
            dispatcher,
            result_channel,
        );
        orchestrator.task_context = Some(TaskContext {
            task_id: task_uuid,
            task: None,
            execution_nonce,
        });
        orchestrator
    }

    async fn try_purge_hnsw(path: &Path, hnsw_index_uuid: Option<IndexUuid>) {
        if let Some(hnsw_index_uuid) = hnsw_index_uuid {
            let _ = HnswIndexProvider::purge_one_id(path, hnsw_index_uuid).await;
        }
    }

    async fn do_task(
        &mut self,
        log_records: Chunk<LogRecord>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        // Get all needed data, cloning immediately to avoid borrow conflicts
        let task = match self.get_task().cloned() {
            Ok(t) => t,
            Err(e) => {
                self.terminate_with_result(Err(e), ctx).await;
                return;
            }
        };

        let output_collection = match self
            .ok_or_terminate(self.get_output_collection(), ctx)
            .await
        {
            Some(collection) => collection,
            None => return,
        };

        let output_record_segment = match self
            .ok_or_terminate(self.get_output_record_segment(), ctx)
            .await
        {
            Some(segment) => segment,
            None => return,
        };

        let output_collection_id = match self
            .ok_or_terminate(self.get_output_collection_id(), ctx)
            .await
        {
            Some(id) => id,
            None => return,
        };

        // TODO: Get the actual task executor based on operator_id
        // For now, hardcode CountTask as a placeholder
        let task_executor = Arc::new(CountTask);

        let execute_task_op = ExecuteTaskOperator {
            log_client: self.log.clone(),
            task_executor,
        };

        let execute_task_input = ExecuteTaskInput {
            log_records,
            tenant_id: output_collection.tenant.clone(),
            output_collection_id,
            completion_offset: task.completion_offset,
            output_record_segment,
            blockfile_provider: self.blockfile_provider.clone(),
        };

        let task_msg = wrap(
            Box::new(execute_task_op),
            execute_task_input,
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task_msg, ctx, Some(Span::current())).await;
    }

    async fn partition(
        &mut self,
        records: Chunk<LogRecord>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        self.state = ExecutionState::Partition;
        let operator = PartitionOperator::new();
        tracing::info!("Sending N Records: {:?}", records.len());
        let input = PartitionInput::new(records, self.max_partition_size);
        let task = wrap(
            operator,
            input,
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
    }

    async fn materialize_log(
        &mut self,
        partitions: Vec<Chunk<LogRecord>>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        self.state = ExecutionState::MaterializeApplyCommitFlush;

        // NOTE: We allow writers to be uninitialized for the case when the materialized logs are empty
        let record_reader = self
            .get_segment_writers()
            .ok()
            .and_then(|writers| writers.record_reader);

        let next_max_offset_id = Arc::new(
            record_reader
                .as_ref()
                .map(|reader| AtomicU32::new(reader.get_max_offset_id() + 1))
                .unwrap_or_default(),
        );

        if let Some(rr) = record_reader.as_ref() {
            self.total_records_post_compaction = match rr.count().await {
                Ok(count) => count as u64,
                Err(err) => {
                    return self
                        .terminate_with_result(Err(CompactionError::CountError(err)), ctx)
                        .await;
                }
            };
        }

        self.num_uncompleted_materialization_tasks = partitions.len();
        for partition in partitions.iter() {
            let operator = MaterializeLogOperator::new();
            let input = MaterializeLogInput::new(
                partition.clone(),
                record_reader.clone(),
                next_max_offset_id.clone(),
            );
            let task = wrap(
                operator,
                input,
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            self.send(task, ctx, Some(Span::current())).await;
        }
    }

    async fn dispatch_apply_log_to_segment_writer_tasks(
        &mut self,
        materialized_logs: MaterializeLogsResult,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        self.num_materialized_logs += materialized_logs.len() as u64;

        let writers = match self.ok_or_terminate(self.get_segment_writers(), ctx).await {
            Some(writers) => writers,
            None => return,
        };

        {
            self.num_uncompleted_tasks_by_segment
                .entry(writers.record_writer.id)
                .and_modify(|v| {
                    *v += 1;
                })
                .or_insert(1);

            let writer = ChromaSegmentWriter::RecordSegment(writers.record_writer);
            let span = self.get_segment_writer_span(&writer);
            let operator = ApplyLogToSegmentWriterOperator::new();
            let input = ApplyLogToSegmentWriterInput::new(
                writer,
                materialized_logs.clone(),
                writers.record_reader.clone(),
                None,
            );
            let task = wrap(
                operator,
                input,
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            let res = self.dispatcher().send(task, Some(span)).await;
            if self.ok_or_terminate(res, ctx).await.is_none() {
                return;
            }
        }

        {
            self.num_uncompleted_tasks_by_segment
                .entry(writers.metadata_writer.id)
                .and_modify(|v| {
                    *v += 1;
                })
                .or_insert(1);

            let writer = ChromaSegmentWriter::MetadataSegment(writers.metadata_writer);
            let span = self.get_segment_writer_span(&writer);
            let operator = ApplyLogToSegmentWriterOperator::new();
            let input = ApplyLogToSegmentWriterInput::new(
                writer,
                materialized_logs.clone(),
                writers.record_reader.clone(),
                self.output_collection.get().and_then(|c| c.schema.clone()),
            );
            let task = wrap(
                operator,
                input,
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            let res = self.dispatcher().send(task, Some(span)).await;
            if self.ok_or_terminate(res, ctx).await.is_none() {
                return;
            }
        }

        {
            self.num_uncompleted_tasks_by_segment
                .entry(writers.vector_writer.get_id())
                .and_modify(|v| {
                    *v += 1;
                })
                .or_insert(1);

            let writer = ChromaSegmentWriter::VectorSegment(writers.vector_writer);
            let span = self.get_segment_writer_span(&writer);
            let operator = ApplyLogToSegmentWriterOperator::new();
            let input = ApplyLogToSegmentWriterInput::new(
                writer,
                materialized_logs,
                writers.record_reader,
                None,
            );
            let task = wrap(
                operator,
                input,
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            let res = self.dispatcher().send(task, Some(span)).await;
            self.ok_or_terminate(res, ctx).await;
        }
    }

    async fn dispatch_segment_writer_commit(
        &mut self,
        segment_writer: ChromaSegmentWriter<'static>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let span = self.get_segment_writer_span(&segment_writer);
        let operator = CommitSegmentWriterOperator::new();
        let input = CommitSegmentWriterInput::new(segment_writer);
        let task = wrap(
            operator,
            input,
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        let res = self.dispatcher().send(task, Some(span)).await;
        self.ok_or_terminate(res, ctx).await;
    }

    async fn dispatch_segment_flush(
        &mut self,
        segment_flusher: ChromaSegmentFlusher,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let span = self.get_segment_flusher_span(&segment_flusher);
        let operator = FlushSegmentWriterOperator::new();
        let input = FlushSegmentWriterInput::new(segment_flusher);
        let task = wrap(
            operator,
            input,
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        let res = self.dispatcher().send(task, Some(span)).await;
        self.ok_or_terminate(res, ctx).await;
    }

    async fn register(&mut self, ctx: &ComponentContext<CompactOrchestrator>) {
        self.metrics
            .total_logs_applied_flushed
            .add(self.num_materialized_logs, &[]);

        self.state = ExecutionState::Register;
        // Register uses OUTPUT collection
        let collection_cell =
            self.output_collection
                .get()
                .cloned()
                .ok_or(CompactionError::InvariantViolation(
                    "Output collection information should have been obtained",
                ));
        let collection = match self.ok_or_terminate(collection_cell, ctx).await {
            Some(collection) => collection,
            None => return,
        };
        let collection_logical_size_bytes = if self.rebuild {
            match u64::try_from(self.collection_logical_size_delta_bytes) {
                Ok(size_bytes) => size_bytes,
                _ => {
                    self.terminate_with_result(
                        Err(CompactionError::InvariantViolation(
                            "The collection size delta after rebuild should be non-negative",
                        )),
                        ctx,
                    )
                    .await;
                    return;
                }
            }
        } else {
            collection
                .size_bytes_post_compaction
                .saturating_add_signed(self.collection_logical_size_delta_bytes)
        };
        let operator = RegisterOperator::new();
        let input = RegisterInput::new(
            collection.tenant,
            collection.collection_id,
            self.pulled_log_offset,
            collection.version,
            self.flush_results.clone().into(),
            self.total_records_post_compaction,
            collection_logical_size_bytes,
            self.sysdb.clone(),
            self.log.clone(),
            self.schema.clone(),
            self.task_context.clone(),
        );

        let task = wrap(
            operator,
            input,
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
    }

    fn get_segment_writers(&self) -> Result<CompactWriters, CompactionError> {
        self.writers
            .get()
            .cloned()
            .ok_or(CompactionError::InvariantViolation(
                "Segment writers should have been set",
            ))
    }

    async fn get_segment_writer_by_id(
        &mut self,
        segment_id: SegmentUuid,
    ) -> Result<ChromaSegmentWriter<'static>, CompactionError> {
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

        Err(CompactionError::InvariantViolation(
            "Segment id should match one of the writer segment id",
        ))
    }

    fn get_segment_writer_span(&mut self, writer: &ChromaSegmentWriter) -> Span {
        let span = self
            .segment_spans
            .entry(writer.get_id())
            .or_insert_with(|| {
                tracing::span!(
                    tracing::Level::INFO,
                    "Segment",
                    otel.name = format!("Segment: {:?}", writer.get_name())
                )
            });
        span.clone()
    }

    fn get_segment_flusher_span(&mut self, flusher: &ChromaSegmentFlusher) -> Span {
        match self.segment_spans.get(&flusher.get_id()) {
            Some(span) => span.clone(),
            None => {
                tracing::error!(
                    "No span found for segment: {:?}. This should never happen because get_segment_writer_span() should have previously created a span.",
                    flusher.get_name()
                );
                Span::current()
            }
        }
    }

    /// Get task_context or return error
    fn get_task_context(&self) -> Result<&TaskContext, CompactionError> {
        self.task_context
            .as_ref()
            .ok_or(CompactionError::InvariantViolation(
                "Task context should be set for task-based compaction",
            ))
    }

    /// Get mutable task_context or return error
    fn get_task_context_mut(&mut self) -> Result<&mut TaskContext, CompactionError> {
        self.task_context
            .as_mut()
            .ok_or(CompactionError::InvariantViolation(
                "Task context should be set for task-based compaction",
            ))
    }

    /// Get task from task_context or return error
    fn get_task(&self) -> Result<&Task, CompactionError> {
        let task_context = self.get_task_context()?;
        task_context
            .task
            .as_ref()
            .ok_or(CompactionError::InvariantViolation(
                "Task should be populated by PrepareTask",
            ))
    }

    /// Get output_collection or return error
    fn get_output_collection(&self) -> Result<Collection, CompactionError> {
        self.output_collection
            .get()
            .cloned()
            .ok_or(CompactionError::InvariantViolation(
                "Output collection should be set",
            ))
    }

    /// Get output_collection_id or return error
    fn get_output_collection_id(&self) -> Result<CollectionUuid, CompactionError> {
        self.output_collection_id
            .get()
            .copied()
            .ok_or(CompactionError::InvariantViolation(
                "Output collection ID should be set",
            ))
    }

    /// Get output_segments or return error
    fn get_output_segments(&self) -> Result<Vec<Segment>, CompactionError> {
        self.output_segments
            .get()
            .cloned()
            .ok_or(CompactionError::InvariantViolation(
                "Output segments should be set",
            ))
    }

    /// Get output record segment or return error
    fn get_output_record_segment(&self) -> Result<Segment, CompactionError> {
        let segments = self.get_output_segments()?;
        segments
            .iter()
            .find(|s| s.r#type == SegmentType::BlockfileRecord)
            .cloned()
            .ok_or(CompactionError::InvariantViolation(
                "Output record segment should exist",
            ))
    }

    /// Get input_collection or return error
    fn get_input_collection(&self) -> Result<Collection, CompactionError> {
        self.input_collection
            .get()
            .cloned()
            .ok_or(CompactionError::InvariantViolation(
                "Input collection should be set",
            ))
    }
}

// ============== Component Implementation ==============

#[async_trait]
impl Orchestrator for CompactOrchestrator {
    type Output = CompactionResponse;
    type Error = CompactionError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.context.dispatcher.clone()
    }

    fn context(&self) -> &OrchestratorContext {
        &self.context
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        // For task-based compaction, start with PrepareTask to fetch the task
        if let Some(task_context) = self.task_context.as_ref() {
            return vec![(
                wrap(
                    Box::new(PrepareTaskOperator {
                        sysdb: self.sysdb.clone(),
                        log: self.log.clone(),
                        task_uuid: task_context.task_id,
                    }),
                    PrepareTaskInput {
                        nonce: task_context.execution_nonce,
                    },
                    ctx.receiver(),
                    self.context.task_cancellation_token.clone(),
                ),
                Some(Span::current()),
            )];
        }

        // For non-task compaction, start with GetCollectionAndSegments
        vec![(
            wrap(
                Box::new(GetCollectionAndSegmentsOperator {
                    sysdb: self.sysdb.clone(),
                    input_collection_id: self.input_collection_id,
                    output_collection_id: *self.output_collection_id.get().expect(
                        "output_collection_id should be set in constructor, should not fail",
                    ),
                }),
                (),
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            ),
            Some(Span::current()),
        )]
    }

    fn set_result_channel(&mut self, sender: Sender<Result<CompactionResponse, CompactionError>>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(
        &mut self,
    ) -> Option<Sender<Result<CompactionResponse, CompactionError>>> {
        self.result_channel.take()
    }

    async fn cleanup(&mut self) {
        Self::try_purge_hnsw(
            &self.hnsw_provider.temporary_storage_path,
            self.hnsw_index_uuid,
        )
        .await
    }
}

// ============== Handlers ==============
#[async_trait]
impl Handler<TaskResult<PrepareTaskOutput, PrepareTaskError>> for CompactOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<PrepareTaskOutput, PrepareTaskError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        tracing::info!(
            "[CompactOrchestrator] PrepareTask completed, task_id={}, execution_nonce={}",
            output.task.id.0,
            output.execution_nonce
        );

        // Store the task and execution_nonce in task_context
        let task_context = match self.get_task_context_mut() {
            Ok(tc) => tc,
            Err(e) => {
                self.terminate_with_result(Err(e), ctx).await;
                return;
            }
        };
        task_context.task = Some(output.task.clone());
        task_context.execution_nonce = output.execution_nonce;
        self.output_collection_id = output.output_collection_id.into();

        if output.should_skip_execution {
            // Proceed to FinishTask
            let task = wrap(
                FinishTaskOperator::new(self.log.clone(), self.sysdb.clone()),
                FinishTaskInput::new(output.task),
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            self.send(task, ctx, Some(Span::current())).await;
            return;
        }

        // Proceed to GetCollectionAndSegments
        let task = wrap(
            Box::new(GetCollectionAndSegmentsOperator {
                sysdb: self.sysdb.clone(),
                input_collection_id: self.input_collection_id,
                output_collection_id: output.output_collection_id,
            }),
            (),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<GetCollectionAndSegmentsOutput, GetCollectionAndSegmentsError>>
    for CompactOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<GetCollectionAndSegmentsOutput, GetCollectionAndSegmentsError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        // Store input collection and segments
        let mut input_collection = output.input.collection.clone();
        if self.input_collection.set(input_collection.clone()).is_err() {
            self.terminate_with_result(
                Err(CompactionError::InvariantViolation(
                    "Input collection information should not have been initialized",
                )),
                ctx,
            )
            .await;
            return;
        }
        self.schema = input_collection.schema.clone();
        // Create input segments vec from individual segment fields
        let input_segments = vec![
            output.input.metadata_segment.clone(),
            output.input.record_segment.clone(),
            output.input.vector_segment.clone(),
        ];
        if self.input_segments.set(input_segments).is_err() {
            self.terminate_with_result(
                Err(CompactionError::InvariantViolation(
                    "Input segments should not have been initialized",
                )),
                ctx,
            )
            .await;
            return;
        }

        // Store output collection
        let output_collection = output.output.collection.clone();
        if self
            .output_collection
            .set(output_collection.clone())
            .is_err()
        {
            self.terminate_with_result(
                Err(CompactionError::InvariantViolation(
                    "Output collection information should not have been initialized",
                )),
                ctx,
            )
            .await;
            return;
        }

        // Create output segments vec from individual segment fields
        let output_segments = vec![
            output.output.metadata_segment.clone(),
            output.output.record_segment.clone(),
            output.output.vector_segment.clone(),
        ];
        if self.output_segments.set(output_segments).is_err() {
            self.terminate_with_result(
                Err(CompactionError::InvariantViolation(
                    "Output segments should not have been initialized",
                )),
                ctx,
            )
            .await;
            return;
        }

        // TODO(tanujnay112): move this somewhere cleaner
        if let Some(task_context) = &self.task_context {
            let Some(task) = &task_context.task else {
                self.terminate_with_result(
                    Err(CompactionError::InvariantViolation(
                        "Task should not have been initialized",
                    )),
                    ctx,
                )
                .await;
                return;
            };

            let result: i64 = match task.completion_offset.try_into() {
                Ok(value) => value,
                Err(_) => {
                    self.terminate_with_result(
                        Err(CompactionError::InvariantViolation(
                            "Completion offset does not fit into an i64",
                        )),
                        ctx,
                    )
                    .await;
                    return;
                }
            };
            input_collection.log_position = result;
        }

        // Set pulled_log_offset from INPUT collection's log position
        self.pulled_log_offset = input_collection.log_position;

        // Create record reader from INPUT segments (for reading existing data)
        let input_record_reader = match self
            .ok_or_terminate(
                match Box::pin(RecordSegmentReader::from_segment(
                    &output.input.record_segment,
                    &self.blockfile_provider,
                ))
                .await
                {
                    Ok(reader) => Ok(Some(reader)),
                    Err(err) => match *err {
                        RecordSegmentReaderCreationError::UninitializedSegment => Ok(None),
                        _ => Err(*err),
                    },
                },
                ctx,
            )
            .await
        {
            Some(reader) => reader,
            None => return,
        };

        let log_task = match self.rebuild || self.task_context.is_some() {
            true => wrap(
                Box::new(SourceRecordSegmentOperator {}),
                SourceRecordSegmentInput {
                    record_segment_reader: input_record_reader.clone(),
                },
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            ),
            false => wrap(
                Box::new(FetchLogOperator {
                    log_client: self.log.clone(),
                    batch_size: self.fetch_log_batch_size,
                    // We need to start fetching from the first log that has not been compacted from INPUT collection
                    start_log_offset_id: u64::try_from(input_collection.log_position + 1)
                        .unwrap_or_default(),
                    maximum_fetch_count: Some(self.max_compaction_size as u32),
                    collection_uuid: self.input_collection_id, // Fetch logs from INPUT collection
                    tenant: input_collection.tenant.clone(),
                }),
                (),
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            ),
        };

        // Check dimension from OUTPUT collection (writers will be for output)
        let dimension = match output_collection.dimension {
            Some(dim) => dim as usize,
            None => {
                // Collection is not yet initialized, there is no need to initialize the writers
                // Future handlers should return early on empty materialized logs without using writers
                self.send(log_task, ctx, Some(Span::current())).await;
                return;
            }
        };

        // Create writers from OUTPUT collection segments
        let mut metadata_segment = output.output.metadata_segment.clone();
        let mut record_segment = output.output.record_segment.clone();
        let mut vector_segment = output.output.vector_segment.clone();
        if self.rebuild {
            // Reset the metadata and vector segments by purging the file paths
            metadata_segment.file_path = Default::default();
            record_segment.file_path = Default::default();
            vector_segment.file_path = Default::default();
        }

        let record_writer = match self
            .ok_or_terminate(
                RecordSegmentWriter::from_segment(
                    &output_collection.tenant,
                    &output_collection.database_id,
                    &record_segment,
                    &self.blockfile_provider,
                )
                .await,
                ctx,
            )
            .await
        {
            Some(writer) => writer,
            None => return,
        };
        let metadata_writer = match self
            .ok_or_terminate(
                MetadataSegmentWriter::from_segment(
                    &output_collection.tenant,
                    &output_collection.database_id,
                    &metadata_segment,
                    &self.blockfile_provider,
                )
                .await,
                ctx,
            )
            .await
        {
            Some(writer) => writer,
            None => return,
        };
        let (hnsw_index_uuid, vector_writer, is_vector_segment_spann) = match vector_segment.r#type
        {
            SegmentType::Spann => match self
                .ok_or_terminate(
                    self.spann_provider
                        .write(&output_collection, &vector_segment, dimension)
                        .await,
                    ctx,
                )
                .await
            {
                Some(writer) => (
                    writer.hnsw_index_uuid(),
                    VectorSegmentWriter::Spann(writer),
                    true,
                ),
                None => return,
            },
            _ => match self
                .ok_or_terminate(
                    DistributedHNSWSegmentWriter::from_segment(
                        &output_collection,
                        &vector_segment,
                        dimension,
                        self.hnsw_provider.clone(),
                    )
                    .await
                    .map_err(|err| *err),
                    ctx,
                )
                .await
            {
                Some(writer) => (
                    writer.index_uuid(),
                    VectorSegmentWriter::Hnsw(writer),
                    false,
                ),
                None => return,
            },
        };

        let mut output_record_reader = input_record_reader.clone();

        if output_collection.collection_id != input_collection.collection_id {
            output_record_reader = match self
                .ok_or_terminate(
                    match Box::pin(RecordSegmentReader::from_segment(
                        &output.output.record_segment,
                        &self.blockfile_provider,
                    ))
                    .await
                    {
                        Ok(reader) => Ok(Some(reader)),
                        Err(err) => match *err {
                            RecordSegmentReaderCreationError::UninitializedSegment => Ok(None),
                            _ => Err(*err),
                        },
                    },
                    ctx,
                )
                .await
            {
                Some(reader) => reader,
                None => return,
            };
        }
        let writers = CompactWriters {
            record_reader: output_record_reader.clone().filter(|_| !self.rebuild),
            metadata_writer,
            record_writer,
            vector_writer,
        };

        if self.writers.set(writers).is_err() {
            self.terminate_with_result(
                Err(CompactionError::InvariantViolation(
                    "Segment writers should not have been initialized",
                )),
                ctx,
            )
            .await;
            return;
        }

        self.hnsw_index_uuid = Some(hnsw_index_uuid);

        // Prefetch segments (OUTPUT segments where we write to)
        let prefetch_segments = match self.rebuild {
            true => vec![output.output.record_segment],
            false => {
                let mut segments =
                    vec![output.output.metadata_segment, output.output.record_segment];
                if is_vector_segment_spann {
                    segments.push(output.output.vector_segment);
                }
                segments
            }
        };
        for segment in prefetch_segments {
            let segment_id = segment.id;
            let prefetch_task = wrap(
                Box::new(PrefetchSegmentOperator::new()),
                PrefetchSegmentInput::new(segment, self.blockfile_provider.clone()),
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );

            // Prefetch task is detached from the orchestrator
            let prefetch_span =
                tracing::info_span!(parent: None, "Prefetch segment", segment_id = %segment_id);
            Span::current().add_link(prefetch_span.context().span().span_context().clone());

            self.send(prefetch_task, ctx, Some(prefetch_span)).await;
        }

        self.send(log_task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>> for CompactOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        self.ok_or_terminate(message.into_inner(), ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for CompactOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchLogOutput, FetchLogError>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(recs) => recs,
            None => {
                tracing::info!("cancelled fetch log task");
                return;
            }
        };
        tracing::info!("Pulled Records: {}", output.len());
        match output.iter().last() {
            Some((rec, _)) => {
                self.pulled_log_offset = rec.log_offset;
                tracing::info!("Pulled Logs Up To Offset: {:?}", self.pulled_log_offset);
            }
            None => {
                tracing::warn!("No logs were pulled from the log service, this can happen when the log compaction offset is behing the sysdb.");
                if let Some(collection) = self.input_collection.get() {
                    self.terminate_with_result(
                        Ok(CompactionResponse::RequireCompactionOffsetRepair {
                            collection_id: collection.collection_id,
                            witnessed_offset_in_sysdb: collection.log_position,
                        }),
                        ctx,
                    )
                    .await;
                } else {
                    self.terminate_with_result(
                        Err(CompactionError::InvariantViolation(
                            "self.input_collection not set",
                        )),
                        ctx,
                    )
                    .await;
                }
                return;
            }
        }

        // For task-based compaction, call ExecuteTask to run task logic
        if self.task_context.is_some() {
            self.do_task(output, ctx).await;
        } else {
            // For regular compaction, go directly to partition
            self.partition(output, ctx).await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<ExecuteTaskOutput, ExecuteTaskError>> for CompactOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ExecuteTaskOutput, ExecuteTaskError>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        tracing::info!(
            "[CompactOrchestrator] ExecuteTask completed. Processed {} records",
            output.records_processed
        );

        // Proceed to partition the output records from the task
        self.partition(output.output_records, ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<SourceRecordSegmentOutput, SourceRecordSegmentError>>
    for CompactOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<SourceRecordSegmentOutput, SourceRecordSegmentError>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        tracing::info!("Sourced Records: {}", output.len());
        // Each record should corresond to a log
        self.total_records_post_compaction = output.len() as u64;
        if output.is_empty() && self.task_context.is_none() {
            let writers = match self.ok_or_terminate(self.get_segment_writers(), ctx).await {
                Some(writer) => writer,
                None => return,
            };
            self.dispatch_segment_writer_commit(
                ChromaSegmentWriter::MetadataSegment(writers.metadata_writer),
                ctx,
            )
            .await;
            self.dispatch_segment_writer_commit(
                ChromaSegmentWriter::VectorSegment(writers.vector_writer),
                ctx,
            )
            .await;
        } else if self.task_context.is_some() {
            let input_collection =
                match self.ok_or_terminate(self.get_input_collection(), ctx).await {
                    Some(collection) => collection,
                    None => return,
                };
            self.pulled_log_offset = input_collection.log_position;
            self.do_task(output, ctx).await;
        } else {
            self.partition(output, ctx).await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<PartitionOutput, PartitionError>> for CompactOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<PartitionOutput, PartitionError>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(recs) => recs.records,
            None => return,
        };
        self.materialize_log(output, ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<MaterializeLogOutput, MaterializeLogOperatorError>>
    for CompactOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<MaterializeLogOutput, MaterializeLogOperatorError>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(res) => res,
            None => return,
        };

        if output.result.is_empty() {
            // We check the number of remaining materialization tasks to prevent a race condition
            if self.num_uncompleted_materialization_tasks == 1
                && self.num_uncompleted_tasks_by_segment.is_empty()
            {
                // There is nothing to flush, proceed to register
                self.register(ctx).await;
            }
        } else {
            self.collection_logical_size_delta_bytes += output.collection_logical_size_delta;
            Box::pin(self.dispatch_apply_log_to_segment_writer_tasks(output.result, ctx)).await;
        }

        self.num_uncompleted_materialization_tasks -= 1;
    }
}

#[async_trait]
impl Handler<TaskResult<ApplyLogToSegmentWriterOutput, ApplyLogToSegmentWriterOperatorError>>
    for CompactOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ApplyLogToSegmentWriterOutput, ApplyLogToSegmentWriterOperatorError>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let message = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(message) => message,
            None => return,
        };

        if message.segment_type == "MetadataSegmentWriter" {
            if let Some(update) = message.schema_update {
                match self.schema.take() {
                    Some(existing) => match existing.merge(&update) {
                        Ok(merged) => {
                            self.schema = Some(merged);
                        }
                        Err(err) => {
                            let err = CompactionError::ApplyLog(
                                ApplyLogToSegmentWriterOperatorError::ApplyMaterializedLogsError(
                                    ApplyMaterializedLogError::Schema(err),
                                ),
                            );
                            self.terminate_with_result(Err(err), ctx).await;
                            return;
                        }
                    },
                    None => {
                        let err = CompactionError::ApplyLog(
                            ApplyLogToSegmentWriterOperatorError::ApplyMaterializedLogsError(
                                ApplyMaterializedLogError::Schema(SchemaError::InvalidSchema {
                                    reason: "schema not found".to_string(),
                                }),
                            ),
                        );
                        self.terminate_with_result(Err(err), ctx).await;
                        return;
                    }
                }
            }
        }
        self.num_uncompleted_tasks_by_segment
            .entry(message.segment_id)
            .and_modify(|v| {
                *v -= 1;
            });

        let num_tasks_left = {
            let num_tasks_left = self
                .num_uncompleted_tasks_by_segment
                .get(&message.segment_id)
                .ok_or(CompactionError::InvariantViolation(
                    "Invariant violation: segment writer task count not found",
                ))
                .cloned();
            match self.ok_or_terminate(num_tasks_left, ctx).await {
                Some(num_tasks_left) => num_tasks_left,
                None => return,
            }
        };

        if num_tasks_left == 0 && self.num_uncompleted_materialization_tasks == 0 {
            let segment_writer = self.get_segment_writer_by_id(message.segment_id).await;
            let segment_writer = match self.ok_or_terminate(segment_writer, ctx).await {
                Some(writer) => writer,
                None => return,
            };

            self.dispatch_segment_writer_commit(segment_writer, ctx)
                .await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<CommitSegmentWriterOutput, CommitSegmentWriterOperatorError>>
    for CompactOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<CommitSegmentWriterOutput, CommitSegmentWriterOperatorError>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let message = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(message) => message,
            None => return,
        };

        // If the flusher recieved is a record segment flusher, get the number of keys for the blockfile and set it on the orchestrator
        if let ChromaSegmentFlusher::RecordSegment(record_segment_flusher) = &message.flusher {
            self.total_records_post_compaction = record_segment_flusher.count();
        }

        self.dispatch_segment_flush(message.flusher, ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<FlushSegmentWriterOutput, FlushSegmentWriterOperatorError>>
    for CompactOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FlushSegmentWriterOutput, FlushSegmentWriterOperatorError>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let message = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(message) => message,
            None => return,
        };

        let segment_id = message.flush_info.segment_id;

        // Drops the span so that the end timestamp is accurate
        let _ = self.segment_spans.remove(&segment_id);

        self.flush_results.push(message.flush_info);
        self.num_uncompleted_tasks_by_segment.remove(&segment_id);

        if self.num_uncompleted_tasks_by_segment.is_empty() {
            self.register(ctx).await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<FinishTaskOutput, FinishTaskError>> for CompactOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FinishTaskOutput, FinishTaskError>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        self.state = ExecutionState::FinishTask;
        let _finish_output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        let output_collection_id = match self.get_output_collection_id() {
            Ok(id) => id,
            Err(e) => {
                self.terminate_with_result(Err(e), ctx).await;
                return;
            }
        };

        let task_id = match self.get_task_context().map(|tc| tc.task_id) {
            Ok(id) => id,
            Err(e) => {
                self.terminate_with_result(Err(e), ctx).await;
                return;
            }
        };

        tracing::info!(
            "Task finish_task completed for output collection {}",
            output_collection_id
        );

        // Task verification complete, terminate with success
        // TODO(tanujnay112): This no longer applied to functions, change the return type
        // to a more suitable name.
        self.terminate_with_result(Ok(CompactionResponse::Success { job_id: task_id.0 }), ctx)
            .await;
    }
}

#[async_trait]
impl Handler<TaskResult<RegisterOutput, RegisterError>> for CompactOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<RegisterOutput, RegisterError>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let register_output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        // If this was a task-based compaction, invoke finish_task operator
        if let Some(updated_task) = register_output.updated_task {
            tracing::info!(
                "Invoking finish_task operator for task {}",
                updated_task.id.0
            );

            let finish_task_op = FinishTaskOperator::new(self.log.clone(), self.sysdb.clone());
            let finish_task_input = FinishTaskInput::new(updated_task);

            let task = wrap(
                finish_task_op,
                finish_task_input,
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            self.send(task, ctx, Some(Span::current())).await;
        } else {
            // No task, terminate immediately with success
            let output_collection_id = match self
                .ok_or_terminate(self.get_output_collection_id(), ctx)
                .await
            {
                Some(id) => id,
                None => return,
            };
            self.terminate_with_result(
                Ok(CompactionResponse::Success {
                    job_id: output_collection_id.0,
                }),
                ctx,
            )
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use chroma_blockstore::provider::BlockfileProvider;
    use chroma_config::{registry::Registry, Configurable};
    use chroma_log::{
        in_memory_log::{InMemoryLog, InternalLogRecord},
        test::{add_delete_generator, LogGenerator},
        Log,
    };
    use chroma_segment::{blockfile_record::RecordSegmentReader, test::TestDistributedSegment};
    use chroma_sysdb::{SysDb, TestSysDb};
    use chroma_system::{Dispatcher, Orchestrator, System};
    use chroma_types::{
        operator::{Filter, Limit, Projection},
        CollectionUuid, DocumentExpression, DocumentOperator, MetadataExpression,
        PrimitiveOperator, Where,
    };
    use regex::Regex;

    use crate::{
        config::RootConfig,
        execution::{operators::fetch_log::FetchLogOperator, orchestration::get::GetOrchestrator},
    };

    use super::CompactOrchestrator;

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

        let compact_orchestrator = CompactOrchestrator::new(
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
            None,
        );
        assert!(compact_orchestrator.run(system.clone()).await.is_ok());

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

        let rebuild_orchestrator = CompactOrchestrator::new(
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
            None,
        );
        assert!(rebuild_orchestrator.run(system.clone()).await.is_ok());

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

    // Helper to read total_count from task result metadata
    async fn get_total_count_output(
        sysdb: &mut SysDb,
        collection_id: CollectionUuid,
        blockfile_provider: &BlockfileProvider,
    ) -> i64 {
        let output_info = sysdb
            .get_collection_with_segments(collection_id)
            .await
            .expect("Should get output collection");
        let reader = Box::pin(RecordSegmentReader::from_segment(
            &output_info.record_segment,
            blockfile_provider,
        ))
        .await
        .expect("Should create reader");
        let offset_id = reader
            .get_offset_id_for_user_id("task_result")
            .await
            .expect("Should get offset")
            .expect("task_result should exist");
        let data_record = reader
            .get_data_for_offset_id(offset_id)
            .await
            .expect("Should get data")
            .expect("Data should exist");
        let metadata = data_record.metadata.expect("Metadata should exist");
        match metadata.get("total_count") {
            Some(chroma_types::MetadataValue::Int(c)) => *c,
            _ => panic!("total_count should be an Int"),
        }
    }

    // This does an end to end test of attached function execution. It first creates a collection,
    // then attached a record counting function to it. Once a few records have been added to the
    // collection and compacted, the attached function is manually run. The attached function
    // should create the output collection and populate it with the total number of records in the input
    // collection. The test verified the completion offset and the lowest live offset of the attached function
    // entry in sysdb after this run.
    // The above is done twice.
    #[tokio::test]
    async fn test_k8s_integration_task_execution() {
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

        // Create input collection via HTTP API
        let collection_name = format!("test_task_collection_{}", uuid::Uuid::new_v4());

        let collection_id = CollectionUuid::new();
        sysdb
            .create_collection(
                test_segments.collection.tenant,
                test_segments.collection.database,
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
        let input_collection_id = collection_id;
        let tenant = "default_tenant".to_string();
        let db = "default_database".to_string();

        // Update input collection's log_position to -1 (no logs compacted yet)
        sysdb
            .flush_compaction(
                tenant.clone(),
                input_collection_id,
                -1,                      // log_position = -1 means no logs compacted yet
                0,                       // collection_version
                std::sync::Arc::new([]), // no segment flushes
                0,                       // total_records
                0,                       // size_bytes
                None,                    // schema
            )
            .await
            .expect("Should be able to update log_position");

        // Add 50 log records
        add_delete_generator
            .generate_vec(1..=50)
            .into_iter()
            .for_each(|log| {
                in_memory_log.add_log(
                    input_collection_id,
                    InternalLogRecord {
                        collection_id: input_collection_id,
                        log_offset: log.log_offset - 1,
                        log_ts: log.log_offset,
                        record: log,
                    },
                )
            });
        let log = Log::InMemory(in_memory_log.clone());
        let task_name = "test_count_task";

        //  Create a task via sysdb
        let task_id = sysdb
            .create_task(
                task_name.to_string(),
                "record_counter".to_string(),
                input_collection_id,
                format!("test_output_collection_{}", uuid::Uuid::new_v4()),
                serde_json::Value::Null,
                tenant.clone(),
                db.clone(),
                10,
            )
            .await
            .expect("Task creation should succeed");

        // compact everything
        let compact_orchestrator = CompactOrchestrator::new(
            input_collection_id,
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
            None,
        );

        let result = compact_orchestrator.run(system.clone()).await;
        assert!(
            result.is_ok(),
            "First compaction should succeed: {:?}",
            result.err()
        );

        // Fetch the task to get the current nonce
        let task_before_run = sysdb
            .get_task_by_name(input_collection_id, task_name.to_string())
            .await
            .expect("Task should be found");
        let execution_nonce = task_before_run.next_nonce;

        // Run first compaction (PrepareTask will fetch and populate the task)
        let compact_orchestrator = CompactOrchestrator::new_for_task(
            input_collection_id,
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
            None,
            task_id,
            execution_nonce,
        );
        let result = compact_orchestrator.run(system.clone()).await;
        assert!(
            result.is_ok(),
            "First task run should succeed: {:?}",
            result.err()
        );
        // Verify task was updated with output collection ID
        let updated_task = sysdb
            .get_task_by_name(input_collection_id, task_name.to_string())
            .await
            .expect("Task should be found");
        assert_eq!(
            updated_task.completion_offset, 49,
            "Processed logs 0-49, so completion_offset should be 49 (last offset processed)"
        );

        assert_eq!(
            updated_task.lowest_live_nonce,
            Some(updated_task.next_nonce),
            "After a successful run, lowest_live_nonce should be equal to next_nonce"
        );

        let output_collection_id = updated_task.output_collection_id.unwrap();

        // Verify first run: Read total_count from task result metadata
        let total_count = Box::pin(get_total_count_output(
            &mut sysdb,
            output_collection_id,
            &test_segments.blockfile_provider,
        ))
        .await;
        assert_eq!(
            total_count, 34,
            "CountTask should have counted 34 records in input collection"
        );

        tracing::info!(
            "First task run completed. CountTask result: total_count={}",
            total_count
        );

        // SECOND ATTACHED FUNCTION INVOCATION

        // Add 50 more records and run again
        add_delete_generator
            .generate_vec(51..=100)
            .into_iter()
            .for_each(|log| {
                in_memory_log.add_log(
                    input_collection_id,
                    InternalLogRecord {
                        collection_id: input_collection_id,
                        log_offset: log.log_offset - 1,
                        log_ts: log.log_offset,
                        record: log,
                    },
                )
            });

        let log_2 = Log::InMemory(in_memory_log.clone());

        // compact everything
        let compact_orchestrator = CompactOrchestrator::new(
            input_collection_id,
            false,
            50,
            1000,
            50,
            log_2.clone(),
            sysdb.clone(),
            test_segments.blockfile_provider.clone(),
            test_segments.hnsw_provider.clone(),
            test_segments.spann_provider.clone(),
            dispatcher_handle.clone(),
            None,
        );

        let result = compact_orchestrator.run(system.clone()).await;
        assert!(
            result.is_ok(),
            "Second compaction should succeed: {:?}",
            result.err()
        );

        let output_collection_id = updated_task.output_collection_id.unwrap();

        // Fetch the task to get the updated nonce for second run
        let task_before_run_2 = sysdb
            .get_task_by_name(input_collection_id, "test_count_task".to_string())
            .await
            .expect("Task should be found");
        let execution_nonce_2 = task_before_run_2.next_nonce;

        // Run second task (PrepareTask will fetch updated task state)
        let compact_orchestrator_2 = CompactOrchestrator::new_for_task(
            input_collection_id,
            false,
            100,
            1000,
            50,
            log_2.clone(),
            sysdb.clone(),
            test_segments.blockfile_provider.clone(),
            test_segments.hnsw_provider.clone(),
            test_segments.spann_provider.clone(),
            dispatcher_handle.clone(),
            None,
            task_id,
            execution_nonce_2,
        );
        let result = compact_orchestrator_2.run(system.clone()).await;
        assert!(
            result.is_ok(),
            "Second task run should succeed: {:?}",
            result.err()
        );

        let updated_task_2 = sysdb
            .get_task_by_name(input_collection_id, "test_count_task".to_string())
            .await
            .expect("Task should be found");
        assert_eq!(
            updated_task_2.completion_offset, 99,
            "Processed logs 0-99, so completion_offset should be 99 (last offset processed)"
        );

        // Verify second run: Read updated total_count from task result metadata
        let total_count_2 = Box::pin(get_total_count_output(
            &mut sysdb,
            output_collection_id,
            &test_segments.blockfile_provider,
        ))
        .await;
        assert_eq!(
            total_count_2, 67,
            "CountTask should have counted 67 total records in input collection"
        );

        assert_eq!(
            updated_task_2.lowest_live_nonce,
            Some(updated_task_2.next_nonce),
            "After a successful run, lowest_live_nonce should be equal to next_nonce"
        );

        tracing::info!(
            "Task execution test completed. First run: total_count=50, Second run: total_count={}",
            total_count_2
        );
    }
}
