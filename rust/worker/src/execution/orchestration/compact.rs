use std::{
    cell::OnceCell,
    collections::HashMap,
    sync::{atomic::AtomicU32, Arc},
};

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_log::Log;
use chroma_segment::{
    blockfile_metadata::{MetadataSegmentError, MetadataSegmentWriter},
    blockfile_record::{
        RecordSegmentReader, RecordSegmentReaderCreationError, RecordSegmentWriter,
        RecordSegmentWriterCreationError,
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
    PanicError, ReceiverForMessage, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{Chunk, LogRecord, SegmentFlushInfo, SegmentType, SegmentUuid};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::{
    compactor::CompactionJob,
    execution::operators::{
        apply_log_to_segment_writer::{
            ApplyLogToSegmentWriterInput, ApplyLogToSegmentWriterOperator,
            ApplyLogToSegmentWriterOperatorError, ApplyLogToSegmentWriterOutput,
        },
        commit_segment_writer::{
            CommitSegmentWriterInput, CommitSegmentWriterOperator,
            CommitSegmentWriterOperatorError, CommitSegmentWriterOutput,
        },
        fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
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
            PrefetchSegmentError, PrefetchSegmentInput, PrefetchSegmentOperator,
            PrefetchSegmentOutput,
        },
        register::{RegisterError, RegisterInput, RegisterOperator, RegisterOutput},
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
enum ExecutionState {
    Pending,
    Partition,
    MaterializeApplyCommitFlush,
    Register,
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
    compaction_job: CompactionJob,
    state: ExecutionState,
    // Dependencies
    log: Log,
    sysdb: SysDb,
    blockfile_provider: BlockfileProvider,
    hnsw_provider: HnswIndexProvider,
    spann_provider: SpannProvider,
    // State we hold across the execution
    pulled_log_offset: Option<i64>,
    // Dispatcher
    dispatcher: ComponentHandle<Dispatcher>,
    // Tracks the total remaining number of MaterializeLogs tasks
    num_uncompleted_materialization_tasks: usize,
    // Tracks the total remaining number of tasks per segment
    num_uncompleted_tasks_by_segment: HashMap<SegmentUuid, usize>,
    // Tracks the total collection size in number of bytes
    collection_logical_size_bytes: i64,
    // Result Channel
    result_channel: Option<Sender<Result<CompactionResponse, CompactionError>>>,
    max_compaction_size: usize,
    max_partition_size: usize,
    // Populated during the compaction process
    writers: OnceCell<CompactWriters>,
    flush_results: Vec<SegmentFlushInfo>,
    // We track a parent span for each segment type so we can group all the spans for a given segment type (makes the resulting trace much easier to read)
    segment_spans: HashMap<SegmentUuid, Span>,
    // Total number of records in the collection after the compaction
    total_records_last_compaction: u64,
    // How much to pull from fetch_logs
    fetch_log_batch_size: u32,
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
    #[error("Error fetching logs: {0}")]
    FetchLog(#[from] FetchLogError),
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
    #[error("Error creating record segment reader: {0}")]
    RecordSegmentReader(#[from] RecordSegmentReaderCreationError),
    #[error("Error creating record segment writer: {0}")]
    RecordSegmentWriter(#[from] RecordSegmentWriterCreationError),
    #[error("Error registering compaction result: {0}")]
    Register(#[from] RegisterError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Error creaitng spann writer: {0}")]
    SpannSegment(#[from] SpannSegmentWriterError),
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
}

#[derive(Debug)]
pub struct CompactionResponse {
    pub(crate) compaction_job: CompactionJob,
}

impl CompactOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        compaction_job: CompactionJob,
        log: Log,
        sysdb: SysDb,
        blockfile_provider: BlockfileProvider,
        hnsw_provider: HnswIndexProvider,
        spann_provider: SpannProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        result_channel: Option<Sender<Result<CompactionResponse, CompactionError>>>,
        max_compaction_size: usize,
        max_partition_size: usize,
        fetch_log_batch_size: u32,
    ) -> Self {
        CompactOrchestrator {
            compaction_job,
            state: ExecutionState::Pending,
            log,
            sysdb,
            blockfile_provider,
            hnsw_provider,
            spann_provider,
            pulled_log_offset: None,
            dispatcher,
            num_uncompleted_materialization_tasks: 0,
            num_uncompleted_tasks_by_segment: HashMap::new(),
            collection_logical_size_bytes: 0,
            result_channel,
            max_compaction_size,
            max_partition_size,
            writers: OnceCell::new(),
            flush_results: Vec::new(),
            segment_spans: HashMap::new(),
            total_records_last_compaction: 0,
            fetch_log_batch_size,
        }
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
        let task = wrap(operator, input, ctx.receiver());
        self.send(task, ctx).await;
    }

    async fn materialize_log(
        &mut self,
        partitions: Vec<Chunk<LogRecord>>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        self.state = ExecutionState::MaterializeApplyCommitFlush;

        let writers = match self.ok_or_terminate(self.get_segment_writers(), ctx) {
            Some(writers) => writers,
            None => return,
        };

        let next_max_offset_id = Arc::new(
            writers
                .record_reader
                .as_ref()
                .map(|reader| AtomicU32::new(reader.get_max_offset_id() + 1))
                .unwrap_or_default(),
        );

        self.num_uncompleted_materialization_tasks = partitions.len();
        for partition in partitions.iter() {
            let operator = MaterializeLogOperator::new();
            let input = MaterializeLogInput::new(
                partition.clone(),
                writers.record_reader.as_ref().cloned(),
                next_max_offset_id.clone(),
            );
            let task = wrap(operator, input, ctx.receiver());
            self.send(task, ctx).await;
        }
    }

    async fn dispatch_apply_log_to_segment_writer_tasks(
        &mut self,
        materialized_logs: MaterializeLogsResult,
        self_address: Box<
            dyn ReceiverForMessage<
                TaskResult<ApplyLogToSegmentWriterOutput, ApplyLogToSegmentWriterOperatorError>,
            >,
        >,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let writers = match self.ok_or_terminate(self.get_segment_writers(), ctx) {
            Some(writers) => writers,
            None => return,
        };

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
            );
            let task = wrap(operator, input, self_address.clone());
            let res = self.dispatcher().send(task, Some(span)).await;
            match self.ok_or_terminate(res, ctx) {
                Some(_) => (),
                None => return,
            }
        }

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
            );
            let task = wrap(operator, input, self_address.clone());
            let res = self.dispatcher().send(task, Some(span)).await;
            match self.ok_or_terminate(res, ctx) {
                Some(_) => (),
                None => return,
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
            let input =
                ApplyLogToSegmentWriterInput::new(writer, materialized_logs, writers.record_reader);
            let task = wrap(operator, input, self_address);
            let res = self.dispatcher().send(task, Some(span)).await;
            self.ok_or_terminate(res, ctx);
        }
    }

    async fn dispatch_segment_writer_commit(
        &mut self,
        segment_writer: ChromaSegmentWriter<'static>,
        self_address: Box<
            dyn ReceiverForMessage<
                TaskResult<CommitSegmentWriterOutput, CommitSegmentWriterOperatorError>,
            >,
        >,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let span = self.get_segment_writer_span(&segment_writer);
        let operator = CommitSegmentWriterOperator::new();
        let input = CommitSegmentWriterInput::new(segment_writer);
        let task = wrap(operator, input, self_address);
        let res = self.dispatcher().send(task, Some(span)).await;
        self.ok_or_terminate(res, ctx);
    }

    async fn dispatch_segment_flush(
        &mut self,
        segment_flusher: ChromaSegmentFlusher,
        self_address: Box<
            dyn ReceiverForMessage<
                TaskResult<FlushSegmentWriterOutput, FlushSegmentWriterOperatorError>,
            >,
        >,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let span = self.get_segment_flusher_span(&segment_flusher);
        let operator = FlushSegmentWriterOperator::new();
        let input = FlushSegmentWriterInput::new(segment_flusher);
        let task = wrap(operator, input, self_address);
        let res = self.dispatcher().send(task, Some(span)).await;
        self.ok_or_terminate(res, ctx);
    }

    async fn register(&mut self, log_position: i64, ctx: &ComponentContext<CompactOrchestrator>) {
        self.state = ExecutionState::Register;
        let operator = RegisterOperator::new();
        let input = RegisterInput::new(
            self.compaction_job.tenant_id.clone(),
            self.compaction_job.collection_id,
            log_position,
            self.compaction_job.collection_version,
            self.flush_results.clone().into(),
            self.total_records_last_compaction,
            // WARN: For legacy collections the logical size is initialized to zero, so the size after compaction might be negative
            // TODO: Backfill collection logical size
            u64::try_from(self.collection_logical_size_bytes).unwrap_or_default(),
            self.sysdb.clone(),
            self.log.clone(),
        );

        let task = wrap(operator, input, ctx.receiver());
        self.send(task, ctx).await;
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
}

// ============== Component Implementation ==============

#[async_trait]
impl Orchestrator for CompactOrchestrator {
    type Output = CompactionResponse;
    type Error = CompactionError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    fn initial_tasks(&self, ctx: &ComponentContext<Self>) -> Vec<TaskMessage> {
        vec![wrap(
            Box::new(GetCollectionAndSegmentsOperator {
                sysdb: self.sysdb.clone(),
                collection_id: self.compaction_job.collection_id,
            }),
            (),
            ctx.receiver(),
        )]
    }

    fn set_result_channel(&mut self, sender: Sender<Result<CompactionResponse, CompactionError>>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Sender<Result<CompactionResponse, CompactionError>> {
        self.result_channel
            .take()
            .expect("The result channel should be set before take")
    }
}

// ============== Handlers ==============
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        let collection = output.collection.clone();
        let dimension = match collection.dimension {
            Some(dim) => dim as usize,
            None => {
                self.terminate_with_result(
                    Err(CompactionError::InvariantViolation(
                        "Collection version should have been populated before compaction",
                    )),
                    ctx,
                );
                return;
            }
        };

        self.collection_logical_size_bytes = match self.compaction_job.rebuild {
            true => 0,
            false => collection.size_bytes_post_compaction as i64,
        };

        let mut metadata_segment = output.metadata_segment.clone();
        let mut vector_segment = output.vector_segment.clone();
        if self.compaction_job.rebuild {
            // Reset the metadata and vector segments by purging the file paths
            metadata_segment.file_path = Default::default();
            vector_segment.file_path = Default::default();
        }

        let record_reader = match self.ok_or_terminate(
            match RecordSegmentReader::from_segment(
                &output.record_segment,
                &self.blockfile_provider,
            )
            .await
            {
                Ok(reader) => Ok(Some(reader)),
                Err(err) => match *err {
                    RecordSegmentReaderCreationError::UninitializedSegment => Ok(None),
                    _ => Err(*err),
                },
            },
            ctx,
        ) {
            Some(reader) => reader,
            None => return,
        }
        .filter(|_| !self.compaction_job.rebuild);
        let record_writer = match self.ok_or_terminate(
            RecordSegmentWriter::from_segment(&output.record_segment, &self.blockfile_provider)
                .await,
            ctx,
        ) {
            Some(writer) => writer,
            None => return,
        };
        let metadata_writer = match self.ok_or_terminate(
            MetadataSegmentWriter::from_segment(&metadata_segment, &self.blockfile_provider).await,
            ctx,
        ) {
            Some(writer) => writer,
            None => return,
        };
        let vector_writer = match vector_segment.r#type {
            SegmentType::Spann => match self.ok_or_terminate(
                self.spann_provider
                    .write(&collection, &vector_segment, dimension)
                    .await,
                ctx,
            ) {
                Some(writer) => VectorSegmentWriter::Spann(writer),
                None => return,
            },
            _ => match self.ok_or_terminate(
                DistributedHNSWSegmentWriter::from_segment(
                    &collection,
                    &vector_segment,
                    dimension,
                    self.hnsw_provider.clone(),
                )
                .await
                .map_err(|err| *err),
                ctx,
            ) {
                Some(writer) => VectorSegmentWriter::Hnsw(writer),
                None => return,
            },
        };

        let writers = CompactWriters {
            record_reader,
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
            );
            return;
        }

        // Prefetch metadata and record segment
        let prefetch_tasks = [output.metadata_segment, output.record_segment].map(|segment| {
            wrap(
                Box::new(PrefetchSegmentOperator::new()),
                PrefetchSegmentInput::new(segment, self.blockfile_provider.clone()),
                ctx.receiver(),
            )
        });
        for task in prefetch_tasks {
            self.send(task, ctx).await;
        }

        let fetch_log_task = wrap(
            Box::new(FetchLogOperator {
                log_client: self.log.clone(),
                batch_size: self.fetch_log_batch_size,
                // Here we do not need to be inclusive since the compaction job
                // offset is the one after the last compaction offset
                start_log_offset_id: self.compaction_job.offset as u32,
                maximum_fetch_count: Some(self.max_compaction_size as u32),
                collection_uuid: self.compaction_job.collection_id,
            }),
            (),
            ctx.receiver(),
        );
        self.send(fetch_log_task, ctx).await;
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
        self.ok_or_terminate(message.into_inner(), ctx);
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
        let records = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(recs) => recs,
            None => {
                tracing::info!("cancelled fetch log task");
                return;
            }
        };
        tracing::info!("Pulled Records: {:?}", records.len());
        let final_record_pulled = if !records.is_empty() {
            records.get(records.len() - 1)
        } else {
            None
        };
        match final_record_pulled {
            Some(record) => {
                self.pulled_log_offset = Some(record.log_offset);
                tracing::info!("Pulled Logs Up To Offset: {:?}", self.pulled_log_offset);
                self.partition(records, ctx).await;
            }
            None => {
                self.terminate_with_result(
                    Err(CompactionError::InvariantViolation(
                        "No records pulled by compaction, this is a system invariant violation",
                    )),
                    ctx,
                );
            }
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
        let records = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(recs) => recs.records,
            None => todo!(),
        };
        self.materialize_log(records, ctx).await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(res) => res,
            None => return,
        };

        if output.result.is_empty() {
            // We check the number of remaining materialization tasks to prevent a race condition
            if self.num_uncompleted_materialization_tasks == 1
                && self.num_uncompleted_tasks_by_segment.is_empty()
            {
                // There is nothing to flush, proceed to register
                self.register(self.pulled_log_offset.unwrap(), ctx).await;
            }
        } else {
            self.collection_logical_size_bytes += output.collection_logical_size_delta;
            self.dispatch_apply_log_to_segment_writer_tasks(output.result, ctx.receiver(), ctx)
                .await;
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
        let message = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(message) => message,
            None => return,
        };

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
            match self.ok_or_terminate(num_tasks_left, ctx) {
                Some(num_tasks_left) => num_tasks_left,
                None => return,
            }
        };

        if num_tasks_left == 0 {
            let segment_writer = self.get_segment_writer_by_id(message.segment_id).await;
            let segment_writer = match self.ok_or_terminate(segment_writer, ctx) {
                Some(writer) => writer,
                None => return,
            };

            self.dispatch_segment_writer_commit(segment_writer, ctx.receiver(), ctx)
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
        let message = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(message) => message,
            None => return,
        };

        let flusher = message.flusher;
        // If the flusher recieved is a record segment flusher, get the number of keys for the blockfile and set it on the orchestrator
        if let ChromaSegmentFlusher::RecordSegment(ref record_segment_flusher) = flusher {
            self.total_records_last_compaction = record_segment_flusher.count();
        }

        self.dispatch_segment_flush(flusher, ctx.receiver(), ctx)
            .await;
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
        let message = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(message) => message,
            None => return,
        };

        let segment_id = message.flush_info.segment_id;

        // Drops the span so that the end timestamp is accurate
        let _ = self.segment_spans.remove(&segment_id);

        self.flush_results.push(message.flush_info);
        self.num_uncompleted_tasks_by_segment.remove(&segment_id);

        if self.num_uncompleted_tasks_by_segment.is_empty() {
            // Unwrap should be safe here as we are guaranteed to have a value by construction
            self.register(self.pulled_log_offset.expect("Invariant violation: pulled_log_offset should have been populated at this point."), ctx).await;
        }
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
        self.terminate_with_result(
            message
                .into_inner()
                .map_err(|e| e.into())
                .map(|_| CompactionResponse {
                    compaction_job: self.compaction_job.clone(),
                }),
            ctx,
        );
    }
}
