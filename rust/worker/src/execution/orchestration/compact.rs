use crate::compactor::CompactionJob;
use crate::execution::operators::apply_log_to_segment_writer::ApplyLogToSegmentWriterInput;
use crate::execution::operators::apply_log_to_segment_writer::ApplyLogToSegmentWriterOperator;
use crate::execution::operators::apply_log_to_segment_writer::ApplyLogToSegmentWriterOperatorError;
use crate::execution::operators::apply_log_to_segment_writer::ApplyLogToSegmentWriterOutput;
use crate::execution::operators::commit_segment_writer::CommitSegmentWriterInput;
use crate::execution::operators::commit_segment_writer::CommitSegmentWriterOperator;
use crate::execution::operators::commit_segment_writer::CommitSegmentWriterOperatorError;
use crate::execution::operators::commit_segment_writer::CommitSegmentWriterOutput;
use crate::execution::operators::fetch_log::FetchLogError;
use crate::execution::operators::fetch_log::FetchLogOperator;
use crate::execution::operators::fetch_log::FetchLogOutput;
use crate::execution::operators::flush_segment_writer::FlushSegmentWriterInput;
use crate::execution::operators::flush_segment_writer::FlushSegmentWriterOperator;
use crate::execution::operators::flush_segment_writer::FlushSegmentWriterOperatorError;
use crate::execution::operators::flush_segment_writer::FlushSegmentWriterOutput;
use crate::execution::operators::materialize_logs::MaterializeLogInput;
use crate::execution::operators::materialize_logs::MaterializeLogOperator;
use crate::execution::operators::materialize_logs::MaterializeLogOperatorError;
use crate::execution::operators::partition::PartitionError;
use crate::execution::operators::partition::PartitionInput;
use crate::execution::operators::partition::PartitionOperator;
use crate::execution::operators::partition::PartitionOutput;
use crate::execution::operators::register::RegisterError;
use crate::execution::operators::register::RegisterInput;
use crate::execution::operators::register::RegisterOperator;
use crate::execution::operators::register::RegisterOutput;
use crate::log::log::Log;
use crate::segment::distributed_hnsw_segment::DistributedHNSWSegmentWriter;
use crate::segment::metadata_segment::MetadataSegmentWriter;
use crate::segment::record_segment::RecordSegmentReader;
use crate::segment::record_segment::RecordSegmentReaderCreationError;
use crate::segment::record_segment::RecordSegmentWriter;
use crate::segment::ChromaSegmentFlusher;
use crate::segment::ChromaSegmentWriter;
use crate::segment::MaterializeLogsResult;
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_sysdb::GetCollectionsError;
use chroma_sysdb::GetSegmentsError;
use chroma_sysdb::SysDb;
use chroma_system::wrap;
use chroma_system::ChannelError;
use chroma_system::ComponentContext;
use chroma_system::ComponentHandle;
use chroma_system::Dispatcher;
use chroma_system::Handler;
use chroma_system::Orchestrator;
use chroma_system::PanicError;
use chroma_system::ReceiverForMessage;
use chroma_system::TaskError;
use chroma_system::TaskMessage;
use chroma_system::TaskResult;
use chroma_types::Chunk;
use chroma_types::SegmentUuid;
use chroma_types::{CollectionUuid, LogRecord, Segment, SegmentFlushInfo, SegmentType};
use core::panic;
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::Sender;
use tokio::sync::OnceCell;
use tracing::Span;
use uuid::Uuid;

/**  The state of the orchestrator.
In chroma, we have a relatively fixed number of query plans that we can execute. Rather
than a flexible state machine abstraction, we just manually define the states that we
expect to encounter for a given query plan. This is a bit more rigid, but it's also simpler and easier to
understand. We can always add more abstraction later if we need it.

```plaintext
                                   ┌────────────────────────────┐
                                   ├─► Apply logs to segment #1 │
                                   │                            ├──► Commit segment #1 ──► Flush segment #1
                                   ├─► Apply logs to segment #1 │
Pending ──► PullLogs ──► Partition │                            │                                            ──► Register ─► Finished
                                   ├─► Apply logs to segment #2 │
                                   │                            ├──► Commit segment #2 ──► Flush segment #2
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
    pub(crate) metadata: MetadataSegmentWriter<'static>,
    pub(crate) record: RecordSegmentWriter,
    pub(crate) vector: Box<DistributedHNSWSegmentWriter>,
}

#[derive(Debug)]
pub struct CompactOrchestrator {
    id: Uuid,
    compaction_job: CompactionJob,
    state: ExecutionState,
    // Component Execution
    collection_id: CollectionUuid,
    // Dependencies
    log: Box<Log>,
    sysdb: Box<SysDb>,
    blockfile_provider: BlockfileProvider,
    hnsw_index_provider: HnswIndexProvider,
    // State we hold across the execution
    pulled_log_offset: Option<i64>,
    // Dispatcher
    dispatcher: ComponentHandle<Dispatcher>,
    // Tracks the total remaining number of MaterializeLogs tasks
    num_uncompleted_materialization_tasks: usize,
    // Tracks the total remaining number of tasks per segment
    num_uncompleted_tasks_by_segment: HashMap<SegmentUuid, usize>,
    // Result Channel
    result_channel: Option<Sender<Result<CompactionResponse, CompactionError>>>,
    max_compaction_size: usize,
    max_partition_size: usize,
    // Populated during the compaction process
    cached_segments: Option<Vec<Segment>>,
    writers: OnceCell<CompactWriters>,
    flush_results: Vec<SegmentFlushInfo>,
    // We track a parent span for each segment type so we can group all the spans for a given segment type (makes the resulting trace much easier to read)
    segment_spans: HashMap<SegmentUuid, Span>,
    // Total number of records in the collection after the compaction
    total_records_last_compaction: u64,
}

#[derive(Error, Debug)]
pub enum GetSegmentWritersError {
    #[error("No segments found for collection")]
    NoSegmentsFound,
    #[error("SysDB GetSegments Error")]
    SysDbGetSegmentsError(#[from] GetSegmentsError),
    #[error("Error creating Record Segment Writer")]
    RecordSegmentWriterError,
    #[error("Error creating Metadata Segment Writer")]
    MetadataSegmentWriterError,
    #[error("Error creating HNSW Segment Writer")]
    HnswSegmentWriterError,
    #[error("Collection not found")]
    CollectionNotFound,
    #[error("Error getting collection")]
    GetCollectionError(#[from] GetCollectionsError),
    #[error("Collection is missing dimension")]
    CollectionMissingDimension,
}

impl ChromaError for GetSegmentWritersError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Error, Debug)]
pub enum CompactionError {
    #[error("Panic during compaction: {0}")]
    Panic(#[from] PanicError),
    #[error("FetchLog error: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Partition error: {0}")]
    Partition(#[from] PartitionError),
    #[error("MaterializeLogs error: {0}")]
    MaterializeLogs(#[from] MaterializeLogOperatorError),
    #[error("Apply logs to segment writer error: {0}")]
    ApplyLogToSegmentWriter(#[from] ApplyLogToSegmentWriterOperatorError),
    #[error("Commit segment writer error: {0}")]
    CommitSegmentWriter(#[from] CommitSegmentWriterOperatorError),
    #[error("Flush segment writer error: {0}")]
    FlushSegmentWriter(#[from] FlushSegmentWriterOperatorError),
    #[error("Could not create record segment reader: {0}")]
    RecordSegmentReaderCreationFailed(#[from] RecordSegmentReaderCreationError),
    #[error("GetSegmentWriters error: {0}")]
    GetSegmentWriters(#[from] GetSegmentWritersError),
    #[error("Register error: {0}")]
    Register(#[from] RegisterError),
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("{0}")]
    Generic(#[from] Box<dyn ChromaError>),
    #[error("Invariant violation: {}", .0)]
    InvariantViolation(&'static str),
}

impl<E> From<TaskError<E>> for CompactionError
where
    E: Into<CompactionError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => CompactionError::Panic(e),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

impl ChromaError for CompactionError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

// TODO: we need to improve this response
#[derive(Debug)]
pub struct CompactionResponse {
    #[allow(dead_code)]
    pub(crate) id: Uuid,
    pub(crate) compaction_job: CompactionJob,
    #[allow(dead_code)]
    pub(crate) message: String,
}

impl CompactOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        compaction_job: CompactionJob,
        collection_id: CollectionUuid,
        log: Box<Log>,
        sysdb: Box<SysDb>,
        blockfile_provider: BlockfileProvider,
        hnsw_index_provider: HnswIndexProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        result_channel: Option<Sender<Result<CompactionResponse, CompactionError>>>,
        max_compaction_size: usize,
        max_partition_size: usize,
    ) -> Self {
        CompactOrchestrator {
            id: Uuid::new_v4(),
            compaction_job,
            state: ExecutionState::Pending,
            collection_id,
            log,
            sysdb,
            blockfile_provider,
            hnsw_index_provider,
            pulled_log_offset: None,
            dispatcher,
            num_uncompleted_materialization_tasks: 0,
            num_uncompleted_tasks_by_segment: HashMap::new(),
            result_channel,
            max_compaction_size,
            max_partition_size,
            cached_segments: None,
            writers: OnceCell::new(),
            flush_results: Vec::new(),
            segment_spans: HashMap::new(),
            total_records_last_compaction: 0,
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
        println!("Sending N Records: {:?}", records.len());
        let input = PartitionInput::new(records, self.max_partition_size);
        let task = wrap(operator, input, ctx.receiver());
        self.send(task, ctx).await;
    }

    async fn materialize_log(
        &mut self,
        partitions: Vec<Chunk<LogRecord>>,
        self_address: Box<
            dyn ReceiverForMessage<TaskResult<MaterializeLogsResult, MaterializeLogOperatorError>>,
        >,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        self.state = ExecutionState::MaterializeApplyCommitFlush;

        let record_segment_result = self.get_segment(SegmentType::BlockfileRecord).await;
        let record_segment = match self.ok_or_terminate(record_segment_result, ctx) {
            Some(segment) => segment,
            None => return,
        };

        let next_max_offset_id = match self.ok_or_terminate(
            match RecordSegmentReader::from_segment(&record_segment, &self.blockfile_provider).await
            {
                Ok(reader) => {
                    let current_max_offset_id =
                        Arc::new(AtomicU32::new(reader.get_max_offset_id()));
                    current_max_offset_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(current_max_offset_id)
                }
                Err(err) => match *err {
                    RecordSegmentReaderCreationError::UninitializedSegment => {
                        Ok(Arc::new(AtomicU32::new(0)))
                    }
                    _ => Err(*err),
                },
            },
            ctx,
        ) {
            Some(offset) => offset,
            None => return,
        };

        self.num_uncompleted_materialization_tasks = partitions.len();
        for partition in partitions.iter() {
            let operator = MaterializeLogOperator::new();
            let input = MaterializeLogInput::new(
                partition.clone(),
                self.blockfile_provider.clone(),
                record_segment.clone(),
                next_max_offset_id.clone(),
            );
            let task = wrap(operator, input, self_address.clone());
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
        let writers = self.get_segment_writers().await;
        let writers = match self.ok_or_terminate(writers, ctx) {
            Some(writers) => writers,
            None => return,
        };

        let record_segment = self.get_segment(SegmentType::BlockfileRecord).await;
        let record_segment = match self.ok_or_terminate(record_segment, ctx) {
            Some(segment) => segment,
            None => return,
        };

        let record_segment_reader: Option<RecordSegmentReader<'_>> = match self.ok_or_terminate(
            match RecordSegmentReader::from_segment(&record_segment, &self.blockfile_provider).await
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
        };

        {
            self.num_uncompleted_tasks_by_segment
                .entry(writers.metadata.id)
                .and_modify(|v| {
                    *v += 1;
                })
                .or_insert(1);

            let writer = ChromaSegmentWriter::MetadataSegment(writers.metadata);
            let span = self.get_segment_writer_span(&writer);
            let operator = ApplyLogToSegmentWriterOperator::new();
            let input = ApplyLogToSegmentWriterInput::new(
                writer,
                materialized_logs.clone(),
                record_segment_reader.clone(),
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
                .entry(writers.record.id)
                .and_modify(|v| {
                    *v += 1;
                })
                .or_insert(1);

            let writer = ChromaSegmentWriter::RecordSegment(writers.record);
            let span = self.get_segment_writer_span(&writer);
            let operator = ApplyLogToSegmentWriterOperator::new();
            let input = ApplyLogToSegmentWriterInput::new(
                writer,
                materialized_logs.clone(),
                record_segment_reader.clone(),
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
                .entry(writers.vector.id)
                .and_modify(|v| {
                    *v += 1;
                })
                .or_insert(1);

            let writer = ChromaSegmentWriter::DistributedHNSWSegment(writers.vector);
            let span = self.get_segment_writer_span(&writer);
            let operator = ApplyLogToSegmentWriterOperator::new();
            let input =
                ApplyLogToSegmentWriterInput::new(writer, materialized_logs, record_segment_reader);
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
            self.sysdb.clone(),
            self.log.clone(),
        );

        let task = wrap(operator, input, ctx.receiver());
        self.send(task, ctx).await;
    }

    async fn get_all_segments(&mut self) -> Result<Vec<Segment>, GetSegmentsError> {
        if let Some(segments) = &self.cached_segments {
            return Ok(segments.clone());
        }

        let segments = self
            .sysdb
            .get_segments(None, None, None, self.collection_id)
            .await?;

        self.cached_segments = Some(segments.clone());
        Ok(segments)
    }

    async fn get_segment(
        &mut self,
        segment_type: SegmentType,
    ) -> Result<Segment, GetSegmentWritersError> {
        let segments = self.get_all_segments().await?;
        let segment = segments
            .iter()
            .find(|segment| segment.r#type == segment_type)
            .cloned();

        tracing::debug!("Found {:?} segment: {:?}", segment_type, segment);

        match segment {
            Some(segment) => Ok(segment),
            None => Err(GetSegmentWritersError::NoSegmentsFound),
        }
    }

    async fn get_segment_writers(&mut self) -> Result<CompactWriters, GetSegmentWritersError> {
        // Care should be taken to use the same writers across the compaction process
        // Since the segment writers are stateful, we should not create new writers for each partition
        // Nor should we create new writers across different tasks

        let blockfile_provider = self.blockfile_provider.clone();
        let hnsw_provider = self.hnsw_index_provider.clone();
        let mut sysdb = self.sysdb.clone();

        let record_segment = self.get_segment(SegmentType::BlockfileRecord).await?;
        let mt_segment = self.get_segment(SegmentType::BlockfileMetadata).await?;
        let hnsw_segment = self.get_segment(SegmentType::HnswDistributed).await?;

        let borrowed_writers = self
            .writers
            .get_or_try_init::<GetSegmentWritersError, _, _>(|| async {
                // Create a record segment writer
                let record_segment_writer =
                    match RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                        .await
                    {
                        Ok(writer) => writer,
                        Err(e) => {
                            tracing::error!("Error creating Record Segment Writer: {:?}", e);
                            return Err(GetSegmentWritersError::RecordSegmentWriterError);
                        }
                    };

                tracing::debug!("Record Segment Writer created");

                // Create a record segment writer
                let mt_segment_writer =
                    match MetadataSegmentWriter::from_segment(&mt_segment, &blockfile_provider)
                        .await
                    {
                        Ok(writer) => writer,
                        Err(e) => {
                            tracing::error!("Error creating metadata segment writer: {:?}", e);
                            return Err(GetSegmentWritersError::MetadataSegmentWriterError);
                        }
                    };

                tracing::debug!("Metadata Segment Writer created");

                // Create a hnsw segment writer
                let collection_res = sysdb
                    .get_collections(Some(self.collection_id), None, None, None)
                    .await;

                let collection_res = match collection_res {
                    Ok(collections) => {
                        if collections.is_empty() {
                            return Err(GetSegmentWritersError::CollectionNotFound);
                        }
                        collections
                    }
                    Err(e) => {
                        return Err(GetSegmentWritersError::GetCollectionError(e));
                    }
                };
                let collection = &collection_res[0];

                if let Some(dimension) = collection.dimension {
                    let hnsw_segment_writer = match DistributedHNSWSegmentWriter::from_segment(
                        &hnsw_segment,
                        dimension as usize,
                        hnsw_provider,
                    )
                    .await
                    {
                        Ok(writer) => writer,
                        Err(e) => {
                            tracing::error!("Error creating HNSW segment writer: {:?}", e);
                            return Err(GetSegmentWritersError::HnswSegmentWriterError);
                        }
                    };

                    return Ok(CompactWriters {
                        metadata: mt_segment_writer,
                        record: record_segment_writer,
                        vector: hnsw_segment_writer,
                    });
                }

                Err(GetSegmentWritersError::CollectionMissingDimension)
            })
            .await?;

        Ok(borrowed_writers.clone())
    }

    async fn get_segment_writer_by_id(
        &mut self,
        segment_id: SegmentUuid,
    ) -> Result<ChromaSegmentWriter<'static>, GetSegmentWritersError> {
        let writers = self.get_segment_writers().await?;

        if writers.metadata.id == segment_id {
            return Ok(ChromaSegmentWriter::MetadataSegment(writers.metadata));
        }

        if writers.record.id == segment_id {
            return Ok(ChromaSegmentWriter::RecordSegment(writers.record));
        }

        if writers.vector.id == segment_id {
            return Ok(ChromaSegmentWriter::DistributedHNSWSegment(writers.vector));
        }

        Err(GetSegmentWritersError::NoSegmentsFound)
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
            Box::new(FetchLogOperator {
                log_client: self.log.clone(),
                batch_size: 100,
                // Here we do not need to be inclusive since the compaction job
                // offset is the one after the last compaction offset
                start_log_offset_id: self.compaction_job.offset as u32,
                maximum_fetch_count: Some(self.max_compaction_size as u32),
                collection_uuid: self.collection_id,
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
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for CompactOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchLogOutput, FetchLogError>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let records = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(recs) => recs,
            None => todo!(),
        };
        tracing::info!("Pulled Records: {:?}", records.len());
        let final_record_pulled = records.get(records.len() - 1);
        match final_record_pulled {
            Some(record) => {
                self.pulled_log_offset = Some(record.log_offset);
                tracing::info!("Pulled Logs Up To Offset: {:?}", self.pulled_log_offset);
                self.partition(records, ctx).await;
            }
            None => {
                tracing::error!(
                    "No records pulled by compaction, this is a system invariant violation"
                );
                panic!("No records pulled by compaction, this is a system invariant violation");
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
        self.materialize_log(records, ctx.receiver(), ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<MaterializeLogsResult, MaterializeLogOperatorError>>
    for CompactOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<MaterializeLogsResult, MaterializeLogOperatorError>,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
        let materialized_result = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(result) => result,
            None => return,
        };

        if materialized_result.is_empty() {
            // We check the number of remaining materialization tasks to prevent a race condition
            if self.num_uncompleted_materialization_tasks == 1
                && self.num_uncompleted_tasks_by_segment.is_empty()
            {
                // There is nothing to flush, proceed to register
                self.register(self.pulled_log_offset.unwrap(), ctx).await;
            }
        } else {
            self.dispatch_apply_log_to_segment_writer_tasks(
                materialized_result,
                ctx.receiver(),
                ctx,
            )
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
                    id: self.id,
                    compaction_job: self.compaction_job.clone(),
                    message: "Compaction Complete".to_string(),
                }),
            ctx,
        );
    }
}
