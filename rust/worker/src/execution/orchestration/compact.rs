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
    PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{
    Chunk, Collection, CollectionUuid, LogRecord, SegmentFlushInfo, SegmentType, SegmentUuid,
};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::execution::operators::{
    apply_log_to_segment_writer::{
        ApplyLogToSegmentWriterInput, ApplyLogToSegmentWriterOperator,
        ApplyLogToSegmentWriterOperatorError, ApplyLogToSegmentWriterOutput,
    },
    commit_segment_writer::{
        CommitSegmentWriterInput, CommitSegmentWriterOperator, CommitSegmentWriterOperatorError,
        CommitSegmentWriterOutput,
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
        PrefetchSegmentError, PrefetchSegmentInput, PrefetchSegmentOperator, PrefetchSegmentOutput,
    },
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
    collection_id: CollectionUuid,
    hnsw_index_uuid: Option<IndexUuid>,
    rebuild: bool,
    fetch_log_batch_size: u32,
    max_compaction_size: usize,
    max_partition_size: usize,

    // Dependencies
    dispatcher: ComponentHandle<Dispatcher>,
    log: Log,
    sysdb: SysDb,
    blockfile_provider: BlockfileProvider,
    hnsw_provider: HnswIndexProvider,
    spann_provider: SpannProvider,

    collection: OnceCell<Collection>,
    writers: OnceCell<CompactWriters>,
    flush_results: Vec<SegmentFlushInfo>,
    result_channel: Option<Sender<Result<CompactionResponse, CompactionError>>>,
    num_uncompleted_materialization_tasks: usize,
    num_uncompleted_tasks_by_segment: HashMap<SegmentUuid, usize>,
    collection_logical_size_delta_bytes: i64,
    // How much to pull from fetch_logs
    pulled_log_offset: i64,
    state: ExecutionState,

    // Total number of records in the collection after the compaction
    total_records_post_compaction: u64,

    // We track a parent span for each segment type so we can group all the spans for a given segment type (makes the resulting trace much easier to read)
    segment_spans: HashMap<SegmentUuid, Span>,
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
    #[error("Error sourcing record segment: {0}")]
    SourceRecordSegment(#[from] SourceRecordSegmentError),
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
    pub(crate) collection_id: CollectionUuid,
}

impl CompactOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        collection_id: CollectionUuid,
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
        CompactOrchestrator {
            collection_id,
            hnsw_index_uuid: None,
            rebuild,
            fetch_log_batch_size,
            max_compaction_size,
            max_partition_size,
            dispatcher,
            log,
            sysdb,
            blockfile_provider,
            hnsw_provider,
            spann_provider,
            collection: OnceCell::new(),
            writers: OnceCell::new(),
            flush_results: Vec::new(),
            result_channel,
            num_uncompleted_materialization_tasks: 0,
            num_uncompleted_tasks_by_segment: HashMap::new(),
            collection_logical_size_delta_bytes: 0,
            pulled_log_offset: 0,
            state: ExecutionState::Pending,
            total_records_post_compaction: 0,
            segment_spans: HashMap::new(),
        }
    }

    async fn try_purge_hnsw(path: &Path, hnsw_index_uuid: Option<IndexUuid>) {
        if let Some(hnsw_index_uuid) = hnsw_index_uuid {
            let _ = HnswIndexProvider::purge_one_id(path, hnsw_index_uuid).await;
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

        self.num_uncompleted_materialization_tasks = partitions.len();
        for partition in partitions.iter() {
            let operator = MaterializeLogOperator::new();
            let input = MaterializeLogInput::new(
                partition.clone(),
                record_reader.clone(),
                next_max_offset_id.clone(),
            );
            let task = wrap(operator, input, ctx.receiver());
            self.send(task, ctx, Some(Span::current())).await;
        }
    }

    async fn dispatch_apply_log_to_segment_writer_tasks(
        &mut self,
        materialized_logs: MaterializeLogsResult,
        ctx: &ComponentContext<CompactOrchestrator>,
    ) {
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
            );
            let task = wrap(operator, input, ctx.receiver());
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
            );
            let task = wrap(operator, input, ctx.receiver());
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
            let input =
                ApplyLogToSegmentWriterInput::new(writer, materialized_logs, writers.record_reader);
            let task = wrap(operator, input, ctx.receiver());
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
        let task = wrap(operator, input, ctx.receiver());
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
        let task = wrap(operator, input, ctx.receiver());
        let res = self.dispatcher().send(task, Some(span)).await;
        self.ok_or_terminate(res, ctx).await;
    }

    async fn register(&mut self, ctx: &ComponentContext<CompactOrchestrator>) {
        self.state = ExecutionState::Register;
        let collection_cell =
            self.collection
                .get()
                .cloned()
                .ok_or(CompactionError::InvariantViolation(
                    "Collection information should have been obtained",
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
        );

        let task = wrap(operator, input, ctx.receiver());
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
}

// ============== Component Implementation ==============

#[async_trait]
impl Orchestrator for CompactOrchestrator {
    type Output = CompactionResponse;
    type Error = CompactionError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        vec![(
            wrap(
                Box::new(GetCollectionAndSegmentsOperator {
                    sysdb: self.sysdb.clone(),
                    collection_id: self.collection_id,
                }),
                (),
                ctx.receiver(),
            ),
            Some(Span::current()),
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

        let collection = output.collection.clone();
        if self.collection.set(collection.clone()).is_err() {
            self.terminate_with_result(
                Err(CompactionError::InvariantViolation(
                    "Collection information should not have been initialized",
                )),
                ctx,
            )
            .await;
            return;
        };

        self.pulled_log_offset = collection.log_position;

        let record_reader = match self
            .ok_or_terminate(
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
            )
            .await
        {
            Some(reader) => reader,
            None => return,
        };

        let log_task = match self.rebuild {
            true => wrap(
                Box::new(SourceRecordSegmentOperator {}),
                SourceRecordSegmentInput {
                    record_segment_reader: record_reader.clone(),
                },
                ctx.receiver(),
            ),
            false => wrap(
                Box::new(FetchLogOperator {
                    log_client: self.log.clone(),
                    batch_size: self.fetch_log_batch_size,
                    // We need to start fetching from the first log that has not been compacted
                    start_log_offset_id: u64::try_from(collection.log_position + 1)
                        .unwrap_or_default(),
                    maximum_fetch_count: Some(self.max_compaction_size as u32),
                    collection_uuid: self.collection_id,
                    tenant: collection.tenant.clone(),
                }),
                (),
                ctx.receiver(),
            ),
        };

        let dimension = match collection.dimension {
            Some(dim) => dim as usize,
            None => {
                // Collection is not yet initialized, there is no need to initialize the writers
                // Future handlers should return early on empty materialized logs without using writers
                self.send(log_task, ctx, Some(Span::current())).await;
                return;
            }
        };

        let mut metadata_segment = output.metadata_segment.clone();
        let mut record_segment = output.record_segment.clone();
        let mut vector_segment = output.vector_segment.clone();
        if self.rebuild {
            // Reset the metadata and vector segments by purging the file paths
            metadata_segment.file_path = Default::default();
            record_segment.file_path = Default::default();
            vector_segment.file_path = Default::default();
        }

        let record_writer = match self
            .ok_or_terminate(
                RecordSegmentWriter::from_segment(&record_segment, &self.blockfile_provider).await,
                ctx,
            )
            .await
        {
            Some(writer) => writer,
            None => return,
        };
        let metadata_writer = match self
            .ok_or_terminate(
                MetadataSegmentWriter::from_segment(&metadata_segment, &self.blockfile_provider)
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
                        .write(&collection, &vector_segment, dimension)
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
                        &collection,
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

        let writers = CompactWriters {
            record_reader: record_reader.clone().filter(|_| !self.rebuild),
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

        // Prefetch segments
        let prefetch_segments = match self.rebuild {
            true => vec![output.record_segment],
            false => {
                let mut segments = vec![output.metadata_segment, output.record_segment];
                if is_vector_segment_spann {
                    segments.push(output.vector_segment);
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
            );
            // Prefetch task is detached from the orchestrator
            let prefetch_span =
                tracing::info_span!(parent: None, "Prefetch segment", segment_id = %segment_id);
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
                // TODO(hammadb): We can repair the log service's understanding of the offset here, as this only happens
                // when the log service is not up to date on the latest compacted offset, which leads it to schedule an already
                // compacted collection.
                self.terminate_with_result(
                    Ok(CompactionResponse {
                        collection_id: self.collection_id,
                    }),
                    ctx,
                )
                .await;
                return;
            }
        }
        self.partition(output, ctx).await;
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
        if output.is_empty() {
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
            self.dispatch_apply_log_to_segment_writer_tasks(output.result, ctx)
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
        let message = match self.ok_or_terminate(message.into_inner(), ctx).await {
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
                    collection_id: self.collection_id,
                }),
            ctx,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use chroma_config::{registry::Registry, Configurable};
    use chroma_log::{
        in_memory_log::{InMemoryLog, InternalLogRecord},
        test::{add_delete_generator, LogGenerator},
        Log,
    };
    use chroma_segment::test::TestDistributedSegment;
    use chroma_sysdb::{SysDb, TestSysDb};
    use chroma_system::{Dispatcher, Orchestrator, System};
    use chroma_types::{
        DocumentExpression, DocumentOperator, MetadataExpression, PrimitiveOperator, Where,
    };

    use crate::{
        config::RootConfig,
        execution::{
            operators::{
                fetch_log::FetchLogOperator, filter::FilterOperator, limit::LimitOperator,
                projection::ProjectionOperator,
            },
            orchestration::get::GetOrchestrator,
        },
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
        let test_segments = TestDistributedSegment::default();
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
        let filter = FilterOperator {
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
        let limit = LimitOperator {
            skip: 0,
            fetch: None,
        };
        let project = ProjectionOperator {
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
            .expect("Get orchestrator should not fail")
            .0;

        assert!(!old_vals.records.is_empty());

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
        expected_new_collection.version_file_path = Some(
            old_cas
                .collection
                .version_file_path
                .clone()
                .unwrap()
                .replace("/1", "/2"),
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
            .expect("Get orchestrator should not fail")
            .0;

        assert_eq!(new_vals, old_vals);
    }
}
