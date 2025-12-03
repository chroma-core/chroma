use std::sync::{atomic::AtomicU32, Arc};

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
    types::VectorSegmentWriter,
};
use chroma_sysdb::SysDb;
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{Chunk, CollectionUuid, JobId, LogRecord, SegmentType};
use opentelemetry::trace::TraceContextExt;
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::execution::{
    operators::{
        fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
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
        source_record_segment::{
            SourceRecordSegmentError, SourceRecordSegmentInput, SourceRecordSegmentOperator,
            SourceRecordSegmentOutput,
        },
    },
    orchestration::compact::CompactionContextError,
};

use super::compact::{CollectionCompactInfo, CompactWriters, CompactionContext, ExecutionState};

#[derive(Error, Debug)]
pub enum LogFetchOrchestratorError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error reading from CompactionContext: {0}")]
    CompactionContext(#[from] CompactionContextError),
    #[error("Error fetching logs: {0}")]
    FetchLog(#[from] FetchLogError),
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
    #[error("Error receiving final result: {0}")]
    RecvError(#[from] RecvError),
    #[error("Error creating spann writer: {0}")]
    SpannSegment(#[from] SpannSegmentWriterError),
    #[error("Error sourcing record segment: {0}")]
    SourceRecordSegment(#[from] SourceRecordSegmentError),
    #[error("Could not count current segment: {0}")]
    CountError(Box<dyn chroma_error::ChromaError>),
}

impl ChromaError for LogFetchOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            LogFetchOrchestratorError::Aborted => ErrorCodes::Aborted,
            _ => ErrorCodes::Internal,
        }
    }

    fn should_trace_error(&self) -> bool {
        if let LogFetchOrchestratorError::FetchLog(FetchLogError::PullLog(e)) = self {
            e.code() != ErrorCodes::NotFound
        } else {
            match self {
                Self::Aborted => true,
                Self::Channel(e) => e.should_trace_error(),
                Self::CompactionContext(e) => e.should_trace_error(),
                Self::FetchLog(e) => e.should_trace_error(),
                Self::GetCollectionAndSegments(e) => e.should_trace_error(),
                Self::HnswSegment(e) => e.should_trace_error(),
                Self::InvariantViolation(_) => true,
                Self::MaterializeLogs(e) => e.should_trace_error(),
                Self::MetadataSegment(e) => e.should_trace_error(),
                Self::Panic(e) => e.should_trace_error(),
                Self::Partition(e) => e.should_trace_error(),
                Self::PrefetchSegment(e) => e.should_trace_error(),
                Self::RecordSegmentReader(e) => e.should_trace_error(),
                Self::RecordSegmentWriter(e) => e.should_trace_error(),
                Self::RecvError(_) => true,
                Self::SpannSegment(e) => e.should_trace_error(),
                Self::SourceRecordSegment(e) => e.should_trace_error(),
                Self::CountError(e) => e.should_trace_error(),
            }
        }
    }
}

impl<E> From<TaskError<E>> for LogFetchOrchestratorError
where
    E: Into<LogFetchOrchestratorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Aborted => LogFetchOrchestratorError::Aborted,
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Success {
    pub materialized: Vec<MaterializeLogOutput>,
    pub collection_info: CollectionCompactInfo,
}

#[derive(Debug)]
pub(crate) struct RequireCompactionOffsetRepair {
    pub job_id: JobId,
    pub witnessed_offset_in_sysdb: i64,
}

#[derive(Debug)]
pub(crate) struct RequireFunctionBackfill {
    pub materialized: Vec<MaterializeLogOutput>,
    pub collection_info: CollectionCompactInfo,
}

impl RequireFunctionBackfill {
    pub fn new(
        materialized: Vec<MaterializeLogOutput>,
        collection_info: CollectionCompactInfo,
    ) -> Self {
        Self {
            materialized,
            collection_info,
        }
    }
}

#[derive(Debug)]
pub(crate) enum LogFetchOrchestratorResponse {
    Success(Success),
    RequireCompactionOffsetRepair(RequireCompactionOffsetRepair),
    #[allow(dead_code)]
    RequireFunctionBackfill(RequireFunctionBackfill),
}

impl Success {
    pub fn new(
        materialized: Vec<MaterializeLogOutput>,
        collection_info: CollectionCompactInfo,
    ) -> Self {
        Self {
            materialized,
            collection_info,
        }
    }
}

impl RequireCompactionOffsetRepair {
    pub fn new(job_id: JobId, witnessed_offset_in_sysdb: i64) -> Self {
        Self {
            job_id,
            witnessed_offset_in_sysdb,
        }
    }
}

impl From<Success> for LogFetchOrchestratorResponse {
    fn from(value: Success) -> Self {
        LogFetchOrchestratorResponse::Success(value)
    }
}

impl From<RequireCompactionOffsetRepair> for LogFetchOrchestratorResponse {
    fn from(value: RequireCompactionOffsetRepair) -> Self {
        LogFetchOrchestratorResponse::RequireCompactionOffsetRepair(value)
    }
}

impl From<RequireFunctionBackfill> for LogFetchOrchestratorResponse {
    fn from(value: RequireFunctionBackfill) -> Self {
        LogFetchOrchestratorResponse::RequireFunctionBackfill(value)
    }
}

#[derive(Debug)]
pub(crate) struct LogFetchOrchestrator {
    collection_id: CollectionUuid,
    context: CompactionContext,
    dispatcher: ComponentHandle<Dispatcher>,
    result_channel: Option<Sender<Result<LogFetchOrchestratorResponse, LogFetchOrchestratorError>>>,
    state: ExecutionState,
    num_uncompleted_materialization_tasks: usize,
    materialized_outputs: Vec<MaterializeLogOutput>,
    has_backfill: bool,
}

#[async_trait]
impl Orchestrator for LogFetchOrchestrator {
    type Output = LogFetchOrchestratorResponse;
    type Error = LogFetchOrchestratorError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    fn context(&self) -> &OrchestratorContext {
        &self.context.orchestrator_context
    }

    fn set_result_channel(&mut self, sender: Sender<Result<Self::Output, Self::Error>>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Option<Sender<Result<Self::Output, Self::Error>>> {
        self.result_channel.take()
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        vec![(
            wrap(
                Box::new(GetCollectionAndSegmentsOperator {
                    sysdb: self.context.sysdb.clone(),
                    collection_id: self.collection_id,
                }),
                (),
                ctx.receiver(),
                self.context
                    .orchestrator_context
                    .task_cancellation_token
                    .clone(),
            ),
            Some(Span::current()),
        )]
    }
}

impl LogFetchOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        collection_id: CollectionUuid,
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
    ) -> Self {
        let context = CompactionContext::new(
            is_rebuild,
            fetch_log_batch_size,
            max_compaction_size,
            max_partition_size,
            log,
            sysdb,
            blockfile_provider,
            hnsw_provider,
            spann_provider,
            dispatcher.clone(),
            false, // LogFetchOrchestrator doesn't need is_function_disabled
        );
        LogFetchOrchestrator {
            collection_id,
            context,
            dispatcher,
            result_channel: None,
            state: ExecutionState::Pending,
            num_uncompleted_materialization_tasks: 0,
            materialized_outputs: Vec::new(),
            has_backfill: false,
        }
    }

    async fn partition(&mut self, records: Chunk<LogRecord>, ctx: &ComponentContext<Self>) {
        self.state = ExecutionState::Partition;
        let operator = PartitionOperator::new();
        tracing::info!("Sending N Records: {:?}", records.len());
        let input = PartitionInput::new(records, self.context.max_partition_size);
        let task = wrap(
            operator,
            input,
            ctx.receiver(),
            self.context
                .orchestrator_context
                .task_cancellation_token
                .clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
    }

    async fn materialize_log(
        &mut self,
        partitions: Vec<Chunk<LogRecord>>,
        ctx: &ComponentContext<Self>,
    ) {
        self.state = ExecutionState::MaterializeApplyCommitFlush;

        // NOTE: We allow writers to be uninitialized for the case when the materialized logs are empty
        let record_reader = self
            .context
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
            let count = match rr.count().await {
                Ok(count) => count as u64,
                Err(err) => {
                    return self
                        .terminate_with_result(Err(LogFetchOrchestratorError::CountError(err)), ctx)
                        .await;
                }
            };

            let collection_info = match self.context.get_collection_info_mut() {
                Ok(info) => info,
                Err(err) => {
                    return self.terminate_with_result(Err(err.into()), ctx).await;
                }
            };
            collection_info.collection.total_records_post_compaction = count;
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
                self.context
                    .orchestrator_context
                    .task_cancellation_token
                    .clone(),
            );
            self.send(task, ctx, Some(Span::current())).await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<GetCollectionAndSegmentsOutput, GetCollectionAndSegmentsError>>
    for LogFetchOrchestrator
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

        let record_reader = match self
            .ok_or_terminate(
                match Box::pin(RecordSegmentReader::from_segment(
                    &output.record_segment,
                    &self.context.blockfile_provider,
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

        let log_task = if self.context.is_rebuild {
            wrap(
                Box::new(SourceRecordSegmentOperator {}),
                SourceRecordSegmentInput {
                    record_segment_reader: record_reader.clone(),
                },
                ctx.receiver(),
                self.context
                    .orchestrator_context
                    .task_cancellation_token
                    .clone(),
            )
        } else {
            wrap(
                Box::new(FetchLogOperator {
                    log_client: self.context.log.clone(),
                    batch_size: self.context.fetch_log_batch_size,
                    // We need to start fetching from the first log that has not been compacted
                    start_log_offset_id: u64::try_from(collection.log_position + 1)
                        .unwrap_or_default(),
                    maximum_fetch_count: Some(self.context.max_compaction_size as u32),
                    collection_uuid: collection.collection_id,
                    tenant: collection.tenant.clone(),
                }),
                (),
                ctx.receiver(),
                self.context
                    .orchestrator_context
                    .task_cancellation_token
                    .clone(),
            )
        };

        let collection_info = CollectionCompactInfo {
            collection_id: collection.collection_id,
            collection: collection.clone(),
            writers: None,
            pulled_log_offset: collection.log_position,
            hnsw_index_uuid: None,
            schema: collection.schema.clone(),
        };

        let result = self.context.collection_info.set(collection_info);
        if result.is_err() {
            self.terminate_with_result(
                Err(LogFetchOrchestratorError::InvariantViolation(
                    "Collection info should not have been set yet",
                )),
                ctx,
            )
            .await;
            return;
        }

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
        if self.context.is_rebuild {
            // Reset the metadata and vector segments by purging the file paths
            metadata_segment.file_path = Default::default();
            record_segment.file_path = Default::default();
            vector_segment.file_path = Default::default();
        }

        let record_writer = match self
            .ok_or_terminate(
                RecordSegmentWriter::from_segment(
                    &collection.tenant,
                    &collection.database_id,
                    &record_segment,
                    &self.context.blockfile_provider,
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
                    &collection.tenant,
                    &collection.database_id,
                    &metadata_segment,
                    &self.context.blockfile_provider,
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
                    self.context
                        .spann_provider
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
                        self.context.hnsw_provider.clone(),
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
            record_reader: record_reader.clone().filter(|_| !self.context.is_rebuild),
            metadata_writer,
            record_writer,
            vector_writer,
        };

        let collection_info = match self.context.get_collection_info_mut() {
            Ok(info) => info,
            Err(err) => {
                self.terminate_with_result(Err(err.into()), ctx).await;
                return;
            }
        };

        collection_info.writers = Some(writers.clone());
        collection_info.hnsw_index_uuid = Some(hnsw_index_uuid);

        // Prefetch segments
        let prefetch_segments = match self.context.is_rebuild {
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
                PrefetchSegmentInput::new(segment, self.context.blockfile_provider.clone()),
                ctx.receiver(),
                self.context
                    .orchestrator_context
                    .task_cancellation_token
                    .clone(),
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
impl Handler<TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>> for LogFetchOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>,
        ctx: &ComponentContext<Self>,
    ) {
        self.ok_or_terminate(message.into_inner(), ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for LogFetchOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchLogOutput, FetchLogError>,
        ctx: &ComponentContext<Self>,
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
                let collection_info = match self.context.get_collection_info_mut() {
                    Ok(info) => info,
                    Err(err) => {
                        self.terminate_with_result(Err(err.into()), ctx).await;
                        return;
                    }
                };
                collection_info.pulled_log_offset = rec.log_offset;
                tracing::info!(
                    "Pulled Logs Up To Offset: {:?}",
                    collection_info.pulled_log_offset
                );
            }
            None => {
                tracing::warn!("No logs were pulled from the log service, this can happen when the log compaction offset is behing the sysdb.");
                let collection_info = match self.context.get_collection_info() {
                    Ok(info) => info,
                    Err(err) => {
                        self.terminate_with_result(Err(err.into()), ctx).await;
                        return;
                    }
                };
                self.terminate_with_result(
                    Ok(RequireCompactionOffsetRepair::new(
                        collection_info.collection_id.into(),
                        collection_info.pulled_log_offset,
                    )
                    .into()),
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
    for LogFetchOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<SourceRecordSegmentOutput, SourceRecordSegmentError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        tracing::info!("Sourced Records: {}", output.len());
        // Each record should corresond to a log
        let collection_info = match self.context.get_collection_info_mut() {
            Ok(info) => info,
            Err(err) => {
                self.terminate_with_result(Err(err.into()), ctx).await;
                return;
            }
        };
        collection_info.collection.total_records_post_compaction = output.len() as u64;

        let collection_info = match self.context.get_collection_info() {
            Ok(info) => info,
            Err(err) => {
                self.terminate_with_result(Err(err.into()), ctx).await;
                return;
            }
        };
        if output.is_empty() {
            self.terminate_with_result(
                Ok(Success::new(vec![], collection_info.clone()).into()),
                ctx,
            )
            .await;
            return;
        } else {
            self.partition(output, ctx).await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<PartitionOutput, PartitionError>> for LogFetchOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<PartitionOutput, PartitionError>,
        ctx: &ComponentContext<Self>,
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
    for LogFetchOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<MaterializeLogOutput, MaterializeLogOperatorError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(res) => res,
            None => return,
        };

        if output.result.has_backfill() {
            self.has_backfill = true;
        }

        if !output.result.is_empty() {
            self.materialized_outputs.push(output);
        }
        self.num_uncompleted_materialization_tasks -= 1;
        if self.num_uncompleted_materialization_tasks == 0 {
            let collection_info = match self.context.collection_info.take() {
                Some(info) => info,
                None => {
                    self.terminate_with_result(
                        Err(LogFetchOrchestratorError::InvariantViolation(
                            "self.collection_info not set",
                        )),
                        ctx,
                    )
                    .await;
                    return;
                }
            };
            let materialized = std::mem::take(&mut self.materialized_outputs);
            if self.has_backfill {
                self.terminate_with_result(
                    Ok(RequireFunctionBackfill::new(materialized, collection_info).into()),
                    ctx,
                )
                .await;
                return;
            }
            self.terminate_with_result(Ok(Success::new(materialized, collection_info).into()), ctx)
                .await;
        }
    }
}
