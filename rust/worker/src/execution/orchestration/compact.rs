use super::super::operator::wrap;
use crate::compactor::CompactionJob;
use crate::execution::dispatcher::Dispatcher;
use crate::execution::operator::TaskResult;
use crate::execution::operators::fetch_log::FetchLogError;
use crate::execution::operators::fetch_log::FetchLogOperator;
use crate::execution::operators::fetch_log::FetchLogOutput;
use crate::execution::operators::flush_s3::FlushS3Input;
use crate::execution::operators::flush_s3::FlushS3Operator;
use crate::execution::operators::flush_s3::FlushS3Output;
use crate::execution::operators::partition::PartitionError;
use crate::execution::operators::partition::PartitionInput;
use crate::execution::operators::partition::PartitionOperator;
use crate::execution::operators::partition::PartitionOutput;
use crate::execution::operators::register::RegisterError;
use crate::execution::operators::register::RegisterInput;
use crate::execution::operators::register::RegisterOperator;
use crate::execution::operators::register::RegisterOutput;
use crate::execution::operators::write_segments::WriteSegmentsInput;
use crate::execution::operators::write_segments::WriteSegmentsOperator;
use crate::execution::operators::write_segments::WriteSegmentsOperatorError;
use crate::execution::operators::write_segments::WriteSegmentsOutput;
use crate::execution::orchestration::common::terminate_with_error;
use crate::log::log::Log;
use crate::segment::distributed_hnsw_segment::DistributedHNSWSegmentWriter;
use crate::segment::metadata_segment::MetadataSegmentWriter;
use crate::segment::record_segment::RecordSegmentReader;
use crate::segment::record_segment::RecordSegmentWriter;
use crate::sysdb::sysdb::GetCollectionsError;
use crate::sysdb::sysdb::GetSegmentsError;
use crate::sysdb::sysdb::SysDb;
use crate::system::Component;
use crate::system::ComponentHandle;
use crate::system::Handler;
use crate::system::ReceiverForMessage;
use crate::system::System;
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_types::Chunk;
use chroma_types::{CollectionUuid, LogRecord, Segment, SegmentFlushInfo, SegmentType};
use core::panic;
use std::sync::atomic;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;
use tracing::Span;
use uuid::Uuid;

/**  The state of the orchestrator.
In chroma, we have a relatively fixed number of query plans that we can execute. Rather
than a flexible state machine abstraction, we just manually define the states that we
expect to encounter for a given query plan. This is a bit more rigid, but it's also simpler and easier to
understand. We can always add more abstraction later if we need it.
```plaintext

                                   ┌───► Write─────-------┐
                                   │                      │
  Pending ─► PullLogs ─► Partition │                      ├─► Flush ─► Finished
                                   │                      │
                                   └───► Write ───────────┘

```
*/
#[derive(Debug)]
enum ExecutionState {
    Pending,
    PullLogs,
    Partition,
    Write,
    Flush,
    Register,
}

#[derive(Debug)]
pub struct CompactOrchestrator {
    id: Uuid,
    compaction_job: CompactionJob,
    state: ExecutionState,
    // Component Execution
    system: System,
    collection_id: CollectionUuid,
    // Dependencies
    log: Box<Log>,
    sysdb: Box<SysDb>,
    blockfile_provider: BlockfileProvider,
    hnsw_index_provider: HnswIndexProvider,
    // State we hold across the execution
    pulled_log_offset: Option<i64>,
    record_segment: Option<Segment>,
    // Dispatcher
    dispatcher: ComponentHandle<Dispatcher>,
    // Shared writers
    writers: Option<(
        RecordSegmentWriter,
        Box<DistributedHNSWSegmentWriter>,
        MetadataSegmentWriter<'static>,
    )>,
    // number of write segments tasks
    num_write_tasks: i32,
    // Result Channel
    result_channel:
        Option<tokio::sync::oneshot::Sender<Result<CompactionResponse, Box<dyn ChromaError>>>>,
    // Next offset id
    next_offset_id: Arc<AtomicU32>,
    max_compaction_size: usize,
    max_partition_size: usize,
}

#[derive(Error, Debug)]
enum GetSegmentWritersError {
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
    #[error("No record segment found for collection")]
    NoRecordSegmentFound,
    #[error("No metadata segment found for collection")]
    NoMetadataSegmentFound,
    #[error("Collection not found")]
    CollectionNotFound,
    #[error("Error getting collection")]
    GetCollectionError(#[from] GetCollectionsError),
    #[error("No hnsw segment found for collection")]
    NoHnswSegmentFound,
}

impl ChromaError for GetSegmentWritersError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Error, Debug)]
enum CompactionError {
    #[error("Task dispatch failed")]
    DispatchFailure,
    #[error("Result channel dropped")]
    ResultChannelDropped,
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
        system: System,
        collection_id: CollectionUuid,
        log: Box<Log>,
        sysdb: Box<SysDb>,
        blockfile_provider: BlockfileProvider,
        hnsw_index_provider: HnswIndexProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        result_channel: Option<
            tokio::sync::oneshot::Sender<Result<CompactionResponse, Box<dyn ChromaError>>>,
        >,
        record_segment: Option<Segment>,
        next_offset_id: Arc<AtomicU32>,
        max_compaction_size: usize,
        max_partition_size: usize,
    ) -> Self {
        CompactOrchestrator {
            id: Uuid::new_v4(),
            compaction_job,
            state: ExecutionState::Pending,
            system,
            collection_id,
            log,
            sysdb,
            blockfile_provider,
            hnsw_index_provider,
            pulled_log_offset: None,
            dispatcher,
            num_write_tasks: 0,
            result_channel,
            record_segment,
            next_offset_id,
            max_compaction_size,
            max_partition_size,
            writers: None,
        }
    }

    async fn fetch_log(
        &mut self,
        self_address: Box<dyn ReceiverForMessage<TaskResult<FetchLogOutput, FetchLogError>>>,
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        self.state = ExecutionState::PullLogs;
        let operator = FetchLogOperator {
            log_client: self.log.clone(),
            batch_size: 100,
            // Here we do not need to be inclusive since the compaction job
            // offset is the one after the last compaction offset
            start_log_offset_id: self.compaction_job.offset as u32,
            maximum_fetch_count: Some(self.max_compaction_size as u32),
            collection_uuid: self.collection_id,
        };
        let task = wrap(Box::new(operator), (), self_address);
        match self.dispatcher.send(task, Some(Span::current())).await {
            Ok(_) => (),
            Err(e) => {
                tracing::error!("Error dispatching pull logs for compaction {:?}", e);
                terminate_with_error(
                    self.result_channel.take(),
                    Box::new(CompactionError::DispatchFailure),
                    ctx,
                );
            }
        }
    }

    async fn partition(
        &mut self,
        records: Chunk<LogRecord>,
        self_address: Box<dyn ReceiverForMessage<TaskResult<PartitionOutput, PartitionError>>>,
    ) {
        self.state = ExecutionState::Partition;
        let operator = PartitionOperator::new();
        tracing::info!("Sending N Records: {:?}", records.len());
        println!("Sending N Records: {:?}", records.len());
        let input = PartitionInput::new(records, self.max_partition_size);
        let task = wrap(operator, input, self_address);
        match self.dispatcher.send(task, Some(Span::current())).await {
            Ok(_) => (),
            Err(e) => {
                tracing::error!("Error dispatching partition for compaction {:?}", e);
                panic!(
                    "Invariant violation. Somehow the dispatcher receiver is dropped. Error: {:?}",
                    e
                )
            }
        }
    }

    async fn write(
        &mut self,
        partitions: Vec<Chunk<LogRecord>>,
        self_address: Box<
            dyn ReceiverForMessage<TaskResult<WriteSegmentsOutput, WriteSegmentsOperatorError>>,
        >,
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        self.state = ExecutionState::Write;

        if let Err(e) = self.init_segment_writers().await {
            tracing::error!("Error creating writers for compaction {:?}", e);
            terminate_with_error(self.result_channel.take(), e, ctx);
            return;
        }
        let (record_segment_writer, hnsw_segment_writer, metadata_segment_writer) =
            match self.writers.clone() {
                Some((rec, hnsw, mt)) => (Some(rec), Some(hnsw), Some(mt)),
                None => (None, None, None),
            };

        self.num_write_tasks = partitions.len() as i32;
        for parition in partitions.iter() {
            let operator = WriteSegmentsOperator::new();
            let input = WriteSegmentsInput::new(
                record_segment_writer.clone(),
                hnsw_segment_writer.clone(),
                metadata_segment_writer.clone(),
                parition.clone(),
                self.blockfile_provider.clone(),
                self.record_segment
                    .as_ref()
                    .expect("WriteSegmentsInput: Record segment not set in the input")
                    .clone(),
                self.next_offset_id.clone(),
            );
            let task = wrap(operator, input, self_address.clone());
            match self.dispatcher.send(task, Some(Span::current())).await {
                Ok(_) => (),
                Err(e) => {
                    tracing::error!("Error dispatching writers for compaction {:?}", e);
                    panic!(
                        "Invariant violation. Somehow the dispatcher receiver is dropped. Error: {:?}",
                        e)
                }
            }
        }
    }

    async fn flush_s3(
        &mut self,
        record_segment_writer: RecordSegmentWriter,
        hnsw_segment_writer: Box<DistributedHNSWSegmentWriter>,
        metadata_segment_writer: MetadataSegmentWriter<'static>,
        self_address: Box<dyn ReceiverForMessage<TaskResult<FlushS3Output, Box<dyn ChromaError>>>>,
    ) {
        self.state = ExecutionState::Flush;

        let operator = FlushS3Operator::new();
        let input = FlushS3Input::new(
            record_segment_writer,
            hnsw_segment_writer,
            metadata_segment_writer,
        );

        let task = wrap(operator, input, self_address);
        match self.dispatcher.send(task, Some(Span::current())).await {
            Ok(_) => (),
            Err(e) => {
                tracing::error!("Error dispatching flush to S3 for compaction {:?}", e);
                panic!(
                    "Invariant violation. Somehow the dispatcher receiver is dropped. Error: {:?}",
                    e
                );
            }
        }
    }

    async fn register(
        &mut self,
        log_position: i64,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
        self_address: Box<dyn ReceiverForMessage<TaskResult<RegisterOutput, RegisterError>>>,
    ) {
        self.state = ExecutionState::Register;
        let operator = RegisterOperator::new();
        let input = RegisterInput::new(
            self.compaction_job.tenant_id.clone(),
            self.compaction_job.collection_id,
            log_position,
            self.compaction_job.collection_version,
            segment_flush_info,
            self.sysdb.clone(),
            self.log.clone(),
        );

        let task = wrap(operator, input, self_address);
        match self.dispatcher.send(task, Some(Span::current())).await {
            Ok(_) => (),
            Err(e) => {
                tracing::error!("Error dispatching register for compaction {:?}", e);
                panic!(
                    "Invariant violation. Somehow the dispatcher receiver is dropped. Error: {:?}",
                    e
                );
            }
        }
    }

    async fn init_segment_writers(&mut self) -> Result<(), Box<dyn ChromaError>> {
        // Care should be taken to use the same writers across the compaction process
        // Since the segment writers are stateful, we should not create new writers for each partition
        // Nor should we create new writers across different tasks
        // This method is for convenience to create the writers in a single place
        // It is not meant to be called multiple times in the same compaction job

        let segments = self
            .sysdb
            .get_segments(None, None, None, self.collection_id)
            .await;

        tracing::info!("Retrived segments: {:?}", segments);

        let segments = match segments {
            Ok(segments) => {
                if segments.is_empty() {
                    return Err(Box::new(GetSegmentWritersError::NoSegmentsFound));
                }
                segments
            }
            Err(e) => {
                return Err(Box::new(GetSegmentWritersError::SysDbGetSegmentsError(e)));
            }
        };

        let record_segment = segments
            .iter()
            .find(|segment| segment.r#type == SegmentType::BlockfileRecord);

        tracing::debug!("Found Record Segment: {:?}", record_segment);

        if record_segment.is_none() {
            return Err(Box::new(GetSegmentWritersError::NoRecordSegmentFound));
        }
        // Create a record segment writer
        let record_segment = record_segment.unwrap();
        let record_segment_writer =
            match RecordSegmentWriter::from_segment(record_segment, &self.blockfile_provider).await
            {
                Ok(writer) => writer,
                Err(e) => {
                    tracing::error!("Error creating Record Segment Writer: {:?}", e);
                    return Err(Box::new(GetSegmentWritersError::RecordSegmentWriterError));
                }
            };

        tracing::debug!("Record Segment Writer created");
        match RecordSegmentReader::from_segment(record_segment, &self.blockfile_provider).await {
            Ok(reader) => {
                self.next_offset_id = Arc::new(AtomicU32::new(
                    reader
                        .get_current_max_offset_id()
                        .load(atomic::Ordering::SeqCst)
                        + 1,
                ));
            }
            Err(_) => {
                self.next_offset_id = Arc::new(AtomicU32::new(1));
            }
        };
        self.record_segment = Some(record_segment.clone()); // auto deref.

        let metadata_segment = segments
            .iter()
            .find(|segment| segment.r#type == SegmentType::BlockfileMetadata);

        tracing::debug!("Found metadata segment {:?}", metadata_segment);

        if metadata_segment.is_none() {
            return Err(Box::new(GetSegmentWritersError::NoMetadataSegmentFound));
        }
        // Create a record segment writer
        let mt_segment = metadata_segment.unwrap(); // safe to unwrap here.
        let mt_segment_writer =
            match MetadataSegmentWriter::from_segment(mt_segment, &self.blockfile_provider).await {
                Ok(writer) => writer,
                Err(e) => {
                    println!("Error creating metadata Segment Writer: {:?}", e);
                    return Err(Box::new(GetSegmentWritersError::MetadataSegmentWriterError));
                }
            };

        tracing::debug!("Metadata Segment Writer created");

        // Create a hnsw segment writer
        let collection_res = self
            .sysdb
            .get_collections(Some(self.collection_id), None, None, None)
            .await;

        let collection_res = match collection_res {
            Ok(collections) => {
                if collections.is_empty() {
                    return Err(Box::new(GetSegmentWritersError::CollectionNotFound));
                }
                collections
            }
            Err(e) => {
                return Err(Box::new(GetSegmentWritersError::GetCollectionError(e)));
            }
        };
        let collection = &collection_res[0];

        let hnsw_segment = segments
            .iter()
            .find(|segment| segment.r#type == SegmentType::HnswDistributed);
        if hnsw_segment.is_none() {
            return Err(Box::new(GetSegmentWritersError::NoHnswSegmentFound));
        }
        let hnsw_segment = hnsw_segment.unwrap();
        if let Some(dim) = collection.dimension {
            let hnsw_segment_writer = match DistributedHNSWSegmentWriter::from_segment(
                hnsw_segment,
                dim as usize,
                self.hnsw_index_provider.clone(),
            )
            .await
            {
                Ok(writer) => writer,
                Err(e) => {
                    println!("Error creating HNSW Segment Writer: {:?}", e);
                    return Err(Box::new(GetSegmentWritersError::HnswSegmentWriterError));
                }
            };
            self.writers = Some((
                record_segment_writer,
                hnsw_segment_writer,
                mt_segment_writer,
            ))
        }

        Ok(())
    }

    pub(crate) async fn run(mut self) -> Result<CompactionResponse, Box<dyn ChromaError>> {
        println!("Running compaction job: {:?}", self.compaction_job);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.result_channel = Some(tx);
        let mut handle = self.system.clone().start_component(self);
        let result = rx.await;
        handle.stop();
        result
            .map_err(|_| Box::new(CompactionError::ResultChannelDropped) as Box<dyn ChromaError>)?
    }
}

// ============== Component Implementation ==============

#[async_trait]
impl Component for CompactOrchestrator {
    fn get_name() -> &'static str {
        "Compaction orchestrator"
    }

    fn queue_size(&self) -> usize {
        1000 // TODO: make configurable
    }

    async fn on_start(&mut self, ctx: &crate::system::ComponentContext<Self>) -> () {
        self.fetch_log(ctx.receiver(), ctx).await;
    }
}

// ============== Handlers ==============
#[async_trait]
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for CompactOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchLogOutput, FetchLogError>,
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        let message = message.into_inner();
        let records = match message {
            Ok(result) => result,
            Err(e) => {
                terminate_with_error(self.result_channel.take(), Box::new(e), ctx);
                return;
            }
        };
        tracing::info!("Pulled Records: {:?}", records.len());
        let final_record_pulled = records.get(records.len() - 1);
        match final_record_pulled {
            Some(record) => {
                self.pulled_log_offset = Some(record.log_offset);
                tracing::info!("Pulled Logs Up To Offset: {:?}", self.pulled_log_offset);
                self.partition(records, ctx.receiver()).await;
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
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        let message = message.into_inner();
        let records = match message {
            Ok(result) => result.records,
            Err(e) => {
                tracing::error!("Error partitioning records: {:?}", e);
                terminate_with_error(self.result_channel.take(), Box::new(e), ctx);
                return;
            }
        };
        self.write(records, ctx.receiver(), ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<WriteSegmentsOutput, WriteSegmentsOperatorError>> for CompactOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<WriteSegmentsOutput, WriteSegmentsOperatorError>,
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        let message = message.into_inner();
        let output = match message {
            Ok(output) => {
                self.num_write_tasks -= 1;
                output
            }
            Err(e) => {
                tracing::error!("Error writing segments: {:?}", e);
                terminate_with_error(self.result_channel.take(), Box::new(e), ctx);
                return;
            }
        };
        if self.num_write_tasks == 0 {
            if let (Some(rec), Some(hnsw), Some(mt)) = (
                output.record_segment_writer,
                output.hnsw_segment_writer,
                output.metadata_segment_writer,
            ) {
                self.flush_s3(rec, hnsw, mt, ctx.receiver()).await;
            } else {
                // There is nothing to flush, proceed to register
                self.register(
                    self.pulled_log_offset.unwrap(),
                    Arc::new([]),
                    ctx.receiver(),
                )
                .await;
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<FlushS3Output, Box<dyn ChromaError>>> for CompactOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FlushS3Output, Box<dyn ChromaError>>,
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        let message = message.into_inner();
        match message {
            Ok(msg) => {
                // Unwrap should be safe here as we are guaranteed to have a value by construction
                self.register(
                    self.pulled_log_offset.unwrap(),
                    msg.segment_flush_info,
                    ctx.receiver(),
                )
                .await;
            }
            Err(e) => {
                tracing::error!("Error flushing to S3: {:?}", e);
                terminate_with_error(self.result_channel.take(), e.boxed(), ctx);
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<RegisterOutput, RegisterError>> for CompactOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<RegisterOutput, RegisterError>,
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        let message = message.into_inner();
        // Return execution state to the compaction manager
        let result_channel = self
            .result_channel
            .take()
            .expect("Invariant violation. Result channel is not set.");

        match message {
            Ok(_) => {
                let response = CompactionResponse {
                    id: self.id,
                    compaction_job: self.compaction_job.clone(),
                    message: "Compaction Complete".to_string(),
                };
                let _ = result_channel.send(Ok(response));
            }
            Err(e) => {
                tracing::error!("Error registering compaction: {:?}", e);
                terminate_with_error(Some(result_channel), Box::new(e), ctx);
            }
        }
    }
}
