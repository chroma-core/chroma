use super::super::operator::wrap;
use super::orchestrator::Orchestrator;
use crate::compactor::CompactionJob;
use crate::execution::dispatcher::Dispatcher;
use crate::execution::operator::TaskError;
use crate::execution::operator::TaskMessage;
use crate::execution::operator::TaskResult;
use crate::execution::operators::fetch_log::FetchLogError;
use crate::execution::operators::fetch_log::FetchLogOperator;
use crate::execution::operators::fetch_log::FetchLogOutput;
use crate::execution::operators::flush_s3::FlushS3Input;
use crate::execution::operators::flush_s3::FlushS3Operator;
use crate::execution::operators::flush_s3::FlushS3Output;
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
use crate::execution::operators::write_segments::WriteSegmentsInput;
use crate::execution::operators::write_segments::WriteSegmentsOperator;
use crate::execution::operators::write_segments::WriteSegmentsOperatorError;
use crate::execution::operators::write_segments::WriteSegmentsOutput;
use crate::log::log::Log;
use crate::segment::distributed_hnsw_segment::DistributedHNSWSegmentWriter;
use crate::segment::metadata_segment::MetadataSegmentWriter;
use crate::segment::record_segment::RecordSegmentReader;
use crate::segment::record_segment::RecordSegmentReaderCreationError;
use crate::segment::record_segment::RecordSegmentWriter;
use crate::segment::MaterializeLogsResult;
use crate::sysdb::sysdb::GetCollectionsError;
use crate::sysdb::sysdb::GetSegmentsError;
use crate::sysdb::sysdb::SysDb;
use crate::system::ChannelError;
use crate::system::ComponentContext;
use crate::system::ComponentHandle;
use crate::system::Handler;
use crate::system::ReceiverForMessage;
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_types::Chunk;
use chroma_types::{CollectionUuid, LogRecord, Segment, SegmentFlushInfo, SegmentType};
use core::panic;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::Sender;
use tokio::sync::OnceCell;
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
    Partition,
    MaterializeAndWrite,
    Flush,
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
    // number of write segments tasks
    num_write_tasks: i32,
    // Result Channel
    result_channel: Option<Sender<Result<CompactionResponse, CompactionError>>>,
    max_compaction_size: usize,
    max_partition_size: usize,
    // Populated during the compaction process
    cached_segments: Option<Vec<Segment>>,
    writers: OnceCell<CompactWriters>,
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
    #[error("Panic running task: {0}")]
    Panic(String),
    #[error("FetchLog error: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Partition error: {0}")]
    Partition(#[from] PartitionError),
    #[error("MaterializeLogs error: {0}")]
    MaterializeLogs(#[from] MaterializeLogOperatorError),
    #[error("WriteSegments error: {0}")]
    WriteSegments(#[from] WriteSegmentsOperatorError),
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
}

impl<E> From<TaskError<E>> for CompactionError
where
    E: Into<CompactionError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => CompactionError::Panic(e.unwrap_or_default()),
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
            num_write_tasks: 0,
            result_channel,
            max_compaction_size,
            max_partition_size,
            cached_segments: None,
            writers: OnceCell::new(),
        }
    }

    async fn partition(
        &mut self,
        records: Chunk<LogRecord>,
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
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
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        self.state = ExecutionState::MaterializeAndWrite;

        let record_segment_result = self.get_segment(SegmentType::BlockfileRecord).await;
        let record_segment = match self.ok_or_terminate(record_segment_result, ctx) {
            Some(segment) => segment,
            None => return,
        };

        let next_max_offset_id = match self.ok_or_terminate(
            match RecordSegmentReader::from_segment(&record_segment, &self.blockfile_provider).await
            {
                Ok(reader) => {
                    let current_max_offset_id = reader.get_current_max_offset_id();
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

        self.num_write_tasks = partitions.len() as i32;
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

    async fn write(
        &mut self,
        materialized_logs: MaterializeLogsResult,
        self_address: Box<
            dyn ReceiverForMessage<TaskResult<WriteSegmentsOutput, WriteSegmentsOperatorError>>,
        >,
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
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

        let operator = WriteSegmentsOperator::new();
        let input = WriteSegmentsInput::new(
            writers,
            self.blockfile_provider.clone(),
            record_segment,
            materialized_logs,
        );
        let task = wrap(operator, input, self_address);
        self.send(task, ctx).await;
    }

    async fn flush_s3(
        &mut self,
        record_segment_writer: RecordSegmentWriter,
        hnsw_segment_writer: Box<DistributedHNSWSegmentWriter>,
        metadata_segment_writer: MetadataSegmentWriter<'static>,
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        self.state = ExecutionState::Flush;

        let operator = FlushS3Operator::new();
        let input = FlushS3Input::new(
            record_segment_writer,
            hnsw_segment_writer,
            metadata_segment_writer,
        );

        let task = wrap(operator, input, ctx.receiver());
        self.send(task, ctx).await;
    }

    async fn register(
        &mut self,
        log_position: i64,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
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
                            println!("Error creating metadata Segment Writer: {:?}", e);
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
                            println!("Error creating HNSW Segment Writer: {:?}", e);
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
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
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
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
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
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        let materialized_result = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(result) => result,
            None => return,
        };

        if materialized_result.is_empty() {
            self.num_write_tasks -= 1;

            if self.num_write_tasks == 0 {
                // There is nothing to flush, proceed to register
                self.register(self.pulled_log_offset.unwrap(), Arc::new([]), ctx)
                    .await;
            }
        } else {
            self.write(materialized_result, ctx.receiver(), ctx).await;
        }
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };
        self.num_write_tasks -= 1;
        if self.num_write_tasks == 0 {
            self.flush_s3(
                output.writers.record,
                output.writers.vector,
                output.writers.metadata,
                ctx,
            )
            .await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };
        self.register(
            self.pulled_log_offset.unwrap(),
            output.segment_flush_info,
            ctx,
        )
        .await;
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
