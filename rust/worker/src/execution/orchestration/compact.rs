use super::super::operator::{wrap, TaskMessage};
use super::hnsw;
use crate::blockstore::provider::BlockfileProvider;
use crate::compactor::CompactionJob;
use crate::errors::ChromaError;
use crate::execution::data::data_chunk::Chunk;
use crate::execution::operators::flush_s3::FlushS3Input;
use crate::execution::operators::flush_s3::FlushS3Operator;
use crate::execution::operators::flush_s3::FlushS3Result;
use crate::execution::operators::partition::PartitionInput;
use crate::execution::operators::partition::PartitionOperator;
use crate::execution::operators::partition::PartitionResult;
use crate::execution::operators::pull_log::PullLogsInput;
use crate::execution::operators::pull_log::PullLogsOperator;
use crate::execution::operators::pull_log::PullLogsResult;
use crate::execution::operators::register::RegisterInput;
use crate::execution::operators::register::RegisterOperator;
use crate::execution::operators::register::RegisterResult;
use crate::execution::operators::write_segments::WriteSegmentsInput;
use crate::execution::operators::write_segments::WriteSegmentsOperator;
use crate::execution::operators::write_segments::WriteSegmentsResult;
use crate::index::hnsw_provider::HnswIndexProvider;
use crate::log::log::Log;
use crate::segment::distributed_hnsw_segment::DistributedHNSWSegment;
use crate::segment::record_segment::RecordSegmentWriter;
use crate::segment::LogMaterializer;
use crate::segment::SegmentFlusher;
use crate::segment::SegmentWriter;
use crate::sysdb::sysdb::GetCollectionsError;
use crate::sysdb::sysdb::GetSegmentsError;
use crate::sysdb::sysdb::SysDb;
use crate::system::Component;
use crate::system::Handler;
use crate::system::Receiver;
use crate::system::System;
use crate::types::LogRecord;
use crate::types::SegmentFlushInfo;
use crate::types::SegmentType;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
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
    Finished,
}

#[derive(Debug)]
pub struct CompactOrchestrator {
    id: Uuid,
    compaction_job: CompactionJob,
    state: ExecutionState,
    // Component Execution
    system: System,
    collection_id: Uuid,
    // Dependencies
    log: Box<dyn Log>,
    sysdb: Box<dyn SysDb>,
    blockfile_provider: BlockfileProvider,
    hnsw_index_provider: HnswIndexProvider,
    // State we hold across the execution
    pulled_log_offset: Option<i64>,
    // Dispatcher
    dispatcher: Box<dyn Receiver<TaskMessage>>,
    // number of write segments tasks
    num_write_tasks: i32,
    // Result Channel
    result_channel:
        Option<tokio::sync::oneshot::Sender<Result<CompactionResponse, Box<dyn ChromaError>>>>,
}

#[derive(Error, Debug)]
enum GetSegmentWritersError {
    #[error("No segments found for collection")]
    NoSegmentsFound,
    #[error("SysDB GetSegments Error")]
    SysDbGetSegmentsError(#[from] GetSegmentsError),
    #[error("Error creating Record Segment Writer")]
    RecordSegmentWriterError,
    #[error("Error creating HNSW Segment Writer")]
    HnswSegmentWriterError,
    #[error("No record segment found for collection")]
    NoRecordSegmentFound,
    #[error("Collection not found")]
    CollectionNotFound,
    #[error("Error getting collection")]
    GetCollectionError(#[from] GetCollectionsError),
    #[error("No hnsw segment found for collection")]
    NoHnswSegmentFound,
}

impl ChromaError for GetSegmentWritersError {
    fn code(&self) -> crate::errors::ErrorCodes {
        crate::errors::ErrorCodes::Internal
    }
}

// TODO: we need to improve this response
#[derive(Debug)]
pub struct CompactionResponse {
    id: Uuid,
    compaction_job: CompactionJob,
    message: String,
}

impl CompactOrchestrator {
    pub fn new(
        compaction_job: CompactionJob,
        system: System,
        collection_id: Uuid,
        log: Box<dyn Log>,
        sysdb: Box<dyn SysDb>,
        blockfile_provider: BlockfileProvider,
        hnsw_index_provider: HnswIndexProvider,
        dispatcher: Box<dyn Receiver<TaskMessage>>,
        result_channel: Option<
            tokio::sync::oneshot::Sender<Result<CompactionResponse, Box<dyn ChromaError>>>,
        >,
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
        }
    }

    // TODO: It is possible that the offset_id from the compaction job is wrong since the log service
    // can have an outdated view of the offset. We should filter out entries from the log based on the start offset
    // of the segment, and not fully respect the offset_id from the compaction job
    async fn pull_logs(&mut self, self_address: Box<dyn Receiver<PullLogsResult>>) {
        self.state = ExecutionState::PullLogs;
        let operator = PullLogsOperator::new(self.log.clone());
        let collection_id = self.collection_id;
        let end_timestamp = SystemTime::now().duration_since(UNIX_EPOCH);
        let end_timestamp = match end_timestamp {
            // TODO: change protobuf definition to use u64 instead of i64
            Ok(end_timestamp) => end_timestamp.as_nanos() as i64,
            Err(e) => {
                // Log an error and reply + return
                return;
            }
        };
        let input = PullLogsInput::new(
            collection_id,
            // Here we do not need to be inclusive since the compaction job
            // offset is the one after the last compaction offset
            self.compaction_job.offset,
            100,
            None,
            Some(end_timestamp),
        );
        let task = wrap(operator, input, self_address);
        match self.dispatcher.send(task, None).await {
            Ok(_) => (),
            Err(e) => {
                // TODO: log an error and reply to caller
            }
        }
    }

    async fn partition(
        &mut self,
        records: Chunk<LogRecord>,
        self_address: Box<dyn Receiver<PartitionResult>>,
    ) {
        self.state = ExecutionState::Partition;
        // TODO: make this configurable
        let max_partition_size = 100;
        let operator = PartitionOperator::new();
        println!("Sending N Records: {:?}", records.len());
        let input = PartitionInput::new(records, max_partition_size);
        let task = wrap(operator, input, self_address);
        match self.dispatcher.send(task, None).await {
            Ok(_) => (),
            Err(e) => {
                // TODO: log an error and reply to caller
            }
        }
    }

    async fn write(
        &mut self,
        partitions: Vec<Chunk<LogRecord>>,
        self_address: Box<dyn Receiver<WriteSegmentsResult>>,
    ) {
        self.state = ExecutionState::Write;

        let writer_res = self.get_segment_writers().await;
        let (record_segment_writer, hnsw_segment_writer) = match writer_res {
            Ok(writers) => writers,
            Err(e) => {
                // Log an error and return
                return;
            }
        };

        self.num_write_tasks = partitions.len() as i32;
        for parition in partitions.iter() {
            let operator = WriteSegmentsOperator::new();
            let input = WriteSegmentsInput::new(
                record_segment_writer.clone(),
                hnsw_segment_writer.clone(),
                parition.clone(),
            );
            let task = wrap(operator, input, self_address.clone());
            match self.dispatcher.send(task, Some(Span::current())).await {
                Ok(_) => (),
                Err(e) => {
                    // Log an error and reply to caller
                }
            }
        }
    }

    async fn flush_s3(
        &mut self,
        record_segment_writer: RecordSegmentWriter,
        hnsw_segment_writer: Box<DistributedHNSWSegment>,
        self_address: Box<dyn Receiver<FlushS3Result>>,
    ) {
        self.state = ExecutionState::Flush;

        let operator = FlushS3Operator::new();
        let input = FlushS3Input::new(record_segment_writer, hnsw_segment_writer);

        let task = wrap(operator, input, self_address);
        match self.dispatcher.send(task, Some(Span::current())).await {
            Ok(_) => (),
            Err(e) => {
                // Log an error and reply to caller
            }
        }
    }

    async fn register(
        &mut self,
        log_position: i64,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
        self_address: Box<dyn Receiver<RegisterResult>>,
    ) {
        self.state = ExecutionState::Register;
        let operator = RegisterOperator::new();
        let input = RegisterInput::new(
            self.compaction_job.tenant_id.clone(),
            self.compaction_job.collection_id,
            log_position,
            self.compaction_job.collection_version,
            segment_flush_info.into(),
            self.sysdb.clone(),
            self.log.clone(),
        );

        let task = wrap(operator, input, self_address);
        match self.dispatcher.send(task, None).await {
            Ok(_) => (),
            Err(e) => {
                // TODO: log an error and reply to caller
            }
        }
    }

    async fn get_segment_writers(
        &mut self,
    ) -> Result<(RecordSegmentWriter, Box<DistributedHNSWSegment>), Box<dyn ChromaError>> {
        // Care should be taken to use the same writers across the compaction process
        // Since the segment writers are stateful, we should not create new writers for each partition
        // Nor should we create new writers across different tasks
        // This method is for convenience to create the writers in a single place
        // It is not meant to be called multiple times in the same compaction job

        let segments = self
            .sysdb
            .get_segments(None, None, None, Some(self.collection_id))
            .await;

        println!("Retrived segments: {:?}", segments);

        let segments = match segments {
            Ok(segments) => {
                if segments.is_empty() {
                    // Log an error and return
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
            .find(|segment| segment.r#type == SegmentType::Record);

        println!("Found Record Segment: {:?}", record_segment);

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
                    println!("Error creating Record Segment Writer: {:?}", e);
                    return Err(Box::new(GetSegmentWritersError::RecordSegmentWriterError));
                }
            };

        println!("Record Segment Writer created");

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
        let dimension = collection
            .dimension
            .expect("Dimension is required in the compactor");

        let hnsw_segment_writer = match DistributedHNSWSegment::from_segment(
            hnsw_segment,
            dimension as usize,
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

        Ok((record_segment_writer, hnsw_segment_writer))
    }

    pub(crate) async fn run(mut self) -> Result<CompactionResponse, Box<dyn ChromaError>> {
        println!("Running compaction job: {:?}", self.compaction_job);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.result_channel = Some(tx);
        let mut handle = self.system.clone().start_component(self);
        let result = rx.await;
        handle.stop();
        result.unwrap()
    }
}

// ============== Component Implementation ==============

#[async_trait]
impl Component for CompactOrchestrator {
    fn queue_size(&self) -> usize {
        1000 // TODO: make configurable
    }

    async fn on_start(&mut self, ctx: &crate::system::ComponentContext<Self>) -> () {
        self.pull_logs(ctx.sender.as_receiver()).await;
    }
}

// ============== Handlers ==============
#[async_trait]
impl Handler<PullLogsResult> for CompactOrchestrator {
    async fn handle(
        &mut self,
        message: PullLogsResult,
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        let records = match message {
            Ok(result) => result.logs(),
            Err(e) => {
                // Log an error and return
                let result_channel = match self.result_channel.take() {
                    Some(tx) => tx,
                    None => {
                        // Log an error
                        return;
                    }
                };
                let _ = result_channel.send(Err(Box::new(e)));
                return;
            }
        };
        println!("Pulled Records: {:?}", records.len());
        let final_record_pulled = records.get(records.len() - 1);
        match final_record_pulled {
            Some(record) => {
                self.pulled_log_offset = Some(record.log_offset);
                println!("Pulled Logs Up To Offset: {:?}", self.pulled_log_offset);
                self.partition(records, ctx.sender.as_receiver()).await;
            }
            None => {
                // Log an error and return
                return;
            }
        }
    }
}

#[async_trait]
impl Handler<PartitionResult> for CompactOrchestrator {
    async fn handle(
        &mut self,
        message: PartitionResult,
        _ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        let records = match message {
            Ok(result) => result.records,
            Err(e) => {
                // Log an error and return
                let result_channel = match self.result_channel.take() {
                    Some(tx) => tx,
                    None => {
                        // Log an error
                        return;
                    }
                };
                let _ = result_channel.send(Err(Box::new(e)));
                return;
            }
        };
        self.write(records, _ctx.sender.as_receiver()).await;
    }
}

#[async_trait]
impl Handler<WriteSegmentsResult> for CompactOrchestrator {
    async fn handle(
        &mut self,
        message: WriteSegmentsResult,
        _ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        println!("Write Segments Result: {:?}", message);
        let output = match message {
            Ok(output) => {
                self.num_write_tasks -= 1;
                output
            }
            Err(e) => {
                // Log an error
                return;
            }
        };
        if self.num_write_tasks == 0 {
            self.flush_s3(
                output.record_segment_writer,
                output.hnsw_segment_writer,
                _ctx.sender.as_receiver(),
            )
            .await;
        }
    }
}

#[async_trait]
impl Handler<FlushS3Result> for CompactOrchestrator {
    async fn handle(
        &mut self,
        message: FlushS3Result,
        _ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        match message {
            Ok(msg) => {
                // Unwrap should be safe here as we are guaranteed to have a value by construction
                self.register(
                    self.pulled_log_offset.unwrap(),
                    msg.segment_flush_info,
                    _ctx.sender.as_receiver(),
                )
                .await;
            }
            Err(e) => {
                // Log an error
            }
        }
    }
}

#[async_trait]
impl Handler<RegisterResult> for CompactOrchestrator {
    async fn handle(
        &mut self,
        message: RegisterResult,
        _ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        // Return execution state to the compaction manager
        let result_channel = match self.result_channel.take() {
            Some(tx) => tx,
            None => {
                // Log an error
                return;
            }
        };

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
                // Log an error
                let _ = result_channel.send(Err(Box::new(e)));
            }
        }
    }
}
