use super::super::operator::{wrap, TaskMessage};
use crate::blockstore::provider::BlockfileProvider;
use crate::compactor::CompactionJob;
use crate::errors::ChromaError;
use crate::execution::data::data_chunk::Chunk;
use crate::execution::operators::flush_sysdb::FlushSysDbInput;
use crate::execution::operators::flush_sysdb::FlushSysDbOperator;
use crate::execution::operators::flush_sysdb::FlushSysDbResult;
use crate::execution::operators::partition::PartitionInput;
use crate::execution::operators::partition::PartitionOperator;
use crate::execution::operators::partition::PartitionResult;
use crate::execution::operators::pull_log::PullLogsInput;
use crate::execution::operators::pull_log::PullLogsOperator;
use crate::execution::operators::pull_log::PullLogsResult;
use crate::execution::operators::write_segments::WriteSegmentsResult;
use crate::index::hnsw_provider::HnswIndexProvider;
use crate::log::log::Log;
use crate::segment::distributed_hnsw_segment::DistributedHNSWSegment;
use crate::segment::record_segment::RecordSegmentWriter;
use crate::segment::LogMaterializer;
use crate::segment::SegmentFlusher;
use crate::segment::SegmentWriter;
use crate::sysdb::sysdb::SysDb;
use crate::system::Component;
use crate::system::Handler;
use crate::system::Receiver;
use crate::system::System;
use crate::types::LogRecord;
use crate::types::SegmentFlushInfo;
use crate::types::SegmentType;
use async_trait::async_trait;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use uuid::Uuid;

/**  The state of the orchestrator.
In chroma, we have a relatively fixed number of query plans that we can execute. Rather
than a flexible state machine abstraction, we just manually define the states that we
expect to encounter for a given query plan. This is a bit more rigid, but it's also simpler and easier to
understand. We can always add more abstraction later if we need it.
```plaintext

                               ┌───► Write─────-------┐
                               │                      │
  Pending ─► PullLogs ─► Group │                      ├─► Flush ─► Finished
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
    // Dispatcher
    dispatcher: Box<dyn Receiver<TaskMessage>>,
    // number of write segments tasks
    num_write_tasks: i32,
    // Result Channel
    result_channel:
        Option<tokio::sync::oneshot::Sender<Result<CompactionResponse, Box<dyn ChromaError>>>>,
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
            dispatcher,
            num_write_tasks: 0,
            result_channel,
        }
    }

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
        let input = PullLogsInput::new(collection_id, 0, 100, None, Some(end_timestamp));
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

    async fn write(&mut self, partitions: Vec<Chunk<LogRecord>>) {
        self.state = ExecutionState::Write;

        // TODO: move this into an operator
        let segments = self
            .sysdb
            .get_segments(None, None, None, Some(self.collection_id))
            .await;

        println!("Writing to Segments: {:?}", segments);

        let segments = match segments {
            Ok(segments) => {
                if segments.is_empty() {
                    // Log an error and return
                    return;
                }
                segments
            }
            Err(e) => {
                // Log an error and return
                return;
            }
        };

        let record_segment = segments
            .iter()
            .find(|segment| segment.r#type == SegmentType::Record);

        println!("RS Record Segment: {:?}", record_segment);

        if record_segment.is_none() {
            // Log an error and return
            return;
        }
        // Create a record segment writer
        let record_segment = record_segment.unwrap();
        let record_segment_writer =
            match RecordSegmentWriter::from_segment(record_segment, &self.blockfile_provider) {
                Ok(writer) => writer,
                Err(e) => {
                    println!("Error creating Record Segment Writer: {:?}", e);
                    // Log an error and return
                    return;
                }
            };

        println!("Record Segment Writer created");

        // Create a hnsw segment writer
        // TODO: do this elsewhere
        let collection_res = self
            .sysdb
            .get_collections(Some(self.collection_id), None, None, None)
            .await;

        let collection_res = match collection_res {
            Ok(collections) => {
                if collections.is_empty() {
                    // Log an error and return
                    return;
                }
                collections
            }
            Err(e) => {
                // Log an error and return
                return;
            }
        };
        let collection = &collection_res[0];

        let hnsw_segment = segments
            .iter()
            .find(|segment| segment.r#type == SegmentType::HnswDistributed);
        if hnsw_segment.is_none() {
            // Log an error and return
            return;
        }
        let hnsw_segment = hnsw_segment.unwrap();
        let dimension = collection
            .dimension
            .expect("Dimension is required in the compactor");

        // TODO: real path
        let path = "/tmp/hnsw_segment";
        let as_path = std::path::Path::new(path);
        let hnsw_segment_writer = match DistributedHNSWSegment::from_segment(
            hnsw_segment,
            as_path,
            dimension as usize,
            self.hnsw_index_provider.clone(),
        ) {
            Ok(writer) => writer,
            Err(e) => {
                // Log an error and return
                return;
            }
        };

        println!("Partitions: {:?}", partitions.len());
        for partition in partitions {
            println!("Materializing N Records: {:?}", partition.len());
            let res = record_segment_writer.materialize(&partition).await;
            println!("Materialized Records: {:?}", res);
            hnsw_segment_writer.apply_materialized_log_chunk(res);
        }

        let record_segment_flusher = record_segment_writer.commit();
        match record_segment_flusher {
            Ok(flusher) => {
                let res = flusher.flush().await;
                match res {
                    Ok(_) => {
                        println!("Record Segment Flushed")
                    }
                    Err(e) => {
                        println!("Error Flushing Record Segment: {:?}", e)
                    }
                }
            }
            Err(e) => {
                println!("Error Commiting Record Segment: {:?}", e)
            }
        }

        let hnsw_segment_flusher = hnsw_segment_writer.commit();
        match hnsw_segment_flusher {
            Ok(flusher) => {
                let res = flusher.flush().await;
                match res {
                    Ok(_) => {
                        println!("HNSW Segment Flushed")
                    }
                    Err(e) => {
                        println!("Error Flushing HNSW Segment: {:?}", e)
                    }
                }
            }
            Err(e) => {
                println!("Error Commiting HNSW Segment: {:?}", e)
            }
        }
        println!("HNSW FLUSHED");
    }

    async fn flush_s3(&mut self, self_address: Box<dyn Receiver<WriteSegmentsResult>>) {
        self.state = ExecutionState::Flush;
        // TODO: implement flush to s3
    }

    async fn flush_sysdb(
        &mut self,
        log_position: i64,
        segment_flush_info: Vec<SegmentFlushInfo>,
        self_address: Box<dyn Receiver<FlushSysDbResult>>,
    ) {
        self.state = ExecutionState::Register;
        let operator = FlushSysDbOperator::new();
        let input = FlushSysDbInput::new(
            self.compaction_job.tenant_id.clone(),
            self.compaction_job.collection_id.clone(),
            log_position,
            self.compaction_job.collection_version,
            segment_flush_info.into(),
            self.sysdb.clone(),
        );

        let task = wrap(operator, input, self_address);
        match self.dispatcher.send(task, None).await {
            Ok(_) => (),
            Err(e) => {
                // TODO: log an error and reply to caller
            }
        }
    }

    pub(crate) async fn run(mut self) -> Result<CompactionResponse, Box<dyn ChromaError>> {
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
        self.partition(records, ctx.sender.as_receiver()).await;
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
        // TODO: implement write records as operator and handle in WriteSegmentsResult
        self.write(records).await;

        // For now, we will return to execution state to the compaction manager
        let result_channel = match self.result_channel.take() {
            Some(tx) => tx,
            None => {
                // Log an error
                return;
            }
        };
        let response = CompactionResponse {
            id: self.id,
            compaction_job: self.compaction_job.clone(),
            message: "Compaction Complete".to_string(),
        };
        let _ = result_channel.send(Ok(response));
    }
}

#[async_trait]
impl Handler<WriteSegmentsResult> for CompactOrchestrator {
    async fn handle(
        &mut self,
        message: WriteSegmentsResult,
        _ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        match message {
            Ok(result) => {
                // Log an error
                self.num_write_tasks -= 1;
            }
            Err(e) => {
                // Log an error
            }
        }
        if self.num_write_tasks == 0 {
            self.flush_s3(_ctx.sender.as_receiver()).await;
        }
    }
}
