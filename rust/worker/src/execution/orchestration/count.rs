use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::bloom_filter::BloomFilterManager;
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{plan::ReadLevel, CollectionAndSegments};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::execution::operators::{
    count_records::{
        CountRecordsError, CountRecordsInput, CountRecordsOperator, CountRecordsOutput,
    },
    fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
    filter_logs_for_shard::{
        FilterLogsForShardError, FilterLogsForShardOperator, FilterLogsForShardOutput,
    },
};

#[derive(Error, Debug)]
pub enum CountError {
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error running Fetch Log Operator: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error running Count Record Operator: {0}")]
    CountRecord(#[from] CountRecordsError),
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error partitioning logs to shard: {0}")]
    FilterLogsForShard(#[from] FilterLogsForShardError),
}

impl ChromaError for CountError {
    fn code(&self) -> ErrorCodes {
        match self {
            CountError::Channel(e) => e.code(),
            CountError::FetchLog(e) => e.code(),
            CountError::CountRecord(e) => e.code(),
            CountError::Panic(_) => ErrorCodes::Aborted,
            CountError::Result(_) => ErrorCodes::Internal,
            CountError::Aborted => ErrorCodes::ResourceExhausted,
            CountError::FilterLogsForShard(e) => e.code(),
        }
    }
}

impl<E> From<TaskError<E>> for CountError
where
    E: Into<CountError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => CountError::Panic(e),
            TaskError::TaskFailed(e) => e.into(),
            TaskError::Aborted => CountError::Aborted,
        }
    }
}

type CountOutput = (u32, u64);
type CountResult = Result<CountOutput, CountError>;

#[derive(Debug)]
pub struct CountOrchestrator {
    // Orchestrator parameters
    context: OrchestratorContext,
    blockfile_provider: BlockfileProvider,
    queue: usize,
    // Collection and segments
    collection_and_segments: CollectionAndSegments,

    // Fetch logs
    fetch_log: FetchLogOperator,

    // Read level
    read_level: ReadLevel,

    // Maximum number of WAL entries to read for IndexAndBoundedWal.
    bounded_wal_limit: u32,

    // Bloom filter manager
    bloom_filter_manager: Option<BloomFilterManager>,

    // Sharding
    shard_index: u32,
    num_shards: u32,

    // Fetched log size
    fetch_log_bytes: Option<u64>,

    // Result channel
    result_channel: Option<Sender<CountResult>>,
}

impl CountOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: chroma_system::ComponentHandle<Dispatcher>,
        queue: usize,
        collection_and_segments: CollectionAndSegments,
        fetch_log: FetchLogOperator,
        read_level: ReadLevel,
        bounded_wal_limit: u32,
        bloom_filter_manager: Option<BloomFilterManager>,
        shard_index: u32,
        num_shards: u32,
    ) -> Self {
        let context = OrchestratorContext::new(dispatcher);
        Self {
            context,
            blockfile_provider,
            collection_and_segments,
            queue,
            fetch_log,
            read_level,
            bounded_wal_limit,
            bloom_filter_manager,
            shard_index,
            num_shards,
            fetch_log_bytes: None,
            result_channel: None,
        }
    }
}

#[async_trait]
impl Orchestrator for CountOrchestrator {
    type Output = CountOutput;
    type Error = CountError;

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
        let mut tasks = Vec::new();
        match self.read_level {
            ReadLevel::IndexOnly => {
                tracing::info!("Skipping log fetch for IndexOnly read level");
                let empty_logs = FetchLogOutput::new(Vec::new().into());
                self.fetch_log_bytes.replace(0);
                let task = wrap(
                    CountRecordsOperator::new(),
                    CountRecordsInput::new(
                        self.collection_and_segments.record_segment.clone(),
                        self.blockfile_provider.clone(),
                        empty_logs,
                        self.bloom_filter_manager.clone(),
                        self.shard_index,
                    ),
                    ctx.receiver(),
                    self.context.task_cancellation_token.clone(),
                );
                tasks.push((task, Some(Span::current())));
            }
            ReadLevel::IndexAndWal => {
                let fetch_log_task = wrap(
                    Box::new(self.fetch_log.clone()),
                    (),
                    ctx.receiver(),
                    self.context.task_cancellation_token.clone(),
                );
                tasks.push((fetch_log_task, Some(Span::current())));
            }
            ReadLevel::IndexAndBoundedWal => {
                // Bounded WAL read: fetch up to `bounded_wal_limit` log
                // entries from the compaction frontier. This provides a
                // consistent prefix of the WAL with bounded query latency —
                // the operator will read at most K entries regardless of how
                // far behind compaction is.
                tracing::info!(
                    limit = self.bounded_wal_limit,
                    "Fetching bounded logs for IndexAndBoundedWal"
                );
                let mut bounded_fetch_log = self.fetch_log.clone();
                bounded_fetch_log.maximum_fetch_count = Some(self.bounded_wal_limit);
                let fetch_log_task = wrap(
                    Box::new(bounded_fetch_log),
                    (),
                    ctx.receiver(),
                    self.context.task_cancellation_token.clone(),
                );
                tasks.push((fetch_log_task, Some(Span::current())));
            }
        }
        tasks
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(&mut self, sender: Sender<CountResult>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Option<Sender<CountResult>> {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for CountOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchLogOutput, FetchLogError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        let task = wrap(
            Box::new(FilterLogsForShardOperator {
                shard_index: self.shard_index,
                num_shards: self.num_shards,
                record_segment: self.collection_and_segments.record_segment.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                bloom_filter_manager: self.bloom_filter_manager.clone(),
            }),
            output,
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<FilterLogsForShardOutput, FilterLogsForShardError>> for CountOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FilterLogsForShardOutput, FilterLogsForShardError>,
        ctx: &ComponentContext<Self>,
    ) {
        let partitioned = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        self.fetch_log_bytes
            .replace(partitioned.iter().map(|(l, _)| l.size_bytes()).sum());
        let task = wrap(
            CountRecordsOperator::new(),
            CountRecordsInput::new(
                self.collection_and_segments.record_segment.clone(),
                self.blockfile_provider.clone(),
                partitioned,
                self.bloom_filter_manager.clone(),
                self.shard_index,
            ),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<CountRecordsOutput, CountRecordsError>> for CountOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<CountRecordsOutput, CountRecordsError>,
        ctx: &ComponentContext<Self>,
    ) {
        self.terminate_with_result(
            message.into_inner().map_err(|e| e.into()).map(|output| {
                (
                    output.count as u32,
                    self.fetch_log_bytes
                        .expect("FetchLogOperator should have finished already"),
                )
            }),
            ctx,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_config::{registry::Registry, Configurable};
    use chroma_log::{
        in_memory_log::{InMemoryLog, InternalLogRecord},
        test::{upsert_generator, LogGenerator, TEST_EMBEDDING_DIMENSION},
        Log,
    };
    use chroma_segment::test::TestDistributedSegment;
    use chroma_system::{Dispatcher, Orchestrator, System};
    use chroma_types::Chunk;

    use crate::config::RootConfig;

    /// Verifies the semantic behavior of all ReadLevel variants using a
    /// fixture with 5 compacted + 10 uncompacted records.
    #[tokio::test]
    async fn test_read_level_semantics() {
        // -- Setup: 5 compacted records in segments, 10 more only in the log.
        const COMPACTED: usize = 5;
        const UNCOMPACTED: usize = 10;
        const TOTAL: usize = COMPACTED + UNCOMPACTED;

        let config = RootConfig::default();
        let system = System::default();
        let registry = Registry::new();
        let dispatcher = Dispatcher::try_from_config(&config.query_service.dispatcher, &registry)
            .await
            .expect("Should be able to initialize dispatcher");
        let dispatcher_handle = system.start_component(dispatcher);

        let mut test_segments =
            TestDistributedSegment::new_with_dimension(TEST_EMBEDDING_DIMENSION).await;
        let collection_id = test_segments.collection.collection_id;

        let all_records = upsert_generator.generate_vec(0..TOTAL);
        let compacted_chunk = Chunk::new(all_records[..COMPACTED].to_vec().into());
        Box::pin(test_segments.compact_log(compacted_chunk, 0)).await;
        test_segments.collection.log_position = (COMPACTED - 1) as i64;

        let mut in_memory_log = InMemoryLog::new();
        for record in &all_records {
            in_memory_log.add_log(
                collection_id,
                InternalLogRecord {
                    collection_id,
                    log_offset: record.log_offset,
                    log_ts: record.log_offset + 1,
                    record: record.clone(),
                },
            );
        }
        let log = Log::InMemory(in_memory_log);

        // -- Helper closure: run CountOrchestrator with given params.
        let count = |read_level: ReadLevel, limit: u32| {
            let system = system.clone();
            let dh = dispatcher_handle.clone();
            let ts = &test_segments;
            let log = log.clone();
            async move {
                let orchestrator = CountOrchestrator::new(
                    ts.blockfile_provider.clone(),
                    dh,
                    1000,
                    ts.into(),
                    FetchLogOperator {
                        log_client: log,
                        batch_size: 100,
                        start_log_offset_id: COMPACTED as u64,
                        maximum_fetch_count: None,
                        collection_uuid: ts.collection.collection_id,
                        tenant: ts.collection.tenant.clone(),
                        database_name: chroma_types::DatabaseName::new(
                            ts.collection.database.clone(),
                        )
                        .unwrap(),
                        fetch_log_concurrency: 10,
                        fragment_fetcher: None,
                        log_upper_bound_offset: TOTAL as i64,
                    },
                    read_level,
                    limit,
                    ts.bloom_filter_manager.clone(),
                    0,
                    1,
                );
                let (c, _) = orchestrator
                    .run(system)
                    .await
                    .expect("count should succeed");
                c
            }
        };

        // IndexAndWal: full consistency — sees compacted + WAL.
        assert_eq!(count(ReadLevel::IndexAndWal, 250).await, TOTAL as u32);

        // IndexOnly: skips WAL — sees only compacted.
        assert_eq!(count(ReadLevel::IndexOnly, 250).await, COMPACTED as u32);

        // Bounded WAL, limit < WAL size: reads partial WAL.
        assert_eq!(
            count(ReadLevel::IndexAndBoundedWal, 3).await,
            (COMPACTED + 3) as u32,
        );

        // Bounded WAL, limit == WAL size: reads entire WAL.
        assert_eq!(
            count(ReadLevel::IndexAndBoundedWal, UNCOMPACTED as u32).await,
            TOTAL as u32,
        );

        // Bounded WAL, limit > WAL size: reads all available, same as IndexAndWal.
        assert_eq!(
            count(ReadLevel::IndexAndBoundedWal, 100).await,
            TOTAL as u32,
        );
    }
}
