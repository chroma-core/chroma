use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_log::Log;
use chroma_segment::spann_provider::SpannProvider;
use chroma_sysdb::SysDb;
use chroma_system::{Component, ComponentContext, ComponentHandle, Dispatcher, Handler, System};
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{instrument, span};

use crate::compactor::config::CompactorConfig;
use crate::execution::orchestration::compact::CompactionContext;
use crate::execution::orchestration::function_execution::{
    FunctionExecutionContext, FunctionExecutionInput, FunctionExecutionOutcome,
};
use crate::fn_consumer::config::FnConsumerConfig;
use crate::work_queue::work_queue_client::WorkQueueClient;

#[derive(Debug)]
pub struct InProgressFn {
    expires_at: SystemTime,
    expiry_logged: bool,
}

impl InProgressFn {
    pub fn new(job_expiry_seconds: u64) -> Self {
        Self {
            expires_at: SystemTime::now() + Duration::from_secs(job_expiry_seconds),
            expiry_logged: false,
        }
    }

    pub fn is_expired(&self) -> bool {
        SystemTime::now() >= self.expires_at
    }
}

#[derive(Error, Debug)]
pub enum DispatchError {
    #[error("Dispatcher not initialized")]
    DispatcherNotInitialized,

    #[error("Compaction workflow failed: {0}")]
    CompactionFailed(#[from] crate::execution::orchestration::compact::CompactionError),

    #[error("Function consumer dispatch task panicked")]
    DispatchPanicked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FnDispatchOutcome {
    Completed,
    RetryLater,
}

impl ChromaError for DispatchError {
    fn code(&self) -> ErrorCodes {
        match self {
            DispatchError::DispatcherNotInitialized => ErrorCodes::Internal,
            DispatchError::CompactionFailed(_) => ErrorCodes::Internal,
            DispatchError::DispatchPanicked => ErrorCodes::Internal,
        }
    }
}

type FnDispatchOutput = Result<FnDispatchOutcome, DispatchError>;
type FnDispatchFuture = Pin<Box<dyn Future<Output = FnDispatchOutput> + Send>>;

struct FnDispatchTask {
    fn_id: AttachedFunctionUuid,
    future: FnDispatchFuture,
    // Retained separately because the dispatch future may panic before it can
    // report failures to the work queue itself.
    work_queue_client: Option<WorkQueueClient>,
    batch: Vec<FunctionExecutionInput>,
}

struct FnDispatchCompletion {
    fn_id: AttachedFunctionUuid,
    batch_size: usize,
    result: FnDispatchOutput,
}

#[derive(Clone)]
pub struct FnConsumerContext {
    pub system: System,
    pub dispatcher: Option<ComponentHandle<Dispatcher>>,
    pub poll_interval: Duration,
    pub max_concurrent_workers: usize,
    pub get_work_batch_size: u32,
    pub job_expiry_seconds: u64,
    pub max_failure_count: i32,
    pub my_member_id: String,
    pub log: Log,
    pub sysdb: SysDb,
    pub blockfile_provider: BlockfileProvider,
    pub hnsw_provider: HnswIndexProvider,
    pub spann_provider: SpannProvider,
    pub fetch_log_batch_size: u32,
    pub fetch_log_concurrency: usize,
    pub max_compaction_size: usize,
    pub max_partition_size: usize,
}

impl std::fmt::Debug for FnConsumerContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnConsumerContext")
            .field("poll_interval", &self.poll_interval)
            .field("max_concurrent_workers", &self.max_concurrent_workers)
            .field("get_work_batch_size", &self.get_work_batch_size)
            .field("job_expiry_seconds", &self.job_expiry_seconds)
            .field("my_member_id", &self.my_member_id)
            .finish()
    }
}

pub struct FnConsumerManager {
    context: FnConsumerContext,
    in_progress: HashMap<AttachedFunctionUuid, InProgressFn>,
    work_queue_client: WorkQueueClient,
    dispatch_awaiter_channel: mpsc::Sender<FnDispatchTask>,
    dispatch_awaiter_completion_channel: mpsc::UnboundedReceiver<FnDispatchCompletion>,
    dispatch_awaiter: tokio::task::JoinHandle<()>,
}

impl std::fmt::Debug for FnConsumerManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnConsumerManager")
            .field("context", &self.context)
            .field("in_progress_count", &self.in_progress.len())
            .finish()
    }
}

impl FnConsumerManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: FnConsumerConfig,
        compactor_config: CompactorConfig,
        my_member_id: String,
        system: System,
        work_queue_client: WorkQueueClient,
        log: Log,
        sysdb: SysDb,
        blockfile_provider: BlockfileProvider,
        hnsw_provider: HnswIndexProvider,
        spann_provider: SpannProvider,
    ) -> Self {
        let context = FnConsumerContext {
            system,
            dispatcher: None,
            poll_interval: Duration::from_secs(config.poll_interval_sec),
            max_concurrent_workers: config.max_concurrent_workers,
            get_work_batch_size: config.get_work_batch_size,
            job_expiry_seconds: config.job_expiry_seconds,
            max_failure_count: config.max_failure_count,
            my_member_id,
            log,
            sysdb,
            blockfile_provider,
            hnsw_provider,
            spann_provider,
            fetch_log_batch_size: compactor_config.fetch_log_batch_size,
            fetch_log_concurrency: compactor_config.fetch_log_concurrency,
            max_compaction_size: compactor_config.max_compaction_size,
            max_partition_size: compactor_config.max_partition_size,
        };
        let (dispatch_awaiter_tx, dispatch_awaiter_rx) =
            mpsc::channel::<FnDispatchTask>(config.max_concurrent_workers.max(1));
        // Every dispatched function sends exactly one completion, and we retain its
        // in-progress slot until that completion is drained. Therefore, pending
        // completions are bounded by max_concurrent_workers and need no backpressure.
        let (completion_tx, completion_rx) = mpsc::unbounded_channel::<FnDispatchCompletion>();
        let dispatch_awaiter = tokio::spawn(async move {
            fn_dispatch_awaiter_loop(dispatch_awaiter_rx, completion_tx).await;
        });
        Self {
            context,
            in_progress: HashMap::new(),
            work_queue_client,
            dispatch_awaiter_channel: dispatch_awaiter_tx,
            dispatch_awaiter_completion_channel: completion_rx,
            dispatch_awaiter,
        }
    }

    pub fn set_dispatcher(&mut self, dispatcher: ComponentHandle<Dispatcher>) {
        self.context.dispatcher = Some(dispatcher);
    }

    fn warn_expired(&mut self) {
        for (fn_id, job) in &mut self.in_progress {
            if job.is_expired() && !job.expiry_logged {
                tracing::warn!(
                    fn_id = %fn_id,
                    "Function consumer dispatch exceeded its expiry; retaining slot until completion"
                );
                job.expiry_logged = true;
            }
        }
    }

    fn compute_remaining_capacity(&self) -> usize {
        self.context
            .max_concurrent_workers
            .saturating_sub(self.in_progress.len())
    }

    fn fn_in_progress(&self, fn_id: AttachedFunctionUuid) -> bool {
        self.in_progress.contains_key(&fn_id)
    }

    /// Runs the attached function workflow for the given function across a batch of input collections.
    #[instrument(
        name = "FnConsumerManager::dispatch_batch",
        parent = None,
        skip(context, work_queue_client),
        err
    )]
    async fn dispatch_batch(
        context: FnConsumerContext,
        mut work_queue_client: WorkQueueClient,
        fn_id: AttachedFunctionUuid,
        batch: Vec<FunctionExecutionInput>,
    ) -> FnDispatchOutput {
        let Some(dispatcher) = context.dispatcher.clone() else {
            tracing::error!("Dispatcher not set on FnConsumerManager");
            return Err(DispatchError::DispatcherNotInitialized);
        };

        if batch.is_empty() {
            return Err(DispatchError::CompactionFailed(
                crate::execution::orchestration::compact::CompactionError::InvariantViolation(
                    "Function consumer dispatch requires at least one input collection",
                ),
            ));
        }

        // Create CompactionContext with is_fn_consumer = true. The function
        // execution flow applies each input collection's completion offset when
        // fetching logs, so the shared base context should not carry one.
        let compaction_context = CompactionContext::new(
            None, // rebuild_info
            context.fetch_log_batch_size,
            context.fetch_log_concurrency,
            context.max_compaction_size,
            context.max_partition_size,
            context.log.clone(),
            context.sysdb.clone(),
            context.blockfile_provider.clone(),
            context.hnsw_provider.clone(),
            context.spann_provider.clone(),
            dispatcher,
            false,                           // is_function_disabled
            true,                            // is_fn_consumer
            None,                            // fragment_fetcher
            None,                            // bloom_filter_manager
            None,                            // shard_size
            Some(work_queue_client.clone()), // work_queue_client
        );

        let function_execution_context = FunctionExecutionContext::new(&compaction_context);
        let result =
            Box::pin(function_execution_context.run(fn_id, batch.clone(), context.system.clone()))
                .await;

        match result {
            Ok(FunctionExecutionOutcome::Completed) => {
                tracing::info!(
                    fn_id = %fn_id,
                    batch_size = batch.len(),
                    "Function consumer workflow completed successfully"
                );
                Ok(FnDispatchOutcome::Completed)
            }
            Ok(FunctionExecutionOutcome::RetryLater) => {
                defer_batch(&mut work_queue_client, fn_id, &batch).await;
                tracing::debug!(
                    fn_id = %fn_id,
                    batch_size = batch.len(),
                    "Function consumer work is not ready; retrying on a later poll"
                );
                Ok(FnDispatchOutcome::RetryLater)
            }
            Err(e) => {
                tracing::error!(
                    fn_id = %fn_id,
                    batch_size = batch.len(),
                    "Function consumer workflow failed: {}",
                    e,
                );
                report_batch_failure(&mut work_queue_client, fn_id, &batch).await;
                Err(e.into())
            }
        }
    }

    fn process_completions(&mut self) {
        while let Ok(completion) = self.dispatch_awaiter_completion_channel.try_recv() {
            self.in_progress.remove(&completion.fn_id);

            match completion.result {
                Ok(FnDispatchOutcome::Completed) => {
                    tracing::debug!(
                        fn_id = %completion.fn_id,
                        batch_size = completion.batch_size,
                        "Successfully completed work batch"
                    );
                }
                Ok(FnDispatchOutcome::RetryLater) => {
                    tracing::debug!(
                        fn_id = %completion.fn_id,
                        batch_size = completion.batch_size,
                        "Work batch will be retried on a later poll"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        fn_id = %completion.fn_id,
                        batch_size = completion.batch_size,
                        error = %e,
                        "Failed to process work batch"
                    );
                }
            }
        }
    }

    async fn poll_and_dispatch(&mut self) {
        let span = tracing::debug_span!("FnConsumerManager::poll_and_dispatch");
        let _guard = span.enter();

        self.process_completions();
        self.warn_expired();
        let mut remaining_capacity = self.compute_remaining_capacity();
        if remaining_capacity == 0 {
            tracing::debug!("fn_consumer at capacity, skipping poll");
            return;
        }
        let limit = self.context.get_work_batch_size;
        let resp = match self
            .work_queue_client
            .get_work_with_failure_limit(
                self.context.my_member_id.clone(),
                limit,
                self.context.max_failure_count,
            )
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                tracing::error!("GetWork failed: {}", e);
                return;
            }
        };
        // Collect valid work items first
        let mut work_items = Vec::new();
        for item in resp.items {
            let Ok(fn_id) = item.fn_id.parse::<AttachedFunctionUuid>() else {
                tracing::error!(fn_id = item.fn_id, "skipping work item: invalid fn_id");
                continue;
            };
            let Ok(input_coll_id) = item.input_coll_id.parse::<CollectionUuid>() else {
                tracing::error!(
                    input_coll_id = item.input_coll_id,
                    "skipping work item: invalid input_coll_id"
                );
                continue;
            };
            let Some(compaction_offset) = item.compaction_offset else {
                tracing::error!(
                    fn_id = %fn_id,
                    input_coll_id = %input_coll_id,
                    completion_offset = item.completion_offset,
                    "skipping work item: missing required compaction_offset"
                );
                continue;
            };

            work_items.push((fn_id, input_coll_id, compaction_offset));
        }

        let mut grouped_work_items: HashMap<AttachedFunctionUuid, Vec<FunctionExecutionInput>> =
            HashMap::new();
        for (fn_id, input_coll_id, compaction_offset) in work_items {
            grouped_work_items
                .entry(fn_id)
                .or_default()
                .push(FunctionExecutionInput {
                    collection_id: input_coll_id,
                    queue_compaction_offset: compaction_offset,
                });
        }

        let mut batches_to_process = Vec::new();
        for (fn_id, items) in grouped_work_items {
            if remaining_capacity == 0 {
                break;
            }

            if self.fn_in_progress(fn_id) {
                tracing::debug!(fn_id = %fn_id, "skipping batch: function already in progress");
                continue;
            }

            // TODO(tanujnay112): Cap how many input collections a single function
            // execution can process at once instead of only relying on
            // get_work_batch_size to indirectly bound this batch.
            if !items.is_empty() {
                self.in_progress
                    .insert(fn_id, InProgressFn::new(self.context.job_expiry_seconds));
                batches_to_process.push((fn_id, items));
                remaining_capacity -= 1;
            }
        }

        for (fn_id, batch) in batches_to_process {
            let task = FnDispatchTask {
                fn_id,
                future: Box::pin(Self::dispatch_batch(
                    self.context.clone(),
                    self.work_queue_client.clone(),
                    fn_id,
                    batch.clone(),
                )),
                work_queue_client: Some(self.work_queue_client.clone()),
                batch,
            };
            if let Err(e) = self.dispatch_awaiter_channel.send(task).await {
                self.in_progress.remove(&fn_id);
                tracing::error!(
                    fn_id = %fn_id,
                    error = ?e,
                    "Failed to enqueue function dispatch task"
                );
            }
        }
    }
}

async fn report_batch_failure(
    work_queue_client: &mut WorkQueueClient,
    fn_id: AttachedFunctionUuid,
    batch: &[FunctionExecutionInput],
) {
    for item in batch {
        if let Err(report_error) = work_queue_client
            .fail_function(fn_id.to_string(), item.collection_id.to_string())
            .await
        {
            tracing::error!(
                fn_id = %fn_id,
                input_coll_id = %item.collection_id,
                error = %report_error,
                "Failed to report attached function execution failure"
            );
        }
    }
}

async fn defer_batch(
    work_queue_client: &mut WorkQueueClient,
    fn_id: AttachedFunctionUuid,
    batch: &[FunctionExecutionInput],
) {
    for item in batch {
        if let Err(defer_error) = work_queue_client
            .defer_work(fn_id.to_string(), item.collection_id.to_string())
            .await
        {
            tracing::warn!(
                fn_id = %fn_id,
                input_coll_id = %item.collection_id,
                error = %defer_error,
                "Failed to defer attached function work"
            );
        }
    }
}

fn panic_message(panic_payload: &(dyn Any + Send)) -> String {
    if let Some(message) = panic_payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic_payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

async fn fn_dispatch_awaiter_loop(
    mut task_rx: mpsc::Receiver<FnDispatchTask>,
    completion_tx: mpsc::UnboundedSender<FnDispatchCompletion>,
) {
    let mut futures = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            Some(completion) = futures.next() => {
                if completion_tx.send(completion).is_err() {
                    tracing::error!("Failed to record function dispatch result");
                }
            }
            Some(task) = task_rx.recv() => {
                futures.push(async move {
                    let FnDispatchTask {
                        fn_id,
                        future,
                        mut work_queue_client,
                        batch,
                    } = task;
                    let result = AssertUnwindSafe(future).catch_unwind().await;
                    let result = match result {
                        Ok(result) => result,
                        Err(panic_payload) => {
                            tracing::error!(
                                fn_id = %fn_id,
                                panic = %panic_message(&*panic_payload),
                                "Function consumer dispatch task panicked"
                            );
                            if let Some(work_queue_client) = work_queue_client.as_mut() {
                                report_batch_failure(work_queue_client, fn_id, &batch).await;
                            }
                            Err(DispatchError::DispatchPanicked)
                        }
                    };
                    FnDispatchCompletion {
                        fn_id,
                        batch_size: batch.len(),
                        result,
                    }
                });
            }
            else => break,
        }
    }
}

impl Drop for FnConsumerManager {
    fn drop(&mut self) {
        self.dispatch_awaiter.abort();
    }
}

#[derive(Clone, Debug)]
pub struct ScheduledPollMessage;

#[async_trait]
impl Component for FnConsumerManager {
    fn get_name() -> &'static str {
        "Fn consumer manager"
    }

    fn queue_size(&self) -> usize {
        1000
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        tracing::info!("Starting FnConsumerManager");
        ctx.scheduler.schedule(
            ScheduledPollMessage,
            self.context.poll_interval,
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled fn-consumer poll")),
        );
    }
}

#[async_trait]
impl Handler<ScheduledPollMessage> for FnConsumerManager {
    type Result = ();

    async fn handle(&mut self, _: ScheduledPollMessage, ctx: &ComponentContext<Self>) {
        Box::pin(self.poll_and_dispatch()).await;
        ctx.scheduler.schedule(
            ScheduledPollMessage,
            self.context.poll_interval,
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled fn-consumer poll")),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn dispatch_awaiter_completes_later_tasks_while_one_is_running() {
        let (task_tx, task_rx) = mpsc::channel(2);
        let (completion_tx, mut completion_rx) = mpsc::unbounded_channel();
        let awaiter = tokio::spawn(fn_dispatch_awaiter_loop(task_rx, completion_tx));
        let slow_fn_id = AttachedFunctionUuid::new();
        let fast_fn_id = AttachedFunctionUuid::new();
        let (slow_started_tx, slow_started_rx) = oneshot::channel();
        let (release_slow_tx, release_slow_rx) = oneshot::channel();

        task_tx
            .send(FnDispatchTask {
                fn_id: slow_fn_id,
                future: Box::pin(async move {
                    let _ = slow_started_tx.send(());
                    let _ = release_slow_rx.await;
                    Ok(FnDispatchOutcome::Completed)
                }),
                work_queue_client: None,
                batch: Vec::new(),
            })
            .await
            .unwrap();
        slow_started_rx.await.unwrap();

        task_tx
            .send(FnDispatchTask {
                fn_id: fast_fn_id,
                future: Box::pin(async { Ok(FnDispatchOutcome::Completed) }),
                work_queue_client: None,
                batch: Vec::new(),
            })
            .await
            .unwrap();

        let completion = timeout(Duration::from_secs(1), completion_rx.recv())
            .await
            .expect("fast task should complete while slow task is running")
            .expect("completion channel should remain open");
        assert_eq!(completion.fn_id, fast_fn_id);
        completion
            .result
            .expect("fast task should complete successfully");

        release_slow_tx.send(()).unwrap();
        let completion = timeout(Duration::from_secs(1), completion_rx.recv())
            .await
            .expect("slow task should complete after release")
            .expect("completion channel should remain open");
        assert_eq!(completion.fn_id, slow_fn_id);
        completion
            .result
            .expect("slow task should complete successfully");

        drop(task_tx);
        awaiter.await.unwrap();
    }

    #[tokio::test]
    async fn dispatch_awaiter_completes_panicked_tasks() {
        let (task_tx, task_rx) = mpsc::channel(1);
        let (completion_tx, mut completion_rx) = mpsc::unbounded_channel();
        let awaiter = tokio::spawn(fn_dispatch_awaiter_loop(task_rx, completion_tx));
        let fn_id = AttachedFunctionUuid::new();

        task_tx
            .send(FnDispatchTask {
                fn_id,
                future: Box::pin(async { panic!("expected test panic") }),
                work_queue_client: None,
                batch: Vec::new(),
            })
            .await
            .unwrap();

        let completion = timeout(Duration::from_secs(1), completion_rx.recv())
            .await
            .expect("panicked task should complete")
            .expect("completion channel should remain open");
        assert_eq!(completion.fn_id, fn_id);
        assert!(matches!(
            completion.result,
            Err(DispatchError::DispatchPanicked)
        ));

        drop(task_tx);
        awaiter.await.unwrap();
    }

    #[test]
    fn formats_panic_payloads_for_logging() {
        assert_eq!(panic_message(&"panic message"), "panic message");
        assert_eq!(panic_message(&"panic message".to_owned()), "panic message");
        assert_eq!(panic_message(&42_u32), "non-string panic payload");
    }
}
