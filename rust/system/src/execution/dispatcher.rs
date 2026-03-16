use super::operator::OperatorType;
use super::{operator::TaskMessage, worker_thread::WorkerThread};
use crate::execution::affinity::{io_core_for_task, pin_current_thread};
use crate::execution::config::DispatcherConfig;
use crate::utils::duration_ms;
use crate::{
    Component, ComponentContext, ComponentHandle, ConsumeJoinHandleError, Handler,
    ReceiverForMessage, System,
};
use async_trait::async_trait;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::runtime::Runtime;
use tracing::{trace_span, Instrument, Span};

/// The dispatcher is responsible for distributing tasks to worker threads.
/// It is a component that receives tasks and distributes them to worker threads.
/**
```plaintext
                            ┌─────────────────────────────────────────┐
                            │                                         │
                            │                                         │
                            │                                         │
    TaskMessage ───────────►├─────┐          Dispatcher               │
                            │     ▼                                   │
                            │    ┌┬───────────────────────────────┐   │
                            │    │┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼│   │
                            └────┴──────────────┴─────────────────┴───┘
                                                ▲
                                                │     │
                                                │     │
                           TaskRequestMessage   │     │  TaskMessage
                                                │     │
                                                │     │
                                                      ▼
                     ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
                     │                │   │                │   │                │
                     │                │   │                │   │                │
                     │                │   │                │   │                │
                     │     Worker     │   │     Worker     │   │     Worker     │
                     │                │   │                │   │                │
                     │                │   │                │   │                │
                     │                │   │                │   │                │
                     └────────────────┘   └────────────────┘   └────────────────┘
```
## Implementation notes
- The dispatcher has a queue of tasks that it distributes to worker threads
- A worker thread sends a TaskRequestMessage to the dispatcher when it is ready for a new task
- If no task is available for the worker thread, the dispatcher will place that worker's reciever
  in a queue and send a task to the worker when it recieves another one
- The reason to introduce this abstraction is to allow us to control fairness and dynamically adjust
  system utilization. It also makes mechanisms like pausing/stopping work easier.
  It would have likely been more performant to use the Tokio MT runtime, but we chose to use
  this abstraction to grant us flexibility. We can always switch to Tokio MT later if we need to,
  or make this dispatcher much more performant through implementing memory-awareness, task-batches,
  coarser work-stealing, and other optimizations.
*/
pub struct Dispatcher {
    config: DispatcherConfig,
    task_queue: VecDeque<(TaskMessage, Span)>,
    waiters: Vec<TaskRequestMessage>,
    active_io_tasks: Arc<AtomicU64>,
    io_runtime: Arc<Runtime>,
    worker_handles: Arc<Mutex<Vec<ComponentHandle<WorkerThread>>>>,
    metrics: DispatcherMetrics,
}

impl Debug for Dispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dispatcher")
            .field("config", &self.config)
            .finish()
    }
}

#[derive(Debug, Clone)]
struct DispatcherMetrics {
    task_queue_depth: opentelemetry::metrics::Histogram<u64>,
    waiter_depth: opentelemetry::metrics::Histogram<u64>,
    active_io_slots: opentelemetry::metrics::Histogram<u64>,
    task_enqueued_total: opentelemetry::metrics::Counter<u64>,
    task_dispatched_total: opentelemetry::metrics::Counter<u64>,
    task_abort_total: opentelemetry::metrics::Counter<u64>,
    worker_request_total: opentelemetry::metrics::Counter<u64>,
    worker_send_fail_total: opentelemetry::metrics::Counter<u64>,
    queue_latency_ms: opentelemetry::metrics::Histogram<f64>,
}

impl DispatcherMetrics {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("chroma.system");
        DispatcherMetrics {
            task_queue_depth: meter
                .u64_histogram("chroma.system.dispatcher.task_queue_depth")
                .with_description("Dispatcher task queue depth")
                .build(),
            waiter_depth: meter
                .u64_histogram("chroma.system.dispatcher.waiter_depth")
                .with_description("Dispatcher waiter queue depth")
                .build(),
            active_io_slots: meter
                .u64_histogram("chroma.system.dispatcher.active_io_slots")
                .with_description("Active IO task slots in use")
                .build(),
            task_enqueued_total: meter
                .u64_counter("chroma.system.dispatcher.task_enqueued_total")
                .with_description("Tasks enqueued by dispatcher")
                .build(),
            task_dispatched_total: meter
                .u64_counter("chroma.system.dispatcher.task_dispatched_total")
                .with_description("Tasks dispatched by dispatcher")
                .build(),
            task_abort_total: meter
                .u64_counter("chroma.system.dispatcher.task_abort_total")
                .with_description("Tasks aborted by dispatcher with reason")
                .build(),
            worker_request_total: meter
                .u64_counter("chroma.system.dispatcher.worker_request_total")
                .with_description("Worker requests handled by dispatcher")
                .build(),
            worker_send_fail_total: meter
                .u64_counter("chroma.system.dispatcher.worker_send_fail_total")
                .with_description("Failures while sending tasks to workers")
                .build(),
            queue_latency_ms: meter
                .f64_histogram("chroma.system.dispatcher.queue_latency_ms")
                .with_description("Dispatcher queue latency in milliseconds")
                .build(),
        }
    }
}

fn task_attrs(task_type: &'static str, operator: &'static str) -> [opentelemetry::KeyValue; 2] {
    [
        opentelemetry::KeyValue::new("task_type", task_type),
        opentelemetry::KeyValue::new("operator", operator),
    ]
}

fn task_attrs_with(
    task_type: &'static str,
    operator: &'static str,
    key: &'static str,
    value: &'static str,
) -> [opentelemetry::KeyValue; 3] {
    [
        opentelemetry::KeyValue::new("task_type", task_type),
        opentelemetry::KeyValue::new("operator", operator),
        opentelemetry::KeyValue::new(key, value),
    ]
}

/// Point-in-time snapshot of dispatcher queue depths and worker counts.
#[derive(Debug, Clone, Copy)]
pub struct DispatcherStats {
    /// Number of tasks waiting in the dispatcher's queue.
    pub task_queue_depth: usize,
    /// Number of idle workers waiting for tasks.
    pub waiter_depth: usize,
    /// Remaining IO concurrency slots.
    pub active_io_slots: u64,
    /// Total number of worker threads.
    pub num_workers: usize,
}

impl Dispatcher {
    /// Create a new dispatcher from a configuration.
    pub fn new(config: DispatcherConfig) -> Self {
        let io_runtime = Self::build_io_runtime(&config);
        Dispatcher {
            config: config.clone(),
            task_queue: VecDeque::new(),
            waiters: Vec::new(),
            active_io_tasks: Arc::new(AtomicU64::new(config.active_io_tasks as u64)),
            io_runtime: Arc::new(io_runtime),
            worker_handles: Arc::new(Mutex::new(Vec::new())),
            metrics: DispatcherMetrics::new(),
        }
    }

    /// Build a dedicated tokio runtime for IO tasks.
    ///
    /// When `io_affinity_num_cores` is set, each runtime thread is pinned to a
    /// core on startup via `on_thread_start`, filling from the right
    /// (total - 1, total - 2, ...) and wrapping after `affinity_count` slots.
    fn build_io_runtime(config: &DispatcherConfig) -> Runtime {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();
        builder.thread_name("chroma-io");
        if let Some(affinity_count) = config.io_affinity_num_cores {
            let total_cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            let thread_index = Arc::new(AtomicU64::new(0));
            builder.on_thread_start(move || {
                let idx = thread_index.fetch_add(1, Ordering::Relaxed) as usize;
                if let Some(core) = io_core_for_task(idx, affinity_count, total_cores) {
                    if !pin_current_thread(core) {
                        tracing::warn!(core_id = core, "failed to pin IO runtime thread");
                    }
                }
            });
        }
        builder.build().expect("IO tokio runtime should build")
    }

    /// Spawn worker threads
    /// # Parameters
    /// - system: The system to spawn the worker threads in
    /// - self_receiver: The receiver to send tasks to the worker threads, this is a address back to the dispatcher
    fn spawn_workers(
        &self,
        system: &mut System,
        self_receiver: Box<dyn ReceiverForMessage<TaskRequestMessage>>,
    ) {
        let mut worker_handles = self.worker_handles.lock();
        for worker_id in 0..self.config.num_worker_threads {
            let worker = WorkerThread::new(
                self_receiver.clone(),
                self.config.worker_queue_size,
                worker_id,
                self.config.cpu_affinity_num_cores,
            );
            worker_handles.push(system.start_component(worker));
        }
    }

    /// Return a point-in-time snapshot of the dispatcher's internal state.
    pub fn stats(&self) -> DispatcherStats {
        DispatcherStats {
            task_queue_depth: self.task_queue.len(),
            waiter_depth: self.waiters.len(),
            active_io_slots: self.active_io_tasks.load(Ordering::Relaxed),
            num_workers: self.worker_handles.lock().len(),
        }
    }

    fn record_depths(&self) {
        self.metrics
            .task_queue_depth
            .record(self.task_queue.len() as u64, &[]);
        self.metrics
            .waiter_depth
            .record(self.waiters.len() as u64, &[]);
        self.metrics.active_io_slots.record(
            self.config.active_io_tasks as u64 - self.active_io_tasks.load(Ordering::Relaxed),
            &[],
        );
    }

    /// Enqueue a task to be processed.
    async fn enqueue_task(&mut self, mut task: TaskMessage) {
        let task_type = task.get_type().as_str();
        let operator = task.get_name();
        let task_kv = task_attrs(task_type, operator);
        let task_created_at = task.created_at();
        self.metrics.task_enqueued_total.add(1, &task_kv);
        match task.get_type() {
            OperatorType::IO => {
                let child_span = trace_span!(parent: Span::current(), "IO task execution", name = task.get_name(), task_type = "io");
                // This spin loop:
                // - reads a witness from active_io_tasks.
                // - aborts the task if witness is zero.
                // - tries to decrement the value to one less than the witness.
                // - if the decrement fails, it retries;  this loads a new witness and tries again.
                // - if the decrement succeeds, it spawns the task.
                // This is conceptually what a semaphore is doing, except that it bails if
                // acquisition fails rather than blocking.
                let mut witness = self.active_io_tasks.load(Ordering::Relaxed);
                loop {
                    if witness == 0 {
                        task.abort().await;
                        self.metrics.task_abort_total.add(
                            1,
                            &task_attrs_with(task_type, operator, "reason", "io_limit"),
                        );
                        self.record_depths();
                        return;
                    }
                    match self.active_io_tasks.compare_exchange(
                        witness,
                        witness - 1,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(new_witness) => {
                            witness = new_witness;
                        }
                    }
                }
                let counter = Arc::clone(&self.active_io_tasks);
                let counter = IncrementOnDrop(counter);
                self.metrics.task_dispatched_total.add(
                    1,
                    &task_attrs_with(task_type, operator, "dispatch", "direct"),
                );
                self.metrics
                    .queue_latency_ms
                    .record(duration_ms(task_created_at.elapsed()), &task_kv);
                self.record_depths();
                self.io_runtime.spawn(async move {
                    task.run().instrument(child_span).await;
                    drop(counter);
                });
            }
            OperatorType::Other => {
                // If a worker is waiting for a task, send it to the worker in FIFO order.
                // Otherwise, add it to the task queue.
                let span = trace_span!(parent: Span::current(), "Other task execution", name = task.get_name(), task_type = "other");
                match self.waiters.pop() {
                    Some(channel) => match channel.reply_to.send(task, Some(span)).await {
                        Ok(_) => {
                            self.metrics.task_dispatched_total.add(
                                1,
                                &task_attrs_with(task_type, operator, "dispatch", "direct"),
                            );
                            self.metrics
                                .queue_latency_ms
                                .record(duration_ms(task_created_at.elapsed()), &task_kv);
                        }
                        Err(e) => {
                            tracing::error!("Error sending task to worker: {:?}", e);
                            self.metrics.worker_send_fail_total.add(
                                1,
                                &[opentelemetry::KeyValue::new(
                                    "worker_id",
                                    channel.worker_id as i64,
                                )],
                            );
                        }
                    },
                    None => {
                        if self.task_queue.len() >= self.config.task_queue_limit {
                            task.abort().await;
                            self.metrics.task_abort_total.add(
                                1,
                                &task_attrs_with(task_type, operator, "reason", "queue_limit"),
                            );
                        } else {
                            self.task_queue.push_back((task, span));
                        }
                    }
                }
                self.record_depths();
            }
        }
    }

    /// Handle a work request from a worker thread.
    /// If no work is available, the worker will be placed in a queue and a task will be sent to
    /// it when one is available.
    async fn handle_work_request(&mut self, request: TaskRequestMessage) {
        self.metrics.worker_request_total.add(1, &[]);
        match self.task_queue.pop_front() {
            Some((task, span)) => {
                let task_type = task.get_type().as_str();
                let operator = task.get_name();
                let task_kv = task_attrs(task_type, operator);
                let queue_latency = duration_ms(task.created_at().elapsed());
                match request.reply_to.send(task, Some(span)).await {
                    Ok(_) => {
                        self.metrics.task_dispatched_total.add(
                            1,
                            &task_attrs_with(task_type, operator, "dispatch", "queued"),
                        );
                        self.metrics
                            .queue_latency_ms
                            .record(queue_latency, &task_kv);
                    }
                    Err(e) => {
                        tracing::error!("Error sending task to worker: {:?}", e);
                        self.metrics.worker_send_fail_total.add(
                            1,
                            &[opentelemetry::KeyValue::new(
                                "worker_id",
                                request.worker_id as i64,
                            )],
                        );
                    }
                }
            }
            None => {
                self.waiters.push(request);
            }
        }
        self.record_depths();
    }
}

#[async_trait]
impl Configurable<DispatcherConfig> for Dispatcher {
    async fn try_from_config(
        config: &DispatcherConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        Ok(Dispatcher::new(config.clone()))
    }
}

/// A message that a worker thread sends to the dispatcher to request a task
/// # Members
/// - reply_to: The receiver to send the task to, this is the worker thread
#[derive(Debug)]
pub(super) struct TaskRequestMessage {
    reply_to: Box<dyn ReceiverForMessage<TaskMessage>>,
    worker_id: usize,
}

impl TaskRequestMessage {
    /// Create a new TaskRequestMessage
    /// # Parameters
    /// - reply_to: The receiver to send the task to, this is the worker thread
    ///   that is requesting the task
    pub(super) fn new(
        reply_to: Box<dyn ReceiverForMessage<TaskMessage>>,
        worker_id: usize,
    ) -> Self {
        TaskRequestMessage {
            reply_to,
            worker_id,
        }
    }
}

#[derive(Debug, Error)]
enum DispatcherStopError {
    #[error("Failed to stop worker thread: {0}")]
    JoinError(ConsumeJoinHandleError),
}

impl ChromaError for DispatcherStopError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            DispatcherStopError::JoinError(_) => chroma_error::ErrorCodes::Internal,
        }
    }
}

// ============= Component implementation =============

#[async_trait]
impl Component for Dispatcher {
    fn get_name() -> &'static str {
        "Dispatcher"
    }

    fn queue_size(&self) -> usize {
        self.config.dispatcher_queue_size
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        self.spawn_workers(&mut ctx.system.clone(), ctx.receiver());
    }

    async fn on_stop(&mut self) -> Result<(), Box<dyn ChromaError>> {
        let mut worker_handles = {
            let mut handles = self.worker_handles.lock();
            handles.drain(..).collect::<Vec<_>>()
        };

        for mut handle in worker_handles.drain(..) {
            handle.stop();
            handle
                .join()
                .await
                .map_err(|e| DispatcherStopError::JoinError(e).boxed())?;
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<TaskMessage> for Dispatcher {
    type Result = ();

    async fn handle(&mut self, task: TaskMessage, _ctx: &ComponentContext<Dispatcher>) {
        self.enqueue_task(task).await;
    }
}

// Worker sends a request for task
#[async_trait]
impl Handler<TaskRequestMessage> for Dispatcher {
    type Result = ();

    async fn handle(&mut self, message: TaskRequestMessage, _ctx: &ComponentContext<Dispatcher>) {
        self.handle_work_request(message).await;
    }
}

struct IncrementOnDrop(Arc<AtomicU64>);

impl Drop for IncrementOnDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use parking_lot::Mutex;
    use rand::{distributions::Alphanumeric, Rng};
    use tokio::{
        fs::File,
        io::{AsyncReadExt, AsyncWriteExt},
    };
    use uuid::Uuid;

    use super::*;
    use crate::{operator::*, ComponentHandle};
    use std::{
        collections::HashSet,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    // Create a component that will schedule DISPATCH_COUNT invocations of the MockOperator
    // on an interval of DISPATCH_FREQUENCY_MS.
    // Each invocation will sleep for MOCK_OPERATOR_SLEEP_DURATION_MS to simulate work
    // Use THREAD_COUNT worker threads
    const MOCK_OPERATOR_SLEEP_DURATION_MS: u64 = 100;
    const DISPATCH_FREQUENCY_MS: u64 = 5;
    const DISPATCH_COUNT: usize = 50;
    const THREAD_COUNT: usize = 4;

    #[derive(Debug)]
    struct MockOperator {}
    #[async_trait]
    impl Operator<f32, String> for MockOperator {
        type Error = std::io::Error;

        fn get_name(&self) -> &'static str {
            "MockOperator"
        }

        async fn run(&self, input: &f32) -> Result<String, Self::Error> {
            // sleep to simulate work
            tokio::time::sleep(tokio::time::Duration::from_millis(
                MOCK_OPERATOR_SLEEP_DURATION_MS,
            ))
            .await;
            Ok(input.to_string())
        }
    }

    #[derive(Debug)]
    struct MockIoOperator {}
    #[async_trait]
    impl Operator<String, String> for MockIoOperator {
        type Error = std::io::Error;

        fn get_name(&self) -> &'static str {
            "MockIoOperator"
        }

        async fn run(&self, input: &String) -> Result<String, Self::Error> {
            // perform some io to simulate work.
            let tmp_dir = tempfile::tempdir().unwrap();
            let file_path = tmp_dir.path().join(input);
            let mut tmp_file = File::create(file_path.clone()).await.unwrap();
            tmp_file.write_all(b"Test write").await.unwrap();
            tmp_file.flush().await.unwrap();
            let mut read_fs = File::open(file_path)
                .await
                .expect("Error opening file previously created");
            let mut buffer = [0; 10];
            read_fs.read_exact(&mut buffer[..]).await.unwrap();
            let read_value =
                String::from_utf8(buffer.to_vec()).expect("Error creating string from utf8");
            assert_eq!(read_value, String::from("Test write"));
            Ok(input.to_string())
        }

        fn get_type(&self) -> OperatorType {
            OperatorType::IO
        }
    }

    #[derive(Debug)]
    struct MockIoDispatchUser {
        pub dispatcher: ComponentHandle<Dispatcher>,
        counter: Arc<AtomicUsize>, // We expect to recieve DISPATCH_COUNT messages
        sent_tasks: Arc<Mutex<HashSet<Uuid>>>,
        received_tasks: Arc<Mutex<HashSet<Uuid>>>,
    }
    #[async_trait]
    impl Component for MockIoDispatchUser {
        fn get_name() -> &'static str {
            "Mock Io dispatcher"
        }

        fn queue_size(&self) -> usize {
            1000
        }

        async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
            // dispatch a new task every DISPATCH_FREQUENCY_MS for DISPATCH_COUNT times
            let duration = std::time::Duration::from_millis(DISPATCH_FREQUENCY_MS);
            ctx.scheduler
                .schedule_interval((), duration, Some(DISPATCH_COUNT), ctx, || None);
        }
    }
    #[async_trait]
    impl Handler<TaskResult<String, std::io::Error>> for MockIoDispatchUser {
        type Result = ();

        async fn handle(
            &mut self,
            _message: TaskResult<String, std::io::Error>,
            ctx: &ComponentContext<MockIoDispatchUser>,
        ) {
            self.counter.fetch_add(1, Ordering::SeqCst);
            let curr_count = self.counter.load(Ordering::SeqCst);
            // Cancel self
            if curr_count == DISPATCH_COUNT {
                ctx.cancellation_token.cancel();
            }
            self.received_tasks.lock().insert(_message.id());
        }
    }

    #[async_trait]
    impl Handler<()> for MockIoDispatchUser {
        type Result = ();

        async fn handle(&mut self, _message: (), ctx: &ComponentContext<MockIoDispatchUser>) {
            let rng = rand::thread_rng();
            // Generate a random filename for writing and reading.
            let filename = rng
                .sample_iter(&Alphanumeric)
                .take(5)
                .map(char::from)
                .collect();
            println!("Scheduling mock io operator with filename {}", filename);
            let task = wrap(
                Box::new(MockIoOperator {}),
                filename,
                ctx.receiver(),
                ctx.cancellation_token.clone(),
            );
            let task_id = task.id();
            self.sent_tasks.lock().insert(task_id);
            let _res = self.dispatcher.send(task, None).await;
        }
    }

    #[derive(Debug)]
    struct MockDispatchUser {
        pub dispatcher: ComponentHandle<Dispatcher>,
        counter: Arc<AtomicUsize>, // We expect to recieve DISPATCH_COUNT messages
        sent_tasks: Arc<Mutex<HashSet<Uuid>>>,
        received_tasks: Arc<Mutex<HashSet<Uuid>>>,
    }
    #[async_trait]
    impl Component for MockDispatchUser {
        fn get_name() -> &'static str {
            "Mock dispatcher"
        }

        fn queue_size(&self) -> usize {
            1000
        }

        async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
            // dispatch a new task every DISPATCH_FREQUENCY_MS for DISPATCH_COUNT times
            let duration = std::time::Duration::from_millis(DISPATCH_FREQUENCY_MS);
            ctx.scheduler
                .schedule_interval((), duration, Some(DISPATCH_COUNT), ctx, || None);
        }
    }
    #[async_trait]
    impl Handler<TaskResult<String, std::io::Error>> for MockDispatchUser {
        type Result = ();

        async fn handle(
            &mut self,
            _message: TaskResult<String, std::io::Error>,
            ctx: &ComponentContext<MockDispatchUser>,
        ) {
            self.counter.fetch_add(1, Ordering::SeqCst);
            let curr_count = self.counter.load(Ordering::SeqCst);
            // Cancel self
            if curr_count == DISPATCH_COUNT {
                ctx.cancellation_token.cancel();
            }
            self.received_tasks.lock().insert(_message.id());
        }
    }

    #[async_trait]
    impl Handler<()> for MockDispatchUser {
        type Result = ();

        async fn handle(&mut self, _message: (), ctx: &ComponentContext<MockDispatchUser>) {
            println!("Scheduling mock cpu operator with input {}", 42.0);
            let task = wrap(
                Box::new(MockOperator {}),
                42.0,
                ctx.receiver(),
                ctx.cancellation_token.clone(),
            );
            let task_id = task.id();
            self.sent_tasks.lock().insert(task_id);
            let _res = self.dispatcher.send(task, None).await;
        }
    }

    #[tokio::test]
    async fn test_dispatcher_io_tasks() {
        let system = System::new();
        let dispatcher = Dispatcher::new(DispatcherConfig {
            num_worker_threads: THREAD_COUNT,
            task_queue_limit: 1000,
            dispatcher_queue_size: 1000,
            worker_queue_size: 1000,
            active_io_tasks: 1000,
            cpu_affinity_num_cores: None,
            io_affinity_num_cores: None,
        });
        let dispatcher_handle = system.start_component(dispatcher);
        let counter = Arc::new(AtomicUsize::new(0));
        let sent_tasks = Arc::new(Mutex::new(HashSet::new()));
        let received_tasks = Arc::new(Mutex::new(HashSet::new()));
        let dispatch_user = MockIoDispatchUser {
            dispatcher: dispatcher_handle,
            counter: counter.clone(),
            sent_tasks: sent_tasks.clone(),
            received_tasks: received_tasks.clone(),
        };
        let mut dispatch_user_handle = system.start_component(dispatch_user);
        // yield to allow the component to process the messages
        tokio::task::yield_now().await;
        // Join on the dispatch user, since it will kill itself after DISPATCH_COUNT messages
        dispatch_user_handle.join().await.unwrap();
        // We should have received DISPATCH_COUNT messages
        assert_eq!(counter.load(Ordering::SeqCst), DISPATCH_COUNT);
        // The sent tasks should be equal to the received tasks
        assert_eq!(*sent_tasks.lock(), *received_tasks.lock());
        // The length of the sent/recieved tasks should be equal to the number of dispatched tasks
        assert_eq!(sent_tasks.lock().len(), DISPATCH_COUNT);
        assert_eq!(received_tasks.lock().len(), DISPATCH_COUNT);
    }

    #[tokio::test]
    async fn test_dispatcher_non_io_tasks() {
        let system = System::new();
        let dispatcher = Dispatcher::new(DispatcherConfig {
            num_worker_threads: THREAD_COUNT,
            task_queue_limit: 1000,
            dispatcher_queue_size: 1000,
            worker_queue_size: 1000,
            active_io_tasks: 1000,
            cpu_affinity_num_cores: None,
            io_affinity_num_cores: None,
        });
        let dispatcher_handle = system.start_component(dispatcher);
        let counter = Arc::new(AtomicUsize::new(0));
        let sent_tasks = Arc::new(Mutex::new(HashSet::new()));
        let received_tasks = Arc::new(Mutex::new(HashSet::new()));
        let dispatch_user = MockDispatchUser {
            dispatcher: dispatcher_handle,
            counter: counter.clone(),
            sent_tasks: sent_tasks.clone(),
            received_tasks: received_tasks.clone(),
        };
        let mut dispatch_user_handle = system.start_component(dispatch_user);
        // yield to allow the component to process the messages
        tokio::task::yield_now().await;
        // Join on the dispatch user, since it will kill itself after DISPATCH_COUNT messages
        dispatch_user_handle.join().await.unwrap();
        // We should have received DISPATCH_COUNT messages
        assert_eq!(counter.load(Ordering::SeqCst), DISPATCH_COUNT);
        // The sent tasks should be equal to the received tasks
        assert_eq!(*sent_tasks.lock(), *received_tasks.lock());
        // The length of the sent/recieved tasks should be equal to the number of dispatched tasks
        assert_eq!(sent_tasks.lock().len(), DISPATCH_COUNT);
        assert_eq!(received_tasks.lock().len(), DISPATCH_COUNT);
    }

    #[tokio::test]
    async fn test_dispatcher_non_io_tasks_reject() {
        let system = System::new();
        let dispatcher = Dispatcher::new(DispatcherConfig {
            num_worker_threads: THREAD_COUNT,
            // Must be zero to fail things.
            task_queue_limit: 0,
            dispatcher_queue_size: 1,
            worker_queue_size: 1,
            active_io_tasks: 1,
            cpu_affinity_num_cores: None,
            io_affinity_num_cores: None,
        });
        let dispatcher_handle = system.start_component(dispatcher);
        let counter = Arc::new(AtomicUsize::new(0));
        let sent_tasks = Arc::new(Mutex::new(HashSet::new()));
        let received_tasks = Arc::new(Mutex::new(HashSet::new()));
        let dispatch_user = MockDispatchUser {
            dispatcher: dispatcher_handle,
            counter: counter.clone(),
            sent_tasks: sent_tasks.clone(),
            received_tasks: received_tasks.clone(),
        };
        let dispatch_user_handle = system.start_component(dispatch_user);
        let mut is_err = false;
        for _ in 0..1000 {
            is_err |= dispatch_user_handle.request((), None).await.is_err();
        }
        assert!(is_err);
    }

    #[tokio::test]
    async fn test_dispatcher_io_tasks_reject() {
        let system = System::new();
        let dispatcher = Dispatcher::new(DispatcherConfig {
            num_worker_threads: THREAD_COUNT,
            // Must be zero to fail things.
            task_queue_limit: 0,
            dispatcher_queue_size: 1,
            worker_queue_size: 1,
            active_io_tasks: 1,
            cpu_affinity_num_cores: None,
            io_affinity_num_cores: None,
        });
        let dispatcher_handle = system.start_component(dispatcher);
        let counter = Arc::new(AtomicUsize::new(0));
        let sent_tasks = Arc::new(Mutex::new(HashSet::new()));
        let received_tasks = Arc::new(Mutex::new(HashSet::new()));
        let dispatch_user = MockIoDispatchUser {
            dispatcher: dispatcher_handle,
            counter: counter.clone(),
            sent_tasks: sent_tasks.clone(),
            received_tasks: received_tasks.clone(),
        };
        let dispatch_user_handle = system.start_component(dispatch_user);
        // yield to allow the component to process the messages
        tokio::task::yield_now().await;
        let mut is_err = false;
        for _ in 0..1000 {
            is_err |= dispatch_user_handle.request((), None).await.is_err();
        }
        assert!(is_err);
    }
}
