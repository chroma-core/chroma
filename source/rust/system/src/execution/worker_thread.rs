use super::{dispatcher::TaskRequestMessage, operator::TaskMessage};
use crate::execution::affinity::{cpu_core_for_worker, pin_current_thread};
use crate::{
    utils::duration_ms, Component, ComponentContext, ComponentRuntime, Handler, ReceiverForMessage,
};
use async_trait::async_trait;
use std::fmt::{Debug, Formatter, Result};
use std::sync::LazyLock;

/// A worker thread is responsible for executing tasks
/// It sends requests to the dispatcher for new tasks.
/// # Implementation notes
/// - The actor loop will block until work is available
pub(super) struct WorkerThread {
    dispatcher: Box<dyn ReceiverForMessage<TaskRequestMessage>>,
    queue_size: usize,
    worker_id: usize,
    cpu_affinity_num_cores: Option<usize>,
    worker_id_kv: opentelemetry::KeyValue,
}

struct WorkerMetrics {
    request_total: opentelemetry::metrics::Counter<u64>,
    task_run_total: opentelemetry::metrics::Counter<u64>,
    dispatcher_send_fail_total: opentelemetry::metrics::Counter<u64>,
    task_run_latency_ms: opentelemetry::metrics::Histogram<f64>,
}

impl WorkerMetrics {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("chroma.system");
        Self {
            request_total: meter
                .u64_counter("chroma.system.worker.request_total")
                .with_description("Task request messages sent by workers")
                .build(),
            task_run_total: meter
                .u64_counter("chroma.system.worker.task_run_total")
                .with_description("Tasks run by workers")
                .build(),
            dispatcher_send_fail_total: meter
                .u64_counter("chroma.system.worker.dispatcher_send_fail_total")
                .with_description("Worker failures sending back to dispatcher")
                .build(),
            task_run_latency_ms: meter
                .f64_histogram("chroma.system.worker.task_run_latency_ms")
                .with_description("Task run latency on worker threads")
                .build(),
        }
    }
}

static WORKER_METRICS: LazyLock<WorkerMetrics> = LazyLock::new(WorkerMetrics::new);

impl WorkerThread {
    pub(super) fn new(
        dispatcher: Box<dyn ReceiverForMessage<TaskRequestMessage>>,
        queue_size: usize,
        worker_id: usize,
        cpu_affinity_num_cores: Option<usize>,
    ) -> WorkerThread {
        WorkerThread {
            dispatcher,
            queue_size,
            worker_id,
            cpu_affinity_num_cores,
            worker_id_kv: opentelemetry::KeyValue::new("worker_id", worker_id as i64),
        }
    }
}

impl Debug for WorkerThread {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("WorkerThread").finish()
    }
}

#[async_trait]
impl Component for WorkerThread {
    fn get_name() -> &'static str {
        "Worker thread"
    }

    fn queue_size(&self) -> usize {
        self.queue_size
    }

    fn runtime() -> ComponentRuntime {
        ComponentRuntime::Dedicated
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        if let Some(affinity_count) = self.cpu_affinity_num_cores {
            let total_cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            if let Some(core) = cpu_core_for_worker(self.worker_id, affinity_count, total_cores) {
                if !pin_current_thread(core) {
                    tracing::warn!(
                        worker_id = self.worker_id,
                        core_id = core,
                        "failed to pin worker thread"
                    );
                }
            }
        }
        WORKER_METRICS
            .request_total
            .add(1, std::slice::from_ref(&self.worker_id_kv));
        let req = TaskRequestMessage::new(ctx.receiver(), self.worker_id);
        if self.dispatcher.send(req, None).await.is_err() {
            WORKER_METRICS
                .dispatcher_send_fail_total
                .add(1, std::slice::from_ref(&self.worker_id_kv));
        }
    }
}

#[async_trait]
impl Handler<TaskMessage> for WorkerThread {
    type Result = ();

    async fn handle(&mut self, mut task: TaskMessage, ctx: &ComponentContext<WorkerThread>) {
        let started = std::time::Instant::now();
        task.run().await;
        let result_attrs = [
            self.worker_id_kv.clone(),
            opentelemetry::KeyValue::new("result", "ok"),
        ];
        WORKER_METRICS.task_run_total.add(1, &result_attrs);
        WORKER_METRICS
            .task_run_latency_ms
            .record(duration_ms(started.elapsed()), &result_attrs);

        WORKER_METRICS
            .request_total
            .add(1, std::slice::from_ref(&self.worker_id_kv));
        let req: TaskRequestMessage = TaskRequestMessage::new(ctx.receiver(), self.worker_id);
        if self.dispatcher.send(req, None).await.is_err() {
            WORKER_METRICS
                .dispatcher_send_fail_total
                .add(1, std::slice::from_ref(&self.worker_id_kv));
        }
    }
}
