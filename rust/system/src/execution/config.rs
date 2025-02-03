use serde::Deserialize;

#[derive(Clone, Deserialize, Debug)]
pub struct DispatcherConfig {
    /// The number of worker threads to use
    pub num_worker_threads: usize,
    /// The maximum number of tasks that can be enqueued.
    #[serde(default = "default_task_queue_limit")]
    pub task_queue_limit: usize,
    /// The number of tasks enqueued.
    pub dispatcher_queue_size: usize,
    /// The size of the worker components queue.
    pub worker_queue_size: usize,
    /// The number of active I/O tasks.
    #[serde(default = "default_active_io_tasks")]
    pub active_io_tasks: usize,
}

fn default_task_queue_limit() -> usize {
    1000
}

fn default_active_io_tasks() -> usize {
    1000
}
