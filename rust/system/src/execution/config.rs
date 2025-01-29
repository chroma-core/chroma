use serde::Deserialize;

#[derive(Clone, Deserialize, Debug)]
pub struct DispatcherConfig {
    pub num_worker_threads: usize,
    #[serde(default = "default_task_queue_limit")]
    pub task_queue_limit: usize,
    pub dispatcher_queue_size: usize,
    pub worker_queue_size: usize,
    pub active_io_tasks: usize,
}

fn default_task_queue_limit() -> usize {
    1000
}
