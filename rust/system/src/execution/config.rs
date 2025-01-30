use serde::Deserialize;

#[derive(Clone, Deserialize, Debug)]
pub struct DispatcherConfig {
    pub num_worker_threads: usize,
    pub task_queue_limit: usize,
    pub dispatcher_queue_size: usize,
    pub worker_queue_size: usize,
}
