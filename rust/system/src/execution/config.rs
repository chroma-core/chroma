use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct DispatcherConfig {
    pub num_worker_threads: usize,
    pub dispatcher_queue_size: usize,
    pub worker_queue_size: usize,
}
