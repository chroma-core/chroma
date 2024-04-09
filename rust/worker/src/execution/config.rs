use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct DispatcherConfig {
    pub(crate) num_worker_threads: usize,
    pub(crate) dispatcher_queue_size: usize,
    pub(crate) worker_queue_size: usize,
}
