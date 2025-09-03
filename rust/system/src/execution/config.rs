use chroma_config::helpers::deserialize_duration_from_milliseconds;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct DispatcherConfig {
    /// The number of worker threads to use
    #[serde(default = "DispatcherConfig::default_num_worker_threads")]
    pub num_worker_threads: usize,
    /// The maximum number of tasks that can be enqueued.
    #[serde(default = "DispatcherConfig::default_task_queue_limit")]
    pub task_queue_limit: usize,
    /// The number of tasks enqueued.
    #[serde(default = "DispatcherConfig::default_dispatcher_queue_size")]
    pub dispatcher_queue_size: usize,
    /// The timeout that the dispatcher will wait before erroring when attempting to send a message.
    #[serde(
        default = "DispatcherConfig::default_dispatcher_send_timeout",
        deserialize_with = "deserialize_duration_from_milliseconds"
    )]
    pub dispatcher_send_timeout: Duration,
    /// The size of the worker components queue.
    #[serde(default = "DispatcherConfig::default_worker_queue_size")]
    pub worker_queue_size: usize,
    /// The timeout that the worker will wait before erroring when attempting to send a message.
    #[serde(
        default = "DispatcherConfig::default_worker_send_timeout",
        deserialize_with = "deserialize_duration_from_milliseconds"
    )]
    pub worker_send_timeout: Duration,
    /// The number of active I/O tasks.
    #[serde(default = "DispatcherConfig::default_active_io_tasks")]
    pub active_io_tasks: usize,
}

impl DispatcherConfig {
    fn default_num_worker_threads() -> usize {
        5
    }

    fn default_task_queue_limit() -> usize {
        1000
    }

    fn default_dispatcher_queue_size() -> usize {
        100
    }

    fn default_dispatcher_send_timeout() -> Duration {
        Duration::from_millis(500)
    }

    fn default_worker_queue_size() -> usize {
        100
    }

    fn default_worker_send_timeout() -> Duration {
        Duration::from_millis(500)
    }

    fn default_active_io_tasks() -> usize {
        1000
    }
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        DispatcherConfig {
            num_worker_threads: DispatcherConfig::default_num_worker_threads(),
            task_queue_limit: DispatcherConfig::default_task_queue_limit(),
            dispatcher_queue_size: DispatcherConfig::default_dispatcher_queue_size(),
            dispatcher_send_timeout: DispatcherConfig::default_dispatcher_send_timeout(),
            worker_queue_size: DispatcherConfig::default_worker_queue_size(),
            worker_send_timeout: DispatcherConfig::default_worker_send_timeout(),
            active_io_tasks: DispatcherConfig::default_active_io_tasks(),
        }
    }
}
