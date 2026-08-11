use serde::{Deserialize, Serialize};

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
    /// The size of the worker components queue.
    #[serde(default = "DispatcherConfig::default_worker_queue_size")]
    pub worker_queue_size: usize,
    /// The number of active I/O tasks.
    #[serde(default = "DispatcherConfig::default_active_io_tasks")]
    pub active_io_tasks: usize,

    // NOTE(rescrv):  The next two fields are for cpu and I/O affinity.
    //
    // If cpu_affinity_num_cores + io_affinity_num_cores <= |CPUs| you'll get one OR I/O task per
    // core.  If cpu_affinity_num_cores + io_affinity_num_cores > |CPUs| you'll get the excess
    // threads scheduled in a way that balances I/O and CPU threads.
    //
    // Put another way:
    // - CPU fills in from the left
    // - I/O fills in from the right
    // - If you reach the end, you go back to where you started.
    /// Number of CPU cores used for worker-thread pinning.
    /// Worker threads are pinned as 0, 1, 2, ... and wrap at this count.
    /// If unset, CPU worker pinning is disabled.
    #[serde(default, alias = "cpu_affinity_max_core")]
    pub cpu_affinity_num_cores: Option<usize>,
    /// Number of IO cores used for IO task pinning.
    /// IO tasks are pinned as N-1, N-2, ... 0 and then wrap.
    /// If unset, IO task pinning is disabled.
    #[serde(default, alias = "io_affinity_start_core")]
    pub io_affinity_num_cores: Option<usize>,
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

    fn default_worker_queue_size() -> usize {
        100
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
            worker_queue_size: DispatcherConfig::default_worker_queue_size(),
            active_io_tasks: DispatcherConfig::default_active_io_tasks(),
            cpu_affinity_num_cores: None,
            io_affinity_num_cores: None,
        }
    }
}
