use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct DispatcherConfig {
    /// The number of worker threads to allocate for running tasks.
    #[serde(default = "DispatcherConfig::default_num_worker_threads")]
    pub num_worker_threads: usize,

    /// The maximum number of tasks that can be enqueued onto the dispatcher's
    /// internal task queue.
    #[serde(default = "DispatcherConfig::default_task_queue_limit")]
    pub task_queue_limit: usize,

    /// The maximum number of messages (TaskMessage and TaskRequestMessage)
    /// that can be queued onto the dispatcher component's channel.
    #[serde(default = "DispatcherConfig::default_dispatcher_queue_size")]
    pub dispatcher_queue_size: usize,

    /// The maximum number of messages that can be queued onto the worker
    /// threads' component channels.
    #[serde(default = "DispatcherConfig::default_worker_queue_size")]
    pub worker_queue_size: usize,

    /// The maximum number of active I/O tasks managed by the dispatcher at any
    /// given time.
    #[serde(default = "DispatcherConfig::default_active_io_tasks")]
    pub active_io_tasks: usize,
}

impl DispatcherConfig {
    fn default_num_worker_threads() -> usize {
        // Determine the number of logical CPUs available.
        let num_cpus = num_cpus::get();

        // Reserve 20% of the available logical CPUs for IO-bound tasks within
        // the dispatcher and general system overhead (other components and
        // tokio tasks besides the dispatcher).
        let reserved_cpus = (num_cpus as f64 * 0.2).floor() as usize;

        // Always allocate a minimum of 4 worker threads.
        std::cmp::max(num_cpus - reserved_cpus, 4)
    }

    fn default_task_queue_limit() -> usize {
        // Default to a large number that is unlikely to be exceeded by the
        // number of tasks that are expected to be enqueued at once. This is
        // to prevent the task queue from growing indefinitely, or filling to
        // the point where requests must be cancelled due to a full task queue.
        //
        // Setting this value to a multiple of the number of worker threads
        // is a somewhat reasonable way to construct the default value because
        // the total number of created tasks is partially related to the number
        // of worker threads, as the orchestrator will generate new tasks after
        // previous tasks are completed. Or, to phrase the intuition in another
        // way, the total number of tasks that can be completed in any given
        // amount of time is directly related to the number of worker threads
        // that are available to execute them.
        1000 * DispatcherConfig::default_num_worker_threads()
    }

    fn default_dispatcher_queue_size() -> usize {
        // The dispatcher component's channel shouldn't need to hold a large
        // amount of messages, because it immediately takes the incoming task
        // messages and either enqueues them onto its internal task queue or
        // sends them directly to a worker thread's channel.
        //
        // Therefore, we set this value to a constant default, to be able to
        // handle a burst of task messages that are enqueued before the
        // dispatcher's tokio task is able to process them.
        1000
    }

    fn default_worker_queue_size() -> usize {
        // Within the dispatcher, each worker component will only receive one
        // task message at a time and will process each task sequentially.
        // Therefore, we set this value to a constant, minimal default value.
        10
    }

    fn default_active_io_tasks() -> usize {
        // By default, we allow the dispatcher to run a relatively large number
        // of concurrent I/O tasks. This value is somewhat "experimental" and
        // is based on examining previous production workloads. It is subject
        // to change as we gather more data, and we may want to adjust how we
        // determine this default value in the future.
        //
        // The main risk with setting this value too high is that allowing too
        // many concurrent I/O tasks will potentially allow the system to hit
        // the process limit on the number of open file descriptors
        // (RLIMIT_NOFILE). Unfortunately, this is difficult to reason about
        // because it's not obvious how many file descriptors each I/O task
        // will actually open.
        //
        // The risk with setting this value too low is that the dispatcher will
        // not be able to process all of the I/O tasks that are enqueued, which
        // will result in failed tasks.
        10000
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
        }
    }
}
