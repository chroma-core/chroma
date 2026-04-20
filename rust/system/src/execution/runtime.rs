use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::execution::affinity::pin_current_thread;
use crate::execution::config::DispatcherConfig;

/// Build the `chroma-main` tokio runtime pinned to the cores not reserved
/// for dedicated CPU workers: `[cpu .. total_cores)`.
pub fn build_tokio_main_runtime(config: &DispatcherConfig) -> std::io::Result<Runtime> {
    let total_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let cpu = config.cpu_affinity_num_cores.unwrap_or(0).min(total_cores);
    let io = total_cores - cpu;

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    builder.thread_name("chroma-main");

    if io > 0 && cpu > 0 {
        builder.worker_threads(io);
        let thread_index = Arc::new(AtomicU64::new(0));
        builder.on_thread_start(move || {
            let idx = thread_index.fetch_add(1, Ordering::Relaxed) as usize;
            let core = cpu + (idx % io);
            if !pin_current_thread(core) {
                tracing::warn!(core_id = core, "failed to pin main runtime thread");
            }
        });
    }

    builder.build()
}
