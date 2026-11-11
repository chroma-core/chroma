//! Preserve active compaction work in stdout even when an OOM prevents span export.

mod layer;
mod memory;
#[cfg(test)]
mod tests;

use serde::Serialize;
use std::{
    collections::BTreeMap,
    io::Write,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub use layer::CompactionDiagnostics;

#[derive(Clone, Serialize)]
struct Work {
    collection_id: String,
    job_attempt_id: String,
    stage: String,
    segment: Option<String>,
    started_unix_ms: u64,
    #[serde(skip)]
    started: Instant,
    #[serde(flatten)]
    counters: BTreeMap<String, u64>,
}

struct State {
    process_id: String,
    pod: String,
    work: Mutex<BTreeMap<u64, Work>>,
    emit: Arc<dyn Fn(serde_json::Value) + Send + Sync>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl State {
    fn emit(&self, event: &str, data: serde_json::Value) {
        (self.emit)(serde_json::json!({
            "event": event, "timestamp_unix_ms": now_ms(),
            "pod": self.pod, "process_instance_id": self.process_id,
            "pid": std::process::id(), "data": data,
        }));
    }
}

impl CompactionDiagnostics {
    pub fn new() -> Self {
        Self::with_emitter(Arc::new(|value| {
            // Avoid tracing here: this callback also runs inside subscriber hooks.
            let _ = writeln!(std::io::stdout().lock(), "{value}");
        }))
    }

    fn with_emitter(emit: Arc<dyn Fn(serde_json::Value) + Send + Sync>) -> Self {
        Self {
            allocator_stats: || serde_json::Value::Null,
            state: Arc::new(State {
                process_id: uuid::Uuid::new_v4().to_string(),
                pod: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
                work: Mutex::new(BTreeMap::new()),
                emit,
            }),
        }
    }

    /// Supply allocator statistics from the worker without linking an allocator into this shared crate.
    pub fn with_allocator_stats(mut self, reader: fn() -> serde_json::Value) -> Self {
        self.allocator_stats = reader;
        self
    }

    pub fn snapshot(&self) {
        let work: Vec<_> = self.state.work.lock().unwrap().values().cloned().collect();
        let mut memory = memory::read();
        memory["jemalloc"] = (self.allocator_stats)();
        self.state.emit(
            "compaction_memory_snapshot",
            serde_json::json!({
                "memory": memory, "active_stages": work,
            }),
        );
    }

    pub async fn report(self, seconds: u64) {
        let mut interval = tokio::time::interval(Duration::from_secs(seconds.max(1)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            self.snapshot();
        }
    }
}

impl Default for CompactionDiagnostics {
    fn default() -> Self {
        Self::new()
    }
}
