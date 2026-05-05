//! `TaskPool`: one-tokio-task batched poller for `/api/v3/getTasks`.
//!
//! Without this, every concurrent `enqueueExportBlock` worker would poll
//! `getTasks` independently every few seconds, hammering the endpoint and
//! triggering 429s with N=parallel workers. The pool batches all in-flight
//! task ids into a single `getTasks([id1, id2, ...])` call per tick, so RPS
//! is `1/poll_interval` regardless of `parallel`.

use parking_lot::Mutex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use super::client::NotionInternal;
use super::rate_limit::{default_initial_backoff_s, RateLimitedError, TokenBucket, MAX_BACKOFF_S};

#[derive(Debug, Clone)]
pub struct TaskState {
    pub state: String,
    pub status: Value,
    #[allow(dead_code)]
    pub raw: Value,
}

#[derive(Default)]
struct PoolInner {
    /// task_id -> latest known state from getTasks.
    states: HashMap<String, TaskState>,
    /// task_ids registered but not yet completed.
    waiting: HashMap<String, Arc<Notify>>,
    /// Stats.
    poll_count: u64,
    batched_count: u64,
}

#[derive(Clone)]
pub struct TaskPool {
    client: NotionInternal,
    poll_interval: Duration,
    poll_bucket: TokenBucket,
    inner: Arc<Mutex<PoolInner>>,
    /// Notifier woken whenever a new task is registered, so the poller can
    /// kick immediately instead of waiting out a full tick.
    new_task: Arc<Notify>,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// Sticky stop flag. The run loop checks this on every iteration; the
    /// `Notify` exists only to wake the loop out of any current sleep.
    /// (We can't rely on `Notify::notify_waiters` alone because it doesn't
    /// buffer past calls to `.notified()`.)
    stop_flag: Arc<AtomicBool>,
    stop_notify: Arc<Notify>,
}

impl TaskPool {
    pub fn new(client: NotionInternal, poll_interval_s: f64, poll_bucket: TokenBucket) -> Self {
        Self {
            client,
            poll_interval: Duration::from_secs_f64(poll_interval_s.max(0.5)),
            poll_bucket,
            inner: Arc::new(Mutex::new(PoolInner::default())),
            new_task: Arc::new(Notify::new()),
            handle: Arc::new(Mutex::new(None)),
            stop_flag: Arc::new(AtomicBool::new(false)),
            stop_notify: Arc::new(Notify::new()),
        }
    }

    pub fn start(&self) {
        let me = self.clone();
        let handle = tokio::spawn(async move { me.run().await });
        *self.handle.lock() = Some(handle);
    }

    pub async fn stop(&self) {
        // Set the sticky flag *before* notifying so the run loop sees it the
        // moment we wake it up. notify_one buffers a permit if the loop
        // hasn't yet entered `.notified()`, which is the safe direction.
        self.stop_flag.store(true, Ordering::SeqCst);
        self.stop_notify.notify_one();
        let h = self.handle.lock().take();
        if let Some(h) = h {
            let _ = h.await;
        }
    }

    /// Register interest in a task id. Returns a Notify that gets woken when
    /// the task transitions to `success` or `failure`. Caller should then
    /// read `status(task_id)` to get the final state.
    pub fn register(&self, task_id: &str) -> Arc<Notify> {
        let mut g = self.inner.lock();
        let n = g
            .waiting
            .entry(task_id.to_string())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone();
        drop(g);
        self.new_task.notify_one();
        n
    }

    pub fn status(&self, task_id: &str) -> Option<TaskState> {
        self.inner.lock().states.get(task_id).cloned()
    }

    pub fn poll_count(&self) -> u64 {
        self.inner.lock().poll_count
    }

    pub fn batched_count(&self) -> u64 {
        self.inner.lock().batched_count
    }

    async fn run(self) {
        let mut backoff = default_initial_backoff_s();
        loop {
            if self.stop_flag.load(Ordering::SeqCst) {
                return;
            }
            let waiting_ids: Vec<String> = {
                let g = self.inner.lock();
                g.waiting.keys().cloned().collect()
            };
            if waiting_ids.is_empty() {
                tokio::select! {
                    _ = self.new_task.notified() => continue,
                    _ = self.stop_notify.notified() => return,
                    _ = sleep(self.poll_interval) => continue,
                }
            }
            self.poll_bucket.take().await;
            self.client.gate.wait_if_open().await;
            let res = self.client.get_tasks(&waiting_ids).await;
            match res {
                Ok(results) => {
                    backoff = default_initial_backoff_s();
                    let mut to_wake: Vec<Arc<Notify>> = Vec::new();
                    {
                        let mut g = self.inner.lock();
                        g.poll_count += 1;
                        g.batched_count += waiting_ids.len() as u64;
                        for r in results {
                            let id = match r.get("id").and_then(Value::as_str) {
                                Some(s) => s.to_string(),
                                None => continue,
                            };
                            let state = r
                                .get("state")
                                .and_then(Value::as_str)
                                .unwrap_or("in_progress")
                                .to_string();
                            let status = r.get("status").cloned().unwrap_or(Value::Null);
                            let terminal = matches!(state.as_str(), "success" | "failure");
                            g.states.insert(
                                id.clone(),
                                TaskState {
                                    state: state.clone(),
                                    status,
                                    raw: r,
                                },
                            );
                            if terminal {
                                if let Some(n) = g.waiting.remove(&id) {
                                    to_wake.push(n);
                                }
                            }
                        }
                    }
                    for n in to_wake {
                        n.notify_waiters();
                    }
                }
                Err(e) => {
                    let retry_after = if let Some(rl) = e.downcast_ref::<RateLimitedError>() {
                        rl.retry_after
                    } else {
                        backoff
                    };
                    backoff = (backoff * 2.0).min(MAX_BACKOFF_S);
                    tokio::select! {
                        _ = self.stop_notify.notified() => return,
                        _ = sleep(Duration::from_secs_f64(retry_after)) => {}
                    }
                    continue;
                }
            }
            tokio::select! {
                _ = self.stop_notify.notified() => return,
                _ = sleep(self.poll_interval) => {}
                _ = self.new_task.notified() => {}
            }
        }
    }
}
