//! Notion `/api/v3` client + supporting machinery.
//!
//! The rate-limited `NotionInternal` HTTP client, the `RateLimitedError`
//! / `RateLimitGate` shared cooldown, the per-client `TokenBucket`, and
//! the `TaskPool` that batches `getTasks` polling across many concurrent
//! `enqueueExportBlock` jobs. Originally ported from the python
//! prototype's `NotionInternal` class.

pub mod client;
pub mod export;
pub mod rate_limit;
pub mod search;
pub mod task_pool;
pub mod types;

pub use client::NotionInternal;
pub use rate_limit::{RateLimitGate, TokenBucket};
#[allow(unused_imports)]
pub use rate_limit::RateLimitedError;
pub use task_pool::TaskPool;
#[allow(unused_imports)]
pub use task_pool::TaskState;
pub use types::*;
