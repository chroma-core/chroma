//! Independent block operations finish before the owner publishes its manifest.
use super::{GenerationError, Result};
use rayon::prelude::*;
use std::sync::OnceLock;

/// Share a bounded storage pool across saves in this process. Blocking storage
/// calls must not occupy the construction pool, and serialized payloads stay
/// inside each task so at most 32 block buffers are materialized concurrently.
pub(crate) fn save_blocks_parallel<T: Sync, R: Send>(
    blocks: &[T],
    operation: impl Fn(&T) -> Result<R> + Sync + Send,
) -> Result<Vec<R>> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }
    static POOL: OnceLock<std::result::Result<rayon::ThreadPool, String>> = OnceLock::new();
    let pool = POOL
        .get_or_init(|| {
            let workers = save_worker_count(std::env::var("SPANN_SAVE_WORKERS").ok().as_deref())?;
            rayon::ThreadPoolBuilder::new()
                .num_threads(workers)
                .thread_name(|i| format!("spann-save-{i}"))
                .build()
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| GenerationError::Invalid(format!("save worker pool: {error}")))?;
    // Join every task, including failures, before returning. Retain input order
    // for deterministic references and error selection despite completion order.
    let results: Vec<Result<R>> = pool.install(|| blocks.par_iter().map(operation).collect());
    results.into_iter().collect()
}

/// Read once when the process first saves blocks. A fixed upper bound prevents
/// storage tasks from growing without limit; one worker provides a matched
/// sequential control using the same binary and durability checks.
fn save_worker_count(value: Option<&str>) -> std::result::Result<usize, String> {
    match value {
        None => Ok(32),
        Some(value) => value
            .parse::<usize>()
            .ok()
            .filter(|workers| (1..=32).contains(workers))
            .ok_or_else(|| "SPANN_SAVE_WORKERS must be an integer from 1 through 32".into()),
    }
}

#[cfg(test)]
#[path = "generation_parallel_tests.rs"]
mod tests;
