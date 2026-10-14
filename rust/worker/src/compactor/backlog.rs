//! Track eligible work after the scheduler applies ownership and exclusion rules.

mod progress;

use progress::Progress;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use chroma_types::CollectionUuid;
use parking_lot::Mutex;

/// Process-local observation starts when work becomes eligible, so normal batching time is excluded.
/// Successful publication or an advancing committed offset resets the clock. Retries do not.
#[derive(Default)]
pub(super) struct EligibleBacklog {
    progress: Arc<Mutex<Progress<CollectionUuid>>>,
}

impl EligibleBacklog {
    pub(super) fn new() -> Self {
        let result = Self::default();
        let meter = opentelemetry::global::meter("chroma_compactor");
        let progress = result.progress.clone();
        meter
            .u64_observable_gauge("compactor_eligible_collections_count")
            .with_description("Owned, enabled collections eligible for automatic compaction, including active attempts")
            .with_callback(move |observer| {
                observer.observe(progress.lock().collections.len() as u64, &[]);
            })
            .build();
        let progress = result.progress.clone();
        meter
            .f64_observable_gauge("compactor_oldest_eligible_no_progress_seconds")
            .with_unit("s")
            .with_description("Longest observed time an eligible collection has not advanced its committed offset; resets on completion or process restart; queued work is removed when it loses ownership")
            .with_callback(move |observer| {
                observer.observe(progress.lock().snapshot(Instant::now()).1, &[]);
            })
            .build();
        result
    }

    pub(super) fn update(
        &self,
        records: impl IntoIterator<Item = (CollectionUuid, i64)>,
        active: &HashSet<CollectionUuid>,
    ) {
        self.progress.lock().update(records, active, Instant::now());
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> (u64, f64) {
        self.progress.lock().snapshot(Instant::now())
    }

    pub(super) fn completed(&self, id: CollectionUuid) {
        self.progress.lock().collections.remove(&id);
    }
}
