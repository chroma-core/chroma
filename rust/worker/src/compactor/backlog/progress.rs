use std::collections::{HashMap, HashSet};
use std::time::Instant;

pub(super) struct Progress<K> {
    pub(super) collections: HashMap<K, (i64, Instant)>,
}

impl<K> Default for Progress<K> {
    fn default() -> Self {
        Self {
            collections: HashMap::new(),
        }
    }
}

impl<K: Copy + Eq + std::hash::Hash> Progress<K> {
    pub(super) fn update(
        &mut self,
        records: impl IntoIterator<Item = (K, i64)>,
        active: &HashSet<K>,
        now: Instant,
    ) {
        let mut present = active.clone();
        for (id, offset) in records {
            present.insert(id);
            let entry = self.collections.entry(id).or_insert((offset, now));
            if offset > entry.0 {
                *entry = (offset, now);
            }
        }
        self.collections.retain(|id, _| present.contains(id));
    }

    pub(super) fn snapshot(&self, now: Instant) -> (u64, f64) {
        let oldest = self
            .collections
            .values()
            .map(|(_, since)| now.saturating_duration_since(*since).as_secs_f64())
            .fold(0.0, f64::max);
        (self.collections.len() as u64, oldest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn only_observed_eligible_work_starts_a_clock() {
        let mut progress = Progress::default();
        let now = Instant::now();
        let id = 1_u64;
        progress.update([], &HashSet::new(), now);
        assert_eq!(progress.snapshot(now + Duration::from_secs(1800)), (0, 0.0));
        progress.update(
            [(id, 100)],
            &HashSet::new(),
            now + Duration::from_secs(1800),
        );
        assert_eq!(
            progress.snapshot(now + Duration::from_secs(1810)),
            (1, 10.0)
        );
    }

    #[test]
    fn active_attempts_and_retries_keep_the_clock_but_progress_resets_it() {
        let mut progress = Progress::default();
        let now = Instant::now();
        let id = 1_u64;
        progress.update([(id, 100)], &HashSet::new(), now);
        progress.update([], &HashSet::from([id]), now + Duration::from_secs(100));
        progress.update([(id, 100)], &HashSet::new(), now + Duration::from_secs(200));
        assert_eq!(
            progress.snapshot(now + Duration::from_secs(300)),
            (1, 300.0)
        );
        progress.update([(id, 101)], &HashSet::new(), now + Duration::from_secs(300));
        assert_eq!(progress.snapshot(now + Duration::from_secs(310)), (1, 10.0));
    }

    #[test]
    fn clock_keeps_advancing_without_scheduler_updates() {
        let mut progress = Progress::default();
        let now = Instant::now();
        progress.update([(1_u64, 0)], &HashSet::new(), now);
        assert_eq!(
            progress.snapshot(now + Duration::from_secs(7200)),
            (1, 7200.0)
        );
    }

    #[test]
    fn removal_and_reappearance_start_a_new_clock() {
        let mut progress = Progress::default();
        let now = Instant::now();
        progress.update([(1, 100)], &HashSet::new(), now);
        progress.update([], &HashSet::new(), now + Duration::from_secs(100));
        assert_eq!(progress.snapshot(now + Duration::from_secs(200)), (0, 0.0));
        progress.update([(1, 100)], &HashSet::new(), now + Duration::from_secs(200));
        assert_eq!(progress.snapshot(now + Duration::from_secs(210)), (1, 10.0));
    }

    #[test]
    fn regressing_offsets_do_not_count_as_progress() {
        let mut progress = Progress::default();
        let now = Instant::now();
        progress.update([(1, 100)], &HashSet::new(), now);
        progress.update([(1, 99)], &HashSet::new(), now + Duration::from_secs(100));
        assert_eq!(
            progress.snapshot(now + Duration::from_secs(200)),
            (1, 200.0)
        );
    }
}
