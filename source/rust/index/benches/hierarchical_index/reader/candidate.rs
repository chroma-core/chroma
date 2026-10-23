//! Isolated CPU component; no query path selects this collector yet.
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkingSetLimits {
    pub max_retained_entries: usize,
    pub max_staging_entries: usize,
    pub max_cutoff_ties: usize,
    pub max_output_entries: usize,
    pub max_seen_bitmap_bytes: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectError {
    Invalid,
    Capacity,
    Allocation,
    Failed,
}
#[derive(Clone, Copy, Debug)]
struct Distance(f32);
impl Distance {
    fn new(value: f32) -> Self {
        Self(if value == 0.0 { 0.0 } else { value })
    }
}
impl PartialEq for Distance {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for Distance {}
impl PartialOrd for Distance {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Distance {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Default, Debug, Serialize)]
pub struct WorkingSetMetrics {
    pub unique_scanned: usize,
    pub seen_bitmap_capacity_bytes: usize,
    pub peak_resident_entries_including_staging: usize,
    pub peak_distance_groups: usize,
    pub peak_map_capacity: usize,
    pub peak_map_key_value_capacity_payload_bytes: usize,
    pub peak_output_capacity_entries: usize,
}
/// A failure permanently invalidates the private operation, so no caller can
/// expose an intermediate remove/insert/prune state as a valid cutoff/result.
pub struct CandidateWorkingSet {
    m: usize,
    local_count: usize,
    limits: WorkingSetLimits,
    failed: bool,
    seen: Vec<u8>,
    best: HashMap<u32, f32>,
    groups: BTreeMap<Distance, BTreeSet<u32>>,
    metrics: WorkingSetMetrics,
}
impl CandidateWorkingSet {
    pub fn new(
        m: usize,
        local_count: usize,
        limits: WorkingSetLimits,
    ) -> Result<Self, CollectError> {
        if m == 0
            || m > 1_000_000
            || local_count > 100_000
            || limits.max_retained_entries == 0
            || limits.max_retained_entries > 100_000
            || limits.max_staging_entries > 1
            || limits.max_cutoff_ties == 0
            || limits.max_cutoff_ties > 100_000
            || limits.max_output_entries > 100_000
            || limits.max_seen_bitmap_bytes > 12_500
        {
            return Err(CollectError::Invalid);
        }
        let bytes = local_count.checked_add(7).ok_or(CollectError::Invalid)? / 8;
        if bytes > limits.max_seen_bitmap_bytes {
            return Err(CollectError::Capacity);
        }
        let mut seen = Vec::new();
        seen.try_reserve_exact(bytes)
            .map_err(|_| CollectError::Allocation)?;
        seen.resize(bytes, 0);
        let metrics = WorkingSetMetrics {
            seen_bitmap_capacity_bytes: seen.capacity(),
            ..Default::default()
        };
        Ok(Self {
            m,
            local_count,
            limits,
            failed: false,
            seen,
            best: HashMap::new(),
            groups: BTreeMap::new(),
            metrics,
        })
    }
    pub fn push(&mut self, id: u32, distance: f32) -> Result<(), CollectError> {
        if self.failed {
            return Err(CollectError::Failed);
        }
        let result = self.update(id, distance);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn update(&mut self, id: u32, distance: f32) -> Result<(), CollectError> {
        if !distance.is_finite() || id as usize >= self.local_count {
            return Err(CollectError::Invalid);
        }
        let index = id as usize / 8;
        let bit = 1u8 << (id % 8);
        if self.seen[index] & bit == 0 {
            self.seen[index] |= bit;
            self.metrics.unique_scanned += 1;
        }
        let old = self.best.get(&id).copied();
        if old.is_some_and(|value| distance >= value) {
            return Ok(());
        }
        if old.is_none() && self.best.len() >= self.m {
            let cutoff = self.groups.last_key_value().ok_or(CollectError::Invalid)?.0;
            if Distance::new(distance) > *cutoff {
                return Ok(());
            }
        }
        let transient = self
            .best
            .len()
            .checked_add(usize::from(old.is_none()))
            .ok_or(CollectError::Capacity)?;
        let cap = self
            .limits
            .max_retained_entries
            .checked_add(self.limits.max_staging_entries)
            .ok_or(CollectError::Capacity)?;
        // This check precedes new map/group entries, even if pruning would
        // immediately remove an old group. A staging allowance is explicit.
        if transient > cap {
            return Err(CollectError::Capacity);
        }
        if let Some(previous) = old {
            let key = Distance::new(previous);
            let group = self.groups.get_mut(&key).ok_or(CollectError::Invalid)?;
            group.remove(&id);
            if group.is_empty() {
                self.groups.remove(&key);
            }
        } else {
            self.best
                .try_reserve(1)
                .map_err(|_| CollectError::Allocation)?;
        }
        self.best.insert(id, distance);
        self.groups
            .entry(Distance::new(distance))
            .or_default()
            .insert(id);
        self.metrics.peak_resident_entries_including_staging = self
            .metrics
            .peak_resident_entries_including_staging
            .max(self.best.len());
        self.metrics.peak_distance_groups =
            self.metrics.peak_distance_groups.max(self.groups.len());
        self.metrics.peak_map_capacity = self.metrics.peak_map_capacity.max(self.best.capacity());
        self.metrics.peak_map_key_value_capacity_payload_bytes =
            self.metrics.peak_map_capacity * std::mem::size_of::<(u32, f32)>();
        while self.best.len() > self.m {
            let (key, group) = self.groups.last_key_value().ok_or(CollectError::Invalid)?;
            if self.best.len() - group.len() < self.m {
                break;
            }
            let key = *key;
            let removed = self.groups.remove(&key).ok_or(CollectError::Invalid)?;
            for discard in removed {
                self.best.remove(&discard);
            }
        }
        if self.best.len() > self.limits.max_retained_entries {
            return Err(CollectError::Capacity);
        }
        if self.best.len() >= self.m
            && self
                .groups
                .last_key_value()
                .ok_or(CollectError::Invalid)?
                .1
                .len()
                > self.limits.max_cutoff_ties
        {
            return Err(CollectError::Capacity);
        }
        Ok(())
    }
    pub fn snapshot(&mut self) -> Result<Vec<(u32, f32)>, CollectError> {
        if self.failed {
            return Err(CollectError::Failed);
        }
        if self.best.len() > self.limits.max_output_entries {
            self.failed = true;
            return Err(CollectError::Capacity);
        }
        let mut output = Vec::new();
        if output.try_reserve_exact(self.best.len()).is_err() {
            self.failed = true;
            return Err(CollectError::Allocation);
        }
        output.extend(self.best.iter().map(|(id, distance)| (*id, *distance)));
        output.sort_unstable_by(|a, b| {
            Distance::new(a.1)
                .cmp(&Distance::new(b.1))
                .then(a.0.cmp(&b.0))
        });
        self.metrics.peak_output_capacity_entries = self
            .metrics
            .peak_output_capacity_entries
            .max(output.capacity());
        Ok(output)
    }
    pub fn unique_scanned(&self) -> Result<usize, CollectError> {
        if self.failed {
            Err(CollectError::Failed)
        } else {
            Ok(self.metrics.unique_scanned)
        }
    }
    pub fn metrics(&self) -> &WorkingSetMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn limits() -> WorkingSetLimits {
        WorkingSetLimits {
            max_retained_entries: 100,
            max_staging_entries: 1,
            max_cutoff_ties: 100,
            max_output_entries: 100,
            max_seen_bitmap_bytes: 16,
        }
    }
    fn oracle(best: &HashMap<u32, f32>, m: usize) -> Vec<(u32, f32)> {
        let mut rows: Vec<_> = best.iter().map(|(id, d)| (*id, *d)).collect();
        rows.sort_unstable_by(|a, b| {
            Distance::new(a.1)
                .cmp(&Distance::new(b.1))
                .then(a.0.cmp(&b.0))
        });
        if rows.len() > m {
            let cutoff = rows[m - 1].1;
            rows.retain(|(_, d)| *d <= cutoff);
        }
        rows
    }
    #[test]
    fn every_prefix_matches_full_minimum_oracle_across_permutations() {
        let mut prefixes = 0;
        for seed in 0..24u64 {
            for m in [1, 2, 7, 32] {
                let mut state = seed + 1;
                let mut collector = CandidateWorkingSet::new(m, 32, limits()).unwrap();
                let mut all = HashMap::new();
                for _ in 0..160 {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let id = ((state >> 32) % 32) as u32;
                    let d = match state % 11 {
                        0 => -0.0,
                        1 => 0.0,
                        _ => ((state >> 40) % 9) as f32 - 4.0,
                    };
                    let old = all.entry(id).or_insert(d);
                    if d < *old {
                        *old = d;
                    }
                    collector.push(id, d).unwrap();
                    assert_eq!(collector.snapshot().unwrap(), oracle(&all, m));
                    assert_eq!(collector.unique_scanned().unwrap(), all.len());
                    prefixes += 1;
                    assert_eq!(
                        collector.best.len(),
                        collector.groups.values().map(|g| g.len()).sum::<usize>()
                    );
                }
            }
        }
        println!("WORKING_SET_PREFIX_PROOF {{\"prefixes\":{prefixes},\"seeds\":24,\"m_values\":[1,2,7,32]}}");
    }
    #[test]
    fn discarded_duplicate_reenters_all_ties_and_signed_zero_group() {
        let mut collector = CandidateWorkingSet::new(2, 4, limits()).unwrap();
        let mut all = HashMap::new();
        for (id, d) in [(0, 9.), (1, 2.), (2, 2.), (0, 1.), (1, 3.)] {
            let old = all.entry(id).or_insert(d);
            if d < *old {
                *old = d;
            }
            collector.push(id, d).unwrap();
            assert_eq!(collector.snapshot().unwrap(), oracle(&all, 2));
        }
        assert_eq!(
            collector.snapshot().unwrap(),
            vec![(0, 1.), (1, 2.), (2, 2.)]
        );
        assert_eq!(collector.unique_scanned().unwrap(), 3);
        let mut zeros = CandidateWorkingSet::new(1, 4, limits()).unwrap();
        for (id, d) in [(3, -0.0), (0, 0.0), (2, -0.0)] {
            zeros.push(id, d).unwrap();
        }
        assert_eq!(zeros.groups.len(), 1);
        assert_eq!(zeros.snapshot().unwrap().len(), 3);
        let mut selective = CandidateWorkingSet::new(2, 32, limits()).unwrap();
        for id in 0..32 {
            selective.push(id, id as f32).unwrap();
        }
        assert_eq!(selective.snapshot().unwrap(), vec![(0, 0.), (1, 1.)]);
        assert_eq!(selective.unique_scanned().unwrap(), 32);
        println!(
            "WORKING_SET_SELECTIVE_PROOF {}",
            serde_json::json!({"m":2,"local_count":32,"retained":selective.best.len(),"metrics":selective.metrics()})
        );
    }
    #[test]
    fn all_capacity_failures_latch_and_expose_no_partial_results() {
        let base = limits();
        assert!(matches!(
            CandidateWorkingSet::new(
                1,
                9,
                WorkingSetLimits {
                    max_seen_bitmap_bytes: 1,
                    ..base
                }
            ),
            Err(CollectError::Capacity)
        ));
        assert!(matches!(
            CandidateWorkingSet::new(0, 0, base),
            Err(CollectError::Invalid)
        ));
        // An improvement needs a declared staging slot even when the final
        // result would fit. Exact final/staging bounds and one-over are distinct.
        let mut staged = CandidateWorkingSet::new(
            1,
            3,
            WorkingSetLimits {
                max_retained_entries: 1,
                max_staging_entries: 1,
                ..base
            },
        )
        .unwrap();
        staged.push(0, 5.).unwrap();
        staged.push(1, 1.).unwrap();
        assert_eq!(staged.snapshot().unwrap(), vec![(1, 1.)]);
        assert_eq!(staged.metrics().peak_resident_entries_including_staging, 2);
        let mut no_stage = CandidateWorkingSet::new(
            1,
            3,
            WorkingSetLimits {
                max_retained_entries: 1,
                max_staging_entries: 0,
                ..base
            },
        )
        .unwrap();
        no_stage.push(0, 5.).unwrap();
        assert_eq!(no_stage.push(1, 1.), Err(CollectError::Capacity));
        assert_eq!(no_stage.snapshot(), Err(CollectError::Failed));
        for limited in [
            WorkingSetLimits {
                max_retained_entries: 1,
                ..base
            },
            WorkingSetLimits {
                max_cutoff_ties: 1,
                ..base
            },
        ] {
            let mut ties = CandidateWorkingSet::new(1, 2, limited).unwrap();
            ties.push(0, 0.).unwrap();
            assert_eq!(ties.push(1, -0.), Err(CollectError::Capacity));
            assert_eq!(ties.unique_scanned(), Err(CollectError::Failed));
            assert_eq!(ties.snapshot(), Err(CollectError::Failed));
        }
        let mut output = CandidateWorkingSet::new(
            2,
            2,
            WorkingSetLimits {
                max_output_entries: 1,
                ..base
            },
        )
        .unwrap();
        output.push(0, 0.).unwrap();
        output.push(1, 1.).unwrap();
        assert_eq!(output.snapshot(), Err(CollectError::Capacity));
        assert_eq!(output.push(0, -1.), Err(CollectError::Failed));
        let mut invalid = CandidateWorkingSet::new(1, 2, base).unwrap();
        invalid.push(0, 0.).unwrap();
        assert_eq!(invalid.push(0, f32::NAN), Err(CollectError::Invalid));
        assert_eq!(invalid.snapshot(), Err(CollectError::Failed));
        let mut empty = CandidateWorkingSet::new(
            1,
            0,
            WorkingSetLimits {
                max_seen_bitmap_bytes: 0,
                ..base
            },
        )
        .unwrap();
        assert_eq!(empty.snapshot().unwrap(), Vec::<(u32, f32)>::new());
        assert_eq!(empty.push(0, 1.), Err(CollectError::Invalid));
    }
    #[test]
    fn json_limits_reject_boolean_fractional_and_unknown_fields() {
        for raw in [
            r#"{"max_retained_entries":true,"max_staging_entries":1,"max_cutoff_ties":1,"max_output_entries":1,"max_seen_bitmap_bytes":1}"#,
            r#"{"max_retained_entries":1.5,"max_staging_entries":1,"max_cutoff_ties":1,"max_output_entries":1,"max_seen_bitmap_bytes":1}"#,
        ] {
            assert!(serde_json::from_str::<WorkingSetLimits>(raw).is_err());
        }
        let mut unknown = serde_json::to_value(limits()).unwrap();
        unknown["unexpected"] = serde_json::json!(1);
        assert!(serde_json::from_value::<WorkingSetLimits>(unknown).is_err());
    }
}
