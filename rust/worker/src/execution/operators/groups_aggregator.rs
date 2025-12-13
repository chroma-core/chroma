//! Groups aggregator for iterative group-by search.
//!
//! This module provides the `GroupsAggregator` struct which manages the state
//! of groups during iterative KNN searches. It tracks discovered groups, their
//! records, and determines when enough groups have been filled.

use chroma_types::{Metadata, MetadataValue};
use std::collections::{HashMap, HashSet};

/// A composite group key from multiple metadata field values.
///
/// The key is represented as a vector of optional metadata values,
/// where each position corresponds to a field in the `group_by.keys` spec.
/// `None` values represent missing metadata fields.
///
/// Note: We store both the original values (for filtering/response) and
/// a string representation (for hashing, since MetadataValue contains floats).
#[derive(Clone, Debug)]
pub struct GroupKey {
    /// Original metadata values
    pub values: Vec<Option<MetadataValue>>,
    /// String representation for hashing
    hash_key: String,
}

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.hash_key == other.hash_key
    }
}

impl Eq for GroupKey {}

impl std::hash::Hash for GroupKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash_key.hash(state);
    }
}

impl GroupKey {
    /// Creates a group key by extracting values from record metadata.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The record's metadata (may be None)
    /// * `keys` - The field names to extract for the composite key
    pub fn from_metadata(metadata: Option<&Metadata>, keys: &[String]) -> Self {
        let values: Vec<_> = keys
            .iter()
            .map(|key| metadata.and_then(|m| m.get(key).cloned()))
            .collect();

        let hash_key = Self::compute_hash_key(&values);
        GroupKey { values, hash_key }
    }

    /// Computes a stable string representation for hashing.
    fn compute_hash_key(values: &[Option<MetadataValue>]) -> String {
        values
            .iter()
            .map(|v| match v {
                Some(MetadataValue::Bool(b)) => format!("b:{}", b),
                Some(MetadataValue::Int(i)) => format!("i:{}", i),
                Some(MetadataValue::Float(f)) => format!("f:{}", f.to_bits()),
                Some(MetadataValue::Str(s)) => format!("s:{}", s),
                Some(MetadataValue::SparseVector(_)) => "v:<sparse>".to_string(),
                None => "n:null".to_string(),
            })
            .collect::<Vec<_>>()
            .join("|")
    }

    /// Converts the group key to a vector of string representations.
    ///
    /// This is used for serialization in the proto response.
    pub fn to_string_values(&self) -> Vec<String> {
        self.values
            .iter()
            .map(|v| match v {
                Some(MetadataValue::Bool(b)) => b.to_string(),
                Some(MetadataValue::Int(i)) => i.to_string(),
                Some(MetadataValue::Float(f)) => f.to_string(),
                Some(MetadataValue::Str(s)) => s.clone(),
                Some(MetadataValue::SparseVector(_)) => "<sparse_vector>".to_string(),
                None => "<null>".to_string(),
            })
            .collect()
    }
}

/// A record with its offset ID and score/measure.
#[derive(Clone, Debug)]
pub struct RecordWithScore {
    pub offset_id: u32,
    pub score: f32,
}

/// A group of records sharing the same group key.
#[derive(Clone, Debug)]
pub struct RecordGroup {
    /// The composite key that identifies this group.
    pub group_key: GroupKey,
    /// Records in this group, ordered by score (ascending).
    pub records: Vec<RecordWithScore>,
    /// The best (lowest) score in this group.
    pub best_score: f32,
}

/// Aggregator for managing groups during iterative search.
///
/// The aggregator tracks:
/// - All groups discovered so far
/// - Best scores per group (for ranking groups)
/// - Which groups have reached capacity (group_size)
/// - All seen point IDs (for deduplication across iterations)
pub struct GroupsAggregator {
    /// All groups found, keyed by composite group key.
    groups: HashMap<GroupKey, Vec<RecordWithScore>>,
    /// Keys to group by.
    group_by_keys: Vec<String>,
    /// Target number of groups.
    max_groups: usize,
    /// Max results per group.
    group_size: usize,
    /// Best score per group (for ranking groups).
    group_best_scores: HashMap<GroupKey, f32>,
    /// Groups that have reached group_size.
    full_groups: HashSet<GroupKey>,
    /// All seen point IDs (for deduplication across iterations).
    seen_ids: HashSet<u32>,
}

impl GroupsAggregator {
    /// Creates a new GroupsAggregator.
    ///
    /// # Arguments
    ///
    /// * `group_by_keys` - Metadata field names to group by
    /// * `max_groups` - Target number of groups (limit)
    /// * `group_size` - Maximum records per group
    pub fn new(group_by_keys: Vec<String>, max_groups: usize, group_size: usize) -> Self {
        Self {
            groups: HashMap::new(),
            group_by_keys,
            max_groups,
            group_size,
            group_best_scores: HashMap::new(),
            full_groups: HashSet::new(),
            seen_ids: HashSet::new(),
        }
    }

    /// Adds points from a search iteration.
    ///
    /// Points are grouped by their metadata values for the specified keys.
    /// Duplicate points (by offset_id) are ignored.
    ///
    /// # Arguments
    ///
    /// * `records` - Records with their offset IDs and scores
    /// * `metadata_map` - Mapping from offset_id to metadata
    pub fn add_points(
        &mut self,
        records: &[(u32, f32)], // (offset_id, score)
        metadata_map: &HashMap<u32, Metadata>,
    ) {
        for &(offset_id, score) in records {
            // Skip if already seen
            if self.seen_ids.contains(&offset_id) {
                continue;
            }

            // Get the group key from metadata
            let metadata = metadata_map.get(&offset_id);
            let group_key = GroupKey::from_metadata(metadata, &self.group_by_keys);

            // Skip if this group is already full (don't add to seen_ids either)
            if self.full_groups.contains(&group_key) {
                continue;
            }

            // Mark as seen only after we know the group isn't full
            self.seen_ids.insert(offset_id);

            // Add to group
            let group = self.groups.entry(group_key.clone()).or_default();
            group.push(RecordWithScore { offset_id, score });

            // Update best score for this group
            let best = self.group_best_scores.entry(group_key.clone()).or_insert(f32::MAX);
            if score < *best {
                *best = score;
            }

            // Check if group is now full
            if group.len() >= self.group_size {
                self.full_groups.insert(group_key);
            }
        }
    }

    /// Returns the set of all seen point IDs.
    ///
    /// Used to build exclusion filters for subsequent iterations.
    pub fn seen_ids(&self) -> &HashSet<u32> {
        &self.seen_ids
    }

    /// Returns the number of distinct groups discovered.
    pub fn num_groups(&self) -> usize {
        self.groups.len()
    }

    /// Returns the number of filled groups among the best groups.
    ///
    /// A group is "filled" if it has reached `group_size` records.
    /// This only counts groups that would be in the top `max_groups` by best score.
    pub fn num_filled_best_groups(&self) -> usize {
        let best_groups = self.get_best_group_keys();
        best_groups
            .iter()
            .filter(|key| self.full_groups.contains(*key))
            .count()
    }

    /// Checks if we have enough filled groups.
    ///
    /// Returns true if we have at least `max_groups` groups that are filled.
    pub fn has_enough_filled_groups(&self) -> bool {
        self.num_filled_best_groups() >= self.max_groups
    }

    /// Returns keys of groups that are full (reached group_size).
    ///
    /// Used to build exclusion filters.
    pub fn keys_of_filled_groups(&self) -> Vec<GroupKey> {
        self.full_groups.iter().cloned().collect()
    }

    /// Returns keys of unfilled best groups.
    ///
    /// These are the groups in the top `max_groups` (by best score) that haven't
    /// reached `group_size` yet. Used for phase 2 targeted filling.
    pub fn keys_of_unfilled_best_groups(&self) -> Vec<GroupKey> {
        let best_groups = self.get_best_group_keys();
        best_groups
            .into_iter()
            .filter(|key| !self.full_groups.contains(key))
            .collect()
    }

    /// Gets the keys of the best groups (top `max_groups` by best score).
    fn get_best_group_keys(&self) -> Vec<GroupKey> {
        let mut scored_groups: Vec<_> = self
            .group_best_scores
            .iter()
            .map(|(key, score)| (key.clone(), *score))
            .collect();

        // Sort by score ascending (lower is better)
        scored_groups.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        scored_groups
            .into_iter()
            .take(self.max_groups)
            .map(|(key, _)| key)
            .collect()
    }

    /// Distills the aggregated results into the final grouped output.
    ///
    /// Returns the top `max_groups` groups sorted by best score,
    /// with each group containing up to `group_size` records sorted by score.
    pub fn distill(&self) -> Vec<RecordGroup> {
        let best_keys = self.get_best_group_keys();

        let mut result = Vec::with_capacity(best_keys.len());
        for key in best_keys {
            if let Some(mut records) = self.groups.get(&key).cloned() {
                // Sort records by score ascending
                records.sort_by(|a, b| {
                    a.score
                        .partial_cmp(&b.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });

                // Truncate to group_size
                records.truncate(self.group_size);

                let best_score = records.first().map(|r| r.score).unwrap_or(f32::MAX);

                result.push(RecordGroup {
                    group_key: key,
                    records,
                    best_score,
                });
            }
        }

        // Sort groups by best score ascending
        result.sort_by(|a, b| {
            a.best_score
                .partial_cmp(&b.best_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        result
    }

    /// Returns metadata values that should be matched for phase 2 targeted search.
    ///
    /// For each unfilled best group, returns the metadata values that records
    /// must have to belong to that group. Used to construct Where filters.
    pub fn get_unfilled_group_metadata_values(&self) -> Vec<Vec<(String, MetadataValue)>> {
        self.keys_of_unfilled_best_groups()
            .into_iter()
            .filter_map(|key| {
                let pairs: Vec<_> = self
                    .group_by_keys
                    .iter()
                    .zip(key.values.iter())
                    .filter_map(|(field, value)| {
                        value.as_ref().map(|v| (field.clone(), v.clone()))
                    })
                    .collect();

                if pairs.is_empty() {
                    None
                } else {
                    Some(pairs)
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_metadata(category: &str, author: &str) -> Metadata {
        let mut m = Metadata::new();
        m.insert("category".to_string(), MetadataValue::Str(category.to_string()));
        m.insert("author".to_string(), MetadataValue::Str(author.to_string()));
        m
    }

    #[test]
    fn test_basic_grouping() {
        let mut agg = GroupsAggregator::new(vec!["category".to_string()], 2, 3);

        let mut metadata_map = HashMap::new();
        metadata_map.insert(1, make_metadata("tech", "alice"));
        metadata_map.insert(2, make_metadata("tech", "bob"));
        metadata_map.insert(3, make_metadata("science", "carol"));
        metadata_map.insert(4, make_metadata("tech", "dave"));

        let records = vec![(1, 0.1), (2, 0.2), (3, 0.15), (4, 0.3)];
        agg.add_points(&records, &metadata_map);

        assert_eq!(agg.num_groups(), 2);
        assert_eq!(agg.seen_ids().len(), 4);
    }

    #[test]
    fn test_group_fullness() {
        let mut agg = GroupsAggregator::new(vec!["category".to_string()], 2, 2);

        let mut metadata_map = HashMap::new();
        metadata_map.insert(1, make_metadata("tech", "a"));
        metadata_map.insert(2, make_metadata("tech", "b"));
        metadata_map.insert(3, make_metadata("tech", "c"));

        // First two should fill the tech group
        agg.add_points(&[(1, 0.1), (2, 0.2)], &metadata_map);
        assert!(agg.full_groups.len() == 1);

        // Third should be skipped (group full)
        agg.add_points(&[(3, 0.3)], &metadata_map);
        assert_eq!(agg.seen_ids().len(), 2); // Only 2 seen, not 3
    }

    #[test]
    fn test_distill() {
        let mut agg = GroupsAggregator::new(vec!["category".to_string()], 2, 2);

        let mut metadata_map = HashMap::new();
        metadata_map.insert(1, make_metadata("tech", "a"));
        metadata_map.insert(2, make_metadata("science", "b"));
        metadata_map.insert(3, make_metadata("tech", "c"));

        agg.add_points(&[(1, 0.3), (2, 0.1), (3, 0.2)], &metadata_map);

        let groups = agg.distill();
        assert_eq!(groups.len(), 2);

        // Science should be first (best score 0.1)
        assert_eq!(groups[0].best_score, 0.1);
        assert_eq!(groups[0].group_key.to_string_values(), vec!["science"]);

        // Tech should be second (best score 0.2)
        assert_eq!(groups[1].best_score, 0.2);
        assert_eq!(groups[1].records.len(), 2);
    }

    #[test]
    fn test_deduplication() {
        let mut agg = GroupsAggregator::new(vec!["category".to_string()], 2, 3);

        let mut metadata_map = HashMap::new();
        metadata_map.insert(1, make_metadata("tech", "a"));

        // Add same record twice
        agg.add_points(&[(1, 0.1)], &metadata_map);
        agg.add_points(&[(1, 0.2)], &metadata_map);

        assert_eq!(agg.seen_ids().len(), 1);
        let groups = agg.distill();
        assert_eq!(groups[0].records.len(), 1);
    }
}
