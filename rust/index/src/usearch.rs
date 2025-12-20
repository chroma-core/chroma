use std::collections::HashSet;

use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use dashmap::DashSet;
use thiserror::Error;
use usearch::{Index as UsearchNativeIndex, IndexOptions, MetricKind, ScalarKind};

use crate::{Index, IndexConfig, IndexUuid};

/// Configuration for UsearchIndex.
#[derive(Clone, Debug)]
pub struct UsearchIndexConfig {
    /// Maximum number of elements the index can hold.
    pub max_elements: usize,
    /// Number of bi-directional links created for every new element (connectivity).
    /// Higher values lead to better recall but slower indexing.
    pub m: usize,
    /// Size of the dynamic candidate list during construction.
    pub ef_construction: usize,
    /// Size of the dynamic candidate list during search.
    pub ef_search: usize,
}

impl UsearchIndexConfig {
    pub fn new(max_elements: usize, m: usize, ef_construction: usize, ef_search: usize) -> Self {
        Self {
            max_elements,
            m,
            ef_construction,
            ef_search,
        }
    }
}

impl Default for UsearchIndexConfig {
    fn default() -> Self {
        Self {
            max_elements: 100,
            m: 16,
            ef_construction: 100,
            ef_search: 100,
        }
    }
}

/// USearch-based HNSW index wrapper.
///
/// This is thread-safe (`Send + Sync`) as usearch::Index uses fine-grained
/// per-node locking internally.
pub struct UsearchIndex {
    index: UsearchNativeIndex,
    pub id: IndexUuid,
    pub dimensionality: usize,
    pub distance_function: DistanceFunction,
    /// Track live IDs (usearch doesn't provide ID enumeration).
    ids: DashSet<usize>,
}

// UsearchNativeIndex is Send + Sync (uses internal fine-grained locking)
unsafe impl Send for UsearchIndex {}
unsafe impl Sync for UsearchIndex {}

#[derive(Error, Debug)]
pub enum UsearchError {
    #[error("USearch error: {0}")]
    Native(String),

    #[error("No config provided")]
    NoConfigProvided,

    #[error("Dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },

    #[error("ID not found: {0}")]
    IdNotFound(usize),
}

impl ChromaError for UsearchError {
    fn code(&self) -> ErrorCodes {
        match self {
            UsearchError::Native(_) => ErrorCodes::Internal,
            UsearchError::NoConfigProvided => ErrorCodes::InvalidArgument,
            UsearchError::DimensionMismatch { .. } => ErrorCodes::InvalidArgument,
            UsearchError::IdNotFound(_) => ErrorCodes::NotFound,
        }
    }
}

/// Helper to convert usearch errors to boxed ChromaError
fn usearch_err(e: impl std::fmt::Display) -> Box<dyn ChromaError> {
    UsearchError::Native(e.to_string()).boxed()
}

/// Map Chroma's DistanceFunction to USearch's MetricKind.
fn to_usearch_metric(df: &DistanceFunction) -> MetricKind {
    match df {
        DistanceFunction::Euclidean => MetricKind::L2sq,
        DistanceFunction::Cosine => MetricKind::Cos,
        DistanceFunction::InnerProduct => MetricKind::IP,
    }
}

impl UsearchIndex {
    /// Returns the number of vectors in the index (excluding deleted).
    pub fn len(&self) -> usize {
        self.index.size()
    }

    /// Returns the total count including deleted items.
    /// For usearch, this is the same as len() since we remove IDs on delete.
    pub fn len_with_deleted(&self) -> usize {
        self.ids.len()
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.index.size() == 0
    }

    /// Returns the current capacity of the index.
    pub fn capacity(&self) -> usize {
        self.index.capacity()
    }

    /// Resize the index to accommodate more elements.
    pub fn resize(&self, new_capacity: usize) -> Result<(), Box<dyn ChromaError>> {
        self.index.reserve(new_capacity).map_err(usearch_err)
    }
}

impl Index<UsearchIndexConfig> for UsearchIndex {
    fn init(
        index_config: &IndexConfig,
        usearch_config: Option<&UsearchIndexConfig>,
        id: IndexUuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let config = usearch_config.ok_or_else(|| UsearchError::NoConfigProvided.boxed())?;

        let options = IndexOptions {
            dimensions: index_config.dimensionality as usize,
            metric: to_usearch_metric(&index_config.distance_function),
            quantization: ScalarKind::F32,
            connectivity: config.m,
            expansion_add: config.ef_construction,
            expansion_search: config.ef_search,
            multi: false,
        };

        let index = UsearchNativeIndex::new(&options).map_err(usearch_err)?;

        index.reserve(config.max_elements).map_err(usearch_err)?;

        Ok(UsearchIndex {
            index,
            id,
            dimensionality: index_config.dimensionality as usize,
            distance_function: index_config.distance_function.clone(),
            ids: DashSet::new(),
        })
    }

    fn add(&self, id: usize, vector: &[f32]) -> Result<(), Box<dyn ChromaError>> {
        // Auto-resize if needed
        if self.index.size() >= self.index.capacity() {
            let new_capacity = (self.index.capacity() * 2).max(100);
            self.index.reserve(new_capacity).map_err(usearch_err)?;
        }

        self.index.add(id as u64, vector).map_err(usearch_err)?;
        // Track the ID for get_all_ids() and len_with_deleted()
        self.ids.insert(id);
        Ok(())
    }

    fn delete(&self, id: usize) -> Result<(), Box<dyn ChromaError>> {
        self.index.remove(id as u64).map_err(usearch_err)?;
        self.ids.remove(&id);
        Ok(())
    }

    fn query(
        &self,
        vector: &[f32],
        k: usize,
        allowed_ids: &[usize],
        disallowed_ids: &[usize],
    ) -> Result<(Vec<usize>, Vec<f32>), Box<dyn ChromaError>> {
        if allowed_ids.is_empty() && disallowed_ids.is_empty() {
            // Fast path: no filtering
            let result = self.index.search(vector, k).map_err(usearch_err)?;

            let ids: Vec<usize> = result.keys.iter().map(|&k| k as usize).collect();
            Ok((ids, result.distances))
        } else {
            // Filtered search using HashSet for O(1) lookups
            let allowed_set: HashSet<u64> = allowed_ids.iter().map(|&id| id as u64).collect();
            let disallowed_set: HashSet<u64> = disallowed_ids.iter().map(|&id| id as u64).collect();

            let filter = |key: u64| -> bool {
                let allowed_ok = allowed_set.is_empty() || allowed_set.contains(&key);
                let not_disallowed = !disallowed_set.contains(&key);
                allowed_ok && not_disallowed
            };

            let result = self
                .index
                .filtered_search(vector, k, &filter)
                .map_err(usearch_err)?;

            let ids: Vec<usize> = result.keys.iter().map(|&k| k as usize).collect();
            Ok((ids, result.distances))
        }
    }

    fn get(&self, id: usize) -> Result<Option<Vec<f32>>, Box<dyn ChromaError>> {
        if !self.index.contains(id as u64) {
            return Ok(None);
        }

        let mut vector = vec![0.0f32; self.dimensionality];
        let count = self
            .index
            .get(id as u64, &mut vector)
            .map_err(usearch_err)?;

        if count == 0 {
            Ok(None)
        } else {
            Ok(Some(vector))
        }
    }

    fn get_all_ids(&self) -> Result<(Vec<usize>, Vec<usize>), Box<dyn ChromaError>> {
        // Return all live IDs. Second vec is empty (no deleted tracking).
        let ids: Vec<usize> = self.ids.iter().map(|r| *r).collect();
        Ok((ids, vec![]))
    }

    fn get_all_ids_sizes(&self) -> Result<Vec<usize>, Box<dyn ChromaError>> {
        // Each ID has exactly one vector in usearch (multi=false).
        Ok(vec![1; self.ids.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_index(dim: usize, capacity: usize) -> UsearchIndex {
        let index_config = IndexConfig {
            dimensionality: dim as i32,
            distance_function: DistanceFunction::Euclidean,
        };
        let usearch_config = UsearchIndexConfig::new(capacity, 16, 100, 100);
        UsearchIndex::init(&index_config, Some(&usearch_config), IndexUuid::default()).unwrap()
    }

    #[test]
    fn test_basic_add_and_query() {
        let index = create_test_index(4, 100);

        // Add some vectors
        index.add(0, &[1.0, 0.0, 0.0, 0.0]).unwrap();
        index.add(1, &[0.0, 1.0, 0.0, 0.0]).unwrap();
        index.add(2, &[0.0, 0.0, 1.0, 0.0]).unwrap();

        assert_eq!(index.len(), 3);

        // Query for nearest neighbor
        let (ids, distances) = index.query(&[1.0, 0.0, 0.0, 0.0], 1, &[], &[]).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], 0);
        assert!(distances[0] < 0.001); // Should be very close to 0
    }

    #[test]
    fn test_filtered_query_with_allowed() {
        let index = create_test_index(4, 100);

        index.add(0, &[1.0, 0.0, 0.0, 0.0]).unwrap();
        index.add(1, &[0.9, 0.1, 0.0, 0.0]).unwrap();
        index.add(2, &[0.0, 1.0, 0.0, 0.0]).unwrap();

        // Query with allowed_ids filter - only allow id 1 and 2
        let (ids, _) = index.query(&[1.0, 0.0, 0.0, 0.0], 1, &[1, 2], &[]).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], 1); // id 1 is closest among allowed
    }

    #[test]
    fn test_filtered_query_with_disallowed() {
        let index = create_test_index(4, 100);

        index.add(0, &[1.0, 0.0, 0.0, 0.0]).unwrap();
        index.add(1, &[0.9, 0.1, 0.0, 0.0]).unwrap();
        index.add(2, &[0.0, 1.0, 0.0, 0.0]).unwrap();

        // Query with disallowed_ids filter - disallow id 0
        let (ids, _) = index.query(&[1.0, 0.0, 0.0, 0.0], 1, &[], &[0]).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], 1); // id 1 is closest after disallowing 0
    }

    #[test]
    fn test_get_vector() {
        let index = create_test_index(4, 100);

        let vec = [1.0, 2.0, 3.0, 4.0];
        index.add(0, &vec).unwrap();

        let retrieved = index.get(0).unwrap().unwrap();
        assert_eq!(retrieved.len(), 4);
        for (a, b) in retrieved.iter().zip(vec.iter()) {
            assert!((a - b).abs() < 0.001);
        }

        // Non-existent ID
        assert!(index.get(999).unwrap().is_none());
    }

    #[test]
    fn test_delete() {
        let index = create_test_index(4, 100);

        index.add(0, &[1.0, 0.0, 0.0, 0.0]).unwrap();
        index.add(1, &[0.0, 1.0, 0.0, 0.0]).unwrap();

        assert_eq!(index.len(), 2);

        index.delete(0).unwrap();

        // After deletion, the vector should not be found
        assert!(!index.index.contains(0));
    }

    #[test]
    fn test_auto_resize() {
        let index = create_test_index(4, 2); // Start with capacity 2

        // Add more vectors than initial capacity
        for i in 0..10 {
            index.add(i, &[i as f32, 0.0, 0.0, 0.0]).unwrap();
        }

        assert_eq!(index.len(), 10);
        assert!(index.capacity() >= 10);
    }

    #[test]
    fn test_cosine_distance() {
        let index_config = IndexConfig {
            dimensionality: 4,
            distance_function: DistanceFunction::Cosine,
        };
        let usearch_config = UsearchIndexConfig::new(100, 16, 100, 100);
        let index =
            UsearchIndex::init(&index_config, Some(&usearch_config), IndexUuid::default()).unwrap();

        // Add normalized vectors
        index.add(0, &[1.0, 0.0, 0.0, 0.0]).unwrap();
        index.add(1, &[0.0, 1.0, 0.0, 0.0]).unwrap();

        let (ids, _) = index.query(&[1.0, 0.0, 0.0, 0.0], 1, &[], &[]).unwrap();
        assert_eq!(ids[0], 0);
    }
}
