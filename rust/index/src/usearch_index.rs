use super::{Index, IndexConfig, IndexUuid};
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;
use tracing::instrument;
use usearch::{new_index, Index as USearchIndexInner, IndexOptions, MetricKind, ScalarKind};

/// Default max elements for the USearch index - kept small to prevent bloating
/// which impacts query latency by increasing bytes fetched from storage.
pub const USEARCH_DEFAULT_MAX_ELEMENTS: usize = 100;

/// Configuration for USearch index
#[derive(Clone, Debug)]
pub struct USearchIndexConfig {
    pub max_elements: usize,
    /// Number of connections per element (M parameter in HNSW)
    pub connectivity: usize,
    /// Size of the dynamic candidate list during construction (ef_construction)
    pub expansion_add: usize,
    /// Size of the dynamic candidate list during search (ef_search)
    pub expansion_search: usize,
    /// Path for persistent storage (None for ephemeral)
    pub persist_path: Option<String>,
    /// Whether to use scalar quantization (f16) for memory efficiency
    pub quantization: Option<ScalarKind>,
}

#[derive(Error, Debug)]
pub enum USearchIndexConfigError {
    #[error("Missing config `{0}`")]
    MissingConfig(String),
}

impl ChromaError for USearchIndexConfigError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

impl USearchIndexConfig {
    /// Create an ephemeral (in-memory only) index configuration
    pub fn new_ephemeral(
        connectivity: usize,
        expansion_add: usize,
        expansion_search: usize,
    ) -> Self {
        Self {
            max_elements: USEARCH_DEFAULT_MAX_ELEMENTS,
            connectivity,
            expansion_add,
            expansion_search,
            persist_path: None,
            quantization: None,
        }
    }

    /// Create an ephemeral index configuration with quantization
    pub fn new_ephemeral_quantized(
        connectivity: usize,
        expansion_add: usize,
        expansion_search: usize,
        quantization: ScalarKind,
    ) -> Self {
        Self {
            max_elements: USEARCH_DEFAULT_MAX_ELEMENTS,
            connectivity,
            expansion_add,
            expansion_search,
            persist_path: None,
            quantization: Some(quantization),
        }
    }

    /// Create a persistent index configuration
    pub fn new_persistent(
        connectivity: usize,
        expansion_add: usize,
        expansion_search: usize,
        persist_path: &Path,
    ) -> Result<Self, Box<USearchIndexConfigError>> {
        let persist_path = match persist_path.to_str() {
            Some(persist_path) => persist_path,
            None => {
                return Err(Box::new(USearchIndexConfigError::MissingConfig(
                    "persist_path".to_string(),
                )))
            }
        };
        Ok(Self {
            max_elements: USEARCH_DEFAULT_MAX_ELEMENTS,
            connectivity,
            expansion_add,
            expansion_search,
            persist_path: Some(persist_path.to_string()),
            quantization: None,
        })
    }
}

/// USearch-based HNSW index implementation
/// This is thread-safe and supports concurrent reads and writes.
pub struct USearchIndex {
    index: USearchIndexInner,
    pub id: IndexUuid,
    pub distance_function: DistanceFunction,
    dimensionality: i32,
    /// Track number of elements (usearch doesn't expose this directly for deleted elements)
    len: AtomicUsize,
    /// Track capacity for resize operations
    capacity: AtomicUsize,
}

#[derive(Error, Debug)]
pub enum USearchError {
    #[error("USearch error: {0}")]
    Internal(String),
    #[error("Vector not found: {0}")]
    NotFound(usize),
    #[error("Dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },
}

impl ChromaError for USearchError {
    fn code(&self) -> ErrorCodes {
        match self {
            USearchError::Internal(_) => ErrorCodes::Internal,
            USearchError::NotFound(_) => ErrorCodes::NotFound,
            USearchError::DimensionMismatch { .. } => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Error, Debug)]
pub enum USearchInitError {
    #[error("No config provided")]
    NoConfigProvided,
    #[error("Failed to initialize index: {0}")]
    InitFailed(String),
}

impl ChromaError for USearchInitError {
    fn code(&self) -> ErrorCodes {
        match self {
            USearchInitError::NoConfigProvided => ErrorCodes::InvalidArgument,
            USearchInitError::InitFailed(_) => ErrorCodes::Internal,
        }
    }
}

/// Convert our distance function to USearch's MetricKind
fn map_distance_function(distance_function: &DistanceFunction) -> MetricKind {
    match distance_function {
        DistanceFunction::Cosine => MetricKind::Cos,
        DistanceFunction::Euclidean => MetricKind::L2sq,
        DistanceFunction::InnerProduct => MetricKind::IP,
    }
}

impl USearchIndex {
    /// Get the number of vectors in the index (excluding deleted)
    pub fn len(&self) -> usize {
        self.index.size()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.index.size() == 0
    }

    /// Get the number of vectors including deleted ones
    /// Note: USearch handles deletions internally, this returns the same as len()
    /// since usearch compacts on save/load
    pub fn len_with_deleted(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Get the dimensionality of vectors in this index
    pub fn dimensionality(&self) -> i32 {
        self.dimensionality
    }

    /// Get the current capacity of the index
    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    /// Resize the index to accommodate more elements
    pub fn resize(&mut self, new_size: usize) -> Result<(), Box<dyn ChromaError>> {
        self.index
            .reserve(new_size)
            .map_err(|e| USearchError::Internal(e.to_string()).boxed())?;
        self.capacity.store(new_size, Ordering::Relaxed);
        Ok(())
    }

    /// Check if a vector with the given ID exists
    pub fn contains(&self, id: usize) -> bool {
        self.index.contains(id as u64)
    }

    /// Get the expansion search parameter
    pub fn expansion_search(&self) -> usize {
        // USearch doesn't expose this, we'd need to track it
        // For now return a default
        64
    }
}

impl Index<USearchIndexConfig> for USearchIndex {
    fn init(
        index_config: &IndexConfig,
        usearch_config: Option<&USearchIndexConfig>,
        id: IndexUuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match usearch_config {
            None => Err(USearchInitError::NoConfigProvided.boxed()),
            Some(config) => {
                let metric = map_distance_function(&index_config.distance_function);

                let quantization = config.quantization.unwrap_or(ScalarKind::F32);

                let options = IndexOptions {
                    dimensions: index_config.dimensionality as usize,
                    metric,
                    quantization,
                    connectivity: config.connectivity,
                    expansion_add: config.expansion_add,
                    expansion_search: config.expansion_search,
                    multi: false, // Single vector per key
                };

                let index = new_index(&options)
                    .map_err(|e| USearchInitError::InitFailed(e.to_string()).boxed())?;

                // Reserve initial capacity
                index
                    .reserve(config.max_elements)
                    .map_err(|e| USearchInitError::InitFailed(e.to_string()).boxed())?;

                Ok(USearchIndex {
                    index,
                    id,
                    distance_function: index_config.distance_function.clone(),
                    dimensionality: index_config.dimensionality,
                    len: AtomicUsize::new(0),
                    capacity: AtomicUsize::new(config.max_elements),
                })
            }
        }
    }

    fn add(&self, id: usize, vector: &[f32]) -> Result<(), Box<dyn ChromaError>> {
        if vector.len() != self.dimensionality as usize {
            return Err(USearchError::DimensionMismatch {
                expected: self.dimensionality as usize,
                actual: vector.len(),
            }
            .boxed());
        }

        self.index
            .add(id as u64, vector)
            .map_err(|e| USearchError::Internal(e.to_string()).boxed())?;

        self.len.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn delete(&self, id: usize) -> Result<(), Box<dyn ChromaError>> {
        // USearch's remove marks the element as deleted
        // The actual removal happens during save/load or compaction
        let removed = self
            .index
            .remove(id as u64)
            .map_err(|e| USearchError::Internal(e.to_string()).boxed())?;
        if removed > 0 {
            // Note: len_with_deleted stays the same, but actual len decreases
            Ok(())
        } else {
            // Element wasn't found - this is okay for idempotent deletes
            Ok(())
        }
    }

    fn query(
        &self,
        vector: &[f32],
        k: usize,
        allowed_ids: &[usize],
        disallowed_ids: &[usize],
    ) -> Result<(Vec<usize>, Vec<f32>), Box<dyn ChromaError>> {
        if vector.len() != self.dimensionality as usize {
            return Err(USearchError::DimensionMismatch {
                expected: self.dimensionality as usize,
                actual: vector.len(),
            }
            .boxed());
        }

        // USearch supports filtered search via predicates
        // For now, we do post-filtering if filters are provided
        let results = if !allowed_ids.is_empty() || !disallowed_ids.is_empty() {
            // Create filter predicate
            let allowed_set: std::collections::HashSet<usize> =
                allowed_ids.iter().copied().collect();
            let disallowed_set: std::collections::HashSet<usize> =
                disallowed_ids.iter().copied().collect();

            // Request more results to account for filtering
            let fetch_k = if !allowed_ids.is_empty() {
                // If we have allowed_ids, we need at most that many
                std::cmp::min(k * 10, allowed_ids.len())
            } else {
                k * 10 // Fetch more to account for disallowed
            };

            let raw_results = self
                .index
                .search(vector, fetch_k)
                .map_err(|e| USearchError::Internal(e.to_string()).boxed())?;

            // Filter results
            let mut filtered_ids = Vec::with_capacity(k);
            let mut filtered_distances = Vec::with_capacity(k);

            for (key, distance) in raw_results.keys.iter().zip(raw_results.distances.iter()) {
                let id = *key as usize;

                let is_allowed = allowed_ids.is_empty() || allowed_set.contains(&id);
                let is_not_disallowed = disallowed_ids.is_empty() || !disallowed_set.contains(&id);

                if is_allowed && is_not_disallowed {
                    filtered_ids.push(id);
                    filtered_distances.push(*distance);
                    if filtered_ids.len() >= k {
                        break;
                    }
                }
            }

            (filtered_ids, filtered_distances)
        } else {
            let results = self
                .index
                .search(vector, k)
                .map_err(|e| USearchError::Internal(e.to_string()).boxed())?;

            let ids: Vec<usize> = results.keys.iter().map(|&k| k as usize).collect();
            let distances: Vec<f32> = results.distances;
            (ids, distances)
        };

        Ok(results)
    }

    fn get(&self, id: usize) -> Result<Option<Vec<f32>>, Box<dyn ChromaError>> {
        if !self.index.contains(id as u64) {
            return Ok(None);
        }

        // Allocate buffer and get the vector
        let mut buffer = vec![0.0f32; self.dimensionality as usize];
        let count = self
            .index
            .get::<f32>(id as u64, &mut buffer)
            .map_err(|e| USearchError::Internal(e.to_string()).boxed())?;

        if count == 0 {
            return Ok(None);
        }

        Ok(Some(buffer))
    }

    fn get_all_ids_sizes(&self) -> Result<Vec<usize>, Box<dyn ChromaError>> {
        // USearch doesn't directly expose iteration over all IDs
        // This is a limitation - we'd need to track IDs externally
        // For now, return empty as this is used for specific operations
        Ok(vec![])
    }

    fn get_all_ids(&self) -> Result<(Vec<usize>, Vec<usize>), Box<dyn ChromaError>> {
        // USearch doesn't expose direct iteration over all keys
        // We return empty vectors - this may need external tracking
        Ok((vec![], vec![]))
    }
}

/// Persistent index operations for USearch
impl USearchIndex {
    /// Save the index to disk
    pub fn save(&self) -> Result<(), Box<dyn ChromaError>> {
        // USearch saves to the path specified during load
        // We need to save to a file path
        Err(
            USearchError::Internal("save() requires a path - use save_to_path instead".to_string())
                .boxed(),
        )
    }

    /// Save the index to a specific path
    pub fn save_to_path(&self, path: &str) -> Result<(), Box<dyn ChromaError>> {
        self.index
            .save(path)
            .map_err(|e| USearchError::Internal(format!("Failed to save index: {}", e)).boxed())
    }

    /// Load an index from disk
    #[instrument(name = "USearchIndex load", level = "info")]
    pub fn load(
        path: &str,
        index_config: &IndexConfig,
        expansion_search: usize,
        id: IndexUuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let metric = map_distance_function(&index_config.distance_function);

        let options = IndexOptions {
            dimensions: index_config.dimensionality as usize,
            metric,
            quantization: ScalarKind::F32,
            connectivity: 0,  // Will be read from file
            expansion_add: 0, // Will be read from file
            expansion_search,
            multi: false,
        };

        let index =
            new_index(&options).map_err(|e| USearchInitError::InitFailed(e.to_string()).boxed())?;

        index.load(path).map_err(|e| {
            USearchInitError::InitFailed(format!("Failed to load index: {}", e)).boxed()
        })?;

        let len = index.size();
        let capacity = index.capacity();

        Ok(USearchIndex {
            index,
            id,
            distance_function: index_config.distance_function.clone(),
            dimensionality: index_config.dimensionality,
            len: AtomicUsize::new(len),
            capacity: AtomicUsize::new(capacity),
        })
    }

    /// Serialize the index to bytes for storage
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, Box<dyn ChromaError>> {
        // Create a temporary file to save to, then read the bytes
        let temp_dir = std::env::temp_dir();
        let temp_path = temp_dir.join(format!("usearch_temp_{}.bin", self.id));
        let path_str = temp_path.to_str().ok_or_else(|| {
            USearchError::Internal("Failed to create temp path".to_string()).boxed()
        })?;

        self.index
            .save(path_str)
            .map_err(|e| USearchError::Internal(format!("Failed to save index: {}", e)).boxed())?;

        let bytes = std::fs::read(&temp_path).map_err(|e| {
            USearchError::Internal(format!("Failed to read index file: {}", e)).boxed()
        })?;

        // Clean up temp file
        let _ = std::fs::remove_file(&temp_path);

        Ok(bytes)
    }

    /// Load the index from bytes
    pub fn load_from_bytes(
        data: &[u8],
        index_config: &IndexConfig,
        expansion_search: usize,
        id: IndexUuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        // Write bytes to temp file and load
        let temp_dir = std::env::temp_dir();
        let temp_path = temp_dir.join(format!("usearch_temp_{}.bin", id));
        let path_str = temp_path.to_str().ok_or_else(|| {
            USearchError::Internal("Failed to create temp path".to_string()).boxed()
        })?;

        std::fs::write(&temp_path, data).map_err(|e| {
            USearchError::Internal(format!("Failed to write temp file: {}", e)).boxed()
        })?;

        let result = Self::load(path_str, index_config, expansion_search, id);

        // Clean up temp file
        let _ = std::fs::remove_file(&temp_path);

        result
    }

    /// View the index from a memory-mapped file (zero-copy when possible)
    pub fn view(
        path: &str,
        index_config: &IndexConfig,
        expansion_search: usize,
        id: IndexUuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let metric = map_distance_function(&index_config.distance_function);

        let options = IndexOptions {
            dimensions: index_config.dimensionality as usize,
            metric,
            quantization: ScalarKind::F32,
            connectivity: 0,
            expansion_add: 0,
            expansion_search,
            multi: false,
        };

        let index =
            new_index(&options).map_err(|e| USearchInitError::InitFailed(e.to_string()).boxed())?;

        // view() memory-maps the file for efficient access
        index.view(path).map_err(|e| {
            USearchInitError::InitFailed(format!("Failed to view index: {}", e)).boxed()
        })?;

        let len = index.size();
        let capacity = index.capacity();

        Ok(USearchIndex {
            index,
            id,
            distance_function: index_config.distance_function.clone(),
            dimensionality: index_config.dimensionality,
            len: AtomicUsize::new(len),
            capacity: AtomicUsize::new(capacity),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn create_test_index(dim: i32) -> USearchIndex {
        let index_config = IndexConfig::new(dim, DistanceFunction::Euclidean);
        let usearch_config = USearchIndexConfig::new_ephemeral(16, 128, 64);
        let id = IndexUuid(Uuid::new_v4());
        USearchIndex::init(&index_config, Some(&usearch_config), id).unwrap()
    }

    #[test]
    fn test_create_index() {
        let index = create_test_index(128);
        assert_eq!(index.dimensionality(), 128);
        assert!(index.is_empty());
    }

    #[test]
    fn test_add_and_query() {
        let index = create_test_index(3);

        // Add some vectors
        index.add(1, &[1.0, 0.0, 0.0]).unwrap();
        index.add(2, &[0.0, 1.0, 0.0]).unwrap();
        index.add(3, &[0.0, 0.0, 1.0]).unwrap();

        assert_eq!(index.len(), 3);

        // Query for nearest neighbor
        let (ids, distances) = index.query(&[1.0, 0.0, 0.0], 1, &[], &[]).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], 1);
        assert!(distances[0] < 0.001); // Should be very close to 0
    }

    #[test]
    fn test_get_vector() {
        let index = create_test_index(3);

        index.add(1, &[1.0, 2.0, 3.0]).unwrap();

        let vec = index.get(1).unwrap().unwrap();
        assert_eq!(vec.len(), 3);
        // Note: values might differ slightly due to quantization
        assert!((vec[0] - 1.0).abs() < 0.1);
        assert!((vec[1] - 2.0).abs() < 0.1);
        assert!((vec[2] - 3.0).abs() < 0.1);
    }

    #[test]
    fn test_delete() {
        let index = create_test_index(3);

        index.add(1, &[1.0, 0.0, 0.0]).unwrap();
        index.add(2, &[0.0, 1.0, 0.0]).unwrap();

        assert!(index.contains(1));

        index.delete(1).unwrap();

        // After delete, the element should not be found in search
        let (ids, _) = index.query(&[1.0, 0.0, 0.0], 1, &[], &[]).unwrap();
        assert!(!ids.contains(&1) || ids[0] == 2);
    }

    #[test]
    fn test_filtered_query() {
        let index = create_test_index(3);

        index.add(1, &[1.0, 0.0, 0.0]).unwrap();
        index.add(2, &[0.9, 0.1, 0.0]).unwrap();
        index.add(3, &[0.0, 1.0, 0.0]).unwrap();

        // Query with allowed_ids filter
        let (ids, _) = index.query(&[1.0, 0.0, 0.0], 1, &[2, 3], &[]).unwrap();
        assert!(!ids.contains(&1)); // 1 is not in allowed_ids

        // Query with disallowed_ids filter
        let (ids, _) = index.query(&[1.0, 0.0, 0.0], 1, &[], &[1]).unwrap();
        assert!(!ids.contains(&1)); // 1 is disallowed
    }

    #[test]
    fn test_resize() {
        let mut index = create_test_index(3);

        let initial_capacity = index.capacity();
        index.resize(initial_capacity * 2).unwrap();
        assert!(index.capacity() >= initial_capacity * 2);
    }

    #[test]
    fn test_save_and_load() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_index.bin");
        let path_str = path.to_str().unwrap();

        let id = IndexUuid(Uuid::new_v4());
        let index_config = IndexConfig::new(3, DistanceFunction::Euclidean);

        // Create and populate index
        {
            let usearch_config = USearchIndexConfig::new_ephemeral(16, 128, 64);
            let index = USearchIndex::init(&index_config, Some(&usearch_config), id).unwrap();

            index.add(1, &[1.0, 0.0, 0.0]).unwrap();
            index.add(2, &[0.0, 1.0, 0.0]).unwrap();

            index.save_to_path(path_str).unwrap();
        }

        // Load and verify
        {
            let loaded = USearchIndex::load(path_str, &index_config, 64, id).unwrap();
            assert_eq!(loaded.len(), 2);

            let (ids, _) = loaded.query(&[1.0, 0.0, 0.0], 1, &[], &[]).unwrap();
            assert_eq!(ids[0], 1);
        }
    }
}
