use std::sync::Arc;

use chroma_cache::{Cache, Weighted};
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, GetOptions, PutOptions, Storage, StorageError,
};
use chroma_types::{Cmek, CollectionUuid};
use crossbeam::sync::ShardedLock;
use simsimd::SpatialSimilarity;
use thiserror::Error;
use tracing::Instrument;
use usearch::{IndexOptions, MetricKind, ScalarKind};
use uuid::Uuid;

use crate::quantization::Code;
use crate::IndexUuid;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheKey {
    Raw(CollectionUuid),
    Quantized(CollectionUuid),
}

#[derive(Error, Debug)]
pub enum USearchError {
    #[error("Cache error: {0}")]
    Cache(#[from] chroma_cache::CacheError),
    #[error("Index error: {0}")]
    Index(String),
    #[error("Lock poisoned")]
    Poison,
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
}

impl<T> From<std::sync::PoisonError<T>> for USearchError {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl ChromaError for USearchError {
    fn code(&self) -> ErrorCodes {
        match self {
            USearchError::Cache(_) => ErrorCodes::Internal,
            USearchError::Index(_) => ErrorCodes::Internal,
            USearchError::Poison => ErrorCodes::Internal,
            USearchError::Storage(err) => err.code(),
        }
    }
}

#[derive(Clone)]
pub struct USearchIndex {
    id: IndexUuid,
    cache_key: CacheKey,
    index: Arc<ShardedLock<usearch::Index>>,
    prefix_path: String,
    quantization_center: Option<Arc<[f32]>>,
}

impl USearchIndex {
    /// Add a vector to the index with the given key.
    /// For quantized indexes, the vector will be encoded internally.
    pub fn add(&self, key: u64, vector: &[f32]) -> Result<(), USearchError> {
        let index = self.index.read()?;

        if let Some(center) = &self.quantization_center {
            let code = Code::<_>::quantize(vector, center);
            let i8_slice = bytemuck::cast_slice::<_, i8>(code.as_ref());
            index.add(key, i8_slice)
        } else {
            index.add(key, vector)
        }
        .map_err(|e| USearchError::Index(e.to_string()))
    }

    /// Mark a key as deleted in the index.
    pub fn delete(&self, key: u64) -> Result<(), USearchError> {
        self.index
            .read()?
            .remove(key)
            .map_err(|e| USearchError::Index(e.to_string()))?;
        Ok(())
    }

    /// Query for k nearest neighbors.
    /// Returns (keys, distances).
    pub fn query(&self, vector: &[f32], k: usize) -> Result<(Vec<u64>, Vec<f32>), USearchError> {
        let index = self.index.read()?;

        let matches = if let Some(center) = &self.quantization_center {
            let code = Code::<_>::quantize(vector, center);
            let i8_slice = bytemuck::cast_slice::<_, i8>(code.as_ref());
            index.search(i8_slice, k)
        } else {
            index.search(vector, k)
        }
        .map_err(|e| USearchError::Index(e.to_string()))?;

        Ok((matches.keys, matches.distances))
    }

    /// Retrieve the vector for a given key.
    /// Returns None if key doesn't exist or if this is a quantized index.
    pub fn get(&self, key: u64) -> Result<Option<Vec<f32>>, USearchError> {
        if self.quantization_center.is_some() {
            return Ok(None);
        }

        let mut vector = Vec::new();
        let count = self
            .index
            .read()?
            .export(key, &mut vector)
            .map_err(|e| USearchError::Index(e.to_string()))?;

        Ok((count > 0).then_some(vector))
    }

    /// Number of vectors in the index.
    pub fn len(&self) -> Result<usize, USearchError> {
        Ok(self.index.read()?.size())
    }

    /// Check if index is empty.
    pub fn is_empty(&self) -> Result<bool, USearchError> {
        Ok(self.len()? == 0)
    }

    /// Current capacity of the index.
    pub fn capacity(&self) -> Result<usize, USearchError> {
        Ok(self.index.read()?.capacity())
    }

    /// Reserve capacity for at least `capacity` vectors.
    /// No-op if current capacity is already sufficient.
    pub fn reserve(&self, capacity: usize) -> Result<(), USearchError> {
        self.index
            .write()?
            .reserve(capacity)
            .map_err(|e| USearchError::Index(e.to_string()))
    }

    pub fn format_storage_key(prefix_path: &str, id: IndexUuid) -> String {
        if prefix_path.is_empty() {
            format!("usearch/{}.usearch", id)
        } else {
            format!("{}/usearch/{}.usearch", prefix_path, id)
        }
    }

    /// Create a new empty index.
    fn new(
        id: IndexUuid,
        cache_key: CacheKey,
        prefix_path: &str,
        distance_function: DistanceFunction,
        options: IndexOptions,
        quantization_center: Option<Arc<[f32]>>,
    ) -> Result<Self, USearchError> {
        let mut index =
            usearch::Index::new(&options).map_err(|e| USearchError::Index(e.to_string()))?;

        if let Some(center) = &quantization_center {
            let c_norm = f32::dot(center, center).unwrap_or(0.0).sqrt() as f32;
            let dim = center.len();
            let df = distance_function;
            let code_len = Code::<&[u8]>::size(dim);
            index.change_metric::<i8>(Box::new(move |a_ptr, b_ptr| {
                // SAFETY: usearch passes valid pointers of `code_len` i8 elements
                let a_i8 = unsafe { std::slice::from_raw_parts(a_ptr, code_len) };
                let b_i8 = unsafe { std::slice::from_raw_parts(b_ptr, code_len) };
                let a = bytemuck::cast_slice(a_i8);
                let b = bytemuck::cast_slice(b_i8);
                Code::<_>::new(a).distance_code(&df, &Code::<_>::new(b), c_norm, dim)
            }));
        }

        Ok(Self {
            id,
            cache_key,
            index: Arc::new(index.into()),
            prefix_path: prefix_path.to_string(),
            quantization_center,
        })
    }

    /// Load serialized data into the index.
    fn load(&self, data: &[u8]) -> Result<(), USearchError> {
        self.index
            .write()?
            .load_from_buffer(data)
            .map_err(|e| USearchError::Index(e.to_string()))
    }
}

impl Weighted for USearchIndex {
    fn weight(&self) -> usize {
        let Ok(index) = self.index.read() else {
            return 1;
        };
        let bytes = index.memory_usage();
        (bytes / 1024 / 1024).max(1)
    }
}

/// Cache key ensures fairness: at most one index per collection per type
/// (raw vs quantized) in cache, preventing a single hot collection from
/// monopolizing cache space.
pub struct USearchIndexProvider {
    cache: Arc<dyn Cache<CacheKey, USearchIndex>>,
    storage: Storage,
}

impl USearchIndexProvider {
    /// Create a new provider with the given storage backend and cache.
    pub fn new(storage: Storage, cache: Box<dyn Cache<CacheKey, USearchIndex>>) -> Self {
        Self {
            cache: cache.into(),
            storage,
        }
    }

    /// Open an existing index from S3, or create a new empty index.
    ///
    /// - `id`: `None` to create new, `Some(id)` to load existing
    /// - `collection_id`: Cache key for fairness (one index per collection)
    /// - `prefix_path`: S3 path prefix for storage
    /// - `dimensions`: Vector dimensionality
    /// - `distance_function`: Distance metric (Cosine, Euclidean, InnerProduct)
    /// - `connectivity`: HNSW M parameter
    /// - `expansion_add`: HNSW ef_construction parameter
    /// - `expansion_search`: HNSW ef_search parameter
    /// - `quantization_center`: If provided, use I8 quantization with custom metric
    /// - `fork`: If true, fork from existing index for writes (new UUID, clone data).
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        &self,
        id: Option<IndexUuid>,
        collection_id: CollectionUuid,
        prefix_path: &str,
        dimensions: usize,
        distance_function: DistanceFunction,
        connectivity: usize,
        expansion_add: usize,
        expansion_search: usize,
        quantization_center: Option<Arc<[f32]>>,
        fork: bool,
    ) -> Result<USearchIndex, USearchError> {
        let (cache_key, scalar, index_dimensions) = match &quantization_center {
            Some(_) => (
                CacheKey::Quantized(collection_id),
                ScalarKind::I8,
                Code::<&[u8]>::size(dimensions),
            ),
            None => (CacheKey::Raw(collection_id), ScalarKind::F32, dimensions),
        };

        let metric = match distance_function {
            DistanceFunction::Cosine => MetricKind::Cos,
            DistanceFunction::Euclidean => MetricKind::L2sq,
            DistanceFunction::InnerProduct => MetricKind::IP,
        };

        let options = IndexOptions {
            dimensions: index_dimensions,
            metric,
            quantization: scalar,
            connectivity,
            expansion_add,
            expansion_search,
            multi: false,
        };

        // Check cache first for existing index
        if let Some(id) = id {
            if let Some(index) = self
                .get_cache(
                    id,
                    cache_key,
                    prefix_path,
                    distance_function.clone(),
                    options.clone(),
                    quantization_center.clone(),
                    fork,
                )
                .await?
            {
                return Ok(index);
            }
        }

        let new_id = id
            .filter(|_| !fork)
            .unwrap_or_else(|| IndexUuid(Uuid::new_v4()));
        let index = USearchIndex::new(
            new_id,
            cache_key,
            prefix_path,
            distance_function.clone(),
            options.clone(),
            quantization_center.clone(),
        )?;

        // Load from S3 if existing index
        if let Some(id) = id {
            let key = USearchIndex::format_storage_key(prefix_path, id);
            let bytes = self
                .storage
                .get(&key, GetOptions::new(StorageRequestPriority::P0))
                .instrument(tracing::trace_span!("fetch_usearch_index", %id, %collection_id))
                .await?;

            // Double-check cache after fetch (another thread may have loaded it)
            if let Some(index) = self
                .get_cache(
                    id,
                    cache_key,
                    prefix_path,
                    distance_function,
                    options,
                    quantization_center,
                    fork,
                )
                .await?
            {
                return Ok(index);
            }

            index.load(&bytes)?;
            self.cache.insert(cache_key, index.clone()).await;
        }

        Ok(index)
    }

    /// Finalize the index and return its ID for later retrieval.
    pub fn commit(&self, index: &USearchIndex) -> IndexUuid {
        index.id
    }

    /// Flush the index to S3 and insert into cache.
    pub async fn flush(
        &self,
        index: &USearchIndex,
        cmek: Option<Cmek>,
    ) -> Result<(), USearchError> {
        // USearch uses the buffer directly via memcpy - no serialization/deserialization
        // cost, just pointer arithmetic and memory copies. Safe to run on async runtime.
        let buffer = {
            let guard = index.index.read()?;
            let len = guard.serialized_length();
            let mut buffer = vec![0u8; len];
            guard
                .save_to_buffer(&mut buffer)
                .map_err(|e| USearchError::Index(e.to_string()))?;
            buffer
        };

        let key = USearchIndex::format_storage_key(&index.prefix_path, index.id);
        let mut options = PutOptions::default().with_priority(StorageRequestPriority::P0);
        if let Some(cmek) = cmek {
            options = options.with_cmek(cmek);
        }

        self.storage
            .put_bytes(&key, buffer, options)
            .instrument(tracing::trace_span!("flush_usearch_index", id = %index.id))
            .await?;

        self.cache.insert(index.cache_key, index.clone()).await;

        Ok(())
    }

    /// Get index from cache, fork if requested.
    #[allow(clippy::too_many_arguments)]
    async fn get_cache(
        &self,
        id: IndexUuid,
        cache_key: CacheKey,
        prefix_path: &str,
        distance_function: DistanceFunction,
        options: IndexOptions,
        quantization_center: Option<Arc<[f32]>>,
        fork: bool,
    ) -> Result<Option<USearchIndex>, USearchError> {
        let Some(cached) = self.cache.get(&cache_key).await? else {
            return Ok(None);
        };
        if cached.id != id {
            return Ok(None);
        }
        if !fork {
            return Ok(Some(cached));
        }

        let buffer = {
            let guard = cached.index.read()?;
            let mut buffer = vec![0u8; guard.serialized_length()];
            guard
                .save_to_buffer(&mut buffer)
                .map_err(|e| USearchError::Index(e.to_string()))?;
            buffer
        };

        let index = USearchIndex::new(
            IndexUuid(Uuid::new_v4()),
            cache_key,
            prefix_path,
            distance_function,
            options,
            quantization_center,
        )?;
        index.load(&buffer)?;
        Ok(Some(index))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chroma_cache::new_non_persistent_cache_for_test;
    use chroma_storage::test_storage;
    use chroma_types::CollectionUuid;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    fn random_vector(rng: &mut impl Rng, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| rng.gen_range(-4.0_f32..4.0)).collect()
    }

    #[tokio::test]
    async fn test_persist() {
        let (_temp_dir, storage) = test_storage();
        let collection_id = CollectionUuid(Uuid::new_v4());
        const DIM: usize = 1024;

        // Generate all test vectors upfront
        let mut rng = StdRng::seed_from_u64(42);
        let mut vectors: HashMap<u64, Vec<f32>> = HashMap::new();
        for i in 0..64 {
            vectors.insert(i, random_vector(&mut rng, DIM));
        }

        // Phase 1: Create new index, add 32 vectors, flush
        let index_a_id = {
            let provider =
                USearchIndexProvider::new(storage.clone(), new_non_persistent_cache_for_test());
            let index = provider
                .open(
                    None,
                    collection_id,
                    "",
                    DIM,
                    DistanceFunction::Euclidean,
                    16,
                    128,
                    64,
                    None,
                    false,
                )
                .await
                .unwrap();
            index.reserve(32).unwrap();
            for i in 0..32 {
                index.add(i, &vectors[&i]).unwrap();
            }
            let id = provider.commit(&index);
            provider.flush(&index, None).await.unwrap();
            id
        };

        // Phase 2: Recreate provider, verify persistence, fork, add 32 more, flush
        let index_b_id = {
            let provider =
                USearchIndexProvider::new(storage.clone(), new_non_persistent_cache_for_test());
            let index = provider
                .open(
                    Some(index_a_id),
                    collection_id,
                    "",
                    DIM,
                    DistanceFunction::Euclidean,
                    16,
                    128,
                    64,
                    None,
                    false,
                )
                .await
                .unwrap();
            assert_eq!(provider.commit(&index), index_a_id);
            assert_eq!(index.len().unwrap(), 32);
            for i in 0..32 {
                assert_eq!(index.get(i).unwrap().unwrap(), vectors[&i]);
            }

            // Fork and add 32 more vectors
            let forked = provider
                .open(
                    Some(index_a_id),
                    collection_id,
                    "",
                    DIM,
                    DistanceFunction::Euclidean,
                    16,
                    128,
                    64,
                    None,
                    true,
                )
                .await
                .unwrap();
            let forked_id = provider.commit(&forked);
            assert_ne!(forked_id, index_a_id);
            forked.reserve(64).unwrap();
            for i in 32..64 {
                forked.add(i, &vectors[&i]).unwrap();
            }
            provider.flush(&forked, None).await.unwrap();
            forked_id
        };

        // Phase 3: Recreate provider, verify isolation
        {
            let provider =
                USearchIndexProvider::new(storage.clone(), new_non_persistent_cache_for_test());

            // Original unchanged
            let index_a = provider
                .open(
                    Some(index_a_id),
                    collection_id,
                    "",
                    DIM,
                    DistanceFunction::Euclidean,
                    16,
                    128,
                    64,
                    None,
                    false,
                )
                .await
                .unwrap();
            assert_eq!(index_a.len().unwrap(), 32);
            assert!(index_a.get(32).unwrap().is_none());
            for i in 0..32 {
                assert_eq!(index_a.get(i).unwrap().unwrap(), vectors[&i]);
            }

            // Forked has all 64 vectors
            let index_b = provider
                .open(
                    Some(index_b_id),
                    collection_id,
                    "",
                    DIM,
                    DistanceFunction::Euclidean,
                    16,
                    128,
                    64,
                    None,
                    false,
                )
                .await
                .unwrap();
            assert_eq!(index_b.len().unwrap(), 64);
            for i in 0..64 {
                assert_eq!(index_b.get(i).unwrap().unwrap(), vectors[&i]);
            }
        }
    }

    #[tokio::test]
    async fn test_quantize() {
        let (_temp_dir, storage) = test_storage();
        let collection_id = CollectionUuid(Uuid::new_v4());
        let provider =
            USearchIndexProvider::new(storage.clone(), new_non_persistent_cache_for_test());
        const DIM: usize = 1024;

        // Generate 128 random vectors
        let mut rng = StdRng::seed_from_u64(42);
        let vectors = (0..128)
            .map(|_| random_vector(&mut rng, DIM))
            .collect::<Vec<_>>();

        // Phase 1: Full precision index
        let raw_index = provider
            .open(
                None,
                collection_id,
                "",
                DIM,
                DistanceFunction::Euclidean,
                16,
                128,
                64,
                None,
                false,
            )
            .await
            .unwrap();
        raw_index.reserve(128).unwrap();
        for (i, v) in vectors.iter().enumerate() {
            raw_index.add(i as u64, v).unwrap();
        }

        // Verify top-1 recall is 100%
        for (i, v) in vectors.iter().enumerate() {
            let (keys, _) = raw_index.query(v, 1).unwrap();
            assert_eq!(keys[0], i as u64, "Full precision: top-1 mismatch at {}", i);
        }

        // Phase 2: Quantized index (center at origin)
        let center = Arc::from(vec![0.0f32; DIM]);
        let quantized_index = provider
            .open(
                None,
                collection_id,
                "",
                DIM,
                DistanceFunction::Euclidean,
                16,
                128,
                64,
                Some(center),
                false,
            )
            .await
            .unwrap();
        quantized_index.reserve(128).unwrap();
        for (i, v) in vectors.iter().enumerate() {
            quantized_index.add(i as u64, v).unwrap();
        }

        // Verify top-1 recall is 100% and distance relative error < 2%
        for (i, v) in vectors.iter().enumerate() {
            let (keys, distances) = quantized_index.query(v, 8).unwrap();
            assert_eq!(keys[0], i as u64, "Quantized: top-1 mismatch at {}", i);

            // Check distance relative error for results 2-8 (skip first which is self-match)
            for (&key, &quantized_dist_sq) in keys.iter().zip(distances.iter()).skip(1) {
                let true_dist_sq =
                    f32::sqeuclidean(v, &vectors[key as usize]).unwrap_or(0.0) as f32;
                let relative_err = (quantized_dist_sq - true_dist_sq).abs() / true_dist_sq;
                assert!(
                    relative_err < 0.02,
                    "Distance relative error {} > 2% for query {} -> key {} (quantized: {}, true: {})",
                    relative_err,
                    i,
                    key,
                    quantized_dist_sq,
                    true_dist_sq
                );
            }
        }
    }
}
