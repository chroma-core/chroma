use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

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
use tokio::task::spawn_blocking;
use tracing::Instrument;
use usearch::{IndexOptions, MetricKind, ScalarKind};
use uuid::Uuid;

use crate::{
    quantization::Code, IndexUuid, OpenMode, SearchResult, VectorIndex, VectorIndexProvider,
};

/// Configuration for opening a USearch index.
#[derive(Clone)]
pub struct USearchIndexConfig {
    /// Collection ID used as cache key for fairness (one index per collection per type).
    pub collection_id: CollectionUuid,
    /// Customer-managed encryption key for storage.
    pub cmek: Option<Cmek>,
    /// S3 path prefix for storage.
    pub prefix_path: String,
    /// Vector dimensionality.
    pub dimensions: usize,
    /// Distance metric (Cosine, Euclidean, InnerProduct).
    pub distance_function: DistanceFunction,
    /// HNSW M parameter (number of connections per node).
    pub connectivity: usize,
    /// HNSW ef_construction parameter (search width during index building).
    pub expansion_add: usize,
    /// HNSW ef_search parameter (search width during queries).
    pub expansion_search: usize,
    /// If provided, use RaBitQ quantization with this center point.
    pub quantization_center: Option<Arc<[f32]>>,
}

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
    #[error("Cannot retrieve embeddings from quantized index")]
    QuantizedEmbedding,
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
            USearchError::Cache(err) => err.code(),
            USearchError::Index(_) => ErrorCodes::Internal,
            USearchError::Poison => ErrorCodes::Internal,
            USearchError::QuantizedEmbedding => ErrorCodes::InvalidArgument,
            USearchError::Storage(err) => err.code(),
        }
    }
}

// NOTE(sicheng): The reserve buffer prevents concurrent adds passing capacity check at the
// same time and overflow the reserved space. The buffer size should be greater than the
// number of concurrent threads working on the same index.
const RESERVE_BUFFER: usize = 128;

#[derive(Clone)]
pub struct USearchIndex {
    id: IndexUuid,
    cache_key: CacheKey,
    cmek: Option<Cmek>,
    index: Arc<ShardedLock<usearch::Index>>,
    prefix_path: String,
    quantization_center: Option<Arc<[f32]>>,
    tombstones: Arc<AtomicUsize>,
}

impl USearchIndex {
    /// Format the storage key for this index.
    pub fn format_storage_key(prefix_path: &str, id: IndexUuid, quantized: bool) -> String {
        let kind = if quantized { "quantized" } else { "raw" };
        if prefix_path.is_empty() {
            format!("usearch/{}/{}.bin", kind, id)
        } else {
            format!("{}/usearch/{}/{}.bin", prefix_path, kind, id)
        }
    }

    /// Load serialized data into the index.
    async fn load(&self, data: Arc<Vec<u8>>) -> Result<(), USearchError> {
        let index = self.index.clone();
        let tombstones = self.tombstones.clone();
        spawn_blocking(move || {
            index
                .write()?
                .load_from_buffer(&data)
                .map_err(|e| USearchError::Index(e.to_string()))?;
            tombstones.store(0, Ordering::Relaxed);
            Ok(())
        })
        .await
        .map_err(|e| USearchError::Index(e.to_string()))?
    }

    /// Serialize the index to a buffer.
    async fn save(&self) -> Result<Vec<u8>, USearchError> {
        let index = self.index.clone();
        spawn_blocking(move || {
            let guard = index.write()?;
            let mut buffer = vec![0u8; guard.serialized_length()];
            guard
                .save_to_buffer(&mut buffer)
                .map_err(|e| USearchError::Index(e.to_string()))?;
            Ok(buffer)
        })
        .await
        .map_err(|e| USearchError::Index(e.to_string()))?
    }

    /// Create a new empty index.
    fn new(
        id: IndexUuid,
        cache_key: CacheKey,
        cmek: Option<Cmek>,
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
            cmek,
            index: Arc::new(index.into()),
            prefix_path: prefix_path.to_string(),
            quantization_center,
            tombstones: Default::default(),
        })
    }
}

impl VectorIndex for USearchIndex {
    type Error = USearchError;

    fn add(&self, key: u64, vector: &[f32]) -> Result<(), Self::Error> {
        let need_resize = {
            let index = self.index.read()?;
            let raw_size = index.size() + self.tombstones.load(Ordering::Relaxed);
            raw_size + RESERVE_BUFFER >= index.capacity()
        };

        if need_resize {
            let index = self.index.write()?;
            let raw_size = index.size() + self.tombstones.load(Ordering::Relaxed);
            if raw_size + RESERVE_BUFFER >= index.capacity() {
                let new_capacity = (index.capacity() * 2).max(RESERVE_BUFFER);
                index
                    .reserve(new_capacity)
                    .map_err(|e| USearchError::Index(e.to_string()))?;
            }
        }

        if let Some(center) = &self.quantization_center {
            let code = Code::<_>::quantize(vector, center);
            let i8_slice = bytemuck::cast_slice::<_, i8>(code.as_ref());
            self.index.read()?.add(key, i8_slice)
        } else {
            self.index.read()?.add(key, vector)
        }
        .map_err(|e| USearchError::Index(e.to_string()))
    }

    fn capacity(&self) -> Result<usize, Self::Error> {
        Ok(self.index.read()?.capacity())
    }

    fn get(&self, key: u64) -> Result<Option<Vec<f32>>, Self::Error> {
        if self.quantization_center.is_some() {
            return Err(USearchError::QuantizedEmbedding);
        }

        let mut vector = Vec::new();
        let count = self
            .index
            .read()?
            .export(key, &mut vector)
            .map_err(|e| USearchError::Index(e.to_string()))?;

        Ok((count > 0).then_some(vector))
    }

    fn len(&self) -> Result<usize, Self::Error> {
        Ok(self.index.read()?.size())
    }

    fn remove(&self, key: u64) -> Result<(), Self::Error> {
        self.tombstones.fetch_add(1, Ordering::Relaxed);
        self.index
            .read()?
            .remove(key)
            .map_err(|e| USearchError::Index(e.to_string()))?;
        Ok(())
    }

    fn reserve(&self, capacity: usize) -> Result<(), Self::Error> {
        self.index
            .write()?
            .reserve(capacity)
            .map_err(|e| USearchError::Index(e.to_string()))
    }

    fn search(&self, query: &[f32], count: usize) -> Result<SearchResult, Self::Error> {
        let matches = if let Some(center) = &self.quantization_center {
            let code = Code::<_>::quantize(query, center);
            let i8_slice = bytemuck::cast_slice::<_, i8>(code.as_ref());
            self.index.read()?.search(i8_slice, count)
        } else {
            self.index.read()?.search(query, count)
        }
        .map_err(|e| USearchError::Index(e.to_string()))?;

        Ok(SearchResult {
            keys: matches.keys,
            distances: matches.distances,
        })
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

    /// Get index from cache if it matches the given ID.
    /// If `fork` is true, returns a forked copy with a new UUID.
    async fn get_cache(
        &self,
        id: IndexUuid,
        cache_key: CacheKey,
        config: &USearchIndexConfig,
        options: &IndexOptions,
        fork: bool,
    ) -> Result<Option<USearchIndex>, USearchError> {
        let Some(cached) = self.cache.get(&cache_key).await?.filter(|c| c.id == id) else {
            return Ok(None);
        };

        if !fork {
            return Ok(Some(cached));
        }

        // Fork: serialize and create new index
        let buffer = cached.save().await?;
        let index = USearchIndex::new(
            IndexUuid(Uuid::new_v4()),
            cache_key,
            config.cmek.clone(),
            &config.prefix_path,
            config.distance_function.clone(),
            options.clone(),
            config.quantization_center.clone(),
        )?;
        index.load(buffer.into()).await?;
        Ok(Some(index))
    }
}

#[async_trait::async_trait]
impl VectorIndexProvider for USearchIndexProvider {
    type Index = USearchIndex;
    type Config = USearchIndexConfig;
    type Error = USearchError;

    async fn commit(&self, index: &Self::Index) -> Result<IndexUuid, Self::Error> {
        Ok(index.id)
    }

    async fn flush(&self, index: &Self::Index) -> Result<(), Self::Error> {
        let buffer = index.save().await?;

        let key = USearchIndex::format_storage_key(
            &index.prefix_path,
            index.id,
            index.quantization_center.is_some(),
        );
        let mut options = PutOptions::default().with_priority(StorageRequestPriority::P0);
        if let Some(cmek) = &index.cmek {
            options = options.with_cmek(cmek.clone());
        }

        self.storage
            .put_bytes(&key, buffer, options)
            .instrument(tracing::trace_span!("flush_usearch_index", id = %index.id))
            .await?;

        self.cache.insert(index.cache_key, index.clone()).await;

        Ok(())
    }

    async fn open(
        &self,
        config: &Self::Config,
        mode: OpenMode,
    ) -> Result<Self::Index, Self::Error> {
        let (cache_key, scalar, index_dimensions) = match &config.quantization_center {
            Some(_) => (
                CacheKey::Quantized(config.collection_id),
                ScalarKind::I8,
                Code::<&[u8]>::size(config.dimensions),
            ),
            None => (
                CacheKey::Raw(config.collection_id),
                ScalarKind::F32,
                config.dimensions,
            ),
        };

        let metric = match config.distance_function {
            DistanceFunction::Cosine => MetricKind::Cos,
            DistanceFunction::Euclidean => MetricKind::L2sq,
            DistanceFunction::InnerProduct => MetricKind::IP,
        };

        let options = IndexOptions {
            dimensions: index_dimensions,
            metric,
            quantization: scalar,
            connectivity: config.connectivity,
            expansion_add: config.expansion_add,
            expansion_search: config.expansion_search,
            multi: false,
        };

        match mode {
            OpenMode::Create => USearchIndex::new(
                IndexUuid(Uuid::new_v4()),
                cache_key,
                config.cmek.clone(),
                &config.prefix_path,
                config.distance_function.clone(),
                options,
                config.quantization_center.clone(),
            ),
            OpenMode::Open(id) | OpenMode::Fork(id) => {
                let is_fork = matches!(mode, OpenMode::Fork(_));

                // Check cache (returns forked copy if is_fork)
                if let Some(index) = self
                    .get_cache(id, cache_key, config, &options, is_fork)
                    .await?
                {
                    return Ok(index);
                }

                // Load from S3
                let key = USearchIndex::format_storage_key(
                    &config.prefix_path,
                    id,
                    config.quantization_center.is_some(),
                );
                let bytes = self
                    .storage
                    .get(&key, GetOptions::new(StorageRequestPriority::P0))
                    .instrument(tracing::trace_span!("fetch_usearch_index", %id, collection_id = %config.collection_id))
                    .await?;

                // Double-check cache after fetch (another thread may have loaded it)
                if let Some(index) = self
                    .get_cache(id, cache_key, config, &options, is_fork)
                    .await?
                {
                    return Ok(index);
                }

                // Create index
                let new_id = if is_fork {
                    IndexUuid(Uuid::new_v4())
                } else {
                    id
                };
                let index = USearchIndex::new(
                    new_id,
                    cache_key,
                    config.cmek.clone(),
                    &config.prefix_path,
                    config.distance_function.clone(),
                    options,
                    config.quantization_center.clone(),
                )?;
                index.load(bytes).await?;

                if !is_fork {
                    self.cache.insert(cache_key, index.clone()).await;
                }

                Ok(index)
            }
        }
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

        let config = USearchIndexConfig {
            collection_id,
            cmek: None,
            prefix_path: String::new(),
            dimensions: DIM,
            distance_function: DistanceFunction::Euclidean,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
            quantization_center: None,
        };

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
            let index = provider.open(&config, OpenMode::Create).await.unwrap();
            for i in 0..32 {
                index.add(i, &vectors[&i]).unwrap();
            }
            let id = provider.commit(&index).await.unwrap();
            provider.flush(&index).await.unwrap();
            id
        };

        // Phase 2: Recreate provider, verify persistence, fork, add 32 more, flush
        let index_b_id = {
            let provider =
                USearchIndexProvider::new(storage.clone(), new_non_persistent_cache_for_test());
            let index = provider
                .open(&config, OpenMode::Open(index_a_id))
                .await
                .unwrap();
            assert_eq!(provider.commit(&index).await.unwrap(), index_a_id);
            assert_eq!(index.len().unwrap(), 32);
            for i in 0..32 {
                assert_eq!(index.get(i).unwrap().unwrap(), vectors[&i]);
            }

            // Fork and add 32 more vectors
            let forked = provider
                .open(&config, OpenMode::Fork(index_a_id))
                .await
                .unwrap();
            let forked_id = provider.commit(&forked).await.unwrap();
            assert_ne!(forked_id, index_a_id);
            for i in 32..64 {
                forked.add(i, &vectors[&i]).unwrap();
            }
            provider.flush(&forked).await.unwrap();
            forked_id
        };

        // Phase 3: Recreate provider, verify isolation
        {
            let provider =
                USearchIndexProvider::new(storage.clone(), new_non_persistent_cache_for_test());

            // Original unchanged
            let index_a = provider
                .open(&config, OpenMode::Open(index_a_id))
                .await
                .unwrap();
            assert_eq!(index_a.len().unwrap(), 32);
            assert!(index_a.get(32).unwrap().is_none());
            for i in 0..32 {
                assert_eq!(index_a.get(i).unwrap().unwrap(), vectors[&i]);
            }

            // Forked has all 64 vectors
            let index_b = provider
                .open(&config, OpenMode::Open(index_b_id))
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

        let config = USearchIndexConfig {
            collection_id,
            cmek: None,
            prefix_path: String::new(),
            dimensions: DIM,
            distance_function: DistanceFunction::Euclidean,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
            quantization_center: None,
        };

        // Generate 128 random vectors
        let mut rng = StdRng::seed_from_u64(42);
        let vectors = (0..128)
            .map(|_| random_vector(&mut rng, DIM))
            .collect::<Vec<_>>();

        // Phase 1: Full precision index
        let raw_index = provider.open(&config, OpenMode::Create).await.unwrap();
        for (i, v) in vectors.iter().enumerate() {
            raw_index.add(i as u64, v).unwrap();
        }

        // Verify top-1 recall is 100%
        for (i, v) in vectors.iter().enumerate() {
            let result = raw_index.search(v, 1).unwrap();
            assert_eq!(
                result.keys[0], i as u64,
                "Full precision: top-1 mismatch at {}",
                i
            );
        }

        // Phase 2: Quantized index (center at origin)
        let center = Arc::from(vec![0.0f32; DIM]);
        let quantized_config = USearchIndexConfig {
            quantization_center: Some(center),
            ..config
        };
        let quantized_index = provider
            .open(&quantized_config, OpenMode::Create)
            .await
            .unwrap();
        for (i, v) in vectors.iter().enumerate() {
            quantized_index.add(i as u64, v).unwrap();
        }

        // Verify top-1 recall is 100% and distance relative error < 2%
        for (i, v) in vectors.iter().enumerate() {
            let result = quantized_index.search(v, 8).unwrap();
            assert_eq!(
                result.keys[0], i as u64,
                "Quantized: top-1 mismatch at {}",
                i
            );

            // Check distance relative error for results 2-8 (skip first which is self-match)
            for (&key, &quantized_dist_sq) in
                result.keys.iter().zip(result.distances.iter()).skip(1)
            {
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
