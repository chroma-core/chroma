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
enum CacheKey {
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
            let bytes = code.as_ref();
            let i8_slice =
                unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const i8, bytes.len()) };
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
            let bytes = code.as_ref();
            let i8_slice =
                unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const i8, bytes.len()) };
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
                let a = unsafe { std::slice::from_raw_parts(a_ptr as *const u8, code_len) };
                let b = unsafe { std::slice::from_raw_parts(b_ptr as *const u8, code_len) };
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
