//! Binary quantized index for fast centroid lookup.
//!
//! This module implements a simple flat index that uses sign-bit binary quantization
//! and SIMD-accelerated Hamming distance for fast approximate nearest neighbor search.
//!
//! ## Binary Quantization
//!
//! Each f32 dimension is quantized to a single bit based on its sign relative to a center:
//! - `bit[i] = 1` if `vector[i] - center[i] >= 0`
//! - `bit[i] = 0` if `vector[i] - center[i] < 0`
//!
//! This provides 32x compression (1024-dim f32 = 4KB → 128 bytes).
//!
//! ## Distance Computation
//!
//! Hamming distance (number of differing bits) approximates angular distance for
//! normalized vectors. Uses simsimd's SIMD-accelerated implementation which
//! auto-selects ARM NEON, x86 AVX2/AVX-512, or portable fallback.

use std::collections::HashMap;
use std::sync::Arc;

use chroma_error::{ChromaError, ErrorCodes};
use parking_lot::RwLock;
use simsimd::BinarySimilarity;
use thiserror::Error;

use crate::{SearchResult, VectorIndex};

/// Error type for binary quantized index operations.
#[derive(Error, Debug)]
pub enum BinaryQuantizedError {
    #[error("Key not found: {0}")]
    KeyNotFound(u32),
    #[error("Dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
}

impl ChromaError for BinaryQuantizedError {
    fn code(&self) -> ErrorCodes {
        match self {
            BinaryQuantizedError::KeyNotFound(_) => ErrorCodes::NotFound,
            BinaryQuantizedError::DimensionMismatch { .. } => ErrorCodes::InvalidArgument,
        }
    }
}

/// An entry in the binary quantized index.
#[derive(Clone)]
struct Entry {
    /// Unique key for this vector.
    key: u32,
    /// Sign-bit quantized binary code (dim/8 bytes).
    binary: Vec<u8>,
    /// Original f32 vector (for `get()` support).
    raw: Vec<f32>,
}

/// Configuration for creating a binary quantized index.
#[derive(Clone)]
pub struct BinaryQuantizedConfig {
    /// Vector dimensionality.
    pub dimensions: usize,
    /// Quantization center for sign-relative encoding.
    /// If None, uses origin (all zeros).
    pub center: Option<Arc<[f32]>>,
}

/// A flat index using binary quantization and Hamming distance.
///
/// Provides O(n) brute-force search which is fast for small-to-medium
/// datasets (<100K vectors) due to SIMD-accelerated Hamming distance.
#[derive(Clone)]
pub struct BinaryQuantizedIndex {
    /// Vector dimensionality.
    dimensions: usize,
    /// Quantization center for sign-relative encoding.
    center: Arc<[f32]>,
    /// Stored entries.
    entries: Arc<RwLock<Vec<Entry>>>,
    /// Key to index mapping for O(1) lookup/removal.
    key_to_idx: Arc<RwLock<HashMap<u32, usize>>>,
}

impl BinaryQuantizedIndex {
    /// Creates a new binary quantized index.
    pub fn new(config: &BinaryQuantizedConfig) -> Self {
        let center = config
            .center
            .clone()
            .unwrap_or_else(|| vec![0.0; config.dimensions].into());

        Self {
            dimensions: config.dimensions,
            center,
            entries: Arc::new(RwLock::new(Vec::new())),
            key_to_idx: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns the number of bytes needed for the binary code.
    pub fn code_size(&self) -> usize {
        binary_code_size(self.dimensions)
    }

    /// Returns the quantization center.
    pub fn center(&self) -> &[f32] {
        &self.center
    }

    /// Quantizes a vector to binary code relative to the center.
    fn quantize(&self, vector: &[f32]) -> Vec<u8> {
        binary_quantize(vector, &self.center)
    }
}

/// Returns the number of bytes needed for a binary code of given dimensions.
#[inline]
pub fn binary_code_size(dimensions: usize) -> usize {
    dimensions.div_ceil(8)
}

/// Quantizes a vector to binary code relative to a center.
///
/// Each bit represents the sign of (vector[i] - center[i]):
/// - bit = 1 if vector[i] >= center[i]
/// - bit = 0 if vector[i] < center[i]
pub fn binary_quantize(vector: &[f32], center: &[f32]) -> Vec<u8> {
    let mut code = vec![0u8; binary_code_size(vector.len())];

    for (i, (&v, &c)) in vector.iter().zip(center.iter()).enumerate() {
        if v >= c {
            code[i / 8] |= 1 << (i % 8);
        }
    }

    code
}

/// Computes Hamming distance between two binary codes using SIMD.
#[inline]
pub fn hamming_distance(a: &[u8], b: &[u8]) -> u32 {
    u8::hamming(a, b).unwrap_or(0.0) as u32
}

impl VectorIndex for BinaryQuantizedIndex {
    type Error = BinaryQuantizedError;

    fn add(&self, key: u32, vector: &[f32]) -> Result<(), Self::Error> {
        if vector.len() != self.dimensions {
            return Err(BinaryQuantizedError::DimensionMismatch {
                expected: self.dimensions,
                got: vector.len(),
            });
        }

        let binary = self.quantize(vector);
        let entry = Entry {
            key,
            binary,
            raw: vector.to_vec(),
        };

        let mut entries = self.entries.write();
        let mut key_to_idx = self.key_to_idx.write();

        // If key already exists, update in place
        if let Some(&idx) = key_to_idx.get(&key) {
            entries[idx] = entry;
        } else {
            let idx = entries.len();
            entries.push(entry);
            key_to_idx.insert(key, idx);
        }

        Ok(())
    }

    fn capacity(&self) -> Result<usize, Self::Error> {
        Ok(self.entries.read().capacity())
    }

    fn get(&self, key: u32) -> Result<Option<Vec<f32>>, Self::Error> {
        let entries = self.entries.read();
        let key_to_idx = self.key_to_idx.read();

        Ok(key_to_idx.get(&key).map(|&idx| entries[idx].raw.clone()))
    }

    fn len(&self) -> Result<usize, Self::Error> {
        Ok(self.entries.read().len())
    }

    fn remove(&self, key: u32) -> Result<(), Self::Error> {
        let mut entries = self.entries.write();
        let mut key_to_idx = self.key_to_idx.write();

        let Some(idx) = key_to_idx.remove(&key) else {
            return Ok(()); // Key not found, nothing to do
        };

        // Swap-remove: move last entry to this position
        let last_idx = entries.len() - 1;
        if idx != last_idx {
            let last_key = entries[last_idx].key;
            entries.swap(idx, last_idx);
            key_to_idx.insert(last_key, idx);
        }
        entries.pop();

        Ok(())
    }

    fn reserve(&self, capacity: usize) -> Result<(), Self::Error> {
        self.entries.write().reserve(capacity);
        self.key_to_idx.write().reserve(capacity);
        Ok(())
    }

    fn search(&self, query: &[f32], count: usize) -> Result<SearchResult, Self::Error> {
        if query.len() != self.dimensions {
            return Err(BinaryQuantizedError::DimensionMismatch {
                expected: self.dimensions,
                got: query.len(),
            });
        }

        let query_code = self.quantize(query);
        let entries = self.entries.read();

        if entries.is_empty() || count == 0 {
            return Ok(SearchResult::default());
        }

        // Compute Hamming distances for all entries
        let mut distances: Vec<(u32, u32)> = entries
            .iter()
            .map(|e| (e.key, hamming_distance(&query_code, &e.binary)))
            .collect();

        // Partial sort to get top-k (smallest Hamming distances)
        let k = count.min(distances.len());
        distances.select_nth_unstable_by_key(k - 1, |&(_, d)| d);
        distances.truncate(k);
        distances.sort_by_key(|&(_, d)| d);

        Ok(SearchResult {
            keys: distances.iter().map(|&(key, _)| key).collect(),
            // Convert Hamming distance to f32 for compatibility
            distances: distances.iter().map(|&(_, d)| d as f32).collect(),
        })
    }
}

impl BinaryQuantizedIndex {
    /// Search with two-stage retrieval: fast Hamming candidate selection + exact reranking.
    ///
    /// This provides significantly higher recall than pure binary quantization by:
    /// 1. Using fast Hamming distance to find `count * oversample_factor` candidates
    /// 2. Reranking candidates using exact cosine distance on full-precision vectors
    /// 3. Returning top `count` results
    ///
    /// # Arguments
    /// * `query` - Query vector (f32)
    /// * `count` - Number of results to return
    /// * `oversample_factor` - How many candidates to consider (e.g., 10 means 10x candidates)
    ///
    /// # Example
    /// ```ignore
    /// // Get top-10 with 10x oversampling (100 candidates reranked)
    /// let results = index.search_with_rerank(&query, 10, 10)?;
    /// ```
    pub fn search_with_rerank(
        &self,
        query: &[f32],
        count: usize,
        oversample_factor: usize,
    ) -> Result<SearchResult, BinaryQuantizedError> {
        use simsimd::SpatialSimilarity;

        if query.len() != self.dimensions {
            return Err(BinaryQuantizedError::DimensionMismatch {
                expected: self.dimensions,
                got: query.len(),
            });
        }

        let entries = self.entries.read();

        if entries.is_empty() || count == 0 {
            return Ok(SearchResult::default());
        }

        // Stage 1: Fast Hamming distance to get candidates
        let query_code = self.quantize(query);
        let num_candidates = (count * oversample_factor).min(entries.len());

        let mut hamming_distances: Vec<(usize, u32)> = entries
            .iter()
            .enumerate()
            .map(|(idx, e)| (idx, hamming_distance(&query_code, &e.binary)))
            .collect();

        // Partial sort to get top candidates by Hamming distance
        if num_candidates < hamming_distances.len() {
            hamming_distances.select_nth_unstable_by_key(num_candidates - 1, |&(_, d)| d);
            hamming_distances.truncate(num_candidates);
        }

        // Stage 2: Exact reranking using cosine distance on raw vectors
        // Note: simsimd::cos returns cosine DISTANCE (1 - similarity), not similarity
        let mut reranked: Vec<(u32, f32)> = hamming_distances
            .iter()
            .map(|&(idx, _)| {
                let entry = &entries[idx];
                let distance =
                    <f32 as SpatialSimilarity>::cos(query, &entry.raw).unwrap_or(1.0) as f32;
                (entry.key, distance)
            })
            .collect();

        // Sort by exact distance and truncate to final count
        reranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        reranked.truncate(count);

        Ok(SearchResult {
            keys: reranked.iter().map(|&(key, _)| key).collect(),
            distances: reranked.iter().map(|&(_, d)| d).collect(),
        })
    }

    /// Search with reranking using a custom distance function.
    ///
    /// Similar to `search_with_rerank` but allows specifying the distance function
    /// for the reranking stage.
    pub fn search_with_rerank_fn<F>(
        &self,
        query: &[f32],
        count: usize,
        oversample_factor: usize,
        distance_fn: F,
    ) -> Result<SearchResult, BinaryQuantizedError>
    where
        F: Fn(&[f32], &[f32]) -> f32,
    {
        if query.len() != self.dimensions {
            return Err(BinaryQuantizedError::DimensionMismatch {
                expected: self.dimensions,
                got: query.len(),
            });
        }

        let entries = self.entries.read();

        if entries.is_empty() || count == 0 {
            return Ok(SearchResult::default());
        }

        // Stage 1: Fast Hamming distance to get candidates
        let query_code = self.quantize(query);
        let num_candidates = (count * oversample_factor).min(entries.len());

        let mut hamming_distances: Vec<(usize, u32)> = entries
            .iter()
            .enumerate()
            .map(|(idx, e)| (idx, hamming_distance(&query_code, &e.binary)))
            .collect();

        // Partial sort to get top candidates by Hamming distance
        if num_candidates < hamming_distances.len() {
            hamming_distances.select_nth_unstable_by_key(num_candidates - 1, |&(_, d)| d);
            hamming_distances.truncate(num_candidates);
        }

        // Stage 2: Exact reranking using provided distance function
        let mut reranked: Vec<(u32, f32)> = hamming_distances
            .iter()
            .map(|&(idx, _)| {
                let entry = &entries[idx];
                let distance = distance_fn(query, &entry.raw);
                (entry.key, distance)
            })
            .collect();

        // Sort by exact distance and truncate to final count
        reranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        reranked.truncate(count);

        Ok(SearchResult {
            keys: reranked.iter().map(|&(key, _)| key).collect(),
            distances: reranked.iter().map(|&(_, d)| d).collect(),
        })
    }
}

// ============================================================================
// Provider Implementation
// ============================================================================

use chroma_cache::{Cache, Weighted};
use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, GetOptions, PutOptions, Storage, StorageError,
};
use chroma_types::{Cmek, CollectionUuid};
use tracing::Instrument;
use uuid::Uuid;

use crate::{IndexUuid, OpenMode, VectorIndexProvider};

/// Additional error types for provider operations.
#[derive(Error, Debug)]
pub enum BinaryQuantizedProviderError {
    #[error("Index error: {0}")]
    Index(#[from] BinaryQuantizedError),
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Cache error: {0}")]
    Cache(#[from] chroma_cache::CacheError),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl ChromaError for BinaryQuantizedProviderError {
    fn code(&self) -> ErrorCodes {
        match self {
            BinaryQuantizedProviderError::Index(e) => e.code(),
            BinaryQuantizedProviderError::Storage(e) => e.code(),
            BinaryQuantizedProviderError::Cache(e) => e.code(),
            BinaryQuantizedProviderError::Serialization(_) => ErrorCodes::Internal,
        }
    }
}

/// Configuration for the binary quantized index provider.
#[derive(Clone)]
pub struct BinaryQuantizedIndexConfig {
    /// Collection ID used as cache key.
    pub collection_id: CollectionUuid,
    /// Customer-managed encryption key for storage.
    pub cmek: Option<Cmek>,
    /// S3 path prefix for storage.
    pub prefix_path: String,
    /// Vector dimensionality.
    pub dimensions: usize,
    /// Quantization center for sign-relative encoding.
    pub center: Option<Arc<[f32]>>,
}

/// A persistable binary quantized index with ID and metadata.
#[derive(Clone)]
pub struct PersistableBinaryQuantizedIndex {
    /// Unique identifier for this index.
    pub id: IndexUuid,
    /// Cache key (collection ID).
    cache_key: CollectionUuid,
    /// Customer-managed encryption key.
    cmek: Option<Cmek>,
    /// S3 path prefix.
    prefix_path: String,
    /// The underlying index.
    inner: BinaryQuantizedIndex,
}

impl PersistableBinaryQuantizedIndex {
    /// Creates a new persistable index.
    fn new(
        id: IndexUuid,
        cache_key: CollectionUuid,
        cmek: Option<Cmek>,
        prefix_path: String,
        config: &BinaryQuantizedConfig,
    ) -> Self {
        Self {
            id,
            cache_key,
            cmek,
            prefix_path,
            inner: BinaryQuantizedIndex::new(config),
        }
    }

    /// Format the storage key for this index.
    pub fn format_storage_key(prefix_path: &str, id: IndexUuid) -> String {
        if prefix_path.is_empty() {
            format!("binary_quantized/{}.bin", id)
        } else {
            format!("{}/binary_quantized/{}.bin", prefix_path, id)
        }
    }

    /// Serialize the index to bytes.
    fn serialize(&self) -> Result<Vec<u8>, BinaryQuantizedProviderError> {
        let entries = self.inner.entries.read();
        let key_to_idx = self.inner.key_to_idx.read();

        // Calculate buffer size
        let header_size = 4 + 4 + self.inner.dimensions * 4; // dimensions + count + center
        let entry_size = |e: &Entry| 4 + e.binary.len() + e.raw.len() * 4; // key + binary + raw
        let total_size: usize = header_size + entries.iter().map(entry_size).sum::<usize>();

        let mut buffer = Vec::with_capacity(total_size);

        // Header: dimensions (u32) + count (u32) + center (f32 * dimensions)
        buffer.extend_from_slice(&(self.inner.dimensions as u32).to_le_bytes());
        buffer.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for &c in self.inner.center.iter() {
            buffer.extend_from_slice(&c.to_le_bytes());
        }

        // Entries: key (u32) + binary + raw
        for entry in entries.iter() {
            buffer.extend_from_slice(&entry.key.to_le_bytes());
            buffer.extend_from_slice(&entry.binary);
            for &v in entry.raw.iter() {
                buffer.extend_from_slice(&v.to_le_bytes());
            }
        }

        drop(entries);
        drop(key_to_idx);

        Ok(buffer)
    }

    /// Deserialize index from bytes.
    fn deserialize(
        id: IndexUuid,
        cache_key: CollectionUuid,
        cmek: Option<Cmek>,
        prefix_path: String,
        data: &[u8],
    ) -> Result<Self, BinaryQuantizedProviderError> {
        if data.len() < 8 {
            return Err(BinaryQuantizedProviderError::Serialization(
                "Buffer too small for header".to_string(),
            ));
        }

        let mut offset = 0;

        // Read dimensions
        let dimensions = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read count
        let count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        // Read center
        let mut center = Vec::with_capacity(dimensions);
        for _ in 0..dimensions {
            center.push(f32::from_le_bytes(
                data[offset..offset + 4].try_into().unwrap(),
            ));
            offset += 4;
        }

        let binary_size = binary_code_size(dimensions);

        // Read entries
        let mut entries = Vec::with_capacity(count);
        let mut key_to_idx = HashMap::with_capacity(count);

        for idx in 0..count {
            let key = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;

            let binary = data[offset..offset + binary_size].to_vec();
            offset += binary_size;

            let mut raw = Vec::with_capacity(dimensions);
            for _ in 0..dimensions {
                raw.push(f32::from_le_bytes(
                    data[offset..offset + 4].try_into().unwrap(),
                ));
                offset += 4;
            }

            key_to_idx.insert(key, idx);
            entries.push(Entry { key, binary, raw });
        }

        let inner = BinaryQuantizedIndex {
            dimensions,
            center: center.into(),
            entries: Arc::new(RwLock::new(entries)),
            key_to_idx: Arc::new(RwLock::new(key_to_idx)),
        };

        Ok(Self {
            id,
            cache_key,
            cmek,
            prefix_path,
            inner,
        })
    }
}

impl VectorIndex for PersistableBinaryQuantizedIndex {
    type Error = BinaryQuantizedError;

    fn add(&self, key: u32, vector: &[f32]) -> Result<(), Self::Error> {
        self.inner.add(key, vector)
    }

    fn capacity(&self) -> Result<usize, Self::Error> {
        self.inner.capacity()
    }

    fn get(&self, key: u32) -> Result<Option<Vec<f32>>, Self::Error> {
        self.inner.get(key)
    }

    fn len(&self) -> Result<usize, Self::Error> {
        self.inner.len()
    }

    fn remove(&self, key: u32) -> Result<(), Self::Error> {
        self.inner.remove(key)
    }

    fn reserve(&self, capacity: usize) -> Result<(), Self::Error> {
        self.inner.reserve(capacity)
    }

    fn search(&self, query: &[f32], count: usize) -> Result<SearchResult, Self::Error> {
        self.inner.search(query, count)
    }
}

impl PersistableBinaryQuantizedIndex {
    /// Search with two-stage retrieval: fast Hamming + exact cosine rerank.
    ///
    /// See [`BinaryQuantizedIndex::search_with_rerank`] for details.
    pub fn search_with_rerank(
        &self,
        query: &[f32],
        count: usize,
        oversample_factor: usize,
    ) -> Result<SearchResult, BinaryQuantizedError> {
        self.inner.search_with_rerank(query, count, oversample_factor)
    }
}

impl Weighted for PersistableBinaryQuantizedIndex {
    fn weight(&self) -> usize {
        // Approximate memory usage in MB
        let entries = self.inner.entries.read();
        let binary_bytes: usize = entries.iter().map(|e| e.binary.len()).sum();
        let raw_bytes: usize = entries.iter().map(|e| e.raw.len() * 4).sum();
        ((binary_bytes + raw_bytes) / 1024 / 1024).max(1)
    }
}

/// Provider for creating and loading binary quantized indexes.
#[derive(Clone)]
pub struct BinaryQuantizedIndexProvider {
    cache: Arc<dyn Cache<CollectionUuid, PersistableBinaryQuantizedIndex>>,
    storage: Storage,
}

impl BinaryQuantizedIndexProvider {
    /// Create a new provider with the given storage backend and cache.
    pub fn new(
        storage: Storage,
        cache: Box<dyn Cache<CollectionUuid, PersistableBinaryQuantizedIndex>>,
    ) -> Self {
        Self {
            cache: cache.into(),
            storage,
        }
    }

    /// Get index from cache if it matches the given ID.
    async fn get_cache(
        &self,
        id: IndexUuid,
        cache_key: CollectionUuid,
        config: &BinaryQuantizedIndexConfig,
        fork: bool,
    ) -> Result<Option<PersistableBinaryQuantizedIndex>, BinaryQuantizedProviderError> {
        let Some(cached) = self.cache.get(&cache_key).await?.filter(|c| c.id == id) else {
            return Ok(None);
        };

        if !fork {
            return Ok(Some(cached));
        }

        // Fork: serialize and deserialize with new ID
        let buffer = cached.serialize()?;
        let forked = PersistableBinaryQuantizedIndex::deserialize(
            IndexUuid(Uuid::new_v4()),
            cache_key,
            config.cmek.clone(),
            config.prefix_path.clone(),
            &buffer,
        )?;

        Ok(Some(forked))
    }
}

#[async_trait::async_trait]
impl VectorIndexProvider for BinaryQuantizedIndexProvider {
    type Index = PersistableBinaryQuantizedIndex;
    type Config = BinaryQuantizedIndexConfig;
    type Error = BinaryQuantizedProviderError;

    async fn flush(&self, index: &Self::Index) -> Result<IndexUuid, Self::Error> {
        let buffer = index.serialize()?;

        let key = PersistableBinaryQuantizedIndex::format_storage_key(&index.prefix_path, index.id);
        let mut options = PutOptions::default().with_priority(StorageRequestPriority::P0);
        if let Some(cmek) = &index.cmek {
            options = options.with_cmek(cmek.clone());
        }

        self.storage
            .put_bytes(&key, buffer, options)
            .instrument(tracing::trace_span!("flush_binary_quantized_index", id = %index.id))
            .await?;

        self.cache.insert(index.cache_key, index.clone()).await;

        Ok(index.id)
    }

    async fn open(
        &self,
        config: &Self::Config,
        mode: OpenMode,
    ) -> Result<Self::Index, Self::Error> {
        let binary_config = BinaryQuantizedConfig {
            dimensions: config.dimensions,
            center: config.center.clone(),
        };

        match mode {
            OpenMode::Create => Ok(PersistableBinaryQuantizedIndex::new(
                IndexUuid(Uuid::new_v4()),
                config.collection_id,
                config.cmek.clone(),
                config.prefix_path.clone(),
                &binary_config,
            )),
            OpenMode::Open(id) | OpenMode::Fork(id) => {
                let is_fork = matches!(mode, OpenMode::Fork(_));

                // Check cache
                if let Some(index) = self
                    .get_cache(id, config.collection_id, config, is_fork)
                    .await?
                {
                    return Ok(index);
                }

                // Load from storage
                let key =
                    PersistableBinaryQuantizedIndex::format_storage_key(&config.prefix_path, id);
                let bytes = self
                    .storage
                    .get(&key, GetOptions::new(StorageRequestPriority::P0))
                    .instrument(tracing::trace_span!("fetch_binary_quantized_index", %id, collection_id = %config.collection_id))
                    .await?;

                // Double-check cache after fetch
                if let Some(index) = self
                    .get_cache(id, config.collection_id, config, is_fork)
                    .await?
                {
                    return Ok(index);
                }

                // Deserialize
                let new_id = if is_fork {
                    IndexUuid(Uuid::new_v4())
                } else {
                    id
                };

                let index = PersistableBinaryQuantizedIndex::deserialize(
                    new_id,
                    config.collection_id,
                    config.cmek.clone(),
                    config.prefix_path.clone(),
                    &bytes,
                )?;

                if !is_fork {
                    self.cache.insert(config.collection_id, index.clone()).await;
                }

                Ok(index)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, Rng, SeedableRng};

    fn random_vector(rng: &mut impl Rng, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
    }

    #[test]
    fn test_binary_quantize() {
        let center = vec![0.0; 16];
        let vector = vec![
            1.0, -1.0, 1.0, -1.0, // byte 0: 0b0101 = 5
            1.0, 1.0, -1.0, -1.0, // byte 1: 0b0011 = 3
            -1.0, -1.0, -1.0, -1.0, // byte 2: 0b0000 = 0
            1.0, 1.0, 1.0, 1.0, // byte 3: 0b1111 = 15
        ];

        let code = binary_quantize(&vector, &center);
        assert_eq!(code.len(), 2);
        assert_eq!(code[0], 0b00110101); // bits 0-7: 1,0,1,0,1,1,0,0 (LSB first)
        assert_eq!(code[1], 0b11110000); // bits 8-15: 0,0,0,0,1,1,1,1
    }

    #[test]
    fn test_hamming_distance() {
        let a = vec![0b11110000u8, 0b00001111];
        let b = vec![0b11110000u8, 0b00001111];
        assert_eq!(hamming_distance(&a, &b), 0);

        let c = vec![0b00000000u8, 0b00000000];
        // a has 8 ones, c has 0 ones, XOR has 8 ones
        assert_eq!(hamming_distance(&a, &c), 8);
    }

    #[test]
    fn test_add_and_get() {
        let config = BinaryQuantizedConfig {
            dimensions: 128,
            center: None,
        };
        let index = BinaryQuantizedIndex::new(&config);

        let mut rng = StdRng::seed_from_u64(42);
        let v1 = random_vector(&mut rng, 128);
        let v2 = random_vector(&mut rng, 128);

        index.add(1, &v1).unwrap();
        index.add(2, &v2).unwrap();

        assert_eq!(index.len().unwrap(), 2);
        assert_eq!(index.get(1).unwrap().unwrap(), v1);
        assert_eq!(index.get(2).unwrap().unwrap(), v2);
        assert!(index.get(3).unwrap().is_none());
    }

    #[test]
    fn test_remove() {
        let config = BinaryQuantizedConfig {
            dimensions: 128,
            center: None,
        };
        let index = BinaryQuantizedIndex::new(&config);

        let mut rng = StdRng::seed_from_u64(42);
        for i in 0..10 {
            index.add(i, &random_vector(&mut rng, 128)).unwrap();
        }

        assert_eq!(index.len().unwrap(), 10);

        // Remove middle element
        index.remove(5).unwrap();
        assert_eq!(index.len().unwrap(), 9);
        assert!(index.get(5).unwrap().is_none());

        // Other elements still accessible
        assert!(index.get(0).unwrap().is_some());
        assert!(index.get(9).unwrap().is_some());
    }

    #[test]
    fn test_search_self_match() {
        let config = BinaryQuantizedConfig {
            dimensions: 1024,
            center: None,
        };
        let index = BinaryQuantizedIndex::new(&config);

        let mut rng = StdRng::seed_from_u64(42);
        let vectors: Vec<Vec<f32>> = (0..100).map(|_| random_vector(&mut rng, 1024)).collect();

        for (i, v) in vectors.iter().enumerate() {
            index.add(i as u32, v).unwrap();
        }

        // Each vector should find itself as top-1
        for (i, v) in vectors.iter().enumerate() {
            let result = index.search(v, 1).unwrap();
            assert_eq!(result.keys[0], i as u32, "Vector {} should find itself", i);
            assert_eq!(result.distances[0], 0.0, "Self-distance should be 0");
        }
    }

    #[test]
    fn test_search_top_k() {
        let config = BinaryQuantizedConfig {
            dimensions: 128,
            center: None,
        };
        let index = BinaryQuantizedIndex::new(&config);

        let mut rng = StdRng::seed_from_u64(42);
        for i in 0..50 {
            index.add(i, &random_vector(&mut rng, 128)).unwrap();
        }

        let query = random_vector(&mut rng, 128);
        let result = index.search(&query, 10).unwrap();

        assert_eq!(result.keys.len(), 10);
        assert_eq!(result.distances.len(), 10);

        // Distances should be sorted ascending
        for i in 1..result.distances.len() {
            assert!(
                result.distances[i] >= result.distances[i - 1],
                "Results should be sorted by distance"
            );
        }
    }

    #[test]
    fn test_search_with_rerank() {
        let config = BinaryQuantizedConfig {
            dimensions: 128,
            center: None,
        };

        let index = BinaryQuantizedIndex::new(&config);

        // Create normalized vectors
        let mut rng = StdRng::seed_from_u64(42);

        for i in 0..50 {
            let mut v = random_vector(&mut rng, 128);
            // Normalize
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            for x in &mut v {
                *x /= norm;
            }
            index.add(i as u32, &v).unwrap();
        }

        // Use a fresh random query
        let mut query = random_vector(&mut rng, 128);
        let norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
        for x in &mut query {
            *x /= norm;
        }

        // Get reranked results
        let reranked_result = index.search_with_rerank(&query, 10, 5).unwrap();

        // Should return 10 results
        assert_eq!(reranked_result.keys.len(), 10);
        assert_eq!(reranked_result.distances.len(), 10);

        // Reranked distances should be sorted ascending
        for i in 1..reranked_result.distances.len() {
            assert!(
                reranked_result.distances[i] >= reranked_result.distances[i - 1],
                "Results should be sorted by distance"
            );
        }

        // Reranked distances should be cosine distances (in [0, 2] range for normalized vectors)
        // Note: simsimd returns cosine distance which can be slightly outside [0,2] due to precision
        for d in &reranked_result.distances {
            assert!(
                *d >= -0.01 && *d <= 2.01,
                "Cosine distance should be roughly in [0, 2], got {}",
                d
            );
        }
    }

    #[test]
    fn test_rerank_improves_recall() {
        use std::collections::HashSet;

        let config = BinaryQuantizedConfig {
            dimensions: 256,
            center: None,
        };

        let index = BinaryQuantizedIndex::new(&config);

        // Generate normalized vectors
        let mut rng = StdRng::seed_from_u64(123);
        let mut vectors: Vec<Vec<f32>> = Vec::new();

        for i in 0..500 {
            let mut v = random_vector(&mut rng, 256);
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            for x in &mut v {
                *x /= norm;
            }
            index.add(i as u32, &v).unwrap();
            vectors.push(v);
        }

        // Compute ground truth for a query using exact cosine distance
        let query = &vectors[50];
        let mut ground_truth: Vec<(u32, f32)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| {
                // simsimd::cos returns cosine DISTANCE (1 - similarity)
                let dist = <f32 as simsimd::SpatialSimilarity>::cos(query.as_slice(), v.as_slice())
                    .unwrap_or(1.0) as f32;
                (i as u32, dist)
            })
            .collect();
        ground_truth.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let gt_top10: HashSet<u32> = ground_truth.iter().take(10).map(|(k, _)| *k).collect();

        // Measure recall for pure Hamming
        let hamming_result = index.search(query, 10).unwrap();
        let hamming_set: HashSet<u32> = hamming_result.keys.iter().copied().collect();
        let hamming_recall = hamming_set.intersection(&gt_top10).count() as f64 / 10.0;

        // Measure recall for reranked (10x oversample)
        let reranked_result = index.search_with_rerank(query, 10, 10).unwrap();
        let reranked_set: HashSet<u32> = reranked_result.keys.iter().copied().collect();
        let reranked_recall = reranked_set.intersection(&gt_top10).count() as f64 / 10.0;

        println!(
            "Hamming recall@10: {:.0}%, Reranked recall@10: {:.0}%",
            hamming_recall * 100.0,
            reranked_recall * 100.0
        );

        // Reranking should improve or maintain recall
        assert!(
            reranked_recall >= hamming_recall,
            "Reranking should not decrease recall"
        );
    }

    #[test]
    fn test_serialization_roundtrip() {
        let config = BinaryQuantizedConfig {
            dimensions: 128,
            center: Some(vec![0.5; 128].into()),
        };

        let index = PersistableBinaryQuantizedIndex::new(
            IndexUuid(Uuid::new_v4()),
            CollectionUuid(Uuid::new_v4()),
            None,
            String::new(),
            &config,
        );

        let mut rng = StdRng::seed_from_u64(42);
        let vectors: Vec<Vec<f32>> = (0..20).map(|_| random_vector(&mut rng, 128)).collect();

        for (i, v) in vectors.iter().enumerate() {
            index.add(i as u32, v).unwrap();
        }

        // Serialize
        let buffer = index.serialize().unwrap();

        // Deserialize
        let loaded = PersistableBinaryQuantizedIndex::deserialize(
            index.id,
            index.cache_key,
            None,
            String::new(),
            &buffer,
        )
        .unwrap();

        // Verify
        assert_eq!(loaded.len().unwrap(), 20);
        for (i, v) in vectors.iter().enumerate() {
            assert_eq!(loaded.get(i as u32).unwrap().unwrap(), *v);
        }

        // Verify search still works
        let result = loaded.search(&vectors[0], 1).unwrap();
        assert_eq!(result.keys[0], 0);
    }

    #[tokio::test]
    async fn test_provider_persist() {
        use chroma_cache::new_non_persistent_cache_for_test;
        use chroma_storage::test_storage;

        let (_temp_dir, storage) = test_storage();
        let collection_id = CollectionUuid(Uuid::new_v4());
        const DIM: usize = 256;

        let config = BinaryQuantizedIndexConfig {
            collection_id,
            cmek: None,
            prefix_path: String::new(),
            dimensions: DIM,
            center: None,
        };

        // Generate test vectors
        let mut rng = StdRng::seed_from_u64(42);
        let vectors: Vec<Vec<f32>> = (0..32).map(|_| random_vector(&mut rng, DIM)).collect();

        // Phase 1: Create index, add vectors, flush
        let index_id = {
            let provider = BinaryQuantizedIndexProvider::new(
                storage.clone(),
                new_non_persistent_cache_for_test(),
            );
            let index = provider.open(&config, OpenMode::Create).await.unwrap();

            for (i, v) in vectors.iter().enumerate() {
                index.add(i as u32, v).unwrap();
            }

            provider.flush(&index).await.unwrap()
        };

        // Phase 2: Load from storage, verify persistence
        {
            let provider = BinaryQuantizedIndexProvider::new(
                storage.clone(),
                new_non_persistent_cache_for_test(),
            );
            let index = provider
                .open(&config, OpenMode::Open(index_id))
                .await
                .unwrap();

            assert_eq!(index.len().unwrap(), 32);
            for (i, v) in vectors.iter().enumerate() {
                assert_eq!(index.get(i as u32).unwrap().unwrap(), *v);
            }

            // Search should work
            let result = index.search(&vectors[0], 1).unwrap();
            assert_eq!(result.keys[0], 0);
        }
    }

    #[tokio::test]
    async fn test_provider_fork() {
        use chroma_cache::new_non_persistent_cache_for_test;
        use chroma_storage::test_storage;

        let (_temp_dir, storage) = test_storage();
        let collection_id = CollectionUuid(Uuid::new_v4());
        const DIM: usize = 128;

        let config = BinaryQuantizedIndexConfig {
            collection_id,
            cmek: None,
            prefix_path: String::new(),
            dimensions: DIM,
            center: None,
        };

        let mut rng = StdRng::seed_from_u64(42);
        let vectors: Vec<Vec<f32>> = (0..64).map(|_| random_vector(&mut rng, DIM)).collect();

        // Create and flush original
        let provider =
            BinaryQuantizedIndexProvider::new(storage.clone(), new_non_persistent_cache_for_test());

        let index_a = provider.open(&config, OpenMode::Create).await.unwrap();
        for (i, v) in vectors.iter().take(32).enumerate() {
            index_a.add(i as u32, v).unwrap();
        }
        let index_a_id = provider.flush(&index_a).await.unwrap();

        // Fork and add more vectors
        let index_b = provider
            .open(&config, OpenMode::Fork(index_a_id))
            .await
            .unwrap();
        assert_ne!(index_b.id, index_a_id, "Forked index should have new ID");
        assert_eq!(
            index_b.len().unwrap(),
            32,
            "Forked index should have same data"
        );

        for (i, v) in vectors.iter().skip(32).enumerate() {
            index_b.add((i + 32) as u32, v).unwrap();
        }
        let index_b_id = provider.flush(&index_b).await.unwrap();

        // Reload and verify isolation
        let provider2 =
            BinaryQuantizedIndexProvider::new(storage.clone(), new_non_persistent_cache_for_test());

        let loaded_a = provider2
            .open(&config, OpenMode::Open(index_a_id))
            .await
            .unwrap();
        assert_eq!(loaded_a.len().unwrap(), 32, "Original should be unchanged");

        let loaded_b = provider2
            .open(&config, OpenMode::Open(index_b_id))
            .await
            .unwrap();
        assert_eq!(
            loaded_b.len().unwrap(),
            64,
            "Forked should have all vectors"
        );
    }

    /// Test that binary quantization achieves reasonable recall for centroid lookup.
    ///
    /// For the centroid lookup use case with nprobe > 1, lower recall is acceptable
    /// since we probe multiple clusters. This test verifies basic functionality.
    #[test]
    fn test_recall_at_k() {
        use simsimd::SpatialSimilarity;

        const DIM: usize = 256;
        const N_VECTORS: usize = 500;
        const N_QUERIES: usize = 50;
        const K: usize = 20; // Higher K for better recall measurement

        let mut rng = StdRng::seed_from_u64(42);

        // Generate vectors and normalize them (simulating centroid vectors)
        let vectors: Vec<Vec<f32>> = (0..N_VECTORS)
            .map(|_| {
                let v = random_vector(&mut rng, DIM);
                let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                v.iter().map(|x| x / norm.max(f32::EPSILON)).collect()
            })
            .collect();

        // Compute center as mean of all vectors
        let mut center = vec![0.0f32; DIM];
        for v in &vectors {
            for (i, &val) in v.iter().enumerate() {
                center[i] += val;
            }
        }
        for c in &mut center {
            *c /= N_VECTORS as f32;
        }

        let config = BinaryQuantizedConfig {
            dimensions: DIM,
            center: Some(center.into()),
        };
        let index = BinaryQuantizedIndex::new(&config);

        for (i, v) in vectors.iter().enumerate() {
            index.add(i as u32, v).unwrap();
        }

        // Generate normalized queries
        let queries: Vec<Vec<f32>> = (0..N_QUERIES)
            .map(|_| {
                let v = random_vector(&mut rng, DIM);
                let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                v.iter().map(|x| x / norm.max(f32::EPSILON)).collect()
            })
            .collect();

        // Compute recall
        let mut total_recall = 0.0;

        for query in &queries {
            // Ground truth: compute exact cosine distances (since vectors are normalized)
            let mut exact_distances: Vec<(u32, f32)> = vectors
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let dist = <f32 as SpatialSimilarity>::cos(query, v).unwrap_or(1.0) as f32;
                    (i as u32, dist)
                })
                .collect();
            exact_distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let ground_truth: std::collections::HashSet<u32> =
                exact_distances.iter().take(K).map(|(k, _)| *k).collect();

            // Binary quantized search
            let result = index.search(query, K).unwrap();
            let retrieved: std::collections::HashSet<u32> = result.keys.iter().copied().collect();

            // Recall = |retrieved ∩ ground_truth| / K
            let hits = retrieved.intersection(&ground_truth).count();
            total_recall += hits as f64 / K as f64;
        }

        let avg_recall = total_recall / N_QUERIES as f64;
        eprintln!(
            "Binary quantization recall@{}: {:.2}% ({} vectors, {} queries, {} dims)",
            K,
            avg_recall * 100.0,
            N_VECTORS,
            N_QUERIES,
            DIM
        );

        // For centroid lookup, we use nprobe (typically 5-20) which compensates for
        // lower per-query recall. We just need the recall to be non-trivial.
        // With normalized vectors and a proper center, we expect 30-50%+ recall@20.
        assert!(
            avg_recall >= 0.20,
            "Recall@{} should be at least 20%, got {:.2}%",
            K,
            avg_recall * 100.0
        );
    }

    /// Test that Hamming distance correlates with cosine distance for normalized vectors.
    #[test]
    fn test_hamming_cosine_correlation() {
        use simsimd::SpatialSimilarity;

        const DIM: usize = 512;
        const N_PAIRS: usize = 1000;

        let mut rng = StdRng::seed_from_u64(42);
        let center = vec![0.0; DIM];

        let mut hamming_distances = Vec::with_capacity(N_PAIRS);
        let mut cosine_distances = Vec::with_capacity(N_PAIRS);

        for _ in 0..N_PAIRS {
            let v1 = random_vector(&mut rng, DIM);
            let v2 = random_vector(&mut rng, DIM);

            // Binary quantize
            let b1 = binary_quantize(&v1, &center);
            let b2 = binary_quantize(&v2, &center);

            // Compute distances
            let hamming = hamming_distance(&b1, &b2) as f64;
            let cosine = <f32 as SpatialSimilarity>::cos(&v1, &v2).unwrap_or(0.0) as f64;

            hamming_distances.push(hamming);
            cosine_distances.push(cosine);
        }

        // Compute Spearman rank correlation
        // Sort by hamming and compute ranks
        let mut hamming_ranks: Vec<(usize, f64)> = hamming_distances
            .iter()
            .enumerate()
            .map(|(i, &d)| (i, d))
            .collect();
        hamming_ranks.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        let mut cosine_ranks: Vec<(usize, f64)> = cosine_distances
            .iter()
            .enumerate()
            .map(|(i, &d)| (i, d))
            .collect();
        cosine_ranks.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // Assign ranks
        let mut h_rank = vec![0.0; N_PAIRS];
        let mut c_rank = vec![0.0; N_PAIRS];
        for (rank, (idx, _)) in hamming_ranks.iter().enumerate() {
            h_rank[*idx] = rank as f64;
        }
        for (rank, (idx, _)) in cosine_ranks.iter().enumerate() {
            c_rank[*idx] = rank as f64;
        }

        // Compute Spearman correlation
        let mean_h: f64 = h_rank.iter().sum::<f64>() / N_PAIRS as f64;
        let mean_c: f64 = c_rank.iter().sum::<f64>() / N_PAIRS as f64;

        let mut num = 0.0;
        let mut den_h = 0.0;
        let mut den_c = 0.0;

        for i in 0..N_PAIRS {
            let dh = h_rank[i] - mean_h;
            let dc = c_rank[i] - mean_c;
            num += dh * dc;
            den_h += dh * dh;
            den_c += dc * dc;
        }

        let correlation = num / (den_h.sqrt() * den_c.sqrt());

        eprintln!(
            "Hamming-Cosine Spearman correlation: {:.4} ({} dims)",
            correlation, DIM
        );

        // Hamming distance should have positive correlation with cosine distance
        // (both measure dissimilarity)
        assert!(
            correlation > 0.5,
            "Hamming and cosine distance should be positively correlated, got {:.4}",
            correlation
        );
    }
}
