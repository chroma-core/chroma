use std::sync::Arc;

use chroma_distance::{normalize, DistanceFunction};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Cmek, QuantizedClusterOwned};
use dashmap::DashMap;
use faer::{col::ColRef, Mat};
use thiserror::Error;

use crate::{SearchResult, VectorIndex};

/// In-memory staging for a quantized cluster head.
struct QuantizedDelta {
    cluster: QuantizedClusterOwned,
    length: u64,
}

/// Configuration for quantized SPANN index.
#[derive(Clone)]
struct QuantizedSpannConfig {
    // === Shared ===
    cmek: Option<Cmek>,
    prefix_path: String,
    dimensions: usize,
    distance_function: DistanceFunction,

    // === Rebuild Criteria ===
    drift_threshold: f32,

    // === SPANN ===
    spann_nprobe: u32,
    spann_replica_count: u32,
    spann_rng_epsilon: f32,
    spann_rng_factor: f32,
    spann_split_threshold: usize,
    spann_merge_threshold: usize,

    // === USearch ===
    usearch_connectivity: usize,
    usearch_expansion_add: usize,
    usearch_expansion_search: usize,
}

#[derive(Error, Debug)]
pub enum QuantizedSpannError {
    #[error("Get not supported for quantized SPANN index")]
    GetNotSupported,
    #[error("Centroid index error: {0}")]
    CentroidIndex(#[from] Box<dyn ChromaError>),
}

impl ChromaError for QuantizedSpannError {
    fn code(&self) -> ErrorCodes {
        match self {
            QuantizedSpannError::GetNotSupported => ErrorCodes::InvalidArgument,
            QuantizedSpannError::CentroidIndex(e) => e.code(),
        }
    }
}

/// Mutable quantized SPANN index, generic over centroid index.
pub struct MutableQuantizedSpannIndex<I: VectorIndex> {
    // === Config ===
    config: QuantizedSpannConfig,

    // === Centroid Index ===
    centroid: I,

    // === Quantization ===
    rotation: Mat<f32>,

    // === In-Memory State ===
    deltas: Arc<DashMap<u64, QuantizedDelta>>,
    embeddings: Arc<DashMap<u64, Arc<[f32]>>>,
    versions: Arc<DashMap<u64, u64>>,
}

impl<I: VectorIndex> VectorIndex for MutableQuantizedSpannIndex<I> {
    type Error = QuantizedSpannError;

    fn add(&self, key: u64, vector: &[f32]) -> Result<(), Self::Error> {
        let rotated = self.rotate(vector);
        let _version = self.upgrade_version(key);
        self.embeddings.insert(key, rotated);
        // TODO: add to posting list
        todo!()
    }

    /// Quantized SPANN is blockfile-backed and has no fixed capacity.
    fn capacity(&self) -> Result<usize, Self::Error> {
        Ok(usize::MAX)
    }

    fn get(&self, _key: u64) -> Result<Option<Vec<f32>>, Self::Error> {
        Err(QuantizedSpannError::GetNotSupported)
    }

    fn len(&self) -> Result<usize, Self::Error> {
        Ok(self.versions.len())
    }

    fn remove(&self, key: u64) -> Result<(), Self::Error> {
        self.upgrade_version(key);
        Ok(())
    }

    /// No-op: quantized SPANN is blockfile-backed and does not require pre-allocation.
    fn reserve(&self, _capacity: usize) -> Result<(), Self::Error> {
        Ok(())
    }

    fn search(&self, _query: &[f32], _count: usize) -> Result<SearchResult, Self::Error> {
        todo!()
    }
}

impl<I: VectorIndex> MutableQuantizedSpannIndex<I> {
    /// Compute distance between two vectors using the configured distance function.
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        self.config.distance_function.distance(a, b)
    }

    /// Query the centroid index for the nearest cluster heads.
    fn navigate(&self, query: &[f32]) -> Result<SearchResult, QuantizedSpannError> {
        self.centroid
            .search(query, self.config.spann_nprobe as usize)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))
    }

    /// Normalize (if cosine) and rotate a vector for RaBitQ quantization.
    fn rotate(&self, vector: &[f32]) -> Arc<[f32]> {
        let rotated = match self.config.distance_function {
            DistanceFunction::Cosine => {
                let normalized = normalize(vector);
                &self.rotation * ColRef::from_slice(&normalized)
            }
            _ => &self.rotation * ColRef::from_slice(vector),
        };
        rotated.iter().copied().collect()
    }

    /// Increment and return the next version for a key.
    fn upgrade_version(&self, key: u64) -> u64 {
        let mut entry = self.versions.entry(key).or_default();
        *entry += 1;
        *entry
    }
}
