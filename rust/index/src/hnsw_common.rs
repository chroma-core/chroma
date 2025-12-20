//! Unified HNSW index abstraction that supports multiple backends.
//!
//! This module provides enums that wrap both hnswlib and usearch implementations,
//! allowing SPANN and other components to switch between implementations at runtime.
//!
//! ## Thread Safety
//!
//! The two backends have different thread-safety characteristics:
//! - **hnswlib**: NOT thread-safe, requires external locking. We wrap it in `RwLock`.
//! - **usearch**: Internally thread-safe with fine-grained per-node locking. No external lock needed.
//!
//! The `HnswIndexImpl` enum handles this by placing the `RwLock` inside the `Hnswlib` variant only,
//! avoiding lock overhead for usearch operations.

use std::sync::Arc;

use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;
use parking_lot::RwLock;

use crate::{
    hnsw::{HnswIndex, HnswIndexConfig},
    hnsw_provider::HnswIndexProvider,
    usearch::{UsearchIndex, UsearchIndexConfig},
    usearch_provider::UsearchIndexProvider,
    Index, IndexConfig, IndexUuid, PersistentIndex,
};

/// Enum representing which HNSW backend to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HnswBackend {
    #[default]
    Hnswlib,
    Usearch,
}

/// Unified HNSW index that can be either hnswlib or usearch.
///
/// Note: The RwLock is inside the Hnswlib variant because hnswlib is not thread-safe,
/// while usearch is internally thread-safe and doesn't need external locking.
pub enum HnswIndexImpl {
    /// Hnswlib wrapped in RwLock because it's not thread-safe
    Hnswlib(RwLock<HnswIndex>),
    /// Usearch is internally thread-safe, no lock needed
    Usearch(UsearchIndex),
}

impl HnswIndexImpl {
    /// Get the index UUID.
    pub fn id(&self) -> IndexUuid {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().id,
            HnswIndexImpl::Usearch(idx) => idx.id,
        }
    }

    /// Get the distance function.
    pub fn distance_function(&self) -> DistanceFunction {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().distance_function.clone(),
            HnswIndexImpl::Usearch(idx) => idx.distance_function.clone(),
        }
    }

    /// Get the number of vectors in the index.
    pub fn len(&self) -> usize {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().len(),
            HnswIndexImpl::Usearch(idx) => idx.len(),
        }
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().is_empty(),
            HnswIndexImpl::Usearch(idx) => idx.is_empty(),
        }
    }

    /// Get the number of vectors including deleted ones (hnswlib specific).
    /// For usearch, this returns the same as len() since usearch doesn't track deleted separately.
    pub fn len_with_deleted(&self) -> usize {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().len_with_deleted(),
            HnswIndexImpl::Usearch(idx) => idx.len(), // usearch doesn't track deleted separately
        }
    }

    /// Get the current capacity of the index.
    pub fn capacity(&self) -> usize {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().capacity(),
            HnswIndexImpl::Usearch(idx) => idx.capacity(),
        }
    }

    /// Get the dimensionality of the index.
    pub fn dimensionality(&self) -> i32 {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().dimensionality(),
            HnswIndexImpl::Usearch(idx) => idx.dimensionality as i32,
        }
    }

    /// Resize the index to a new capacity.
    /// Note: This requires write access for hnswlib.
    pub fn resize(&self, new_capacity: usize) -> Result<(), Box<dyn ChromaError>> {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.write().resize(new_capacity),
            HnswIndexImpl::Usearch(idx) => idx.resize(new_capacity),
        }
    }

    /// Add a vector to the index.
    pub fn add(&self, id: usize, vector: &[f32]) -> Result<(), Box<dyn ChromaError>> {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().add(id, vector),
            HnswIndexImpl::Usearch(idx) => idx.add(id, vector),
        }
    }

    /// Delete a vector from the index.
    pub fn delete(&self, id: usize) -> Result<(), Box<dyn ChromaError>> {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().delete(id),
            HnswIndexImpl::Usearch(idx) => idx.delete(id),
        }
    }

    /// Query the index for nearest neighbors.
    pub fn query(
        &self,
        vector: &[f32],
        k: usize,
        allowed_ids: &[usize],
        disallowed_ids: &[usize],
    ) -> Result<(Vec<usize>, Vec<f32>), Box<dyn ChromaError>> {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().query(vector, k, allowed_ids, disallowed_ids),
            HnswIndexImpl::Usearch(idx) => idx.query(vector, k, allowed_ids, disallowed_ids),
        }
    }

    /// Get a vector by ID.
    pub fn get(&self, id: usize) -> Result<Option<Vec<f32>>, Box<dyn ChromaError>> {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().get(id),
            HnswIndexImpl::Usearch(idx) => idx.get(id),
        }
    }

    /// Get all IDs in the index.
    pub fn get_all_ids(&self) -> Result<(Vec<usize>, Vec<usize>), Box<dyn ChromaError>> {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().get_all_ids(),
            HnswIndexImpl::Usearch(idx) => idx.get_all_ids(),
        }
    }

    /// Get sizes for all IDs.
    pub fn get_all_ids_sizes(&self) -> Result<Vec<usize>, Box<dyn ChromaError>> {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().get_all_ids_sizes(),
            HnswIndexImpl::Usearch(idx) => idx.get_all_ids_sizes(),
        }
    }

    /// Save the index to disk (hnswlib only, usearch returns Ok for in-memory).
    pub fn save(&self) -> Result<(), Box<dyn ChromaError>> {
        match self {
            HnswIndexImpl::Hnswlib(idx) => idx.read().save(),
            HnswIndexImpl::Usearch(_) => Ok(()), // usearch is in-memory, no save needed
        }
    }

    /// Serialize to HnswData (hnswlib only).
    /// Returns None for usearch since it doesn't support this format.
    pub fn serialize_to_hnsw_data(
        &self,
    ) -> Option<Result<hnswlib::HnswData, crate::hnsw::WrappedHnswError>> {
        match self {
            HnswIndexImpl::Hnswlib(idx) => Some(idx.read().serialize_to_hnsw_data()),
            HnswIndexImpl::Usearch(_) => None,
        }
    }

    /// Check if this is using the usearch backend.
    pub fn is_usearch(&self) -> bool {
        matches!(self, HnswIndexImpl::Usearch(_))
    }

    /// Ensure the index has capacity for at least one more element, then add the vector.
    /// This is atomic for hnswlib (holds write lock throughout) and is fine for usearch
    /// (which auto-resizes internally).
    pub fn ensure_capacity_and_add(
        &self,
        id: usize,
        vector: &[f32],
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            HnswIndexImpl::Hnswlib(idx) => {
                let mut guard = idx.write();
                let len = guard.len_with_deleted();
                let cap = guard.capacity();
                if len + 1 > cap {
                    guard.resize(cap * 2)?;
                }
                guard.add(id, vector)
            }
            HnswIndexImpl::Usearch(idx) => {
                // Usearch auto-resizes, so just add directly
                idx.add(id, vector)
            }
        }
    }

    /// Get current capacity and length atomically (for capacity checks).
    /// Returns (len_with_deleted, capacity).
    pub fn get_len_and_capacity(&self) -> (usize, usize) {
        match self {
            HnswIndexImpl::Hnswlib(idx) => {
                let guard = idx.read();
                (guard.len_with_deleted(), guard.capacity())
            }
            HnswIndexImpl::Usearch(idx) => (idx.len(), idx.capacity()),
        }
    }
}

/// Inner struct for UnifiedHnswIndexRef, containing the index and metadata.
/// Note: No RwLock here - the locking is handled inside HnswIndexImpl for hnswlib only.
pub struct UnifiedHnswInner {
    pub hnsw_index: HnswIndexImpl,
    pub prefix_path: String,
}

/// Reference to a unified HNSW index.
/// The index is wrapped in Arc for shared ownership.
/// Thread-safety is handled differently per backend:
/// - hnswlib: RwLock is inside HnswIndexImpl
/// - usearch: No lock needed (internally thread-safe)
#[derive(Clone)]
pub struct UnifiedHnswIndexRef {
    pub inner: Arc<UnifiedHnswInner>,
}

impl std::fmt::Debug for UnifiedHnswIndexRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnifiedHnswIndexRef")
            .field("id", &self.inner.hnsw_index.id())
            .field("len", &self.inner.hnsw_index.len())
            .field("dimensionality", &self.inner.hnsw_index.dimensionality())
            .finish_non_exhaustive()
    }
}

/// Unified HNSW provider that can create either hnswlib or usearch indexes.
#[derive(Clone)]
pub enum HnswProviderImpl {
    Hnswlib(HnswIndexProvider),
    Usearch(UsearchIndexProvider),
}

impl std::fmt::Debug for HnswProviderImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HnswProviderImpl::Hnswlib(p) => write!(f, "HnswProviderImpl::Hnswlib({:?})", p),
            HnswProviderImpl::Usearch(p) => write!(f, "HnswProviderImpl::Usearch({:?})", p),
        }
    }
}

impl HnswProviderImpl {
    /// Get the backend type.
    pub fn backend(&self) -> HnswBackend {
        match self {
            HnswProviderImpl::Hnswlib(_) => HnswBackend::Hnswlib,
            HnswProviderImpl::Usearch(_) => HnswBackend::Usearch,
        }
    }

    /// Check if this is using usearch backend.
    pub fn is_usearch(&self) -> bool {
        matches!(self, HnswProviderImpl::Usearch(_))
    }

    /// Get the hnswlib provider (if applicable).
    pub fn as_hnswlib(&self) -> Option<&HnswIndexProvider> {
        match self {
            HnswProviderImpl::Hnswlib(p) => Some(p),
            HnswProviderImpl::Usearch(_) => None,
        }
    }

    /// Get the usearch provider (if applicable).
    pub fn as_usearch(&self) -> Option<&UsearchIndexProvider> {
        match self {
            HnswProviderImpl::Hnswlib(_) => None,
            HnswProviderImpl::Usearch(p) => Some(p),
        }
    }
}

/// Helper to create an HNSW index using the specified backend.
pub fn create_hnsw_index(
    backend: HnswBackend,
    index_config: &IndexConfig,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    max_elements: usize,
    id: IndexUuid,
) -> Result<HnswIndexImpl, Box<dyn ChromaError>> {
    match backend {
        HnswBackend::Hnswlib => {
            let hnsw_config = HnswIndexConfig::new_ephemeral(m, ef_construction, ef_search);
            let index = HnswIndex::init(index_config, Some(&hnsw_config), id)?;
            Ok(HnswIndexImpl::Hnswlib(RwLock::new(index)))
        }
        HnswBackend::Usearch => {
            let usearch_config =
                UsearchIndexConfig::new(max_elements, m, ef_construction, ef_search);
            let index = UsearchIndex::init(index_config, Some(&usearch_config), id)?;
            Ok(HnswIndexImpl::Usearch(index))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_index_operations(backend: HnswBackend) {
        let index_config = IndexConfig {
            dimensionality: 4,
            distance_function: DistanceFunction::Euclidean,
        };

        let index = create_hnsw_index(
            backend,
            &index_config,
            16,
            100,
            100,
            1000,
            IndexUuid::default(),
        )
        .unwrap();

        // Test add
        index.add(0, &[1.0, 0.0, 0.0, 0.0]).unwrap();
        index.add(1, &[0.0, 1.0, 0.0, 0.0]).unwrap();
        index.add(2, &[0.0, 0.0, 1.0, 0.0]).unwrap();

        assert_eq!(index.len(), 3);

        // Test query
        let (ids, distances) = index.query(&[1.0, 0.0, 0.0, 0.0], 1, &[], &[]).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], 0);
        assert!(distances[0] < 0.001);

        // Test get
        let vec = index.get(0).unwrap().unwrap();
        assert_eq!(vec.len(), 4);
        assert!((vec[0] - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_hnswlib_backend() {
        test_index_operations(HnswBackend::Hnswlib);
    }

    #[test]
    fn test_usearch_backend() {
        test_index_operations(HnswBackend::Usearch);
    }

    #[test]
    fn test_unified_index_ref() {
        let index_config = IndexConfig {
            dimensionality: 4,
            distance_function: DistanceFunction::Euclidean,
        };

        let index = create_hnsw_index(
            HnswBackend::Usearch,
            &index_config,
            16,
            100,
            100,
            1000,
            IndexUuid::default(),
        )
        .unwrap();

        let index_ref = UnifiedHnswIndexRef {
            inner: Arc::new(UnifiedHnswInner {
                hnsw_index: index,
                prefix_path: "".to_string(),
            }),
        };

        // Test through the ref - no locking needed at this level for usearch
        index_ref
            .inner
            .hnsw_index
            .add(0, &[1.0, 0.0, 0.0, 0.0])
            .unwrap();
        assert_eq!(index_ref.inner.hnsw_index.len(), 1);
    }
}
