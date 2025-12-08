//! Abstraction layer for vector index implementations.
//!
//! This module provides a unified interface for different vector index implementations
//! (HNSW via hnswlib, USearch, etc.) allowing easy switching between backends.

use async_trait::async_trait;
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::CollectionUuid;
use std::fmt::Debug;
use thiserror::Error;

use crate::IndexUuid;

/// Errors that can occur when working with vector indices
#[derive(Error, Debug)]
pub enum VectorIndexError {
    #[error("Index creation failed: {0}")]
    CreateError(String),
    #[error("Index open failed: {0}")]
    OpenError(String),
    #[error("Index fork failed: {0}")]
    ForkError(String),
    #[error("Index commit failed: {0}")]
    CommitError(String),
    #[error("Index flush failed: {0}")]
    FlushError(String),
    #[error("Index mutation failed: {0}")]
    MutationError(String),
    #[error("Index query failed: {0}")]
    QueryError(String),
    #[error("Index resize failed: {0}")]
    ResizeError(String),
    #[error("Vector not found: {0}")]
    NotFound(usize),
    #[error("Internal error: {0}")]
    Internal(String),
}

impl ChromaError for VectorIndexError {
    fn code(&self) -> ErrorCodes {
        match self {
            VectorIndexError::CreateError(_) => ErrorCodes::Internal,
            VectorIndexError::OpenError(_) => ErrorCodes::Internal,
            VectorIndexError::ForkError(_) => ErrorCodes::Internal,
            VectorIndexError::CommitError(_) => ErrorCodes::Internal,
            VectorIndexError::FlushError(_) => ErrorCodes::Internal,
            VectorIndexError::MutationError(_) => ErrorCodes::Internal,
            VectorIndexError::QueryError(_) => ErrorCodes::Internal,
            VectorIndexError::ResizeError(_) => ErrorCodes::Internal,
            VectorIndexError::NotFound(_) => ErrorCodes::NotFound,
            VectorIndexError::Internal(_) => ErrorCodes::Internal,
        }
    }
}

/// A reference to a vector index that can be cloned and shared
pub trait VectorIndexRef: Clone + Send + Sync + Debug {
    /// Get the unique ID of this index
    fn id(&self) -> IndexUuid;

    /// Get the prefix path for storage
    fn prefix_path(&self) -> String;

    /// Get the number of vectors in the index
    fn len(&self) -> usize;

    /// Check if the index is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the number of vectors including deleted ones
    fn len_with_deleted(&self) -> usize;

    /// Get the dimensionality of vectors in this index
    fn dimensionality(&self) -> i32;

    /// Get the current capacity of the index
    fn capacity(&self) -> usize;

    /// Resize the index to accommodate more elements
    fn resize(&self, new_size: usize) -> Result<(), VectorIndexError>;

    /// Add a vector to the index
    fn add(&self, id: usize, vector: &[f32]) -> Result<(), VectorIndexError>;

    /// Delete a vector from the index
    fn delete(&self, id: usize) -> Result<(), VectorIndexError>;

    /// Query for k nearest neighbors
    fn query(
        &self,
        vector: &[f32],
        k: usize,
        allowed_ids: &[usize],
        disallowed_ids: &[usize],
    ) -> Result<(Vec<usize>, Vec<f32>), VectorIndexError>;

    /// Get a vector by ID
    fn get(&self, id: usize) -> Result<Option<Vec<f32>>, VectorIndexError>;

    /// Get all non-deleted IDs
    fn get_all_ids(&self) -> Result<(Vec<usize>, Vec<usize>), VectorIndexError>;

    /// Get sizes of all ID groups
    fn get_all_ids_sizes(&self) -> Result<Vec<usize>, VectorIndexError>;
}

/// Provider for vector indices with caching and storage integration
#[async_trait]
pub trait VectorIndexProvider: Clone + Send + Sync + Debug {
    /// The type of index reference this provider creates
    type IndexRef: VectorIndexRef;

    /// Get an index from cache
    async fn get(&self, index_id: &IndexUuid, cache_key: &CollectionUuid)
        -> Option<Self::IndexRef>;

    /// Create a new empty index
    #[allow(clippy::too_many_arguments)]
    async fn create(
        &self,
        cache_key: &CollectionUuid,
        m: usize,               // connectivity
        ef_construction: usize, // expansion_add
        ef_search: usize,       // expansion_search
        dimensionality: i32,
        distance_function: DistanceFunction,
        prefix_path: &str,
    ) -> Result<Self::IndexRef, VectorIndexError>;

    /// Open an existing index from storage
    #[allow(clippy::too_many_arguments)]
    async fn open(
        &self,
        id: &IndexUuid,
        cache_key: &CollectionUuid,
        dimensionality: i32,
        distance_function: DistanceFunction,
        ef_search: usize,
        prefix_path: &str,
    ) -> Result<Self::IndexRef, VectorIndexError>;

    /// Fork an existing index (create a copy with a new ID)
    #[allow(clippy::too_many_arguments)]
    async fn fork(
        &self,
        source_id: &IndexUuid,
        cache_key: &CollectionUuid,
        dimensionality: i32,
        distance_function: DistanceFunction,
        ef_search: usize,
        prefix_path: &str,
    ) -> Result<Self::IndexRef, VectorIndexError>;

    /// Commit changes to the index
    fn commit(&self, index: Self::IndexRef) -> Result<(), VectorIndexError>;

    /// Flush the index to storage
    async fn flush(
        &self,
        prefix_path: &str,
        id: &IndexUuid,
        index: &Self::IndexRef,
    ) -> Result<(), VectorIndexError>;
}

// ============================================================================
// HNSW Implementation
// ============================================================================

use crate::hnsw_provider::{HnswIndexProvider, HnswIndexRef};

impl VectorIndexRef for HnswIndexRef {
    fn id(&self) -> IndexUuid {
        self.inner.read().hnsw_index.id
    }

    fn prefix_path(&self) -> String {
        self.inner.read().prefix_path.clone()
    }

    fn len(&self) -> usize {
        self.inner.read().hnsw_index.len()
    }

    fn len_with_deleted(&self) -> usize {
        self.inner.read().hnsw_index.len_with_deleted()
    }

    fn dimensionality(&self) -> i32 {
        self.inner.read().hnsw_index.dimensionality()
    }

    fn capacity(&self) -> usize {
        self.inner.read().hnsw_index.capacity()
    }

    fn resize(&self, new_size: usize) -> Result<(), VectorIndexError> {
        self.inner
            .write()
            .hnsw_index
            .resize(new_size)
            .map_err(|e| VectorIndexError::ResizeError(e.to_string()))
    }

    fn add(&self, id: usize, vector: &[f32]) -> Result<(), VectorIndexError> {
        use crate::Index;
        self.inner
            .write()
            .hnsw_index
            .add(id, vector)
            .map_err(|e| VectorIndexError::MutationError(e.to_string()))
    }

    fn delete(&self, id: usize) -> Result<(), VectorIndexError> {
        use crate::Index;
        self.inner
            .write()
            .hnsw_index
            .delete(id)
            .map_err(|e| VectorIndexError::MutationError(e.to_string()))
    }

    fn query(
        &self,
        vector: &[f32],
        k: usize,
        allowed_ids: &[usize],
        disallowed_ids: &[usize],
    ) -> Result<(Vec<usize>, Vec<f32>), VectorIndexError> {
        use crate::Index;
        self.inner
            .read()
            .hnsw_index
            .query(vector, k, allowed_ids, disallowed_ids)
            .map_err(|e| VectorIndexError::QueryError(e.to_string()))
    }

    fn get(&self, id: usize) -> Result<Option<Vec<f32>>, VectorIndexError> {
        use crate::Index;
        self.inner
            .read()
            .hnsw_index
            .get(id)
            .map_err(|e| VectorIndexError::QueryError(e.to_string()))
    }

    fn get_all_ids(&self) -> Result<(Vec<usize>, Vec<usize>), VectorIndexError> {
        use crate::Index;
        self.inner
            .read()
            .hnsw_index
            .get_all_ids()
            .map_err(|e| VectorIndexError::QueryError(e.to_string()))
    }

    fn get_all_ids_sizes(&self) -> Result<Vec<usize>, VectorIndexError> {
        use crate::Index;
        self.inner
            .read()
            .hnsw_index
            .get_all_ids_sizes()
            .map_err(|e| VectorIndexError::QueryError(e.to_string()))
    }
}

#[async_trait]
impl VectorIndexProvider for HnswIndexProvider {
    type IndexRef = HnswIndexRef;

    async fn get(
        &self,
        index_id: &IndexUuid,
        cache_key: &CollectionUuid,
    ) -> Option<Self::IndexRef> {
        HnswIndexProvider::get(self, index_id, cache_key).await
    }

    async fn create(
        &self,
        cache_key: &CollectionUuid,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        dimensionality: i32,
        distance_function: DistanceFunction,
        prefix_path: &str,
    ) -> Result<Self::IndexRef, VectorIndexError> {
        HnswIndexProvider::create(
            self,
            cache_key,
            m,
            ef_construction,
            ef_search,
            dimensionality,
            distance_function,
            prefix_path,
        )
        .await
        .map_err(|e| VectorIndexError::CreateError(e.to_string()))
    }

    async fn open(
        &self,
        id: &IndexUuid,
        cache_key: &CollectionUuid,
        dimensionality: i32,
        distance_function: DistanceFunction,
        ef_search: usize,
        prefix_path: &str,
    ) -> Result<Self::IndexRef, VectorIndexError> {
        HnswIndexProvider::open(
            self,
            id,
            cache_key,
            dimensionality,
            distance_function,
            ef_search,
            prefix_path,
        )
        .await
        .map_err(|e| VectorIndexError::OpenError(e.to_string()))
    }

    async fn fork(
        &self,
        source_id: &IndexUuid,
        cache_key: &CollectionUuid,
        dimensionality: i32,
        distance_function: DistanceFunction,
        ef_search: usize,
        prefix_path: &str,
    ) -> Result<Self::IndexRef, VectorIndexError> {
        HnswIndexProvider::fork(
            self,
            source_id,
            cache_key,
            dimensionality,
            distance_function,
            ef_search,
            prefix_path,
        )
        .await
        .map_err(|e| VectorIndexError::ForkError(e.to_string()))
    }

    fn commit(&self, index: Self::IndexRef) -> Result<(), VectorIndexError> {
        HnswIndexProvider::commit(self, index)
            .map_err(|e| VectorIndexError::CommitError(e.to_string()))
    }

    async fn flush(
        &self,
        prefix_path: &str,
        id: &IndexUuid,
        index: &Self::IndexRef,
    ) -> Result<(), VectorIndexError> {
        HnswIndexProvider::flush(self, prefix_path, id, index)
            .await
            .map_err(|e| VectorIndexError::FlushError(e.to_string()))
    }
}

// ============================================================================
// USearch Implementation
// ============================================================================

use crate::usearch_provider::{USearchIndexProvider, USearchIndexRef};

impl VectorIndexRef for USearchIndexRef {
    fn id(&self) -> IndexUuid {
        self.inner.read().usearch_index.id
    }

    fn prefix_path(&self) -> String {
        self.inner.read().prefix_path.clone()
    }

    fn len(&self) -> usize {
        self.inner.read().usearch_index.len()
    }

    fn len_with_deleted(&self) -> usize {
        self.inner.read().usearch_index.len_with_deleted()
    }

    fn dimensionality(&self) -> i32 {
        self.inner.read().usearch_index.dimensionality()
    }

    fn capacity(&self) -> usize {
        self.inner.read().usearch_index.capacity()
    }

    fn resize(&self, new_size: usize) -> Result<(), VectorIndexError> {
        self.inner
            .write()
            .usearch_index
            .resize(new_size)
            .map_err(|e| VectorIndexError::ResizeError(e.to_string()))
    }

    fn add(&self, id: usize, vector: &[f32]) -> Result<(), VectorIndexError> {
        use crate::Index;
        self.inner
            .write()
            .usearch_index
            .add(id, vector)
            .map_err(|e| VectorIndexError::MutationError(e.to_string()))
    }

    fn delete(&self, id: usize) -> Result<(), VectorIndexError> {
        use crate::Index;
        self.inner
            .write()
            .usearch_index
            .delete(id)
            .map_err(|e| VectorIndexError::MutationError(e.to_string()))
    }

    fn query(
        &self,
        vector: &[f32],
        k: usize,
        allowed_ids: &[usize],
        disallowed_ids: &[usize],
    ) -> Result<(Vec<usize>, Vec<f32>), VectorIndexError> {
        use crate::Index;
        self.inner
            .read()
            .usearch_index
            .query(vector, k, allowed_ids, disallowed_ids)
            .map_err(|e| VectorIndexError::QueryError(e.to_string()))
    }

    fn get(&self, id: usize) -> Result<Option<Vec<f32>>, VectorIndexError> {
        use crate::Index;
        self.inner
            .read()
            .usearch_index
            .get(id)
            .map_err(|e| VectorIndexError::QueryError(e.to_string()))
    }

    fn get_all_ids(&self) -> Result<(Vec<usize>, Vec<usize>), VectorIndexError> {
        use crate::Index;
        self.inner
            .read()
            .usearch_index
            .get_all_ids()
            .map_err(|e| VectorIndexError::QueryError(e.to_string()))
    }

    fn get_all_ids_sizes(&self) -> Result<Vec<usize>, VectorIndexError> {
        use crate::Index;
        self.inner
            .read()
            .usearch_index
            .get_all_ids_sizes()
            .map_err(|e| VectorIndexError::QueryError(e.to_string()))
    }
}

#[async_trait]
impl VectorIndexProvider for USearchIndexProvider {
    type IndexRef = USearchIndexRef;

    async fn get(
        &self,
        index_id: &IndexUuid,
        cache_key: &CollectionUuid,
    ) -> Option<Self::IndexRef> {
        USearchIndexProvider::get(self, index_id, cache_key).await
    }

    async fn create(
        &self,
        cache_key: &CollectionUuid,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        dimensionality: i32,
        distance_function: DistanceFunction,
        prefix_path: &str,
    ) -> Result<Self::IndexRef, VectorIndexError> {
        USearchIndexProvider::create(
            self,
            cache_key,
            m,               // connectivity
            ef_construction, // expansion_add
            ef_search,       // expansion_search
            dimensionality,
            distance_function,
            prefix_path,
        )
        .await
        .map_err(|e| VectorIndexError::CreateError(e.to_string()))
    }

    async fn open(
        &self,
        id: &IndexUuid,
        cache_key: &CollectionUuid,
        dimensionality: i32,
        distance_function: DistanceFunction,
        ef_search: usize,
        prefix_path: &str,
    ) -> Result<Self::IndexRef, VectorIndexError> {
        USearchIndexProvider::open(
            self,
            id,
            cache_key,
            dimensionality,
            distance_function,
            ef_search,
            prefix_path,
        )
        .await
        .map_err(|e| VectorIndexError::OpenError(e.to_string()))
    }

    async fn fork(
        &self,
        source_id: &IndexUuid,
        cache_key: &CollectionUuid,
        dimensionality: i32,
        distance_function: DistanceFunction,
        ef_search: usize,
        prefix_path: &str,
    ) -> Result<Self::IndexRef, VectorIndexError> {
        USearchIndexProvider::fork(
            self,
            source_id,
            cache_key,
            dimensionality,
            distance_function,
            ef_search,
            prefix_path,
        )
        .await
        .map_err(|e| VectorIndexError::ForkError(e.to_string()))
    }

    fn commit(&self, index: Self::IndexRef) -> Result<(), VectorIndexError> {
        USearchIndexProvider::commit(self, index)
            .map_err(|e| VectorIndexError::CommitError(e.to_string()))
    }

    async fn flush(
        &self,
        prefix_path: &str,
        id: &IndexUuid,
        index: &Self::IndexRef,
    ) -> Result<(), VectorIndexError> {
        USearchIndexProvider::flush(self, prefix_path, id, index)
            .await
            .map_err(|e| VectorIndexError::FlushError(e.to_string()))
    }
}

// ============================================================================
// Dynamic dispatch wrapper for runtime switching
// ============================================================================

/// Enum to hold either HNSW or USearch index reference for dynamic dispatch
#[derive(Clone, Debug)]
pub enum DynamicVectorIndexRef {
    Hnsw(HnswIndexRef),
    USearch(USearchIndexRef),
}

impl VectorIndexRef for DynamicVectorIndexRef {
    fn id(&self) -> IndexUuid {
        match self {
            DynamicVectorIndexRef::Hnsw(idx) => idx.id(),
            DynamicVectorIndexRef::USearch(idx) => idx.id(),
        }
    }

    fn prefix_path(&self) -> String {
        match self {
            DynamicVectorIndexRef::Hnsw(idx) => idx.prefix_path(),
            DynamicVectorIndexRef::USearch(idx) => idx.prefix_path(),
        }
    }

    fn len(&self) -> usize {
        match self {
            DynamicVectorIndexRef::Hnsw(idx) => idx.len(),
            DynamicVectorIndexRef::USearch(idx) => idx.len(),
        }
    }

    fn len_with_deleted(&self) -> usize {
        match self {
            DynamicVectorIndexRef::Hnsw(idx) => idx.len_with_deleted(),
            DynamicVectorIndexRef::USearch(idx) => idx.len_with_deleted(),
        }
    }

    fn dimensionality(&self) -> i32 {
        match self {
            DynamicVectorIndexRef::Hnsw(idx) => idx.dimensionality(),
            DynamicVectorIndexRef::USearch(idx) => idx.dimensionality(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            DynamicVectorIndexRef::Hnsw(idx) => idx.capacity(),
            DynamicVectorIndexRef::USearch(idx) => idx.capacity(),
        }
    }

    fn resize(&self, new_size: usize) -> Result<(), VectorIndexError> {
        match self {
            DynamicVectorIndexRef::Hnsw(idx) => idx.resize(new_size),
            DynamicVectorIndexRef::USearch(idx) => idx.resize(new_size),
        }
    }

    fn add(&self, id: usize, vector: &[f32]) -> Result<(), VectorIndexError> {
        match self {
            DynamicVectorIndexRef::Hnsw(idx) => idx.add(id, vector),
            DynamicVectorIndexRef::USearch(idx) => idx.add(id, vector),
        }
    }

    fn delete(&self, id: usize) -> Result<(), VectorIndexError> {
        match self {
            DynamicVectorIndexRef::Hnsw(idx) => idx.delete(id),
            DynamicVectorIndexRef::USearch(idx) => idx.delete(id),
        }
    }

    fn query(
        &self,
        vector: &[f32],
        k: usize,
        allowed_ids: &[usize],
        disallowed_ids: &[usize],
    ) -> Result<(Vec<usize>, Vec<f32>), VectorIndexError> {
        match self {
            DynamicVectorIndexRef::Hnsw(idx) => idx.query(vector, k, allowed_ids, disallowed_ids),
            DynamicVectorIndexRef::USearch(idx) => {
                idx.query(vector, k, allowed_ids, disallowed_ids)
            }
        }
    }

    fn get(&self, id: usize) -> Result<Option<Vec<f32>>, VectorIndexError> {
        match self {
            DynamicVectorIndexRef::Hnsw(idx) => idx.get(id),
            DynamicVectorIndexRef::USearch(idx) => idx.get(id),
        }
    }

    fn get_all_ids(&self) -> Result<(Vec<usize>, Vec<usize>), VectorIndexError> {
        match self {
            DynamicVectorIndexRef::Hnsw(idx) => idx.get_all_ids(),
            DynamicVectorIndexRef::USearch(idx) => idx.get_all_ids(),
        }
    }

    fn get_all_ids_sizes(&self) -> Result<Vec<usize>, VectorIndexError> {
        match self {
            DynamicVectorIndexRef::Hnsw(idx) => idx.get_all_ids_sizes(),
            DynamicVectorIndexRef::USearch(idx) => idx.get_all_ids_sizes(),
        }
    }
}

/// Provider that can use either HNSW or USearch based on configuration
#[derive(Clone, Debug)]
pub enum DynamicVectorIndexProvider {
    Hnsw(HnswIndexProvider),
    USearch(USearchIndexProvider),
}

#[async_trait]
impl VectorIndexProvider for DynamicVectorIndexProvider {
    type IndexRef = DynamicVectorIndexRef;

    async fn get(
        &self,
        index_id: &IndexUuid,
        cache_key: &CollectionUuid,
    ) -> Option<Self::IndexRef> {
        match self {
            DynamicVectorIndexProvider::Hnsw(p) => HnswIndexProvider::get(p, index_id, cache_key)
                .await
                .map(DynamicVectorIndexRef::Hnsw),
            DynamicVectorIndexProvider::USearch(p) => {
                USearchIndexProvider::get(p, index_id, cache_key)
                    .await
                    .map(DynamicVectorIndexRef::USearch)
            }
        }
    }

    async fn create(
        &self,
        cache_key: &CollectionUuid,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        dimensionality: i32,
        distance_function: DistanceFunction,
        prefix_path: &str,
    ) -> Result<Self::IndexRef, VectorIndexError> {
        match self {
            DynamicVectorIndexProvider::Hnsw(p) => HnswIndexProvider::create(
                p,
                cache_key,
                m,
                ef_construction,
                ef_search,
                dimensionality,
                distance_function,
                prefix_path,
            )
            .await
            .map(DynamicVectorIndexRef::Hnsw)
            .map_err(|e| VectorIndexError::CreateError(e.to_string())),
            DynamicVectorIndexProvider::USearch(p) => USearchIndexProvider::create(
                p,
                cache_key,
                m,
                ef_construction,
                ef_search,
                dimensionality,
                distance_function,
                prefix_path,
            )
            .await
            .map(DynamicVectorIndexRef::USearch)
            .map_err(|e| VectorIndexError::CreateError(e.to_string())),
        }
    }

    async fn open(
        &self,
        id: &IndexUuid,
        cache_key: &CollectionUuid,
        dimensionality: i32,
        distance_function: DistanceFunction,
        ef_search: usize,
        prefix_path: &str,
    ) -> Result<Self::IndexRef, VectorIndexError> {
        match self {
            DynamicVectorIndexProvider::Hnsw(p) => HnswIndexProvider::open(
                p,
                id,
                cache_key,
                dimensionality,
                distance_function,
                ef_search,
                prefix_path,
            )
            .await
            .map(DynamicVectorIndexRef::Hnsw)
            .map_err(|e| VectorIndexError::OpenError(e.to_string())),
            DynamicVectorIndexProvider::USearch(p) => USearchIndexProvider::open(
                p,
                id,
                cache_key,
                dimensionality,
                distance_function,
                ef_search,
                prefix_path,
            )
            .await
            .map(DynamicVectorIndexRef::USearch)
            .map_err(|e| VectorIndexError::OpenError(e.to_string())),
        }
    }

    async fn fork(
        &self,
        source_id: &IndexUuid,
        cache_key: &CollectionUuid,
        dimensionality: i32,
        distance_function: DistanceFunction,
        ef_search: usize,
        prefix_path: &str,
    ) -> Result<Self::IndexRef, VectorIndexError> {
        match self {
            DynamicVectorIndexProvider::Hnsw(p) => HnswIndexProvider::fork(
                p,
                source_id,
                cache_key,
                dimensionality,
                distance_function,
                ef_search,
                prefix_path,
            )
            .await
            .map(DynamicVectorIndexRef::Hnsw)
            .map_err(|e| VectorIndexError::ForkError(e.to_string())),
            DynamicVectorIndexProvider::USearch(p) => USearchIndexProvider::fork(
                p,
                source_id,
                cache_key,
                dimensionality,
                distance_function,
                ef_search,
                prefix_path,
            )
            .await
            .map(DynamicVectorIndexRef::USearch)
            .map_err(|e| VectorIndexError::ForkError(e.to_string())),
        }
    }

    fn commit(&self, index: Self::IndexRef) -> Result<(), VectorIndexError> {
        match (self, index) {
            (DynamicVectorIndexProvider::Hnsw(p), DynamicVectorIndexRef::Hnsw(idx)) => {
                HnswIndexProvider::commit(p, idx)
                    .map_err(|e| VectorIndexError::CommitError(e.to_string()))
            }
            (DynamicVectorIndexProvider::USearch(p), DynamicVectorIndexRef::USearch(idx)) => {
                USearchIndexProvider::commit(p, idx)
                    .map_err(|e| VectorIndexError::CommitError(e.to_string()))
            }
            _ => Err(VectorIndexError::Internal(
                "Provider/Index type mismatch".to_string(),
            )),
        }
    }

    async fn flush(
        &self,
        prefix_path: &str,
        id: &IndexUuid,
        index: &Self::IndexRef,
    ) -> Result<(), VectorIndexError> {
        match (self, index) {
            (DynamicVectorIndexProvider::Hnsw(p), DynamicVectorIndexRef::Hnsw(idx)) => {
                HnswIndexProvider::flush(p, prefix_path, id, idx)
                    .await
                    .map_err(|e| VectorIndexError::FlushError(e.to_string()))
            }
            (DynamicVectorIndexProvider::USearch(p), DynamicVectorIndexRef::USearch(idx)) => {
                USearchIndexProvider::flush(p, prefix_path, id, idx)
                    .await
                    .map_err(|e| VectorIndexError::FlushError(e.to_string()))
            }
            _ => Err(VectorIndexError::Internal(
                "Provider/Index type mismatch".to_string(),
            )),
        }
    }
}
