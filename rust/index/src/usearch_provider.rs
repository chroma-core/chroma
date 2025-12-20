//! Simple in-memory provider for UsearchIndex.
//!
//! This is a prototype implementation for benchmarking purposes.
//! It does NOT support:
//! - Persistence to disk or S3
//! - Complex caching
//! - Forking from existing indexes
//!
//! It DOES support:
//! - Creating new in-memory indexes
//! - Thread-safe access (UsearchIndex is internally thread-safe)

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use parking_lot::RwLock;
use thiserror::Error;
use uuid::Uuid;

use crate::usearch::{UsearchError, UsearchIndex, UsearchIndexConfig};
use crate::{Index, IndexConfig, IndexUuid};

/// Reference to a UsearchIndex.
/// No RwLock needed - UsearchIndex is internally thread-safe.
#[derive(Clone)]
pub struct UsearchIndexRef {
    pub inner: Arc<UsearchIndex>,
    pub prefix_path: String,
}

impl Debug for UsearchIndexRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsearchIndexRef")
            .field("id", &self.inner.id)
            .field("dimensionality", &self.inner.dimensionality)
            .field("len", &self.inner.len())
            .field("prefix_path", &self.prefix_path)
            .finish_non_exhaustive()
    }
}

/// Simple in-memory provider for UsearchIndex.
///
/// This provider creates and manages UsearchIndex instances in memory.
/// It's designed for benchmarking and prototyping, not production use.
#[derive(Clone)]
pub struct UsearchIndexProvider {
    /// Cache of created indexes by their UUID.
    indexes: Arc<RwLock<HashMap<IndexUuid, UsearchIndexRef>>>,
    /// Default HNSW parameters.
    pub default_m: usize,
    pub default_ef_construction: usize,
    pub default_ef_search: usize,
}

impl Debug for UsearchIndexProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsearchIndexProvider")
            .field("num_indexes", &self.indexes.read().len())
            .finish_non_exhaustive()
    }
}

impl Default for UsearchIndexProvider {
    fn default() -> Self {
        Self::new(16, 100, 100)
    }
}

impl UsearchIndexProvider {
    /// Create a new UsearchIndexProvider with default HNSW parameters.
    pub fn new(default_m: usize, default_ef_construction: usize, default_ef_search: usize) -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
            default_m,
            default_ef_construction,
            default_ef_search,
        }
    }

    /// Create a new in-memory UsearchIndex.
    ///
    /// # Arguments
    /// * `dimensionality` - Number of dimensions for vectors
    /// * `distance_function` - Distance metric to use
    /// * `max_elements` - Initial capacity for the index
    /// * `m` - HNSW connectivity parameter (optional, uses default if None)
    /// * `ef_construction` - HNSW construction parameter (optional, uses default if None)
    /// * `ef_search` - HNSW search parameter (optional, uses default if None)
    /// * `prefix_path` - Path prefix for the index (for SPANN compatibility)
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        &self,
        dimensionality: usize,
        distance_function: DistanceFunction,
        max_elements: usize,
        m: Option<usize>,
        ef_construction: Option<usize>,
        ef_search: Option<usize>,
        prefix_path: &str,
    ) -> Result<UsearchIndexRef, Box<dyn ChromaError>> {
        let id = IndexUuid(Uuid::new_v4());

        let index_config = IndexConfig {
            dimensionality: dimensionality as i32,
            distance_function,
        };

        let usearch_config = UsearchIndexConfig::new(
            max_elements,
            m.unwrap_or(self.default_m),
            ef_construction.unwrap_or(self.default_ef_construction),
            ef_search.unwrap_or(self.default_ef_search),
        );

        let index = UsearchIndex::init(&index_config, Some(&usearch_config), id)?;

        let index_ref = UsearchIndexRef {
            inner: Arc::new(index),
            prefix_path: prefix_path.to_string(),
        };

        self.indexes.write().insert(id, index_ref.clone());

        Ok(index_ref)
    }

    /// Get an existing index by its UUID.
    pub fn get(&self, id: &IndexUuid) -> Option<UsearchIndexRef> {
        self.indexes.read().get(id).cloned()
    }

    /// Remove an index from the provider.
    pub fn remove(&self, id: &IndexUuid) -> Option<UsearchIndexRef> {
        self.indexes.write().remove(id)
    }

    /// Clear all indexes from the provider.
    pub fn clear(&self) {
        self.indexes.write().clear();
    }

    /// Get the number of indexes managed by this provider.
    pub fn len(&self) -> usize {
        self.indexes.read().len()
    }

    /// Check if there are no indexes.
    pub fn is_empty(&self) -> bool {
        self.indexes.read().is_empty()
    }
}

#[derive(Error, Debug)]
pub enum UsearchIndexProviderError {
    #[error("Index not found: {0}")]
    IndexNotFound(IndexUuid),

    #[error("Index creation failed: {0}")]
    CreateError(#[from] UsearchError),
}

impl ChromaError for UsearchIndexProviderError {
    fn code(&self) -> ErrorCodes {
        match self {
            UsearchIndexProviderError::IndexNotFound(_) => ErrorCodes::NotFound,
            UsearchIndexProviderError::CreateError(e) => e.code(),
        }
    }
}
