use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct IndexConfig {
    pub dimensionality: i32,
    pub distance_function: DistanceFunction,
}

impl IndexConfig {
    pub fn new(dimensionality: i32, distance_function: DistanceFunction) -> Self {
        IndexConfig {
            dimensionality,
            distance_function,
        }
    }
}

/// Result of a vector search operation.
#[derive(Clone, Debug)]
pub struct SearchResult {
    pub keys: Vec<u64>,
    pub distances: Vec<f32>,
}

/// Trait for dense vector indexes supporting CRUD and similarity search.
pub trait VectorIndex {
    type Error: ChromaError;

    /// Add a vector to the index with the given key.
    fn add(&self, key: u64, vector: &[f32]) -> Result<(), Self::Error>;

    /// Returns the current capacity of the index.
    fn capacity(&self) -> Result<usize, Self::Error>;

    /// Retrieve the vector for a given key.
    /// Returns `None` if the key doesn't exist.
    fn get(&self, key: u64) -> Result<Option<Vec<f32>>, Self::Error>;

    /// Returns true if the index contains no vectors.
    fn is_empty(&self) -> Result<bool, Self::Error> {
        Ok(self.len()? == 0)
    }

    /// Returns the number of vectors in the index.
    fn len(&self) -> Result<usize, Self::Error>;

    /// Remove a vector from the index by key.
    fn remove(&self, key: u64) -> Result<(), Self::Error>;

    /// Reserve capacity for at least `capacity` vectors.
    fn reserve(&self, capacity: usize) -> Result<(), Self::Error>;

    /// Search for the nearest neighbors of a query vector.
    fn search(&self, query: &[f32], count: usize) -> Result<SearchResult, Self::Error>;
}

/// IndexUuid is a wrapper around Uuid to provide a type for the index id.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IndexUuid(pub Uuid);

impl std::fmt::Display for IndexUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Mode for opening a vector index.
#[derive(Clone, Debug)]
pub enum OpenMode {
    /// Create a new empty index.
    Create,
    /// Open an existing index by ID.
    Open(IndexUuid),
    /// Fork an existing index for writing (clones data, assigns new UUID).
    Fork(IndexUuid),
}

/// Trait for managing the lifecycle of vector indexes.
#[async_trait::async_trait]
pub trait VectorIndexProvider {
    type Index: VectorIndex;
    type Config;
    type Error: ChromaError;

    /// Finalize the index and return its ID.
    async fn commit(&self, index: &Self::Index) -> Result<IndexUuid, Self::Error>;

    /// Persist the index to storage.
    async fn flush(&self, index: &Self::Index) -> Result<(), Self::Error>;

    /// Open a vector index.
    ///
    /// # Modes
    /// - `Create`: Create a new empty index
    /// - `Open(id)`: Load an existing index by ID
    /// - `Fork(id)`: Clone an existing index for writing (new UUID)
    async fn open(&self, config: &Self::Config, mode: OpenMode)
        -> Result<Self::Index, Self::Error>;
}
