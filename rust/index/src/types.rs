use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;
use uuid::Uuid;

use crate::WrappedHnswError;

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

/// The index trait.
/// # Description
/// This trait defines the interface for a KNN index.
/// # Methods
/// - `init` - Initialize the index with a given dimension and distance function.
/// - `add` - Add a vector to the index.
/// - `delete` - Delete a vector from the index.
/// - `query` - Query the index for the K nearest neighbors of a given vector.
/// - `resize` - Resize the index to a new capacity.
pub trait Index<C> {
    fn init(
        index_config: &IndexConfig,
        custom_config: Option<&C>,
        id: IndexUuid,
    ) -> Result<Self, Box<dyn ChromaError>>
    where
        Self: Sized;
    fn add(&self, id: usize, vector: &[f32]) -> Result<(), Box<dyn ChromaError>>;
    fn delete(&self, id: usize) -> Result<(), Box<dyn ChromaError>>;
    fn query(
        &self,
        vector: &[f32],
        k: usize,
        allowed_ids: &[usize],
        disallow_ids: &[usize],
    ) -> Result<(Vec<usize>, Vec<f32>), Box<dyn ChromaError>>;
    fn get(&self, id: usize) -> Result<Option<Vec<f32>>, Box<dyn ChromaError>>;
    fn get_all_ids(&self) -> Result<(Vec<usize>, Vec<usize>), Box<dyn ChromaError>>;
    fn get_all_ids_sizes(&self) -> Result<Vec<usize>, Box<dyn ChromaError>>;
}

/// The persistent index trait.
/// # Description
/// This trait defines the interface for a persistent KNN index.
/// # Methods
/// - `save` - Save the index to a given path. Configuration of the destination is up to the implementation.
/// - `load` - Load the index from a given path.
/// # Notes
/// This defines a rudimentary interface for saving and loading indices.
/// TODO: Right now load() takes IndexConfig because we don't implement save/load of the config.
pub trait PersistentIndex<C>: Index<C> {
    fn save(&self) -> Result<(), Box<dyn ChromaError>>;
    fn load(
        path: &str,
        index_config: &IndexConfig,
        ef_search: usize,
        id: IndexUuid,
    ) -> Result<Self, Box<dyn ChromaError>>
    where
        Self: Sized;

    // This function is used to load the index from memory without using disk.
    // TODO(tanujnay112): Replace `load` from above with this once we stablize
    // loading HNSW via memory.
    fn load_from_hnsw_data(
        hnsw_data: &hnswlib::HnswData,
        index_config: &IndexConfig,
        ef_search: usize,
        id: IndexUuid,
    ) -> Result<Self, Box<dyn ChromaError>>
    where
        Self: Sized;

    fn serialize_to_hnsw_data(&self) -> Result<hnswlib::HnswData, WrappedHnswError>;
}

/// IndexUuid is a wrapper around Uuid to provide a type for the index id.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IndexUuid(pub Uuid);

impl std::fmt::Display for IndexUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
