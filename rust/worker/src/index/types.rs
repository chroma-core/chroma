use crate::distance::{DistanceFunction, DistanceFunctionError};
use crate::errors::{ChromaError, ErrorCodes};
use crate::types::{MetadataValue, Segment};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub(crate) struct IndexConfig {
    pub(crate) dimensionality: i32,
    pub(crate) distance_function: DistanceFunction,
}

#[derive(Error, Debug)]
pub(crate) enum IndexConfigFromSegmentError {
    #[error("Invalid distance function")]
    InvalidDistanceFunction(#[from] DistanceFunctionError),
}

impl ChromaError for IndexConfigFromSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            IndexConfigFromSegmentError::InvalidDistanceFunction(_) => ErrorCodes::InvalidArgument,
        }
    }
}

impl IndexConfig {
    pub(crate) fn from_segment(
        segment: &Segment,
        dimensionality: i32,
    ) -> Result<Self, Box<IndexConfigFromSegmentError>> {
        let space = match segment.metadata {
            Some(ref metadata) => match metadata.get("hnsw:space") {
                Some(MetadataValue::Str(space)) => space,
                _ => "l2",
            },
            None => "l2",
        };
        match DistanceFunction::try_from(space) {
            Ok(distance_function) => Ok(IndexConfig {
                dimensionality,
                distance_function,
            }),
            Err(e) => Err(Box::new(
                IndexConfigFromSegmentError::InvalidDistanceFunction(e),
            )),
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
pub(crate) trait Index<C> {
    fn init(
        index_config: &IndexConfig,
        custom_config: Option<&C>,
        id: Uuid,
    ) -> Result<Self, Box<dyn ChromaError>>
    where
        Self: Sized;
    fn add(&self, id: usize, vector: &[f32]);
    fn delete(&self, id: usize);
    fn query(&self, vector: &[f32], k: usize) -> (Vec<usize>, Vec<f32>);
    fn get(&self, id: usize) -> Option<Vec<f32>>;
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
pub(crate) trait PersistentIndex<C>: Index<C> {
    fn save(&self) -> Result<(), Box<dyn ChromaError>>;
    fn load(path: &str, index_config: &IndexConfig, id: Uuid) -> Result<Self, Box<dyn ChromaError>>
    where
        Self: Sized;
}
