use crate::errors::{ChromaError, ErrorCodes};
use thiserror::Error;

/// The index trait.
/// # Description
/// This trait defines the interface for a KNN index.
/// # Methods
/// - `init` - Initialize the index with a given dimension.
/// - `add` - Add a vector to the index.
/// - `query` - Query the index for the K nearest neighbors of a given vector.
/// - `get_distance_function` - Get the distance function used by the index.
pub(crate) trait Index {
    fn init(&self, dim: i64);
    fn add(&self, id: usize, vector: &[f32]);
    fn query(&self, vector: &[f32], k: usize) -> Vec<(usize, f32)>;
    fn get_distance_function(&self) -> DistanceFunction;
}

/// The persistent index trait.
/// # Description
/// This trait defines the interface for a persistent KNN index.
/// # Methods
/// - `save` - Save the index to a given path.
/// - `load` - Load the index from a given path.
/// # Notes
/// This defines a rudimentary interface for saving and loading indices.
pub(crate) trait PersistentIndex: Index {
    fn save(&self, path: &str) -> Result<(), Box<dyn ChromaError>>;
    fn load(&mut self, path: &str) -> Result<(), Box<dyn ChromaError>>;
}

/// The distance function enum.
/// # Description
/// This enum defines the distance functions supported by indices in Chroma.
/// # Variants
/// - `Euclidean` - The Euclidean or l2 norm.
/// - `Cosine` - The cosine distance. Specifically, 1 - cosine.
/// - `InnerProduct` - The inner product. Specifically, 1 - inner product.
/// # Notes
/// See https://docs.trychroma.com/usage-guide#changing-the-distance-function
pub(crate) enum DistanceFunction {
    Euclidean,
    Cosine,
    InnerProduct,
}

#[derive(Error, Debug)]
pub(crate) enum DistanceFunctionError {
    #[error("Invalid distance function `{0}`")]
    InvalidDistanceFunction(String),
}

impl ChromaError for DistanceFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            DistanceFunctionError::InvalidDistanceFunction(_) => ErrorCodes::InvalidArgument,
        }
    }
}

impl TryFrom<&str> for DistanceFunction {
    type Error = DistanceFunctionError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "l2" => Ok(DistanceFunction::Euclidean),
            "cosine" => Ok(DistanceFunction::Cosine),
            "ip" => Ok(DistanceFunction::InnerProduct),
            _ => Err(DistanceFunctionError::InvalidDistanceFunction(
                value.to_string(),
            )),
        }
    }
}
