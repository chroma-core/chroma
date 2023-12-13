use crate::errors::{ChromaError, ErrorCodes};
use thiserror::Error;

#[derive(Clone, Debug)]
pub(crate) struct IndexConfig {
    pub(crate) dimensionality: i32,
    pub(crate) distance_function: DistanceFunction,
}

/// The index trait.
/// # Description
/// This trait defines the interface for a KNN index.
/// # Methods
/// - `init` - Initialize the index with a given dimension and distance function.
/// - `add` - Add a vector to the index.
/// - `query` - Query the index for the K nearest neighbors of a given vector.
pub(crate) trait Index<C> {
    fn init(
        index_config: &IndexConfig,
        custom_config: Option<&C>,
    ) -> Result<Self, Box<dyn ChromaError>>
    where
        Self: Sized;
    fn add(&self, id: usize, vector: &[f32]);
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
    fn load(path: &str, index_config: &IndexConfig) -> Result<Self, Box<dyn ChromaError>>
    where
        Self: Sized;
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
#[derive(Clone, Debug)]
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

impl Into<String> for DistanceFunction {
    fn into(self) -> String {
        match self {
            DistanceFunction::Euclidean => "l2".to_string(),
            DistanceFunction::Cosine => "cosine".to_string(),
            DistanceFunction::InnerProduct => "ip".to_string(),
        }
    }
}
