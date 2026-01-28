use chroma_distance::DistanceFunction;
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

// NOTE: The Index and PersistentIndex traits have been removed.
// Methods are now implemented directly on HnswIndex in hnsw.rs.

/// IndexUuid is a wrapper around Uuid to provide a type for the index id.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IndexUuid(pub Uuid);

impl std::fmt::Display for IndexUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
