// Blockfile prefix constants
pub const PREFIX_ROOT: &str = "root";
pub const PREFIX_NEXT_NODE: &str = "next_node";
pub const PREFIX_DIM: &str = "dim";
pub const PREFIX_MAX_VECTOR_ID: &str = "max_vector_id";
pub const PREFIX_EMBEDDING: &str = "embedding";
pub const PREFIX_CENTROID: &str = "centroid";

pub const SINGLETON_KEY: u32 = 0;
pub const NO_PARENT: u32 = u32::MAX;

/// Checkpoint layout 2 stores exactly one posting per immutable vector.
pub const PREFIX_FORMAT: &str = "format";
pub const VERSION_FREE_FORMAT: u32 = 2;
