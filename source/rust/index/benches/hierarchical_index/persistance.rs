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

// An absent marker means legacy one-bit node codes. Version 1 stores packed
// four-bit origin codes, including dimensions and distance corrections.
pub const PREFIX_NAVIGATION_FORMAT: &str = "navigation_format";
pub const PACKED_NAVIGATION_FORMAT: u32 = 1;

#[derive(Debug, thiserror::Error)]
#[error("unsupported hierarchical navigation format {0}")]
struct UnsupportedNavigationFormat(u32);

impl chroma_error::ChromaError for UnsupportedNavigationFormat {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::DataLoss
    }
}

pub fn validate_navigation_format(
    format: Option<u32>,
) -> Result<(), Box<dyn chroma_error::ChromaError>> {
    match format {
        None | Some(PACKED_NAVIGATION_FORMAT) => Ok(()),
        Some(other) => Err(Box::new(UnsupportedNavigationFormat(other))),
    }
}

#[derive(Debug, thiserror::Error)]
#[error(
    "node {node} centroid is missing, nonfinite, or has {actual} dimensions instead of {expected}"
)]
struct InvalidOriginalCentroid {
    node: u32,
    actual: usize,
    expected: usize,
}

impl chroma_error::ChromaError for InvalidOriginalCentroid {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::DataLoss
    }
}

pub fn validate_original_centroid(
    node: u32,
    centroid: &[f32],
    dim: usize,
) -> Result<(), Box<dyn chroma_error::ChromaError>> {
    if dim == 0 || centroid.len() != dim || centroid.iter().any(|v| !v.is_finite()) {
        return Err(Box::new(InvalidOriginalCentroid {
            node,
            actual: centroid.len(),
            expected: dim,
        }));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("centroid packing task failed: {0}")]
pub struct CentroidPackingTaskError(pub tokio::task::JoinError);

impl chroma_error::ChromaError for CentroidPackingTaskError {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::Internal
    }
}
