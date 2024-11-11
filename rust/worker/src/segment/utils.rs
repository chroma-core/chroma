use chroma_distance::{DistanceFunction, DistanceFunctionError};
use chroma_index::{DEFAULT_HNSW_EF_CONSTRUCTION, DEFAULT_HNSW_EF_SEARCH, DEFAULT_HNSW_M};
use chroma_types::{get_metadata_value_as, MetadataValue, Segment};

use super::distributed_hnsw_segment::HnswIndexParamsFromSegment;

pub(super) fn hnsw_params_from_segment(segment: &Segment) -> HnswIndexParamsFromSegment {
    let metadata = match &segment.metadata {
        Some(metadata) => metadata,
        None => {
            return HnswIndexParamsFromSegment {
                m: DEFAULT_HNSW_M,
                ef_construction: DEFAULT_HNSW_EF_CONSTRUCTION,
                ef_search: DEFAULT_HNSW_EF_SEARCH,
            };
        }
    };

    let m = match get_metadata_value_as::<i64>(metadata, "hnsw:M") {
        Ok(m) => m as usize,
        Err(_) => DEFAULT_HNSW_M,
    };
    let ef_construction = match get_metadata_value_as::<i64>(metadata, "hnsw:construction_ef") {
        Ok(ef_construction) => ef_construction as usize,
        Err(_) => DEFAULT_HNSW_EF_CONSTRUCTION,
    };
    let ef_search = match get_metadata_value_as::<i64>(metadata, "hnsw:search_ef") {
        Ok(ef_search) => ef_search as usize,
        Err(_) => DEFAULT_HNSW_EF_SEARCH,
    };

    HnswIndexParamsFromSegment {
        m,
        ef_construction,
        ef_search,
    }
}

pub(crate) fn distance_function_from_segment(
    segment: &Segment,
) -> Result<DistanceFunction, Box<DistanceFunctionError>> {
    let space = match segment.metadata {
        Some(ref metadata) => match metadata.get("hnsw:space") {
            Some(MetadataValue::Str(space)) => space,
            _ => "l2",
        },
        None => "l2",
    };
    match DistanceFunction::try_from(space) {
        Ok(distance_function) => Ok(distance_function),
        Err(e) => Err(Box::new(e)),
    }
}
