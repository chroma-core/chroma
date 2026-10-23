use crate::{HnswIndexConfig, Metadata};
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use std::num::NonZero;
use thiserror::Error;
use validator::Validate;

#[derive(Debug, Error)]
pub enum HnswParametersFromSegmentError {
    #[error("Invalid metadata: {0}")]
    InvalidMetadata(#[from] serde_json::Error),
    #[error("Invalid parameters: {0}")]
    InvalidParameters(#[from] validator::ValidationErrors),
    #[error("Incompatible vector index types")]
    IncompatibleVectorIndexTypes,
}

impl ChromaError for HnswParametersFromSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            HnswParametersFromSegmentError::InvalidMetadata(_) => ErrorCodes::InvalidArgument,
            HnswParametersFromSegmentError::InvalidParameters(_) => ErrorCodes::InvalidArgument,
            HnswParametersFromSegmentError::IncompatibleVectorIndexTypes => {
                ErrorCodes::InvalidArgument
            }
        }
    }
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum Space {
    #[default]
    #[serde(rename = "l2")]
    L2,
    #[serde(rename = "cosine")]
    Cosine,
    #[serde(rename = "ip")]
    Ip,
}

pub fn default_construction_ef() -> usize {
    100
}

pub fn default_search_ef() -> usize {
    100
}

pub fn default_m() -> usize {
    16
}

pub fn default_num_threads() -> usize {
    std::thread::available_parallelism()
        .unwrap_or(NonZero::new(1).unwrap())
        .get()
}

pub fn default_resize_factor() -> f64 {
    1.2
}

pub fn default_sync_threshold() -> usize {
    1000
}

pub fn default_batch_size() -> usize {
    100
}

pub fn default_space() -> Space {
    Space::L2
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct InternalHnswConfiguration {
    #[serde(default = "default_space")]
    pub space: Space,
    #[serde(default = "default_construction_ef")]
    pub ef_construction: usize,
    #[serde(default = "default_search_ef")]
    pub ef_search: usize,
    #[serde(default = "default_m")]
    pub max_neighbors: usize,
    #[serde(default = "default_num_threads")]
    #[serde(skip_serializing)]
    pub num_threads: usize,
    #[serde(default = "default_resize_factor")]
    pub resize_factor: f64,
    #[validate(range(min = 2))]
    #[serde(default = "default_sync_threshold")]
    pub sync_threshold: usize,
    #[validate(range(min = 2))]
    #[serde(default = "default_batch_size")]
    #[serde(skip_serializing)]
    pub batch_size: usize,
}

impl Default for InternalHnswConfiguration {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl From<(Option<&Space>, Option<&HnswIndexConfig>)> for InternalHnswConfiguration {
    fn from((space, config): (Option<&Space>, Option<&HnswIndexConfig>)) -> Self {
        let mut internal = InternalHnswConfiguration::default();

        if let Some(space) = space {
            internal.space = space.clone();
        }

        if let Some(config) = config {
            if let Some(ef_construction) = config.ef_construction {
                internal.ef_construction = ef_construction;
            }
            if let Some(max_neighbors) = config.max_neighbors {
                internal.max_neighbors = max_neighbors;
            }
            if let Some(ef_search) = config.ef_search {
                internal.ef_search = ef_search;
            }
            if let Some(num_threads) = config.num_threads {
                internal.num_threads = num_threads;
            }
            if let Some(batch_size) = config.batch_size {
                internal.batch_size = batch_size;
            }
            if let Some(sync_threshold) = config.sync_threshold {
                internal.sync_threshold = sync_threshold;
            }
            if let Some(resize_factor) = config.resize_factor {
                internal.resize_factor = resize_factor;
            }
        }

        internal
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct HnswConfiguration {
    pub space: Option<Space>,
    pub ef_construction: Option<usize>,
    pub ef_search: Option<usize>,
    pub max_neighbors: Option<usize>,
    #[serde(skip_serializing)]
    pub num_threads: Option<usize>,
    pub resize_factor: Option<f64>,
    #[validate(range(min = 2))]
    pub sync_threshold: Option<usize>,
    #[validate(range(min = 2))]
    #[serde(skip_serializing)]
    pub batch_size: Option<usize>,
}

impl From<InternalHnswConfiguration> for HnswConfiguration {
    fn from(config: InternalHnswConfiguration) -> Self {
        Self {
            space: Some(config.space),
            ef_construction: Some(config.ef_construction),
            ef_search: Some(config.ef_search),
            max_neighbors: Some(config.max_neighbors),
            num_threads: Some(config.num_threads),
            resize_factor: Some(config.resize_factor),
            sync_threshold: Some(config.sync_threshold),
            batch_size: Some(config.batch_size),
        }
    }
}

impl From<HnswConfiguration> for InternalHnswConfiguration {
    fn from(config: HnswConfiguration) -> Self {
        Self {
            space: config.space.unwrap_or(default_space()),
            ef_construction: config.ef_construction.unwrap_or(default_construction_ef()),
            ef_search: config.ef_search.unwrap_or(default_search_ef()),
            max_neighbors: config.max_neighbors.unwrap_or(default_m()),
            num_threads: config.num_threads.unwrap_or(default_num_threads()),
            resize_factor: config.resize_factor.unwrap_or(default_resize_factor()),
            sync_threshold: config.sync_threshold.unwrap_or(default_sync_threshold()),
            batch_size: config.batch_size.unwrap_or(default_batch_size()),
        }
    }
}

impl Default for HnswConfiguration {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl InternalHnswConfiguration {
    pub(crate) fn from_legacy_segment_metadata(
        segment_metadata: &Option<Metadata>,
    ) -> Result<Self, HnswParametersFromSegmentError> {
        if let Some(metadata) = segment_metadata {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct LegacyMetadataLocalHnswParameters {
                #[serde(rename = "hnsw:space", default)]
                pub space: Space,
                #[serde(rename = "hnsw:construction_ef", default = "default_construction_ef")]
                pub construction_ef: usize,
                #[serde(rename = "hnsw:search_ef", default = "default_search_ef")]
                pub search_ef: usize,
                #[serde(rename = "hnsw:M", default = "default_m")]
                pub m: usize,
                #[serde(rename = "hnsw:num_threads", default = "default_num_threads")]
                pub num_threads: usize,
                #[serde(rename = "hnsw:resize_factor", default = "default_resize_factor")]
                pub resize_factor: f64,
                #[serde(rename = "hnsw:sync_threshold", default = "default_sync_threshold")]
                pub sync_threshold: usize,
                #[serde(rename = "hnsw:batch_size", default = "default_batch_size")]
                pub batch_size: usize,
            }

            let filtered_metadata = metadata
                .clone()
                .into_iter()
                .filter(|(k, _)| k.starts_with("hnsw:"))
                .collect::<Metadata>();

            let metadata_str = serde_json::to_string(&filtered_metadata)?;
            let parsed = serde_json::from_str::<LegacyMetadataLocalHnswParameters>(&metadata_str)?;
            let params = InternalHnswConfiguration {
                space: parsed.space,
                ef_construction: parsed.construction_ef,
                ef_search: parsed.search_ef,
                max_neighbors: parsed.m,
                num_threads: parsed.num_threads,
                resize_factor: parsed.resize_factor,
                sync_threshold: parsed.sync_threshold,
                batch_size: parsed.batch_size,
            };
            params.validate()?;
            Ok(params)
        } else {
            Ok(InternalHnswConfiguration::default())
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, Validate)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct UpdateHnswConfiguration {
    pub ef_search: Option<usize>,
    pub max_neighbors: Option<usize>,
    pub num_threads: Option<usize>,
    pub resize_factor: Option<f64>,
    #[validate(range(min = 2))]
    pub sync_threshold: Option<usize>,
    #[validate(range(min = 2))]
    pub batch_size: Option<usize>,
}
