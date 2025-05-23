use crate::HnswSpace;
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;
use validator::Validate;

fn default_search_nprobe() -> u32 {
    64
}

fn default_search_rng_factor() -> f32 {
    1.0
}

fn default_search_rng_epsilon() -> f32 {
    10.0
}

fn default_write_nprobe() -> u32 {
    64
}

fn default_write_rng_factor() -> f32 {
    1.0
}

fn default_write_rng_epsilon() -> f32 {
    10.0
}

fn default_split_threshold() -> u32 {
    200
}

fn default_num_samples_kmeans() -> usize {
    1000
}

fn default_initial_lambda() -> f32 {
    100.0
}

fn default_reassign_neighbor_count() -> u32 {
    64
}

fn default_merge_threshold() -> u32 {
    100
}

fn default_num_centers_to_merge_to() -> u32 {
    8
}

fn default_construction_ef_spann() -> usize {
    200
}

fn default_search_ef_spann() -> usize {
    200
}

fn default_m_spann() -> usize {
    64
}

#[derive(Debug, Error)]
pub enum DistributedSpannParametersFromSegmentError {
    #[error("Invalid metadata: {0}")]
    InvalidMetadata(#[from] serde_json::Error),
    #[error("Invalid parameters: {0}")]
    InvalidParameters(#[from] validator::ValidationErrors),
}

impl ChromaError for DistributedSpannParametersFromSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            DistributedSpannParametersFromSegmentError::InvalidMetadata(_) => {
                ErrorCodes::InvalidArgument
            }
            DistributedSpannParametersFromSegmentError::InvalidParameters(_) => {
                ErrorCodes::InvalidArgument
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Validate, PartialEq, ToSchema)]
pub struct InternalSpannConfiguration {
    #[serde(default = "default_search_nprobe")]
    pub search_nprobe: u32,
    #[serde(default = "default_search_rng_factor")]
    pub search_rng_factor: f32,
    #[serde(default = "default_search_rng_epsilon")]
    pub search_rng_epsilon: f32,
    #[serde(default = "default_write_nprobe")]
    #[validate(range(max = 128))]
    pub write_nprobe: u32,
    #[serde(default = "default_write_rng_factor")]
    pub write_rng_factor: f32,
    #[serde(default = "default_write_rng_epsilon")]
    pub write_rng_epsilon: f32,
    #[serde(default = "default_split_threshold")]
    #[validate(range(min = 100, max = 200))]
    pub split_threshold: u32,
    #[serde(default = "default_num_samples_kmeans")]
    pub num_samples_kmeans: usize,
    #[serde(default = "default_initial_lambda")]
    pub initial_lambda: f32,
    #[serde(default = "default_reassign_neighbor_count")]
    #[validate(range(max = 64))]
    pub reassign_neighbor_count: u32,
    #[serde(default = "default_merge_threshold")]
    #[validate(range(min = 50, max = 100))]
    pub merge_threshold: u32,
    #[serde(default = "default_num_centers_to_merge_to")]
    #[validate(range(max = 8))]
    pub num_centers_to_merge_to: u32,
    #[serde(default)]
    pub space: HnswSpace,
    #[serde(default = "default_construction_ef_spann")]
    #[validate(range(max = 200))]
    pub ef_construction: usize,
    #[serde(default = "default_search_ef_spann")]
    #[validate(range(max = 200))]
    pub ef_search: usize,
    #[serde(default = "default_m_spann")]
    #[validate(range(max = 64))]
    pub max_neighbors: usize,
}

impl Default for InternalSpannConfiguration {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Validate, PartialEq, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SpannConfiguration {
    pub search_nprobe: Option<u32>,
    pub write_nprobe: Option<u32>,
    #[serde(default)]
    pub space: HnswSpace,
    pub ef_construction: Option<usize>,
    pub ef_search: Option<usize>,
    pub max_neighbors: Option<usize>,
    pub reassign_neighbor_count: Option<u32>,
    pub split_threshold: Option<u32>,
    pub merge_threshold: Option<u32>,
}

impl From<InternalSpannConfiguration> for SpannConfiguration {
    fn from(config: InternalSpannConfiguration) -> Self {
        Self {
            search_nprobe: Some(config.search_nprobe),
            write_nprobe: Some(config.write_nprobe),
            space: config.space,
            ef_construction: Some(config.ef_construction),
            ef_search: Some(config.ef_search),
            max_neighbors: Some(config.max_neighbors),
            reassign_neighbor_count: Some(config.reassign_neighbor_count),
            split_threshold: Some(config.split_threshold),
            merge_threshold: Some(config.merge_threshold),
        }
    }
}

impl From<SpannConfiguration> for InternalSpannConfiguration {
    fn from(config: SpannConfiguration) -> Self {
        Self {
            search_nprobe: config.search_nprobe.unwrap_or(default_search_nprobe()),
            write_nprobe: config.write_nprobe.unwrap_or(default_write_nprobe()),
            space: config.space,
            ef_construction: config
                .ef_construction
                .unwrap_or(default_construction_ef_spann()),
            ef_search: config.ef_search.unwrap_or(default_search_ef_spann()),
            max_neighbors: config.max_neighbors.unwrap_or(default_m_spann()),
            reassign_neighbor_count: config
                .reassign_neighbor_count
                .unwrap_or(default_reassign_neighbor_count()),
            split_threshold: config.split_threshold.unwrap_or(default_split_threshold()),
            merge_threshold: config.merge_threshold.unwrap_or(default_merge_threshold()),
            ..Default::default()
        }
    }
}

impl Default for SpannConfiguration {
    fn default() -> Self {
        InternalSpannConfiguration::default().into()
    }
}
