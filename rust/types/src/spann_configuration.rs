use crate::HnswSpace;
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;
use validator::Validate;

fn default_search_nprobe() -> u32 {
    128
}

fn default_search_rng_factor() -> f32 {
    1.0
}

fn default_search_rng_epsilon() -> f32 {
    10.0
}

fn default_write_nprobe() -> u32 {
    128
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
    #[validate(range(max = 100))]
    pub max_neighbors: usize,
}

impl Default for InternalSpannConfiguration {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Validate, PartialEq, ToSchema)]
pub struct SpannConfiguration {
    #[serde(default = "default_search_nprobe")]
    #[validate(range(max = 128))]
    pub search_nprobe: u32,
    #[serde(default = "default_write_nprobe")]
    #[validate(range(max = 128))]
    pub write_nprobe: u32,
    #[serde(default)]
    pub space: HnswSpace,
    #[serde(default = "default_construction_ef_spann")]
    #[validate(range(max = 200))]
    pub ef_construction: usize,
    #[serde(default = "default_search_ef_spann")]
    #[validate(range(max = 200))]
    pub ef_search: usize,
    #[serde(default = "default_m_spann")]
    #[validate(range(max = 100))]
    pub max_neighbors: usize,
    #[serde(default = "default_reassign_neighbor_count")]
    #[validate(range(max = 100))]
    pub reassign_neighbor_count: u32,
    #[serde(default = "default_split_threshold")]
    #[validate(range(min = 100, max = 200))]
    pub split_threshold: u32,
    #[serde(default = "default_merge_threshold")]
    #[validate(range(min = 50, max = 100))]
    pub merge_threshold: u32,
}

impl From<InternalSpannConfiguration> for SpannConfiguration {
    fn from(config: InternalSpannConfiguration) -> Self {
        Self {
            search_nprobe: config.search_nprobe,
            write_nprobe: config.write_nprobe,
            space: config.space,
            ef_construction: config.ef_construction,
            ef_search: config.ef_search,
            max_neighbors: config.max_neighbors,
            reassign_neighbor_count: config.reassign_neighbor_count,
            split_threshold: config.split_threshold,
            merge_threshold: config.merge_threshold,
        }
    }
}

impl From<SpannConfiguration> for InternalSpannConfiguration {
    fn from(config: SpannConfiguration) -> Self {
        Self {
            search_nprobe: config.search_nprobe,
            write_nprobe: config.write_nprobe,
            space: config.space,
            ef_construction: config.ef_construction,
            ef_search: config.ef_search,
            max_neighbors: config.max_neighbors,
            reassign_neighbor_count: config.reassign_neighbor_count,
            split_threshold: config.split_threshold,
            merge_threshold: config.merge_threshold,
            ..Default::default()
        }
    }
}

impl Default for SpannConfiguration {
    fn default() -> Self {
        InternalSpannConfiguration::default().into()
    }
}
