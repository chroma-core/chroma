use crate::{default_space, hnsw_configuration::Space, SpannIndexConfig};
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use validator::Validate;

pub fn default_search_nprobe() -> u32 {
    64
}

pub fn default_search_rng_factor() -> f32 {
    1.0
}

pub fn default_search_rng_epsilon() -> f32 {
    10.0
}

pub fn default_write_nprobe() -> u32 {
    32
}

pub fn default_nreplica_count() -> u32 {
    8
}

pub fn default_write_rng_factor() -> f32 {
    1.0
}

pub fn default_write_rng_epsilon() -> f32 {
    5.0
}

pub fn default_split_threshold() -> u32 {
    50
}

pub fn default_num_samples_kmeans() -> usize {
    1000
}

pub fn default_initial_lambda() -> f32 {
    100.0
}

pub fn default_reassign_neighbor_count() -> u32 {
    64
}

pub fn default_merge_threshold() -> u32 {
    25
}

pub fn default_num_centers_to_merge_to() -> u32 {
    8
}

pub fn default_construction_ef_spann() -> usize {
    200
}

pub fn default_search_ef_spann() -> usize {
    200
}

pub fn default_m_spann() -> usize {
    64
}

fn default_space_spann() -> Space {
    Space::L2
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

#[derive(Clone, Debug, Serialize, Deserialize, Validate, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
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
    #[serde(default = "default_nreplica_count")]
    #[validate(range(max = 8))]
    pub nreplica_count: u32,
    #[serde(default = "default_write_rng_factor")]
    pub write_rng_factor: f32,
    #[serde(default = "default_write_rng_epsilon")]
    pub write_rng_epsilon: f32,
    #[serde(default = "default_split_threshold")]
    #[validate(range(min = 25, max = 200))]
    pub split_threshold: u32,
    #[serde(default = "default_num_samples_kmeans")]
    pub num_samples_kmeans: usize,
    #[serde(default = "default_initial_lambda")]
    pub initial_lambda: f32,
    #[serde(default = "default_reassign_neighbor_count")]
    #[validate(range(max = 64))]
    pub reassign_neighbor_count: u32,
    #[serde(default = "default_merge_threshold")]
    #[validate(range(min = 12, max = 100))]
    pub merge_threshold: u32,
    #[serde(default = "default_num_centers_to_merge_to")]
    #[validate(range(max = 8))]
    pub num_centers_to_merge_to: u32,
    #[serde(default = "default_space_spann")]
    pub space: Space,
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

impl From<(Option<&Space>, &SpannIndexConfig)> for InternalSpannConfiguration {
    fn from((space, config): (Option<&Space>, &SpannIndexConfig)) -> Self {
        InternalSpannConfiguration {
            search_nprobe: config.search_nprobe.unwrap_or(default_search_nprobe()),
            search_rng_factor: config
                .search_rng_factor
                .unwrap_or(default_search_rng_factor()),
            search_rng_epsilon: config
                .search_rng_epsilon
                .unwrap_or(default_search_rng_epsilon()),
            nreplica_count: config.nreplica_count.unwrap_or(default_nreplica_count()),
            write_rng_factor: config
                .write_rng_factor
                .unwrap_or(default_write_rng_factor()),
            write_rng_epsilon: config
                .write_rng_epsilon
                .unwrap_or(default_write_rng_epsilon()),
            split_threshold: config.split_threshold.unwrap_or(default_split_threshold()),
            num_samples_kmeans: config
                .num_samples_kmeans
                .unwrap_or(default_num_samples_kmeans()),
            initial_lambda: config.initial_lambda.unwrap_or(default_initial_lambda()),
            reassign_neighbor_count: config
                .reassign_neighbor_count
                .unwrap_or(default_reassign_neighbor_count()),
            merge_threshold: config.merge_threshold.unwrap_or(default_merge_threshold()),
            num_centers_to_merge_to: config
                .num_centers_to_merge_to
                .unwrap_or(default_num_centers_to_merge_to()),
            write_nprobe: config.write_nprobe.unwrap_or(default_write_nprobe()),
            ef_construction: config
                .ef_construction
                .unwrap_or(default_construction_ef_spann()),
            ef_search: config.ef_search.unwrap_or(default_search_ef_spann()),
            max_neighbors: config.max_neighbors.unwrap_or(default_m_spann()),
            space: space.unwrap_or(&default_space()).clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Validate, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct SpannConfiguration {
    pub search_nprobe: Option<u32>,
    pub write_nprobe: Option<u32>,
    pub space: Option<Space>,
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
            space: Some(config.space),
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
            space: config.space.unwrap_or(default_space_spann()),
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

#[derive(Clone, Default, Debug, Serialize, Deserialize, Validate, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct UpdateSpannConfiguration {
    #[validate(range(max = 128))]
    pub search_nprobe: Option<u32>,
    #[validate(range(max = 200))]
    pub ef_search: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spann_configuration_to_internal_spann_configuration() {
        let spann_config = SpannConfiguration {
            search_nprobe: Some(100),
            write_nprobe: Some(50),
            space: Some(Space::Cosine),
            ef_construction: Some(150),
            ef_search: Some(180),
            max_neighbors: Some(32),
            reassign_neighbor_count: Some(48),
            split_threshold: Some(75),
            merge_threshold: Some(50),
        };

        let internal_config: InternalSpannConfiguration = spann_config.into();

        assert_eq!(internal_config.search_nprobe, 100);
        assert_eq!(internal_config.write_nprobe, 50);
        assert_eq!(internal_config.space, Space::Cosine);
        assert_eq!(internal_config.ef_construction, 150);
        assert_eq!(internal_config.ef_search, 180);
        assert_eq!(internal_config.max_neighbors, 32);
        assert_eq!(internal_config.reassign_neighbor_count, 48);
        assert_eq!(internal_config.split_threshold, 75);
        assert_eq!(internal_config.merge_threshold, 50);
        assert_eq!(
            internal_config.search_rng_factor,
            default_search_rng_factor()
        );
        assert_eq!(
            internal_config.search_rng_epsilon,
            default_search_rng_epsilon()
        );
        assert_eq!(internal_config.nreplica_count, default_nreplica_count());
        assert_eq!(internal_config.write_rng_factor, default_write_rng_factor());
        assert_eq!(
            internal_config.write_rng_epsilon,
            default_write_rng_epsilon()
        );
        assert_eq!(
            internal_config.num_samples_kmeans,
            default_num_samples_kmeans()
        );
        assert_eq!(internal_config.initial_lambda, default_initial_lambda());
        assert_eq!(
            internal_config.num_centers_to_merge_to,
            default_num_centers_to_merge_to()
        );
    }

    #[test]
    fn test_spann_configuration_to_internal_spann_configuration_with_none_values() {
        let spann_config = SpannConfiguration {
            search_nprobe: None,
            write_nprobe: None,
            space: None,
            ef_construction: None,
            ef_search: None,
            max_neighbors: None,
            reassign_neighbor_count: None,
            split_threshold: None,
            merge_threshold: None,
        };

        let internal_config: InternalSpannConfiguration = spann_config.into();

        assert_eq!(internal_config.search_nprobe, default_search_nprobe());
        assert_eq!(internal_config.write_nprobe, default_write_nprobe());
        assert_eq!(internal_config.space, default_space_spann());
        assert_eq!(
            internal_config.ef_construction,
            default_construction_ef_spann()
        );
        assert_eq!(internal_config.ef_search, default_search_ef_spann());
        assert_eq!(internal_config.max_neighbors, default_m_spann());
        assert_eq!(
            internal_config.reassign_neighbor_count,
            default_reassign_neighbor_count()
        );
        assert_eq!(internal_config.split_threshold, default_split_threshold());
        assert_eq!(internal_config.merge_threshold, default_merge_threshold());
        assert_eq!(
            internal_config.search_rng_factor,
            default_search_rng_factor()
        );
        assert_eq!(
            internal_config.search_rng_epsilon,
            default_search_rng_epsilon()
        );
        assert_eq!(internal_config.nreplica_count, default_nreplica_count());
        assert_eq!(internal_config.write_rng_factor, default_write_rng_factor());
        assert_eq!(
            internal_config.write_rng_epsilon,
            default_write_rng_epsilon()
        );
        assert_eq!(
            internal_config.num_samples_kmeans,
            default_num_samples_kmeans()
        );
        assert_eq!(internal_config.initial_lambda, default_initial_lambda());
        assert_eq!(
            internal_config.num_centers_to_merge_to,
            default_num_centers_to_merge_to()
        );
    }

    #[test]
    fn test_spann_configuration_to_internal_spann_configuration_mixed_values() {
        let spann_config = SpannConfiguration {
            search_nprobe: Some(80),
            write_nprobe: None,
            space: Some(Space::Ip),
            ef_construction: None,
            ef_search: Some(160),
            max_neighbors: Some(48),
            reassign_neighbor_count: None,
            split_threshold: Some(100),
            merge_threshold: None,
        };

        let internal_config: InternalSpannConfiguration = spann_config.into();

        assert_eq!(internal_config.search_nprobe, 80);
        assert_eq!(internal_config.write_nprobe, default_write_nprobe());
        assert_eq!(internal_config.space, Space::Ip);
        assert_eq!(
            internal_config.ef_construction,
            default_construction_ef_spann()
        );
        assert_eq!(internal_config.ef_search, 160);
        assert_eq!(internal_config.max_neighbors, 48);
        assert_eq!(
            internal_config.reassign_neighbor_count,
            default_reassign_neighbor_count()
        );
        assert_eq!(internal_config.split_threshold, 100);
        assert_eq!(internal_config.merge_threshold, default_merge_threshold());
        assert_eq!(
            internal_config.search_rng_factor,
            default_search_rng_factor()
        );
        assert_eq!(
            internal_config.search_rng_epsilon,
            default_search_rng_epsilon()
        );
        assert_eq!(internal_config.nreplica_count, default_nreplica_count());
        assert_eq!(internal_config.write_rng_factor, default_write_rng_factor());
        assert_eq!(
            internal_config.write_rng_epsilon,
            default_write_rng_epsilon()
        );
        assert_eq!(
            internal_config.num_samples_kmeans,
            default_num_samples_kmeans()
        );
        assert_eq!(internal_config.initial_lambda, default_initial_lambda());
        assert_eq!(
            internal_config.num_centers_to_merge_to,
            default_num_centers_to_merge_to()
        );
    }

    #[test]
    fn test_internal_spann_configuration_default() {
        let internal_config = InternalSpannConfiguration::default();

        assert_eq!(internal_config.search_nprobe, default_search_nprobe());
        assert_eq!(internal_config.write_nprobe, default_write_nprobe());
        assert_eq!(internal_config.space, default_space_spann());
        assert_eq!(
            internal_config.ef_construction,
            default_construction_ef_spann()
        );
        assert_eq!(internal_config.ef_search, default_search_ef_spann());
        assert_eq!(internal_config.max_neighbors, default_m_spann());
        assert_eq!(
            internal_config.reassign_neighbor_count,
            default_reassign_neighbor_count()
        );
        assert_eq!(internal_config.split_threshold, default_split_threshold());
        assert_eq!(internal_config.merge_threshold, default_merge_threshold());
        assert_eq!(
            internal_config.search_rng_factor,
            default_search_rng_factor()
        );
        assert_eq!(
            internal_config.search_rng_epsilon,
            default_search_rng_epsilon()
        );
        assert_eq!(internal_config.nreplica_count, default_nreplica_count());
        assert_eq!(internal_config.write_rng_factor, default_write_rng_factor());
        assert_eq!(
            internal_config.write_rng_epsilon,
            default_write_rng_epsilon()
        );
        assert_eq!(
            internal_config.num_samples_kmeans,
            default_num_samples_kmeans()
        );
        assert_eq!(internal_config.initial_lambda, default_initial_lambda());
        assert_eq!(
            internal_config.num_centers_to_merge_to,
            default_num_centers_to_merge_to()
        );
    }

    #[test]
    fn test_spann_configuration_default() {
        let spann_config = SpannConfiguration::default();
        let internal_config: InternalSpannConfiguration = spann_config.into();

        assert_eq!(internal_config.search_nprobe, default_search_nprobe());
        assert_eq!(internal_config.write_nprobe, default_write_nprobe());
        assert_eq!(internal_config.space, default_space_spann());
        assert_eq!(
            internal_config.ef_construction,
            default_construction_ef_spann()
        );
        assert_eq!(internal_config.ef_search, default_search_ef_spann());
        assert_eq!(internal_config.max_neighbors, default_m_spann());
        assert_eq!(
            internal_config.reassign_neighbor_count,
            default_reassign_neighbor_count()
        );
        assert_eq!(internal_config.split_threshold, default_split_threshold());
        assert_eq!(internal_config.merge_threshold, default_merge_threshold());
        assert_eq!(
            internal_config.search_rng_factor,
            default_search_rng_factor()
        );
        assert_eq!(
            internal_config.search_rng_epsilon,
            default_search_rng_epsilon()
        );
        assert_eq!(internal_config.nreplica_count, default_nreplica_count());
        assert_eq!(internal_config.write_rng_factor, default_write_rng_factor());
        assert_eq!(
            internal_config.write_rng_epsilon,
            default_write_rng_epsilon()
        );
        assert_eq!(
            internal_config.num_samples_kmeans,
            default_num_samples_kmeans()
        );
        assert_eq!(internal_config.initial_lambda, default_initial_lambda());
        assert_eq!(
            internal_config.num_centers_to_merge_to,
            default_num_centers_to_merge_to()
        );
    }

    #[test]
    fn test_deserialize_json_without_nreplica_count() {
        let json_without_nreplica = r#"{
            "search_nprobe": 120,
            "search_rng_factor": 2.5,
            "search_rng_epsilon": 15.0,
            "write_nprobe": 60,
            "write_rng_factor": 1.5,
            "write_rng_epsilon": 8.0,
            "split_threshold": 80,
            "num_samples_kmeans": 1500,
            "initial_lambda": 150.0,
            "reassign_neighbor_count": 32,
            "merge_threshold": 30,
            "num_centers_to_merge_to": 6,
            "space": "l2",
            "ef_construction": 180,
            "ef_search": 200,
            "max_neighbors": 56
        }"#;

        let internal_config: InternalSpannConfiguration =
            serde_json::from_str(json_without_nreplica).unwrap();

        assert_eq!(internal_config.search_nprobe, 120);
        assert_eq!(internal_config.search_rng_factor, 2.5);
        assert_eq!(internal_config.search_rng_epsilon, 15.0);
        assert_eq!(internal_config.write_nprobe, 60);
        assert_eq!(internal_config.write_rng_factor, 1.5);
        assert_eq!(internal_config.write_rng_epsilon, 8.0);
        assert_eq!(internal_config.split_threshold, 80);
        assert_eq!(internal_config.num_samples_kmeans, 1500);
        assert_eq!(internal_config.initial_lambda, 150.0);
        assert_eq!(internal_config.reassign_neighbor_count, 32);
        assert_eq!(internal_config.merge_threshold, 30);
        assert_eq!(internal_config.num_centers_to_merge_to, 6);
        assert_eq!(internal_config.space, Space::L2);
        assert_eq!(internal_config.ef_construction, 180);
        assert_eq!(internal_config.ef_search, 200);
        assert_eq!(internal_config.max_neighbors, 56);
        assert_eq!(internal_config.nreplica_count, default_nreplica_count());
    }
}
