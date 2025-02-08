use crate::{Metadata, Segment};
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
}

impl ChromaError for HnswParametersFromSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            HnswParametersFromSegmentError::InvalidMetadata(_) => ErrorCodes::InvalidArgument,
            HnswParametersFromSegmentError::InvalidParameters(_) => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub enum HnswSpace {
    #[default]
    #[serde(rename = "l2")]
    L2,
    #[serde(rename = "cosine")]
    Cosine,
    #[serde(rename = "ip")]
    Ip,
}

fn default_construction_ef() -> usize {
    100
}

fn default_search_ef() -> usize {
    100
}

fn default_search_ef_distributed() -> usize {
    10
}

fn default_m() -> usize {
    16
}

fn default_num_threads() -> usize {
    std::thread::available_parallelism()
        .unwrap_or(NonZero::new(1).unwrap())
        .get()
}

fn default_resize_factor() -> f64 {
    1.2
}

fn default_sync_threshold() -> usize {
    1000
}

fn default_sync_threshold_distributed() -> usize {
    64
}

#[derive(Debug, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct DistributedHnswParameters {
    #[serde(rename = "hnsw:space", default)]
    pub space: HnswSpace,
    #[serde(rename = "hnsw:construction_ef", default = "default_construction_ef")]
    pub construction_ef: usize,
    #[serde(rename = "hnsw:search_ef", default = "default_search_ef_distributed")]
    pub search_ef: usize,
    #[serde(rename = "hnsw:M", default = "default_m")]
    pub m: usize,
    #[serde(rename = "hnsw:num_threads", default = "default_num_threads")]
    pub num_threads: usize,
    #[serde(rename = "hnsw:resize_factor", default = "default_resize_factor")]
    pub resize_factor: f64,
    #[validate(range(min = 2))]
    #[serde(
        rename = "hnsw:sync_threshold",
        default = "default_sync_threshold_distributed"
    )]
    pub sync_threshold: usize,
}

impl Default for DistributedHnswParameters {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl TryFrom<&Segment> for DistributedHnswParameters {
    type Error = HnswParametersFromSegmentError;

    fn try_from(value: &Segment) -> Result<Self, Self::Error> {
        DistributedHnswParameters::try_from(&value.metadata)
    }
}

impl TryFrom<&Option<Metadata>> for DistributedHnswParameters {
    type Error = HnswParametersFromSegmentError;

    fn try_from(metadata: &Option<Metadata>) -> Result<Self, Self::Error> {
        if let Some(metadata) = metadata {
            let filtered_metadata = metadata
                .clone()
                .into_iter()
                .filter(|(k, _)| k.starts_with("hnsw:"))
                .collect::<Metadata>();

            let metadata_str = serde_json::to_string(&filtered_metadata)?;
            let parsed = serde_json::from_str::<DistributedHnswParameters>(&metadata_str)?;
            parsed.validate()?;
            Ok(parsed)
        } else {
            Ok(DistributedHnswParameters::default())
        }
    }
}

impl TryFrom<DistributedHnswParameters> for Metadata {
    type Error = serde_json::Error;

    fn try_from(params: DistributedHnswParameters) -> Result<Self, Self::Error> {
        let json_str = serde_json::to_string(&params)?;
        let parsed = serde_json::from_str::<Metadata>(&json_str)?;
        Ok(parsed)
    }
}

fn default_batch_size() -> usize {
    100
}

#[derive(Debug, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct SingleNodeHnswParameters {
    #[serde(rename = "hnsw:space", default)]
    pub space: HnswSpace,
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
    #[validate(range(min = 2))]
    #[serde(rename = "hnsw:sync_threshold", default = "default_sync_threshold")]
    pub sync_threshold: usize,
    #[validate(range(min = 2))]
    #[serde(rename = "hnsw:batch_size", default = "default_batch_size")]
    pub batch_size: usize,
}

impl Default for SingleNodeHnswParameters {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl TryFrom<&Segment> for SingleNodeHnswParameters {
    type Error = HnswParametersFromSegmentError;

    fn try_from(value: &Segment) -> Result<Self, Self::Error> {
        SingleNodeHnswParameters::try_from(&value.metadata)
    }
}

impl TryFrom<&Option<Metadata>> for SingleNodeHnswParameters {
    type Error = HnswParametersFromSegmentError;

    fn try_from(metadata: &Option<Metadata>) -> Result<Self, Self::Error> {
        if let Some(metadata) = metadata {
            let filtered_metadata = metadata
                .clone()
                .into_iter()
                .filter(|(k, _)| k.starts_with("hnsw:"))
                .collect::<Metadata>();

            let metadata_str = serde_json::to_string(&filtered_metadata)?;
            let parsed = serde_json::from_str::<SingleNodeHnswParameters>(&metadata_str)?;
            parsed.validate()?;
            Ok(parsed)
        } else {
            Ok(SingleNodeHnswParameters::default())
        }
    }
}

impl TryFrom<SingleNodeHnswParameters> for Metadata {
    type Error = serde_json::Error;

    fn try_from(params: SingleNodeHnswParameters) -> Result<Self, Self::Error> {
        let json_str = serde_json::to_string(&params)?;
        let parsed = serde_json::from_str::<Metadata>(&json_str)?;
        Ok(parsed)
    }
}
