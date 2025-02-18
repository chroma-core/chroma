use super::{Index, IndexConfig, IndexUuid, PersistentIndex};
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use std::path::Path;
use thiserror::Error;
use tracing::instrument;

pub const DEFAULT_MAX_ELEMENTS: usize = 10000;

// TODO: Make this config:
// - Watchable - for dynamic updates
// - Have a notion of static vs dynamic config
// - Have a notion of default config
// - TODO: HNSWIndex should store a ref to the config so it can look up the config values.
//   deferring this for a config pass
#[derive(Clone, Debug)]
pub struct HnswIndexConfig {
    pub max_elements: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub random_seed: usize,
    pub persist_path: Option<String>,
}

#[derive(Error, Debug)]
pub enum HnswIndexConfigError {
    #[error("Missing config `{0}`")]
    MissingConfig(String),
}

impl ChromaError for HnswIndexConfigError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

impl HnswIndexConfig {
    pub fn new_ephemeral(m: usize, ef_construction: usize, ef_search: usize) -> Self {
        Self {
            max_elements: DEFAULT_MAX_ELEMENTS,
            m,
            ef_construction,
            ef_search,
            random_seed: 0,
            persist_path: None,
        }
    }

    pub fn new_persistent(
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        persist_path: &Path,
    ) -> Result<Self, Box<HnswIndexConfigError>> {
        let persist_path = match persist_path.to_str() {
            Some(persist_path) => persist_path,
            None => {
                return Err(Box::new(HnswIndexConfigError::MissingConfig(
                    "persist_path".to_string(),
                )))
            }
        };
        Ok(HnswIndexConfig {
            max_elements: DEFAULT_MAX_ELEMENTS,
            m,
            ef_construction,
            ef_search,
            random_seed: 0,
            persist_path: Some(persist_path.to_string()),
        })
    }
}

pub struct HnswIndex {
    index: hnswlib::HnswIndex,
    pub id: IndexUuid,
}

#[derive(Error, Debug)]
#[error(transparent)]
pub struct WrappedHnswError(#[from] hnswlib::HnswError);

impl ChromaError for WrappedHnswError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Error, Debug)]
pub enum WrappedHnswInitError {
    #[error("No config provided")]
    NoConfigProvided,
    #[error(transparent)]
    Other(#[from] hnswlib::HnswInitError),
}

impl ChromaError for WrappedHnswInitError {
    fn code(&self) -> ErrorCodes {
        match self {
            WrappedHnswInitError::NoConfigProvided => ErrorCodes::InvalidArgument,
            WrappedHnswInitError::Other(_) => ErrorCodes::Internal,
        }
    }
}

impl HnswIndex {
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn len_with_deleted(&self) -> usize {
        self.index.len_with_deleted()
    }

    pub fn dimensionality(&self) -> i32 {
        self.index.dimensionality()
    }

    pub fn capacity(&self) -> usize {
        self.index.capacity()
    }

    pub fn resize(&mut self, new_size: usize) -> Result<(), Box<dyn ChromaError>> {
        self.index
            .resize(new_size)
            .map_err(|e| WrappedHnswError(e).boxed())
    }

    pub fn open_fd(&self) {
        self.index.open_fd();
    }

    pub fn close_fd(&self) {
        self.index.close_fd();
    }
}

impl Index<HnswIndexConfig> for HnswIndex {
    fn init(
        index_config: &IndexConfig,
        hnsw_config: Option<&HnswIndexConfig>,
        id: IndexUuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match hnsw_config {
            None => Err(WrappedHnswInitError::NoConfigProvided.boxed()),
            Some(config) => {
                let index = hnswlib::HnswIndex::init(hnswlib::HnswIndexInitConfig {
                    distance_function: map_distance_function(
                        index_config.distance_function.clone(),
                    ),
                    dimensionality: index_config.dimensionality,
                    max_elements: config.max_elements,
                    m: config.m,
                    ef_construction: config.ef_construction,
                    ef_search: config.ef_search,
                    random_seed: config.random_seed,
                    persist_path: config.persist_path.as_ref().map(|s| s.as_str().into()),
                })
                .map_err(|e| WrappedHnswInitError::Other(e).boxed())?;
                Ok(HnswIndex { index, id })
            }
        }
    }

    fn add(&self, id: usize, vector: &[f32]) -> Result<(), Box<dyn ChromaError>> {
        self.index
            .add(id, vector)
            .map_err(|e| WrappedHnswError(e).boxed())
    }

    fn delete(&self, id: usize) -> Result<(), Box<dyn ChromaError>> {
        self.index
            .delete(id)
            .map_err(|e| WrappedHnswError(e).boxed())
    }

    fn query(
        &self,
        vector: &[f32],
        k: usize,
        allowed_ids: &[usize],
        disallowed_ids: &[usize],
    ) -> Result<(Vec<usize>, Vec<f32>), Box<dyn ChromaError>> {
        self.index
            .query(vector, k, allowed_ids, disallowed_ids)
            .map_err(|e| WrappedHnswError(e).boxed())
    }

    fn get(&self, id: usize) -> Result<Option<Vec<f32>>, Box<dyn ChromaError>> {
        self.index.get(id).map_err(|e| WrappedHnswError(e).boxed())
    }

    fn get_all_ids_sizes(&self) -> Result<Vec<usize>, Box<dyn ChromaError>> {
        self.index
            .get_all_ids_sizes()
            .map_err(|e| WrappedHnswError(e).boxed())
    }

    fn get_all_ids(&self) -> Result<(Vec<usize>, Vec<usize>), Box<dyn ChromaError>> {
        self.index
            .get_all_ids()
            .map_err(|e| WrappedHnswError(e).boxed())
    }
}

impl PersistentIndex<HnswIndexConfig> for HnswIndex {
    fn save(&self) -> Result<(), Box<dyn ChromaError>> {
        self.index.save().map_err(|e| WrappedHnswError(e).boxed())
    }

    #[instrument(name = "HnswIndex load", level = "info")]
    fn load(
        path: &str,
        index_config: &IndexConfig,
        id: IndexUuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let index = hnswlib::HnswIndex::load(hnswlib::HnswIndexLoadConfig {
            distance_function: map_distance_function(index_config.distance_function.clone()),
            dimensionality: index_config.dimensionality,
            persist_path: path.into(),
        })
        .map_err(|e| WrappedHnswInitError::Other(e).boxed())?;

        Ok(HnswIndex { index, id })
    }
}

fn map_distance_function(distance_function: DistanceFunction) -> hnswlib::HnswDistanceFunction {
    match distance_function {
        DistanceFunction::Cosine => hnswlib::HnswDistanceFunction::Cosine,
        DistanceFunction::Euclidean => hnswlib::HnswDistanceFunction::Euclidean,
        DistanceFunction::InnerProduct => hnswlib::HnswDistanceFunction::InnerProduct,
    }
}
