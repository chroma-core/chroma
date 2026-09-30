use super::{IndexConfig, IndexUuid};
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use std::{io::Read, mem::size_of, path::Path};
use thiserror::Error;
use tracing::instrument;

// Setting it to a small value to prevent
// bloating of the index which directly impacts query latency by increasing
// the amount of bytes that need to be fetched from s3. The trade off is that
// there will be more data movement/allocations during compaction which is
// acceptable since compaction is a background operation.
pub const DEFAULT_MAX_ELEMENTS: usize = 100;

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

fn read_i32(buf: &[u8], offset: &mut usize) -> Option<i32> {
    let end = offset.checked_add(size_of::<i32>())?;
    let bytes = buf.get(*offset..end)?;
    let mut array = [0; size_of::<i32>()];
    array.copy_from_slice(bytes);
    *offset = end;
    Some(i32::from_ne_bytes(array))
}

fn read_usize(buf: &[u8], offset: &mut usize) -> Option<usize> {
    let end = offset.checked_add(size_of::<usize>())?;
    let bytes = buf.get(*offset..end)?;
    let mut array = [0; size_of::<usize>()];
    array.copy_from_slice(bytes);
    *offset = end;
    Some(usize::from_ne_bytes(array))
}

pub fn parse_persisted_hnsw_dim(header: &[u8]) -> Option<usize> {
    let mut offset = 0;
    let version = read_i32(header, &mut offset)?;
    if version != 1 {
        return None;
    }

    // hnswlib persists native POD fields in order. The vector byte width is
    // not stored directly, but is exactly the gap between the vector payload
    // offset and the label offset.
    let _offset_level0 = read_usize(header, &mut offset)?;
    let _max_elements = read_usize(header, &mut offset)?;
    let _cur_element_count = read_usize(header, &mut offset)?;
    let size_data_per_element = read_usize(header, &mut offset)?;
    let label_offset = read_usize(header, &mut offset)?;
    let offset_data = read_usize(header, &mut offset)?;

    let data_size = label_offset.checked_sub(offset_data)?;
    if data_size == 0 || data_size % size_of::<f32>() != 0 {
        return None;
    }
    if label_offset.checked_add(size_of::<usize>())? > size_data_per_element {
        return None;
    }
    Some(data_size / size_of::<f32>())
}

pub struct HnswIndex {
    index: hnswlib::HnswIndex,
    pub id: IndexUuid,
    pub distance_function: DistanceFunction,
}

#[derive(Error, Debug)]
#[error("Embedding dimensionality {actual} does not match index dimensionality {expected}")]
struct HnswDimensionMismatch {
    expected: usize,
    actual: usize,
}

impl ChromaError for HnswDimensionMismatch {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
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
    #[error("Invalid persisted HNSW header")]
    InvalidHeader,
    #[error("Could not read persisted HNSW header: {0}")]
    HeaderIo(#[source] std::io::Error),
    #[error("No config provided")]
    NoConfigProvided,
    #[error(transparent)]
    Other(#[from] hnswlib::HnswInitError),
}

impl ChromaError for WrappedHnswInitError {
    fn code(&self) -> ErrorCodes {
        match self {
            WrappedHnswInitError::InvalidHeader | WrappedHnswInitError::HeaderIo(_) => {
                ErrorCodes::DataLoss
            }
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

    pub fn init(
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
                Ok(HnswIndex {
                    index,
                    id,
                    distance_function: index_config.distance_function.clone(),
                })
            }
        }
    }

    fn validate_vector(&self, vector: &[f32]) -> Result<(), Box<dyn ChromaError>> {
        let expected = self.dimensionality() as usize;
        if vector.len() != expected {
            return Err(HnswDimensionMismatch {
                expected,
                actual: vector.len(),
            }
            .boxed());
        }
        Ok(())
    }

    pub fn add(&self, id: usize, vector: &[f32]) -> Result<(), Box<dyn ChromaError>> {
        self.validate_vector(vector)?;
        self.index
            .add(id, vector)
            .map_err(|e| WrappedHnswError(e).boxed())
    }

    pub fn delete(&self, id: usize) -> Result<(), Box<dyn ChromaError>> {
        self.index
            .delete(id)
            .map_err(|e| WrappedHnswError(e).boxed())
    }

    pub fn query(
        &self,
        vector: &[f32],
        k: usize,
        allowed_ids: &[usize],
        disallowed_ids: &[usize],
    ) -> Result<(Vec<usize>, Vec<f32>), Box<dyn ChromaError>> {
        self.validate_vector(vector)?;
        self.index
            .query(vector, k, allowed_ids, disallowed_ids)
            .map_err(|e| WrappedHnswError(e).boxed())
    }

    pub fn get(&self, id: usize) -> Result<Option<Vec<f32>>, Box<dyn ChromaError>> {
        self.index.get(id).map_err(|e| WrappedHnswError(e).boxed())
    }

    pub fn get_all_ids_sizes(&self) -> Result<Vec<usize>, Box<dyn ChromaError>> {
        self.index
            .get_all_ids_sizes()
            .map_err(|e| WrappedHnswError(e).boxed())
    }

    pub fn get_all_ids(&self) -> Result<(Vec<usize>, Vec<usize>), Box<dyn ChromaError>> {
        self.index
            .get_all_ids()
            .map_err(|e| WrappedHnswError(e).boxed())
    }

    pub fn save(&self) -> Result<(), Box<dyn ChromaError>> {
        self.index.save().map_err(|e| WrappedHnswError(e).boxed())
    }

    fn validate_header_dimension(header: &[u8], expected: i32) -> Result<(), Box<dyn ChromaError>> {
        let actual = parse_persisted_hnsw_dim(header)
            .ok_or_else(|| WrappedHnswInitError::InvalidHeader.boxed())?;
        if actual != expected as usize {
            return Err(HnswDimensionMismatch {
                expected: expected as usize,
                actual,
            }
            .boxed());
        }
        Ok(())
    }

    #[instrument(name = "HnswIndex load", level = "info")]
    pub fn load(
        path: &str,
        index_config: &IndexConfig,
        ef_search: usize,
        id: IndexUuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let mut header = [0; size_of::<i32>() + 6 * size_of::<usize>()];
        std::fs::File::open(Path::new(path).join("header.bin"))
            .and_then(|mut file| file.read_exact(&mut header))
            .map_err(|err| WrappedHnswInitError::HeaderIo(err).boxed())?;
        Self::validate_header_dimension(&header, index_config.dimensionality)?;
        let index = hnswlib::HnswIndex::load(hnswlib::HnswIndexLoadConfig {
            distance_function: map_distance_function(index_config.distance_function.clone()),
            dimensionality: index_config.dimensionality,
            persist_path: path.into(),
            ef_search,
        })
        .map_err(|e| WrappedHnswInitError::Other(e).boxed())?;

        Ok(HnswIndex {
            index,
            id,
            distance_function: index_config.distance_function.clone(),
        })
    }

    #[instrument(skip(hnsw_data))]
    pub fn load_from_hnsw_data(
        hnsw_data: &hnswlib::HnswData,
        index_config: &IndexConfig,
        ef_search: usize,
        id: IndexUuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        Self::validate_header_dimension(hnsw_data.header_buffer(), index_config.dimensionality)?;
        let index = hnswlib::HnswIndex::load_from_hnsw_data(
            hnswlib::HnswIndexMemoryLoadConfig {
                distance_function: map_distance_function(index_config.distance_function.clone()),
                dimensionality: index_config.dimensionality,
                ef_search,
            },
            hnsw_data,
        )
        .map_err(|e| WrappedHnswInitError::Other(e).boxed())?;

        Ok(HnswIndex {
            index,
            id,
            distance_function: index_config.distance_function.clone(),
        })
    }

    pub fn serialize_to_hnsw_data(&self) -> Result<hnswlib::HnswData, WrappedHnswError> {
        self.index
            .serialize_index_to_hnsw_data()
            .map_err(WrappedHnswError)
    }
}

fn map_distance_function(distance_function: DistanceFunction) -> hnswlib::HnswDistanceFunction {
    match distance_function {
        DistanceFunction::Cosine => hnswlib::HnswDistanceFunction::Cosine,
        DistanceFunction::Euclidean => hnswlib::HnswDistanceFunction::Euclidean,
        DistanceFunction::InnerProduct => hnswlib::HnswDistanceFunction::InnerProduct,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn native_boundary_rejects_wrong_dimensions(dim in 1..65usize, actual in 0..130usize) {
            prop_assume!(dim != actual);
            let index = HnswIndex::init(
                &IndexConfig::new(dim as i32, DistanceFunction::Euclidean),
                Some(&HnswIndexConfig::new_ephemeral(16, 100, 100)),
                IndexUuid(uuid::Uuid::new_v4()),
            ).unwrap();
            let vector = vec![1.0; dim];
            index.add(1, &vector).unwrap();
            let before = index.get(1).unwrap();
            let serialized = index.serialize_to_hnsw_data().unwrap();
            prop_assert!(HnswIndex::load_from_hnsw_data(&serialized,
                &IndexConfig::new(actual as i32, DistanceFunction::Euclidean), 100,
                IndexUuid(uuid::Uuid::new_v4())).is_err());
            let invalid = vec![2.0; actual];
            prop_assert_eq!(index.add(1, &invalid).unwrap_err().code(), ErrorCodes::InvalidArgument);
            prop_assert_eq!(index.query(&invalid, 1, &[], &[]).unwrap_err().code(), ErrorCodes::InvalidArgument);
            prop_assert_eq!((index.len(), index.get(1).unwrap()), (1, before));
            prop_assert_eq!(index.query(&vector, 1, &[], &[]).unwrap(), (vec![1], vec![0.0]));
        }
    }
}
