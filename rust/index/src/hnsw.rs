use super::{IndexConfig, IndexUuid};
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use std::{
    io::{Cursor, Read, Seek, SeekFrom},
    mem::size_of,
    path::Path,
};
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

fn read_u32(buf: &[u8], offset: &mut usize) -> Option<u32> {
    let end = offset.checked_add(size_of::<u32>())?;
    let bytes = buf.get(*offset..end)?;
    let mut array = [0; size_of::<u32>()];
    array.copy_from_slice(bytes);
    *offset = end;
    Some(u32::from_ne_bytes(array))
}

fn read_f64(buf: &[u8], offset: &mut usize) -> Option<f64> {
    let end = offset.checked_add(size_of::<f64>())?;
    let bytes = buf.get(*offset..end)?;
    let mut array = [0; size_of::<f64>()];
    array.copy_from_slice(bytes);
    *offset = end;
    Some(f64::from_ne_bytes(array))
}

struct PersistedHnswHeader {
    dimensionality: usize,
    offset_level0: usize,
    max_elements: usize,
    current_element_count: usize,
    size_data_per_element: usize,
    label_offset: usize,
    offset_data: usize,
    max_level: i32,
    entrypoint_node: u32,
    max_m: usize,
    max_m0: usize,
}

fn parse_persisted_hnsw_header(header: &[u8]) -> Option<PersistedHnswHeader> {
    let mut offset = 0;
    let version = read_i32(header, &mut offset)?;
    if version != 1 {
        return None;
    }

    let offset_level0 = read_usize(header, &mut offset)?;
    let max_elements = read_usize(header, &mut offset)?;
    let current_element_count = read_usize(header, &mut offset)?;
    let size_data_per_element = read_usize(header, &mut offset)?;
    let label_offset = read_usize(header, &mut offset)?;
    let offset_data = read_usize(header, &mut offset)?;
    let max_level = read_i32(header, &mut offset)?;
    let entrypoint_node = read_u32(header, &mut offset)?;
    let max_m = read_usize(header, &mut offset)?;
    let max_m0 = read_usize(header, &mut offset)?;
    let _m = read_usize(header, &mut offset)?;
    let _mult = read_f64(header, &mut offset)?;
    let _ef_construction = read_usize(header, &mut offset)?;

    let data_size = label_offset.checked_sub(offset_data)?;
    if data_size == 0 || data_size % size_of::<f32>() != 0 {
        return None;
    }
    if label_offset.checked_add(size_of::<usize>())? > size_data_per_element {
        return None;
    }
    Some(PersistedHnswHeader {
        dimensionality: data_size / size_of::<f32>(),
        offset_level0,
        max_elements,
        current_element_count,
        size_data_per_element,
        label_offset,
        offset_data,
        max_level,
        entrypoint_node,
        max_m,
        max_m0,
    })
}

pub fn parse_persisted_hnsw_dim(header: &[u8]) -> Option<usize> {
    let mut offset = 0;
    if read_i32(header, &mut offset)? != 1 {
        return None;
    }
    let _offset_level0 = read_usize(header, &mut offset)?;
    let _max_elements = read_usize(header, &mut offset)?;
    let _current_element_count = read_usize(header, &mut offset)?;
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

fn invalid_persisted_hnsw_data(reason: &str) -> Box<dyn ChromaError> {
    WrappedHnswInitError::InvalidPersistedData(reason.to_string()).boxed()
}

fn read_neighbor_id<R: Read + Seek>(reader: &mut R) -> Result<u32, Box<dyn ChromaError>> {
    let mut bytes = [0; size_of::<u32>()];
    reader
        .read_exact(&mut bytes)
        .map_err(|_| invalid_persisted_hnsw_data("truncated neighbor list"))?;
    Ok(u32::from_ne_bytes(bytes))
}

fn validate_neighbor_list<R: Read + Seek>(
    reader: &mut R,
    count_word: u32,
    max_neighbors: usize,
    current_element_count: usize,
    current_node: usize,
) -> Result<(), Box<dyn ChromaError>> {
    let neighbor_count = (count_word & 0xffff) as usize;
    if neighbor_count > max_neighbors {
        return Err(invalid_persisted_hnsw_data(
            "neighbor count exceeds header limit",
        ));
    }
    for _ in 0..neighbor_count {
        let neighbor = read_neighbor_id(reader)? as usize;
        if neighbor >= current_element_count || neighbor == current_node {
            return Err(invalid_persisted_hnsw_data(
                "neighbor index is out of range",
            ));
        }
    }
    Ok(())
}

fn validate_persisted_hnsw_data<L0: Read + Seek, LL: Read + Seek>(
    header_bytes: &[u8],
    data_level0: &mut L0,
    data_level0_len: u64,
    length_len: u64,
    link_lists: &mut LL,
    link_lists_len: u64,
    expected_dimension: i32,
) -> Result<(), Box<dyn ChromaError>> {
    let header = parse_persisted_hnsw_header(header_bytes)
        .ok_or_else(|| invalid_persisted_hnsw_data("invalid header"))?;
    if header.dimensionality != expected_dimension as usize {
        return Err(HnswDimensionMismatch {
            expected: expected_dimension as usize,
            actual: header.dimensionality,
        }
        .boxed());
    }
    if header.current_element_count > header.max_elements
        || header.current_element_count > u32::MAX as usize
        || header.max_m == 0
        || header.max_m0 == 0
        || header.max_m > u16::MAX as usize
        || header.max_m0 > u16::MAX as usize
        || (header.current_element_count > 0
            && header.entrypoint_node as usize >= header.current_element_count)
        || header.max_level < -1
    {
        return Err(invalid_persisted_hnsw_data("inconsistent header values"));
    }

    let data_size = header
        .max_elements
        .checked_mul(header.size_data_per_element)
        .ok_or_else(|| invalid_persisted_hnsw_data("data size overflows"))?;
    let length_size = header
        .max_elements
        .checked_mul(size_of::<f32>())
        .ok_or_else(|| invalid_persisted_hnsw_data("length size overflows"))?;
    let links_per_level = header
        .max_m
        .checked_mul(size_of::<u32>())
        .and_then(|size| size.checked_add(size_of::<u32>()))
        .ok_or_else(|| invalid_persisted_hnsw_data("link size overflows"))?;
    let max_neighbors0_size = header
        .max_m0
        .checked_mul(size_of::<u32>())
        .and_then(|size| size.checked_add(size_of::<u32>()))
        .ok_or_else(|| invalid_persisted_hnsw_data("level-zero link size overflows"))?;
    if header.size_data_per_element == 0
        || header.offset_level0 > header.size_data_per_element
        || header
            .offset_level0
            .checked_add(max_neighbors0_size)
            .is_none_or(|end| end > header.size_data_per_element)
        || header
            .offset_data
            .checked_add(header.dimensionality * size_of::<f32>())
            .is_none_or(|end| end > header.label_offset)
        || header
            .label_offset
            .checked_add(size_of::<usize>())
            .is_none_or(|end| end > header.size_data_per_element)
        || data_level0_len < data_size as u64
        || length_len < length_size as u64
    {
        return Err(invalid_persisted_hnsw_data(
            "persisted files do not match header sizes",
        ));
    }

    for node in 0..header.current_element_count {
        let record = node
            .checked_mul(header.size_data_per_element)
            .and_then(|base| base.checked_add(header.offset_level0))
            .ok_or_else(|| invalid_persisted_hnsw_data("data offset overflows"))?;
        data_level0
            .seek(SeekFrom::Start(record as u64))
            .map_err(|_| invalid_persisted_hnsw_data("cannot seek data file"))?;
        let count_word = read_neighbor_id(data_level0)?;
        validate_neighbor_list(
            data_level0,
            count_word,
            header.max_m0,
            header.current_element_count,
            node,
        )?;
    }

    let mut max_node_level = if header.current_element_count == 0 {
        -1
    } else {
        0
    };
    for node in 0..header.current_element_count {
        let list_size = read_neighbor_id(link_lists)? as usize;
        if list_size % links_per_level != 0 {
            return Err(invalid_persisted_hnsw_data(
                "invalid upper-level link list size",
            ));
        }
        let level_count = list_size / links_per_level;
        if level_count > i32::MAX as usize {
            return Err(invalid_persisted_hnsw_data("upper-level count overflows"));
        }
        max_node_level = max_node_level.max(level_count as i32);
        for _ in 0..level_count {
            let count_word = read_neighbor_id(link_lists)?;
            validate_neighbor_list(
                link_lists,
                count_word,
                header.max_m,
                header.current_element_count,
                node,
            )?;
        }
    }
    if link_lists
        .stream_position()
        .map_err(|_| invalid_persisted_hnsw_data("cannot read link list position"))?
        != link_lists_len
        || max_node_level != header.max_level
    {
        return Err(invalid_persisted_hnsw_data(
            "inconsistent upper-level link lists",
        ));
    }
    Ok(())
}

fn validate_persisted_hnsw_directory(
    path: &Path,
    expected_dimension: i32,
) -> Result<(), Box<dyn ChromaError>> {
    let mut header = Vec::new();
    std::fs::File::open(path.join("header.bin"))
        .and_then(|mut file| file.read_to_end(&mut header))
        .map_err(|err| WrappedHnswInitError::HeaderIo(err).boxed())?;
    let mut data_level0 = std::fs::File::open(path.join("data_level0.bin"))
        .map_err(|err| WrappedHnswInitError::PersistedFileIo(err).boxed())?;
    let data_level0_len = data_level0
        .metadata()
        .map_err(|err| WrappedHnswInitError::PersistedFileIo(err).boxed())?
        .len();
    let length_len = std::fs::metadata(path.join("length.bin"))
        .map_err(|err| WrappedHnswInitError::PersistedFileIo(err).boxed())?
        .len();
    let mut link_lists = std::fs::File::open(path.join("link_lists.bin"))
        .map_err(|err| WrappedHnswInitError::PersistedFileIo(err).boxed())?;
    let link_lists_len = link_lists
        .metadata()
        .map_err(|err| WrappedHnswInitError::PersistedFileIo(err).boxed())?
        .len();
    validate_persisted_hnsw_data(
        &header,
        &mut data_level0,
        data_level0_len,
        length_len,
        &mut link_lists,
        link_lists_len,
        expected_dimension,
    )
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
    #[error("Invalid persisted HNSW data: {0}")]
    InvalidPersistedData(String),
    #[error("Could not read persisted HNSW header: {0}")]
    HeaderIo(#[source] std::io::Error),
    #[error("Could not read persisted HNSW files: {0}")]
    PersistedFileIo(#[source] std::io::Error),
    #[error("No config provided")]
    NoConfigProvided,
    #[error(transparent)]
    Other(#[from] hnswlib::HnswInitError),
}

impl ChromaError for WrappedHnswInitError {
    fn code(&self) -> ErrorCodes {
        match self {
            WrappedHnswInitError::InvalidPersistedData(_) | WrappedHnswInitError::HeaderIo(_) => {
                ErrorCodes::DataLoss
            }
            WrappedHnswInitError::PersistedFileIo(_) => ErrorCodes::Internal,
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

    #[instrument(name = "HnswIndex load", level = "info")]
    pub fn load(
        path: &str,
        index_config: &IndexConfig,
        ef_search: usize,
        id: IndexUuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        validate_persisted_hnsw_directory(Path::new(path), index_config.dimensionality)?;
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
        let mut data_level0 = Cursor::new(hnsw_data.data_level0_buffer());
        let mut link_lists = Cursor::new(hnsw_data.link_list_buffer());
        validate_persisted_hnsw_data(
            hnsw_data.header_buffer(),
            &mut data_level0,
            hnsw_data.data_level0_buffer().len() as u64,
            hnsw_data.length_buffer().len() as u64,
            &mut link_lists,
            hnsw_data.link_list_buffer().len() as u64,
            index_config.dimensionality,
        )?;
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
    use std::sync::Arc;

    fn two_node_hnsw_data() -> (IndexConfig, hnswlib::HnswData) {
        let config = IndexConfig::new(2, DistanceFunction::Euclidean);
        let index = HnswIndex::init(
            &config,
            Some(&HnswIndexConfig::new_ephemeral(16, 100, 100)),
            IndexUuid(uuid::Uuid::new_v4()),
        )
        .unwrap();
        index.add(1, &[1.0, 0.0]).unwrap();
        index.add(2, &[0.0, 1.0]).unwrap();
        (config, index.serialize_to_hnsw_data().unwrap())
    }

    #[test]
    fn load_rejects_out_of_range_neighbor_ids() {
        let (config, data) = two_node_hnsw_data();
        HnswIndex::load_from_hnsw_data(&data, &config, 100, IndexUuid(uuid::Uuid::new_v4()))
            .unwrap();

        let mut data_level0 = data.data_level0_buffer().to_vec();
        let neighbor_count = u32::from_ne_bytes(data_level0[..4].try_into().unwrap());
        assert!(neighbor_count > 0);
        data_level0[4..8].copy_from_slice(&u32::MAX.to_ne_bytes());

        let corrupted = hnswlib::HnswData::builder()
            .header_buffer(Arc::new(data.header_buffer().to_vec()))
            .data_level0_buffer(Arc::new(data_level0))
            .length_buffer(Arc::new(data.length_buffer().to_vec()))
            .link_list_buffer(Arc::new(data.link_list_buffer().to_vec()))
            .build()
            .unwrap();

        assert!(HnswIndex::load_from_hnsw_data(
            &corrupted,
            &config,
            100,
            IndexUuid(uuid::Uuid::new_v4()),
        )
        .is_err());
    }

    #[test]
    fn load_rejects_truncated_link_lists() {
        let (config, data) = two_node_hnsw_data();
        let corrupted = hnswlib::HnswData::builder()
            .header_buffer(Arc::new(data.header_buffer().to_vec()))
            .data_level0_buffer(Arc::new(data.data_level0_buffer().to_vec()))
            .length_buffer(Arc::new(data.length_buffer().to_vec()))
            .link_list_buffer(Arc::new(Vec::new()))
            .build()
            .unwrap();

        assert!(HnswIndex::load_from_hnsw_data(
            &corrupted,
            &config,
            100,
            IndexUuid(uuid::Uuid::new_v4()),
        )
        .is_err());
    }

    #[test]
    fn load_rejects_corrupt_persisted_neighbors() {
        let (config, data) = two_node_hnsw_data();
        let directory = tempfile::tempdir().unwrap();
        let mut data_level0 = data.data_level0_buffer().to_vec();
        assert!(u32::from_ne_bytes(data_level0[..4].try_into().unwrap()) > 0);
        data_level0[4..8].copy_from_slice(&u32::MAX.to_ne_bytes());
        std::fs::write(directory.path().join("header.bin"), data.header_buffer()).unwrap();
        std::fs::write(directory.path().join("data_level0.bin"), data_level0).unwrap();
        std::fs::write(directory.path().join("length.bin"), data.length_buffer()).unwrap();
        std::fs::write(
            directory.path().join("link_lists.bin"),
            data.link_list_buffer(),
        )
        .unwrap();

        assert!(HnswIndex::load(
            directory.path().to_str().unwrap(),
            &config,
            100,
            IndexUuid(uuid::Uuid::new_v4()),
        )
        .is_err());
    }

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
