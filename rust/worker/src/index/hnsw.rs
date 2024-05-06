use std::ffi::CString;
use std::ffi::{c_char, c_int};

use crate::errors::{ChromaError, ErrorCodes};

use super::{Index, IndexConfig, PersistentIndex};
use crate::types::{Metadata, MetadataValue, MetadataValueConversionError, Segment};
use thiserror::Error;
use uuid::Uuid;

// https://doc.rust-lang.org/nomicon/ffi.html#representing-opaque-structs
#[repr(C)]
struct IndexPtrFFI {
    _data: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

// TODO: Make this config:
// - Watchable - for dynamic updates
// - Have a notion of static vs dynamic config
// - Have a notion of default config
// - HNSWIndex should store a ref to the config so it can look up the config values.
//   deferring this for a config pass
#[derive(Clone, Debug)]
pub(crate) struct HnswIndexConfig {
    pub(crate) max_elements: usize,
    pub(crate) m: usize,
    pub(crate) ef_construction: usize,
    pub(crate) ef_search: usize,
    pub(crate) random_seed: usize,
    pub(crate) persist_path: String,
}

#[derive(Error, Debug)]
pub(crate) enum HnswIndexFromSegmentError {
    #[error("Missing config `{0}`")]
    MissingConfig(String),
    #[error("Invalid metadata value")]
    MetadataValueError(#[from] MetadataValueConversionError),
}

impl ChromaError for HnswIndexFromSegmentError {
    fn code(&self) -> ErrorCodes {
        crate::errors::ErrorCodes::InvalidArgument
    }
}

impl HnswIndexConfig {
    pub(crate) fn from_segment(
        segment: &Segment,
        persist_path: &std::path::Path,
    ) -> Result<HnswIndexConfig, Box<HnswIndexFromSegmentError>> {
        let persist_path = match persist_path.to_str() {
            Some(persist_path) => persist_path,
            None => {
                return Err(Box::new(HnswIndexFromSegmentError::MissingConfig(
                    "persist_path".to_string(),
                )))
            }
        };
        let metadata = match &segment.metadata {
            Some(metadata) => metadata,
            None => {
                // TODO: This should error, but the configuration is not stored correctly
                // after the configuration is refactored to be always stored and doesn't rely on defaults we can fix this
                return Ok(HnswIndexConfig {
                    max_elements: 1000,
                    m: 16,
                    ef_construction: 100,
                    ef_search: 10,
                    random_seed: 0,
                    persist_path: persist_path.to_string(),
                });
                // return Err(Box::new(HnswIndexFromSegmentError::MissingConfig(
                //     "metadata".to_string(),
                // )))
            }
        };

        fn get_metadata_value_as<'a, T>(
            metadata: &'a Metadata,
            key: &str,
        ) -> Result<T, Box<HnswIndexFromSegmentError>>
        where
            T: TryFrom<&'a MetadataValue, Error = MetadataValueConversionError>,
        {
            let res = match metadata.get(key) {
                Some(value) => T::try_from(value),
                None => {
                    return Err(Box::new(HnswIndexFromSegmentError::MissingConfig(
                        key.to_string(),
                    )))
                }
            };
            match res {
                Ok(value) => Ok(value),
                Err(e) => Err(Box::new(HnswIndexFromSegmentError::MetadataValueError(e))),
            }
        }

        let max_elements = get_metadata_value_as::<i32>(metadata, "hsnw:max_elements")?;
        let m = get_metadata_value_as::<i32>(metadata, "hnsw:m")?;
        let ef_construction = get_metadata_value_as::<i32>(metadata, "hnsw:ef_construction")?;
        let ef_search = get_metadata_value_as::<i32>(metadata, "hnsw:ef_search")?;
        return Ok(HnswIndexConfig {
            max_elements: max_elements as usize,
            m: m as usize,
            ef_construction: ef_construction as usize,
            ef_search: ef_search as usize,
            random_seed: 0,
            persist_path: persist_path.to_string(),
        });
    }
}

#[repr(C)]
/// The HnswIndex struct.
/// # Description
/// This struct wraps a pointer to the C++ HnswIndex class and presents a safe Rust interface.
/// # Notes
/// This struct is not thread safe for concurrent reads and writes. Callers should
/// synchronize access to the index between reads and writes.
pub(crate) struct HnswIndex {
    ffi_ptr: *const IndexPtrFFI,
    dimensionality: i32,
    pub(crate) id: Uuid,
}

// Make index sync, we should wrap index so that it is sync in the way we expect but for now this implements the trait
unsafe impl Sync for HnswIndex {}
unsafe impl Send for HnswIndex {}

#[derive(Error, Debug)]

pub(crate) enum HnswIndexInitError {
    #[error("No config provided")]
    NoConfigProvided,
    #[error("Invalid distance function `{0}`")]
    InvalidDistanceFunction(String),
    #[error("Invalid path `{0}`. Are you sure the path exists?")]
    InvalidPath(String),
}

impl ChromaError for HnswIndexInitError {
    fn code(&self) -> ErrorCodes {
        crate::errors::ErrorCodes::InvalidArgument
    }
}

impl Index<HnswIndexConfig> for HnswIndex {
    fn init(
        index_config: &IndexConfig,
        hnsw_config: Option<&HnswIndexConfig>,
        id: Uuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match hnsw_config {
            None => return Err(Box::new(HnswIndexInitError::NoConfigProvided)),
            Some(config) => {
                let distance_function_string: String =
                    index_config.distance_function.clone().into();

                let space_name = match CString::new(distance_function_string) {
                    Ok(space_name) => space_name,
                    Err(e) => {
                        return Err(Box::new(HnswIndexInitError::InvalidDistanceFunction(
                            e.to_string(),
                        )))
                    }
                };

                let ffi_ptr =
                    unsafe { create_index(space_name.as_ptr(), index_config.dimensionality) };

                let path = match CString::new(config.persist_path.clone()) {
                    Ok(path) => path,
                    Err(e) => return Err(Box::new(HnswIndexInitError::InvalidPath(e.to_string()))),
                };

                unsafe {
                    init_index(
                        ffi_ptr,
                        config.max_elements,
                        config.m,
                        config.ef_construction,
                        config.random_seed,
                        true,
                        true,
                        path.as_ptr(),
                    );
                }

                let hnsw_index = HnswIndex {
                    ffi_ptr: ffi_ptr,
                    dimensionality: index_config.dimensionality,
                    id,
                };
                hnsw_index.set_ef(config.ef_search);
                Ok(hnsw_index)
            }
        }
    }

    fn add(&self, id: usize, vector: &[f32]) {
        unsafe { add_item(self.ffi_ptr, vector.as_ptr(), id, false) }
    }

    fn query(&self, vector: &[f32], k: usize) -> (Vec<usize>, Vec<f32>) {
        let actual_k = std::cmp::min(k, self.len());
        let mut ids = vec![0usize; actual_k];
        let mut distance = vec![0.0f32; actual_k];
        unsafe {
            knn_query(
                self.ffi_ptr,
                vector.as_ptr(),
                k,
                ids.as_mut_ptr(),
                distance.as_mut_ptr(),
            );
        }
        return (ids, distance);
    }

    fn get(&self, id: usize) -> Option<Vec<f32>> {
        unsafe {
            let mut data: Vec<f32> = vec![0.0f32; self.dimensionality as usize];
            get_item(self.ffi_ptr, id, data.as_mut_ptr());
            return Some(data);
        }
    }
}

impl PersistentIndex<HnswIndexConfig> for HnswIndex {
    fn save(&self) -> Result<(), Box<dyn ChromaError>> {
        unsafe { persist_dirty(self.ffi_ptr) };
        Ok(())
    }

    fn load(
        path: &str,
        index_config: &IndexConfig,
        id: Uuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let distance_function_string: String = index_config.distance_function.clone().into();
        let space_name = match CString::new(distance_function_string) {
            Ok(space_name) => space_name,
            Err(e) => {
                return Err(Box::new(HnswIndexInitError::InvalidDistanceFunction(
                    e.to_string(),
                )))
            }
        };
        let ffi_ptr = unsafe { create_index(space_name.as_ptr(), index_config.dimensionality) };
        let path = match CString::new(path.to_string()) {
            Ok(path) => path,
            Err(e) => return Err(Box::new(HnswIndexInitError::InvalidPath(e.to_string()))),
        };
        unsafe {
            load_index(ffi_ptr, path.as_ptr(), true, true);
        }
        let hnsw_index = HnswIndex {
            ffi_ptr: ffi_ptr,
            dimensionality: index_config.dimensionality,
            id,
        };
        Ok(hnsw_index)
    }
}

impl HnswIndex {
    pub fn set_ef(&self, ef: usize) {
        unsafe { set_ef(self.ffi_ptr, ef as c_int) }
    }

    pub fn get_ef(&self) -> usize {
        unsafe { get_ef(self.ffi_ptr) as usize }
    }

    pub fn len(&self) -> usize {
        unsafe { len(self.ffi_ptr) as usize }
    }
}

#[link(name = "bindings", kind = "static")]
extern "C" {
    fn create_index(space_name: *const c_char, dim: c_int) -> *const IndexPtrFFI;

    fn init_index(
        index: *const IndexPtrFFI,
        max_elements: usize,
        M: usize,
        ef_construction: usize,
        random_seed: usize,
        allow_replace_deleted: bool,
        is_persistent: bool,
        path: *const c_char,
    );

    fn load_index(
        index: *const IndexPtrFFI,
        path: *const c_char,
        allow_replace_deleted: bool,
        is_persistent_index: bool,
    );

    fn persist_dirty(index: *const IndexPtrFFI);

    fn add_item(index: *const IndexPtrFFI, data: *const f32, id: usize, replace_deleted: bool);
    fn get_item(index: *const IndexPtrFFI, id: usize, data: *mut f32);
    fn knn_query(
        index: *const IndexPtrFFI,
        query_vector: *const f32,
        k: usize,
        ids: *mut usize,
        distance: *mut f32,
    );

    fn get_ef(index: *const IndexPtrFFI) -> c_int;
    fn set_ef(index: *const IndexPtrFFI, ef: c_int);
    fn len(index: *const IndexPtrFFI) -> c_int;

}

#[cfg(test)]
pub mod test {
    use super::*;

    use crate::distance::DistanceFunction;
    use crate::index::utils;
    use rand::Rng;
    use rayon::prelude::*;
    use rayon::ThreadPoolBuilder;
    use tempfile::tempdir;

    #[test]
    fn it_initializes_and_can_set_get_ef() {
        let n = 1000;
        let d: usize = 960;
        let tmp_dir = tempdir().unwrap();
        let persist_path = tmp_dir.path().to_str().unwrap().to_string();
        let distance_function = DistanceFunction::Euclidean;
        let index = HnswIndex::init(
            &IndexConfig {
                dimensionality: d as i32,
                distance_function: distance_function,
            },
            Some(&HnswIndexConfig {
                max_elements: n,
                m: 16,
                ef_construction: 100,
                ef_search: 10,
                random_seed: 0,
                persist_path: persist_path,
            }),
            Uuid::new_v4(),
        );
        match index {
            Err(e) => panic!("Error initializing index: {}", e),
            Ok(index) => {
                assert_eq!(index.get_ef(), 10);
                index.set_ef(100);
                assert_eq!(index.get_ef(), 100);
            }
        }
    }

    #[test]
    fn it_can_add_parallel() {
        let n: usize = 100;
        let d: usize = 960;
        let distance_function = DistanceFunction::InnerProduct;
        let tmp_dir = tempdir().unwrap();
        let persist_path = tmp_dir.path().to_str().unwrap().to_string();
        let index = HnswIndex::init(
            &IndexConfig {
                dimensionality: d as i32,
                distance_function: distance_function,
            },
            Some(&HnswIndexConfig {
                max_elements: n,
                m: 16,
                ef_construction: 100,
                ef_search: 100,
                random_seed: 0,
                persist_path: persist_path,
            }),
            Uuid::new_v4(),
        );

        let index = match index {
            Err(e) => panic!("Error initializing index: {}", e),
            Ok(index) => index,
        };

        let ids: Vec<usize> = (0..n).collect();

        // Add data in parallel, using global pool for testing
        ThreadPoolBuilder::new()
            .num_threads(12)
            .build_global()
            .unwrap();

        let mut rng: rand::prelude::ThreadRng = rand::thread_rng();
        let mut datas = Vec::new();
        for _ in 0..n {
            let mut data: Vec<f32> = Vec::new();
            for _ in 0..960 {
                data.push(rng.gen());
            }
            datas.push(data);
        }

        (0..n).into_par_iter().for_each(|i| {
            let data = &datas[i];
            index.add(ids[i], data);
        });

        assert_eq!(index.len(), n);

        // Get the data and check it
        let mut i = 0;
        for id in ids {
            let actual_data = index.get(id);
            match actual_data {
                None => panic!("No data found for id: {}", id),
                Some(actual_data) => {
                    assert_eq!(actual_data.len(), d);
                    for j in 0..d {
                        // Floating point epsilon comparison
                        assert!((actual_data[j] - datas[i][j]).abs() < 0.00001);
                    }
                }
            }
            i += 1;
        }
    }

    #[test]
    fn it_can_add_and_basic_query() {
        let n = 1;
        let d: usize = 960;
        let distance_function = DistanceFunction::Euclidean;
        let tmp_dir = tempdir().unwrap();
        let persist_path = tmp_dir.path().to_str().unwrap().to_string();
        let index = HnswIndex::init(
            &IndexConfig {
                dimensionality: d as i32,
                distance_function: distance_function,
            },
            Some(&HnswIndexConfig {
                max_elements: n,
                m: 16,
                ef_construction: 100,
                ef_search: 100,
                random_seed: 0,
                persist_path: persist_path,
            }),
            Uuid::new_v4(),
        );

        let index = match index {
            Err(e) => panic!("Error initializing index: {}", e),
            Ok(index) => index,
        };
        assert_eq!(index.get_ef(), 100);

        let data: Vec<f32> = utils::generate_random_data(n, d);
        let ids: Vec<usize> = (0..n).collect();

        (0..n).into_iter().for_each(|i| {
            let data = &data[i * d..(i + 1) * d];
            index.add(ids[i], data);
        });

        // Assert length
        assert_eq!(index.len(), n);

        // Get the data and check it
        let mut i = 0;
        for id in ids {
            let actual_data = index.get(id);
            match actual_data {
                None => panic!("No data found for id: {}", id),
                Some(actual_data) => {
                    assert_eq!(actual_data.len(), d);
                    for j in 0..d {
                        // Floating point epsilon comparison
                        assert!((actual_data[j] - data[i * d + j]).abs() < 0.00001);
                    }
                }
            }
            i += 1;
        }

        // Query the data
        let query = &data[0..d];
        let (ids, distances) = index.query(query, 1);
        assert_eq!(ids.len(), 1);
        assert_eq!(distances.len(), 1);
        assert_eq!(ids[0], 0);
        assert_eq!(distances[0], 0.0);
    }

    #[test]
    fn it_can_persist_and_load() {
        let n = 1000;
        let d: usize = 960;
        let distance_function = DistanceFunction::Euclidean;
        let tmp_dir = tempdir().unwrap();
        let persist_path = tmp_dir.path().to_str().unwrap().to_string();
        let id = Uuid::new_v4();
        let index = HnswIndex::init(
            &IndexConfig {
                dimensionality: d as i32,
                distance_function: distance_function.clone(),
            },
            Some(&HnswIndexConfig {
                max_elements: n,
                m: 32,
                ef_construction: 100,
                ef_search: 100,
                random_seed: 0,
                persist_path: persist_path.clone(),
            }),
            id,
        );

        let index = match index {
            Err(e) => panic!("Error initializing index: {}", e),
            Ok(index) => index,
        };

        let data: Vec<f32> = utils::generate_random_data(n, d);
        let ids: Vec<usize> = (0..n).collect();

        (0..n).into_iter().for_each(|i| {
            let data = &data[i * d..(i + 1) * d];
            index.add(ids[i], data);
        });

        // Persist the index
        let res = index.save();
        match res {
            Err(e) => panic!("Error saving index: {}", e),
            Ok(_) => {}
        }

        // Load the index
        let index = HnswIndex::load(
            &persist_path,
            &IndexConfig {
                dimensionality: d as i32,
                distance_function: distance_function,
            },
            id,
        );

        let index = match index {
            Err(e) => panic!("Error loading index: {}", e),
            Ok(index) => index,
        };
        // TODO: This should be set by the load
        index.set_ef(100);
        assert_eq!(index.id, id);

        // Query the data
        let query = &data[0..d];
        let (ids, distances) = index.query(query, 1);
        assert_eq!(ids.len(), 1);
        assert_eq!(distances.len(), 1);
        assert_eq!(ids[0], 0);
        assert_eq!(distances[0], 0.0);

        // Get the data and check it
        let mut i = 0;
        for id in ids {
            let actual_data = index.get(id);
            match actual_data {
                None => panic!("No data found for id: {}", id),
                Some(actual_data) => {
                    assert_eq!(actual_data.len(), d);
                    for j in 0..d {
                        assert_eq!(actual_data[j], data[i * d + j]);
                    }
                }
            }
            i += 1;
        }
    }
}
