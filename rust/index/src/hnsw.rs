use super::{Index, IndexConfig, PersistentIndex};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Metadata, MetadataValue, MetadataValueConversionError, Segment};
use std::ffi::CString;
use std::ffi::{c_char, c_int};
use std::str::Utf8Error;
use thiserror::Error;
use tracing::instrument;
use uuid::Uuid;

const DEFAULT_MAX_ELEMENTS: usize = 10000;
const DEFAULT_HNSW_M: usize = 16;
const DEFAULT_HNSW_EF_CONSTRUCTION: usize = 100;
const DEFAULT_HNSW_EF_SEARCH: usize = 10;

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
// - TODO: HNSWIndex should store a ref to the config so it can look up the config values.
//   deferring this for a config pass
#[derive(Clone, Debug)]
pub struct HnswIndexConfig {
    pub max_elements: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub random_seed: usize,
    pub persist_path: String,
}

#[derive(Error, Debug)]
pub enum HnswIndexFromSegmentError {
    #[error("Missing config `{0}`")]
    MissingConfig(String),
    #[error("Invalid metadata value")]
    MetadataValueError(#[from] MetadataValueConversionError),
}

impl ChromaError for HnswIndexFromSegmentError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

impl HnswIndexConfig {
    pub fn from_segment(
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
                    max_elements: DEFAULT_MAX_ELEMENTS,
                    m: DEFAULT_HNSW_M,
                    ef_construction: DEFAULT_HNSW_EF_CONSTRUCTION,
                    ef_search: DEFAULT_HNSW_EF_SEARCH,
                    random_seed: 0,
                    persist_path: persist_path.to_string(),
                });
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

        let m = get_metadata_value_as::<i32>(metadata, "hnsw:M").unwrap_or(DEFAULT_HNSW_M as i32);
        let ef_construction = get_metadata_value_as::<i32>(metadata, "hnsw:construction_ef")
            .unwrap_or(DEFAULT_HNSW_EF_CONSTRUCTION as i32);
        let ef_search = get_metadata_value_as::<i32>(metadata, "hnsw:search_ef")
            .unwrap_or(DEFAULT_HNSW_EF_SEARCH as i32);
        Ok(HnswIndexConfig {
            max_elements: DEFAULT_MAX_ELEMENTS,
            m: m as usize,
            ef_construction: ef_construction as usize,
            ef_search: ef_search as usize,
            random_seed: 0,
            persist_path: persist_path.to_string(),
        })
    }
}

#[repr(C)]
/// The HnswIndex struct.
/// # Description
/// This struct wraps a pointer to the C++ HnswIndex class and presents a safe Rust interface.
/// # Notes
/// This struct is not thread safe for concurrent reads and writes. Callers should
/// synchronize access to the index between reads and writes.
pub struct HnswIndex {
    ffi_ptr: *const IndexPtrFFI,
    dimensionality: i32,
    pub id: Uuid,
}

// Make index sync, we should wrap index so that it is sync in the way we expect but for now this implements the trait
unsafe impl Sync for HnswIndex {}
unsafe impl Send for HnswIndex {}

#[derive(Error, Debug)]

pub enum HnswIndexInitError {
    #[error("No config provided")]
    NoConfigProvided,
    #[error("Invalid distance function `{0}`")]
    InvalidDistanceFunction(String),
    #[error("Invalid path `{0}`. Are you sure the path exists?")]
    InvalidPath(String),
}

impl ChromaError for HnswIndexInitError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

#[derive(Error, Debug)]
pub enum HnswError {
    // A generic C++ exception, stores the error message
    #[error("HnswError: `{0}`")]
    FFIException(String),
    #[error(transparent)]
    ErrorStringRead(#[from] Utf8Error),
}

impl ChromaError for HnswError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::FFIException(_) => ErrorCodes::Internal,
            Self::ErrorStringRead(_) => ErrorCodes::Internal,
        }
    }
}

impl Index<HnswIndexConfig> for HnswIndex {
    fn init(
        index_config: &IndexConfig,
        hnsw_config: Option<&HnswIndexConfig>,
        id: Uuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match hnsw_config {
            None => Err(Box::new(HnswIndexInitError::NoConfigProvided)),
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
                read_and_return_hnsw_error(ffi_ptr)?;

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
                read_and_return_hnsw_error(ffi_ptr)?;

                let hnsw_index = HnswIndex {
                    ffi_ptr,
                    dimensionality: index_config.dimensionality,
                    id,
                };
                hnsw_index.set_ef(config.ef_search)?;
                Ok(hnsw_index)
            }
        }
    }

    fn add(&self, id: usize, vector: &[f32]) -> Result<(), Box<dyn ChromaError>> {
        unsafe { add_item(self.ffi_ptr, vector.as_ptr(), id, true) }
        read_and_return_hnsw_error(self.ffi_ptr)
    }

    fn delete(&self, id: usize) -> Result<(), Box<dyn ChromaError>> {
        unsafe { mark_deleted(self.ffi_ptr, id) }
        read_and_return_hnsw_error(self.ffi_ptr)
    }

    fn query(
        &self,
        vector: &[f32],
        k: usize,
        allowed_ids: &[usize],
        disallowed_ids: &[usize],
    ) -> Result<(Vec<usize>, Vec<f32>), Box<dyn ChromaError>> {
        let actual_k = std::cmp::min(k, self.len());
        let mut ids = vec![0usize; actual_k];
        let mut distance = vec![0.0f32; actual_k];
        let total_result = unsafe {
            knn_query(
                self.ffi_ptr,
                vector.as_ptr(),
                k,
                ids.as_mut_ptr(),
                distance.as_mut_ptr(),
                allowed_ids.as_ptr(),
                allowed_ids.len(),
                disallowed_ids.as_ptr(),
                disallowed_ids.len(),
            ) as usize
        };
        read_and_return_hnsw_error(self.ffi_ptr)?;

        if total_result < actual_k {
            ids.truncate(total_result);
            distance.truncate(total_result);
        }
        Ok((ids, distance))
    }

    fn get(&self, id: usize) -> Result<Option<Vec<f32>>, Box<dyn ChromaError>> {
        unsafe {
            let mut data: Vec<f32> = vec![0.0f32; self.dimensionality as usize];
            get_item(self.ffi_ptr, id, data.as_mut_ptr());
            read_and_return_hnsw_error(self.ffi_ptr)?;
            Ok(Some(data))
        }
    }
}

impl PersistentIndex<HnswIndexConfig> for HnswIndex {
    fn save(&self) -> Result<(), Box<dyn ChromaError>> {
        unsafe { persist_dirty(self.ffi_ptr) };
        read_and_return_hnsw_error(self.ffi_ptr)?;
        Ok(())
    }

    #[instrument(name = "HnswIndex load", level = "info")]
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
        read_and_return_hnsw_error(ffi_ptr)?;

        let path = match CString::new(path.to_string()) {
            Ok(path) => path,
            Err(e) => return Err(Box::new(HnswIndexInitError::InvalidPath(e.to_string()))),
        };
        unsafe {
            load_index(ffi_ptr, path.as_ptr(), true, true);
        }
        read_and_return_hnsw_error(ffi_ptr)?;

        let hnsw_index = HnswIndex {
            ffi_ptr,
            dimensionality: index_config.dimensionality,
            id,
        };
        Ok(hnsw_index)
    }
}

impl HnswIndex {
    fn set_ef(&self, ef: usize) -> Result<(), Box<dyn ChromaError>> {
        unsafe { set_ef(self.ffi_ptr, ef as c_int) }
        read_and_return_hnsw_error(self.ffi_ptr)
    }

    pub fn len(&self) -> usize {
        unsafe { len(self.ffi_ptr) as usize }
        // Does not return an error
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn dimensionality(&self) -> i32 {
        self.dimensionality
    }

    pub fn capacity(&self) -> usize {
        unsafe { capacity(self.ffi_ptr) as usize }
        // Does not return an error
    }

    pub fn resize(&mut self, new_size: usize) -> Result<(), Box<dyn ChromaError>> {
        unsafe { resize_index(self.ffi_ptr, new_size) }
        read_and_return_hnsw_error(self.ffi_ptr)
    }

    #[cfg(test)]
    fn get_ef(&self) -> Result<usize, Box<dyn ChromaError>> {
        let ret_val;
        unsafe { ret_val = get_ef(self.ffi_ptr) as usize }
        read_and_return_hnsw_error(self.ffi_ptr)?;
        Ok(ret_val)
    }
}

impl Drop for HnswIndex {
    fn drop(&mut self) {
        unsafe { free_index(self.ffi_ptr) }
    }
}

fn read_and_return_hnsw_error(ffi_ptr: *const IndexPtrFFI) -> Result<(), Box<dyn ChromaError>> {
    let err = unsafe { get_last_error(ffi_ptr) };
    if !err.is_null() {
        match unsafe { std::ffi::CStr::from_ptr(err).to_str() } {
            Ok(err_str) => return Err(Box::new(HnswError::FFIException(err_str.to_string()))),
            Err(e) => return Err(Box::new(HnswError::ErrorStringRead(e))),
        }
    }
    Ok(())
}

#[link(name = "bindings", kind = "static")]
extern "C" {
    fn create_index(space_name: *const c_char, dim: c_int) -> *const IndexPtrFFI;

    fn free_index(index: *const IndexPtrFFI);

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
    fn mark_deleted(index: *const IndexPtrFFI, id: usize);
    fn get_item(index: *const IndexPtrFFI, id: usize, data: *mut f32);
    fn knn_query(
        index: *const IndexPtrFFI,
        query_vector: *const f32,
        k: usize,
        ids: *mut usize,
        distance: *mut f32,
        allowed_ids: *const usize,
        allowed_ids_length: usize,
        disallowed_ids: *const usize,
        disallowed_ids_length: usize,
    ) -> c_int;

    #[cfg(test)]
    fn get_ef(index: *const IndexPtrFFI) -> c_int;

    fn set_ef(index: *const IndexPtrFFI, ef: c_int);
    fn len(index: *const IndexPtrFFI) -> c_int;
    fn capacity(index: *const IndexPtrFFI) -> c_int;
    fn resize_index(index: *const IndexPtrFFI, new_size: usize);
    fn get_last_error(index: *const IndexPtrFFI) -> *const c_char;
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::utils;
    use chroma_distance::DistanceFunction;
    use rand::seq::IteratorRandom;
    use rayon::prelude::*;
    use rayon::ThreadPoolBuilder;
    use std::collections::HashMap;
    use tempfile::tempdir;

    const EPS: f32 = 0.00001;

    fn index_data_same(index: &HnswIndex, ids: &[usize], data: &[f32], dim: usize) {
        for (i, id) in ids.iter().cloned().enumerate() {
            let actual_data = index.get(id);
            match actual_data {
                Ok(actual_data) => match actual_data {
                    None => panic!("No data found for id: {}", id),
                    Some(actual_data) => {
                        assert_eq!(actual_data.len(), dim);
                        for j in 0..dim {
                            // Floating point epsilon comparison
                            assert!((actual_data[j] - data[i * dim + j]).abs() < EPS);
                        }
                    }
                },
                Err(_) => panic!("Did not expect error"),
            }
        }
    }

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
                distance_function,
            },
            Some(&HnswIndexConfig {
                max_elements: n,
                m: 16,
                ef_construction: 100,
                ef_search: 10,
                random_seed: 0,
                persist_path,
            }),
            Uuid::new_v4(),
        );
        match index {
            Err(e) => panic!("Error initializing index: {}", e),
            Ok(index) => {
                assert_eq!(index.get_ef().unwrap(), 10);
                index.set_ef(100).expect("Should not error");
                assert_eq!(index.get_ef().unwrap(), 100);
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
                distance_function,
            },
            Some(&HnswIndexConfig {
                max_elements: n,
                m: 16,
                ef_construction: 100,
                ef_search: 100,
                random_seed: 0,
                persist_path,
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

        let data = utils::generate_random_data(n, d);

        (0..n).into_par_iter().for_each(|i| {
            let data = &data[i * d..(i + 1) * d];
            index.add(ids[i], data).expect("Should not error");
        });

        assert_eq!(index.len(), n);

        // Get the data and check it
        index_data_same(&index, &ids, &data, d);
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
                distance_function,
            },
            Some(&HnswIndexConfig {
                max_elements: n,
                m: 16,
                ef_construction: 100,
                ef_search: 100,
                random_seed: 0,
                persist_path,
            }),
            Uuid::new_v4(),
        );

        let index = match index {
            Err(e) => panic!("Error initializing index: {}", e),
            Ok(index) => index,
        };
        assert_eq!(index.get_ef().unwrap(), 100);

        let data: Vec<f32> = utils::generate_random_data(n, d);
        let ids: Vec<usize> = (0..n).collect();

        (0..n).for_each(|i| {
            let data = &data[i * d..(i + 1) * d];
            index.add(ids[i], data).expect("Should not error");
        });

        // Assert length
        assert_eq!(index.len(), n);

        // Get the data and check it
        index_data_same(&index, &ids, &data, d);

        // Query the data
        let query = &data[0..d];
        let allow_ids = &[];
        let disallow_ids = &[];
        let (ids, distances) = index.query(query, 1, allow_ids, disallow_ids).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(distances.len(), 1);
        assert_eq!(ids[0], 0);
        assert_eq!(distances[0], 0.0);
    }

    #[test]
    fn it_can_add_and_delete() {
        let n = 1000;
        let d = 960;

        let distance_function = DistanceFunction::Euclidean;
        let tmp_dir = tempdir().unwrap();
        let persist_path = tmp_dir.path().to_str().unwrap().to_string();
        let index = HnswIndex::init(
            &IndexConfig {
                dimensionality: d as i32,
                distance_function,
            },
            Some(&HnswIndexConfig {
                max_elements: n,
                m: 16,
                ef_construction: 100,
                ef_search: 100,
                random_seed: 0,
                persist_path,
            }),
            Uuid::new_v4(),
        );

        let index = match index {
            Err(e) => panic!("Error initializing index: {}", e),
            Ok(index) => index,
        };

        let data: Vec<f32> = utils::generate_random_data(n, d);
        let ids: Vec<usize> = (0..n).collect();

        (0..n).for_each(|i| {
            let data = &data[i * d..(i + 1) * d];
            index.add(ids[i], data).expect("Should not error");
        });

        assert_eq!(index.len(), n);

        // Delete some of the data
        let mut rng = rand::thread_rng();
        let delete_ids: Vec<usize> = (0..n).choose_multiple(&mut rng, n / 20);

        for id in &delete_ids {
            index.delete(*id).expect("Should not error");
        }

        assert_eq!(index.len(), n - delete_ids.len());

        let allow_ids = &[];
        let disallow_ids = &[];
        // Query for the deleted ids and ensure they are not found
        for deleted_id in &delete_ids {
            let target_vector = &data[*deleted_id * d..(*deleted_id + 1) * d];
            let (ids, _) = index
                .query(target_vector, 10, allow_ids, disallow_ids)
                .unwrap();
            for check_deleted_id in &delete_ids {
                assert!(!ids.contains(check_deleted_id));
            }
        }
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

        (0..n).for_each(|i| {
            let data = &data[i * d..(i + 1) * d];
            index.add(ids[i], data).expect("Should not error");
        });

        // Persist the index
        let res = index.save();
        if let Err(e) = res {
            panic!("Error saving index: {}", e);
        }

        // Load the index
        let index = HnswIndex::load(
            &persist_path,
            &IndexConfig {
                dimensionality: d as i32,
                distance_function,
            },
            id,
        );

        let index = match index {
            Err(e) => panic!("Error loading index: {}", e),
            Ok(index) => index,
        };
        // TODO: This should be set by the load
        index.set_ef(100).expect("Should not error");
        assert_eq!(index.id, id);

        // Query the data
        let query = &data[0..d];
        let allow_ids = &[];
        let disallow_ids = &[];
        let (ids, distances) = index.query(query, 1, allow_ids, disallow_ids).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(distances.len(), 1);
        assert_eq!(ids[0], 0);
        assert_eq!(distances[0], 0.0);

        // Get the data and check it
        index_data_same(&index, &ids, &data, d);
    }

    #[test]
    fn it_can_add_and_query_with_allowed_and_disallowed_ids() {
        let n = 1000;
        let d: usize = 960;
        let distance_function = DistanceFunction::Euclidean;
        let tmp_dir = tempdir().unwrap();
        let persist_path = tmp_dir.path().to_str().unwrap().to_string();
        let index = HnswIndex::init(
            &IndexConfig {
                dimensionality: d as i32,
                distance_function,
            },
            Some(&HnswIndexConfig {
                max_elements: n,
                m: 16,
                ef_construction: 100,
                ef_search: 100,
                random_seed: 0,
                persist_path,
            }),
            Uuid::new_v4(),
        );

        let index = match index {
            Err(e) => panic!("Error initializing index: {}", e),
            Ok(index) => index,
        };

        let data: Vec<f32> = utils::generate_random_data(n, d);
        let ids: Vec<usize> = (0..n).collect();

        (0..n).for_each(|i| {
            let data = &data[i * d..(i + 1) * d];
            index.add(ids[i], data).expect("Should not error");
        });

        // Query the data
        let query = &data[0..d];
        let allow_ids = &[0, 2];
        let disallow_ids = &[3];
        let (ids, distances) = index.query(query, 10, allow_ids, disallow_ids).unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(distances.len(), 2);
    }

    #[test]
    fn it_can_resize() {
        let n = 1000;
        let d: usize = 960;
        let distance_function = DistanceFunction::Euclidean;
        let tmp_dir = tempdir().unwrap();
        let persist_path = tmp_dir.path().to_str().unwrap().to_string();
        let index = HnswIndex::init(
            &IndexConfig {
                dimensionality: d as i32,
                distance_function,
            },
            Some(&HnswIndexConfig {
                max_elements: n,
                m: 16,
                ef_construction: 100,
                ef_search: 100,
                random_seed: 0,
                persist_path,
            }),
            Uuid::new_v4(),
        );

        let mut index = match index {
            Err(e) => panic!("Error initializing index: {}", e),
            Ok(index) => index,
        };

        let data: Vec<f32> = utils::generate_random_data(2 * n, d);
        let ids: Vec<usize> = (0..2 * n).collect();

        (0..n).for_each(|i| {
            let data = &data[i * d..(i + 1) * d];
            index.add(ids[i], data).expect("Should not error");
        });
        assert_eq!(index.capacity(), n);

        // Resize the index to 2*n
        index.resize(2 * n).expect("Should not error");

        assert_eq!(index.len(), n);
        assert_eq!(index.capacity(), 2 * n);

        // Add another n elements from n to 2n
        (n..2 * n).for_each(|i| {
            let data = &data[i * d..(i + 1) * d];
            index.add(ids[i], data).expect("Should not error");
        });
    }

    #[test]
    fn parameter_defaults() {
        let segment = Segment {
            id: Uuid::new_v4(),
            r#type: chroma_types::SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            metadata: Some(HashMap::new()),
            collection: Uuid::new_v4(),
            file_path: HashMap::new(),
        };

        let persist_path = tempdir().unwrap().path().to_owned();
        let config = HnswIndexConfig::from_segment(&segment, &persist_path)
            .expect("Failed to create config from segment");

        assert_eq!(config.max_elements, DEFAULT_MAX_ELEMENTS);
        assert_eq!(config.m, DEFAULT_HNSW_M);
        assert_eq!(config.ef_construction, DEFAULT_HNSW_EF_CONSTRUCTION);
        assert_eq!(config.ef_search, DEFAULT_HNSW_EF_SEARCH);
        assert_eq!(config.random_seed, 0);
        assert_eq!(config.persist_path, persist_path.to_str().unwrap());

        // Try partial metadata
        let mut metadata = HashMap::new();
        metadata.insert("hnsw:M".to_string(), MetadataValue::Int(10_i32));

        let segment = Segment {
            id: Uuid::new_v4(),
            r#type: chroma_types::SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            metadata: Some(metadata),
            collection: Uuid::new_v4(),
            file_path: HashMap::new(),
        };

        let config = HnswIndexConfig::from_segment(&segment, &persist_path)
            .expect("Failed to create config from segment");

        assert_eq!(config.max_elements, DEFAULT_MAX_ELEMENTS);
        assert_eq!(config.m, 10);
        assert_eq!(config.ef_construction, DEFAULT_HNSW_EF_CONSTRUCTION);
        assert_eq!(config.ef_search, DEFAULT_HNSW_EF_SEARCH);
        assert_eq!(config.random_seed, 0);
        assert_eq!(config.persist_path, persist_path.to_str().unwrap());
    }

    #[test]
    fn it_can_catch_error() {
        let n = 10;
        let d: usize = 960;
        let distance_function = DistanceFunction::Euclidean;
        let tmp_dir = tempdir().unwrap();
        let persist_path = tmp_dir.path().to_str().unwrap().to_string();
        let index = HnswIndex::init(
            &IndexConfig {
                dimensionality: d as i32,
                distance_function,
            },
            Some(&HnswIndexConfig {
                max_elements: n,
                m: 10,
                ef_construction: 100,
                ef_search: 100,
                random_seed: 0,
                persist_path,
            }),
            Uuid::new_v4(),
        );

        let index = match index {
            Err(e) => panic!("Error initializing index: {}", e),
            Ok(index) => index,
        };

        let data: Vec<f32> = utils::generate_random_data(n, d);
        let ids: Vec<usize> = (0..n).collect();

        (0..n).for_each(|i| {
            let data = &data[i * d..(i + 1) * d];
            index.add(ids[i], data).expect("Should not error");
        });

        // Add more elements than the index can hold
        let data = &data[0..d];
        let res = index.add(n, data);
        match res {
            Err(_) => {}
            Ok(_) => panic!("Expected error"),
        }
    }
}
