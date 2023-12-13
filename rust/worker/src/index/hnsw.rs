use std::ffi::CString;
use std::ffi::{c_char, c_int};

// https://doc.rust-lang.org/nomicon/ffi.html#representing-opaque-structs
#[repr(C)]
struct IndexPtrFFI {
    _data: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

#[repr(C)]
pub struct HnswIndex {
    ptr: Option<*const IndexPtrFFI>,
    // TOOD: put configuration in a struct
    // make a trait Configurable
    dim: Option<usize>,
    max_elements: usize,
    m: usize,
    ef_construction: usize,
    random_seed: usize,
    allow_replace_deleted: bool,
    space_name: String,
    is_persistent: bool,
    persist_path: String,
    pub initialized: bool,
}

// Make index sync, we should wrap index so that it is sync in the way we expect but for now this implements the trait
unsafe impl Sync for HnswIndex {}
unsafe impl Send for HnswIndex {}

// Index impl that is public and wraps the private index extern "C" struct
impl HnswIndex {
    pub fn new(
        space_name: &str,
        max_elements: usize,
        m: usize,
        ef_construction: usize,
        random_seed: usize,
        allow_replace_deleted: bool,
        is_persistent: bool,
        persist_path: &str,
    ) -> HnswIndex {
        println!("Creating index in rust");
        // TODO: enum for spaces
        return HnswIndex {
            ptr: None,
            dim: None,
            max_elements: max_elements,
            m: m,
            ef_construction: ef_construction,
            random_seed: random_seed,
            allow_replace_deleted: allow_replace_deleted,
            is_persistent: is_persistent,
            persist_path: persist_path.to_string(),
            space_name: space_name.to_string(),
            initialized: false,
        };
    }

    pub fn init(
        &mut self,
        dim: usize, // The dimen
    ) {
        if self.initialized {
            return;
        }
        self.dim = Some(dim);
        let space_name = CString::new(self.space_name.to_string()).unwrap();
        let persist_path = CString::new(self.persist_path.to_string()).unwrap();
        let index = unsafe { create_index(space_name.as_ptr(), dim as c_int) };
        self.ptr = Some(index);
        println!("Initializing index in rust");
        unsafe {
            init_index(
                index,
                self.max_elements,
                self.m,
                self.ef_construction,
                self.random_seed,
                self.allow_replace_deleted,
                self.is_persistent,
                persist_path.as_ptr(),
            );
        }
        self.initialized = true;
    }

    pub fn get_ef(&self) -> i32 {
        // TODO: return result and error for all methods
        match self.ptr {
            None => return 0,
            Some(ptr) => unsafe { get_ef(ptr) },
        }
    }

    pub fn set_ef(&self, ef: i32) {
        match self.ptr {
            None => return,
            Some(ptr) => unsafe { set_ef(ptr, ef) },
        }
    }

    pub fn add_item(&self, data: &[f32], id: usize, replace_deleted: bool) {
        match self.ptr {
            None => return,
            Some(ptr) => unsafe { add_item(ptr, data.as_ptr(), id, replace_deleted) },
        }
    }

    pub fn get_item(&self, id: usize) -> Vec<f32> {
        match (self.ptr, self.dim) {
            (None, _) => {
                // TODO: return Result
                let mut data: Vec<f32> = vec![0.0f32; 0];
                return data;
            }
            (Some(ptr), None) => {
                let mut data: Vec<f32> = vec![0.0f32; 0];
                return data;
            }
            (Some(ptr), Some(dim)) => unsafe {
                let mut data: Vec<f32> = vec![0.0f32; dim];
                get_item(ptr, id, data.as_mut_ptr());
                return data;
            },
        }
    }

    pub fn knn_query(&self, query_vector: &[f32], k: usize) -> (Vec<i32>, Vec<f32>) {
        let mut ids = vec![0i32; k];
        let mut distance = vec![0.0f32; k];
        match self.ptr {
            None => return (ids, distance),
            Some(ptr) => {
                unsafe {
                    knn_query(
                        ptr,
                        query_vector.as_ptr(),
                        k,
                        ids.as_mut_ptr(),
                        distance.as_mut_ptr(),
                    );
                }
                return (ids, distance);
            }
        }
    }

    pub fn persist_dirty(&self) {
        // TODO: return result, error if not initialized
        match self.ptr {
            None => return,
            Some(ptr) => unsafe { persist_dirty(ptr) },
        }
    }

    // TODO: clean up this clunkiness where we have to pass in the dimensionality
    pub fn load(&mut self, dim: usize) {
        if self.initialized {
            return;
        }
        let space_name = CString::new(self.space_name.to_string()).unwrap();
        self.dim = Some(dim);
        let index = unsafe { create_index(space_name.as_ptr(), dim as c_int) };
        self.ptr = Some(index);
        match self.ptr {
            None => return,
            Some(ptr) => unsafe {
                let persist_path = CString::new(self.persist_path.to_string()).unwrap();
                println!("RUST IS LOADING INDEX from {}", self.persist_path);
                load_index(
                    ptr,
                    persist_path.as_ptr(),
                    self.allow_replace_deleted,
                    self.is_persistent,
                )
            },
        }
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

    fn add_item(index: *const IndexPtrFFI, data: *const f32, id: usize, replace_deleted: bool);
    fn get_item(index: *const IndexPtrFFI, id: usize, data: *mut f32);

    fn knn_query(
        index: *const IndexPtrFFI,
        query_vector: *const f32,
        k: usize,
        ids: *mut i32,
        distance: *mut f32,
    );

    fn get_ef(index: *const IndexPtrFFI) -> c_int;
    fn set_ef(index: *const IndexPtrFFI, ef: c_int);

    fn persist_dirty(index: *const IndexPtrFFI);
    fn load_index(
        index: *const IndexPtrFFI,
        path: *const c_char,
        allow_replace_deleted: bool,
        is_persistent_index: bool,
    );
}

#[cfg(test)]
pub mod test {
    use super::*;

    use rand::Rng;
    use rayon::prelude::*;
    use rayon::ThreadPoolBuilder;
    use worker::index::Index;
    mod utils;

    #[test]
    fn it_initializes_and_can_set_ef() {
        let n = 1000;
        let d: usize = 960;
        let space_name = "ip";
        let mut index = HnswIndex::new(space_name, n, 16, 100, 0, true, false, "");
        index.init(d);
        assert_eq!(index.get_ef(), 10);
        index.set_ef(100);
        assert_eq!(index.get_ef(), 100);
    }

    #[test]
    fn it_can_add_parallel() {
        let n = 10000;
        let d: usize = 960;
        let space_name = "ip";
        let mut index = HnswIndex::new(space_name, n, 16, 100, 0, true, false, "");
        index.init(d);

        // let data: Vec<f32> = utils::generate_random_data(n, d);
        let ids: Vec<usize> = (0..n).collect();

        // Add data in parallel, using global pool for testing
        ThreadPoolBuilder::new()
            .num_threads(12)
            .build_global()
            .unwrap();

        let mut rng: rand::prelude::ThreadRng = rand::thread_rng();
        let mut datas = Vec::new();
        for i in 0..n {
            let mut data: Vec<f32> = Vec::new();
            for i in 0..960 {
                data.push(rng.gen());
            }
            datas.push(data);
        }

        (0..n).into_par_iter().for_each(|i| {
            // let data = &data[i * d..(i + 1) * d];
            let data = &datas[i];
            println!("Adding item: {}", i);
            index.add_item(data, ids[i], false)
        });

        // Get the data and check it
        let mut i = 0;
        for id in ids {
            let actual_data = index.get_item(id);
            assert_eq!(actual_data.len(), d);
            for j in 0..d {
                assert_eq!(actual_data[j], datas[i][j]);
            }
            i += 1;
        }
    }

    #[test]
    fn it_can_add_and_basic_query() {
        let n = 1000;
        let d: usize = 960;
        let space_name = "l2";
        let mut index = HnswIndex::new(space_name, n, 16, 100, 0, true, false, "");
        index.init(d);
        index.set_ef(100);

        let data: Vec<f32> = utils::generate_random_data(n, d);
        let ids: Vec<usize> = (0..n).collect();

        (0..n).into_iter().for_each(|i| {
            let data = &data[i * d..(i + 1) * d];
            index.add_item(data, ids[i], false)
        });

        // Get the data and check it
        let mut i = 0;
        for id in ids {
            let actual_data = index.get_item(id);
            assert_eq!(actual_data.len(), d);
            for j in 0..d {
                assert_eq!(actual_data[j], data[i * d + j]);
            }
            i += 1;
        }

        // Query the data
        let query = &data[0..d];
        let (ids, distances) = index.knn_query(query, 1);
        assert_eq!(ids.len(), 1);
        assert_eq!(distances.len(), 1);
        assert_eq!(ids[0], 0);
        assert_eq!(distances[0], 0.0);
    }

    #[test]
    fn it_can_persist_and_load() {
        let n = 1000;
        let d: usize = 960;
        let persist_path = "/Users/hammad/Documents/chroma/rust_test/"; // TODO: rust test path creation / teardown
        let space_name = "l2";
        let mut index = HnswIndex::new(space_name, n, 16, 100, 0, true, true, persist_path);
        index.init(d);
        index.set_ef(100);

        let data: Vec<f32> = utils::generate_random_data(n, d);
        let ids: Vec<usize> = (0..n).collect();

        (0..n).into_iter().for_each(|i| {
            let data = &data[i * d..(i + 1) * d];
            index.add_item(data, ids[i], false)
        });

        // Persist the index
        index.persist_dirty();

        // Load the index
        let mut index = HnswIndex::new(space_name, n, 16, 100, 0, true, true, persist_path);
        index.load(d);

        // // Query the data
        let query = &data[0..d];
        let (ids, distances) = index.knn_query(query, 1);
        assert_eq!(ids.len(), 1);
        assert_eq!(distances.len(), 1);
        assert_eq!(ids[0], 0);
        assert_eq!(distances[0], 0.0);

        // // Get the data and check it
        let mut i = 0;
        for id in ids {
            let actual_data = index.get_item(id as usize);
            assert_eq!(actual_data.len(), d);
            for j in 0..d {
                assert_eq!(actual_data[j], data[i * d + j]);
            }
            i += 1;
        }
    }
}
