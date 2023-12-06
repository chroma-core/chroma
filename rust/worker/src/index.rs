use std::ffi::CString;
use std::ffi::{c_char, c_int};

// TODO: is usize the right type for ids?

// https://doc.rust-lang.org/nomicon/ffi.html#representing-opaque-structs
#[repr(C)]
struct IndexPtrFFI {
    _data: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

// #[repr(C)]
pub struct Index {
    ptr: *const IndexPtrFFI,
    dim: usize,
    pub initialized: bool,
}

// Make index sync, we should wrap index so that it is sync in the way we expect but for now this implements the trait
unsafe impl Sync for Index {}
unsafe impl Send for Index {}

// Index impl that is public and wraps the private index extern "C" struct
impl Index {
    pub fn new(space_name: &str, dim: usize) -> Index {
        // TODO: handle unwrap panic
        // TODO: enum for spaces
        let space_name = CString::new(space_name).unwrap();
        let index = unsafe { create_index(space_name.as_ptr(), dim as c_int) };
        println!("Pointer to index: {:?}", index);
        return Index {
            ptr: index,
            dim: dim,
            initialized: false,
        };
    }

    pub fn init(
        &self, // Init will not mutate the index ptr so we can borrow it
        max_elements: usize,
        m: usize,
        ef_construction: usize,
        random_seed: usize,
        allow_replace_deleted: bool,
    ) {
        if self.initialized {
            return;
        }
        println!("Initializing index in rust");
        println!("Pointer to index: {:?}", self.ptr);
        unsafe {
            init_index(
                self.ptr,
                max_elements,
                m,
                ef_construction,
                random_seed,
                allow_replace_deleted,
            );
        }
    }

    pub fn get_ef(&self) -> i32 {
        unsafe { get_ef(self.ptr) }
    }

    pub fn set_ef(&self, ef: i32) {
        unsafe {
            set_ef(self.ptr, ef);
        }
    }

    pub fn add_item(&self, data: &[f32], id: usize, replace_deleted: bool) {
        unsafe {
            add_item(self.ptr, data.as_ptr(), id, replace_deleted);
        }
    }

    pub fn get_item(&self, id: usize) -> Vec<f32> {
        let mut data = vec![0.0f32; self.dim];
        unsafe {
            get_item(self.ptr, id, data.as_mut_ptr());
        }
        return data;
    }

    pub fn knn_query(&self, query_vector: &[f32], k: usize) -> (Vec<i32>, Vec<f32>) {
        let mut ids = vec![0i32; k];
        let mut distance = vec![0.0f32; k];
        unsafe {
            knn_query(
                self.ptr,
                query_vector.as_ptr(),
                k,
                ids.as_mut_ptr(),
                distance.as_mut_ptr(),
            );
        }
        return (ids, distance);
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
}
