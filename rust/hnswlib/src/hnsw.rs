use std::ffi::{c_char, c_int};

// https://doc.rust-lang.org/nomicon/ffi.html#representing-opaque-structs
#[repr(C)]
pub struct HnswIndexPtrFFI {
    _data: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

#[link(name = "bindings", kind = "static")]
extern "C" {
    pub fn create_index(space_name: *const c_char, dim: c_int) -> *const HnswIndexPtrFFI;

    pub fn free_index(index: *const HnswIndexPtrFFI);

    pub fn init_index(
        index: *const HnswIndexPtrFFI,
        max_elements: usize,
        M: usize,
        ef_construction: usize,
        random_seed: usize,
        allow_replace_deleted: bool,
        is_persistent: bool,
        path: *const c_char,
    );

    pub fn load_index(
        index: *const HnswIndexPtrFFI,
        path: *const c_char,
        allow_replace_deleted: bool,
        is_persistent_index: bool,
        max_elements: usize,
    );

    pub fn persist_dirty(index: *const HnswIndexPtrFFI);

    pub fn add_item(
        index: *const HnswIndexPtrFFI,
        data: *const f32,
        id: usize,
        replace_deleted: bool,
    );
    pub fn mark_deleted(index: *const HnswIndexPtrFFI, id: usize);
    pub fn get_item(index: *const HnswIndexPtrFFI, id: usize, data: *mut f32);
    pub fn get_all_ids_sizes(index: *const HnswIndexPtrFFI, sizes: *mut usize);
    pub fn get_all_ids(
        index: *const HnswIndexPtrFFI,
        non_deleted_ids: *mut usize,
        deleted_ids: *mut usize,
    );
    pub fn knn_query(
        index: *const HnswIndexPtrFFI,
        query_vector: *const f32,
        k: usize,
        ids: *mut usize,
        distance: *mut f32,
        allowed_ids: *const usize,
        allowed_ids_length: usize,
        disallowed_ids: *const usize,
        disallowed_ids_length: usize,
    ) -> c_int;
    pub fn open_fd(index: *const HnswIndexPtrFFI);
    pub fn close_fd(index: *const HnswIndexPtrFFI);
    pub fn get_ef(index: *const HnswIndexPtrFFI) -> c_int;
    pub fn set_ef(index: *const HnswIndexPtrFFI, ef: c_int);
    pub fn len(index: *const HnswIndexPtrFFI) -> c_int;
    pub fn len_with_deleted(index: *const HnswIndexPtrFFI) -> c_int;
    pub fn capacity(index: *const HnswIndexPtrFFI) -> c_int;
    pub fn resize_index(index: *const HnswIndexPtrFFI, new_size: usize);
    pub fn get_last_error(index: *const HnswIndexPtrFFI) -> *const c_char;
}
