use std::ffi::CString;
use std::ffi::{c_char, c_int};

// https://doc.rust-lang.org/nomicon/ffi.html#representing-opaque-structs
#[repr(C)]
pub struct Index {
    _data: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

#[link(name = "bindings", kind = "static")]
extern "C" {
    fn create_index(space_name: *const c_char, dim: c_int) -> *mut Index;
    //     void init_index(Index<float> *index, size_t max_elements, size_t M, size_t ef_construction, size_t random_seed, bool allow_replace_deleted)
    fn init_index(
        index: *mut Index,
        max_elements: usize,
        M: usize,
        ef_construction: usize,
        random_seed: usize,
        allow_replace_deleted: bool,
    );
    fn get_ef(index: *mut Index) -> c_int;
}

fn main() {
    let space_name = CString::new("l2").unwrap();
    let dim = 128;
    let index = unsafe { create_index(space_name.as_ptr(), dim) };
    println!("Hello, world!");
    unsafe {
        init_index(index, 100, 16, 100, 0, true);
    }
    println!("ef: {}", unsafe { get_ef(index) });
}
