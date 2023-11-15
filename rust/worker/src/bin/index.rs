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
    fn get_ef(index: *mut Index) -> c_int;
}

fn main() {
    let space_name = CString::new("l2").unwrap();
    let dim = 128;
    let index = unsafe { create_index(space_name.as_ptr(), dim) };
    println!("Hello, world!");
    println!("ef: {}", unsafe { get_ef(index) });
}
