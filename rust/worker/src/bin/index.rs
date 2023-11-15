use rand::Rng;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::ffi::CString;
use std::ffi::{c_char, c_int};

// https://doc.rust-lang.org/nomicon/ffi.html#representing-opaque-structs
#[repr(C)]
pub struct Index {
    _data: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

#[repr(C)]
pub struct IndexPtr {
    pub ptr: *mut Index,
}

#[repr(C)]
pub struct DataPtrWrapper {
    pub ptr: *mut f32,
}

// Make index sync, we should wrap index so that it is sync in the way we expect but for now this implements the trait
unsafe impl Sync for IndexPtr {}
unsafe impl Sync for DataPtrWrapper {}

#[link(name = "bindings", kind = "static")]
extern "C" {
    fn create_index(space_name: *const c_char, dim: c_int) -> *mut Index;

    fn init_index(
        index: *mut Index,
        max_elements: usize,
        M: usize,
        ef_construction: usize,
        random_seed: usize,
        allow_replace_deleted: bool,
    );

    fn add_item(index: &IndexPtr, data: &DataPtrWrapper, id: usize, replace_deleted: bool);

    fn knn_query(
        index: *mut Index,
        query_vector: *mut f32,
        k: usize,
        ids: *mut i32,
        distance: *mut f32,
    );

    fn get_ef(index: *mut Index) -> c_int;
}

fn main() {
    let N = 1000;
    let D = 960;
    let space_name = CString::new("ip").unwrap();
    let index = unsafe { create_index(space_name.as_ptr(), D) };
    println!("Hello, world!");
    unsafe {
        init_index(index, N, 16, 100, 0, true);
    }
    println!("ef: {}", unsafe { get_ef(index) });

    // simple test
    // add some data
    // let data = vec![1.0f32, 2.0f32, 3.0f32];
    // let data_ptr = data.as_ptr() as *mut f32;
    // unsafe {
    //     add_item(index, data_ptr, 1, false);
    //     add_item(index, data_ptr, 2, false);
    // }

    // // query for it
    // let k = 2;
    // let mut ids = vec![0; k];
    // let mut distance = vec![0.0f32; k];
    // let ids_ptr = ids.as_mut_ptr();
    // let distance_ptr = distance.as_mut_ptr();
    // unsafe {
    //     knn_query(index, data_ptr, k, ids_ptr, distance_ptr);
    // }
    // println!("ids: {:?}", ids);

    let N = 1000;
    let D = 960;
    let mut rng: rand::prelude::ThreadRng = rand::thread_rng();

    let mut data = vec![0.0f32; N * D];
    // Generate random data
    println!("Generating data");
    for i in 0..N {
        for j in 0..D {
            data[i * D + j] = rng.gen();
        }
        if i % 1000 == 0 {
            println!("Generated: {}", i);
        }
    }
    let data_ptr = data.as_mut_ptr();
    println!("Done generating data");

    // Generate ids sequentially
    let mut ids = vec![0usize; N];
    for i in 0..N {
        ids[i] = i;
    }

    // Add data in parallel, using global pool for testing
    ThreadPoolBuilder::new()
        .num_threads(12)
        .build_global()
        .unwrap();

    let index_ptr_wrapper = IndexPtr { ptr: index };
    let dpw: DataPtrWrapper = DataPtrWrapper { ptr: data_ptr };

    // time this loop
    let start = std::time::Instant::now();
    (0..N).into_par_iter().for_each(|i| unsafe {
        let data = &data[i * D..(i + 1) * D];
        if i % 1000 == 0 {
            println!("Writing: {}", i);
            // Print the 0th data of each 1000th id
            let val = data[0];
            println!(
                "data in rust for id: {}, val: {} with length {}",
                i,
                val,
                data.len()
            );
            // print thread id
            println!("Elapsed: {:?}", start.elapsed());
        }
        // This is uncessary, but here we borrow data as a slice and then get a pointer to the first element
        // we can clean this up now that me and the borrow checker are friends
        let dpwi = DataPtrWrapper {
            ptr: data.as_ptr() as *mut f32,
        };
        add_item(&index_ptr_wrapper, &dpwi, ids[i], false);
    });
    let elapsed = start.elapsed();
    println!("time: {:?}", elapsed);

    // for i in 0..N {
    //     // print 0th data
    //     if i == 0 {
    //         println!("data: {:?}", &data[0..D]);
    //     }
    //     unsafe {
    //         add_item(&index_ptr_wrapper, &dpw, ids[i], false);
    //     }
    // }

    // Query for the 10th vector
    let k = 10;
    let mut query = vec![0.0f32; D];
    for i in 0..D {
        query[i] = data[10 * D + i];
    }

    let mut query_ids = vec![0i32; k];
    let mut query_distance = vec![0.0f32; k];

    let query_ptr = query.as_mut_ptr();
    let query_ids_ptr = query_ids.as_mut_ptr();

    unsafe {
        knn_query(
            index,
            query_ptr,
            k,
            query_ids_ptr,
            query_distance.as_mut_ptr(),
        );
    }
    println!("ids: {:?}", query_ids);
}
