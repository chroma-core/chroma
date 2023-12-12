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
    let mut index = Index::new(space_name, n, 16, 100, 0, true, false, "");
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
    let mut index = Index::new(space_name, n, 16, 100, 0, true, false, "");
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
    let mut index = Index::new(space_name, n, 16, 100, 0, true, false, "");
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
    let mut index = Index::new(space_name, n, 16, 100, 0, true, true, persist_path);
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
    let mut index = Index::new(space_name, n, 16, 100, 0, true, true, persist_path);
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
