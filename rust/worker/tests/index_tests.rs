use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use worker::index::Index;

mod utils;

#[test]
fn it_initializes_and_can_set_ef() {
    let n = 1000;
    let d: usize = 960;
    let space_name = "ip";
    let index = Index::new(space_name, d);
    index.init(n, 16, 100, 0, true);
    assert_eq!(index.get_ef(), 10);
    index.set_ef(100);
    assert_eq!(index.get_ef(), 100);
}

#[test]
fn it_can_add_parallel() {
    let n = 1000;
    let d: usize = 960;
    let space_name = "ip";
    let index = Index::new(space_name, d);
    index.init(n, 16, 100, 0, true);

    let data = utils::generate_random_data(n, d);
    let ids: Vec<usize> = (0..n).collect();

    // Add data in parallel, using global pool for testing
    ThreadPoolBuilder::new()
        .num_threads(12)
        .build_global()
        .unwrap();

    (0..n).into_par_iter().for_each(|i| {
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
}

#[test]
fn it_can_add_and_basic_query() {
    let n = 1000;
    let d: usize = 960;
    let space_name = "l2";
    let index = Index::new(space_name, d);
    index.init(n, 16, 100, 0, true);
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
