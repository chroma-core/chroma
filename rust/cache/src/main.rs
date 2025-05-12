use chroma_cache::{FoyerCacheConfig, Weighted};
use futures::future;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VecValue(Vec<u8>);

impl Weighted for VecValue {
    fn weight(&self) -> usize {
        self.0.len()
    }
}

#[tokio::main]
async fn main() {
    let cc =
        chroma_cache::from_config_persistent(&chroma_cache::CacheConfig::Disk(FoyerCacheConfig {
            dir: Some("./cache/hammad_testing".to_string()),
            capacity: 7000,  //
            mem: 52488,      // 56 GiB
            disk: 10 * 1024, // 10 * 1024 Mib = 10 GiB
            file_size: 256,
            // buffer_pool: 1000,
            admission_rate_limit: 512,
            ..Default::default()
        }))
        .await
        .expect("Should be able to create cache");

    let data_size_mb = 1.0;
    let data_in_bytes = (data_size_mb * 1024.0 * 1024.0) as usize;
    let data = VecValue(vec![0u8; data_in_bytes]);
    let num_objects = 64;

    let start_time = std::time::Instant::now();
    cc.insert_to_disk("test_key".to_string(), data.clone())
        .await;
    println!("inserting one key took {:?}", start_time.elapsed());

    // sleep to let disk I/O finish
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // fetch one key
    let start_time = std::time::Instant::now();
    let _result = cc
        .obtain("test_key".to_string())
        .await
        .expect("Should be able to fetch")
        .expect("Key should exist");
    println!("Fetching one object took {:?}", start_time.elapsed());

    println!("Beginning to insert {} objects", num_objects);
    let mut keys = Vec::new();
    let start_time = std::time::Instant::now();
    for _ in 0..num_objects {
        let data = VecValue(vec![0u8; data_in_bytes]);
        let uuid = uuid::Uuid::new_v4();
        keys.push(uuid.to_string());
        cc.insert_to_disk(uuid.to_string(), data).await;
    }
    println!(
        "Inserting {} objects took {:?}",
        num_objects,
        start_time.elapsed()
    );
    // sleep to let disk I/O finish
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Fetch all entries from disk in parallel
    let mut futures = Vec::new();
    for key in keys {
        let future = cc.obtain(key);
        futures.push(future);
    }
    let start_time = std::time::Instant::now();
    let results = futures::future::join_all(futures).await;
    let mut not_found_count = 0;
    println!(
        "Fetching {} objects took {:?}",
        num_objects,
        start_time.elapsed()
    );
    for result in results {
        match result {
            Ok(Some(_)) => {}
            Ok(None) => {
                not_found_count += 1;
                println!("Key not found");
            }
            Err(err) => println!("Error: {:?}", err),
        }
    }
    println!(
        "Finished fetching {} objects, {} not found",
        num_objects, not_found_count
    );

    // Attempt to fetch a non-existent key, in parallel
    let mut futures = Vec::new();
    let num_non_existent_fetches = 9000;
    for i in 0..num_non_existent_fetches {
        let future = cc.obtain(format!("non_existent_key_{}", i));
        // let future = async {
        //     let x = 5 + 10;
        // };
        futures.push(future);
    }
    let start_time = std::time::Instant::now();
    let _results = futures::future::join_all(futures).await;
    println!(
        "Fetching {} non-existent objects took {:?}",
        num_non_existent_fetches,
        start_time.elapsed()
    );
}
