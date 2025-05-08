use chroma_cache::{FoyerCacheConfig, Weighted};
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
            ..Default::default()
        }))
        .await
        .expect("Should be able to create cache");

    let data_size_mb = 8;
    let data = VecValue(vec![0u8; data_size_mb * 1024 * 1024]); // 8 MB

    // cc.insert("test_key".to_string(), data).await;
    let start_time = std::time::Instant::now();
    cc.insert_to_disk("test_key".to_string(), data.clone())
        .await;
    println!("fetching one key took {:?}", start_time.elapsed());

    println!("Beginning to insert 64 objects");
    let mut keys = Vec::new();
    let start_time = std::time::Instant::now();
    let num_objects = 64;
    for _ in 0..num_objects {
        let data = VecValue(vec![0u8; data_size_mb * 1024 * 1024]);
        let uuid = uuid::Uuid::new_v4();
        keys.push(uuid.to_string());
        cc.insert_to_disk(uuid.to_string(), data).await;
    }
    println!("Inserting 64 objects took {:?}", start_time.elapsed());

    // sleep to let disk I/O finish
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Fetch all 64 from disk in parallel
    let start_time = std::time::Instant::now();
    let mut futures = Vec::new();
    for key in keys {
        let future = cc.obtain(key);
        futures.push(future);
    }
    let results = futures::future::join_all(futures).await;
    let mut not_found_count = 0;
    println!("Fetching 64 objects took {:?}", start_time.elapsed());
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
        "Finished fetching 64 objects, {} not found",
        not_found_count
    );
}
