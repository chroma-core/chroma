#[cfg(test)]
mod tests {
    use crate::{
        arrow::{config::TEST_MAX_BLOCK_SIZE_BYTES, provider::ArrowBlockfileProvider},
        cache::{
            cache::Cache,
            config::{CacheConfig, LruConfig},
        },
        storage::{local::LocalStorage, Storage},
    };
    use rand::Rng;
    use shuttle::{future, thread};

    #[test]
    fn test_blockfile_shuttle() {
        const BLOCK_CACHE_CAPACITY: usize = 1000;
        const SPARSE_INDEX_CACHE_CAPACITY: usize = 1000;
        shuttle::check_random(
            || {
                let tmp_dir = tempfile::tempdir().unwrap();
                let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
                let block_cache = Cache::new(&CacheConfig::Lru(LruConfig {
                    capacity: BLOCK_CACHE_CAPACITY,
                }));
                let sparse_index_cache = Cache::new(&CacheConfig::Lru(LruConfig {
                    capacity: SPARSE_INDEX_CACHE_CAPACITY,
                }));
                let blockfile_provider = ArrowBlockfileProvider::new(
                    storage,
                    TEST_MAX_BLOCK_SIZE_BYTES,
                    block_cache,
                    sparse_index_cache,
                );
                let writer = blockfile_provider.create::<&str, u32>().unwrap();
                let id = writer.id();
                // Generate N datapoints and then have T threads write them to the blockfile
                let range_min = 10;
                let range_max = 10000;
                let n = shuttle::rand::thread_rng().gen_range(range_min..range_max);
                // Make the max threads the number of cores * 2
                let max_threads = num_cpus::get() * 2;
                let t = shuttle::rand::thread_rng().gen_range(2..max_threads);
                let mut join_handles = Vec::with_capacity(t);
                for i in 0..t {
                    let range_start = i * n / t;
                    let range_end = (i + 1) * n / t;
                    let writer = writer.clone();
                    let handle = thread::spawn(move || {
                        for j in range_start..range_end {
                            let key_string = format!("key{}", j);
                            future::block_on(async {
                                writer
                                    .set::<&str, u32>("", key_string.as_str(), j as u32)
                                    .await
                                    .unwrap();
                            });
                        }
                    });
                    join_handles.push(handle);
                }

                for handle in join_handles {
                    handle.join().unwrap();
                }

                // commit the writer
                future::block_on(async {
                    let flusher = writer.commit::<&str, u32>().unwrap();
                    flusher.flush::<&str, u32>().await.unwrap();
                });

                let reader = future::block_on(async {
                    blockfile_provider.open::<&str, u32>(&id).await.unwrap()
                });
                // Read the data back
                for i in 0..n {
                    let key_string = format!("key{}", i);
                    let value =
                        future::block_on(async { reader.get("", key_string.as_str()).await });
                    let value = value.expect("Expect key to exist and there to be no error");
                    assert_eq!(value, i as u32);
                }
            },
            100,
        );
    }
}
