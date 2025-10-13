#[cfg(test)]
mod tests {
    use crate::{
        arrow::{
            config::{BlockManagerConfig, TEST_MAX_BLOCK_SIZE_BYTES},
            provider::{ArrowBlockfileProvider, BlockfileReaderOptions},
        },
        BlockfileWriterOptions,
    };
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage, Storage};
    use rand::Rng;
    use shuttle::{future, scheduler::RandomScheduler, thread, Runner};

    #[test]
    fn test_blockfile_shuttle() {
        let mut config = shuttle::Config::default();
        config.stack_size = 1024 * 1024; // 1MB

        let scheduler = RandomScheduler::new(100);
        let runner = Runner::new(scheduler, config);

        runner.run(|| {
            let tmp_dir = tempfile::tempdir().unwrap();
            let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
            // NOTE(rescrv):  I chose to use non-persistent caches here to maximize chance of a
            // race condition outside the cache.
            let block_cache = new_cache_for_test();
            let sparse_index_cache = new_cache_for_test();
            let max_block_size_bytes = 500;
            let blockfile_provider = ArrowBlockfileProvider::new(
                storage,
                max_block_size_bytes,
                block_cache,
                sparse_index_cache,
                BlockManagerConfig::default_num_concurrent_block_flushes(),
            );
            let prefix_path = String::from("");
            let writer = future::block_on(
                blockfile_provider.write::<&str, u32>(BlockfileWriterOptions::new(prefix_path)),
            )
            .unwrap();
            let id = writer.id();
            // Generate N datapoints and then have T threads write them to the blockfile
            let range_min = 10;
            let range_max = 10000;
            let n = shuttle::rand::thread_rng().gen_range(range_min..range_max);
            // Make the max threads the number of cores * 2
            let max_threads = num_cpus::get() * 2;
            let t = shuttle::rand::thread_rng().gen_range(2..max_threads);
            println!("Writing {} keys with {} threads", n, t);
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
                let flusher = writer.commit::<&str, u32>().await.unwrap();
                flusher.flush::<&str, u32>().await.unwrap();
            });

            let read_options = BlockfileReaderOptions::new(id, "".to_string());
            let reader = future::block_on(async {
                blockfile_provider
                    .read::<&str, u32>(read_options)
                    .await
                    .unwrap()
            });
            // Read the data back
            for i in 0..n {
                let key_string = format!("key{}", i);
                let value = future::block_on(async { reader.get("", key_string.as_str()).await });
                let value = value
                    .expect("Expect key to exist and there to be no error")
                    .expect("Key should have a value");
                assert_eq!(value, i as u32);
            }
        });
    }

    #[test]
    fn test_concurrent_readers() {
        let mut config = shuttle::Config::default();
        config.stack_size = 1024 * 1024; // 1MB

        let scheduler = RandomScheduler::new(100);
        let runner = Runner::new(scheduler, config);

        runner.run(|| {
            let tmp_dir = tempfile::tempdir().unwrap();
            let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
            let block_cache = new_cache_for_test();
            let sparse_index_cache = new_cache_for_test();
            let blockfile_provider = ArrowBlockfileProvider::new(
                storage,
                TEST_MAX_BLOCK_SIZE_BYTES,
                block_cache,
                sparse_index_cache,
                BlockManagerConfig::default_num_concurrent_block_flushes(),
            );
            let prefix_path = String::from("");
            let reader = future::block_on(async {
                let writer = blockfile_provider
                    .write::<&str, u32>(BlockfileWriterOptions::new(prefix_path))
                    .await
                    .expect("Failed to create writer");
                let id = writer.id();
                writer
                    .set::<&str, u32>("", "key1", 1)
                    .await
                    .expect("Failed to set key");
                let flusher = writer.commit::<&str, Vec<u32>>().await.unwrap();
                flusher.flush::<&str, u32>().await.unwrap();

                // Clear cache.
                blockfile_provider.clear().await.expect("Clear bf provider");

                let read_options = BlockfileReaderOptions::new(id, "".to_string());
                blockfile_provider
                    .read::<&str, u32>(read_options)
                    .await
                    .unwrap()
            });
            // Make the max threads the number of cores * 2
            let max_threads = num_cpus::get() * 2;
            let t = shuttle::rand::thread_rng().gen_range(2..max_threads);
            let mut join_handles = Vec::with_capacity(t);
            for _ in 0..t {
                let reader_clone = reader.clone();
                let handle = thread::spawn(move || {
                    future::block_on(async {
                        reader_clone
                            .get("", "key1")
                            .await
                            .expect("Expected value")
                            .expect("Expected value")
                    })
                });
                join_handles.push(handle);
            }

            // No errors.
            for handle in join_handles {
                let val = handle.join().unwrap();
                assert_eq!(val, 1);
            }
        });
    }
}
