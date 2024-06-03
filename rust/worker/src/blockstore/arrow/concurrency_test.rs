#[cfg(test)]
mod tests {
    use crate::{
        blockstore::arrow::provider::ArrowBlockfileProvider,
        storage::{local::LocalStorage, Storage},
    };
    use shuttle::{check_random, future, thread};

    #[test]
    fn test_blockfile_shuttle() {
        shuttle::check_random(
            || {
                let tmp_dir = tempfile::tempdir().unwrap();
                let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
                let blockfile_provider = ArrowBlockfileProvider::new(storage);
                let writer = blockfile_provider.create::<&str, u32>().unwrap();
                let id = writer.id();

                // Generate N datapoints and then have T threads write them to the blockfile
                let n = 1000;
                let t = 5;
                let mut join_handles = Vec::with_capacity(t);
                for i in 0..t {
                    let writer = writer.clone();
                    let range_start = i * n / t;
                    let range_end = (i + 1) * n / t;
                    let handle = thread::spawn(move || {
                        for j in range_start..range_end {
                            let key_string = format!("key{}", j);
                            future::block_on(async {
                                writer
                                    .set::<&str, u32>("", key_string.as_str(), j as u32)
                                    .await
                                    .unwrap()
                            });
                        }
                    });
                    join_handles.push(handle);
                }

                for handle in join_handles {
                    handle.join().unwrap();
                }

                writer.commit::<&str, u32>().unwrap();

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
