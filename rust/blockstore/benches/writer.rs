use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, BlockfileWriterOptions};
use chroma_cache::UnboundedCacheConfig;
use chroma_storage::{local::LocalStorage, Storage};
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{thread_rng, Rng};
use uuid::Uuid;

// Ends up creating ~1.1MB:
// prefix: 3char * 100_000
// key:    (size_of(u32) * 100_000)
// value:  (size_of(u32) * 100_000)
const NUM_KV_PAIRS: usize = 100_000;
const BLOCK_SIZE: usize = 1024 * 256; // 256KB, so there should be 5 blocks

fn generate_trigram_kv_pairs() -> Vec<((String, u32), u32)> {
    let mut pairs = Vec::with_capacity(NUM_KV_PAIRS);
    let mut rng = thread_rng();

    for _ in 0..NUM_KV_PAIRS {
        let key = (0..3)
            .map(|_| {
                let ascii_code = rng.gen_range(32..127) as u8;
                ascii_code as char
            })
            .collect::<String>();
        let value = rng.gen::<u32>();
        pairs.push(((key, value), value));
    }

    pairs
}

async fn create_populated_blockfile(provider: &ArrowBlockfileProvider) -> Uuid {
    let writer = provider
        .write::<u32, u32>(BlockfileWriterOptions::new().unordered_mutations())
        .await
        .unwrap();
    let id = writer.id();

    for (key, value) in generate_trigram_kv_pairs() {
        writer.set(&key.0, key.1, value).await.unwrap();
    }

    let flusher = writer.commit::<u32, u32>().await.unwrap();
    flusher.flush::<u32, u32>().await.unwrap();
    id
}

/// This benchmark compares the performance of UnorderedBlockfileWriter and OrderedBlockfileWriter.
pub fn benchmark(c: &mut Criterion) {
    let runner = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create runtime");

    let data = generate_trigram_kv_pairs();
    let mut sorted_data = data.clone();
    sorted_data.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let tmp_dir = tempfile::tempdir().unwrap();
    let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
    let block_cache = Box::new(UnboundedCacheConfig {}.build()) as _;
    let sparse_index_cache = Box::new(UnboundedCacheConfig {}.build()) as _;
    let arrow_blockfile_provider =
        ArrowBlockfileProvider::new(storage.clone(), BLOCK_SIZE, block_cache, sparse_index_cache);

    {
        let mut new_writer = c.benchmark_group("new writer");
        new_writer.bench_function("UnorderedBlockfileWriter", |b| {
            b.to_async(&runner).iter_with_large_drop(|| async {
                let writer = arrow_blockfile_provider
                    .write::<u32, u32>(BlockfileWriterOptions::new().unordered_mutations())
                    .await
                    .unwrap();
                for (key, value) in data.iter() {
                    writer.set(&key.0, key.1, *value).await.unwrap();
                }
                writer.commit::<u32, u32>().await.unwrap();
            });
        });

        new_writer.bench_function("OrderedBlockfileWriter", |b| {
            b.to_async(&runner).iter_with_large_drop(|| async {
                let writer = arrow_blockfile_provider
                    .write::<u32, u32>(BlockfileWriterOptions::new().ordered_mutations())
                    .await
                    .unwrap();
                for (key, value) in sorted_data.iter() {
                    writer.set(&key.0, key.1, *value).await.unwrap();
                }
                writer.commit::<u32, u32>().await.unwrap();
            });
        });
    }

    {
        let populated_blockfile_id =
            runner.block_on(create_populated_blockfile(&arrow_blockfile_provider));
        let mut forked_writer = c.benchmark_group("forked writer");
        forked_writer.bench_function("UnorderedBlockfileWriter", |b| {
            b.to_async(&runner).iter_with_large_drop(|| async {
                let writer = arrow_blockfile_provider
                    .write::<u32, u32>(
                        BlockfileWriterOptions::new()
                            .unordered_mutations()
                            .fork(populated_blockfile_id),
                    )
                    .await
                    .unwrap();
                for (key, value) in data.iter() {
                    writer.set(&key.0, key.1, *value).await.unwrap();
                }
                writer.commit::<u32, u32>().await.unwrap();
            });
        });

        forked_writer.bench_function("OrderedBlockfileWriter", |b| {
            b.to_async(&runner).iter_with_large_drop(|| async {
                let writer = arrow_blockfile_provider
                    .write::<u32, u32>(
                        BlockfileWriterOptions::new()
                            .ordered_mutations()
                            .fork(populated_blockfile_id),
                    )
                    .await
                    .unwrap();
                for (key, value) in sorted_data.iter() {
                    writer.set(&key.0, key.1, *value).await.unwrap();
                }
                writer.commit::<u32, u32>().await.unwrap();
            });
        });
    }
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
