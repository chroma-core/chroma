//! Criterion benchmarks comparing blockfile implementations:
//! - Raw DashMap (baseline)
//! - DashMap Blockfile (new concurrent in-memory implementation)
//! - Memory Blockfile (existing in-memory implementation with RwLock)
//! - Arrow Blockfile (production implementation with local storage)
//!
//! All data is pre-generated before benchmarks run. Only insert time is measured.
//! Data is cycled through and cloned on each iteration (cloning is not timed).
//!
//! Throughput is reported as elements/second where each element is one set() operation.

use ahash::RandomState;
use chroma_blockstore::arrow::provider::ArrowBlockfileProvider;
use chroma_blockstore::dashmap::{
    reader_writer::DashMapBlockfileWriter, storage::StorageManager as DashMapStorageManager,
};
use chroma_blockstore::memory::{
    reader_writer::MemoryBlockfileWriter, storage::StorageManager as MemoryStorageManager,
};
use chroma_blockstore::BlockfileWriterOptions;
use chroma_storage::local::LocalStorage;
use chroma_storage::Storage;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dashmap::DashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;

const NUM_ENTRIES: usize = 10_000;
const POOL_SIZE: usize = 100;

/// Generate blockfile data
fn generate_blockfile_data(num_entries: usize) -> Vec<(String, String, String)> {
    (0..num_entries)
        .map(|i| {
            (
                format!("p{}", i % 4),
                format!("key_{:06}", i),
                format!("value_{:06}", i),
            )
        })
        .collect()
}

/// Chunk data into N parts
fn chunk_data(
    data: &[(String, String, String)],
    num_chunks: usize,
) -> Vec<Vec<(String, String, String)>> {
    let chunk_size = (data.len() + num_chunks - 1) / num_chunks;
    data.chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}

// ============ Baseline: Raw DashMap performance ============

fn bench_baseline_dashmap(c: &mut Criterion) {
    let mut group = c.benchmark_group("baseline_raw_dashmap");
    group.sample_size(20);
    group.throughput(Throughput::Elements(NUM_ENTRIES as u64));

    // Single-threaded: uses references, same data reused
    let data: Vec<(String, String)> = (0..NUM_ENTRIES)
        .map(|i| (format!("key_{:06}", i), format!("value_{:06}", i)))
        .collect();

    group.bench_function("single_thread", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map: DashMap<&str, &str, RandomState> =
                    DashMap::with_capacity_and_hasher(NUM_ENTRIES, RandomState::new());

                let start = std::time::Instant::now();
                for (k, v) in &data {
                    map.insert(k.as_str(), v.as_str());
                }
                total += start.elapsed();

                black_box(&map);
            }
            total
        });
    });

    // Concurrent: uses references
    for num_threads in [1, 2, 4, 8] {
        let chunk_size = (data.len() + num_threads - 1) / num_threads;
        let chunks: Vec<&[(String, String)]> = data.chunks(chunk_size).collect();

        group.bench_with_input(
            BenchmarkId::new("concurrent", num_threads),
            &chunks,
            |b, chunks| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let map: Arc<DashMap<&str, &str, RandomState>> = Arc::new(
                            DashMap::with_capacity_and_hasher(NUM_ENTRIES, RandomState::new()),
                        );

                        let start = std::time::Instant::now();
                        thread::scope(|s| {
                            for chunk in chunks.iter() {
                                let map = &map;
                                s.spawn(move || {
                                    for (k, v) in *chunk {
                                        map.insert(k.as_str(), v.as_str());
                                    }
                                });
                            }
                        });
                        total += start.elapsed();

                        black_box(&map);
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

// ============ DashMap Blockfile ============

fn bench_dashmap_blockfile(c: &mut Criterion) {
    let mut group = c.benchmark_group("dashmap_blockfile");
    group.sample_size(20);
    group.throughput(Throughput::Elements(NUM_ENTRIES as u64));

    // Pre-generate data pool
    let data_pool: Vec<Vec<(String, String, String)>> = (0..POOL_SIZE)
        .map(|_| generate_blockfile_data(NUM_ENTRIES))
        .collect();

    // Single-threaded
    group.bench_function("single_thread", |b| {
        let mut iter_idx = 0usize;
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                // Clone from pool (not timed)
                let data = data_pool[iter_idx % POOL_SIZE].clone();
                iter_idx += 1;

                let storage = DashMapStorageManager::new();
                let writer = DashMapBlockfileWriter::new(storage);

                let start = std::time::Instant::now();
                for (prefix, key, value) in data {
                    writer.set(&prefix, key.as_str(), value).unwrap();
                }
                total += start.elapsed();

                black_box(&writer);
            }
            total
        });
    });

    // Concurrent
    for num_threads in [1, 2, 4, 8] {
        // Pre-generate chunked data pool
        let chunk_pool: Vec<Vec<Vec<(String, String, String)>>> = (0..POOL_SIZE)
            .map(|_| {
                let data = generate_blockfile_data(NUM_ENTRIES);
                chunk_data(&data, num_threads)
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("concurrent", num_threads),
            &chunk_pool,
            |b, chunk_pool| {
                let mut iter_idx = 0usize;
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // Clone from pool (not timed)
                        let chunks = chunk_pool[iter_idx % POOL_SIZE].clone();
                        iter_idx += 1;

                        let storage = DashMapStorageManager::new();
                        let writer = DashMapBlockfileWriter::new(storage);

                        let start = std::time::Instant::now();
                        thread::scope(|s| {
                            for chunk in chunks {
                                let writer = &writer;
                                s.spawn(move || {
                                    for (prefix, key, value) in chunk {
                                        writer.set(&prefix, key.as_str(), value).unwrap();
                                    }
                                });
                            }
                        });
                        total += start.elapsed();

                        black_box(&writer);
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

// ============ Memory Blockfile ============

fn bench_memory_blockfile(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_blockfile");
    group.sample_size(20);
    group.throughput(Throughput::Elements(NUM_ENTRIES as u64));

    // Pre-generate data pool
    let data_pool: Vec<Vec<(String, String, String)>> = (0..POOL_SIZE)
        .map(|_| generate_blockfile_data(NUM_ENTRIES))
        .collect();

    // Single-threaded
    group.bench_function("single_thread", |b| {
        let mut iter_idx = 0usize;
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                // Clone from pool (not timed)
                let data = data_pool[iter_idx % POOL_SIZE].clone();
                iter_idx += 1;

                let storage = MemoryStorageManager::new();
                let writer = MemoryBlockfileWriter::new(storage);

                let start = std::time::Instant::now();
                for (prefix, key, value) in data {
                    writer.set(&prefix, key.as_str(), value).unwrap();
                }
                total += start.elapsed();

                black_box(&writer);
            }
            total
        });
    });

    // Concurrent
    for num_threads in [1, 2, 4, 8] {
        // Pre-generate chunked data pool
        let chunk_pool: Vec<Vec<Vec<(String, String, String)>>> = (0..POOL_SIZE)
            .map(|_| {
                let data = generate_blockfile_data(NUM_ENTRIES);
                chunk_data(&data, num_threads)
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("concurrent", num_threads),
            &chunk_pool,
            |b, chunk_pool| {
                let mut iter_idx = 0usize;
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // Clone from pool (not timed)
                        let chunks = chunk_pool[iter_idx % POOL_SIZE].clone();
                        iter_idx += 1;

                        let storage = MemoryStorageManager::new();
                        let writer = MemoryBlockfileWriter::new(storage);

                        let start = std::time::Instant::now();
                        thread::scope(|s| {
                            for chunk in chunks {
                                let writer = &writer;
                                s.spawn(move || {
                                    for (prefix, key, value) in chunk {
                                        writer.set(&prefix, key.as_str(), value).unwrap();
                                    }
                                });
                            }
                        });
                        total += start.elapsed();

                        black_box(&writer);
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

// ============ Arrow Blockfile ============

fn bench_arrow_blockfile(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_blockfile");
    group.sample_size(20);
    group.throughput(Throughput::Elements(NUM_ENTRIES as u64));

    // Create a multi-threaded runtime for async operations
    let rt = Runtime::new().unwrap();

    // Pre-generate data pool
    let data_pool: Vec<Vec<(String, String, String)>> = (0..POOL_SIZE)
        .map(|_| generate_blockfile_data(NUM_ENTRIES))
        .collect();

    // Single-threaded
    group.bench_function("single_thread", |b| {
        let mut iter_idx = 0usize;
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                // Clone from pool (not timed)
                let data = data_pool[iter_idx % POOL_SIZE].clone();
                iter_idx += 1;

                // Setup Arrow provider with tempdir (not timed)
                let tmp_dir = tempfile::tempdir().unwrap();
                let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
                let provider = rt.block_on(async {
                    ArrowBlockfileProvider::new(
                        storage,
                        2 * 1024 * 1024, // 2 MB max block size
                        chroma_cache::from_config_persistent(&chroma_cache::CacheConfig::Nop)
                            .await
                            .unwrap(),
                        chroma_cache::from_config_persistent(&chroma_cache::CacheConfig::Nop)
                            .await
                            .unwrap(),
                        1,
                    )
                });

                let writer = rt.block_on(async {
                    provider
                        .write::<&str, String>(BlockfileWriterOptions::new(String::new()))
                        .await
                        .unwrap()
                });

                let start = std::time::Instant::now();
                rt.block_on(async {
                    for (prefix, key, value) in data {
                        writer.set(&prefix, key.as_str(), value).await.unwrap();
                    }
                });
                total += start.elapsed();

                black_box(&writer);
            }
            total
        });
    });

    // Concurrent using tokio tasks
    for num_tasks in [1, 2, 4, 8] {
        // Pre-generate chunked data pool
        let chunk_pool: Vec<Vec<Vec<(String, String, String)>>> = (0..POOL_SIZE)
            .map(|_| {
                let data = generate_blockfile_data(NUM_ENTRIES);
                chunk_data(&data, num_tasks)
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("concurrent", num_tasks),
            &chunk_pool,
            |b, chunk_pool| {
                let mut iter_idx = 0usize;
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // Clone from pool (not timed)
                        let chunks = chunk_pool[iter_idx % POOL_SIZE].clone();
                        iter_idx += 1;

                        // Setup Arrow provider with tempdir (not timed)
                        let tmp_dir = tempfile::tempdir().unwrap();
                        let storage =
                            Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
                        let provider = rt.block_on(async {
                            ArrowBlockfileProvider::new(
                                storage,
                                2 * 1024 * 1024,
                                chroma_cache::from_config_persistent(
                                    &chroma_cache::CacheConfig::Nop,
                                )
                                .await
                                .unwrap(),
                                chroma_cache::from_config_persistent(
                                    &chroma_cache::CacheConfig::Nop,
                                )
                                .await
                                .unwrap(),
                                1,
                            )
                        });

                        let writer = Arc::new(rt.block_on(async {
                            provider
                                .write::<&str, String>(BlockfileWriterOptions::new(String::new()))
                                .await
                                .unwrap()
                        }));

                        let start = std::time::Instant::now();
                        rt.block_on(async {
                            let mut handles = Vec::with_capacity(chunks.len());
                            for chunk in chunks {
                                let writer = Arc::clone(&writer);
                                handles.push(tokio::spawn(async move {
                                    for (prefix, key, value) in chunk {
                                        writer.set(&prefix, key.as_str(), value).await.unwrap();
                                    }
                                }));
                            }
                            for handle in handles {
                                handle.await.unwrap();
                            }
                        });
                        total += start.elapsed();

                        black_box(&writer);
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_baseline_dashmap,
    bench_dashmap_blockfile,
    bench_memory_blockfile,
    bench_arrow_blockfile,
);

criterion_main!(benches);
