use base64::{engine::general_purpose, Engine as _};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

use chroma_frontend::base64_decode::{decode_base64_embedding, decode_base64_embeddings};

fn create_test_embedding(size: usize) -> String {
    let embedding: Vec<f32> = (0..size).map(|i| (i as f32) * 0.001).collect();
    let bytes: Vec<u8> = embedding.iter().flat_map(|f| f.to_le_bytes()).collect();
    general_purpose::STANDARD.encode(&bytes)
}

fn bench_single_embedding_decode(c: &mut Criterion) {
    let test_data = create_test_embedding(1536);

    let mut group = c.benchmark_group("single_embedding_decode");
    group.throughput(Throughput::Elements(1536)); // 1536 floats per operation

    group.bench_function("1536_dim", |b| {
        b.iter(|| decode_base64_embedding(black_box(&test_data)))
    });

    group.finish();
}

fn bench_batch_decode(c: &mut Criterion) {
    let single_embedding = create_test_embedding(1536);
    let batch_sizes = [10, 100, 1000];

    let mut group = c.benchmark_group("batch_decode");

    for batch_size in batch_sizes {
        let batch_data: Vec<String> = (0..batch_size).map(|_| single_embedding.clone()).collect();

        let total_floats = batch_size * 1536;
        group.throughput(Throughput::Elements(total_floats as u64));

        group.bench_with_input(
            format!("batch_{}", batch_size),
            &batch_data,
            |b, batch_data| b.iter(|| decode_base64_embeddings(black_box(batch_data))),
        );
    }

    group.finish();
}

fn bench_large_batch_decode(c: &mut Criterion) {
    let single_embedding = create_test_embedding(1536);
    let large_batch: Vec<String> = (0..2000).map(|_| single_embedding.clone()).collect();

    let mut group = c.benchmark_group("large_batch_decode");
    group.throughput(Throughput::Elements(2000 * 1536)); // Total floats
    group.sample_size(10); // Fewer samples since each run takes ~100ms

    group.bench_function("batch_2000", |b| {
        b.iter(|| decode_base64_embeddings(black_box(&large_batch)))
    });

    group.finish();
}

fn bench_mixed_sizes(c: &mut Criterion) {
    let embedding_sizes = [128, 256, 512, 1024, 1536];
    let test_data: Vec<String> = embedding_sizes
        .iter()
        .map(|&size| create_test_embedding(size))
        .collect();

    let mut group = c.benchmark_group("mixed_sizes");

    // Test individual sizes
    for (i, &size) in embedding_sizes.iter().enumerate() {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(format!("single_{}_dim", size), &test_data[i], |b, data| {
            b.iter(|| decode_base64_embedding(black_box(data)))
        });
    }

    // Test mixed batch
    let mixed_batch: Vec<String> = test_data.iter().cycle().take(100).cloned().collect();
    let total_floats = (128 + 256 + 512 + 1024 + 1536) * 20; // 20 complete cycles
    group.throughput(Throughput::Elements(total_floats as u64));

    group.bench_function("mixed_batch_100", |b| {
        b.iter(|| decode_base64_embeddings(black_box(&mixed_batch)))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_embedding_decode,
    bench_batch_decode,
    bench_large_batch_decode,
    bench_mixed_sizes
);
criterion_main!(benches);
