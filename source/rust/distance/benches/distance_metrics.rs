use chroma_distance::DistanceFunction;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;

fn distance_metrics(c: &mut Criterion) {
    c.bench_function("distance_metrics", |b| {
        let mut x: Vec<f32> = Vec::with_capacity(786);
        for _ in 0..x.capacity() {
            x.push(rand::random());
        }
        let mut y: Vec<f32> = Vec::with_capacity(786);
        for _ in 0..y.capacity() {
            y.push(rand::random());
        }
        b.iter(|| {
            let d = DistanceFunction::Cosine;
            std::hint::black_box(DistanceFunction::distance(&d, &x, &y));
        });
    });
}

#[allow(dead_code)]
fn generate_random_vectors(size: usize) -> (Vec<f32>, Vec<f32>) {
    let mut rng = rand::thread_rng();
    let x: Vec<f32> = (0..size).map(|_| rng.gen_range(-10.0..10.0)).collect();
    let y: Vec<f32> = (0..size).map(|_| rng.gen_range(-10.0..10.0)).collect();
    (x, y)
}

#[cfg(all(target_feature = "avx", target_feature = "fma"))]
fn bench_avx_cosine_distance(c: &mut Criterion) {
    let (x, y) = generate_random_vectors(1024);
    c.bench_function("avx_cosine_distance", |b| {
        b.iter(|| unsafe { chroma_distance::distance_avx::cosine_distance(&x, &y) });
    });
}

#[cfg(all(
    target_feature = "avx512f",
    target_feature = "avx512dq",
    target_feature = "avx512bw",
    target_feature = "avx512vl",
    target_feature = "fma"
))]
fn bench_avx512_cosine_distance(c: &mut Criterion) {
    let (x, y) = generate_random_vectors(1024);
    c.bench_function("avx512_cosine_distance", |b| {
        b.iter(|| unsafe { chroma_distance::distance_avx512::cosine_distance(&x, &y) });
    });
}

#[cfg(all(target_feature = "avx", target_feature = "fma"))]
fn bench_avx_inner_product(c: &mut Criterion) {
    let (x, y) = generate_random_vectors(1024);
    c.bench_function("avx_inner_product", |b| {
        b.iter(|| unsafe { chroma_distance::distance_avx::inner_product(&x, &y) });
    });
}

#[cfg(all(
    target_feature = "avx512f",
    target_feature = "avx512dq",
    target_feature = "avx512bw",
    target_feature = "avx512vl",
    target_feature = "fma"
))]
fn bench_avx512_inner_product(c: &mut Criterion) {
    let (x, y) = generate_random_vectors(1024);
    c.bench_function("avx512_inner_product", |b| {
        b.iter(|| unsafe { chroma_distance::distance_avx512::inner_product(&x, &y) });
    });
}

#[cfg(all(target_feature = "avx", target_feature = "fma"))]
fn bench_avx_euclidean_distance(c: &mut Criterion) {
    let (x, y) = generate_random_vectors(1024);
    c.bench_function("avx_euclidean_distance", |b| {
        b.iter(|| unsafe { chroma_distance::distance_avx::euclidean_distance(&x, &y) });
    });
}

#[cfg(all(
    target_feature = "avx512f",
    target_feature = "avx512dq",
    target_feature = "avx512bw",
    target_feature = "avx512vl",
    target_feature = "fma"
))]
fn bench_avx512_euclidean_distance(c: &mut Criterion) {
    let (x, y) = generate_random_vectors(1024);
    c.bench_function("avx512_euclidean_distance", |b| {
        b.iter(|| unsafe { chroma_distance::distance_avx512::euclidean_distance(&x, &y) });
    });
}

// Benchmark different vector sizes
#[cfg(all(target_feature = "avx", target_feature = "fma"))]
fn bench_avx_different_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("avx_different_sizes");

    for size in [256, 512, 1024, 2048, 4096] {
        let (x, y) = generate_random_vectors(size);
        group.bench_function(&format!("cosine_{}", size), |b| {
            b.iter(|| unsafe { chroma_distance::distance_avx::cosine_distance(&x, &y) });
        });
    }
    group.finish();
}

#[cfg(all(
    target_feature = "avx512f",
    target_feature = "avx512dq",
    target_feature = "avx512bw",
    target_feature = "avx512vl",
    target_feature = "fma"
))]
fn bench_avx512_different_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("avx512_different_sizes");

    for size in [256, 512, 1024, 2048, 4096] {
        let (x, y) = generate_random_vectors(size);
        group.bench_function(&format!("cosine_{}", size), |b| {
            b.iter(|| unsafe { chroma_distance::distance_avx512::cosine_distance(&x, &y) });
        });
    }
    group.finish();
}

// Configure benchmark groups
fn configure_benches(c: &mut Criterion) {
    // Original benchmark
    distance_metrics(c);

    // AVX benchmarks
    #[cfg(all(target_feature = "avx", target_feature = "fma"))]
    {
        bench_avx_cosine_distance(c);
        bench_avx_inner_product(c);
        bench_avx_euclidean_distance(c);
        bench_avx_different_sizes(c);
    }

    // AVX512 benchmarks
    #[cfg(all(
        target_feature = "avx512f",
        target_feature = "avx512dq",
        target_feature = "avx512bw",
        target_feature = "avx512vl",
        target_feature = "fma"
    ))]
    {
        bench_avx512_cosine_distance(c);
        bench_avx512_inner_product(c);
        bench_avx512_euclidean_distance(c);
        bench_avx512_different_sizes(c);
    }
}

criterion_group!(benches, configure_benches);
criterion_main!(benches);
