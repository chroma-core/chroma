use chroma_distance::DistanceFunction;
use criterion::{criterion_group, criterion_main, Criterion};

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

criterion_group!(benches, distance_metrics,);
criterion_main!(benches);
