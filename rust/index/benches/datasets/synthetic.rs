//! Synthetic dataset with configurable dimension.
//!
//! Generates random vectors from a uniform distribution and computes
//! ground truth via brute-force KNN in-process. No download required.

use std::io;
use std::sync::Arc;

use chroma_distance::DistanceFunction;
use rand::{rngs::StdRng, Rng, SeedableRng};
use rayon::prelude::*;

use super::{Dataset, Query};

const NUM_QUERIES: usize = 100;
const GT_K: usize = 100;
const BATCH_SIZE: usize = 1_000_000;

pub struct Synthetic {
    dim: usize,
    size: usize,
    name: String,
    vectors: Vec<Vec<f32>>,
    query_vectors: Vec<Vec<f32>>,
}

impl Synthetic {
    pub fn load(dim: usize, size: usize) -> io::Result<Self> {
        println!("Generating synthetic dataset: {size} vectors x {dim} dims ...");
        let t0 = std::time::Instant::now();

        let mut rng = StdRng::seed_from_u64(42);
        let vectors: Vec<Vec<f32>> = (0..size)
            .map(|_| (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect())
            .collect();

        let mut query_rng = StdRng::seed_from_u64(1337);
        let query_vectors: Vec<Vec<f32>> = (0..NUM_QUERIES)
            .map(|_| (0..dim).map(|_| query_rng.gen_range(-1.0f32..1.0)).collect())
            .collect();

        println!(
            "  Generated in {:.2}s ({:.1} MB)",
            t0.elapsed().as_secs_f64(),
            (size * dim * 4) as f64 / 1e6,
        );

        Ok(Self {
            dim,
            size,
            name: format!("synthetic-{dim}d"),
            vectors,
            query_vectors,
        })
    }
}

fn exact_distance(a: &[f32], b: &[f32], df: &DistanceFunction) -> f32 {
    match df {
        DistanceFunction::Euclidean => a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum(),
        DistanceFunction::Cosine | DistanceFunction::InnerProduct => {
            1.0 - a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>()
        }
    }
}

impl Dataset for Synthetic {
    fn name(&self) -> &str {
        &self.name
    }

    fn dimension(&self) -> usize {
        self.dim
    }

    fn data_len(&self) -> usize {
        self.size
    }

    fn k(&self) -> usize {
        GT_K
    }

    fn load_range(&self, offset: usize, limit: usize) -> io::Result<Vec<(u32, Arc<[f32]>)>> {
        let end = (offset + limit).min(self.size);
        Ok((offset..end)
            .map(|i| (i as u32, Arc::from(self.vectors[i].as_slice())))
            .collect())
    }

    fn queries(&self, distance_function: DistanceFunction) -> io::Result<Vec<Query>> {
        let mut queries = Vec::new();
        let mut boundary = BATCH_SIZE.min(self.size);

        loop {
            println!(
                "  Computing ground truth ({} queries x {} vectors) ...",
                NUM_QUERIES, boundary
            );
            let t0 = std::time::Instant::now();
            let db = &self.vectors[..boundary];

            let batch_queries: Vec<Query> = self
                .query_vectors
                .par_iter()
                .map(|qv| {
                    let mut dists: Vec<(u32, f32)> = db
                        .iter()
                        .enumerate()
                        .map(|(i, v)| (i as u32, exact_distance(v, qv, &distance_function)))
                        .collect();
                    dists.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
                    Query {
                        vector: qv.clone(),
                        neighbors: dists.iter().take(GT_K).map(|(i, _)| *i).collect(),
                        max_vector_id: boundary as u64,
                    }
                })
                .collect();

            println!("  Ground truth at {}M in {:.2}s", boundary / 1_000_000.max(1), t0.elapsed().as_secs_f64());
            queries.extend(batch_queries);

            if boundary >= self.size {
                break;
            }
            boundary = (boundary + BATCH_SIZE).min(self.size);
        }

        Ok(queries)
    }
}
