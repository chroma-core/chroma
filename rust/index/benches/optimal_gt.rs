use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;

use chroma_distance::DistanceFunction;
use chroma_index::spann::utils::{cluster, KMeansAlgorithmInput};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rayon::prelude::*;

use crate::datasets::Query;

const MIN_TRAINING_SAMPLE: usize = 10_000;
const MAX_TRAINING_SAMPLE: usize = 50_000;
const TRAINING_SAMPLE_FACTOR: usize = 3;
const INITIAL_LAMBDA: f32 = 100.0;

pub struct FlatKmeansGtIndex {
    num_clusters: usize,
    num_non_empty_clusters: usize,
    training_sample_size: usize,
    gt_id_to_cluster: HashMap<u32, usize>,
}

impl FlatKmeansGtIndex {
    pub fn build(
        all_vectors: &[(u32, Arc<[f32]>)],
        queries: &[&Query],
        dimension: usize,
        num_clusters: usize,
        distance_fn: DistanceFunction,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        if all_vectors.is_empty() || num_clusters == 0 {
            return Ok(Self {
                num_clusters: 0,
                num_non_empty_clusters: 0,
                training_sample_size: 0,
                gt_id_to_cluster: HashMap::new(),
            });
        }

        let training_sample_size = choose_training_sample_size(all_vectors.len(), num_clusters);
        let sample_embeddings = sample_embeddings(all_vectors, training_sample_size);
        let num_clusters = num_clusters.min(sample_embeddings.len()).max(1);

        let mut kmeans_input = KMeansAlgorithmInput::new(
            (0..sample_embeddings.len()).collect(),
            &sample_embeddings,
            dimension,
            num_clusters,
            0,
            sample_embeddings.len(),
            sample_embeddings.len(),
            distance_fn.clone(),
            INITIAL_LAMBDA,
        );
        let kmeans = cluster(&mut kmeans_input)?;

        let gt_ids: HashSet<u32> = queries
            .iter()
            .flat_map(|q| q.neighbors.iter().take(100).copied())
            .collect();

        let gt_vectors: Vec<(u32, Arc<[f32]>)> = all_vectors
            .iter()
            .filter(|(id, _)| gt_ids.contains(id))
            .map(|(id, emb)| (*id, Arc::clone(emb)))
            .collect();

        let gt_id_to_cluster: HashMap<u32, usize> = gt_vectors
            .par_iter()
            .map(|(id, emb)| {
                let cluster = nearest_center(&kmeans.cluster_centers, emb, &distance_fn);
                (*id, cluster)
            })
            .collect();

        Ok(Self {
            num_clusters,
            num_non_empty_clusters: kmeans.num_clusters,
            training_sample_size,
            gt_id_to_cluster,
        })
    }

    pub fn gt_cluster_counts(&self, gt_100: &HashSet<u32>) -> (usize, usize, usize) {
        if gt_100.is_empty() {
            return (0, 0, 0);
        }

        let mut cluster_hits: HashMap<usize, usize> = HashMap::new();
        for id in gt_100 {
            if let Some(&cluster) = self.gt_id_to_cluster.get(id) {
                *cluster_hits.entry(cluster).or_default() += 1;
            }
        }

        if cluster_hits.is_empty() {
            return (0, 0, 0);
        }

        let total = cluster_hits.values().sum::<usize>();
        let p90_target = (total as f64 * 0.90).ceil() as usize;
        let p95_target = (total as f64 * 0.95).ceil() as usize;

        let mut counts: Vec<usize> = cluster_hits.into_values().collect();
        counts.sort_unstable_by(|a, b| b.cmp(a));

        let p100 = counts.len();
        let mut covered = 0usize;
        let mut p95 = 0usize;
        let mut p90 = 0usize;
        for (i, count) in counts.iter().enumerate() {
            covered += count;
            if p90 == 0 && covered >= p90_target {
                p90 = i + 1;
            }
            if p95 == 0 && covered >= p95_target {
                p95 = i + 1;
            }
            if covered >= total {
                break;
            }
        }

        (p100, p95, p90)
    }

    pub fn training_sample_size(&self) -> usize {
        self.training_sample_size
    }

    pub fn num_clusters(&self) -> usize {
        self.num_clusters
    }

    pub fn num_non_empty_clusters(&self) -> usize {
        self.num_non_empty_clusters
    }
}

fn choose_training_sample_size(total_vectors: usize, num_clusters: usize) -> usize {
    (num_clusters.saturating_mul(TRAINING_SAMPLE_FACTOR))
        .clamp(MIN_TRAINING_SAMPLE, MAX_TRAINING_SAMPLE)
        .min(total_vectors)
}

fn sample_embeddings(
    all_vectors: &[(u32, Arc<[f32]>)],
    training_sample_size: usize,
) -> Vec<Arc<[f32]>> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut indices: Vec<usize> = (0..all_vectors.len()).collect();
    indices.shuffle(&mut rng);
    indices.truncate(training_sample_size);
    indices
        .into_iter()
        .map(|idx| Arc::clone(&all_vectors[idx].1))
        .collect()
}

fn nearest_center(
    centers: &[Arc<[f32]>],
    embedding: &[f32],
    distance_fn: &DistanceFunction,
) -> usize {
    centers
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| {
            distance_fn
                .distance(embedding, a.as_ref())
                .partial_cmp(&distance_fn.distance(embedding, b.as_ref()))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(idx, _)| idx)
        .unwrap_or(0)
}
