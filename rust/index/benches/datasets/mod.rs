pub mod dbpedia;
pub mod ground_truth;

use std::collections::HashSet;

/// A query with ground truth neighbors.
#[derive(Clone)]
pub struct Query {
    pub vector: Vec<f32>,
    pub neighbors: Vec<u32>,
    /// Ground truth is computed against vectors [0, max_vector_id).
    pub max_vector_id: u64,
}

/// Compute recall@k.
pub fn recall_at_k(predicted: &[u32], ground_truth: &[u32], k: usize) -> f64 {
    let gt: HashSet<u32> = ground_truth.iter().take(k).copied().collect();

    if gt.is_empty() {
        return 0.0;
    }

    let predicted_set: HashSet<u32> = predicted.iter().copied().collect();
    let found = predicted_set.intersection(&gt).count();
    found as f64 / gt.len() as f64
}

pub fn format_count(count: usize) -> String {
    if count < 1000 {
        format!("{}", count)
    } else if count < 1_000_000 {
        format!("{:.1}K", count as f64 / 1000.0)
    } else {
        format!("{:.2}M", count as f64 / 1_000_000.0)
    }
}
