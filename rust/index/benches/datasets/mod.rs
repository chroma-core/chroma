pub mod arxiv;
pub mod dbpedia;
pub mod ground_truth;
pub mod msmarco;
pub mod sec;
pub mod wikipedia;

use std::collections::HashSet;
use std::io;
use std::sync::Arc;

use chroma_distance::DistanceFunction;
use clap::ValueEnum;

// =============================================================================
// Dataset Type Enum (for CLI)
// =============================================================================

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum DatasetType {
    #[default]
    DbPedia,
    Arxiv,
    Sec,
    MsMarco,
    WikipediaEn,
}

// =============================================================================
// Metric Type Enum (for CLI)
// =============================================================================

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum MetricType {
    #[default]
    L2,
    Ip,
    Cosine,
}

impl MetricType {
    pub fn to_distance_function(self) -> DistanceFunction {
        match self {
            MetricType::L2 => DistanceFunction::Euclidean,
            MetricType::Ip => DistanceFunction::InnerProduct,
            MetricType::Cosine => DistanceFunction::Cosine,
        }
    }
}

// =============================================================================
// Dataset Trait
// =============================================================================

/// Trait for benchmark datasets.
pub trait Dataset: Send + Sync {
    /// Dataset name for display.
    fn name(&self) -> &str;

    /// Vector dimension.
    fn dimension(&self) -> usize;

    /// Total number of vectors in the dataset.
    fn data_len(&self) -> usize;

    /// Number of neighbors in ground truth (typically 100).
    fn k(&self) -> usize;

    /// Load vectors in range [offset, offset+limit).
    /// Returns (global_id, embedding) pairs.
    fn load_range(&self, offset: usize, limit: usize) -> io::Result<Vec<(u32, Arc<[f32]>)>>;

    /// Load ground truth queries for the given distance function.
    fn queries(&self, distance_function: DistanceFunction) -> io::Result<Vec<Query>>;
}

// =============================================================================
// Query & Utilities
// =============================================================================

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
