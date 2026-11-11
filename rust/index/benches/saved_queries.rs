//! Query inputs for a saved index, without a source-corpus loader.

use std::io;
use std::path::Path;
use std::sync::Arc;

use chroma_distance::DistanceFunction;

use super::datasets::{ground_truth, Dataset, Query};

pub struct SavedQueries {
    queries: Vec<Query>,
    total_vectors: usize,
    dimension: usize,
}

impl SavedQueries {
    pub fn load(path: &Path, total_vectors: usize, metric: DistanceFunction) -> io::Result<Self> {
        Self::from_queries(ground_truth::load(path, metric)?, total_vectors)
    }

    fn from_queries(mut queries: Vec<Query>, total_vectors: usize) -> io::Result<Self> {
        queries.retain(|q| q.max_vector_id == total_vectors as u64);
        let dimension = queries.first().map_or(0, |q| q.vector.len());
        if dimension == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("ground truth has no nonempty queries for exactly {total_vectors} vectors"),
            ));
        }
        for query in &queries {
            if query.vector.len() != dimension || query.vector.iter().any(|v| !v.is_finite()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "query vectors must have the same dimension and finite values",
                ));
            }
            if query.neighbors.len() < ground_truth::K
                || query
                    .neighbors
                    .iter()
                    .any(|&id| id as usize >= total_vectors)
            {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "ground truth must contain at least 100 neighbors within the checkpoint's vector prefix"));
            }
        }
        Ok(Self {
            queries,
            total_vectors,
            dimension,
        })
    }
}

impl Dataset for SavedQueries {
    fn name(&self) -> &str {
        "saved-checkpoint"
    }
    fn dimension(&self) -> usize {
        self.dimension
    }
    fn data_len(&self) -> usize {
        self.total_vectors
    }
    fn k(&self) -> usize {
        ground_truth::K
    }
    fn queries(&self, _: DistanceFunction) -> io::Result<Vec<Query>> {
        Ok(self.queries.clone())
    }
    fn load_range(&self, _: usize, _: usize) -> io::Result<Vec<(u32, Arc<[f32]>)>> {
        Err(io::Error::other(
            "query-only mode cannot load source vectors",
        ))
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;

    fn query(count: u64) -> Query {
        Query {
            vector: vec![1.0; 8],
            neighbors: (0..100).collect(),
            max_vector_id: count,
        }
    }

    #[test]
    fn selects_only_exact_checkpoint_answers() {
        let inputs = SavedQueries::from_queries(vec![query(100), query(200)], 100).unwrap();
        assert_eq!(inputs.queries.len(), 1);
        assert_eq!(inputs.dimension(), 8);
        assert!(inputs.load_range(0, 1).is_err());
        assert!(SavedQueries::from_queries(vec![query(200)], 100).is_err());
    }

    #[test]
    fn rejects_invalid_query_inputs() {
        let mut wrong_dimension = query(100);
        wrong_dimension.vector.pop();
        assert!(SavedQueries::from_queries(vec![query(100), wrong_dimension], 100).is_err());
        let mut outside_prefix = query(100);
        outside_prefix.neighbors[0] = 100;
        assert!(SavedQueries::from_queries(vec![outside_prefix], 100).is_err());
        let mut incomplete = query(100);
        incomplete.neighbors.pop();
        assert!(SavedQueries::from_queries(vec![incomplete], 100).is_err());
    }
}
