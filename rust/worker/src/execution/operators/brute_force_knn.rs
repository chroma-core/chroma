use crate::{distance::DistanceFunction, execution::operator::Operator};
use async_trait::async_trait;
use std::cmp;

/// The brute force k-nearest neighbors operator is responsible for computing the k-nearest neighbors
/// of a given query vector against a set of vectors using brute force calculation.
/// # Note
/// - Callers should ensure that the input vectors are normalized if using the cosine similarity metric.
#[derive(Debug)]
pub struct BruteForceKnnOperator {}

/// The input to the brute force k-nearest neighbors operator.
/// # Parameters
/// * `data` - The vectors to query against.
/// * `query` - The query vector.
/// * `k` - The number of nearest neighbors to find.
/// * `distance_metric` - The distance metric to use.
#[derive(Debug)]
pub struct BruteForceKnnOperatorInput {
    pub data: Vec<Vec<f32>>,
    pub query: Vec<f32>,
    pub k: usize,
    pub distance_metric: DistanceFunction,
}

/// The output of the brute force k-nearest neighbors operator.
/// # Parameters
/// * `indices` - The indices of the nearest neighbors. This is a mask against the `query_vecs` input.
/// One row for each query vector.
/// * `distances` - The distances of the nearest neighbors.
/// One row for each query vector.
#[derive(Debug)]
pub struct BruteForceKnnOperatorOutput {
    pub indices: Vec<usize>,
    pub distances: Vec<f32>,
}

pub type BruteForceKnnOperatorResult = Result<BruteForceKnnOperatorOutput, ()>;

#[async_trait]
impl Operator<BruteForceKnnOperatorInput, BruteForceKnnOperatorOutput> for BruteForceKnnOperator {
    type Error = ();

    async fn run(&self, input: &BruteForceKnnOperatorInput) -> BruteForceKnnOperatorResult {
        // We could use a heap approach here, but for now we just sort the distances and take the
        // first k.
        let mut sorted_indices_distances = input
            .data
            .iter()
            .map(|data| input.distance_metric.distance(&input.query, data))
            .enumerate()
            .collect::<Vec<(usize, f32)>>();
        sorted_indices_distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let (sorted_indices, sorted_distances) = sorted_indices_distances
            .drain(..cmp::min(input.k, input.data.len()))
            .unzip();

        Ok(BruteForceKnnOperatorOutput {
            indices: sorted_indices,
            distances: sorted_distances,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_brute_force_knn_l2sqr() {
        let operator = BruteForceKnnOperator {};
        let input = BruteForceKnnOperatorInput {
            data: vec![
                vec![0.0, 0.0, 0.0],
                vec![0.0, 1.0, 1.0],
                vec![7.0, 8.0, 9.0],
            ],
            query: vec![0.0, 0.0, 0.0],
            k: 2,
            distance_metric: DistanceFunction::Euclidean,
        };
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.indices, vec![0, 1]);
        let distance_1 = 0.0_f32.powi(2) + 1.0_f32.powi(2) + 1.0_f32.powi(2);
        assert_eq!(output.distances, vec![0.0, distance_1]);
    }

    #[tokio::test]
    async fn test_brute_force_knn_cosine() {
        let operator = BruteForceKnnOperator {};

        let norm_1 = (1.0_f32.powi(2) + 2.0_f32.powi(2) + 3.0_f32.powi(2)).sqrt();
        let data_1 = vec![1.0 / norm_1, 2.0 / norm_1, 3.0 / norm_1];

        let norm_2 = (0.0_f32.powi(2) + -1.0_f32.powi(2) + 6.0_f32.powi(2)).sqrt();
        let data_2 = vec![0.0 / norm_2, -1.0 / norm_2, 6.0 / norm_2];

        let input = BruteForceKnnOperatorInput {
            data: vec![vec![0.0, 1.0, 0.0], data_1.clone(), data_2.clone()],
            query: vec![0.0, 1.0, 0.0],
            k: 2,
            distance_metric: DistanceFunction::InnerProduct,
        };
        let output = operator.run(&input).await.unwrap();

        assert_eq!(output.indices, vec![0, 1]);
        let expected_distance_1 =
            1.0f32 - ((data_1[0] * 0.0) + (data_1[1] * 1.0) + (data_1[2] * 0.0));
        assert_eq!(output.distances, vec![0.0, expected_distance_1]);
    }
}
