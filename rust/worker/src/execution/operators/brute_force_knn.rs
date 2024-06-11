use crate::execution::data::data_chunk::Chunk;
use crate::execution::operators::normalize_vectors::normalize;
use crate::types::LogRecord;
use crate::types::Operation;
use crate::{distance::DistanceFunction, execution::operator::Operator};
use async_trait::async_trait;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tracing::trace;

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
    pub data: Chunk<LogRecord>,
    pub query: Vec<f32>,
    pub k: usize,
    pub distance_metric: DistanceFunction,
    pub allowed_ids: Arc<[String]>,
    // This is just a subset of allowed_ids containing
    // only the ids that are allowed and present in the log.
    pub allowed_ids_brute_force: Arc<[String]>,
}

/// The output of the brute force k-nearest neighbors operator.
/// # Parameters
/// * `data` - The vectors to query against. Only the vectors that are nearest neighbors are visible.
/// * `indices` - The indices of the nearest neighbors. This is a mask against the `data` input.
/// One row for each query vector.
/// * `distances` - The distances of the nearest neighbors.
/// One row for each query vector.
#[derive(Debug)]
pub struct BruteForceKnnOperatorOutput {
    pub data: Chunk<LogRecord>,
    pub indices: Vec<usize>,
    pub distances: Vec<f32>,
}

#[derive(Debug)]
struct Entry {
    index: usize,
    distance: f32,
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.distance == other.distance {
            Ordering::Equal
        } else if self.distance > other.distance {
            // This is a min heap, so we need to reverse the ordering.
            Ordering::Less
        } else {
            // This is a min heap, so we need to reverse the ordering.
            Ordering::Greater
        }
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for Entry {}

#[async_trait]
impl Operator<BruteForceKnnOperatorInput, BruteForceKnnOperatorOutput> for BruteForceKnnOperator {
    type Error = ();

    async fn run(
        &self,
        input: &BruteForceKnnOperatorInput,
    ) -> Result<BruteForceKnnOperatorOutput, Self::Error> {
        let should_normalize = match input.distance_metric {
            DistanceFunction::Cosine => true,
            _ => false,
        };

        let normalized_query = match should_normalize {
            true => Some(normalize(&input.query)),
            false => None,
        };

        let mut heap = BinaryHeap::with_capacity(input.k);
        let data_chunk = &input.data;
        for data in data_chunk.iter() {
            let log_record = data.0;
            let index = data.1;

            if log_record.record.operation == Operation::Delete {
                // Explicitly skip deleted records.
                continue;
            }

            // Skip records that are disallowed. If allowed list is empty then
            // don't exclude anything.
            // Empty allowed list is passed when where filtering is absent.
            if !input.allowed_ids.is_empty()
                && !input
                    .allowed_ids_brute_force
                    .contains(&log_record.record.id)
            {
                continue;
            }
            let embedding = match &log_record.record.embedding {
                Some(embedding) => embedding,
                None => {
                    // implies that the record is a delete or update of irrelevant field
                    continue;
                }
            };
            if should_normalize {
                let normalized_query = normalized_query.as_ref().expect("Invariant violation. Should have set normalized query if should_normalize is true.");
                let normalized_embedding = normalize(&embedding[..]);
                let distance = input
                    .distance_metric
                    .distance(&normalized_embedding[..], &normalized_query[..]);
                heap.push(Entry { index, distance });
            } else {
                let distance = input.distance_metric.distance(&embedding[..], &input.query);
                heap.push(Entry { index, distance });
            }
        }

        let mut visibility = vec![false; data_chunk.total_len()];
        let mut sorted_indices = Vec::with_capacity(input.k);
        let mut sorted_distances = Vec::with_capacity(input.k);
        let mut i = 0;
        while i < input.k {
            let entry = match heap.pop() {
                Some(entry) => entry,
                None => {
                    break;
                }
            };
            sorted_indices.push(entry.index);
            sorted_distances.push(entry.distance);
            visibility[entry.index] = true;
            i += 1;
        }
        let mut data_chunk = data_chunk.clone();
        data_chunk.set_visibility(visibility);
        trace!(
            "Brute force Knn result. data: {:?}, indices: {:?}, distances: {:?}",
            data_chunk,
            sorted_indices,
            sorted_distances
        );
        Ok(BruteForceKnnOperatorOutput {
            data: data_chunk,
            indices: sorted_indices,
            distances: sorted_distances,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::types::LogRecord;
    use crate::types::Operation;
    use crate::types::OperationRecord;

    use super::*;

    #[tokio::test]
    async fn test_brute_force_knn_l2sqr() {
        let operator = BruteForceKnnOperator {};
        let data = vec![
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![0.0, 0.0, 0.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: Some(vec![0.0, 1.0, 1.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_3".to_string(),
                    embedding: Some(vec![7.0, 8.0, 9.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
        ];
        let data_chunk = Chunk::new(data.into());

        let input = BruteForceKnnOperatorInput {
            data: data_chunk,
            query: vec![0.0, 0.0, 0.0],
            k: 2,
            distance_metric: DistanceFunction::Euclidean,
            allowed_ids: Arc::new([]),
            allowed_ids_brute_force: Arc::new([]),
        };
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.indices, vec![0, 1]);
        let distance_1 = 0.0_f32.powi(2) + 1.0_f32.powi(2) + 1.0_f32.powi(2);
        assert_eq!(output.distances, vec![0.0, distance_1]);
        assert_eq!(output.data.get_visibility(0), Some(true));
        assert_eq!(output.data.get_visibility(1), Some(true));
        assert_eq!(output.data.get_visibility(2), Some(false));
    }

    #[tokio::test]
    async fn test_brute_force_knn_cosine() {
        let operator = BruteForceKnnOperator {};

        let norm_1 = (1.0_f32.powi(2) + 2.0_f32.powi(2) + 3.0_f32.powi(2)).sqrt();
        let data_1 = vec![1.0 / norm_1, 2.0 / norm_1, 3.0 / norm_1];

        let norm_2 = (0.0_f32.powi(2) + -1.0_f32.powi(2) + 6.0_f32.powi(2)).sqrt();
        let data_2 = vec![0.0 / norm_2, -1.0 / norm_2, 6.0 / norm_2];
        let data = vec![
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![0.0, 1.0, 0.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: Some(data_1.clone()),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_3".to_string(),
                    embedding: Some(data_2.clone()),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
        ];
        let data_chunk = Chunk::new(data.into());

        let input = BruteForceKnnOperatorInput {
            data: data_chunk,
            query: vec![0.0, 1.0, 0.0],
            k: 2,
            distance_metric: DistanceFunction::InnerProduct,
            allowed_ids: Arc::new([]),
            allowed_ids_brute_force: Arc::new([]),
        };
        let output = operator.run(&input).await.unwrap();

        assert_eq!(output.indices, vec![0, 1]);
        let expected_distance_1 =
            1.0f32 - ((data_1[0] * 0.0) + (data_1[1] * 1.0) + (data_1[2] * 0.0));
        assert_eq!(output.distances, vec![0.0, expected_distance_1]);
        assert_eq!(output.data.get_visibility(0), Some(true));
        assert_eq!(output.data.get_visibility(1), Some(true));
        assert_eq!(output.data.get_visibility(2), Some(false));
    }

    #[tokio::test]
    async fn test_data_less_than_k() {
        // If we have less data than k, we should return all the data, sorted by distance.
        let operator = BruteForceKnnOperator {};
        let data = vec![LogRecord {
            log_offset: 1,
            record: OperationRecord {
                id: "embedding_id_1".to_string(),
                embedding: Some(vec![0.0, 0.0, 0.0]),
                encoding: None,
                metadata: None,
                document: None,
                operation: Operation::Add,
            },
        }];

        let data_chunk = Chunk::new(data.into());

        let input = BruteForceKnnOperatorInput {
            data: data_chunk,
            query: vec![0.0, 0.0, 0.0],
            k: 2,
            distance_metric: DistanceFunction::Euclidean,
            allowed_ids: Arc::new([]),
            allowed_ids_brute_force: Arc::new([]),
        };
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.indices, vec![0]);
        assert_eq!(output.distances, vec![0.0]);
        assert_eq!(output.data.get_visibility(0), Some(true));
    }

    #[tokio::test]
    async fn test_skip_deleted_records() {
        let operator = BruteForceKnnOperator {};
        let data = vec![
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![0.0, 0.0, 0.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_3".to_string(),
                    embedding: Some(vec![7.0, 8.0, 9.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
        ];
        let data_chunk = Chunk::new(data.into());

        let input = BruteForceKnnOperatorInput {
            data: data_chunk,
            query: vec![0.0, 0.0, 0.0],
            k: 2,
            distance_metric: DistanceFunction::Euclidean,
            allowed_ids: Arc::new([]),
            allowed_ids_brute_force: Arc::new([]),
        };
        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.indices, vec![0, 2]);
        let distance_1 = 7.0_f32.powi(2) + 8.0_f32.powi(2) + 9.0_f32.powi(2);
        assert_eq!(output.distances, vec![0.0, distance_1]);
        assert_eq!(output.data.get_visibility(0), Some(true));
        assert_eq!(output.data.get_visibility(1), Some(false));
        assert_eq!(output.data.get_visibility(2), Some(true));
    }
}
