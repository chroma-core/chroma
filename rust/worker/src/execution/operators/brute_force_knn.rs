use crate::blockstore::provider::BlockfileProvider;
use crate::errors::ChromaError;
use crate::errors::ErrorCodes;
use crate::execution::data::data_chunk::Chunk;
use crate::execution::operators::normalize_vectors::normalize;
use crate::segment::record_segment::RecordSegmentReader;
use crate::segment::LogMaterializer;
use crate::segment::LogMaterializerError;
use crate::segment::MaterializedLogRecord;
use crate::types::LogRecord;
use crate::types::MaterializedLogOperation;
use crate::types::Operation;
use crate::types::Segment;
use crate::{distance::DistanceFunction, execution::operator::Operator};
use async_trait::async_trait;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;
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
    pub log: Chunk<LogRecord>,
    pub query: Vec<f32>,
    pub k: usize,
    pub distance_metric: DistanceFunction,
    pub allowed_ids: Arc<[String]>,
    // Deps to create the log materializer
    pub record_segment_definition: Segment,
    pub blockfile_provider: BlockfileProvider,
}

/// The output of the brute force k-nearest neighbors operator.
/// # Parameters
/// * `user_ids` - The user ids of the nearest neighbors.
/// * `embeddings` - The embeddings of the nearest neighbors.
/// * `distances` - The distances of the nearest neighbors.
/// One row for each query vector.
#[derive(Debug)]
pub struct BruteForceKnnOperatorOutput {
    pub user_ids: Vec<String>,
    pub embeddings: Vec<Vec<f32>>,
    pub distances: Vec<f32>,
}

#[derive(Debug)]
struct Entry<'record> {
    user_id: &'record str,
    embedding: &'record [f32],
    distance: f32,
}

impl Ord for Entry<'_> {
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

impl PartialOrd for Entry<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Entry<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for Entry<'_> {}

#[derive(Debug, Error)]
pub enum BruteForceKnnOperatorError {
    #[error(transparent)]
    RecordSegmentReaderCreationError(
        #[from] crate::segment::record_segment::RecordSegmentReaderCreationError,
    ),
    #[error("Error while materializing log records: {0}")]
    LogMaterializationError(#[from] LogMaterializerError),
}

impl ChromaError for BruteForceKnnOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            BruteForceKnnOperatorError::RecordSegmentReaderCreationError(e) => e.code(),
            BruteForceKnnOperatorError::LogMaterializationError(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<BruteForceKnnOperatorInput, BruteForceKnnOperatorOutput> for BruteForceKnnOperator {
    type Error = BruteForceKnnOperatorError;

    async fn run(
        &self,
        input: &BruteForceKnnOperatorInput,
    ) -> Result<BruteForceKnnOperatorOutput, Self::Error> {
        // Materialize the log records
        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment_definition,
            &input.blockfile_provider,
        )
        .await {
            Ok(reader) => Some(reader),
            Err(e) => {
                match *e {
                    crate::segment::record_segment::RecordSegmentReaderCreationError::UninitializedSegment => None,
                    _ => return Err(BruteForceKnnOperatorError::RecordSegmentReaderCreationError(*e))
                }
            }
        };
        let log_materializer = LogMaterializer::new(record_segment_reader, input.log.clone(), None);
        let logs = match log_materializer.materialize().await {
            Ok(logs) => logs,
            Err(e) => {
                return Err(BruteForceKnnOperatorError::LogMaterializationError(e));
            }
        };

        let should_normalize = match input.distance_metric {
            DistanceFunction::Cosine => true,
            _ => false,
        };

        let normalized_query = match should_normalize {
            true => Some(normalize(&input.query)),
            false => None,
        };

        let mut heap = BinaryHeap::with_capacity(input.k);
        let data_chunk = logs;
        for data in data_chunk.iter() {
            let log_record = data.0;

            if log_record.final_operation == MaterializedLogOperation::DeleteExisting {
                // Explicitly skip deleted records.
                continue;
            }

            // Skip records that are disallowed. If allowed list is empty then
            // don't exclude anything.
            // Empty allowed list is passed when where filtering is absent.
            // TODO: This should not need to use merged_user_id, which clones the id.
            if !input.allowed_ids.is_empty()
                && !input.allowed_ids.contains(&log_record.merged_user_id())
            {
                continue;
            }
            let embedding = &log_record.merged_embeddings();
            if should_normalize {
                let normalized_query = normalized_query.as_ref().expect("Invariant violation. Should have set normalized query if should_normalize is true.");
                let normalized_embedding = normalize(&embedding[..]);
                let distance = input
                    .distance_metric
                    .distance(&normalized_embedding[..], &normalized_query[..]);
                heap.push(Entry {
                    user_id: log_record.merged_user_id_ref(),
                    embedding,
                    distance,
                });
            } else {
                let distance = input.distance_metric.distance(&embedding[..], &input.query);
                heap.push(Entry {
                    user_id: log_record.merged_user_id_ref(),
                    embedding,
                    distance,
                });
            }
        }

        let mut sorted_embeddings = Vec::with_capacity(input.k);
        let mut sorted_distances = Vec::with_capacity(input.k);
        let mut sorted_user_ids = Vec::with_capacity(input.k);
        let mut i = 0;
        while i < input.k {
            let entry = match heap.pop() {
                Some(entry) => entry,
                None => {
                    break;
                }
            };
            sorted_user_ids.push(entry.user_id.to_string());
            sorted_embeddings.push(entry.embedding.to_vec());
            sorted_distances.push(entry.distance);
            i += 1;
        }

        tracing::info!("Brute force Knn result. distances: {:?}", sorted_distances);
        Ok(BruteForceKnnOperatorOutput {
            user_ids: sorted_user_ids,
            embeddings: sorted_embeddings,
            distances: sorted_distances,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogRecord;
    use crate::types::Operation;
    use crate::types::OperationRecord;
    use std::collections::HashMap;
    use uuid::uuid;

    // Helper for tests
    fn get_blockfile_provider_and_record_segment_definition() -> (BlockfileProvider, Segment) {
        // Create a blockfile provider for the log materializer
        let blockfile_provider = BlockfileProvider::new_memory();

        // Create an empty record segment definition
        let record_segment_definition = Segment {
            id: uuid!("00000000-0000-0000-0000-000000000000"),
            r#type: crate::types::SegmentType::BlockfileRecord,
            scope: crate::types::SegmentScope::RECORD,
            collection: Some(uuid!("00000000-0000-0000-0000-000000000000")),
            metadata: None,
            file_path: HashMap::new(),
        };
        return (blockfile_provider, record_segment_definition);
    }

    #[tokio::test]
    async fn test_brute_force_knn_l2sqr() {
        let operator = BruteForceKnnOperator {};
        let (blockfile_provider, record_segment_definition) =
            get_blockfile_provider_and_record_segment_definition();
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
            log: data_chunk,
            query: vec![0.0, 0.0, 0.0],
            k: 2,
            distance_metric: DistanceFunction::Euclidean,
            allowed_ids: Arc::new([]),
            blockfile_provider,
            record_segment_definition,
        };

        let output = operator.run(&input).await.unwrap();
        assert_eq!(output.user_ids, vec!["embedding_id_1", "embedding_id_2"]);
        let distance_1 = 0.0_f32.powi(2) + 1.0_f32.powi(2) + 1.0_f32.powi(2);
        assert_eq!(output.distances, vec![0.0, distance_1]);
        assert_eq!(
            output.embeddings,
            vec![vec![0.0, 0.0, 0.0], vec![0.0, 1.0, 1.0]]
        );
    }

    #[tokio::test]
    async fn test_brute_force_knn_cosine() {
        let operator = BruteForceKnnOperator {};
        let (blockfile_provider, record_segment_definition) =
            get_blockfile_provider_and_record_segment_definition();

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
            log: data_chunk,
            query: vec![0.0, 1.0, 0.0],
            k: 2,
            distance_metric: DistanceFunction::InnerProduct,
            allowed_ids: Arc::new([]),
            blockfile_provider,
            record_segment_definition,
        };
        let output = operator.run(&input).await.unwrap();

        assert_eq!(output.user_ids, vec!["embedding_id_1", "embedding_id_2"]);
        let expected_distance_1 = 1.0 - ((data_1[0] * 0.0) + (data_1[1] * 1.0) + (data_1[2] * 0.0));
        assert_eq!(output.distances, vec![0.0, expected_distance_1]);
        assert_eq!(
            output.embeddings,
            vec![
                vec![0.0, 1.0, 0.0],
                vec![1.0 / norm_1, 2.0 / norm_1, 3.0 / norm_1]
            ]
        );
    }

    #[tokio::test]
    async fn test_data_less_than_k() {
        let (blockfile_provider, record_segment_definition) =
            get_blockfile_provider_and_record_segment_definition();

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
            log: data_chunk,
            query: vec![0.0, 0.0, 0.0],
            k: 2,
            distance_metric: DistanceFunction::Euclidean,
            allowed_ids: Arc::new([]),
            blockfile_provider,
            record_segment_definition,
        };
        let output = operator.run(&input).await.unwrap();

        assert_eq!(output.user_ids, vec!["embedding_id_1"]);
        assert_eq!(output.distances, vec![0.0]);
        assert_eq!(output.embeddings, vec![vec![0.0, 0.0, 0.0]]);
    }

    #[tokio::test]
    async fn test_malformed_record_errors() {
        let operator = BruteForceKnnOperator {};
        let (blockfile_provider, record_segment_definition) =
            get_blockfile_provider_and_record_segment_definition();
        let data = vec![
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![7.0, 8.0, 9.0]),
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
            log: data_chunk,
            query: vec![0.0, 0.0, 0.0],
            k: 2,
            distance_metric: DistanceFunction::Euclidean,
            allowed_ids: Arc::new([]),
            blockfile_provider,
            record_segment_definition,
        };
        let res = operator.run(&input).await;
        match res {
            Ok(_) => panic!("Expected error"),
            Err(e) => match e {
                BruteForceKnnOperatorError::LogMaterializationError(e) => {
                    // We expect an error here because the second record is malformed.
                }
                _ => panic!("Unexpected error"),
            },
        }
    }

    #[tokio::test]
    async fn test_skip_deleted_record() {
        let operator = BruteForceKnnOperator {};
        let (blockfile_provider, record_segment_definition) =
            get_blockfile_provider_and_record_segment_definition();
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
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Delete,
                },
            },
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_3".to_string(),
                    embedding: Some(vec![0.0, 0.0, 0.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
        ];
        let data_chunk = Chunk::new(data.into());

        let input = BruteForceKnnOperatorInput {
            log: data_chunk,
            query: vec![0.0, 0.0, 0.0],
            k: 2,
            distance_metric: DistanceFunction::Euclidean,
            allowed_ids: Arc::new([]),
            blockfile_provider,
            record_segment_definition,
        };
        let output = operator.run(&input).await.unwrap();

        assert_eq!(output.user_ids, vec!["embedding_id_3"]);
        assert_eq!(output.distances, vec![0.0]);
        assert_eq!(output.embeddings, vec![vec![0.0, 0.0, 0.0]]);
    }
}
