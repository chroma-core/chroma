use std::collections::BinaryHeap;

use chroma_blockstore::provider::BlockfileProvider;
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::ChromaError;
use chroma_types::{MaterializedLogOperation, Segment, SignedRoaringBitmap};
use thiserror::Error;
use tonic::async_trait;

use crate::{
    execution::operator::Operator,
    segment::{
        record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
        LogMaterializer, LogMaterializerError,
    },
};

use super::{
    fetch_log::{FetchLogError, FetchLogOutput},
    knn::{KnnOperator, RecordDistance},
};

#[derive(Clone, Debug)]
pub struct KnnLogInput {
    pub logs: FetchLogOutput,
    pub blockfile_provider: BlockfileProvider,
    pub record_segment: Segment,
    pub log_offset_ids: SignedRoaringBitmap,
    pub distance_function: DistanceFunction,
}

#[derive(Debug)]
pub struct KnnLogOutput {
    pub record_distances: Vec<RecordDistance>,
}

#[derive(Error, Debug)]
pub enum KnnLogError {
    #[error("Error processing fetch log output: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
}

impl ChromaError for KnnLogError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            KnnLogError::FetchLog(e) => e.code(),
            KnnLogError::LogMaterializer(e) => e.code(),
            KnnLogError::RecordReader(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<KnnLogInput, KnnLogOutput> for KnnOperator {
    type Error = KnnLogError;

    async fn run(&self, input: &KnnLogInput) -> Result<KnnLogOutput, KnnLogError> {
        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => Ok(Some(reader)),
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => Err(*e),
        }?;

        let materializer = LogMaterializer::new(record_segment_reader, input.logs.clone(), None);
        let logs = materializer.materialize().await?;

        let target_vector;
        let target_embedding = if let DistanceFunction::Cosine = input.distance_function {
            target_vector = normalize(&self.embedding);
            &target_vector
        } else {
            &self.embedding
        };

        let mut max_heap = BinaryHeap::with_capacity(self.fetch as usize);

        for (log, _) in logs.iter() {
            if !matches!(
                log.final_operation,
                MaterializedLogOperation::DeleteExisting
            ) && match &input.log_offset_ids {
                SignedRoaringBitmap::Include(rbm) => rbm.contains(log.offset_id),
                SignedRoaringBitmap::Exclude(rbm) => !rbm.contains(log.offset_id),
            } {
                let log_vector;
                let log_embedding = if let DistanceFunction::Cosine = input.distance_function {
                    log_vector = normalize(log.merged_embeddings());
                    &log_vector
                } else {
                    log.merged_embeddings()
                };

                let distance = RecordDistance {
                    offset_id: log.offset_id,
                    measure: input
                        .distance_function
                        .distance(target_embedding, log_embedding),
                };
                if max_heap.len() < self.fetch as usize {
                    max_heap.push(distance);
                } else if let Some(furthest_distance) = max_heap.peek() {
                    if &distance < furthest_distance {
                        max_heap.pop();
                        max_heap.push(distance);
                    }
                }
            }
        }
        Ok(KnnLogOutput {
            record_distances: max_heap.into_sorted_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use chroma_distance::{normalize, DistanceFunction};
    use chroma_types::SignedRoaringBitmap;

    use crate::{
        execution::{operator::Operator, operators::knn::KnnOperator},
        log::test::{random_embedding, upsert_generator, LogGenerator, TEST_EMBEDDING_DIMENSION},
        segment::test::TestSegment,
    };

    use super::KnnLogInput;

    /// The unit tests for `KnnLogOperator` uses 100 log records
    /// with random embeddings
    fn setup_knn_log_input(
        metric: DistanceFunction,
        log_offset_ids: SignedRoaringBitmap,
    ) -> KnnLogInput {
        let test_segment = TestSegment::default();
        let generator = LogGenerator {
            generator: upsert_generator,
        };
        KnnLogInput {
            logs: generator.generate_chunk(1..=100),
            blockfile_provider: test_segment.blockfile_provider,
            record_segment: test_segment.record_segment,
            distance_function: metric,
            log_offset_ids,
        }
    }

    #[tokio::test]
    async fn test_simple_euclidean() {
        let knn_log_input =
            setup_knn_log_input(DistanceFunction::Euclidean, SignedRoaringBitmap::full());

        let knn_operator = KnnOperator {
            embedding: random_embedding(TEST_EMBEDDING_DIMENSION),
            fetch: 6,
        };

        let mut brute_force_distances: Vec<_> = knn_log_input
            .logs
            .iter()
            .map(|(log, _)| {
                knn_log_input.distance_function.distance(
                    log.record
                        .embedding
                        .as_ref()
                        .expect("Embedding should be present in generated logs"),
                    &knn_operator.embedding,
                )
            })
            .collect();

        brute_force_distances.sort_by(|x, y| x.total_cmp(y));

        let knn_log_output = knn_operator
            .run(&knn_log_input)
            .await
            .expect("KnnLogOperator should not fail");

        assert_eq!(knn_log_output.record_distances.len(), 6);
        assert!(knn_log_output
            .record_distances
            .iter()
            .zip(brute_force_distances)
            .all(|(record, distance)| record.measure == distance));
    }

    #[tokio::test]
    async fn test_overfetch() {
        let knn_log_input =
            setup_knn_log_input(DistanceFunction::Euclidean, SignedRoaringBitmap::full());

        let knn_operator = KnnOperator {
            embedding: random_embedding(TEST_EMBEDDING_DIMENSION),
            fetch: 200,
        };

        let mut brute_force_distances: Vec<_> = knn_log_input
            .logs
            .iter()
            .map(|(log, _)| {
                knn_log_input.distance_function.distance(
                    log.record
                        .embedding
                        .as_ref()
                        .expect("Embedding should be present in generated logs"),
                    &knn_operator.embedding,
                )
            })
            .collect();

        brute_force_distances.sort_by(|x, y| x.total_cmp(y));

        let knn_log_output = knn_operator
            .run(&knn_log_input)
            .await
            .expect("KnnLogOperator should not fail");

        assert_eq!(knn_log_output.record_distances.len(), 100);
        assert!(knn_log_output
            .record_distances
            .iter()
            .zip(brute_force_distances)
            .all(|(record, distance)| record.measure == distance));
    }

    #[tokio::test]
    async fn test_complex_cosine() {
        let knn_log_input = setup_knn_log_input(
            DistanceFunction::Cosine,
            SignedRoaringBitmap::Exclude((0..100).filter(|offset_id| offset_id % 2 == 0).collect()),
        );

        let knn_operator = KnnOperator {
            embedding: random_embedding(TEST_EMBEDDING_DIMENSION),
            fetch: 6,
        };

        let mut brute_force_distances: Vec<_> = knn_log_input
            .logs
            .iter()
            .filter_map(|(log, _)| {
                (log.log_offset % 2 != 0).then_some(
                    knn_log_input.distance_function.distance(
                        &normalize(
                            log.record
                                .embedding
                                .as_ref()
                                .expect("Embedding should be present in generated logs"),
                        ),
                        &normalize(&knn_operator.embedding),
                    ),
                )
            })
            .collect();

        brute_force_distances.sort_by(|x, y| x.total_cmp(y));

        let knn_log_output = knn_operator
            .run(&knn_log_input)
            .await
            .expect("KnnLogOperator should not fail");

        assert_eq!(knn_log_output.record_distances.len(), 6);
        assert!(knn_log_output
            .record_distances
            .iter()
            .zip(brute_force_distances)
            .all(|(record, distance)| record.measure == distance));
    }
}
