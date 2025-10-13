use crate::execution::operators::projection::ProjectionInput;
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_system::Operator;
use chroma_types::{
    operator::{KnnProjection, KnnProjectionOutput, KnnProjectionRecord, RecordMeasure},
    Segment,
};
use thiserror::Error;

use super::{fetch_log::FetchLogOutput, projection::ProjectionError};

/// The `KnnProjection` operator retrieves record content by offset ids
/// It is based on `ProjectionOperator`, and it attaches the distance
/// of the records to the target embedding to the record content
///
/// # Parameters
/// - `projection`: The parameters of the `ProjectionOperator`
/// - `distance`: Whether to attach distance information
///
/// # Inputs
/// - `logs`: The latest logs of the collection
/// - `blockfile_provider`: The blockfile provider
/// - `record_segment`: The record segment information
/// - `record_distances`: The offset ids of the record to retrieve for,
///   along with their distances to the target embedding
///
/// # Outputs
/// - `records`: The retrieved records in the same order as `record_distances`
///
/// # Usage
/// It can be used to retrieve record contents as user requested
/// It should be run as the last step of an orchestrator
#[derive(Clone, Debug)]
pub struct KnnProjectionInput {
    pub logs: FetchLogOutput,
    pub blockfile_provider: BlockfileProvider,
    pub record_segment: Segment,
    pub record_distances: Vec<RecordMeasure>,
}

#[derive(Error, Debug)]
pub enum KnnProjectionError {
    #[error("Error running projection operator: {0}")]
    Projection(#[from] ProjectionError),
}

impl ChromaError for KnnProjectionError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            KnnProjectionError::Projection(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<KnnProjectionInput, KnnProjectionOutput> for KnnProjection {
    type Error = KnnProjectionError;

    async fn run(
        &self,
        input: &KnnProjectionInput,
    ) -> Result<KnnProjectionOutput, KnnProjectionError> {
        let projection_input = ProjectionInput {
            logs: input.logs.clone(),
            blockfile_provider: input.blockfile_provider.clone(),
            record_segment: input.record_segment.clone(),
            offset_ids: input
                .record_distances
                .iter()
                .map(|record| record.offset_id)
                .collect(),
        };

        let result = self.projection.run(&projection_input).await?;

        return Ok(KnnProjectionOutput {
            records: result
                .records
                .into_iter()
                .zip(input.record_distances.clone())
                .map(
                    |(
                        record,
                        RecordMeasure {
                            offset_id: _,
                            measure,
                        },
                    )| KnnProjectionRecord {
                        record,
                        distance: self.distance.then_some(measure),
                    },
                )
                .collect(),
        });
    }
}

#[cfg(test)]
mod tests {
    use chroma_log::test::{int_as_id, upsert_generator, LoadFromGenerator, LogGenerator};
    use chroma_segment::test::TestDistributedSegment;
    use chroma_system::Operator;
    use chroma_types::operator::{KnnProjection, Projection, RecordMeasure};

    use super::KnnProjectionInput;

    /// The unit tests for `KnnProjection` operator uses the following test data
    /// It first generates 100 log records and compact them,
    /// then generate 20 log records that overwrite the compacted data,
    /// and finally generate 20 log records of new data:
    ///
    /// - Log: Upsert [81..=120]
    /// - Compacted: Upsert [1..=100]
    async fn setup_knn_projection_input(
        record_distances: Vec<RecordMeasure>,
    ) -> (TestDistributedSegment, KnnProjectionInput) {
        let mut test_segment = TestDistributedSegment::new().await;
        test_segment
            .populate_with_generator(100, upsert_generator)
            .await;
        let blockfile_provider = test_segment.blockfile_provider.clone();
        let record_segment = test_segment.record_segment.clone();
        (
            test_segment,
            KnnProjectionInput {
                logs: upsert_generator.generate_chunk(81..=120),
                blockfile_provider,
                record_segment,
                record_distances,
            },
        )
    }

    #[tokio::test]
    async fn test_trivial_knn_projection() {
        let (_test_segment, knn_projection_input) = setup_knn_projection_input(
            (71..=90)
                .rev()
                .map(|offset_id| RecordMeasure {
                    offset_id,
                    measure: -(offset_id as f32),
                })
                .collect(),
        )
        .await;

        let knn_projection_operator = KnnProjection {
            projection: Projection {
                document: false,
                embedding: false,
                metadata: false,
            },
            distance: false,
        };

        let knn_projection_output = knn_projection_operator
            .run(&knn_projection_input)
            .await
            .expect("KnnProjectionOperator should not fail");

        assert_eq!(knn_projection_output.records.len(), 20);
        for (knn_record, offset_id) in knn_projection_output
            .records
            .into_iter()
            .zip((71..=90).rev())
        {
            assert_eq!(knn_record.record.id, int_as_id(offset_id));
            assert!(knn_record.record.document.is_none());
            assert!(knn_record.record.embedding.is_none());
            assert!(knn_record.record.metadata.is_none());
            assert!(knn_record.distance.is_none());
        }
    }

    #[tokio::test]
    async fn test_simple_knn_projection() {
        let (_test_segment, knn_projection_input) = setup_knn_projection_input(
            (71..=90)
                .rev()
                .map(|offset_id| RecordMeasure {
                    offset_id,
                    measure: -(offset_id as f32),
                })
                .collect(),
        )
        .await;

        let knn_projection_operator = KnnProjection {
            projection: Projection {
                document: false,
                embedding: true,
                metadata: false,
            },
            distance: true,
        };

        let knn_projection_output = knn_projection_operator
            .run(&knn_projection_input)
            .await
            .expect("KnnProjectionOperator should not fail");

        assert_eq!(knn_projection_output.records.len(), 20);
        for (knn_record, offset_id) in knn_projection_output
            .records
            .into_iter()
            .zip((71..=90).rev())
        {
            assert_eq!(knn_record.record.id, int_as_id(offset_id));
            assert!(knn_record.record.document.is_none());
            assert!(knn_record.record.embedding.is_some());
            assert!(knn_record.record.metadata.is_none());
            assert_eq!(knn_record.distance, Some(-(offset_id as f32)));
        }
    }
}
