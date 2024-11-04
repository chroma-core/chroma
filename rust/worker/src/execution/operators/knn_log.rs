use std::collections::BinaryHeap;

use chroma_blockstore::provider::BlockfileProvider;
use chroma_distance::{normalize, DistanceFunction, DistanceFunctionError};
use chroma_error::ChromaError;
use chroma_types::{MaterializedLogOperation, MetadataValue, Segment, SignedRoaringBitmap};
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

#[derive(Debug)]
struct KnnLogInput {
    logs: FetchLogOutput,
    blockfile_provider: BlockfileProvider,
    record_segment: Segment,
    vector_segment: Segment,
    log_offset_ids: SignedRoaringBitmap,
}

#[derive(Debug)]
pub struct KnnLogOutput {
    pub record_distances: Vec<RecordDistance>,
}

#[derive(Error, Debug)]
pub enum KnnLogError {
    #[error("Error instantiating distance function: {0}")]
    DistanceFunction(#[from] DistanceFunctionError),
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
            KnnLogError::DistanceFunction(e) => e.code(),
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

        let space = match input.vector_segment.metadata.as_ref() {
            Some(metadata) => match metadata.get("hnsw:space") {
                Some(MetadataValue::Str(space)) => space,
                _ => "l2",
            },
            None => "l2",
        };
        let metric = DistanceFunction::try_from(space)?;
        let target_vector;
        let target_embedding = if let DistanceFunction::Cosine = metric {
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
                let log_embedding = if let DistanceFunction::Cosine = metric {
                    log_vector = normalize(log.merged_embeddings());
                    &log_vector
                } else {
                    log.merged_embeddings()
                };

                let distance = RecordDistance {
                    offset_id: log.offset_id,
                    measure: metric.distance(target_embedding, log_embedding),
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
