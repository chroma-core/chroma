use std::collections::BinaryHeap;

use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;
use chroma_types::{MaterializedLogOperation, SignedRoaringBitmap};
use thiserror::Error;
use tonic::async_trait;

use crate::{
    execution::{
        operator::Operator,
        utils::{normalize, RecordDistance},
    },
    segment::{LogMaterializer, LogMaterializerError},
};

use super::{
    fetch_log::FetchLogOutput,
    fetch_segment::{FetchSegmentError, FetchSegmentOutput},
    knn::KnnOperator,
};

#[derive(Debug)]
struct KnnLogInput {
    logs: FetchLogOutput,
    segments: FetchSegmentOutput,
    log_offset_ids: SignedRoaringBitmap,
}

#[derive(Debug)]
pub struct KnnLogOutput {
    pub record_distances: Vec<RecordDistance>,
}

#[derive(Error, Debug)]
pub enum KnnLogError {
    #[error("Error processing fetch log output: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error processing fetch segment output: {0}")]
    FetchSegment(#[from] FetchSegmentError),
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
}

impl ChromaError for KnnLogError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            KnnLogError::FetchLog(e) => e.code(),
            KnnLogError::FetchSegment(e) => e.code(),
            KnnLogError::LogMaterializer(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<KnnLogInput, KnnLogOutput> for KnnOperator {
    type Error = KnnLogError;

    async fn run(&self, input: &KnnLogInput) -> Result<KnnLogOutput, KnnLogError> {
        let materializer = LogMaterializer::new(
            input.segments.record_segment_reader().await?,
            input.logs.clone(),
            None,
        );
        let logs = materializer.materialize().await?;

        let metric = input.segments.knn_config()?.distance_function;
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
