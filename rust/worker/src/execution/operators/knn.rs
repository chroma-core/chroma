use std::{cmp::Ordering, collections::BinaryHeap};

use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{MaterializedLogOperation, SignedRoaringBitmap};
use thiserror::Error;
use tonic::async_trait;

use crate::{execution::operator::Operator, segment::LogMaterializerError};

use super::scan::{ScanError, ScanOutput};

pub fn normalize(vector: &[f32]) -> Vec<f32> {
    let norm = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        vector.iter().map(|x| x / norm).collect()
    } else {
        vector.to_vec()
    }
}

#[derive(Debug)]
struct KNNOperator {
    embedding: Vec<f32>,
    fetch: u32,
}

#[derive(Debug)]
struct KNNInput {
    scan: ScanOutput,
    log_oids: SignedRoaringBitmap,
    compact_oids: SignedRoaringBitmap,
}

#[derive(Debug)]
pub struct KNNOutput {
    pub scan: ScanOutput,
    pub distances: Vec<Distance>,
}

#[derive(Error, Debug)]
pub enum KNNError {
    #[error("Error querying knn index: {0}")]
    KNNIndex(Box<dyn ChromaError>),
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error processing scan output: {0}")]
    Scan(#[from] ScanError),
    #[error("Err")]
    Err,
}

impl ChromaError for KNNError {
    fn code(&self) -> ErrorCodes {
        use KNNError::*;
        match self {
            Scan(e) => e.code(),
            KNNIndex(e) => e.code(),
            _ => ErrorCodes::Internal,
        }
    }
}

#[derive(Clone, Debug)]
struct Distance {
    oid: u32,
    measure: f32,
}

impl PartialEq for Distance {
    fn eq(&self, other: &Self) -> bool {
        self.measure.eq(&other.measure)
    }
}

impl Eq for Distance {}

impl Ord for Distance {
    fn cmp(&self, other: &Self) -> Ordering {
        self.measure.total_cmp(&other.measure)
    }
}

impl PartialOrd for Distance {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl KNNOperator {
    async fn knn_log(&self, input: &KNNInput) -> Result<Vec<Distance>, KNNError> {
        let materializer = input.scan.log_materializer().await?;
        let logs = materializer.materialize().await?;

        let metric = input.scan.knn_config()?.distance_function;
        let target_vector;
        let target_embedding = if let DistanceFunction::Cosine = metric {
            target_vector = normalize(&self.embedding);
            &target_vector
        } else {
            &self.embedding
        };

        let mut heap = BinaryHeap::with_capacity(self.fetch as usize);

        for (log, _) in logs.iter() {
            if !matches!(
                log.final_operation,
                MaterializedLogOperation::DeleteExisting
            ) {
                let log_vector;
                let log_embedding = if let DistanceFunction::Cosine = metric {
                    log_vector = normalize(log.merged_embeddings());
                    &log_vector
                } else {
                    log.merged_embeddings()
                };

                let distance = Distance {
                    oid: log.offset_id,
                    measure: metric.distance(target_embedding, log_embedding),
                };
                if heap.len() < self.fetch as usize {
                    heap.push(distance);
                } else if let Some(far) = heap.peek() {
                    if &distance < far {
                        heap.pop();
                        heap.push(distance);
                    }
                }
            }
        }
        Ok(heap.into_sorted_vec())
    }

    async fn knn_index(&self, input: &KNNInput) -> Result<Vec<Distance>, KNNError> {
        use SignedRoaringBitmap::*;
        let (allowed, disallowed) = match &input.compact_oids {
            Include(rbm) if rbm.is_empty() => return Ok(Vec::new()),
            Include(rbm) => (rbm.iter().map(|oid| oid as usize).collect(), Vec::new()),
            Exclude(rbm) => (Vec::new(), rbm.iter().map(|oid| oid as usize).collect()),
        };
        match input.scan.knn_segment_reader().await?.query(
            &self.embedding,
            self.fetch as usize,
            &allowed,
            &disallowed,
        ) {
            Ok((oids, distances)) => Ok(oids
                .into_iter()
                .map(|oid| oid as u32)
                .zip(distances)
                .map(|(oid, measure)| Distance { oid, measure })
                .collect()),
            Err(e) => Err(KNNError::KNNIndex(e)),
        }
    }
}

#[async_trait]
impl Operator<KNNInput, KNNOutput> for KNNOperator {
    type Error = KNNError;

    async fn run(&self, input: &KNNInput) -> Result<KNNOutput, KNNError> {
        let knn_log = self.knn_log(input).await?;
        let knn_index = self.knn_index(input).await?;

        let mut fetch = self.fetch;
        let mut knn_merged = Vec::new();
        let mut log_cursor = 0;
        let mut index_cursor = 0;

        while fetch > 0 {
            let log_dist = knn_log.get(log_cursor);
            let index_dist = knn_index.get(index_cursor);
            match (log_dist, index_dist) {
                (Some(l), Some(r)) => {
                    if l.measure < r.measure {
                        knn_merged.push(l.clone());
                        log_cursor += 1;
                    } else {
                        knn_merged.push(r.clone());
                        index_cursor += 1;
                    }
                }
                (None, Some(dist)) => {
                    knn_merged.push(dist.clone());
                    index_cursor += 1;
                }
                (Some(dist), None) => {
                    knn_merged.push(dist.clone());
                    log_cursor += 1;
                }
                _ => {}
            };
            fetch -= 1;
        }

        Ok(KNNOutput {
            scan: input.scan.clone(),
            distances: knn_merged,
        })
    }
}
