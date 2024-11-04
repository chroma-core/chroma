use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_types::{Collection, Segment, SignedRoaringBitmap};
use thiserror::Error;
use tonic::async_trait;

use crate::{
    execution::operator::Operator,
    segment::distributed_hnsw_segment::{
        DistributedHNSWSegmentFromSegmentError, DistributedHNSWSegmentReader,
    },
};

use super::knn::{KnnOperator, RecordDistance};

#[derive(Debug)]
struct KnnHnswInput {
    hnsw_provider: HnswIndexProvider,
    collection: Collection,
    hnsw_segment: Segment,
    compact_offset_ids: SignedRoaringBitmap,
}

#[derive(Debug)]
pub struct KnnHnswOutput {
    pub record_distances: Vec<RecordDistance>,
}

#[derive(Error, Debug)]
pub enum KnnHnswError {
    #[error("Error querying hnsw index: {0}")]
    HnswIndex(#[from] Box<dyn ChromaError>),
    #[error("Error creating hnsw segment reader: {0}")]
    HnswReader(#[from] DistributedHNSWSegmentFromSegmentError),
    #[error("Error resolving collection dimension")]
    NoCollectionDimension,
}

impl ChromaError for KnnHnswError {
    fn code(&self) -> ErrorCodes {
        match self {
            KnnHnswError::HnswReader(e) => e.code(),
            KnnHnswError::HnswIndex(e) => e.code(),
            KnnHnswError::NoCollectionDimension => ErrorCodes::InvalidArgument,
        }
    }
}

#[async_trait]
impl Operator<KnnHnswInput, KnnHnswOutput> for KnnOperator {
    type Error = KnnHnswError;

    async fn run(&self, input: &KnnHnswInput) -> Result<KnnHnswOutput, KnnHnswError> {
        let (allowed, disallowed) = match &input.compact_offset_ids {
            SignedRoaringBitmap::Include(rbm) if rbm.is_empty() => {
                return Ok(KnnHnswOutput {
                    record_distances: Vec::new(),
                })
            }
            SignedRoaringBitmap::Include(rbm) => (
                rbm.iter().map(|offset_id| offset_id as usize).collect(),
                Vec::new(),
            ),
            SignedRoaringBitmap::Exclude(rbm) => (
                Vec::new(),
                rbm.iter().map(|offset_id| offset_id as usize).collect(),
            ),
        };

        match DistributedHNSWSegmentReader::from_segment(
            &input.hnsw_segment,
            input
                .collection
                .dimension
                .ok_or(KnnHnswError::NoCollectionDimension)? as usize,
            input.hnsw_provider.clone(),
        )
        .await
        {
            Ok(reader) => {
                let (offset_ids, distances) =
                    reader.query(&self.embedding, self.fetch as usize, &allowed, &disallowed)?;
                Ok(KnnHnswOutput {
                    record_distances: offset_ids
                        .into_iter()
                        .map(|offset_id| offset_id as u32)
                        .zip(distances)
                        .map(|(offset_id, measure)| RecordDistance { offset_id, measure })
                        .collect(),
                })
            }
            Err(e) if matches!(*e, DistributedHNSWSegmentFromSegmentError::Uninitialized) => {
                Ok(KnnHnswOutput {
                    record_distances: Vec::new(),
                })
            }
            Err(e) => Err((*e).into()),
        }
    }
}
