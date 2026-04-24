use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;

use chroma_segment::blockfile_metadata::{MetadataSegmentError, MetadataSegmentReaderShard};
use chroma_system::Operator;
use chroma_types::{
    operator::RecordMeasure, Segment, SegmentShard, SegmentShardError, SignedRoaringBitmap,
    SparseVector,
};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct SparseIndexKnnInput {
    pub blockfile_provider: BlockfileProvider,
    pub mask: SignedRoaringBitmap,
    pub metadata_segment: Segment,
    pub shard_index: u32,
}

#[derive(Clone, Debug)]
pub struct SparseIndexKnnOutput {
    pub records: Vec<RecordMeasure>,
}

#[derive(Debug, Error)]
pub enum SparseIndexKnnError {
    #[error("Error creating metadata segment reader: {0}")]
    MetadataReader(#[from] MetadataSegmentError),
    #[error(transparent)]
    Chroma(#[from] Box<dyn ChromaError>),
    #[error(transparent)]
    SegmentShard(#[from] SegmentShardError),
}

impl ChromaError for SparseIndexKnnError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            SparseIndexKnnError::MetadataReader(err) => err.code(),
            SparseIndexKnnError::Chroma(err) => err.code(),
            SparseIndexKnnError::SegmentShard(e) => e.code(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SparseIndexKnn {
    pub query: SparseVector,
    pub key: String,
    pub limit: u32,
}

#[async_trait]
impl Operator<SparseIndexKnnInput, SparseIndexKnnOutput> for SparseIndexKnn {
    type Error = SparseIndexKnnError;

    async fn run(
        &self,
        input: &SparseIndexKnnInput,
    ) -> Result<SparseIndexKnnOutput, SparseIndexKnnError> {
        let metadata_segment_shard =
            SegmentShard::try_from((&input.metadata_segment, input.shard_index))?;
        let metadata_segement_reader = Box::pin(MetadataSegmentReaderShard::from_segment(
            &metadata_segment_shard,
            &input.blockfile_provider,
        ))
        .await?;

        let Some(ref reader) = metadata_segement_reader.sparse_index_reader else {
            return Ok(SparseIndexKnnOutput {
                records: Vec::new(),
            });
        };

        let mut records = reader
            .knn_query(self.query.iter(), self.limit, input.mask.clone())
            .await?
            .into_iter()
            .map(|score| RecordMeasure {
                offset_id: score.offset,
                // We use `1 - query · document` as similarity metrics.
                measure: 1.0 - score.score,
            })
            .collect::<Vec<_>>();

        // NOTE: Sort results to ensure they're in ascending order by measure (then offset_id for ties)
        // This is required for KnnMerge which expects sorted inputs
        records.sort_unstable();

        Ok(SparseIndexKnnOutput { records })
    }
}
