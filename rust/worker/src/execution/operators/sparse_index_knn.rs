use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_index::sparse::reader::SparseReaderError;
use chroma_segment::blockfile_metadata::{MetadataSegmentError, MetadataSegmentReader};
use chroma_system::Operator;
use chroma_types::{operator::RecordMeasure, Segment, SignedRoaringBitmap, SparseVector};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct SparseIndexKnnInput {
    pub blockfile_provider: BlockfileProvider,
    pub mask: SignedRoaringBitmap,
    pub metadata_segment: Segment,
}

#[derive(Clone, Debug)]
pub struct SparseIndexKnnOutput {
    pub records: Vec<RecordMeasure>,
}

#[derive(Debug, Error)]
pub enum SparseIndexKnnError {
    #[error("Error creating metadata segment reader: {0}")]
    MetadataReader(#[from] MetadataSegmentError),
    #[error("Error using sparse reader: {0}")]
    SparseReader(#[from] SparseReaderError),
}

impl ChromaError for SparseIndexKnnError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            SparseIndexKnnError::MetadataReader(err) => err.code(),
            SparseIndexKnnError::SparseReader(err) => err.code(),
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
        let metadata_segment_reader = Box::pin(MetadataSegmentReader::from_segment(
            &input.metadata_segment,
            &input.blockfile_provider,
        ))
        .await?;

        let Some(sparse_reader) = metadata_segment_reader.sparse_index_reader else {
            return Ok(SparseIndexKnnOutput {
                records: Vec::new(),
            });
        };

        Ok(SparseIndexKnnOutput {
            records: sparse_reader
                .wand(self.query.iter(), self.limit, input.mask.clone())
                .await?
                .into_iter()
                .map(|score| RecordMeasure {
                    offset_id: score.offset,
                    // NOTE: We use `1 - query · document` as similarity metrics
                    measure: 1.0 - score.score,
                })
                .collect(),
        })
    }
}
