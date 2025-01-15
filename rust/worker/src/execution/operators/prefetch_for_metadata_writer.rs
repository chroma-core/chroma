use crate::segment::metadata_segment::MetadataSegmentWriter;
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::fulltext::types::FullTextIndexError;
use chroma_system::{Operator, OperatorType};
use thiserror::Error;

#[derive(Debug, Default)]
pub struct PrefetchForMetadataWriterOperator {}

impl PrefetchForMetadataWriterOperator {
    pub fn new() -> PrefetchForMetadataWriterOperator {
        Self::default()
    }
}

#[derive(Debug)]
pub struct PrefetchForMetadataWriterInput<'a> {
    blockfile_provider: BlockfileProvider,
    writer: MetadataSegmentWriter<'a>,
}

impl PrefetchForMetadataWriterInput<'_> {
    pub fn new(
        blockfile_provider: BlockfileProvider,
        writer: MetadataSegmentWriter,
    ) -> PrefetchForMetadataWriterInput {
        PrefetchForMetadataWriterInput {
            blockfile_provider,
            writer,
        }
    }
}

pub type PrefetchForMetadataWriterOutput = ();

#[derive(Error, Debug)]
pub enum PrefetchForMetadataWriterError {
    #[error("Error while prefetching records: {0}")]
    FullText(#[from] FullTextIndexError),
}

impl ChromaError for PrefetchForMetadataWriterError {
    fn code(&self) -> ErrorCodes {
        match self {
            PrefetchForMetadataWriterError::FullText(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<PrefetchForMetadataWriterInput<'_>, PrefetchForMetadataWriterOutput>
    for PrefetchForMetadataWriterOperator
{
    type Error = PrefetchForMetadataWriterError;

    async fn run(
        &self,
        input: &PrefetchForMetadataWriterInput,
    ) -> Result<PrefetchForMetadataWriterOutput, PrefetchForMetadataWriterError> {
        if let Some(writer) = &input.writer.full_text_index_writer {
            writer
                .prefetch_before_writing_to_blockfiles(&input.blockfile_provider)
                .await?;
        }
        Ok(())
    }

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}
