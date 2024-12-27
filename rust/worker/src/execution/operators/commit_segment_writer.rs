use crate::execution::operator::Operator;
use crate::segment::ChromaSegmentFlusher;
use crate::segment::ChromaSegmentWriter;
use crate::segment::SegmentWriter;
use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use thiserror::Error;
use tracing::Instrument;

#[derive(Error, Debug)]
pub enum CommitSegmentWriterOperatorError {
    #[error("Finishing segment writer failed {0}")]
    FinishSegmentWriterFailed(#[from] Box<dyn ChromaError>),
}

impl ChromaError for CommitSegmentWriterOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            CommitSegmentWriterOperatorError::FinishSegmentWriterFailed(e) => e.code(),
        }
    }
}

#[derive(Debug)]
pub struct CommitSegmentWriterOperator {}

impl CommitSegmentWriterOperator {
    pub fn new() -> Box<Self> {
        Box::new(CommitSegmentWriterOperator {})
    }
}

#[derive(Debug)]
pub struct CommitSegmentWriterInput<'a> {
    segment_writer: ChromaSegmentWriter<'a>,
}

impl<'a> CommitSegmentWriterInput<'a> {
    pub fn new(segment_writer: ChromaSegmentWriter<'a>) -> Self {
        CommitSegmentWriterInput { segment_writer }
    }
}

#[derive(Debug)]
pub struct CommitSegmentWriterOutput {
    pub flusher: ChromaSegmentFlusher,
}

#[async_trait]
impl<'a> Operator<CommitSegmentWriterInput<'a>, CommitSegmentWriterOutput>
    for CommitSegmentWriterOperator
{
    type Error = CommitSegmentWriterOperatorError;

    fn get_name(&self) -> &'static str {
        "CommitSegmentWriterOperator"
    }

    async fn run(
        &self,
        input: &CommitSegmentWriterInput<'a>,
    ) -> Result<CommitSegmentWriterOutput, Self::Error> {
        let mut segment_writer = input.segment_writer.clone();

        segment_writer
            .finish()
            .instrument(tracing::info_span!(
                "segment_writer.finish()",
                otel.name = format!(".finish() on {:?}", input.segment_writer.get_name()),
                segment = input.segment_writer.get_name()
            ))
            .await
            .map_err(CommitSegmentWriterOperatorError::FinishSegmentWriterFailed)?;

        let flusher = segment_writer.commit().await?;

        Ok(CommitSegmentWriterOutput { flusher })
    }
}
