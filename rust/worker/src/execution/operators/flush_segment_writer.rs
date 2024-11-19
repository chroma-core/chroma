use crate::execution::operator::Operator;
use crate::segment::types::SegmentFlusher;
use crate::segment::ChromaSegmentFlusher;
use crate::segment::SegmentWriter;
use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use chroma_types::SegmentFlushInfo;
use thiserror::Error;
use tracing::Instrument;

#[derive(Error, Debug)]
pub enum FlushSegmentWriterOperatorError {
    #[error("Finishing segment writer failed {0}")]
    FinishSegmentWriterFailed(#[from] Box<dyn ChromaError>),
}

impl ChromaError for FlushSegmentWriterOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            FlushSegmentWriterOperatorError::FinishSegmentWriterFailed(e) => e.code(),
        }
    }
}

#[derive(Debug)]
pub struct FlushSegmentWriterOperator {}

impl FlushSegmentWriterOperator {
    pub fn new() -> Box<Self> {
        Box::new(FlushSegmentWriterOperator {})
    }
}

#[derive(Debug)]
pub struct FlushSegmentWriterInput {
    segment_flusher: ChromaSegmentFlusher,
}

impl FlushSegmentWriterInput {
    pub fn new(segment_flusher: ChromaSegmentFlusher) -> Self {
        FlushSegmentWriterInput { segment_flusher }
    }
}

#[derive(Debug)]
pub struct FlushSegmentWriterOutput {
    pub flush_info: SegmentFlushInfo,
}

#[async_trait]
impl Operator<FlushSegmentWriterInput, FlushSegmentWriterOutput> for FlushSegmentWriterOperator {
    type Error = FlushSegmentWriterOperatorError;

    fn get_name(&self) -> &'static str {
        "FlushSegmentWriterOperator"
    }

    async fn run(
        &self,
        input: &FlushSegmentWriterInput,
    ) -> Result<FlushSegmentWriterOutput, Self::Error> {
        // let mut segment_writer = input.segment_writer.clone();

        // segment_writer
        //     .finish()
        //     .instrument(tracing::info_span!(
        //         "segment_writer.finish()",
        //         segment = input.segment_writer.get_name()
        //     ))
        //     .await
        //     .map_err(FlushSegmentWriterOperatorError::FinishSegmentWriterFailed)?;

        // let segment_id = segment_writer.get_id();
        // let flusher = segment_writer.commit().await?;

        let file_paths = input.segment_flusher.clone().flush().await?;

        Ok(FlushSegmentWriterOutput {
            flush_info: SegmentFlushInfo {
                file_paths,
                segment_id: input.segment_flusher.get_id(),
            },
        })
    }
}
