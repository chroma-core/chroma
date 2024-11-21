use crate::execution::operator::Operator;
use crate::segment::types::SegmentFlusher;
use crate::segment::ChromaSegmentFlusher;
use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use chroma_types::SegmentFlushInfo;
use thiserror::Error;
use tracing::trace_span;
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

    fn get_type(&self) -> crate::execution::operator::OperatorType {
        crate::execution::operator::OperatorType::IO
    }

    async fn run(
        &self,
        input: &FlushSegmentWriterInput,
    ) -> Result<FlushSegmentWriterOutput, Self::Error> {
        let file_paths = input
            .segment_flusher
            .clone()
            .flush()
            .instrument(trace_span!(
                "Flush segment",
                otel.name = format!("Flush {:?}", input.segment_flusher.get_name()),
                segment = input.segment_flusher.get_name()
            ))
            .await?;

        Ok(FlushSegmentWriterOutput {
            flush_info: SegmentFlushInfo {
                file_paths,
                segment_id: input.segment_flusher.get_id(),
            },
        })
    }
}
