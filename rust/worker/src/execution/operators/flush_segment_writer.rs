use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use chroma_segment::types::ChromaSegmentFlusher;
use chroma_system::Operator;
use chroma_types::SegmentFlushInfo;
use parking_lot::Mutex;
use thiserror::Error;
use tracing::trace_span;
use tracing::Instrument;

#[derive(Error, Debug)]
pub enum FlushSegmentWriterOperatorError {
    #[error("Finishing segment writer failed {0}")]
    FinishSegmentWriterFailed(#[from] Box<dyn ChromaError>),
    #[error("Segment flusher is missing")]
    SegmentFlusherMissing,
}

impl ChromaError for FlushSegmentWriterOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            FlushSegmentWriterOperatorError::FinishSegmentWriterFailed(e) => e.code(),
            FlushSegmentWriterOperatorError::SegmentFlusherMissing => ErrorCodes::Internal,
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
    segment_flusher: Mutex<Option<ChromaSegmentFlusher>>,
}

impl FlushSegmentWriterInput {
    pub fn new(segment_flusher: ChromaSegmentFlusher) -> Self {
        FlushSegmentWriterInput {
            segment_flusher: Mutex::new(Some(segment_flusher)),
        }
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

    fn get_type(&self) -> chroma_system::OperatorType {
        chroma_system::OperatorType::IO
    }

    async fn run(
        &self,
        input: &FlushSegmentWriterInput,
    ) -> Result<FlushSegmentWriterOutput, Self::Error> {
        let segment_flusher = input
            .segment_flusher
            .lock()
            .take()
            .ok_or(FlushSegmentWriterOperatorError::SegmentFlusherMissing)?;

        let name = segment_flusher.get_name();
        let id = segment_flusher.get_id();

        let file_paths = Box::pin(segment_flusher.flush().instrument(trace_span!(
            "Flush segment",
            otel.name = format!("Flush {:?}", name),
            segment = name
        )))
        .await?;

        Ok(FlushSegmentWriterOutput {
            flush_info: SegmentFlushInfo {
                file_paths,
                segment_id: id,
            },
        })
    }
}
