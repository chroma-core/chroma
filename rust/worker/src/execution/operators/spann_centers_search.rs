use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::distributed_spann::SpannSegmentReader;
use chroma_system::Operator;
use thiserror::Error;

#[derive(Debug)]
pub(crate) struct SpannCentersSearchInput<'referred_data> {
    pub(crate) reader: Option<SpannSegmentReader<'referred_data>>,
    // Assumes that query is already normalized in case of cosine.
    pub(crate) normalized_query: Vec<f32>,
}

#[derive(Debug)]
pub(crate) struct SpannCentersSearchOutput {
    pub(crate) center_ids: Vec<usize>,
}

#[derive(Error, Debug)]
pub enum SpannCentersSearchError {
    #[error("Error creating spann segment reader")]
    SpannSegmentReaderCreationError,
    #[error("Error querying RNG")]
    RngQueryError,
}

impl ChromaError for SpannCentersSearchError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::SpannSegmentReaderCreationError => ErrorCodes::Internal,
            Self::RngQueryError => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SpannCentersSearchOperator {}

#[async_trait]
impl<'referred_data> Operator<SpannCentersSearchInput<'referred_data>, SpannCentersSearchOutput>
    for SpannCentersSearchOperator
{
    type Error = SpannCentersSearchError;

    async fn run(
        &self,
        input: &SpannCentersSearchInput,
    ) -> Result<SpannCentersSearchOutput, SpannCentersSearchError> {
        match &input.reader {
            Some(reader) => {
                // Use the reader to query the centers.
                let res = reader
                    .rng_query(&input.normalized_query)
                    .await
                    .map_err(|_| SpannCentersSearchError::RngQueryError)?;
                Ok(SpannCentersSearchOutput { center_ids: res.0 })
            }
            None => Err(SpannCentersSearchError::SpannSegmentReaderCreationError),
        }
    }
}
