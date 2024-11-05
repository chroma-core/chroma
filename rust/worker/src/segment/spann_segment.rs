use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{hnsw_provider::HnswIndexProvider, spann::types::SpannIndexWriter};
use chroma_types::{Segment, SegmentType};
use thiserror::Error;
use uuid::Uuid;

use super::utils::{distance_function_from_segment, hnsw_params_from_segment};

pub(crate) struct SpannSegmentWriter {
    index: SpannIndexWriter,
    id: Uuid,
}

#[derive(Error, Debug)]
pub enum SpannSegmentWriterError {
    #[error("Invalid argument")]
    InvalidArgument,
    #[error("Distance function not found")]
    DistanceFunctionNotFound,
    #[error("Hnsw index id parsing error")]
    IndexIdParsingError,
    #[error("HNSW index construction error")]
    HnswIndexConstructionError,
}

impl ChromaError for SpannSegmentWriterError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::InvalidArgument => ErrorCodes::InvalidArgument,
            Self::IndexIdParsingError => ErrorCodes::Internal,
            Self::HnswIndexConstructionError => ErrorCodes::Internal,
            Self::DistanceFunctionNotFound => ErrorCodes::Internal,
        }
    }
}

impl SpannSegmentWriter {
    pub async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
        hnsw_provider: HnswIndexProvider,
        dimensionality: usize,
    ) -> Result<SpannSegmentWriter, SpannSegmentWriterError> {
        // TODO(Sanket): Introduce another segment type and propagate here.
        if segment.r#type != SegmentType::HnswDistributed {
            return Err(SpannSegmentWriterError::InvalidArgument);
        }
        match segment.file_path.get("hnsw_path") {
            Some(hnsw_path) => match hnsw_path.first() {
                Some(index_id) => {
                    let index_uuid = match Uuid::parse_str(index_id) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(SpannSegmentWriterError::IndexIdParsingError);
                        }
                    };
                    let distance_function = match distance_function_from_segment(segment) {
                        Ok(distance_function) => distance_function,
                        Err(e) => {
                            return Err(SpannSegmentWriterError::DistanceFunctionNotFound);
                        }
                    };
                    let hnsw_index = match SpannIndexWriter::hnsw_index_from_id(
                        &hnsw_provider,
                        &index_uuid,
                        &segment.collection,
                        distance_function,
                        dimensionality,
                    )
                    .await
                    {
                        Ok(index) => index,
                        Err(_) => {
                            return Err(SpannSegmentWriterError::HnswIndexConstructionError);
                        }
                    };

                    // TODO(Sanket): Remove this.
                    return Err(SpannSegmentWriterError::InvalidArgument);
                }
                // TODO: Create index in this case also.
                None => {
                    return Err(SpannSegmentWriterError::InvalidArgument);
                }
            },
            // TODO(Sanket): Create index in this case.
            None => {
                let hnsw_params = hnsw_params_from_segment(segment);

                let distance_function = match distance_function_from_segment(segment) {
                    Ok(distance_function) => distance_function,
                    Err(e) => {
                        return Err(SpannSegmentWriterError::DistanceFunctionNotFound);
                    }
                };

                let hnsw_index = match SpannIndexWriter::create_hnsw_index(
                    &hnsw_provider,
                    &segment.collection,
                    distance_function,
                    dimensionality,
                    hnsw_params,
                )
                .await
                {
                    Ok(index) => index,
                    Err(_) => {
                        return Err(SpannSegmentWriterError::HnswIndexConstructionError);
                    }
                };

                // First time creation of the segment.
                return Err(SpannSegmentWriterError::InvalidArgument);
            }
        }
    }
}
