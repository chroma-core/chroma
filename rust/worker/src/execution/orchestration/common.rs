use crate::{
    errors::{ChromaError, ErrorCodes},
    sysdb::sysdb::{GetSegmentsError, SysDb},
    types::{Segment, SegmentType},
};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub(super) enum HnswSegmentQueryError {
    #[error("Hnsw segment with id: {0} not found")]
    HnswSegmentNotFound(Uuid),
    #[error("Get segments error")]
    GetSegmentsError(#[from] GetSegmentsError),
}

impl ChromaError for HnswSegmentQueryError {
    fn code(&self) -> ErrorCodes {
        match self {
            HnswSegmentQueryError::HnswSegmentNotFound(_) => ErrorCodes::NotFound,
            HnswSegmentQueryError::GetSegmentsError(_) => ErrorCodes::Internal,
        }
    }
}

pub(super) async fn get_hnsw_segment_from_id(
    mut sysdb: Box<dyn SysDb>,
    hnsw_segment_id: &Uuid,
) -> Result<Segment, Box<dyn ChromaError>> {
    let segments = sysdb
        .get_segments(Some(*hnsw_segment_id), None, None, None)
        .await;
    let segment = match segments {
        Ok(segments) => {
            if segments.is_empty() {
                return Err(Box::new(HnswSegmentQueryError::HnswSegmentNotFound(
                    *hnsw_segment_id,
                )));
            }
            segments[0].clone()
        }
        Err(e) => {
            return Err(Box::new(HnswSegmentQueryError::GetSegmentsError(e)));
        }
    };

    if segment.r#type != SegmentType::HnswDistributed {
        return Err(Box::new(HnswSegmentQueryError::HnswSegmentNotFound(
            *hnsw_segment_id,
        )));
    }
    Ok(segment)
}
