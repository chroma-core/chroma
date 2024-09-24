use crate::{
    sysdb::sysdb::{GetCollectionsError, GetSegmentsError, SysDb},
    system::{Component, ComponentContext},
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Collection, Segment, SegmentType};
use thiserror::Error;
use tracing::{trace_span, Instrument, Span};
use uuid::Uuid;

#[derive(Debug, Error)]
pub(super) enum GetHnswSegmentByIdError {
    #[error("Hnsw segment with id: {0} not found")]
    HnswSegmentNotFound(Uuid),
    #[error("Get segments error: {0}")]
    GetSegmentsError(#[from] GetSegmentsError),
}

impl ChromaError for GetHnswSegmentByIdError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetHnswSegmentByIdError::HnswSegmentNotFound(_) => ErrorCodes::NotFound,
            GetHnswSegmentByIdError::GetSegmentsError(e) => e.code(),
        }
    }
}

pub(super) async fn get_hnsw_segment_by_id(
    mut sysdb: Box<SysDb>,
    hnsw_segment_id: &Uuid,
    collection_id: &Uuid,
) -> Result<Segment, Box<GetHnswSegmentByIdError>> {
    let segments = sysdb
        .get_segments(Some(*hnsw_segment_id), None, None, *collection_id)
        .await;
    let segment = match segments {
        Ok(segments) => {
            if segments.is_empty() {
                return Err(Box::new(GetHnswSegmentByIdError::HnswSegmentNotFound(
                    *hnsw_segment_id,
                )));
            }
            segments[0].clone()
        }
        Err(e) => {
            return Err(Box::new(GetHnswSegmentByIdError::GetSegmentsError(e)));
        }
    };

    if segment.r#type != SegmentType::HnswDistributed {
        return Err(Box::new(GetHnswSegmentByIdError::HnswSegmentNotFound(
            *hnsw_segment_id,
        )));
    }
    Ok(segment)
}

#[derive(Debug, Error)]
pub(super) enum GetCollectionByIdError {
    #[error("Collection with id: {0} not found")]
    CollectionNotFound(Uuid),
    #[error("Get collection error")]
    GetCollectionError(#[from] GetCollectionsError),
}

impl ChromaError for GetCollectionByIdError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionByIdError::CollectionNotFound(_) => ErrorCodes::NotFound,
            GetCollectionByIdError::GetCollectionError(e) => e.code(),
        }
    }
}

pub(super) async fn get_collection_by_id(
    mut sysdb: Box<SysDb>,
    collection_id: &Uuid,
) -> Result<Collection, Box<GetCollectionByIdError>> {
    let child_span: tracing::Span =
        trace_span!(parent: Span::current(), "get collection for collection id");
    let collections = sysdb
        .get_collections(Some(*collection_id), None, None, None)
        .instrument(child_span.clone())
        .await;
    match collections {
        Ok(mut collections) => {
            if collections.is_empty() {
                return Err(Box::new(GetCollectionByIdError::CollectionNotFound(
                    *collection_id,
                )));
            }
            Ok(collections.drain(..).next().unwrap())
        }
        Err(e) => {
            Err(Box::new(GetCollectionByIdError::GetCollectionError(e)))
        }
    }
}

#[derive(Debug, Error)]
pub(super) enum GetRecordSegmentByCollectionIdError {
    #[error("Record segment for collection with id: {0} not found")]
    RecordSegmentNotFound(Uuid),
    #[error("Get segments error: {0}")]
    GetSegmentsError(#[from] GetSegmentsError),
}

impl ChromaError for GetRecordSegmentByCollectionIdError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetRecordSegmentByCollectionIdError::RecordSegmentNotFound(_) => ErrorCodes::NotFound,
            GetRecordSegmentByCollectionIdError::GetSegmentsError(e) => e.code(),
        }
    }
}

pub(super) async fn get_record_segment_by_collection_id(
    mut sysdb: Box<SysDb>,
    collection_id: &Uuid,
) -> Result<Segment, Box<GetRecordSegmentByCollectionIdError>> {
    let segments = sysdb
        .get_segments(
            None,
            Some(SegmentType::BlockfileRecord.into()),
            None,
            *collection_id,
        )
        .await;

    let segment = match segments {
        Ok(mut segments) => {
            if segments.is_empty() {
                return Err(Box::new(
                    GetRecordSegmentByCollectionIdError::RecordSegmentNotFound(*collection_id),
                ));
            }
            segments.drain(..).next().unwrap()
        }
        Err(e) => {
            return Err(Box::new(
                GetRecordSegmentByCollectionIdError::GetSegmentsError(e),
            ));
        }
    };

    if segment.r#type != SegmentType::BlockfileRecord {
        return Err(Box::new(
            GetRecordSegmentByCollectionIdError::RecordSegmentNotFound(*collection_id),
        ));
    }
    Ok(segment)
}

/// Terminate the orchestrator with an error
/// This function sends an error to the result channel and cancels the orchestrator
/// so it stops processing
/// # Arguments
/// * `result_channel` - The result channel to send the error to
/// * `error` - The error to send
/// * `ctx` - The component context
/// # Panics
/// This function panics if the result channel is not set
pub(super) fn terminate_with_error<Output, C>(
    mut result_channel: Option<tokio::sync::oneshot::Sender<Result<Output, Box<dyn ChromaError>>>>,
    error: Box<dyn ChromaError>,
    ctx: &ComponentContext<C>,
) where
    C: Component,
{
    let result_channel = result_channel
        .take()
        .expect("Invariant violation. Result channel is not set.");
    match result_channel.send(Err(error)) {
        Ok(_) => (),
        Err(_) => {
            tracing::error!("Result channel dropped before sending error");
        }
    }
    // Cancel the orchestrator so it stops processing
    ctx.cancellation_token.cancel();
}
