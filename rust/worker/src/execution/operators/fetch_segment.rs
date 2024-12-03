use std::collections::HashMap;

use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Collection, CollectionUuid, Segment, SegmentScope, SegmentType, SegmentUuid};
use thiserror::Error;
use tonic::async_trait;
use tracing::trace;

use crate::{
    execution::operator::{Operator, OperatorType},
    sysdb::sysdb::{GetCollectionsError, GetSegmentsError, SysDb},
};

/// The `FetchSegmentOperator` fetches collection and segment information from SysDB
///
/// # Parameters
/// - `sysdb`: The SysDB reader
/// - `*_uuid`: The uuids of the collection and segments
/// - `collection_version`: The version of the collection to verify against
///
/// # Inputs
/// - No input is required
///
/// # Outputs
/// - `collection`: The collection information
/// - `*_segment`: The segment information
///
/// # Usage
/// It should be run at the start of an orchestrator to get the latest data of a collection
#[derive(Clone, Debug)]
pub struct FetchSegmentOperator {
    pub(crate) sysdb: Box<SysDb>,
    pub collection_uuid: CollectionUuid,
    pub collection_version: u32,
    pub metadata_uuid: SegmentUuid,
    pub record_uuid: SegmentUuid,
    pub vector_uuid: SegmentUuid,
}

type FetchSegmentInput = ();

#[derive(Clone, Debug)]
pub struct FetchSegmentOutput {
    pub collection: Collection,
    pub metadata_segment: Segment,
    pub record_segment: Segment,
    pub vector_segment: Segment,
}

#[derive(Error, Debug)]
pub enum FetchSegmentError {
    #[error("Error when getting collection: {0}")]
    GetCollection(#[from] GetCollectionsError),
    #[error("Error when getting segment: {0}")]
    GetSegment(#[from] GetSegmentsError),
    #[error("No collection found")]
    NoCollection,
    #[error("No segment found")]
    NoSegment,
    // The frontend relies on ths content of the error message here to detect version mismatch
    // TODO: Refactor frontend to properly detect version mismatch
    #[error("Collection version mismatch")]
    VersionMismatch,
}

impl ChromaError for FetchSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            FetchSegmentError::GetCollection(e) => e.code(),
            FetchSegmentError::GetSegment(e) => e.code(),
            FetchSegmentError::NoCollection => ErrorCodes::NotFound,
            FetchSegmentError::NoSegment => ErrorCodes::NotFound,
            FetchSegmentError::VersionMismatch => ErrorCodes::VersionMismatch,
        }
    }
}

impl FetchSegmentOperator {
    async fn get_collection(&self) -> Result<Collection, FetchSegmentError> {
        let collection = self
            .sysdb
            .clone()
            .get_collections(Some(self.collection_uuid), None, None, None)
            .await?
            .pop()
            .ok_or(FetchSegmentError::NoCollection)?;
        if collection.version != self.collection_version as i32 {
            Err(FetchSegmentError::VersionMismatch)
        } else {
            trace!(
                "[Debug-FetchSegment] Collection <{}: {}> version: {}",
                collection.name,
                collection.collection_id,
                collection.version,
            );
            Ok(collection)
        }
    }
    async fn get_segment(&self, scope: SegmentScope) -> Result<Segment, FetchSegmentError> {
        let segment_type = match scope {
            SegmentScope::METADATA => SegmentType::BlockfileMetadata,
            SegmentScope::RECORD => SegmentType::BlockfileRecord,
            SegmentScope::SQLITE => unimplemented!("Unexpected Sqlite segment"),
            SegmentScope::VECTOR => SegmentType::HnswDistributed,
        };
        let segment_id = match scope {
            SegmentScope::METADATA => self.metadata_uuid,
            SegmentScope::RECORD => self.record_uuid,
            SegmentScope::SQLITE => unimplemented!("Unexpected Sqlite segment"),
            SegmentScope::VECTOR => self.vector_uuid,
        };
        self.sysdb
            .clone()
            .get_segments(
                Some(segment_id),
                Some(segment_type.into()),
                Some(scope),
                self.collection_uuid,
            )
            .await?
            // Each scope should have a single segment
            .pop()
            .ok_or(FetchSegmentError::NoSegment)
    }

    async fn fetch_segments(&self) -> Result<(Segment, Segment, Segment), FetchSegmentError> {
        let mut segments: HashMap<_, _> = self
            .sysdb
            .clone()
            .get_segments(None, None, None, self.collection_uuid)
            .await?
            .into_iter()
            .map(|segment| (segment.scope.clone(), segment))
            .collect();

        if let (Some(meta), Some(rec), Some(vec)) = (
            segments.remove(&SegmentScope::METADATA),
            segments.remove(&SegmentScope::RECORD),
            segments.remove(&SegmentScope::VECTOR),
        ) {
            Ok((meta, rec, vec))
        } else {
            Err(FetchSegmentError::NoSegment)
        }
    }
}

#[async_trait]
impl Operator<FetchSegmentInput, FetchSegmentOutput> for FetchSegmentOperator {
    type Error = FetchSegmentError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(&self, _: &FetchSegmentInput) -> Result<FetchSegmentOutput, FetchSegmentError> {
        trace!("[{}]: {:?}", self.get_name(), self);

        let (metadata_segment, record_segment, vector_segment) = self.fetch_segments().await?;

        Ok(FetchSegmentOutput {
            collection: self.get_collection().await?,
            metadata_segment,
            record_segment,
            vector_segment,
        })
    }
}
