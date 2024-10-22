use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{hnsw_provider::HnswIndexProvider, IndexConfig, IndexConfigFromSegmentError};
use chroma_types::{Collection, Segment, SegmentScope, SegmentType};
use thiserror::Error;
use tonic::async_trait;
use tracing::trace;
use uuid::Uuid;

use crate::{
    execution::operator::Operator,
    segment::{
        distributed_hnsw_segment::{
            DistributedHNSWSegmentFromSegmentError, DistributedHNSWSegmentReader,
        },
        metadata_segment::{MetadataSegmentError, MetadataSegmentReader},
        record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
    },
    sysdb::sysdb::{GetCollectionsError, GetSegmentsError, SysDb},
};

#[derive(Clone, Debug)]
pub struct FetchSegmentOperator {
    // Data provider
    pub(crate) sysdb: Box<SysDb>,
    pub hnsw: HnswIndexProvider,
    pub blockfile: BlockfileProvider,
    // pub knn: Uuid,
    pub metadata: Uuid,
    // pub record: Uuid,
    pub collection: Uuid,
    // Version
    pub version: u32,
}

pub type FetchSegmentInput = ();

#[derive(Clone, Debug)]
pub struct FetchSegmentOutput {
    pub hnsw: HnswIndexProvider,
    pub blockfile: BlockfileProvider,
    pub knn: Segment,
    pub metadata: Segment,
    pub record: Segment,
    pub collection: Collection,
}

#[derive(Error, Debug)]
pub enum FetchSegmentError {
    #[error("Error when getting collection: {0}")]
    GetCollection(#[from] GetCollectionsError),
    #[error("Error when getting segment: {0}")]
    GetSegment(#[from] GetSegmentsError),
    #[error("Error when getting HNSW index config: {0}")]
    HNSWConfigError(#[from] IndexConfigFromSegmentError),
    #[error("Unable to create HNSW segment reader: {0}")]
    HNSWSegmentReaderCreation(#[from] DistributedHNSWSegmentFromSegmentError),
    #[error("Unable to create metadata segment reader: {0}")]
    MetadataSegmentReaderCreation(#[from] MetadataSegmentError),
    #[error("No collection found")]
    NoCollection,
    #[error("No collection dimensionality")]
    NoCollectionDimension,
    #[error("No segment found")]
    NoSegment,
    #[error("Unable to create record segment reader: {0}")]
    RecordSegmentReaderCreation(#[from] RecordSegmentReaderCreationError),
    #[error("Version mismatch")]
    VersionMismatch,
}

impl ChromaError for FetchSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            FetchSegmentError::GetCollection(e) => e.code(),
            FetchSegmentError::GetSegment(e) => e.code(),
            FetchSegmentError::HNSWConfigError(e) => e.code(),
            FetchSegmentError::HNSWSegmentReaderCreation(e) => e.code(),
            FetchSegmentError::MetadataSegmentReaderCreation(e) => e.code(),
            FetchSegmentError::NoCollection => ErrorCodes::NotFound,
            FetchSegmentError::NoCollectionDimension => ErrorCodes::InvalidArgument,
            FetchSegmentError::NoSegment => ErrorCodes::NotFound,
            FetchSegmentError::RecordSegmentReaderCreation(e) => e.code(),
            FetchSegmentError::VersionMismatch => ErrorCodes::VersionMismatch,
        }
    }
}

impl FetchSegmentOutput {
    pub(super) async fn knn_segment_reader(
        &self,
    ) -> Result<DistributedHNSWSegmentReader, FetchSegmentError> {
        DistributedHNSWSegmentReader::from_segment(
            &self.knn,
            self.collection
                .dimension
                .ok_or(FetchSegmentError::NoCollectionDimension)? as usize,
            self.hnsw.clone(),
        )
        .await
        .map(|reader| *reader)
        .map_err(|e| (*e).into())
    }

    pub(super) fn knn_config(&self) -> Result<IndexConfig, FetchSegmentError> {
        Ok(IndexConfig::from_segment(
            &self.knn,
            self.collection
                .dimension
                .ok_or(FetchSegmentError::NoCollectionDimension)?,
        )
        .map_err(|e| *e)?)
    }

    pub(super) async fn metadata_segment_reader(
        &self,
    ) -> Result<MetadataSegmentReader, FetchSegmentError> {
        Ok(MetadataSegmentReader::from_segment(&self.metadata, &self.blockfile).await?)
    }

    pub(super) async fn record_segment_reader(
        &self,
    ) -> Result<Option<RecordSegmentReader>, FetchSegmentError> {
        match RecordSegmentReader::from_segment(&self.record, &self.blockfile).await {
            Ok(reader) => Ok(Some(reader)),
            Err(err) if matches!(*err, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => Err((*e).into()),
        }
    }
}

impl FetchSegmentOperator {
    async fn get_collection(&self) -> Result<Collection, FetchSegmentError> {
        let collection = self
            .sysdb
            .clone()
            .get_collections(Some(self.collection), None, None, None)
            .await?
            // Each collection should have a single UUID
            .pop()
            .ok_or(FetchSegmentError::NoCollection)?;
        if collection.version != self.version as i32 {
            Err(FetchSegmentError::VersionMismatch)
        } else {
            Ok(collection)
        }
    }
    async fn get_segment(&self, scope: SegmentScope) -> Result<Segment, FetchSegmentError> {
        let segment_type = match scope {
            SegmentScope::VECTOR => SegmentType::HnswDistributed,
            SegmentScope::METADATA => SegmentType::BlockfileMetadata,
            SegmentScope::RECORD => SegmentType::BlockfileRecord,
            SegmentScope::SQLITE => unimplemented!("Unexpected Sqlite segment"),
        };
        // TODO: Add vector and record segment id
        let segment_id = match scope {
            SegmentScope::VECTOR => None,
            SegmentScope::METADATA => Some(self.metadata),
            SegmentScope::RECORD => None,
            SegmentScope::SQLITE => unimplemented!("Unexpected Sqlite segment"),
        };
        self.sysdb
            .clone()
            .get_segments(
                segment_id,
                Some(segment_type.into()),
                Some(scope),
                self.collection,
            )
            .await?
            // Each scope should have a single segment
            .pop()
            .ok_or(FetchSegmentError::NoSegment)
    }
}

#[async_trait]
impl Operator<FetchSegmentInput, FetchSegmentOutput> for FetchSegmentOperator {
    type Error = FetchSegmentError;
    async fn run(&self, _: &FetchSegmentInput) -> Result<FetchSegmentOutput, FetchSegmentError> {
        trace!("[{}]: {:?}", self.get_name(), self);

        Ok(FetchSegmentOutput {
            hnsw: self.hnsw.clone(),
            blockfile: self.blockfile.clone(),
            knn: self.get_segment(SegmentScope::VECTOR).await?,
            metadata: self.get_segment(SegmentScope::METADATA).await?,
            record: self.get_segment(SegmentScope::RECORD).await?,
            collection: self.get_collection().await?,
        })
    }
}
