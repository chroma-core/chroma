use crate::{
    execution::operator::Operator,
    log::log::{Log, PullLogsError},
    segment::{
        distributed_hnsw_segment::{
            DistributedHNSWSegmentFromSegmentError, DistributedHNSWSegmentReader,
        },
        metadata_segment::{MetadataSegmentError, MetadataSegmentReader},
        record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
        LogMaterializer,
    },
    sysdb::sysdb::{GetCollectionsError, GetSegmentsError, SysDb},
};
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{hnsw_provider::HnswIndexProvider, IndexConfig, IndexConfigFromSegmentError};
use chroma_types::{Chunk, Collection, LogRecord, Segment, SegmentScope, SegmentType};
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use thiserror::Error;
use tonic::async_trait;
use uuid::Uuid;

#[derive(Debug)]
pub struct ScanOperator {
    // Log
    log_client: Box<Log>,
    batch_size: u32,
    skip: u32,
    fetch: Option<u32>,
    // Data provider
    sysdb: Box<SysDb>,
    hnsw: HnswIndexProvider,
    blockfile: BlockfileProvider,
    // Identification
    knn: Uuid,
    metadata: Uuid,
    record: Uuid,
    collection: Uuid,
    // Version
    version: u32,
}

type ScanInput = ();

#[derive(Clone, Debug)]
pub struct ScanOutput {
    logs: Chunk<LogRecord>,
    hnsw: HnswIndexProvider,
    blockfile: BlockfileProvider,
    knn: Segment,
    metadata: Segment,
    record: Segment,
    collection: Collection,
}

#[derive(Error, Debug)]
pub enum ScanError {
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
    #[error("Error when pulling log: {0}")]
    PullLog(#[from] PullLogsError),
    #[error("Unable to create record segment reader: {0}")]
    RecordSegmentReaderCreation(#[from] RecordSegmentReaderCreationError),
    #[error("Error when capturing system time: {0}")]
    SystemTime(#[from] SystemTimeError),
    #[error("Version mismatch")]
    VersionMismatch,
}

impl ChromaError for ScanError {
    fn code(&self) -> ErrorCodes {
        use ScanError::*;
        match self {
            GetCollection(e) => e.code(),
            GetSegment(e) => e.code(),
            HNSWConfigError(e) => e.code(),
            HNSWSegmentReaderCreation(e) => e.code(),
            MetadataSegmentReaderCreation(e) => e.code(),
            NoCollection => ErrorCodes::NotFound,
            NoCollectionDimension => ErrorCodes::InvalidArgument,
            NoSegment => ErrorCodes::NotFound,
            PullLog(e) => e.code(),
            RecordSegmentReaderCreation(e) => e.code(),
            SystemTime(_) => ErrorCodes::Internal,
            VersionMismatch => ErrorCodes::VersionMismatch,
        }
    }
}

impl ScanOutput {
    pub async fn log_materializer(&self) -> Result<LogMaterializer, ScanError> {
        Ok(LogMaterializer::new(
            self.record_segment_reader().await?,
            self.logs.clone(),
            None,
        ))
    }

    pub async fn knn_segment_reader(&self) -> Result<DistributedHNSWSegmentReader, ScanError> {
        DistributedHNSWSegmentReader::from_segment(
            &self.knn,
            self.collection
                .dimension
                .ok_or(ScanError::NoCollectionDimension)? as usize,
            self.hnsw.clone(),
        )
        .await
        .map(|reader| *reader)
        .map_err(|e| (*e).into())
    }

    pub fn knn_config(&self) -> Result<IndexConfig, ScanError> {
        Ok(IndexConfig::from_segment(
            &self.knn,
            self.collection
                .dimension
                .ok_or(ScanError::NoCollectionDimension)?,
        )
        .map_err(|e| *e)?)
    }

    pub async fn metadata_segment_reader(&self) -> Result<MetadataSegmentReader, ScanError> {
        Ok(MetadataSegmentReader::from_segment(&self.metadata, &self.blockfile).await?)
    }

    pub async fn record_segment_reader(&self) -> Result<Option<RecordSegmentReader>, ScanError> {
        use RecordSegmentReaderCreationError::UninitializedSegment;
        match RecordSegmentReader::from_segment(&self.record, &self.blockfile).await {
            Ok(reader) => Ok(Some(reader)),
            Err(err) if matches!(*err, UninitializedSegment) => Ok(None),
            Err(e) => Err((*e).into()),
        }
    }
}

impl ScanOperator {
    async fn pull_log(&self) -> Result<Chunk<LogRecord>, ScanError> {
        let mut fetched = Vec::new();
        let mut log_client = self.log_client.clone();
        let mut offset = self.skip as i64;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as i64;
        loop {
            if let Some(limit) = self.fetch {
                if fetched.len() >= limit as usize {
                    // Enough logs have been fetched
                    fetched.truncate(limit as usize);
                    break;
                }
            }

            let mut log_batch = log_client
                .read(
                    self.collection,
                    offset,
                    self.batch_size as i32,
                    Some(timestamp),
                )
                .await?;

            if let Some(last_log) = log_batch.last() {
                offset = last_log.log_offset + 1;
                fetched.append(&mut log_batch);
            } else {
                // No more logs to fetch
                break;
            }
        }
        tracing::info!(name: "Pulled log records", num_records = fetched.len());
        Ok(Chunk::new(fetched.into()))
    }

    async fn get_collection(&self) -> Result<Collection, ScanError> {
        let collection = self
            .sysdb
            .clone()
            .get_collections(Some(self.collection), None, None, None)
            .await?
            // Each collection should have a single UUID
            .pop()
            .ok_or(ScanError::NoCollection)?;
        if collection.version != self.version as i32 {
            Err(ScanError::VersionMismatch)
        } else {
            Ok(collection)
        }
    }

    async fn get_segment(&self, scope: SegmentScope) -> Result<Segment, ScanError> {
        use SegmentScope::*;
        use SegmentType::*;
        let segment_type = match scope {
            VECTOR => HnswDistributed,
            METADATA => BlockfileMetadata,
            RECORD => BlockfileRecord,
            SQLITE => unimplemented!("Unexpected Sqlite segment"),
        };
        self.sysdb
            .clone()
            .get_segments(
                Some(self.knn),
                Some(segment_type.into()),
                Some(scope),
                self.collection,
            )
            .await?
            // Each scope should have a single segment
            .pop()
            .ok_or(ScanError::NoSegment)
    }
}

#[async_trait]
impl Operator<ScanInput, ScanOutput> for ScanOperator {
    type Error = ScanError;
    async fn run(&self, _: &ScanInput) -> Result<ScanOutput, ScanError> {
        use SegmentScope::*;
        Ok(ScanOutput {
            logs: self.pull_log().await?,
            hnsw: self.hnsw.clone(),
            blockfile: self.blockfile.clone(),
            knn: self.get_segment(VECTOR).await?,
            metadata: self.get_segment(METADATA).await?,
            record: self.get_segment(RECORD).await?,
            collection: self.get_collection().await?,
        })
    }
}
