use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_segment::{
    blockfile_metadata::{MetadataSegmentError, MetadataSegmentWriter},
    blockfile_record::{RecordSegmentWriter, RecordSegmentWriterCreationError},
    distributed_hnsw::{DistributedHNSWSegmentFromSegmentError, DistributedHNSWSegmentWriter},
    types::ChromaSegmentWriter,
};
use chroma_sysdb::{GetCollectionsError, SysDb};
use chroma_system::Operator;
use chroma_types::{Segment, SegmentType};
use thiserror::Error;
use tonic::async_trait;

#[derive(Debug, Default)]
pub struct GetSegmentWriterOperator {}

impl GetSegmentWriterOperator {
    pub fn new() -> Self {
        GetSegmentWriterOperator::default()
    }
}

#[derive(Debug)]
pub struct GetSegmentWriterInput {
    blockfile_provider: BlockfileProvider,
    hnsw_provider: HnswIndexProvider,
    sysdb: SysDb,
    segment: Segment,
}

impl GetSegmentWriterInput {
    pub fn new(
        blockfile_provider: BlockfileProvider,
        hnsw_provider: HnswIndexProvider,
        sysdb: SysDb,
        segment: Segment,
    ) -> Self {
        GetSegmentWriterInput {
            blockfile_provider,
            hnsw_provider,
            sysdb,
            segment,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GetSegmentWriterOutput {
    pub writer: ChromaSegmentWriter<'static>,
}

#[derive(Debug, Error)]
pub enum GetSegmentWriterError {
    #[error("Unsupported segment type: {:?}", 0)]
    UnsupportedSegmentType(SegmentType),
    #[error("Failed to create metadata segment writer: {0}")]
    MetadataSegmentWriter(#[from] MetadataSegmentError),
    #[error("Failed to create record segment writer: {0}")]
    RecordSegmentWriter(#[from] RecordSegmentWriterCreationError),
    #[error("Collection not found")]
    CollectionNotFound,
    #[error("Failed to get collection: {0}")]
    GetCollectionError(#[from] GetCollectionsError),
    #[error("Error creating HNSW segment writer: {0}")]
    HnswSegmentWriterError(#[from] Box<DistributedHNSWSegmentFromSegmentError>),
    #[error("Collection missing dimension (cannot create HNSW writer)")]
    CollectionMissingDimension,
}

impl ChromaError for GetSegmentWriterError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::UnsupportedSegmentType(_) => ErrorCodes::InvalidArgument,
            Self::MetadataSegmentWriter(err) => err.code(),
            Self::RecordSegmentWriter(err) => err.code(),
            Self::CollectionNotFound => ErrorCodes::NotFound,
            Self::GetCollectionError(err) => err.code(),
            Self::HnswSegmentWriterError(err) => err.code(),
            Self::CollectionMissingDimension => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<GetSegmentWriterInput, GetSegmentWriterOutput> for GetSegmentWriterOperator {
    type Error = GetSegmentWriterError;

    async fn run(
        &self,
        input: &GetSegmentWriterInput,
    ) -> Result<GetSegmentWriterOutput, Self::Error> {
        let writer = match input.segment.r#type {
            SegmentType::BlockfileMetadata => ChromaSegmentWriter::MetadataSegment(
                MetadataSegmentWriter::from_segment(&input.segment, &input.blockfile_provider)
                    .await?,
            ),
            SegmentType::BlockfileRecord => ChromaSegmentWriter::RecordSegment(
                RecordSegmentWriter::from_segment(&input.segment, &input.blockfile_provider)
                    .await?,
            ),
            SegmentType::HnswDistributed => {
                let collection_res = input
                    .sysdb
                    .clone()
                    .get_collections(Some(input.segment.collection), None, None, None)
                    .await;

                let collection_res = match collection_res {
                    Ok(collections) => {
                        if collections.is_empty() {
                            return Err(GetSegmentWriterError::CollectionNotFound);
                        }
                        collections
                    }
                    Err(e) => {
                        return Err(GetSegmentWriterError::GetCollectionError(e));
                    }
                };
                let collection = &collection_res[0];

                match collection.dimension {
                    Some(dimension) => {
                        let hnsw_segment_writer = match DistributedHNSWSegmentWriter::from_segment(
                            &input.segment,
                            dimension as usize,
                            input.hnsw_provider.clone(),
                        )
                        .await
                        {
                            Ok(writer) => writer,
                            Err(e) => {
                                return Err(GetSegmentWriterError::HnswSegmentWriterError(e));
                            }
                        };

                        ChromaSegmentWriter::DistributedHNSWSegment(hnsw_segment_writer)
                    }
                    None => {
                        return Err(GetSegmentWriterError::CollectionMissingDimension);
                    }
                }
            }
            _ => {
                return Err(GetSegmentWriterError::UnsupportedSegmentType(
                    input.segment.r#type,
                ))
            }
        };

        Ok(GetSegmentWriterOutput { writer })
    }

    fn get_type(&self) -> chroma_system::OperatorType {
        chroma_system::OperatorType::IO
    }
}
