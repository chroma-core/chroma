use thiserror::Error;
use tonic::async_trait;
use uuid::Uuid;

use crate::{
    blockstore::provider::BlockfileProvider,
    errors::{ChromaError, ErrorCodes},
    execution::operator::Operator,
    segment::record_segment::RecordSegmentReader,
    types::Segment,
};

#[derive(Debug)]
pub(crate) enum RecordSegmentBlockId {
    // Block id.
    OffsetIdToDataBlockId(Uuid),
    UserIdToOffsetIdBlockId(Uuid),
    OffsetIdToUserIdBlockId(Uuid),
}

#[derive(Debug)]
pub(crate) enum MetadataBlockId {
    // Block id.
    StringMetadataBlockId(Uuid),
    F32MetadataBlockId(Uuid),
    BoolMetadataBlockId(Uuid),
    U32MetadataBlockId(Uuid),
}

#[derive(Debug)]
pub(crate) enum FullTextBlockId {
    PostingsListBlockId(Uuid),
    FrequenciesBlockId(Uuid),
}

#[derive(Debug)]
pub(crate) enum MetadataSegmentBlockId {
    MetadataBlockId(MetadataBlockId),
    FullTextBlockId(FullTextBlockId),
}

#[derive(Debug)]
pub(crate) struct RecordSegmentInfo {
    pub(crate) block_id: RecordSegmentBlockId,
    pub(crate) segment: Segment,
    pub(crate) provider: BlockfileProvider,
}

#[derive(Debug)]
pub(crate) struct MetadataSegmentInfo {
    block_id: MetadataSegmentBlockId,
    segment: Segment,
    provider: BlockfileProvider,
}

#[derive(Debug)]
pub(crate) struct HnswSegmentInfo {
    // TODO.
}

#[derive(Debug)]
pub(crate) enum PrefetchData {
    RecordSegmentPrefetch(RecordSegmentInfo),
    MetadataSegmentPrefetch(MetadataSegmentInfo),
    HnswSegmentPrefetch(HnswSegmentInfo),
}

#[derive(Debug)]
pub(crate) struct PrefetchIoInput {
    pub(crate) data: PrefetchData,
}

#[derive(Debug)]
pub(crate) struct PrefetchIoOutput {
    // This is fire and forget so nothing to return.
}

#[derive(Debug)]
pub(crate) struct PrefetchIoOperator {}

impl PrefetchIoOperator {
    pub fn new() -> Box<Self> {
        Box::new(PrefetchIoOperator {})
    }
}

#[derive(Error, Debug)]
pub(crate) enum PrefetchIoOperatorError {
    #[error("Error creating Record Segment reader")]
    RecordSegmentReaderCreationError,
}

impl ChromaError for PrefetchIoOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::RecordSegmentReaderCreationError => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<PrefetchIoInput, PrefetchIoOutput> for PrefetchIoOperator {
    type Error = PrefetchIoOperatorError;

    fn get_name(&self) -> &'static str {
        "PrefetchIoOperator"
    }

    async fn run(&self, input: &PrefetchIoInput) -> Result<PrefetchIoOutput, Self::Error> {
        match &input.data {
            PrefetchData::RecordSegmentPrefetch(prefetch_info) => {
                // Construct record segment reader.
                let record_segment_reader = match RecordSegmentReader::from_segment(
                    &prefetch_info.segment,
                    &prefetch_info.provider,
                )
                .await
                {
                    Ok(reader) => reader,
                    Err(_) => {
                        return Err(PrefetchIoOperatorError::RecordSegmentReaderCreationError);
                    }
                };
                match &prefetch_info.block_id {
                    RecordSegmentBlockId::OffsetIdToDataBlockId(block_id) => {
                        record_segment_reader
                            .prefetch_block_for_id_to_data(*block_id)
                            .await;
                    }
                    RecordSegmentBlockId::OffsetIdToUserIdBlockId(block_id) => {
                        record_segment_reader
                            .prefetch_block_for_id_to_user_id(*block_id)
                            .await;
                    }
                    RecordSegmentBlockId::UserIdToOffsetIdBlockId(block_id) => {
                        record_segment_reader
                            .prefetch_block_for_user_id_to_id(*block_id)
                            .await;
                    }
                }
            }
            // TODO: implement others.
            _ => todo!(),
        }
        Ok(PrefetchIoOutput {})
    }
}
