use thiserror::Error;
use tonic::async_trait;

use crate::{
    blockstore::provider::BlockfileProvider,
    errors::{ChromaError, ErrorCodes},
    execution::operator::Operator,
    segment::record_segment::RecordSegmentReader,
    types::Segment,
};

#[derive(Debug)]
pub(crate) struct OffsetIdToDataKeys {
    pub(crate) keys: Vec<u32>,
}

#[derive(Debug)]
pub(crate) struct UserIdToOffsetIdKeys {
    pub(crate) keys: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct OffsetIdToUserIdKeys {
    pub(crate) keys: Vec<u32>,
}

#[derive(Debug)]
pub(crate) enum RecordSegmentKeys {
    OffsetIdToDataKeys(OffsetIdToDataKeys),
    UserIdToOffsetIdKeys(UserIdToOffsetIdKeys),
    OffsetIdToUserIdKeys(OffsetIdToUserIdKeys),
}

#[derive(Debug)]
pub(crate) struct StringMetadataKeys {
    pub(crate) prefixes: Vec<String>,
    pub(crate) keys: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct F32MetadataKeys {
    pub(crate) prefixes: Vec<String>,
    pub(crate) keys: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct BoolMetadataKeys {
    pub(crate) prefixes: Vec<String>,
    pub(crate) keys: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct U32MetadataKeys {
    pub(crate) prefixes: Vec<String>,
    pub(crate) keys: Vec<String>,
}

#[derive(Debug)]
pub(crate) enum MetadataKeys {
    StringMetadataKeys(StringMetadataKeys),
    F32MetadataKeys(F32MetadataKeys),
    BoolMetadataKeys(BoolMetadataKeys),
    U32MetadataKeys(U32MetadataKeys),
}

#[derive(Debug)]
pub(crate) struct PostingsListKeys {
    pub(crate) prefixes: Vec<String>,
    // Doc Id.
    pub(crate) keys: Vec<u32>,
}

#[derive(Debug)]
pub(crate) struct FrequenciesKeys {
    pub(crate) prefixes: Vec<String>,
    // Frequency.
    pub(crate) keys: Vec<u32>,
}

#[derive(Debug)]
pub(crate) enum FullTextKeys {
    PostingsListKeys(PostingsListKeys),
    FrequenciesKeys(FrequenciesKeys),
}

#[derive(Debug)]
pub(crate) enum MetadataSegmentKeys {
    MetadataKeys(MetadataKeys),
    FullTextKeys(FullTextKeys),
}

#[derive(Debug)]
pub(crate) struct RecordSegmentInfo {
    pub(crate) keys: RecordSegmentKeys,
    pub(crate) segment: Segment,
    pub(crate) provider: BlockfileProvider,
}

#[derive(Debug)]
pub(crate) struct MetadataSegmentInfo {
    keys: MetadataSegmentKeys,
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
                match &prefetch_info.keys {
                    // TODO: Remove clone.
                    RecordSegmentKeys::OffsetIdToDataKeys(keys) => {
                        record_segment_reader.prefetch_id_to_data(&keys.keys).await;
                    }
                    RecordSegmentKeys::OffsetIdToUserIdKeys(keys) => {
                        record_segment_reader
                            .prefetch_id_to_user_id(&keys.keys)
                            .await;
                    }
                    RecordSegmentKeys::UserIdToOffsetIdKeys(keys) => {
                        record_segment_reader
                            .prefetch_user_id_to_id(keys.keys.iter().map(|x| x.as_str()).collect())
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
