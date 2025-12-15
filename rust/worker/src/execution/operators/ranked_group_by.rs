//! RankedGroupBy operator for grouping ranked search results by metadata keys.
//!
//! This operator takes ranked records from the RankExpr operator and:
//! 1. Groups records by one or more metadata field values
//! 2. Sorts records within each group by aggregate keys (MinK/MaxK)
//! 3. Keeps the top k records per group
//! 4. Flattens all groups back into a single list, re-sorted by score

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    blockfile_record::RecordSegmentReaderCreationError, types::LogMaterializerError,
};
use chroma_system::Operator;
use chroma_types::{
    operator::{GroupBy, RecordMeasure},
    Segment,
};
use thiserror::Error;

use crate::execution::operators::fetch_log::FetchLogOutput;

/// Input for the RankedGroupBy operator
#[derive(Clone, Debug)]
pub struct RankedGroupByInput {
    /// Ranked records (already sorted by score ascending from RankExpr)
    pub records: Vec<RecordMeasure>,
    /// Logs for metadata access
    pub logs: FetchLogOutput,
    /// Blockfile provider for segment access
    pub blockfile_provider: BlockfileProvider,
    /// Record segment for metadata lookup
    pub record_segment: Segment,
}

/// Output from the RankedGroupBy operator
#[derive(Clone, Debug)]
pub struct RankedGroupByOutput {
    /// Records after grouping and aggregation (re-sorted by score ascending)
    pub records: Vec<RecordMeasure>,
}

#[derive(Error, Debug)]
pub enum RankedGroupByError {
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
    #[error("Error reading record segment: {0}")]
    RecordSegment(#[from] Box<dyn ChromaError>),
    #[error("Error reading uninitialized record segment")]
    RecordSegmentUninitialized,
}

impl ChromaError for RankedGroupByError {
    fn code(&self) -> ErrorCodes {
        match self {
            RankedGroupByError::LogMaterializer(e) => e.code(),
            RankedGroupByError::RecordReader(e) => e.code(),
            RankedGroupByError::RecordSegment(e) => e.code(),
            RankedGroupByError::RecordSegmentUninitialized => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<RankedGroupByInput, RankedGroupByOutput> for GroupBy {
    type Error = RankedGroupByError;

    async fn run(
        &self,
        input: &RankedGroupByInput,
    ) -> Result<RankedGroupByOutput, RankedGroupByError> {
        // Fast path: no grouping configured
        if self.keys.is_empty() || self.aggregate.is_none() {
            return Ok(RankedGroupByOutput {
                records: input.records.clone(),
            });
        }

        // TODO: Implement the actual grouping logic:
        // 1. Fetch metadata for all input records (reuse Select operator pattern)
        // 2. Group records by group_by.keys (extract metadata values as group key)
        // 3. Within each group, sort by aggregate.keys (MinK=ascending, MaxK=descending)
        //    - Key::Score -> use record.measure
        //    - Key::MetadataField -> use MetadataValue::cmp()
        //    - Key::Document -> string comparison
        //    - Key::Embedding/Key::Metadata -> error (not sortable)
        // 4. Take top k from each group
        // 5. Flatten and re-sort by score ascending

        todo!("Implement RankedGroupBy operator logic")
    }
}
