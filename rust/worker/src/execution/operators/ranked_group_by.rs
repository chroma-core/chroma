//! RankedGroupBy operator for grouping ranked search results by metadata keys.
//!
//! This operator takes ranked records from the RankExpr operator and:
//! 1. Groups records by one or more metadata field values
//! 2. Sorts records within each group by aggregate keys (MinK/MaxK)
//! 3. Keeps the top k records per group
//! 4. Flattens all groups back into a single list, re-sorted by score

use std::collections::HashMap;

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{materialize_logs, LogMaterializerError},
};
use chroma_system::Operator;
use chroma_types::{
    operator::{Aggregate, GroupBy, Key, RecordMeasure},
    MetadataValue, Segment,
};
use thiserror::Error;
use tracing::{Instrument, Span};

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
    #[error("Phantom record not found: {0}")]
    PhantomRecord(u32),
}

impl ChromaError for RankedGroupByError {
    fn code(&self) -> ErrorCodes {
        match self {
            RankedGroupByError::LogMaterializer(e) => e.code(),
            RankedGroupByError::RecordReader(e) => e.code(),
            RankedGroupByError::RecordSegment(e) => e.code(),
            RankedGroupByError::RecordSegmentUninitialized => ErrorCodes::Internal,
            RankedGroupByError::PhantomRecord(_) => ErrorCodes::Internal,
        }
    }
}

/// Record enriched with metadata for grouping and sorting
#[derive(Clone)]
struct EnrichedRecord {
    record: RecordMeasure,
    metadata: HashMap<String, MetadataValue>,
}

impl EnrichedRecord {
    /// Extract key values for grouping or sorting
    fn extract_key(&self, keys: &[Key]) -> Vec<Option<MetadataValue>> {
        keys.iter()
            .map(|key| match key {
                Key::MetadataField(field) => self.metadata.get(field).cloned(),
                Key::Score => Some(MetadataValue::Float(self.record.measure as f64)),
                // Other keys shouldn't appear (validation prevents them)
                _ => None,
            })
            .collect()
    }
}

#[async_trait]
impl Operator<RankedGroupByInput, RankedGroupByOutput> for GroupBy {
    type Error = RankedGroupByError;

    async fn run(
        &self,
        input: &RankedGroupByInput,
    ) -> Result<RankedGroupByOutput, RankedGroupByError> {
        tracing::trace!("[RankedGroupBy] Running on {} records", input.records.len());

        // Fast path: no grouping configured or no records
        let aggregate = match &self.aggregate {
            Some(agg) if !self.keys.is_empty() && !input.records.is_empty() => agg,
            _ => {
                return Ok(RankedGroupByOutput {
                    records: input.records.clone(),
                });
            }
        };

        // --- Metadata hydration ---

        let record_segment_reader = match Box::pin(RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        ))
        .await
        {
            Ok(reader) => Some(reader),
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => None,
            Err(e) => return Err((*e).into()),
        };

        let materialized_logs = materialize_logs(&record_segment_reader, input.logs.clone(), None)
            .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
            .await?;

        let offset_id_to_log = materialized_logs
            .iter()
            .map(|log| (log.get_offset_id(), log))
            .collect::<HashMap<_, _>>();

        let records_in_segment = input
            .records
            .iter()
            .cloned()
            .filter(|record| !offset_id_to_log.contains_key(&record.offset_id))
            .collect::<Vec<_>>();

        // Enrich records with metadata
        let mut enriched_records = Vec::with_capacity(input.records.len());

        if !records_in_segment.is_empty() {
            let Some(reader) = &record_segment_reader else {
                return Err(RankedGroupByError::RecordSegmentUninitialized);
            };
            reader
                .load_id_to_data(records_in_segment.iter().map(|record| record.offset_id))
                .await;
            for record in records_in_segment {
                let metadata = reader
                    .get_data_for_offset_id(record.offset_id)
                    .await?
                    .ok_or(RankedGroupByError::PhantomRecord(record.offset_id))?
                    .metadata
                    .clone()
                    .unwrap_or_default();
                enriched_records.push(EnrichedRecord { record, metadata });
            }
        };

        for record in &input.records {
            if let Some(log) = offset_id_to_log.get(&record.offset_id) {
                let hydrated = log
                    .hydrate(record_segment_reader.as_ref())
                    .await
                    .map_err(RankedGroupByError::LogMaterializer)?;
                enriched_records.push(EnrichedRecord {
                    record: *record,
                    metadata: hydrated.merged_metadata(),
                });
            };
        }

        // --- Group by ---

        enriched_records.sort_by_cached_key(|r| r.extract_key(&self.keys));

        let groups = enriched_records
            .chunk_by(|a, b| a.extract_key(&self.keys) == b.extract_key(&self.keys));

        // --- Aggregate ---

        let mut records = groups
            .flat_map(|group| {
                let mut group_vec = group.to_vec();

                match aggregate {
                    Aggregate::MinK { keys, k } => {
                        group_vec.sort_by_cached_key(|record| record.extract_key(keys));
                        group_vec
                            .into_iter()
                            .take(*k as usize)
                            .map(|record| record.record)
                            .collect::<Vec<_>>()
                    }
                    Aggregate::MaxK { keys, k } => {
                        group_vec.sort_by_cached_key(|record| record.extract_key(keys));
                        group_vec.reverse();
                        group_vec
                            .into_iter()
                            .map(|record| record.record)
                            .take(*k as usize)
                            .collect::<Vec<_>>()
                    }
                }
            })
            .collect::<Vec<_>>();

        // --- Flatten and re-sort ---

        records.sort();

        Ok(RankedGroupByOutput { records })
    }
}
