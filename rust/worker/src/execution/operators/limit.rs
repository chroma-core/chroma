use std::cmp::Ordering;

use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Chunk, LogRecord, MaterializedLogOperation, Segment, SignedRoaringBitmap};
use roaring::RoaringBitmap;
use thiserror::Error;
use tonic::async_trait;
use tracing::{trace, Instrument, Span};

use crate::{
    execution::operator::Operator,
    segment::{
        record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
        LogMaterializer, LogMaterializerError,
    },
};

#[derive(Debug)]
pub struct LimitOperator {}

impl LimitOperator {
    pub fn new() -> Box<Self> {
        Box::new(LimitOperator {})
    }
}

#[derive(Debug)]
pub struct LimitInput {
    blockfile_provider: BlockfileProvider,
    record_segment: Segment,
    log_record: Chunk<LogRecord>,
    log_oids: SignedRoaringBitmap,
    compact_oids: SignedRoaringBitmap,
    skip: u32,
    fetch: Option<u32>,
}

impl LimitInput {
    pub fn new(
        blockfile_provider: BlockfileProvider,
        record_segment: Segment,
        log_record: Chunk<LogRecord>,
        log_oids: SignedRoaringBitmap,
        compact_oids: SignedRoaringBitmap,
        skip: u32,
        fetch: Option<u32>,
    ) -> Self {
        Self {
            blockfile_provider,
            record_segment,
            log_record,
            log_oids,
            compact_oids,
            skip,
            fetch,
        }
    }
}

#[derive(Debug)]
pub struct LimitOutput {
    pub log_records: Chunk<LogRecord>,
    pub offset_ids: RoaringBitmap,
}

#[derive(Error, Debug)]
pub enum LimitError {
    #[error("Error creating record segment reader {0}")]
    RecordSegmentReaderCreationError(#[from] RecordSegmentReaderCreationError),
    #[error("Error materializing logs {0}")]
    LogMaterializationError(#[from] LogMaterializerError),
    #[error("Error reading from record segment")]
    RecordSegmentReaderError,
}

impl ChromaError for LimitError {
    fn code(&self) -> ErrorCodes {
        use LimitError::*;
        match self {
            RecordSegmentReaderCreationError(e) => e.code(),
            LogMaterializationError(e) => e.code(),
            RecordSegmentReaderError => ErrorCodes::Internal,
        }
    }
}

// Sadly the std binary search cannot be directly used with async
// The following implementation is based on std implementation
#[async_trait]
trait AsyncCmp<T, E>
where
    T: Sync,
{
    // Returns how `cursor` compares with target
    async fn cmp(&self, cursor: &T) -> Result<Ordering, E>;
    async fn locate(&self, sorted_elements: &[T]) -> Result<Result<usize, usize>, E> {
        use Ordering::*;
        let mut size = sorted_elements.len();
        if size == 0 {
            return Ok(Err(0));
        }
        let mut base = 0usize;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;

            let cmp = self.cmp(&sorted_elements[mid]).await?;
            base = if cmp == Greater { base } else { mid };
            size -= half;
        }

        Ok(match self.cmp(&sorted_elements[base]).await? {
            Equal => Ok(base),
            Less => Err(base + 1),
            Greater => Err(base),
        })
    }
}

#[async_trait]
trait AsyncPart<T, E>
where
    T: Sync,
{
    // Returns true on first partition
    async fn pred(&self, cursor: &T) -> Result<bool, E>;
}

#[async_trait]
impl<T, E, P> AsyncCmp<T, E> for P
where
    T: Sync,
    P: AsyncPart<T, E> + Sync,
{
    async fn cmp(&self, cursor: &T) -> Result<Ordering, E> {
        use Ordering::*;
        if self.pred(cursor).await? {
            Ok(Less)
        } else {
            Ok(Greater)
        }
    }
}

// This struct help to find the actual index of an offset id in the compact segment
// given its index in an imaginary segment where the offset ids in the tombstone
// has been removed.
//
// The given index must be less than the length of the imaginary segment.
// The tombstone should be a sorted vec of (index, offset_id) tuples.
// The offset ids in the tombstone must be present in the compact segment.
struct CompactPart<'me> {
    index: usize,
    record_segment: &'me RecordSegmentReader<'me>,
    tombstone: &'me Vec<(usize, u32)>,
}

#[async_trait]
impl<'me> AsyncPart<(usize, u32), LimitError> for CompactPart<'me> {
    async fn pred(&self, cursor: &(usize, u32)) -> Result<bool, LimitError> {
        Ok(cursor.1
            <= self
                .record_segment
                .get_offset_id_at_index(self.index + cursor.0)
                .await
                .map_err(|_| LimitError::RecordSegmentReaderError)?)
    }
}

impl<'me> CompactPart<'me> {
    async fn partition_point(&self) -> Result<usize, LimitError> {
        Ok(self
            .locate(self.tombstone)
            .await?
            .unwrap_or_else(|index| index)
            + self.index)
    }
}

// This struct helps to find the starting indexes in both log and compact segment
// given a number of smallest offset ids to skip and the tombstone of
// offset ids that should be ignored in the compact segment.
//
// The tombstone should be a sorted vec of (index, offset_id) tuples.
// The offset ids in the tombstone must be present in the compact segment.
struct SkipCmp<'me> {
    skip: usize,
    sorted_log_oids: &'me Vec<(usize, u32)>,
    record_count: usize,
    record_segment: &'me RecordSegmentReader<'me>,
    tombstone: &'me Vec<(usize, u32)>,
}

#[async_trait]
impl<'me> AsyncCmp<(usize, u32), LimitError> for SkipCmp<'me> {
    async fn cmp(&self, cursor: &(usize, u32)) -> Result<Ordering, LimitError> {
        use Ordering::*;
        let (log_index, _) = cursor;
        if log_index + 1 < self.sorted_log_oids.len() && self.skip > *log_index {
            let compact_part = CompactPart {
                index: self.skip - log_index - 1,
                record_segment: self.record_segment,
                tombstone: self.tombstone,
            };
            if self.sorted_log_oids[log_index + 1].1
                < self
                    .record_segment
                    .get_offset_id_at_index(compact_part.partition_point().await?)
                    .await
                    .map_err(|_| LimitError::RecordSegmentReaderError)?
            {
                return Ok(Less);
            }
        } else if 0 < *log_index && self.skip + 1 < self.record_count + log_index {
            let compact_part = CompactPart {
                index: self.skip - log_index + 1,
                record_segment: self.record_segment,
                tombstone: self.tombstone,
            };
            if self
                .record_segment
                .get_offset_id_at_index(compact_part.partition_point().await?)
                .await
                .map_err(|_| LimitError::RecordSegmentReaderError)?
                < self.sorted_log_oids[log_index - 1].1
            {
                return Ok(Greater);
            }
        }
        Ok(Equal)
    }
}

impl<'me> SkipCmp<'me> {
    async fn skip_start(&self) -> Result<(usize, usize), LimitError> {
        let log_start = if self.skip > 0
            && self.record_count > self.tombstone.len()
            && !self.sorted_log_oids.is_empty()
        {
            self.locate(self.sorted_log_oids)
                // The binary search can only be performed when both log and compact segment are not empty.
                .await?
                .unwrap_or_else(|index| index)
        } else if self.record_count == self.tombstone.len() {
            self.skip
        } else {
            0
        };
        let compact_start = if self.record_count > self.tombstone.len() {
            let compact_part = CompactPart {
                index: self.skip - log_start,
                record_segment: self.record_segment,
                tombstone: self.tombstone,
            };
            compact_part.partition_point().await?
        } else {
            0
        };
        Ok((log_start, compact_start))
    }
}

#[async_trait]
impl Operator<LimitInput, LimitOutput> for LimitOperator {
    type Error = LimitError;

    fn get_name(&self) -> &'static str {
        "LimitOperator"
    }

    async fn run(&self, input: &LimitInput) -> Result<LimitOutput, LimitError> {
        use SignedRoaringBitmap::*;
        trace!(
            "[LimitOperator] segment id: {}",
            input.record_segment.id.to_string()
        );

        // Initialize record segment reader
        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => Ok(Some(reader)),
            // Uninitialized segment is fine and means that the record
            // segment is not yet initialized in storage
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => {
                tracing::error!("Error creating record segment reader {}", e);
                Err(LimitError::RecordSegmentReaderCreationError(*e))
            }
        }?;

        // Materialize the filtered offset ids from the materialized log
        let mut materialized_log_oids = match &input.log_oids {
            Include(rbm) => rbm.clone(),
            Exclude(rbm) => {
                // Materialize the logs
                let materializer = LogMaterializer::new(
                    record_segment_reader.clone(),
                    input.log_record.clone(),
                    None,
                );
                let materialized_logs = materializer
                    .materialize()
                    .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
                    .await
                    .map_err(|e| {
                        tracing::error!("Error materializing log: {}", e);
                        LimitError::LogMaterializationError(e)
                    })?;

                let active_domain: RoaringBitmap = materialized_logs
                    .iter()
                    .filter_map(|(log, _)| {
                        (!matches!(
                            log.final_operation,
                            MaterializedLogOperation::DeleteExisting
                        ))
                        .then_some(log.offset_id)
                    })
                    .collect();
                active_domain - rbm
            }
        };

        // Materialize all filtered offset ids with the compact segment
        let materialized_oids = match &input.compact_oids {
            Include(rbm) => {
                let mut merged_oids = materialized_log_oids | rbm;
                merged_oids.remove_smallest(input.skip as u64);
                if let Some(take_count) = input.fetch {
                    merged_oids.into_iter().take(take_count as usize).collect()
                } else {
                    merged_oids
                }
            }
            Exclude(rbm) => {
                if let Some(reader) = record_segment_reader.as_ref() {
                    let compact_count = reader
                        .count()
                        .await
                        .map_err(|_| LimitError::RecordSegmentReaderError)?;
                    let log_count = materialized_log_oids.len() as usize;
                    let filter_match_count = log_count + compact_count - rbm.len() as usize;
                    let truncated_skip = (input.skip as usize).min(filter_match_count);

                    let log_oid_sorted_vec =
                        materialized_log_oids.into_iter().enumerate().collect();
                    let tombstone = rbm.iter().enumerate().collect();

                    let skip_cmp = SkipCmp {
                        skip: truncated_skip,
                        sorted_log_oids: &log_oid_sorted_vec,
                        record_count: compact_count,
                        record_segment: reader,
                        tombstone: &tombstone,
                    };

                    let (mut log_index, mut compact_index) = skip_cmp.skip_start().await?;
                    let mut truncated_fetch = (input.fetch.unwrap_or(u32::MAX) as usize)
                        .min(filter_match_count - truncated_skip);

                    let mut merged_result = Vec::new();
                    while truncated_fetch > 0
                        && (log_index < log_count || compact_index < compact_count)
                    {
                        if compact_index == compact_count {
                            merged_result.push(log_oid_sorted_vec[log_index].1);
                            log_index += 1;
                        } else {
                            let compact_oid = reader
                                .get_offset_id_at_index(compact_index)
                                .await
                                .map_err(|_| LimitError::RecordSegmentReaderError)?;
                            if rbm.contains(compact_oid) {
                                compact_index += 1;
                                continue;
                            } else if log_index < log_count
                                && log_oid_sorted_vec[log_index].1 < compact_oid
                            {
                                merged_result.push(log_oid_sorted_vec[log_index].1);
                                log_index += 1;
                            } else {
                                merged_result.push(compact_oid);
                                compact_index += 1;
                            }
                        }
                        truncated_fetch -= 1;
                    }
                    RoaringBitmap::from_sorted_iter(merged_result)
                        .expect("Merged offset ids should be sorted")
                } else {
                    materialized_log_oids.remove_smallest(input.skip as u64);
                    if let Some(take_count) = input.fetch {
                        materialized_log_oids
                            .into_iter()
                            .take(take_count as usize)
                            .collect()
                    } else {
                        materialized_log_oids
                    }
                }
            }
        };

        Ok(LimitOutput {
            log_records: input.log_record.clone(),
            offset_ids: materialized_oids,
        })
    }
}
