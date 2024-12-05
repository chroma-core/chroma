use std::{cmp::Ordering, num::TryFromIntError, sync::atomic};

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
        materialize_logs,
        record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
        LogMaterializerError,
    },
};

/// The `LimitOperator` selects a range or records sorted by their offset ids
///
/// # Parameters
/// - `skip`: The number of records to skip in the beginning
/// - `fetch`: The number of records to fetch after `skip`
///
/// # Inputs
/// - `logs`: The latest logs of the collection
/// - `blockfile_provider`: The blockfile provider
/// - `record_segment`: The record segment information
/// - `log_offset_ids`: The offset ids in the logs to include or exclude before range selection
/// - `compact_offset_ids`: The offset ids in the blockfile to include or exclude before range selection
///
/// # Outputs
/// - `offset_ids`: The selected offset ids in either logs or blockfile
///
/// # Usage
/// It can be used to derive the range of offset ids that should be used by the next operator
#[derive(Clone, Debug)]
pub struct LimitOperator {
    pub skip: u32,
    pub fetch: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct LimitInput {
    pub logs: Chunk<LogRecord>,
    pub blockfile_provider: BlockfileProvider,
    pub record_segment: Segment,
    pub log_offset_ids: SignedRoaringBitmap,
    pub compact_offset_ids: SignedRoaringBitmap,
}

#[derive(Debug)]
pub struct LimitOutput {
    pub offset_ids: RoaringBitmap,
}

#[derive(Error, Debug)]
pub enum LimitError {
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Integer conversion out of bound: {0}")]
    OutOfBound(#[from] TryFromIntError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
    #[error("Error reading record segment: {0}")]
    RecordSegment(#[from] Box<dyn ChromaError>),
}

impl ChromaError for LimitError {
    fn code(&self) -> ErrorCodes {
        match self {
            LimitError::LogMaterializer(e) => e.code(),
            LimitError::OutOfBound(_) => ErrorCodes::OutOfRange,
            LimitError::RecordReader(e) => e.code(),
            LimitError::RecordSegment(e) => e.code(),
        }
    }
}

// This struct aims to help scanning a number of elements starting from a given offset
// in the imaginarysegment where the log is compacted and the element in the mask is ignored
struct SeekScanner<'me> {
    log_offset_ids: &'me RoaringBitmap,
    record_segment: &'me RecordSegmentReader<'me>,
    mask: &'me RoaringBitmap,
}

impl<'me> SeekScanner<'me> {
    // Find the rank of the target offset id in the imaginary segment
    //
    // The rank of a target is the number of elements strictly less than it
    //
    // Alternatively, it's the index in the imaginary segment that it can be
    // inserted into while maintaining the order of the imaginary segment
    async fn joint_rank(&self, target: u32) -> Result<u64, LimitError> {
        // # of elements strictly less than target in materialized log
        let log_rank =
            self.log_offset_ids.rank(target) - self.log_offset_ids.contains(target) as u64;
        // # of elements strictly less than target in the record segment
        let record_rank = self.record_segment.get_offset_id_rank(target).await? as u64;
        // # of elements strictly less than target in the mask
        let mask_rank = self.mask.rank(target) - self.mask.contains(target) as u64;
        Ok(log_rank + record_rank - mask_rank)
    }

    // Seek the starting offset given the number of elements to skip
    // There should be exactly skip elements before the starting offset in the maginary segment
    // The implementation is a binary search based on [`std::slice::binary_search_by`]
    //
    // [`std::slice::binary_search_by`]: https://github.com/rust-lang/rust/blob/705cfe0e966399e061d64dd3661bfbc57553ed87/library/core/src/slice/mod.rs#L2731-L2827
    // Retrieval timestamp: Nov 1, 2024
    // Source commit hash: a0215d8e46aab41219dea0bb1cbaaf97dafe2f89
    // Source license: Apache-2.0 or MIT
    async fn seek_starting_offset(&self, skip: u64) -> Result<u32, LimitError> {
        if skip == 0 {
            return Ok(0);
        }

        let mut size = self
            .record_segment
            .get_current_max_offset_id()
            .load(atomic::Ordering::Relaxed)
            .max(self.log_offset_ids.max().unwrap_or(0));
        if size == 0 {
            return Ok(0);
        }

        let mut base = 0;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;

            let cmp = self.joint_rank(mid).await?.cmp(&skip);
            base = if cmp == Ordering::Greater { base } else { mid };
            size -= half;
        }

        Ok(base)
    }

    // Seek the start in the log and record segment, then scan for the specified number of offset ids
    async fn seek_and_scan(&self, skip: u64, mut fetch: u64) -> Result<RoaringBitmap, LimitError> {
        let record_count = self.record_segment.count().await?;
        let starting_offset = self.seek_starting_offset(skip).await?;
        let mut log_index = self.log_offset_ids.rank(starting_offset)
            - self.log_offset_ids.contains(starting_offset) as u64;
        let mut record_index = self
            .record_segment
            .get_offset_id_rank(starting_offset)
            .await?;
        let mut merged_result = Vec::new();

        while fetch > 0 {
            let log_offset_id = self.log_offset_ids.select(u32::try_from(log_index)?);
            let record_offset_id = (record_index < record_count).then_some(
                self.record_segment
                    .get_offset_id_at_index(record_index)
                    .await?,
            );
            match (log_offset_id, record_offset_id) {
                (_, Some(oid)) if self.mask.contains(oid) => {
                    record_index += 1;
                    continue;
                }
                (Some(log_oid), Some(record_oid)) => {
                    if log_oid < record_oid {
                        merged_result.push(log_oid);
                        log_index += 1;
                    } else {
                        merged_result.push(record_oid);
                        record_index += 1;
                    }
                }
                (None, Some(oid)) => {
                    merged_result.push(oid);
                    record_index += 1;
                }
                (Some(oid), None) => {
                    merged_result.push(oid);
                    log_index += 1;
                }
                _ => break,
            };
            fetch -= 1;
        }

        Ok(RoaringBitmap::from_sorted_iter(merged_result)
            .expect("Merged offset ids should be sorted"))
    }
}

#[async_trait]
impl Operator<LimitInput, LimitOutput> for LimitOperator {
    type Error = LimitError;

    async fn run(&self, input: &LimitInput) -> Result<LimitOutput, LimitError> {
        trace!("[{}]: {:?}", self.get_name(), input);

        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => Ok(Some(reader)),
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => Err(*e),
        }?;

        // Materialize the filtered offset ids from the materialized log
        let mut materialized_log_offset_ids = match &input.log_offset_ids {
            SignedRoaringBitmap::Include(rbm) => rbm.clone(),
            SignedRoaringBitmap::Exclude(rbm) => {
                let materialized_logs = materialize_logs(&record_segment_reader, &input.logs, None)
                    .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
                    .await?;

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
        let materialized_offset_ids = match &input.compact_offset_ids {
            SignedRoaringBitmap::Include(rbm) => {
                let mut merged_offset_ids = materialized_log_offset_ids | rbm;
                merged_offset_ids.remove_smallest(self.skip as u64);
                if let Some(fetch_count) = self.fetch {
                    let truncated_fetch_count = merged_offset_ids.len().min(fetch_count as u64);
                    merged_offset_ids
                        .remove_biggest(merged_offset_ids.len() - truncated_fetch_count);
                }
                merged_offset_ids
            }
            SignedRoaringBitmap::Exclude(rbm) => {
                if let Some(reader) = record_segment_reader {
                    let record_count = reader.count().await?;
                    let log_count = materialized_log_offset_ids.len();
                    let filter_match_count = log_count + record_count as u64 - rbm.len();
                    let truncated_skip = (self.skip as u64).min(filter_match_count);
                    let truncated_fetch = (self.fetch.unwrap_or(u32::MAX) as u64)
                        .min(filter_match_count - truncated_skip);

                    let seek_scanner = SeekScanner {
                        log_offset_ids: &materialized_log_offset_ids,
                        record_segment: &reader,
                        mask: rbm,
                    };
                    seek_scanner
                        .seek_and_scan(truncated_skip, truncated_fetch)
                        .await?
                } else {
                    materialized_log_offset_ids.remove_smallest(self.skip as u64);
                    if let Some(take_count) = self.fetch {
                        materialized_log_offset_ids
                            .into_iter()
                            .take(take_count as usize)
                            .collect()
                    } else {
                        materialized_log_offset_ids
                    }
                }
            }
        };

        Ok(LimitOutput {
            offset_ids: materialized_offset_ids,
        })
    }
}

#[cfg(test)]
mod tests {
    use chroma_types::SignedRoaringBitmap;
    use roaring::RoaringBitmap;

    use crate::{
        execution::{operator::Operator, operators::limit::LimitOperator},
        log::test::{upsert_generator, LogGenerator},
        segment::test::TestSegment,
    };

    use super::LimitInput;

    /// The unit tests for `LimitOperator` uses the following test data
    /// It first generates 100 log records and compact them,
    /// then generate 30 log records that overwrite the compacted data
    /// - Log: Upsert [31..=60]
    /// - Compacted: Upsert [1..=100]
    async fn setup_limit_input(
        log_offset_ids: SignedRoaringBitmap,
        compact_offset_ids: SignedRoaringBitmap,
    ) -> LimitInput {
        let mut test_segment = TestSegment::default();
        let generator = LogGenerator {
            generator: upsert_generator,
        };
        test_segment.populate_with_generator(100, &generator).await;
        LimitInput {
            logs: generator.generate_chunk(31..=60),
            blockfile_provider: test_segment.blockfile_provider,
            record_segment: test_segment.record_segment,
            log_offset_ids,
            compact_offset_ids,
        }
    }

    #[tokio::test]
    async fn test_trivial_limit() {
        let limit_input = setup_limit_input(
            SignedRoaringBitmap::full(),
            SignedRoaringBitmap::Exclude((31..=60).collect()),
        )
        .await;

        let limit_operator = LimitOperator {
            skip: 0,
            fetch: None,
        };

        let limit_output = limit_operator
            .run(&limit_input)
            .await
            .expect("LimitOperator should not fail");

        assert_eq!(limit_output.offset_ids, (1..=100).collect());
    }

    #[tokio::test]
    async fn test_overskip() {
        let limit_input = setup_limit_input(
            SignedRoaringBitmap::full(),
            SignedRoaringBitmap::Exclude((31..=60).collect()),
        )
        .await;

        let limit_operator = LimitOperator {
            skip: 100,
            fetch: None,
        };

        let limit_output = limit_operator
            .run(&limit_input)
            .await
            .expect("LimitOperator should not fail");

        assert_eq!(limit_output.offset_ids, RoaringBitmap::new());
    }

    #[tokio::test]
    async fn test_overfetch() {
        let limit_input = setup_limit_input(
            SignedRoaringBitmap::full(),
            SignedRoaringBitmap::Exclude((31..=60).collect()),
        )
        .await;

        let limit_operator = LimitOperator {
            skip: 0,
            fetch: Some(1000),
        };

        let limit_output = limit_operator
            .run(&limit_input)
            .await
            .expect("LimitOperator should not fail");

        assert_eq!(limit_output.offset_ids, (1..=100).collect());
    }

    #[tokio::test]
    async fn test_simple_range() {
        let limit_input = setup_limit_input(
            SignedRoaringBitmap::full(),
            SignedRoaringBitmap::Exclude((31..=60).collect()),
        )
        .await;

        let limit_operator = LimitOperator {
            skip: 60,
            fetch: Some(30),
        };

        let limit_output = limit_operator
            .run(&limit_input)
            .await
            .expect("LimitOperator should not fail");

        assert_eq!(limit_output.offset_ids, (61..=90).collect());
    }

    #[tokio::test]
    async fn test_complex_limit() {
        let limit_input = setup_limit_input(
            SignedRoaringBitmap::Include((31..=60).filter(|offset| offset % 2 == 0).collect()),
            SignedRoaringBitmap::Exclude((21..=80).collect()),
        )
        .await;

        let limit_operator = LimitOperator {
            skip: 30,
            fetch: Some(20),
        };

        let limit_output = limit_operator
            .run(&limit_input)
            .await
            .expect("LimitOperator should not fail");

        assert_eq!(
            limit_output.offset_ids,
            (51..=60)
                .filter(|offset| offset % 2 == 0)
                .chain(81..=95)
                .collect()
        );
    }
}
