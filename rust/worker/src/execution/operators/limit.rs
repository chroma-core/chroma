use std::{cmp::Ordering, num::TryFromIntError};

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{materialize_logs, LogMaterializerError},
};
use chroma_system::Operator;
use chroma_types::{
    operator::Limit, Chunk, LogRecord, MaterializedLogOperation, Segment, SignedRoaringBitmap,
};
use futures::StreamExt;
use roaring::RoaringBitmap;
use thiserror::Error;
use tracing::{Instrument, Span};

/// The `Limit` operator selects a range or records sorted by their offset ids
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

impl SeekScanner<'_> {
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

    // Seek the starting offset given the number of elements to offset
    // There should be exactly offset elements before the starting offset in the maginary segment
    // The implementation is a binary search based on [`std::slice::binary_search_by`]
    //
    // [`std::slice::binary_search_by`]: https://github.com/rust-lang/rust/blob/705cfe0e966399e061d64dd3661bfbc57553ed87/library/core/src/slice/mod.rs#L2731-L2827
    // Retrieval timestamp: Nov 1, 2024
    // Source commit hash: a0215d8e46aab41219dea0bb1cbaaf97dafe2f89
    // Source license: Apache-2.0 or MIT
    async fn seek_starting_offset(&self, offset: u64) -> Result<u32, LimitError> {
        if offset == 0 {
            return Ok(0);
        }

        let mut size = self
            .record_segment
            .get_max_offset_id()
            .max(self.log_offset_ids.max().unwrap_or(0));
        if size == 0 {
            return Ok(0);
        }

        let mut base = 0;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;

            let cmp = self.joint_rank(mid).await?.cmp(&offset);
            base = if cmp == Ordering::Greater { base } else { mid };
            size -= half;
        }

        // The above loop tests all midpoints. However, it does not test the very last element.
        // We want the greatest offset such that self.join_rank(offset) <= offset, so we need to test the last element as well.
        let cmp = self.joint_rank(base).await?.cmp(&offset);
        Ok(base + (cmp == Ordering::Less) as u32)
    }

    // Seek the start in the log and record segment, then scan for the specified number of offset ids
    async fn seek_and_scan(
        &self,
        offset: u64,
        mut limit: u64,
    ) -> Result<RoaringBitmap, LimitError> {
        let starting_offset = self.seek_starting_offset(offset).await?;
        let mut log_index = self.log_offset_ids.rank(starting_offset)
            - self.log_offset_ids.contains(starting_offset) as u64;
        let mut log_offset_id = self.log_offset_ids.select(u32::try_from(log_index)?);
        let mut record_offset_stream = self.record_segment.get_offset_stream(starting_offset..);
        let mut record_offset_id = record_offset_stream.next().await.transpose()?;
        let mut merged_result = Vec::new();

        while limit > 0 {
            match (log_offset_id, record_offset_id) {
                (_, Some(oid)) if self.mask.contains(oid) => {
                    record_offset_id = record_offset_stream.next().await.transpose()?;
                    continue;
                }
                (Some(log_oid), Some(record_oid)) => {
                    if log_oid < record_oid {
                        merged_result.push(log_oid);
                        log_index += 1;
                        log_offset_id = self.log_offset_ids.select(u32::try_from(log_index)?);
                    } else {
                        merged_result.push(record_oid);
                        record_offset_id = record_offset_stream.next().await.transpose()?;
                    }
                }
                (None, Some(oid)) => {
                    merged_result.push(oid);
                    record_offset_id = record_offset_stream.next().await.transpose()?;
                }
                (Some(oid), None) => {
                    merged_result.push(oid);
                    log_index += 1;
                    log_offset_id = self.log_offset_ids.select(u32::try_from(log_index)?);
                }
                _ => break,
            };
            limit -= 1;
        }

        Ok(RoaringBitmap::from_sorted_iter(merged_result)
            .expect("Merged offset ids should be sorted"))
    }
}

#[async_trait]
impl Operator<LimitInput, LimitOutput> for Limit {
    type Error = LimitError;

    async fn run(&self, input: &LimitInput) -> Result<LimitOutput, LimitError> {
        let record_segment_reader = match Box::pin(RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        ))
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
                let materialized_logs =
                    materialize_logs(&record_segment_reader, input.logs.clone(), None)
                        .instrument(
                            tracing::trace_span!(parent: Span::current(), "Materialize logs"),
                        )
                        .await?;

                let active_domain: RoaringBitmap = materialized_logs
                    .iter()
                    .filter_map(|log| {
                        (!matches!(
                            log.get_operation(),
                            MaterializedLogOperation::DeleteExisting
                        ))
                        .then_some(log.get_offset_id())
                    })
                    .collect();
                active_domain - rbm
            }
        };

        // Materialize all filtered offset ids with the compact segment
        let materialized_offset_ids = match &input.compact_offset_ids {
            SignedRoaringBitmap::Include(rbm) => {
                let mut merged_offset_ids = materialized_log_offset_ids | rbm;
                merged_offset_ids.remove_smallest(self.offset as u64);
                if let Some(limit_count) = self.limit {
                    let truncated_limit_count = merged_offset_ids.len().min(limit_count as u64);
                    merged_offset_ids
                        .remove_biggest(merged_offset_ids.len() - truncated_limit_count);
                }
                merged_offset_ids
            }
            SignedRoaringBitmap::Exclude(rbm) => {
                if let Some(reader) = record_segment_reader {
                    let record_count = reader.count().await?;
                    let log_count = materialized_log_offset_ids.len();
                    let filter_match_count = log_count + record_count as u64 - rbm.len();
                    let truncated_offset = (self.offset as u64).min(filter_match_count);
                    let truncated_limit = (self.limit.unwrap_or(u32::MAX) as u64)
                        .min(filter_match_count - truncated_offset);

                    let seek_scanner = SeekScanner {
                        log_offset_ids: &materialized_log_offset_ids,
                        record_segment: &reader,
                        mask: rbm,
                    };
                    seek_scanner
                        .seek_and_scan(truncated_offset, truncated_limit)
                        .await?
                } else {
                    materialized_log_offset_ids.remove_smallest(self.offset as u64);
                    if let Some(limit_count) = self.limit {
                        materialized_log_offset_ids
                            .into_iter()
                            .take(limit_count as usize)
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
    use chroma_log::test::{upsert_generator, LoadFromGenerator, LogGenerator};
    use chroma_segment::test::TestDistributedSegment;
    use chroma_system::Operator;
    use chroma_types::{operator::Limit, SignedRoaringBitmap};
    use roaring::RoaringBitmap;

    use super::LimitInput;

    /// The unit tests for `Limit` operator uses the following test data
    /// It first generates 100 log records and compact them,
    /// then generate 30 log records that overwrite the compacted data
    /// - Log: Upsert [31..=60]
    /// - Compacted: Upsert [1..=100]
    async fn setup_limit_input(
        log_offset_ids: SignedRoaringBitmap,
        compact_offset_ids: SignedRoaringBitmap,
    ) -> (TestDistributedSegment, LimitInput) {
        let mut test_segment = TestDistributedSegment::new().await;
        test_segment
            .populate_with_generator(100, upsert_generator)
            .await;
        let blockfile_provider = test_segment.blockfile_provider.clone();
        let record_segment = test_segment.record_segment.clone();
        (
            test_segment,
            LimitInput {
                logs: upsert_generator.generate_chunk(31..=60),
                blockfile_provider,
                record_segment,
                log_offset_ids,
                compact_offset_ids,
            },
        )
    }

    #[tokio::test]
    async fn test_trivial_limit() {
        let (_test_segment, limit_input) = setup_limit_input(
            SignedRoaringBitmap::full(),
            SignedRoaringBitmap::Exclude((31..=60).collect()),
        )
        .await;

        let limit_operator = Limit {
            offset: 0,
            limit: None,
        };

        let limit_output = limit_operator
            .run(&limit_input)
            .await
            .expect("Limit should not fail");

        assert_eq!(limit_output.offset_ids, (1..=100).collect());
    }

    #[tokio::test]
    async fn test_overoffset() {
        let (_test_segment, limit_input) = setup_limit_input(
            SignedRoaringBitmap::full(),
            SignedRoaringBitmap::Exclude((31..=60).collect()),
        )
        .await;

        let limit_operator = Limit {
            offset: 100,
            limit: None,
        };

        let limit_output = limit_operator
            .run(&limit_input)
            .await
            .expect("Limit should not fail");

        assert_eq!(limit_output.offset_ids, RoaringBitmap::new());
    }

    #[tokio::test]
    async fn test_overlimit() {
        let (_test_segment, limit_input) = setup_limit_input(
            SignedRoaringBitmap::full(),
            SignedRoaringBitmap::Exclude((31..=60).collect()),
        )
        .await;

        let limit_operator = Limit {
            offset: 0,
            limit: Some(1000),
        };

        let limit_output = limit_operator
            .run(&limit_input)
            .await
            .expect("Limit should not fail");

        assert_eq!(limit_output.offset_ids, (1..=100).collect());
    }

    #[tokio::test]
    async fn test_simple_range() {
        let (_test_segment, limit_input) = setup_limit_input(
            SignedRoaringBitmap::full(),
            SignedRoaringBitmap::Exclude((31..=60).collect()),
        )
        .await;

        let limit_operator = Limit {
            offset: 60,
            limit: Some(30),
        };

        let limit_output = limit_operator
            .run(&limit_input)
            .await
            .expect("Limit should not fail");

        assert_eq!(limit_output.offset_ids, (61..=90).collect());
    }

    #[tokio::test]
    async fn test_complex_limit() {
        let (_test_segment, limit_input) = setup_limit_input(
            SignedRoaringBitmap::Include((31..=60).filter(|offset| offset % 2 == 0).collect()),
            SignedRoaringBitmap::Exclude((21..=80).collect()),
        )
        .await;

        let limit_operator = Limit {
            offset: 30,
            limit: Some(20),
        };

        let limit_output = limit_operator
            .run(&limit_input)
            .await
            .expect("Limit should not fail");

        assert_eq!(
            limit_output.offset_ids,
            (51..=60)
                .filter(|offset| offset % 2 == 0)
                .chain(81..=95)
                .collect()
        );
    }

    #[tokio::test]
    async fn test_returns_last_offset() {
        let (_test_segment, limit_input) =
            setup_limit_input(SignedRoaringBitmap::empty(), SignedRoaringBitmap::full()).await;

        let limit_operator = Limit {
            offset: 99,
            limit: Some(1),
        };

        let limit_output = limit_operator
            .run(&limit_input)
            .await
            .expect("Limit should not fail");

        assert_eq!(limit_output.offset_ids, (100..=100).collect());
    }
}
