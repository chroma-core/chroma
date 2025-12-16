//! RankedGroupBy operator for grouping ranked search results by metadata keys.
//!
//! This operator takes ranked records from the RankExpr operator and:
//! 1. Groups records by one or more metadata field values
//! 2. Sorts records within each group by aggregate keys (MinK/MaxK)
//! 3. Keeps the top k records per group
//! 4. Flattens all groups back into a single list, re-sorted by score

use std::{cmp::Reverse, collections::HashMap};

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
                let mut group_vec = group.iter().collect::<Vec<_>>();

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
                        group_vec.sort_by_cached_key(|record| Reverse(record.extract_key(keys)));
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

#[cfg(test)]
mod tests {
    use chroma_log::test::{
        int_as_id, random_embedding, LoadFromGenerator, LogGenerator, TEST_EMBEDDING_DIMENSION,
    };
    use chroma_segment::test::TestDistributedSegment;
    use chroma_system::Operator;
    use chroma_types::{
        operator::{Aggregate, GroupBy, Key, RecordMeasure},
        Operation, OperationRecord, UpdateMetadataValue,
    };
    use std::collections::HashMap;

    use super::{RankedGroupByInput, RankedGroupByOutput};

    /// Generates metadata for group_by testing:
    /// - category: "A", "B", "C" based on (offset - 1) % 3
    /// - year: 2023 or 2024 based on (offset - 1) / 3 % 2
    /// - priority: Designed to have ties within categories for tiebreaker testing
    /// - day: Varying values for secondary sort
    /// - optional_category: Same as category but None for offset % 7 == 0
    fn group_by_metadata(offset: usize) -> HashMap<String, UpdateMetadataValue> {
        let category = match (offset - 1) % 3 {
            0 => "A",
            1 => "B",
            _ => "C",
        };
        let year = if ((offset - 1) / 3) % 2 == 0 {
            2023
        } else {
            2024
        };
        // Priority: create ties within categories
        // A: priorities 1, 1, 2, 2, 3, 3...
        // B: priorities 1, 1, 2, 2, 3, 3...
        // C: priorities 1, 1, 2, 2, 3, 3...
        let priority = ((offset - 1) / 6) + 1;
        // Day: varies to break ties
        let day = ((offset - 1) % 28) + 1;

        let mut metadata: HashMap<String, UpdateMetadataValue> = vec![
            (
                "category".to_string(),
                UpdateMetadataValue::Str(category.to_string()),
            ),
            ("year".to_string(), UpdateMetadataValue::Int(year)),
            (
                "priority".to_string(),
                UpdateMetadataValue::Int(priority as i64),
            ),
            ("day".to_string(), UpdateMetadataValue::Int(day as i64)),
        ]
        .into_iter()
        .collect();

        // optional_category: None for every 7th record
        if offset % 7 != 0 {
            metadata.insert(
                "optional_category".to_string(),
                UpdateMetadataValue::Str(category.to_string()),
            );
        }

        metadata
    }

    /// Log generator for group_by tests
    fn group_by_generator(offset: usize) -> OperationRecord {
        OperationRecord {
            id: int_as_id(offset),
            embedding: Some(random_embedding(TEST_EMBEDDING_DIMENSION)),
            encoding: None,
            metadata: Some(group_by_metadata(offset)),
            document: Some(format!("Document {}", offset)),
            operation: Operation::Add,
        }
    }

    /// Creates RecordMeasure from offset_id with measure = offset_id (predictable scores)
    fn record_measure(offset_id: u32) -> RecordMeasure {
        RecordMeasure {
            offset_id,
            measure: offset_id as f32,
        }
    }

    /// Sets up test input with:
    /// - First `compact_count` records compacted into segment
    /// - Records from `log_range` as input logs
    /// - RecordMeasure for specified offset_ids
    ///
    /// Note: For records in segment, metadata is read from segment.
    /// For records only in logs (offset > compact_count), metadata comes from logs.
    async fn setup_group_by_input(
        compact_count: usize,
        log_range: std::ops::RangeInclusive<usize>,
        record_offset_ids: Vec<u32>,
    ) -> (TestDistributedSegment, RankedGroupByInput) {
        let mut test_segment =
            TestDistributedSegment::new_with_dimension(TEST_EMBEDDING_DIMENSION).await;

        if compact_count > 0 {
            test_segment
                .populate_with_generator(compact_count, group_by_generator)
                .await;
        }

        let blockfile_provider = test_segment.blockfile_provider.clone();
        let record_segment = test_segment.record_segment.clone();

        let logs = group_by_generator.generate_chunk(log_range);

        let records: Vec<RecordMeasure> =
            record_offset_ids.into_iter().map(record_measure).collect();

        (
            test_segment,
            RankedGroupByInput {
                records,
                logs,
                blockfile_provider,
                record_segment,
            },
        )
    }

    /// Helper to extract offset_ids from output
    fn output_offset_ids(output: &RankedGroupByOutput) -> Vec<u32> {
        output.records.iter().map(|r| r.offset_id).collect()
    }

    // =========================================================================
    // Basic / Edge Case Tests
    // =========================================================================

    #[tokio::test]
    async fn test_empty_records() {
        // No records compacted, logs provided but no records queried
        let (_test_segment, input) = setup_group_by_input(0, 1..=6, vec![]).await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 1,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        assert!(output.records.is_empty());
    }

    #[tokio::test]
    async fn test_empty_group_keys() {
        // Setup: 6 records in segment, logs cover same range
        let (_test_segment, input) = setup_group_by_input(6, 1..=6, vec![1, 2, 3, 4, 5, 6]).await;

        let group_by = GroupBy {
            keys: vec![], // Empty keys = pass-through
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 1,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        // Should return all records unchanged
        assert_eq!(output_offset_ids(&output), vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_no_aggregate() {
        let (_test_segment, input) = setup_group_by_input(6, 1..=6, vec![1, 2, 3, 4, 5, 6]).await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: None, // No aggregate = pass-through
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        assert_eq!(output_offset_ids(&output), vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_single_record() {
        let (_test_segment, input) = setup_group_by_input(
            6,
            1..=6,
            vec![1], // Single record
        )
        .await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 1,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        assert_eq!(output_offset_ids(&output), vec![1]);
    }

    // =========================================================================
    // Single Key Grouping Tests
    // =========================================================================

    #[tokio::test]
    async fn test_single_key_min_k_1() {
        // Setup: 9 records, 3 per category (A, B, C)
        // Offsets: 1=A, 2=B, 3=C, 4=A, 5=B, 6=C, 7=A, 8=B, 9=C
        // With MinK k=1 by score, we should get lowest offset per category
        let (_test_segment, input) =
            setup_group_by_input(6, 7..=9, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]).await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 1,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        // Expected: 1 (A), 2 (B), 3 (C) - sorted by score
        assert_eq!(output_offset_ids(&output), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_single_key_max_k_1() {
        // Same setup, but MaxK should return highest offset per category
        let (_test_segment, input) =
            setup_group_by_input(6, 7..=9, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]).await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MaxK {
                keys: vec![Key::Score],
                k: 1,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        // Expected: 7 (A), 8 (B), 9 (C) - sorted by score
        assert_eq!(output_offset_ids(&output), vec![7, 8, 9]);
    }

    #[tokio::test]
    async fn test_single_key_min_k_2() {
        let (_test_segment, input) =
            setup_group_by_input(6, 7..=9, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]).await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 2,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        // Expected: top 2 per category, sorted by score
        // A: 1, 4; B: 2, 5; C: 3, 6
        // Sorted: 1, 2, 3, 4, 5, 6
        assert_eq!(output_offset_ids(&output), vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_min_k_exceeds_group_size() {
        // 9 records, 3 per category, but k=10
        let (_test_segment, input) =
            setup_group_by_input(6, 7..=9, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]).await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 10, // More than group size
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        // Should return all records (k > group size returns all available)
        assert_eq!(output_offset_ids(&output), vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    // =========================================================================
    // Multiple Group Keys Tests
    // =========================================================================

    #[tokio::test]
    async fn test_multiple_group_keys() {
        // Group by (year, category) creates 6 groups:
        // year=2023: A(1), B(2), C(3)
        // year=2024: A(4), B(5), C(6)
        // Then offsets 7,8,9 are year=2023 again: A(7), B(8), C(9)
        let (_test_segment, input) =
            setup_group_by_input(6, 7..=9, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]).await;

        let group_by = GroupBy {
            keys: vec![Key::field("year"), Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 1,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        // Groups: (2023,A): 1,7 -> 1; (2023,B): 2,8 -> 2; (2023,C): 3,9 -> 3
        //         (2024,A): 4 -> 4; (2024,B): 5 -> 5; (2024,C): 6 -> 6
        // Sorted by score: 1, 2, 3, 4, 5, 6
        assert_eq!(output_offset_ids(&output), vec![1, 2, 3, 4, 5, 6]);
    }

    // =========================================================================
    // Aggregate by Metadata Field Tests
    // =========================================================================

    #[tokio::test]
    async fn test_aggregate_by_metadata_min() {
        // Group by category, MinK by priority (not score)
        // Priorities: offset 1-6 have priority 1, offset 7-12 have priority 2
        let (_test_segment, input) =
            setup_group_by_input(9, 10..=12, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]).await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::field("priority")],
                k: 1,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        // MinK by priority: lowest priority per category
        // All offsets 1-6 have priority=1, so one of 1,4 (A), 2,5 (B), 3,6 (C)
        // With ties, the order within group is arbitrary, but output sorted by score
        let ids = output_offset_ids(&output);
        assert_eq!(ids.len(), 3);
        // Check we got one from each category with priority 1
        // A: 1 or 4, B: 2 or 5, C: 3 or 6
        assert!(ids.iter().any(|&id| id == 1 || id == 4)); // A
        assert!(ids.iter().any(|&id| id == 2 || id == 5)); // B
        assert!(ids.iter().any(|&id| id == 3 || id == 6)); // C
    }

    #[tokio::test]
    async fn test_aggregate_by_metadata_max() {
        // Group by category, MaxK by priority
        let (_test_segment, input) =
            setup_group_by_input(9, 10..=12, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]).await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MaxK {
                keys: vec![Key::field("priority")],
                k: 1,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        // MaxK by priority: highest priority per category
        // Offsets 7-12 have priority=2 (highest)
        let ids = output_offset_ids(&output);
        assert_eq!(ids.len(), 3);
        // A: 7 or 10, B: 8 or 11, C: 9 or 12
        assert!(ids.iter().any(|&id| id == 7 || id == 10)); // A
        assert!(ids.iter().any(|&id| id == 8 || id == 11)); // B
        assert!(ids.iter().any(|&id| id == 9 || id == 12)); // C
    }

    // =========================================================================
    // Multi-Key Aggregate (Tiebreaker) Tests
    // =========================================================================

    #[tokio::test]
    async fn test_multi_key_aggregate_tiebreaker() {
        // Group by category, MinK by [priority, day]
        // When priorities tie, day breaks the tie
        let (_test_segment, input) = setup_group_by_input(6, 1..=6, vec![1, 2, 3, 4, 5, 6]).await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::field("priority"), Key::field("day")],
                k: 1,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        // All have priority=1, so day is tiebreaker
        // Days: 1->1, 2->2, 3->3, 4->4, 5->5, 6->6
        // MinK by day within each category:
        // A (1,4): day 1 vs 4 -> offset 1 wins
        // B (2,5): day 2 vs 5 -> offset 2 wins
        // C (3,6): day 3 vs 6 -> offset 3 wins
        assert_eq!(output_offset_ids(&output), vec![1, 2, 3]);
    }

    // =========================================================================
    // Missing Metadata Tests
    // =========================================================================

    #[tokio::test]
    async fn test_missing_group_key() {
        // Group by optional_category (None for offset % 7 == 0)
        // Offset 7 has no optional_category
        let (_test_segment, input) =
            setup_group_by_input(6, 7..=9, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]).await;

        let group_by = GroupBy {
            keys: vec![Key::field("optional_category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 1,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        // Groups: A (1,4), B (2,5,8), C (3,6,9), None (7)
        // MinK k=1: A->1, B->2, C->3, None->7
        // Sorted by score: 1, 2, 3, 7
        assert_eq!(output_offset_ids(&output), vec![1, 2, 3, 7]);
    }

    // =========================================================================
    // Output Ordering Tests
    // =========================================================================

    #[tokio::test]
    async fn test_output_sorted_by_score() {
        // Verify output is always sorted by score (measure) ascending
        let (_test_segment, input) = setup_group_by_input(
            6,
            7..=9,
            vec![9, 7, 8, 3, 1, 2, 6, 4, 5], // Input in random order
        )
        .await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MaxK {
                keys: vec![Key::Score],
                k: 1,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");
        // MaxK k=1: A->7, B->8, C->9
        // Must be sorted by score: 7, 8, 9
        assert_eq!(output_offset_ids(&output), vec![7, 8, 9]);

        // Verify scores are in ascending order
        let scores: Vec<f32> = output.records.iter().map(|r| r.measure).collect();
        assert!(scores.windows(2).all(|w| w[0] <= w[1]));
    }

    // =========================================================================
    // Hybrid Data Source Tests (Segment + Logs)
    // =========================================================================

    #[tokio::test]
    async fn test_hybrid_segment_and_logs() {
        // Records 1-18 in segment, 19-36 in logs
        // 36 total records = 12 per category (A, B, C)
        // This tests both metadata lookup paths with substantial data
        let segment_count = 18;
        let log_start = 19;
        let log_end = 36;
        let all_offsets: Vec<u32> = (1..=36).collect();

        let (_test_segment, input) =
            setup_group_by_input(segment_count, log_start..=log_end, all_offsets).await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 3,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");

        // Categories (offset % 3 pattern): A=1,4,7,10,13,16,19,22,25,28,31,34
        //                                  B=2,5,8,11,14,17,20,23,26,29,32,35
        //                                  C=3,6,9,12,15,18,21,24,27,30,33,36
        // MinK k=3 by score (lowest offset_ids win):
        // A: 1, 4, 7 (all from segment)
        // B: 2, 5, 8 (all from segment)
        // C: 3, 6, 9 (all from segment)
        // Sorted by score: 1, 2, 3, 4, 5, 6, 7, 8, 9
        assert_eq!(output_offset_ids(&output), vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[tokio::test]
    async fn test_hybrid_segment_and_logs_max_k() {
        // Same setup but with MaxK to get records from logs
        let segment_count = 18;
        let log_start = 19;
        let log_end = 36;
        let all_offsets: Vec<u32> = (1..=36).collect();

        let (_test_segment, input) =
            setup_group_by_input(segment_count, log_start..=log_end, all_offsets).await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MaxK {
                keys: vec![Key::Score],
                k: 3,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");

        // MaxK k=3 by score (highest offset_ids win):
        // A: 34, 31, 28 (from logs)
        // B: 35, 32, 29 (from logs)
        // C: 36, 33, 30 (from logs)
        // Sorted by score: 28, 29, 30, 31, 32, 33, 34, 35, 36
        assert_eq!(
            output_offset_ids(&output),
            vec![28, 29, 30, 31, 32, 33, 34, 35, 36]
        );
    }

    #[tokio::test]
    async fn test_hybrid_mixed_results() {
        // Test case where results come from both segment and logs
        let segment_count = 18;
        let log_start = 19;
        let log_end = 36;
        let all_offsets: Vec<u32> = (1..=36).collect();

        let (_test_segment, input) =
            setup_group_by_input(segment_count, log_start..=log_end, all_offsets).await;

        let group_by = GroupBy {
            keys: vec![Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 5,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");

        // MinK k=5 by score:
        // A: 1, 4, 7, 10, 13 (first 4 from segment, 5th from segment too)
        // B: 2, 5, 8, 11, 14 (first 4 from segment, 5th from segment too)
        // C: 3, 6, 9, 12, 15 (first 4 from segment, 5th from segment too)
        // All from segment since k=5 and segment has 6 per category (offsets 1-18)
        // Sorted: 1-15
        assert_eq!(
            output_offset_ids(&output),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );
    }

    #[tokio::test]
    async fn test_hybrid_with_multiple_group_keys() {
        // Test hybrid with composite grouping (year, category)
        // This creates more groups and exercises both data paths
        let segment_count = 18;
        let log_start = 19;
        let log_end = 36;
        let all_offsets: Vec<u32> = (1..=36).collect();

        let (_test_segment, input) =
            setup_group_by_input(segment_count, log_start..=log_end, all_offsets).await;

        let group_by = GroupBy {
            keys: vec![Key::field("year"), Key::field("category")],
            aggregate: Some(Aggregate::MinK {
                keys: vec![Key::Score],
                k: 2,
            }),
        };

        let output = group_by.run(&input).await.expect("Should not fail");

        // Year pattern: offsets 1-3 (2023), 4-6 (2024), 7-9 (2023), 10-12 (2024), ...
        // Groups (year, category) with 2 records each from MinK k=2:
        // (2023, A): 1, 7, 13, 19, 25, 31 -> min 2: 1, 7
        // (2023, B): 2, 8, 14, 20, 26, 32 -> min 2: 2, 8
        // (2023, C): 3, 9, 15, 21, 27, 33 -> min 2: 3, 9
        // (2024, A): 4, 10, 16, 22, 28, 34 -> min 2: 4, 10
        // (2024, B): 5, 11, 17, 23, 29, 35 -> min 2: 5, 11
        // (2024, C): 6, 12, 18, 24, 30, 36 -> min 2: 6, 12
        // Sorted: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
        assert_eq!(
            output_offset_ids(&output),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
    }
}
