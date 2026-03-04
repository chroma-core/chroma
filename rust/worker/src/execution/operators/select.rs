use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{materialize_logs, LogMaterializerError},
};
use chroma_system::Operator;
use chroma_types::{
    operator::{Key, RecordMeasure, SearchPayloadResult, SearchRecord, Select},
    Segment,
};
use futures::{stream, StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use tracing::{Instrument, Span};

use crate::execution::operators::fetch_log::FetchLogOutput;

/// Input for the Select operator when used with scored records
#[derive(Clone, Debug)]
pub struct SelectInput {
    pub records: Vec<RecordMeasure>,
    pub logs: FetchLogOutput,
    pub blockfile_provider: BlockfileProvider,
    pub record_segment: Segment,
}

/// Output from the Select operator - returns SearchPayloadResult
pub type SelectOutput = SearchPayloadResult;

#[derive(Error, Debug)]
pub enum SelectError {
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
    #[error("Error reading record segment: {0}")]
    RecordSegment(#[from] Box<dyn ChromaError>),
    #[error("Error reading uninitialized record segment")]
    RecordSegmentUninitialized,
    #[error("Error reading phantom record: {0}")]
    RecordSegmentPhantomRecord(u32),
}

impl ChromaError for SelectError {
    fn code(&self) -> ErrorCodes {
        match self {
            SelectError::LogMaterializer(e) => e.code(),
            SelectError::RecordReader(e) => e.code(),
            SelectError::RecordSegment(e) => e.code(),
            SelectError::RecordSegmentUninitialized => ErrorCodes::Internal,
            SelectError::RecordSegmentPhantomRecord(_) => ErrorCodes::Internal,
        }
    }
}

/// Implement Operator for Select type to handle selection of scored records
#[async_trait]
impl Operator<SelectInput, SelectOutput> for Select {
    type Error = SelectError;

    async fn run(&self, input: &SelectInput) -> Result<SelectOutput, SelectError> {
        tracing::trace!("Running select operator on {} records", input.records.len());

        if input.records.is_empty() {
            return Ok(SearchPayloadResult {
                records: Vec::new(),
            });
        }

        let record_segment_reader = match Box::pin(RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        ))
        .instrument(tracing::trace_span!(parent: Span::current(), "Create record segment reader"))
        .await
        {
            Ok(reader) => Ok(Some(reader)),
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => Err(*e),
        }?;

        // Determine which keys to select
        let select_document = self.keys.contains(&Key::Document);
        let select_embedding = self.keys.contains(&Key::Embedding);
        let select_score = self.keys.contains(&Key::Score);

        // Check if we need to select any metadata
        let select_all_metadata = self.keys.contains(&Key::Metadata);

        // Collect specific metadata keys to select
        let metadata_fields_to_select = self
            .keys
            .iter()
            .filter_map(|field| {
                if let Key::MetadataField(key) = field {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();

        let select_metadata = select_all_metadata || !metadata_fields_to_select.is_empty();
        let needs_data = select_document || select_embedding || select_metadata;

        let offset_id_set = input
            .records
            .iter()
            .map(|record| record.offset_id)
            .collect::<HashSet<_>>();

        // Conditionally prefetch: only load full data records when needed,
        // otherwise just prefetch the lightweight id_to_user_id mapping.
        if let Some(reader) = &record_segment_reader {
            if needs_data {
                reader
                    .load_id_to_data(offset_id_set.iter().cloned())
                    .instrument(tracing::trace_span!(parent: Span::current(), "Load ID to data", num_ids = offset_id_set.len()))
                    .await;
            } else {
                reader
                    .load_id_to_user_id(offset_id_set.iter().cloned())
                    .instrument(tracing::trace_span!(parent: Span::current(), "Load ID to user ID", num_ids = offset_id_set.len()))
                    .await;
            }
        }

        let materialized_logs = materialize_logs(&record_segment_reader, input.logs.clone(), None)
            .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
            .await?;

        // Create a hash map that maps an offset id to the corresponding log
        let offset_id_to_log_record = materialized_logs
            .iter()
            .flat_map(|log| {
                offset_id_set
                    .contains(&log.get_offset_id())
                    .then_some((log.get_offset_id(), log))
            })
            .collect::<HashMap<_, _>>();

        let futures = input
            .records
            .iter()
            .map(|record| async {
                if needs_data {
                    // Full hydration path: load complete data records
                    let (id, document, embedding, mut full_metadata) =
                        match offset_id_to_log_record.get(&record.offset_id) {
                            // The offset id is in the log
                            Some(log) => {
                                let log = log
                                    .hydrate(record_segment_reader.as_ref())
                                    .await
                                    .map_err(SelectError::LogMaterializer)?;

                                (
                                    log.get_user_id().to_string(),
                                    select_document
                                        .then(|| log.merged_document_ref().map(str::to_string))
                                        .flatten(),
                                    select_embedding.then(|| log.merged_embeddings_ref().to_vec()),
                                    log.merged_metadata(),
                                )
                            }
                            // The offset id is in the record segment
                            None => {
                                if let Some(reader) = &record_segment_reader {
                                    let data = reader
                                        .get_data_for_offset_id(record.offset_id)
                                        .await?
                                        .ok_or(SelectError::RecordSegmentPhantomRecord(
                                            record.offset_id,
                                        ))?;

                                    (
                                        data.id.to_string(),
                                        select_document
                                            .then(|| data.document.map(|s| s.to_string()))
                                            .flatten(),
                                        select_embedding.then(|| data.embedding.to_vec()),
                                        data.metadata.unwrap_or_default(),
                                    )
                                } else {
                                    return Err(SelectError::RecordSegmentUninitialized);
                                }
                            }
                        };

                    if !select_all_metadata {
                        full_metadata.retain(|key, _| metadata_fields_to_select.contains(key));
                    }

                    Ok(SearchRecord {
                        id,
                        document: if select_document { document } else { None },
                        embedding: if select_embedding { embedding } else { None },
                        metadata: if full_metadata.is_empty() {
                            None
                        } else {
                            Some(full_metadata)
                        },
                        score: if select_score {
                            Some(record.measure)
                        } else {
                            None
                        },
                    })
                } else {
                    // Lightweight path: only resolve user ID, skip full data hydration
                    let id = match offset_id_to_log_record.get(&record.offset_id) {
                        Some(log) => log
                            .get_user_id(record_segment_reader.as_ref())
                            .await
                            .map_err(SelectError::LogMaterializer)?,
                        None => {
                            if let Some(reader) = &record_segment_reader {
                                reader
                                    .get_user_id_for_offset_id(record.offset_id)
                                    .await?
                                    .ok_or(SelectError::RecordSegmentPhantomRecord(
                                        record.offset_id,
                                    ))?
                                    .to_string()
                            } else {
                                return Err(SelectError::RecordSegmentUninitialized);
                            }
                        }
                    };

                    Ok(SearchRecord {
                        id,
                        document: None,
                        embedding: None,
                        metadata: None,
                        score: if select_score {
                            Some(record.measure)
                        } else {
                            None
                        },
                    })
                }
            })
            .collect::<Vec<_>>();

        let records = stream::iter(futures)
            .buffered(32)
            .try_collect()
            .instrument(tracing::trace_span!(parent: Span::current(), "Select records", num_records = input.records.len()))
            .await?;

        Ok(SearchPayloadResult { records })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_log::test::{upsert_generator, LoadFromGenerator, LogGenerator};
    use chroma_segment::test::TestDistributedSegment;
    use chroma_system::Operator;
    use std::collections::HashSet;

    async fn setup_select_input() -> (TestDistributedSegment, SelectInput) {
        let mut test_segment = TestDistributedSegment::new().await;
        test_segment
            .populate_with_generator(10, upsert_generator)
            .await;

        let records = vec![
            RecordMeasure {
                offset_id: 1,
                measure: 0.9,
            },
            RecordMeasure {
                offset_id: 5,
                measure: 0.7,
            },
            RecordMeasure {
                offset_id: 8,
                measure: 0.5,
            },
        ];

        let input = SelectInput {
            records,
            logs: upsert_generator.generate_chunk(11..=15),
            blockfile_provider: test_segment.blockfile_provider.clone(),
            record_segment: test_segment.record_segment.clone(),
        };

        (test_segment, input)
    }

    #[tokio::test]
    async fn test_select_with_score_only() {
        let (_test_segment, input) = setup_select_input().await;

        let mut keys = HashSet::new();
        keys.insert(Key::Score);

        let select_operator = Select { keys };

        let output = select_operator
            .run(&input)
            .await
            .expect("Select should succeed");

        assert_eq!(output.records.len(), 3);

        // Check first record - ID should always be present
        assert!(!output.records[0].id.is_empty());
        assert_eq!(output.records[0].score, Some(0.9));
        assert!(output.records[0].document.is_none());
        assert!(output.records[0].embedding.is_none());
        assert!(output.records[0].metadata.is_none());

        // Check scores are preserved
        assert_eq!(output.records[1].score, Some(0.7));
        assert_eq!(output.records[2].score, Some(0.5));
    }

    #[tokio::test]
    async fn test_select_with_all_keys() {
        let (_test_segment, input) = setup_select_input().await;

        let mut keys = HashSet::new();
        keys.insert(Key::Document);
        keys.insert(Key::Embedding);
        keys.insert(Key::Metadata);
        keys.insert(Key::Score);

        let select_operator = Select { keys };

        let output = select_operator
            .run(&input)
            .await
            .expect("Select should succeed");

        assert_eq!(output.records.len(), 3);

        // Check all keys are present
        for record in &output.records {
            assert!(!record.id.is_empty());
            assert!(record.document.is_some());
            assert!(record.embedding.is_some());
            assert!(record.metadata.is_some());
            assert!(record.score.is_some());
        }
    }

    /// Test the lightweight path (no data hydration) when records come from the log.
    /// Log records 11-15 are new (not in segment), so user_id_at_log_index is set.
    #[tokio::test]
    async fn test_select_score_only_with_log_records() {
        let mut test_segment = TestDistributedSegment::new().await;
        test_segment
            .populate_with_generator(10, upsert_generator)
            .await;

        // Offset IDs 11-15 are new log records (not in segment).
        // Offset IDs 1, 5 are in the segment only.
        let records = vec![
            RecordMeasure {
                offset_id: 11,
                measure: 0.95,
            },
            RecordMeasure {
                offset_id: 13,
                measure: 0.85,
            },
            RecordMeasure {
                offset_id: 1,
                measure: 0.6,
            },
            RecordMeasure {
                offset_id: 5,
                measure: 0.4,
            },
        ];

        let input = SelectInput {
            records,
            logs: upsert_generator.generate_chunk(11..=15),
            blockfile_provider: test_segment.blockfile_provider.clone(),
            record_segment: test_segment.record_segment.clone(),
        };

        let mut keys = HashSet::new();
        keys.insert(Key::Score);

        let select_operator = Select { keys };

        let output = select_operator
            .run(&input)
            .await
            .expect("Select should succeed");

        assert_eq!(output.records.len(), 4);

        // Log records: user id resolved from log chunk without hydration
        assert_eq!(output.records[0].id, "id_11");
        assert_eq!(output.records[0].score, Some(0.95));
        assert!(output.records[0].document.is_none());
        assert!(output.records[0].embedding.is_none());
        assert!(output.records[0].metadata.is_none());

        assert_eq!(output.records[1].id, "id_13");
        assert_eq!(output.records[1].score, Some(0.85));

        // Segment-only records: user id resolved from id_to_user_id blockfile
        assert_eq!(output.records[2].id, "id_1");
        assert_eq!(output.records[2].score, Some(0.6));
        assert!(output.records[2].document.is_none());

        assert_eq!(output.records[3].id, "id_5");
        assert_eq!(output.records[3].score, Some(0.4));
    }

    /// Test the lightweight path when log records update existing segment records.
    /// In this case, user_id_at_log_index is set on the materialized log record
    /// and offset_id_exists_in_segment is true, but we should still avoid hydration.
    #[tokio::test]
    async fn test_select_score_only_with_log_updating_segment() {
        let mut test_segment = TestDistributedSegment::new().await;
        test_segment
            .populate_with_generator(10, upsert_generator)
            .await;

        // Logs 8-12: offsets 8-10 update existing segment records, 11-12 are new.
        let records = vec![
            RecordMeasure {
                offset_id: 8,
                measure: 0.9,
            },
            RecordMeasure {
                offset_id: 11,
                measure: 0.7,
            },
            RecordMeasure {
                offset_id: 3,
                measure: 0.5,
            },
        ];

        let input = SelectInput {
            records,
            logs: upsert_generator.generate_chunk(8..=12),
            blockfile_provider: test_segment.blockfile_provider.clone(),
            record_segment: test_segment.record_segment.clone(),
        };

        let mut keys = HashSet::new();
        keys.insert(Key::Score);

        let select_operator = Select { keys };

        let output = select_operator
            .run(&input)
            .await
            .expect("Select should succeed");

        assert_eq!(output.records.len(), 3);

        // offset 8: in segment AND in log (upsert updates it), user_id from log
        assert_eq!(output.records[0].id, "id_8");
        assert_eq!(output.records[0].score, Some(0.9));
        assert!(output.records[0].document.is_none());

        // offset 11: new log record only
        assert_eq!(output.records[1].id, "id_11");
        assert_eq!(output.records[1].score, Some(0.7));

        // offset 3: segment-only, not in log, resolved from id_to_user_id blockfile
        assert_eq!(output.records[2].id, "id_3");
        assert_eq!(output.records[2].score, Some(0.5));
    }

    #[tokio::test]
    async fn test_select_empty_records() {
        let test_segment = TestDistributedSegment::new().await;

        let input = SelectInput {
            records: vec![],
            logs: upsert_generator.generate_chunk(1..=5),
            blockfile_provider: test_segment.blockfile_provider,
            record_segment: test_segment.record_segment,
        };

        let mut keys = HashSet::new();
        keys.insert(Key::Score);

        let select_operator = Select { keys };

        let output = select_operator
            .run(&input)
            .await
            .expect("Select should succeed");

        assert_eq!(output.records.len(), 0);
    }
}
