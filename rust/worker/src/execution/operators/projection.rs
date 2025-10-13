use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{materialize_logs, LogMaterializerError},
};
use chroma_system::Operator;
use chroma_types::{
    operator::{Projection, ProjectionOutput, ProjectionRecord},
    Chunk, LogRecord, Segment,
};
use futures::future::try_join_all;
use thiserror::Error;
use tracing::{error, Instrument, Span};

/// The `Projection` operator retrieves record content by offset ids
///
/// # Inputs
/// - `logs`: The latest logs of the collection
/// - `blockfile_provider`: The blockfile provider
/// - `record_segment`: The record segment information
/// - `offset_ids`: The offset ids in either logs or blockfile to retrieve for
///
/// # Outputs
/// - `records`: The retrieved records in the same order as `offset_ids`
///
/// # Usage
/// It can be used to retrieve record contents as user requested
/// It should be run as the last step of an orchestrator
#[derive(Clone, Debug)]
pub struct ProjectionInput {
    pub logs: Chunk<LogRecord>,
    pub blockfile_provider: BlockfileProvider,
    pub record_segment: Segment,
    pub offset_ids: Vec<u32>,
}

#[derive(Error, Debug)]
pub enum ProjectionError {
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
    #[error("Error reading record segment: {0}")]
    RecordSegment(#[from] Box<dyn ChromaError>),
    #[error("Error reading unitialized record segment")]
    RecordSegmentUninitialized,
    #[error("Error reading phantom record: {0}")]
    RecordSegmentPhantomRecord(u32),
}

impl ChromaError for ProjectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            ProjectionError::LogMaterializer(e) => e.code(),
            ProjectionError::RecordReader(e) => e.code(),
            ProjectionError::RecordSegment(e) => e.code(),
            ProjectionError::RecordSegmentUninitialized => ErrorCodes::Internal,
            ProjectionError::RecordSegmentPhantomRecord(_) => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<ProjectionInput, ProjectionOutput> for Projection {
    type Error = ProjectionError;

    async fn run(&self, input: &ProjectionInput) -> Result<ProjectionOutput, ProjectionError> {
        if input.offset_ids.is_empty() {
            return Ok(ProjectionOutput { records: vec![] });
        }

        tracing::trace!(
            "Running projection on {} offset ids",
            input.offset_ids.len()
        );
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

        let materialized_logs = materialize_logs(&record_segment_reader, input.logs.clone(), None)
            .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
            .await?;

        let offset_id_set: HashSet<_> = HashSet::from_iter(input.offset_ids.iter().cloned());

        // Create a hash map that maps an offset id to the corresponding log
        // It contains all records from the logs that should be present in the final result
        let offset_id_to_log_record: HashMap<_, _> = materialized_logs
            .iter()
            .flat_map(|log| {
                offset_id_set
                    .contains(&log.get_offset_id())
                    .then_some((log.get_offset_id(), log))
            })
            .collect();

        let current_span = Span::current();
        let futures: Vec<_> = input
            .offset_ids
            .iter()
            .map(|offset_id| {
                async {
                    let record = match offset_id_to_log_record.get(offset_id) {
                        // The offset id is in the log
                        Some(log) => {
                            let log = log
                                .hydrate(record_segment_reader.as_ref())
                                .await
                                .map_err(ProjectionError::LogMaterializer)?;

                            ProjectionRecord {
                                id: log.get_user_id().to_string(),
                                document: log
                                    .merged_document_ref()
                                    .filter(|_| self.document)
                                    .map(str::to_string),
                                embedding: self
                                    .embedding
                                    .then_some(log.merged_embeddings_ref().to_vec()),
                                metadata: self
                                    .metadata
                                    .then_some(log.merged_metadata())
                                    .filter(|metadata| !metadata.is_empty()),
                            }
                        }
                        // The offset id is in the record segment
                        None => {
                            if let Some(reader) = &record_segment_reader {
                                let record =
                                    reader.get_data_for_offset_id(*offset_id).await?.ok_or(
                                        ProjectionError::RecordSegmentPhantomRecord(*offset_id),
                                    )?;
                                ProjectionRecord {
                                    id: record.id.to_string(),
                                    document: record
                                        .document
                                        .filter(|_| self.document)
                                        .map(str::to_string),
                                    embedding: self.embedding.then_some(record.embedding.to_vec()),
                                    metadata: record.metadata.filter(|_| self.metadata),
                                }
                            } else {
                                return Err(ProjectionError::RecordSegmentUninitialized);
                            }
                        }
                    };
                    Ok::<_, ProjectionError>(record)
                }
                .instrument(current_span.clone())
            })
            .collect();

        let records: Vec<ProjectionRecord> = try_join_all(futures).await?;

        Ok(ProjectionOutput { records })
    }
}

#[cfg(test)]
mod tests {
    use chroma_log::test::{int_as_id, upsert_generator, LoadFromGenerator, LogGenerator};
    use chroma_segment::test::TestDistributedSegment;
    use chroma_system::Operator;
    use chroma_types::operator::Projection;

    use super::ProjectionInput;

    /// The unit tests for `Projection` operator uses the following test data
    /// It first generates 100 log records and compact them,
    /// then generate 20 log records that overwrite the compacted data,
    /// and finally generate 20 log records of new data:
    ///
    /// - Log: Upsert [81..=120]
    /// - Compacted: Upsert [1..=100]
    async fn setup_projection_input(
        offset_ids: Vec<u32>,
    ) -> (TestDistributedSegment, ProjectionInput) {
        let mut test_segment = TestDistributedSegment::new().await;
        test_segment
            .populate_with_generator(100, upsert_generator)
            .await;
        let blockfile_provider = test_segment.blockfile_provider.clone();
        let record_segment = test_segment.record_segment.clone();
        (
            test_segment,
            ProjectionInput {
                logs: upsert_generator.generate_chunk(81..=120),
                blockfile_provider,
                record_segment,
                offset_ids,
            },
        )
    }

    #[tokio::test]
    async fn test_trivial_projection() {
        let (_test_segment, projection_input) = setup_projection_input((1..=120).collect()).await;

        let projection_operator = Projection {
            document: false,
            embedding: false,
            metadata: false,
        };

        let projection_output = projection_operator
            .run(&projection_input)
            .await
            .expect("ProjectionOperator should not fail");

        assert_eq!(projection_output.records.len(), 120);
        for (offset, record) in projection_output.records.into_iter().enumerate() {
            assert_eq!(record.id, int_as_id(offset + 1));
            assert!(record.document.is_none());
            assert!(record.embedding.is_none());
            assert!(record.metadata.is_none());
        }
    }

    #[tokio::test]
    async fn test_full_projection() {
        let (_test_segment, projection_input) = setup_projection_input((1..=120).collect()).await;

        let projection_operator = Projection {
            document: true,
            embedding: true,
            metadata: true,
        };

        let projection_output = projection_operator
            .run(&projection_input)
            .await
            .expect("ProjectionOperator should not fail");

        assert_eq!(projection_output.records.len(), 120);
        for (offset, record) in projection_output.records.into_iter().enumerate() {
            assert_eq!(record.id, int_as_id(offset + 1));
            assert!(record.document.is_some());
            assert!(record.embedding.is_some());
            assert!(record.metadata.is_some());
        }
    }
}
