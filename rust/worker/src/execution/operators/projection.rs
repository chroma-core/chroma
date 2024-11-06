use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Chunk, LogRecord, Metadata, Segment};
use thiserror::Error;
use tracing::{error, trace, Instrument, Span};

use crate::{
    execution::operator::Operator,
    segment::{
        record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
        LogMaterializer, LogMaterializerError,
    },
};

/// The `ProjectionOperator` retrieves record content by offset ids
///
/// # Parameters
/// - `document`: Whether to retrieve document
/// - `embedding`: Whether to retrieve embedding
/// - `metadata`: Whether to retrieve metadata
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
pub struct ProjectionOperator {
    pub document: bool,
    pub embedding: bool,
    pub metadata: bool,
}

#[derive(Debug)]
pub struct ProjectionInput {
    pub logs: Chunk<LogRecord>,
    pub blockfile_provider: BlockfileProvider,
    pub record_segment: Segment,
    pub offset_ids: Vec<u32>,
}

#[derive(Debug)]
pub struct ProjectionRecord {
    pub id: String,
    pub document: Option<String>,
    pub embedding: Option<Vec<f32>>,
    pub metadata: Option<Metadata>,
}

#[derive(Debug)]
pub struct ProjectionOutput {
    pub records: Vec<ProjectionRecord>,
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
}

impl ChromaError for ProjectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            ProjectionError::LogMaterializer(e) => e.code(),
            ProjectionError::RecordReader(e) => e.code(),
            ProjectionError::RecordSegment(e) => e.code(),
            ProjectionError::RecordSegmentUninitialized => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<ProjectionInput, ProjectionOutput> for ProjectionOperator {
    type Error = ProjectionError;

    async fn run(&self, input: &ProjectionInput) -> Result<ProjectionOutput, ProjectionError> {
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
        let materializer =
            LogMaterializer::new(record_segment_reader.clone(), input.logs.clone(), None);
        let materialized_logs = materializer
            .materialize()
            .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
            .await?;

        let offset_id_set: HashSet<_> = HashSet::from_iter(input.offset_ids.iter().cloned());

        // Create a hash map that maps an offset id to the corresponding log
        // It contains all records from the logs that should be present in the final result
        let oid_to_log_record: HashMap<_, _> = materialized_logs
            .iter()
            .flat_map(|(log, _)| {
                offset_id_set
                    .contains(&log.offset_id)
                    .then_some((log.offset_id, log))
            })
            .collect();

        let mut records = Vec::with_capacity(input.offset_ids.len());

        for offset_id in &input.offset_ids {
            let record = match oid_to_log_record.get(offset_id) {
                // The offset id is in the log
                Some(&log) => ProjectionRecord {
                    id: log.merged_user_id().to_string(),
                    document: log.merged_document().filter(|_| self.document),
                    embedding: self.embedding.then_some(log.merged_embeddings().to_vec()),
                    metadata: self
                        .metadata
                        .then_some(log.merged_metadata())
                        .filter(|metadata| !metadata.is_empty()),
                },
                // The offset id is in the record segment
                None => {
                    if let Some(reader) = &record_segment_reader {
                        let record = reader.get_data_for_offset_id(*offset_id).await?;
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
            records.push(record);
        }

        Ok(ProjectionOutput { records })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        execution::{operator::Operator, operators::projection::ProjectionOperator},
        log::test::{int_as_id, upsert_generator, LogGenerator},
        segment::test::TestSegment,
    };

    use super::ProjectionInput;

    /// The unit tests for `ProjectionOperator` uses the following test data
    /// It first generates 100 log records and compact them,
    /// then generate 20 log records that overwrite the compacted data,
    /// and finally generate 20 log records of new data:
    ///
    /// - Log: Upsert [81..=120]
    /// - Compacted: Upsert [1..=100]
    async fn setup_projection_input(offset_ids: Vec<u32>) -> ProjectionInput {
        let mut test_segment = TestSegment::default();
        let generator = LogGenerator {
            generator: upsert_generator,
        };
        test_segment.populate_with_generator(100, &generator).await;
        ProjectionInput {
            logs: generator.generate_chunk(81..=120),
            blockfile_provider: test_segment.blockfile_provider,
            record_segment: test_segment.record_segment,
            offset_ids,
        }
    }

    #[tokio::test]
    async fn test_trivial_projection() {
        let projection_input = setup_projection_input((1..=120).collect()).await;

        let projection_operator = ProjectionOperator {
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
        let projection_input = setup_projection_input((1..=120).collect()).await;

        let projection_operator = ProjectionOperator {
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
