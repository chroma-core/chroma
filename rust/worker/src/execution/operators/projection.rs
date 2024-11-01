use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::Metadata;
use thiserror::Error;
use tracing::{error, trace, Instrument, Span};

use crate::{
    execution::operator::{Operator, OperatorType},
    segment::{LogMaterializer, LogMaterializerError},
};

use super::{
    fetch_log::FetchLogOutput,
    fetch_segment::{FetchSegmentError, FetchSegmentOutput},
};

#[derive(Clone, Debug)]
pub struct ProjectionOperator {
    pub document: bool,
    pub embedding: bool,
    pub metadata: bool,
}

#[derive(Debug)]
pub struct ProjectionInput {
    pub logs: FetchLogOutput,
    pub segments: FetchSegmentOutput,
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
    #[error("Error processing fetch segment output: {0}")]
    FetchSegment(#[from] FetchSegmentError),
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error reading record segment: {0}")]
    RecordSegment(#[from] Box<dyn ChromaError>),
    #[error("Error reading unitialized record segment")]
    RecordSegmentUninitialized,
}

impl ChromaError for ProjectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            ProjectionError::FetchSegment(e) => e.code(),
            ProjectionError::LogMaterializer(e) => e.code(),
            ProjectionError::RecordSegment(e) => e.code(),
            ProjectionError::RecordSegmentUninitialized => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<ProjectionInput, ProjectionOutput> for ProjectionOperator {
    type Error = ProjectionError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(&self, input: &ProjectionInput) -> Result<ProjectionOutput, ProjectionError> {
        trace!("[{}]: {:?}", self.get_name(), input);

        let record_segment_reader = input.segments.record_segment_reader().await?;
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
                    if let Some(reader) = record_segment_reader.as_ref() {
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
        execution::{
            operator::Operator,
            operators::{fetch_segment::FetchSegmentOutput, projection::ProjectionOperator},
        },
        log::test::{int_as_id, upsert_generator, LogGenerator},
        segment::test::TestSegment,
    };

    use super::ProjectionInput;

    async fn setup_projection_input(offset_ids: Vec<u32>) -> ProjectionInput {
        let mut test_segment = TestSegment::default();
        let generator = LogGenerator {
            generator: upsert_generator,
        };
        test_segment.populate_with_generator(100, &generator).await;
        ProjectionInput {
            logs: generator.generate_chunk(81..=120),
            segments: FetchSegmentOutput {
                hnsw: test_segment.hnsw,
                blockfile: test_segment.blockfile,
                knn: test_segment.knn,
                metadata: test_segment.metadata,
                record: test_segment.record,
                collection: test_segment.collection,
            },
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
