use crate::{execution::operator::Operator, segment::LogMaterializerError};
use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::Metadata;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use thiserror::Error;
use tracing::{error, trace, Instrument, Span};

use super::{
    knn::KNNOutput,
    limit::LimitOutput,
    scan::{ScanError, ScanOutput},
};

#[derive(Clone, Debug)]
pub struct ProjectionOperator {
    pub document: bool,
    pub embedding: bool,
    pub metadata: bool,
}

#[derive(Debug)]
pub struct ProjectionInput {
    scan: ScanOutput,
    offset_ids: RoaringBitmap,
}

impl From<KNNOutput> for ProjectionInput {
    fn from(value: KNNOutput) -> Self {
        Self {
            scan: value.scan,
            offset_ids: value.distances.into_iter().map(|d| d.oid).collect(),
        }
    }
}

impl From<LimitOutput> for ProjectionInput {
    fn from(value: LimitOutput) -> Self {
        Self {
            scan: value.scan,
            offset_ids: value.offset_ids,
        }
    }
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
    #[error("Error reading record segment: {0}")]
    RecordSegment(#[from] Box<dyn ChromaError>),
    #[error("Error reading unitialized record segment")]
    RecordSegmentUninitialized,
    #[error("Error processing scan output: {0}")]
    Scan(#[from] ScanError),
}

impl ChromaError for ProjectionError {
    fn code(&self) -> ErrorCodes {
        use ProjectionError::*;
        match self {
            LogMaterializer(e) => e.code(),
            RecordSegment(e) => e.code(),
            RecordSegmentUninitialized => ErrorCodes::Internal,
            Scan(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<ProjectionInput, ProjectionOutput> for ProjectionOperator {
    type Error = ProjectionError;

    async fn run(&self, input: &ProjectionInput) -> Result<ProjectionOutput, ProjectionError> {
        trace!("[{}]: {:?}", self.get_name(), input);

        let record_segment_reader = input.scan.record_segment_reader().await?;
        let materializer = input.scan.log_materializer().await?;
        let materialized_logs = materializer
            .materialize()
            .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
            .await?;

        // Create a hash map that maps an offset id to the corresponding log
        // It contains all records from the logs that should be present in the final result
        let oid_to_log_record: HashMap<_, _> = materialized_logs
            .iter()
            .flat_map(|(log, _)| {
                input
                    .offset_ids
                    .contains(log.offset_id)
                    .then_some((log.offset_id, log))
            })
            .collect();

        let mut records = Vec::with_capacity(input.offset_ids.len() as usize);

        for oid in &input.offset_ids {
            let record = match oid_to_log_record.get(&oid) {
                // The offset id is in the log
                Some(&log) => ProjectionRecord {
                    id: log.merged_user_id().to_string(),
                    document: log.merged_document().filter(|_| self.document),
                    embedding: self.embedding.then_some(log.merged_embeddings().to_vec()),
                    metadata: self
                        .metadata
                        .then_some(log.merged_metadata())
                        .filter(|m| !m.is_empty()),
                },
                // The offset id is in the record segment
                None => {
                    if let Some(reader) = record_segment_reader.as_ref() {
                        let record = reader.get_data_for_offset_id(oid).await?;
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
