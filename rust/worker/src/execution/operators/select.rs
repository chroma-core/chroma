use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{materialize_logs, LogMaterializerError},
};
use chroma_system::Operator;
use chroma_types::{
    operator::{Select, SelectField, RecordMeasure, SearchPayloadResult, SearchRecord},
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
        tracing::trace!(
            "Running select operator on {} records",
            input.records.len()
        );

        if input.records.is_empty() {
            return Ok(SearchPayloadResult {
                records: Vec::new(),
            });
        }

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

        let materialized_logs = materialize_logs(&record_segment_reader, input.logs.clone(), None)
            .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
            .await?;

        let offset_id_set = input
            .records
            .iter()
            .map(|record| record.offset_id)
            .collect::<HashSet<_>>();

        // Create a hash map that maps an offset id to the corresponding log
        let offset_id_to_log_record = materialized_logs
            .iter()
            .flat_map(|log| {
                offset_id_set
                    .contains(&log.get_offset_id())
                    .then_some((log.get_offset_id(), log))
            })
            .collect::<HashMap<_, _>>();

        // Determine which fields to select
        let select_document = self.fields.contains(&SelectField::Document);
        let select_embedding = self.fields.contains(&SelectField::Embedding);
        let select_score = self.fields.contains(&SelectField::Score);

        // Check if we need to select any metadata
        let select_all_metadata = self.fields.contains(&SelectField::Metadata);

        // Collect specific metadata fields to select
        let metadata_fields_to_select = self
            .fields
            .iter()
            .filter_map(|field| {
                if let SelectField::MetadataField(key) = field {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();

        let futures = input
            .records
            .iter()
            .map(|record| async {
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
                                let record = reader
                                    .get_data_for_offset_id(record.offset_id)
                                    .await?
                                    .ok_or(SelectError::RecordSegmentPhantomRecord(
                                        record.offset_id,
                                    ))?;

                                (
                                    record.id.to_string(),
                                    select_document
                                        .then(|| record.document.map(|s| s.to_string()))
                                        .flatten(),
                                    select_embedding.then(|| record.embedding.to_vec()),
                                    record.metadata.unwrap_or_default(),
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
            })
            .collect::<Vec<_>>();

        Ok(SearchPayloadResult {
            records: stream::iter(futures).buffered(32).try_collect().await?,
        })
    }
}
