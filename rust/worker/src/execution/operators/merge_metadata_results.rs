use crate::{
    blockstore::provider::BlockfileProvider,
    errors::{ChromaError, ErrorCodes},
    execution::{data::data_chunk::Chunk, operator::Operator},
    segment::record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{
        update_metdata_to_metdata, LogRecord, Metadata, MetadataValueConversionError, Segment,
    },
};
use async_trait::async_trait;
use thiserror::Error;
use tracing::{error, trace};

#[derive(Debug)]
pub struct MergeMetadataResultsOperator {}

impl MergeMetadataResultsOperator {
    pub fn new() -> Box<Self> {
        Box::new(MergeMetadataResultsOperator {})
    }
}

#[derive(Debug)]
pub struct MergeMetadataResultsOperatorInput {
    // The records that were found in the log based on the filter conditions
    // TODO: Once we support update/delete this should be MaterializedLogRecord
    filtered_log: Chunk<LogRecord>,
    // The query ids that were not found in the log, that we need to pull from the record segment
    remaining_query_ids: Vec<String>,
    // The offset ids that were found in the log, from where/where_document results
    filtered_index_offset_ids: Vec<u32>,
    record_segment_definition: Segment,
    blockfile_provider: BlockfileProvider,
}

impl MergeMetadataResultsOperatorInput {
    pub fn new(
        filtered_log: Chunk<LogRecord>,
        remaining_query_ids: Vec<String>,
        filtered_index_offset_ids: Vec<u32>,
        record_segment_definition: Segment,
        blockfile_provider: BlockfileProvider,
    ) -> Self {
        Self {
            filtered_log: filtered_log,
            remaining_query_ids: remaining_query_ids,
            filtered_index_offset_ids: filtered_index_offset_ids,
            record_segment_definition,
            blockfile_provider: blockfile_provider,
        }
    }
}

#[derive(Debug)]
pub struct MergeMetadataResultsOperatorOutput {
    pub ids: Vec<String>,
    pub metadata: Vec<Option<Metadata>>,
    pub documents: Vec<Option<String>>,
}

#[derive(Error, Debug)]
pub enum MergeMetadataResultsOperatorError {
    #[error("Error creating Record Segment")]
    RecordSegmentCreationError(#[from] RecordSegmentReaderCreationError),
    #[error("Error reading Record Segment")]
    RecordSegmentReadError,
    #[error("Error converting metadata")]
    MetadataConversionError(#[from] MetadataValueConversionError),
}

impl ChromaError for MergeMetadataResultsOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            MergeMetadataResultsOperatorError::RecordSegmentCreationError(e) => e.code(),
            MergeMetadataResultsOperatorError::RecordSegmentReadError => ErrorCodes::Internal,
            MergeMetadataResultsOperatorError::MetadataConversionError(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<MergeMetadataResultsOperatorInput, MergeMetadataResultsOperatorOutput>
    for MergeMetadataResultsOperator
{
    type Error = MergeMetadataResultsOperatorError;

    async fn run(
        &self,
        input: &MergeMetadataResultsOperatorInput,
    ) -> Result<MergeMetadataResultsOperatorOutput, Self::Error> {
        trace!(
            "[MergeMetadataResultsOperator] segment id: {}",
            input.record_segment_definition.id.to_string()
        );

        let mut ids: Vec<String> = Vec::new();
        let mut metadata = Vec::new();
        let mut documents = Vec::new();
        // Add the data from the brute force results
        for (log_entry, index) in input.filtered_log.iter() {
            ids.push(log_entry.record.id.to_string());
            let output_metadata = match &log_entry.record.metadata {
                Some(log_metadata) => match update_metdata_to_metdata(log_metadata) {
                    Ok(metadata) => Some(metadata),
                    Err(e) => {
                        println!("Error converting log metadata: {:?}", e);
                        return Err(MergeMetadataResultsOperatorError::MetadataConversionError(
                            e,
                        ));
                    }
                },
                None => {
                    println!("No metadata found for log entry");
                    None
                }
            };
            metadata.push(output_metadata);
            documents.push(log_entry.record.document.clone());
        }

        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment_definition,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => reader,
            Err(e) => {
                match *e {
                    RecordSegmentReaderCreationError::UninitializedSegment => {
                        // This means no compaction has occured, so we can just return whats on the log.
                        return Ok(MergeMetadataResultsOperatorOutput {
                            ids,
                            metadata,
                            documents,
                        });
                    }
                    RecordSegmentReaderCreationError::BlockfileOpenError(_) => {
                        error!("Error creating Record Segment: {:?}", e);
                        return Err(
                            MergeMetadataResultsOperatorError::RecordSegmentCreationError(*e),
                        );
                    }
                    RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                        error!("Error creating Record Segment: {:?}", e);
                        return Err(
                            MergeMetadataResultsOperatorError::RecordSegmentCreationError(*e),
                        );
                    }
                }
            }
        };

        // Hydrate the data from the record segment for filtered data
        for index_offset_id in input.filtered_index_offset_ids.iter() {
            let record = match record_segment_reader
                .get_data_for_offset_id(*index_offset_id as u32)
                .await
            {
                Ok(record) => record,
                Err(e) => {
                    println!("Error reading Record Segment: {:?}", e);
                    return Err(MergeMetadataResultsOperatorError::RecordSegmentReadError);
                }
            };

            let user_id = match record_segment_reader
                .get_user_id_for_offset_id(*index_offset_id as u32)
                .await
            {
                Ok(user_id) => user_id,
                Err(e) => {
                    println!("Error reading Record Segment: {:?}", e);
                    return Err(MergeMetadataResultsOperatorError::RecordSegmentReadError);
                }
            };

            ids.push(user_id.to_string());
            metadata.push(record.metadata.clone());
            match record.document {
                Some(document) => documents.push(Some(document.to_string())),
                None => documents.push(None),
            }
        }

        // Hydrate the data from the record segment for the remaining data
        for query_id in input.remaining_query_ids.iter() {
            let offset_id = match record_segment_reader
                .get_offset_id_for_user_id(query_id)
                .await
            {
                Ok(offset_id) => offset_id,
                Err(e) => {
                    println!("Error reading Record Segment: {:?}", e);
                    return Err(MergeMetadataResultsOperatorError::RecordSegmentReadError);
                }
            };

            let record = match record_segment_reader
                .get_data_for_offset_id(offset_id)
                .await
            {
                Ok(record) => record,
                Err(e) => {
                    println!("Error reading Record Segment: {:?}", e);
                    return Err(MergeMetadataResultsOperatorError::RecordSegmentReadError);
                }
            };

            ids.push(record.id.to_string());
            metadata.push(record.metadata.clone());
            match record.document {
                Some(document) => documents.push(Some(document.to_string())),
                None => documents.push(None),
            }
        }

        Ok(MergeMetadataResultsOperatorOutput {
            ids,
            metadata,
            documents,
        })
    }
}
