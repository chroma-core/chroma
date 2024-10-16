use crate::{
    execution::operator::Operator,
    segment::{
        record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
        LogMaterializer, LogMaterializerError,
    },
};
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Chunk, LogRecord, Metadata, MetadataValueConversionError, Segment};
use roaring::RoaringBitmap;
use std::collections::HashMap;
use thiserror::Error;
use tracing::{error, trace, Instrument, Span};

#[derive(Debug)]
pub struct HydrateMetadataResultsOperator {}

impl HydrateMetadataResultsOperator {
    pub fn new() -> Box<Self> {
        Box::new(HydrateMetadataResultsOperator {})
    }
}

#[derive(Debug)]
pub struct HydrateMetadataResultsOperatorInput {
    blockfile_provider: BlockfileProvider,
    record_segment_definition: Segment,
    // Result of PullLogs
    log_record: Chunk<LogRecord>,
    // The matching offset ids (both log and compact)
    offset_ids: RoaringBitmap,
    include_metadata: bool,
}

impl HydrateMetadataResultsOperatorInput {
    pub fn new(
        blockfile_provider: BlockfileProvider,
        record_segment_definition: Segment,
        log_record: Chunk<LogRecord>,
        offset_ids: RoaringBitmap,
        include_metadata: bool,
    ) -> Self {
        Self {
            blockfile_provider,
            record_segment_definition,
            log_record,
            offset_ids,
            include_metadata,
        }
    }
}

#[derive(Debug)]
pub struct HydrateMetadataResultsOperatorOutput {
    pub ids: Vec<String>,
    pub metadata: Vec<Option<Metadata>>,
    pub documents: Vec<Option<String>>,
}

#[derive(Error, Debug)]
pub enum HydrateMetadataResultsOperatorError {
    #[error("Error creating Record Segment")]
    RecordSegmentCreation(#[from] RecordSegmentReaderCreationError),
    #[error("Error reading Record Segment")]
    RecordSegmentRead,
    #[error("Error converting metadata")]
    MetadataConversion(#[from] MetadataValueConversionError),
    #[error("Error materializing logs")]
    LogMaterialization(#[from] LogMaterializerError),
}

impl ChromaError for HydrateMetadataResultsOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            HydrateMetadataResultsOperatorError::RecordSegmentCreation(e) => e.code(),
            HydrateMetadataResultsOperatorError::RecordSegmentRead => ErrorCodes::Internal,
            HydrateMetadataResultsOperatorError::MetadataConversion(e) => e.code(),
            HydrateMetadataResultsOperatorError::LogMaterialization(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<HydrateMetadataResultsOperatorInput, HydrateMetadataResultsOperatorOutput>
    for HydrateMetadataResultsOperator
{
    type Error = HydrateMetadataResultsOperatorError;

    fn get_name(&self) -> &'static str {
        "HydrateMetadataResultsOperator"
    }

    async fn run(
        &self,
        input: &HydrateMetadataResultsOperatorInput,
    ) -> Result<HydrateMetadataResultsOperatorOutput, Self::Error> {
        trace!(
            "[HydrateMetadataResultsOperator] segment id: {}",
            input.record_segment_definition.id.to_string()
        );

        // Initialize record segment reader
        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment_definition,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => Ok(Some(reader)),
            // Uninitialized segment is fine and means that the record
            // segment is not yet initialized in storage
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => {
                tracing::error!("Error creating record segment reader {}", e);
                Err(HydrateMetadataResultsOperatorError::RecordSegmentCreation(
                    *e,
                ))
            }
        }?;

        // Materialize the logs
        let materializer = LogMaterializer::new(
            record_segment_reader.clone(),
            input.log_record.clone(),
            None,
        );
        let mat_records = materializer
            .materialize()
            .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
            .await
            .map_err(|e| {
                tracing::error!("Error materializing log: {}", e);
                HydrateMetadataResultsOperatorError::LogMaterialization(e)
            })?;

        // Create a hash map that maps an offset id to the corresponding log
        // It contains all records from the logs that should be present in the final result
        let oid_to_log_record: HashMap<_, _> = mat_records
            .iter()
            .flat_map(|(log, _)| {
                input
                    .offset_ids
                    .contains(log.offset_id)
                    .then_some((log.offset_id, log))
            })
            .collect();

        // Hydrate data
        let mut ids: Vec<String> = Vec::with_capacity(input.offset_ids.len() as usize);
        let mut metadata = Vec::with_capacity(input.offset_ids.len() as usize);
        let mut documents = Vec::with_capacity(input.offset_ids.len() as usize);

        for oid in &input.offset_ids {
            let (id, meta, doc) = match oid_to_log_record.get(&oid) {
                // The offset id is in the log
                Some(&log) => {
                    let mut log_meta = None;
                    let mut log_doc = None;
                    if input.include_metadata {
                        let final_metadata = log.merged_metadata();
                        log_meta = (!final_metadata.is_empty()).then_some(final_metadata);
                        log_doc = log.merged_document();
                    }
                    (log.merged_user_id(), log_meta, log_doc)
                }
                // The offset id is in the record segment
                None => {
                    if let Some(reader) = record_segment_reader.as_ref() {
                        let rec_id = reader
                            .get_user_id_for_offset_id(oid)
                            .await
                            .map_err(|e| {
                                tracing::error!("Error reading record segment: {}", e);
                                HydrateMetadataResultsOperatorError::RecordSegmentRead
                            })?
                            .to_string();
                        let mut rec_meta = None;
                        let mut rec_doc = None;
                        if input.include_metadata {
                            let record = reader.get_data_for_offset_id(oid).await.map_err(|e| {
                                tracing::error!("Error reading Record Segment: {}", e);
                                HydrateMetadataResultsOperatorError::RecordSegmentRead
                            })?;
                            rec_meta = record.metadata;
                            rec_doc = record.document.map(str::to_string);
                        }
                        (rec_id, rec_meta, rec_doc)
                    } else {
                        tracing::error!("Error reading record segment.");
                        return Err(HydrateMetadataResultsOperatorError::RecordSegmentRead);
                    }
                }
            };
            ids.push(id);
            metadata.push(meta);
            documents.push(doc);
        }

        Ok(HydrateMetadataResultsOperatorOutput {
            ids,
            metadata,
            documents,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{
        execution::{
            operator::Operator,
            operators::hydrate_metadata_results::{
                HydrateMetadataResultsOperator, HydrateMetadataResultsOperatorInput,
            },
        },
        segment::{
            metadata_segment::MetadataSegmentWriter,
            record_segment::{
                RecordSegmentReader, RecordSegmentReaderCreationError, RecordSegmentWriter,
            },
            LogMaterializer, SegmentFlusher, SegmentWriter,
        },
    };
    use chroma_blockstore::{
        arrow::{config::TEST_MAX_BLOCK_SIZE_BYTES, provider::ArrowBlockfileProvider},
        provider::BlockfileProvider,
    };
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{
        Chunk, LogRecord, MetadataValue, Operation, OperationRecord, UpdateMetadataValue,
    };
    use roaring::RoaringBitmap;
    use std::{collections::HashMap, str::FromStr};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_merge_and_hydrate() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut record_segment = chroma_types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: Uuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: Uuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let segment_writer =
                RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                    .await
                    .expect("Error creating segment writer");
            let mut metadata_writer =
                MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                    .await
                    .expect("Error creating segment writer");
            let mut update_metadata = HashMap::new();
            update_metadata.insert(
                String::from("hello"),
                UpdateMetadataValue::Str(String::from("world")),
            );
            update_metadata.insert(
                String::from("bye"),
                UpdateMetadataValue::Str(String::from("world")),
            );
            let data = vec![
                LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: Some(update_metadata.clone()),
                        document: Some(String::from("This is a document about cats.")),
                        operation: Operation::Add,
                    },
                },
                LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: Some(update_metadata),
                        document: Some(String::from("This is a document about dogs.")),
                        operation: Operation::Add,
                    },
                },
            ];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReader> =
                match RecordSegmentReader::from_segment(&record_segment, &blockfile_provider).await
                {
                    Ok(reader) => Some(reader),
                    Err(e) => {
                        match *e {
                            // Uninitialized segment is fine and means that the record
                            // segment is not yet initialized in storage.
                            RecordSegmentReaderCreationError::UninitializedSegment => None,
                            RecordSegmentReaderCreationError::BlockfileOpenError(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                                panic!("Error creating record segment reader");
                            }
                        }
                    }
                };
            let materializer = LogMaterializer::new(record_segment_reader, data, None);
            let mat_records = materializer
                .materialize()
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(mat_records.clone())
                .await
                .expect("Apply materialized log to metadata segment failed");
            segment_writer
                .apply_materialized_log_chunk(mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
            metadata_writer
                .write_to_blockfiles()
                .await
                .expect("Metadata writer: write to blockfile failed");
            let record_flusher = segment_writer
                .commit()
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = metadata_writer
                .commit()
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = record_flusher
                .flush()
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = metadata_flusher
                .flush()
                .await
                .expect("Flush metadata segment writer failed");
        }
        let mut update_metadata = HashMap::new();
        update_metadata.insert(
            String::from("hello"),
            UpdateMetadataValue::Str(String::from("new_world")),
        );
        update_metadata.insert(
            String::from("hello_again"),
            UpdateMetadataValue::Str(String::from("new_world")),
        );
        let data = vec![
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_3".to_string(),
                    embedding: Some(vec![7.0, 8.0, 9.0]),
                    encoding: None,
                    metadata: Some(update_metadata.clone()),
                    document: Some(String::from("This is a document about dogs.")),
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 4,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: Some(update_metadata.clone()),
                    document: None,
                    operation: Operation::Update,
                },
            },
        ];
        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let op = HydrateMetadataResultsOperator::new();
        let input = HydrateMetadataResultsOperatorInput::new(
            blockfile_provider,
            record_segment,
            data,
            RoaringBitmap::from([1, 2, 3]),
            true,
        );
        let output = op.run(&input).await.expect("Error running operator");
        assert_eq!(3, output.ids.len());
        #[allow(clippy::type_complexity)]
        let mut id_to_data: HashMap<
            &String,
            (
                &Option<String>,
                &Option<HashMap<String, chroma_types::MetadataValue>>,
            ),
        > = HashMap::new();
        id_to_data.insert(
            output.ids.first().expect("Not none key"),
            (
                output.documents.first().expect("Not none value"),
                output.metadata.first().expect("Not none value"),
            ),
        );
        id_to_data.insert(
            output.ids.get(1).expect("Not none key"),
            (
                output.documents.get(1).expect("Not none value"),
                output.metadata.get(1).expect("Not none value"),
            ),
        );
        id_to_data.insert(
            output.ids.get(2).expect("Not none key"),
            (
                output.documents.get(2).expect("Not none value"),
                output.metadata.get(2).expect("Not none value"),
            ),
        );
        let mut ids_sorted = output.ids.clone();
        ids_sorted.sort();
        assert_eq!(
            *ids_sorted.first().expect("Expected not none id"),
            String::from("embedding_id_1")
        );
        assert_eq!(
            *ids_sorted.get(1).expect("Expected not none id"),
            String::from("embedding_id_2")
        );
        assert_eq!(
            *ids_sorted.get(2).expect("Expected not none id"),
            String::from("embedding_id_3")
        );
        assert!(id_to_data.contains_key(&String::from("embedding_id_1")),);
        assert!(id_to_data.contains_key(&String::from("embedding_id_2")),);
        assert!(id_to_data.contains_key(&String::from("embedding_id_3")),);
        assert_eq!(
            id_to_data
                .get(&String::from("embedding_id_1"))
                .as_ref()
                .expect("Expected value")
                .1
                .as_ref()
                .expect("Expected not none")
                .get(&String::from("hello"))
                .expect("Expected key to be present"),
            &MetadataValue::Str(String::from("new_world"))
        );
        assert_eq!(
            id_to_data
                .get(&String::from("embedding_id_1"))
                .as_ref()
                .expect("Expected value")
                .1
                .as_ref()
                .expect("Expected not none")
                .get(&String::from("bye"))
                .expect("Expected key to be present"),
            &MetadataValue::Str(String::from("world"))
        );
        assert_eq!(
            id_to_data
                .get(&String::from("embedding_id_1"))
                .as_ref()
                .expect("Expected value")
                .1
                .as_ref()
                .expect("Expected not none")
                .get(&String::from("hello_again"))
                .expect("Expected key to be present"),
            &MetadataValue::Str(String::from("new_world"))
        );
        assert_eq!(
            id_to_data
                .get(&String::from("embedding_id_3"))
                .as_ref()
                .expect("Expected value")
                .1
                .as_ref()
                .expect("Expected not none")
                .get(&String::from("hello"))
                .expect("Expected key to be present"),
            &MetadataValue::Str(String::from("new_world"))
        );
        assert_eq!(
            id_to_data
                .get(&String::from("embedding_id_3"))
                .as_ref()
                .expect("Expected value")
                .1
                .as_ref()
                .expect("Expected not none")
                .get(&String::from("hello_again"))
                .expect("Expected key to be present"),
            &MetadataValue::Str(String::from("new_world"))
        );
        assert_eq!(
            id_to_data
                .get(&String::from("embedding_id_1"))
                .as_ref()
                .expect("Expected value")
                .0
                .as_ref()
                .expect("Expected not none"),
            &String::from("This is a document about cats.")
        );
        assert_eq!(
            id_to_data
                .get(&String::from("embedding_id_3"))
                .as_ref()
                .expect("Expected value")
                .0
                .as_ref()
                .expect("Expected not none"),
            &String::from("This is a document about dogs.")
        );
    }
}
