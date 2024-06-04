use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::AtomicU32, Arc},
};

use crate::{
    blockstore::provider::BlockfileProvider,
    errors::{ChromaError, ErrorCodes},
    execution::{data::data_chunk::Chunk, operator::Operator},
    segment::{
        record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
        LogMaterializer, LogMaterializerError,
    },
    types::{
        update_metdata_to_metdata, LogRecord, Metadata, MetadataValueConversionError, Operation,
        Segment,
    },
    utils::{merge_sorted_vecs_conjunction, merge_sorted_vecs_disjunction},
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
    // Result of PullLogs.
    filtered_log: Chunk<LogRecord>,
    // Offset ids corresponding to the user ids asked for explicitly by the user.
    user_offset_ids: Option<Vec<u32>>,
    // The offset ids filtered by the where and where_document clause.
    filtered_offset_ids: Option<Vec<u32>>,
    record_segment_definition: Segment,
    blockfile_provider: BlockfileProvider,
}

impl MergeMetadataResultsOperatorInput {
    pub fn new(
        filtered_log: Chunk<LogRecord>,
        user_offset_ids: Option<Vec<u32>>,
        filtered_offset_ids: Option<Vec<u32>>,
        record_segment_definition: Segment,
        blockfile_provider: BlockfileProvider,
    ) -> Self {
        Self {
            filtered_log,
            user_offset_ids,
            filtered_offset_ids,
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
    #[error("Error materializing logs")]
    LogMaterializationError(#[from] LogMaterializerError),
}

impl ChromaError for MergeMetadataResultsOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            MergeMetadataResultsOperatorError::RecordSegmentCreationError(e) => e.code(),
            MergeMetadataResultsOperatorError::RecordSegmentReadError => ErrorCodes::Internal,
            MergeMetadataResultsOperatorError::MetadataConversionError(e) => e.code(),
            MergeMetadataResultsOperatorError::LogMaterializationError(e) => e.code(),
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

        let mut merged_offset_ids: Option<HashSet<u32>> = match &input.user_offset_ids {
            Some(user_ids) => {
                match &input.filtered_offset_ids {
                    // Intersect with ids filtered from the where clause.
                    Some(filtered_ids) => {
                        let merged_vecs =
                            merge_sorted_vecs_conjunction(user_ids.clone(), filtered_ids.clone());
                        Some(HashSet::from_iter(merged_vecs.iter().cloned()))
                    }
                    // This means that there was no where clause.
                    None => Some(HashSet::from_iter(user_ids.iter().cloned())),
                }
            }
            None => {
                match &input.filtered_offset_ids {
                    Some(filtered_ids) => Some(HashSet::from_iter(filtered_ids.iter().cloned())),
                    // This means that user supplied neither so we return everything.
                    None => None,
                }
            }
        };

        // Materialize logs.
        let record_segment_reader: Option<RecordSegmentReader>;
        match RecordSegmentReader::from_segment(
            &input.record_segment_definition,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => {
                record_segment_reader = Some(reader);
            }
            Err(e) => {
                match *e {
                    // Uninitialized segment is fine and means that the record
                    // segment is not yet initialized in storage.
                    RecordSegmentReaderCreationError::UninitializedSegment => {
                        record_segment_reader = None;
                    }
                    RecordSegmentReaderCreationError::BlockfileOpenError(e) => {
                        tracing::error!("Error creating record segment reader {}", e);
                        return Err(
                            MergeMetadataResultsOperatorError::RecordSegmentCreationError(
                                RecordSegmentReaderCreationError::BlockfileOpenError(e),
                            ),
                        );
                    }
                    RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                        tracing::error!("Error creating record segment reader {}", e);
                        return Err(
                            MergeMetadataResultsOperatorError::RecordSegmentCreationError(
                                RecordSegmentReaderCreationError::InvalidNumberOfFiles,
                            ),
                        );
                    }
                };
            }
        };
        // Step 0.5: Get the current max offset id for materialization.
        // Offset Ids start from 1.
        let mut curr_max_offset_id = Arc::new(AtomicU32::new(1));
        match &record_segment_reader {
            Some(reader) => {
                curr_max_offset_id = reader.get_current_max_offset_id();
                curr_max_offset_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            None => (),
        };
        // Step 1: Materialize the logs.
        let materializer = LogMaterializer::new(
            record_segment_reader,
            input.filtered_log.clone(),
            curr_max_offset_id,
        );
        let mat_records = match materializer.materialize().await {
            Ok(records) => records,
            Err(e) => {
                return Err(MergeMetadataResultsOperatorError::LogMaterializationError(
                    e,
                ));
            }
        };

        let mut ids: Vec<String> = Vec::new();
        let mut metadata = Vec::new();
        let mut documents = Vec::new();
        let mut visited_ids: HashSet<u32> = HashSet::new();
        match merged_offset_ids {
            Some(merged_ids) => {
                // Hydrate from the logs.
                for (log, _) in mat_records.iter() {
                    if merged_ids.contains(&log.offset_id) {
                        visited_ids.insert(log.offset_id);
                        if log.final_operation != Operation::Delete {
                            // Push the final document.
                            match log.final_document {
                                Some(doc) => documents.push(Some(doc.to_string())),
                                None => match log.data_record.as_ref() {
                                    Some(data_record) => match data_record.document {
                                        Some(doc) => documents.push(Some(doc.to_string())),
                                        None => documents.push(None),
                                    },
                                    None => documents.push(None),
                                },
                            };
                            // Final metadata.
                            let mut final_metadata = match log.data_record.as_ref() {
                                Some(data_record) => match data_record.metadata {
                                    Some(ref map) => map.clone(), // auto deref here.
                                    None => HashMap::new(),
                                },
                                None => HashMap::new(),
                            };
                            if log.metadata_to_be_merged.as_ref().is_some() {
                                for (key, value) in log.metadata_to_be_merged.as_ref().unwrap() {
                                    final_metadata.insert(key.clone(), value.clone());
                                    // auto deref here.
                                }
                            }
                            if !final_metadata.is_empty() {
                                metadata.push(Some(final_metadata));
                            } else {
                                metadata.push(None);
                            }
                            // Final id.
                            match log.user_id {
                                Some(id) => ids.push(id.to_string()),
                                None => match &log.data_record {
                                    Some(data_record) => {
                                        ids.push(data_record.id.to_string());
                                    }
                                    None => panic!("Expected at least one user id to be set"),
                                },
                            }
                        }
                    }
                }
                // Hydrate remaining from record segment.
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
                                    MergeMetadataResultsOperatorError::RecordSegmentCreationError(
                                        *e,
                                    ),
                                );
                            }
                            RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                                error!("Error creating Record Segment: {:?}", e);
                                return Err(
                                    MergeMetadataResultsOperatorError::RecordSegmentCreationError(
                                        *e,
                                    ),
                                );
                            }
                        }
                    }
                };
                for merged_id in merged_ids {
                    // Skip already taken from log.
                    if visited_ids.contains(&merged_id) {
                        continue;
                    }
                    let record = match record_segment_reader
                        .get_data_for_offset_id(merged_id)
                        .await
                    {
                        Ok(record) => record,
                        Err(e) => {
                            tracing::error!("Error reading Record Segment: {:?}", e);
                            return Err(MergeMetadataResultsOperatorError::RecordSegmentReadError);
                        }
                    };

                    let user_id = match record_segment_reader
                        .get_user_id_for_offset_id(merged_id)
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
            }
            // Full scan.
            None => {
                let mut ids_in_log = HashSet::new();
                // Log record.
                for (log, _) in mat_records.iter() {
                    // Final id.
                    let res = match log.user_id {
                        Some(id) => ids_in_log.insert(id.to_string()),
                        None => match &log.data_record {
                            Some(data_record) => ids_in_log.insert(data_record.id.to_string()),
                            None => panic!("Expected at least one user id to be set"),
                        },
                    };
                    if log.final_operation != Operation::Delete {
                        // Push the final document.
                        match log.final_document {
                            Some(doc) => documents.push(Some(doc.to_string())),
                            None => match log.data_record.as_ref() {
                                Some(data_record) => match data_record.document {
                                    Some(doc) => documents.push(Some(doc.to_string())),
                                    None => documents.push(None),
                                },
                                None => documents.push(None),
                            },
                        };
                        // Final metadata.
                        let mut final_metadata = match log.data_record.as_ref() {
                            Some(data_record) => match data_record.metadata {
                                Some(ref map) => map.clone(), // auto deref here.
                                None => HashMap::new(),
                            },
                            None => HashMap::new(),
                        };
                        if log.metadata_to_be_merged.as_ref().is_some() {
                            for (key, value) in log.metadata_to_be_merged.as_ref().unwrap() {
                                final_metadata.insert(key.clone(), value.clone());
                                // auto deref here.
                            }
                        }
                        if !final_metadata.is_empty() {
                            metadata.push(Some(final_metadata));
                        } else {
                            metadata.push(None);
                        }
                        // Final id.
                        match log.user_id {
                            Some(id) => ids.push(id.to_string()),
                            None => match &log.data_record {
                                Some(data_record) => {
                                    ids.push(data_record.id.to_string());
                                }
                                None => panic!("Expected at least one user id to be set"),
                            },
                        }
                    }
                }
                // Hydrate remaining from record segment.
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
                                    MergeMetadataResultsOperatorError::RecordSegmentCreationError(
                                        *e,
                                    ),
                                );
                            }
                            RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                                error!("Error creating Record Segment: {:?}", e);
                                return Err(
                                    MergeMetadataResultsOperatorError::RecordSegmentCreationError(
                                        *e,
                                    ),
                                );
                            }
                        }
                    }
                };
                let data = match record_segment_reader.get_all_data().await {
                    Ok(data) => data,
                    Err(e) => {
                        tracing::info!("Error reading Record Segment: {:?}", e);
                        return Err(MergeMetadataResultsOperatorError::RecordSegmentReadError);
                    }
                };
                for record in data.iter() {
                    // Ignore records processed from the log.
                    if ids_in_log.contains(record.id) {
                        continue;
                    }
                    ids.push(record.id.to_string());
                    metadata.push(record.metadata.clone());
                    match record.document {
                        Some(document) => documents.push(Some(document.to_string())),
                        None => documents.push(None),
                    }
                }
            }
        }

        Ok(MergeMetadataResultsOperatorOutput {
            ids,
            metadata,
            documents,
        })
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        str::FromStr,
        sync::{atomic::AtomicU32, Arc},
    };

    use uuid::Uuid;

    use crate::{
        blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider},
        execution::{
            data::data_chunk::Chunk,
            operator::Operator,
            operators::merge_metadata_results::{
                MergeMetadataResultsOperator, MergeMetadataResultsOperatorInput,
            },
        },
        segment::{
            metadata_segment::MetadataSegmentWriter,
            record_segment::{
                RecordSegmentReader, RecordSegmentReaderCreationError, RecordSegmentWriter,
            },
            LogMaterializer, SegmentFlusher, SegmentWriter,
        },
        storage::{local::LocalStorage, Storage},
        types::{LogRecord, MetadataValue, Operation, OperationRecord, UpdateMetadataValue},
    };

    #[tokio::test]
    async fn test_merge_and_hydrate() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(storage);
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut record_segment = crate::types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: crate::types::SegmentType::Record,
            scope: crate::types::SegmentScope::RECORD,
            collection: Some(
                Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            ),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = crate::types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: crate::types::SegmentType::BlockfileMetadata,
            scope: crate::types::SegmentScope::METADATA,
            collection: Some(
                Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            ),
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
            let mut record_segment_reader: Option<RecordSegmentReader> = None;
            match RecordSegmentReader::from_segment(&record_segment, &blockfile_provider).await {
                Ok(reader) => {
                    record_segment_reader = Some(reader);
                }
                Err(e) => {
                    match *e {
                        // Uninitialized segment is fine and means that the record
                        // segment is not yet initialized in storage.
                        RecordSegmentReaderCreationError::UninitializedSegment => {
                            record_segment_reader = None;
                        }
                        RecordSegmentReaderCreationError::BlockfileOpenError(_) => {
                            panic!("Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                            panic!("Error creating record segment reader");
                        }
                    };
                }
            };
            let curr_max_offset_id = Arc::new(AtomicU32::new(1));
            let materializer =
                LogMaterializer::new(record_segment_reader, data, curr_max_offset_id);
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
                .expect("Commit for segment writer failed");
            let metadata_flusher = metadata_writer
                .commit()
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
        let op = MergeMetadataResultsOperator::new();
        let input = MergeMetadataResultsOperatorInput::new(
            data,
            Some(vec![1, 3]),
            Some(vec![1, 2, 3]),
            record_segment,
            blockfile_provider,
        );
        let output = op.run(&input).await.expect("Error running operator");
        assert_eq!(2, output.ids.len());
        let mut id_to_data: HashMap<
            &String,
            (
                &Option<String>,
                &Option<HashMap<String, crate::types::MetadataValue>>,
            ),
        > = HashMap::new();
        id_to_data.insert(
            output.ids.get(0).expect("Not none key"),
            (
                output.documents.get(0).expect("Not none value"),
                output.metadata.get(0).expect("Not none value"),
            ),
        );
        id_to_data.insert(
            output.ids.get(1).expect("Not none key"),
            (
                output.documents.get(1).expect("Not none value"),
                output.metadata.get(1).expect("Not none value"),
            ),
        );
        let mut ids_sorted = output.ids.clone();
        ids_sorted.sort();
        assert_eq!(
            *ids_sorted.get(0).expect("Expected not none id"),
            String::from("embedding_id_1")
        );
        assert_eq!(
            *ids_sorted.get(1).expect("Expected not none id"),
            String::from("embedding_id_3")
        );
        assert_eq!(
            id_to_data.contains_key(&String::from("embedding_id_1")),
            true
        );
        assert_eq!(
            id_to_data.contains_key(&String::from("embedding_id_3")),
            true
        );
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

    #[tokio::test]
    async fn test_merge_and_hydrate_full_scan() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(storage);
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut record_segment = crate::types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: crate::types::SegmentType::Record,
            scope: crate::types::SegmentScope::RECORD,
            collection: Some(
                Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            ),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = crate::types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: crate::types::SegmentType::BlockfileMetadata,
            scope: crate::types::SegmentScope::METADATA,
            collection: Some(
                Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            ),
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
            let mut record_segment_reader: Option<RecordSegmentReader> = None;
            match RecordSegmentReader::from_segment(&record_segment, &blockfile_provider).await {
                Ok(reader) => {
                    record_segment_reader = Some(reader);
                }
                Err(e) => {
                    match *e {
                        // Uninitialized segment is fine and means that the record
                        // segment is not yet initialized in storage.
                        RecordSegmentReaderCreationError::UninitializedSegment => {
                            record_segment_reader = None;
                        }
                        RecordSegmentReaderCreationError::BlockfileOpenError(_) => {
                            panic!("Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                            panic!("Error creating record segment reader");
                        }
                    };
                }
            };
            let curr_max_offset_id = Arc::new(AtomicU32::new(1));
            let materializer =
                LogMaterializer::new(record_segment_reader, data, curr_max_offset_id);
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
                .expect("Commit for segment writer failed");
            let metadata_flusher = metadata_writer
                .commit()
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
        let op = MergeMetadataResultsOperator::new();
        let input = MergeMetadataResultsOperatorInput::new(
            data,
            None,
            None,
            record_segment,
            blockfile_provider,
        );
        let output = op.run(&input).await.expect("Error running operator");
        assert_eq!(3, output.ids.len());
        let mut id_to_data: HashMap<
            &String,
            (
                &Option<String>,
                &Option<HashMap<String, crate::types::MetadataValue>>,
            ),
        > = HashMap::new();
        id_to_data.insert(
            output.ids.get(0).expect("Not none key"),
            (
                output.documents.get(0).expect("Not none value"),
                output.metadata.get(0).expect("Not none value"),
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
            *ids_sorted.get(0).expect("Expected not none id"),
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
        assert_eq!(
            id_to_data.contains_key(&String::from("embedding_id_1")),
            true
        );
        assert_eq!(
            id_to_data.contains_key(&String::from("embedding_id_2")),
            true
        );
        assert_eq!(
            id_to_data.contains_key(&String::from("embedding_id_3")),
            true
        );
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
                .get(&String::from("embedding_id_2"))
                .as_ref()
                .expect("Expected value")
                .1
                .as_ref()
                .expect("Expected not none")
                .get(&String::from("hello"))
                .expect("Expected key to be present"),
            &MetadataValue::Str(String::from("world"))
        );
        assert_eq!(
            id_to_data
                .get(&String::from("embedding_id_2"))
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
                .get(&String::from("embedding_id_2"))
                .as_ref()
                .expect("Expected value")
                .0
                .as_ref()
                .expect("Expected not none"),
            &String::from("This is a document about dogs.")
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
