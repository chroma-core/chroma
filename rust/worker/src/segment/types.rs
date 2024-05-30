use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use crate::errors::{ChromaError, ErrorCodes};
use crate::execution::data::data_chunk::Chunk;
use crate::types::{
    merge_update_metadata, update_metdata_to_metdata, LogRecord, Metadata,
    MetadataValueConversionError, Operation, OperationRecord,
};
use async_trait::async_trait;
use thiserror::Error;

use super::record_segment::{ApplyMaterializedLogError, RecordSegmentReader};

#[derive(Error, Debug)]
pub enum LogMaterializerError {
    #[error("Error materializing document metadata {0}")]
    MetadataMaterializationError(#[from] MetadataValueConversionError),
    #[error("Error materializing document embedding")]
    EmbeddingMaterializationError,
    #[error("Error reading record segment {0}")]
    RecordSegmentError(#[from] Box<dyn ChromaError>),
}

impl ChromaError for LogMaterializerError {
    fn code(&self) -> ErrorCodes {
        match self {
            LogMaterializerError::MetadataMaterializationError(e) => e.code(),
            LogMaterializerError::EmbeddingMaterializationError => ErrorCodes::Internal,
            LogMaterializerError::RecordSegmentError(e) => e.code(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MaterializedLogRecord<'referred_data> {
    // This is the data record read from the record segment for this id.
    // None if the record exists only in the log.
    pub(super) data_record: Option<DataRecord<'referred_data>>,
    // If present in the record segment then it is the offset id
    // in the record segment at which the record was found.
    // If not present in the segment then it is the offset id
    // at which it should be inserted.
    pub(super) offset_id: u32,
    // Set only for the records that are being inserted for the first time
    // in the log since data_record will be None in such cases. For other
    // cases, just read from data record.
    pub(super) user_id: Option<&'referred_data str>,
    // There can be several entries in the log for an id. This is the final
    // operation that needs to be done on it. For e.g.
    // If log has [Update, Update, Delete] then final operation is Delete.
    // If log has [Insert, Update, Update, Delete] then final operation is Delete.
    // If log has [Insert, Update, Update] then final operation is Insert.
    // If log has [Update, Update] then final operation is Update.
    // All Upserts are converted to Update or Insert as the final operation.
    // For e.g. if log has [Insert, Upsert] then final operation is insert.
    // If log has [Upsert] and the record does not exist in storage then final
    // operation is Insert.
    pub(super) final_operation: Operation,
    // This is the metadata obtained by combining all the operations
    // present in the log for this id.
    // E.g. if has log has [Insert(a: h), Update(a: b, c: d), Update(a: e, f: g)] then this
    // will contain (a: e, c: d, f: g). This is None if the final operation
    // above is Delete.
    pub(super) metadata_to_be_merged: Option<Metadata>,
    // This is the final document obtained from the last non null operation.
    // E.g. if log has [Insert(str0), Update(str1), Update(str2), Update()] then this will contain
    // str2. None if final operation is Delete.
    pub(super) final_document: Option<&'referred_data str>,
    // Similar to above, this is the final embedding obtained
    // from the last non null operation.
    // E.g. if log has [Insert(emb0), Update(emb1), Update(emb2), Update()]
    // then this will contain emb2. None if final operation is Delete.
    pub(super) final_embedding: Option<&'referred_data [f32]>,
}

impl<'referred_data> From<(DataRecord<'referred_data>, u32)>
    for MaterializedLogRecord<'referred_data>
{
    fn from(data_record_info: (DataRecord<'referred_data>, u32)) -> Self {
        let data_record = data_record_info.0;
        let offset_id = data_record_info.1;
        Self {
            data_record: Some(data_record),
            offset_id,
            user_id: None,
            final_operation: Operation::Add,
            metadata_to_be_merged: None,
            final_document: None,
            final_embedding: None,
        }
    }
}

// Creates a materialized log record from the corresponding entry
// in the log (OperationRecord), offset id in storage where it will be stored (u32)
// and user id (str).
impl<'referred_data> TryFrom<(&'referred_data OperationRecord, u32, &'referred_data str)>
    for MaterializedLogRecord<'referred_data>
{
    type Error = LogMaterializerError;

    fn try_from(
        log_operation_info: (&'referred_data OperationRecord, u32, &'referred_data str),
    ) -> Result<Self, Self::Error> {
        let log_record = log_operation_info.0;
        let offset_id = log_operation_info.1;
        let user_id = log_operation_info.2;
        let metadata = match &log_record.metadata {
            Some(metadata) => match update_metdata_to_metdata(metadata) {
                Ok(m) => Some(m),
                Err(e) => {
                    return Err(LogMaterializerError::MetadataMaterializationError(e));
                }
            },
            None => None,
        };

        let document = match &log_record.document {
            Some(doc) => Some(doc.as_str()),
            None => None,
        };

        let embedding = match &log_record.embedding {
            Some(embedding) => Some(embedding.as_slice()),
            None => {
                return Err(LogMaterializerError::EmbeddingMaterializationError);
            }
        };

        Ok(Self {
            data_record: None,
            offset_id,
            user_id: Some(user_id),
            final_operation: Operation::Add,
            metadata_to_be_merged: metadata,
            final_document: document,
            final_embedding: embedding,
        })
    }
}

pub(crate) struct LogMaterializer<'me> {
    // Is None when record segment is uninitialized.
    record_segment_reader: Option<RecordSegmentReader<'me>>,
    logs: Chunk<LogRecord>,
    curr_max_offset_id: Arc<AtomicU32>,
}

impl<'me> LogMaterializer<'me> {
    pub(crate) fn new(
        record_segment_reader: Option<RecordSegmentReader<'me>>,
        logs: Chunk<LogRecord>,
        curr_max_offset_id: Arc<AtomicU32>,
    ) -> Self {
        Self {
            record_segment_reader,
            logs,
            curr_max_offset_id,
        }
    }
    pub(crate) async fn materialize(
        &'me self,
    ) -> Result<Chunk<MaterializedLogRecord<'me>>, LogMaterializerError> {
        // Populate entries that are present in the record segment.
        let mut existing_id_to_materialized: HashMap<&str, MaterializedLogRecord> = HashMap::new();
        let mut new_id_to_materialized: HashMap<&str, MaterializedLogRecord> = HashMap::new();
        match &self.record_segment_reader {
            Some(reader) => {
                for (log_record, _) in self.logs.iter() {
                    let mut exists = false;
                    match reader
                        .data_exists_for_user_id(log_record.record.id.as_str())
                        .await
                    {
                        Ok(res) => exists = res,
                        Err(e) => {
                            return Err(LogMaterializerError::RecordSegmentError(e));
                        }
                    };
                    if exists {
                        match reader
                            .get_data_and_offset_id_for_user_id(log_record.record.id.as_str())
                            .await
                        {
                            Ok((data_record, offset_id)) => {
                                existing_id_to_materialized.insert(
                                    log_record.record.id.as_str(),
                                    MaterializedLogRecord::from((data_record, offset_id)),
                                );
                            }
                            Err(e) => {
                                return Err(LogMaterializerError::RecordSegmentError(e));
                            }
                        }
                    }
                }
            }
            // If record segment is uninitialized then there's nothing
            // in the record segment yet.
            None => (),
        }
        // Populate updates to these and fresh records that are being
        // inserted for the first time.
        for (log_record, _) in self.logs.iter() {
            match log_record.record.operation {
                Operation::Add => {
                    // If user is trying to insert a key that already exists in
                    // storage then ignore. Also if it already existed in the log
                    // before then also ignore.
                    if !existing_id_to_materialized.contains_key(log_record.record.id.as_str())
                        && !new_id_to_materialized.contains_key(log_record.record.id.as_str())
                    {
                        let next_offset_id = self
                            .curr_max_offset_id
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let materialized_record = match MaterializedLogRecord::try_from((
                            &log_record.record,
                            next_offset_id,
                            log_record.record.id.as_str(),
                        )) {
                            Ok(record) => record,
                            Err(e) => {
                                return Err(e);
                            }
                        };
                        new_id_to_materialized
                            .insert(log_record.record.id.as_str(), materialized_record);
                    }
                }
                Operation::Delete => {
                    // If the delete is for a record that is currently not in the
                    // record segment, then we can just NOT process these records
                    // at all. On the other hand if it is for a record that is currently
                    // in the record segment then we'll have to pass it as a delete
                    // to the compactor so that it can be deleted.
                    if new_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                        new_id_to_materialized.remove(log_record.record.id.as_str());
                    } else if existing_id_to_materialized
                        .contains_key(log_record.record.id.as_str())
                    {
                        // Mark state as deleted. Other fields become noop after such a delete.
                        // We should still clear them out since there can be a subsequent insert
                        // for the same id after the delete.
                        let record_from_map = existing_id_to_materialized
                            .get_mut(log_record.record.id.as_str())
                            .unwrap();
                        record_from_map.final_operation = Operation::Delete;
                        record_from_map.final_document = None;
                        record_from_map.final_embedding = None;
                        record_from_map.metadata_to_be_merged = None;
                        record_from_map.user_id = None;
                    }
                }
                Operation::Update => {
                    let mut created_in_log = true;
                    let record_from_map = match existing_id_to_materialized
                        .get_mut(log_record.record.id.as_str())
                    {
                        Some(res) => {
                            created_in_log = false;
                            res
                        }
                        None => match new_id_to_materialized.get_mut(log_record.record.id.as_str())
                        {
                            Some(res) => res,
                            None => {
                                // Does not exist in either maps. Ignore this update.
                                continue;
                            }
                        },
                    };

                    record_from_map.metadata_to_be_merged = match merge_update_metadata(
                        &record_from_map.metadata_to_be_merged,
                        &log_record.record.metadata,
                    ) {
                        Ok(meta) => meta,
                        Err(e) => {
                            return Err(LogMaterializerError::MetadataMaterializationError(e));
                        }
                    };
                    if log_record.record.document.is_some() {
                        record_from_map.final_document =
                            Some(log_record.record.document.as_ref().unwrap().as_str());
                    }
                    if log_record.record.embedding.is_some() {
                        record_from_map.final_embedding =
                            Some(log_record.record.embedding.as_ref().unwrap().as_slice());
                    }
                    // Only update the operation state for records that were not created
                    // from the log.
                    if !created_in_log {
                        record_from_map.final_operation = Operation::Update;
                    }
                }
                Operation::Upsert => {
                    if existing_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                        // Just another update.
                        let record_from_map = existing_id_to_materialized
                            .get_mut(log_record.record.id.as_str())
                            .unwrap();
                        record_from_map.metadata_to_be_merged = match merge_update_metadata(
                            &record_from_map.metadata_to_be_merged,
                            &log_record.record.metadata,
                        ) {
                            Ok(meta) => meta,
                            Err(e) => {
                                return Err(LogMaterializerError::MetadataMaterializationError(e));
                            }
                        };
                        if log_record.record.document.is_some() {
                            record_from_map.final_document =
                                Some(log_record.record.document.as_ref().unwrap().as_str());
                        }
                        if log_record.record.embedding.is_some() {
                            record_from_map.final_embedding =
                                Some(log_record.record.embedding.as_ref().unwrap().as_slice());
                        }
                        // We implicitly convert all upsert operations to either update
                        // or insert depending on whether it already existed in storage or not.
                        record_from_map.final_operation = Operation::Update;
                    } else if new_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                        // Just another update.
                        let record_from_map = new_id_to_materialized
                            .get_mut(log_record.record.id.as_str())
                            .unwrap();
                        record_from_map.metadata_to_be_merged = match merge_update_metadata(
                            &record_from_map.metadata_to_be_merged,
                            &log_record.record.metadata,
                        ) {
                            Ok(meta) => meta,
                            Err(e) => {
                                return Err(LogMaterializerError::MetadataMaterializationError(e));
                            }
                        };
                        if log_record.record.document.is_some() {
                            record_from_map.final_document =
                                Some(log_record.record.document.as_ref().unwrap().as_str());
                        }
                        if log_record.record.embedding.is_some() {
                            record_from_map.final_embedding =
                                Some(log_record.record.embedding.as_ref().unwrap().as_slice());
                        }
                        // This record is not present on storage yet hence final operation is
                        // Add.
                        record_from_map.final_operation = Operation::Add;
                    } else {
                        // Insert.
                        let next_offset_id = self
                            .curr_max_offset_id
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let materialized_record = match MaterializedLogRecord::try_from((
                            &log_record.record,
                            next_offset_id,
                            log_record.record.id.as_str(),
                        )) {
                            Ok(record) => record,
                            Err(e) => {
                                return Err(e);
                            }
                        };
                        new_id_to_materialized
                            .insert(log_record.record.id.as_str(), materialized_record);
                    }
                }
            }
        }
        let mut res = vec![];
        for (_key, value) in existing_id_to_materialized {
            res.push(value);
        }
        for (_key, value) in new_id_to_materialized {
            res.push(value);
        }
        res.sort_by(|x, y| x.offset_id.cmp(&y.offset_id));
        Ok(Chunk::new(res.into()))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DataRecord<'a> {
    pub(crate) id: &'a str,
    pub(crate) embedding: &'a [f32],
    pub(crate) metadata: Option<Metadata>,
    pub(crate) document: Option<&'a str>,
}

impl DataRecord<'_> {
    pub(crate) fn get_size(&self) -> usize {
        let id_size = self.id.len();
        let embedding_size = self.embedding.len() * std::mem::size_of::<f32>();
        // TODO: use serialized_metadata size to calculate the size
        let metadata_size = 0;
        let document_size = match self.document {
            Some(document) => document.len(),
            None => 0,
        };
        id_size + embedding_size + metadata_size + document_size
    }
}

pub(crate) trait SegmentWriter<'a> {
    async fn apply_materialized_log_chunk(
        &self,
        records: Chunk<MaterializedLogRecord<'a>>,
    ) -> Result<(), ApplyMaterializedLogError>;
    fn commit(self) -> Result<impl SegmentFlusher, Box<dyn ChromaError>>;
}

#[async_trait]
pub(crate) trait SegmentFlusher {
    async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>>;
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::{
        blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider},
        segment::record_segment::{RecordSegmentReaderCreationError, RecordSegmentWriter},
        storage::{local::LocalStorage, Storage},
        types::{MetadataValue, Operation, OperationRecord, UpdateMetadataValue},
    };
    use std::{collections::HashMap, str::FromStr};

    #[tokio::test]
    async fn test_materializer() {
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
        {
            let segment_writer =
                RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
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
                        document: Some(String::from("doc1")),
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
                        document: Some(String::from("doc2")),
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
                            assert!(1 == 1, "Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                            assert!(1 == 1, "Error creating record segment reader");
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
            segment_writer
                .apply_materialized_log_chunk(mat_records)
                .await
                .expect("Apply materialized log failed");
            let flusher = segment_writer
                .commit()
                .expect("Commit for segment writer failed");
            record_segment.file_path = flusher.flush().await.expect("Flush segment writer failed");
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
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: Some(update_metadata.clone()),
                    document: None,
                    operation: Operation::Update,
                },
            },
            LogRecord {
                log_offset: 4,
                record: OperationRecord {
                    id: "embedding_id_3".to_string(),
                    embedding: Some(vec![7.0, 8.0, 9.0]),
                    encoding: None,
                    metadata: Some(update_metadata),
                    document: Some(String::from("doc3")),
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 5,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Delete,
                },
            },
        ];
        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let reader = RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
            .await
            .expect("Error creating segment reader");
        let curr_max_offset_id = Arc::new(AtomicU32::new(3));
        let materializer = LogMaterializer {
            record_segment_reader: Some(reader),
            logs: data,
            curr_max_offset_id,
        };
        let res = materializer
            .materialize()
            .await
            .expect("Error materializing logs");
        assert_eq!(3, res.len());
        let mut id1_found = 0;
        let mut id2_found = 0;
        let mut id3_found = 0;
        for (log, _) in res.iter() {
            // Embedding 3.
            if log.user_id.is_some() {
                id3_found += 1;
                assert_eq!("embedding_id_3", log.user_id.unwrap());
                assert_eq!(true, log.data_record.is_none());
                assert_eq!("doc3", log.final_document.unwrap());
                assert_eq!(vec![7.0, 8.0, 9.0], log.final_embedding.unwrap());
                assert_eq!(3, log.offset_id);
                assert_eq!(Operation::Add, log.final_operation);
                let mut hello_found = 0;
                let mut hello_again_found = 0;
                for (key, value) in log.metadata_to_be_merged.as_ref().unwrap() {
                    if key == "hello" {
                        assert_eq!(MetadataValue::Str(String::from("new_world")), *value);
                        hello_found += 1;
                    } else if key == "hello_again" {
                        assert_eq!(MetadataValue::Str(String::from("new_world")), *value);
                        hello_again_found += 1;
                    } else {
                        assert!(1 == 1, "Not expecting any other key");
                    }
                }
                assert_eq!(hello_found, 1);
                assert_eq!(hello_again_found, 1);
            } else if log.data_record.as_ref().unwrap().id == "embedding_id_2" {
                id2_found += 1;
                assert_eq!(Operation::Delete, log.final_operation);
                assert_eq!(2, log.offset_id);
                assert_eq!(None, log.final_document);
                assert_eq!(None, log.final_embedding);
                assert_eq!(None, log.user_id);
                assert_eq!(None, log.metadata_to_be_merged);
                assert_eq!(true, log.data_record.is_some());
            } else if log.data_record.as_ref().unwrap().id == "embedding_id_1" {
                id1_found += 1;
                assert_eq!(Operation::Update, log.final_operation);
                assert_eq!(1, log.offset_id);
                assert_eq!(None, log.final_document);
                assert_eq!(None, log.final_embedding);
                assert_eq!(None, log.user_id);
                let mut hello_found = 0;
                let mut hello_again_found = 0;
                for (key, value) in log.metadata_to_be_merged.as_ref().unwrap() {
                    if key == "hello" {
                        assert_eq!(MetadataValue::Str(String::from("new_world")), *value);
                        hello_found += 1;
                    } else if key == "hello_again" {
                        assert_eq!(MetadataValue::Str(String::from("new_world")), *value);
                        hello_again_found += 1;
                    } else {
                        assert!(1 == 1, "Not expecting any other key");
                    }
                }
                assert_eq!(hello_found, 1);
                assert_eq!(hello_again_found, 1);
                assert_eq!(true, log.data_record.is_some());
                assert_eq!(log.data_record.as_ref().unwrap().document, Some("doc1"));
                assert_eq!(
                    log.data_record.as_ref().unwrap().embedding,
                    vec![1.0, 2.0, 3.0].as_slice()
                );
                hello_found = 0;
                let mut bye_found = 0;
                for (key, value) in log.data_record.as_ref().unwrap().metadata.as_ref().unwrap() {
                    if key == "hello" {
                        assert_eq!(MetadataValue::Str(String::from("world")), *value);
                        hello_found += 1;
                    } else if key == "bye" {
                        assert_eq!(MetadataValue::Str(String::from("world")), *value);
                        bye_found += 1;
                    } else {
                        assert!(1 == 1, "Not expecting any other key");
                    }
                }
                assert_eq!(hello_found, 1);
                assert_eq!(bye_found, 1);
            } else {
                assert!(1 == 1, "Not expecting any other materialized record");
            }
        }
        assert_eq!(1, id1_found);
        assert_eq!(1, id2_found);
        assert_eq!(1, id3_found);
        // Now write this, read again and validate.
        let segment_writer =
            RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        segment_writer
            .apply_materialized_log_chunk(res)
            .await
            .expect("Error applying materialized log chunk");
        let flusher = segment_writer
            .commit()
            .expect("Commit for segment writer failed");
        record_segment.file_path = flusher.flush().await.expect("Flush segment writer failed");
        // Read.
        let segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Error creating segment reader");
        let all_data = segment_reader
            .get_all_data()
            .await
            .expect("Get all data failed");
        for data in all_data {
            assert_ne!(data.id, "embedding_id_2");
            if data.id == "embedding_id_1" {
                assert_eq!(
                    data.metadata
                        .clone()
                        .expect("Metadata is empty")
                        .contains_key("hello"),
                    true
                );
                assert_eq!(
                    data.metadata
                        .clone()
                        .expect("Metadata is empty")
                        .get("hello"),
                    Some(&MetadataValue::Str(String::from("new_world")))
                );
                assert_eq!(
                    data.metadata
                        .clone()
                        .expect("Metadata is empty")
                        .contains_key("bye"),
                    true
                );
                assert_eq!(
                    data.metadata.clone().expect("Metadata is empty").get("bye"),
                    Some(&MetadataValue::Str(String::from("world")))
                );
                assert_eq!(
                    data.metadata
                        .clone()
                        .expect("Metadata is empty")
                        .contains_key("hello_again"),
                    true
                );
                assert_eq!(
                    data.metadata
                        .clone()
                        .expect("Metadata is empty")
                        .get("hello_again"),
                    Some(&MetadataValue::Str(String::from("new_world")))
                );
                assert_eq!(data.document.expect("Non empty document"), "doc1");
                assert_eq!(data.embedding, vec![1.0, 2.0, 3.0]);
            } else if data.id == "embedding_id_3" {
                assert_eq!(
                    data.metadata
                        .clone()
                        .expect("Metadata is empty")
                        .contains_key("hello"),
                    true
                );
                assert_eq!(
                    data.metadata
                        .clone()
                        .expect("Metadata is empty")
                        .get("hello"),
                    Some(&MetadataValue::Str(String::from("new_world")))
                );
                assert_eq!(
                    data.metadata
                        .clone()
                        .expect("Metadata is empty")
                        .contains_key("hello_again"),
                    true
                );
                assert_eq!(
                    data.metadata
                        .clone()
                        .expect("Metadata is empty")
                        .get("hello_again"),
                    Some(&MetadataValue::Str(String::from("new_world")))
                );
                assert_eq!(data.document.expect("Non empty document"), "doc3");
                assert_eq!(data.embedding, vec![7.0, 8.0, 9.0]);
            }
        }
    }
}
