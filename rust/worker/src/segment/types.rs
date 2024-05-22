use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use crate::errors::ChromaError;
use crate::execution::data::data_chunk::Chunk;
use crate::types::{
    merge_update_metadata, update_metdata_to_metdata, LogRecord, Metadata, Operation,
    OperationRecord,
};
use async_trait::async_trait;

use super::record_segment::RecordSegmentReader;

#[derive(Debug)]
pub(crate) struct MaterializedLogRecord<'a> {
    pub(super) segment_offset_id: u32,
    pub(super) log_record: &'a LogRecord,
    pub(super) materialized_record: DataRecord<'a>,
}

impl<'a> MaterializedLogRecord<'a> {
    pub(crate) fn new(
        segment_offset_id: u32,
        log_record: &'a LogRecord,
        materialized_record: DataRecord<'a>,
    ) -> Self {
        Self {
            segment_offset_id,
            log_record,
            materialized_record,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MaterializedLogRecordV2<'a> {
    // This is the data record read from the record segment for this id.
    // None if the record exists only in the log.
    data_record: Option<DataRecord<'a>>,
    // If present in the record segment then it is the offset id
    // in the record segment at which the record was found.
    // If not present in the segment then it is the offset id
    // at which it should be inserted.
    offset_id: u32,
    // Set only for the records that are being inserted for the first time
    // in the log since data_record will be None in such cases.
    user_id: Option<&'a str>,
    // There can be several entries in the log for an id. This is the final
    // operation that needs to be done on it. For e.g.
    // If log has [Update, Update, Delete] then final operation is Delete.
    // If log has [Insert, Update, Update, Delete] then final operation is Delete.
    // If log has [Insert, Update, Update] then final operation is Insert.
    // If log has [Update, Update] then final operation is Update.
    final_operation: Operation,
    // This is the metadata obtained by combining all the operations
    // present in the log for this id.
    // E.g. if has log has [Insert(a: h), Update(a: b, c: d), Update(a: e, f: g)] then this
    // will contain (a: e, c: d, f: g). This is None if the final operation
    // above is Delete.
    metadata_to_be_merged: Option<Metadata>,
    // This is the final document obtained from the last non null operation.
    // E.g. if log has [Insert(str0), Update(str1), Update(str2), Update()] then this will contain
    // str2. None if final operation is Delete.
    final_document: Option<&'a str>,
    // Similar to above, this is the final embedding obtained
    // from the last non null operation.
    // E.g. if log has [Insert(emb0), Update(emb1), Update(emb2), Update()]
    // then this will contain emb2. None if final operation is Delete.
    final_embedding: Option<&'a [f32]>,
}

impl<'a> MaterializedLogRecordV2<'a> {
    pub(crate) fn from_data_record(data_record: DataRecord<'a>, offset_id: u32) -> Self {
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

    pub(crate) fn from_operation_record(
        log_record: &'a OperationRecord,
        offset_id: u32,
        user_id: &'a str,
    ) -> Self {
        let metadata = match &log_record.metadata {
            Some(metadata) => match update_metdata_to_metdata(metadata) {
                Ok(m) => Some(m),
                Err(e) => panic!("Should not panic. TODO"),
            },
            None => None,
        };

        let document = match &log_record.document {
            Some(doc) => Some(doc.as_str()),
            // TODO(Sanket): Should this be an error since this is an insert operation.
            None => None,
        };

        let embedding = match &log_record.embedding {
            Some(embedding) => Some(embedding.as_slice()),
            // TODO(Sanket): Should this be an error since this is an insert operation.
            None => None,
        };

        Self {
            data_record: None,
            offset_id,
            user_id: Some(user_id),
            final_operation: Operation::Add,
            metadata_to_be_merged: metadata,
            final_document: document,
            final_embedding: embedding,
        }
    }
}

pub(crate) struct LogMaterializerV2<'a> {
    record_segment_reader: RecordSegmentReader<'a>,
    logs: Chunk<LogRecord>,
    curr_max_offset_id: Arc<AtomicU32>,
}

impl<'a> LogMaterializerV2<'a> {
    pub(crate) async fn materializeV2(&'a self) -> Chunk<MaterializedLogRecordV2<'a>> {
        // Find entries that can be skipped completely.
        let mut existing_id_to_materialized: HashMap<&str, MaterializedLogRecordV2> =
            HashMap::new();
        let mut new_id_to_materialized: HashMap<&str, MaterializedLogRecordV2> = HashMap::new();
        for (log_record, _) in self.logs.iter() {
            let mut exists: bool = false;
            match self
                .record_segment_reader
                .data_exists_for_user_id(log_record.record.id.as_str())
                .await
            {
                Ok(res) => exists = res,
                Err(e) => (),
            };
            if exists {
                match self
                    .record_segment_reader
                    .get_data_and_offset_id_for_user_id(log_record.record.id.as_str())
                    .await
                {
                    Ok((data_record, offset_id)) => {
                        existing_id_to_materialized.insert(
                            log_record.record.id.as_str(),
                            MaterializedLogRecordV2::from_data_record(data_record, offset_id),
                        );
                    }
                    // TODO(Sanket): Error handling here.
                    Err(e) => (),
                }
            }
        }
        // Time to build the actual records.
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
                        new_id_to_materialized.insert(
                            log_record.record.id.as_str(),
                            MaterializedLogRecordV2::from_operation_record(
                                &log_record.record,
                                next_offset_id,
                                log_record.record.id.as_str(),
                            ),
                        );
                    }
                }
                Operation::Delete => {
                    // If the delete is for a record that is currently not in the
                    // record segment, then we can just not process these records
                    // at all. On the other hand if it is for a record that is currently
                    // in the record segment then we'll have to pass it as a delete
                    // to the compactor so that it can be deleted.
                    if new_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                        new_id_to_materialized.remove(log_record.record.id.as_str());
                    } else if existing_id_to_materialized
                        .contains_key(log_record.record.id.as_str())
                    {
                        // Mark state as deleted.
                        existing_id_to_materialized
                            .get_mut(log_record.record.id.as_str())
                            .unwrap()
                            .final_operation = Operation::Delete;
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
                                continue;
                            }
                        },
                    };

                    record_from_map.metadata_to_be_merged = merge_update_metadata(
                        &record_from_map.metadata_to_be_merged,
                        &log_record.record.metadata,
                    );
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
                        record_from_map.metadata_to_be_merged = merge_update_metadata(
                            &record_from_map.metadata_to_be_merged,
                            &log_record.record.metadata,
                        );
                        if log_record.record.document.is_some() {
                            record_from_map.final_document =
                                Some(log_record.record.document.as_ref().unwrap().as_str());
                        }
                        if log_record.record.embedding.is_some() {
                            record_from_map.final_embedding =
                                Some(log_record.record.embedding.as_ref().unwrap().as_slice());
                        }
                        record_from_map.final_operation = Operation::Upsert;
                    } else if new_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                        // Just another update.
                        let record_from_map = new_id_to_materialized
                            .get_mut(log_record.record.id.as_str())
                            .unwrap();
                        record_from_map.metadata_to_be_merged = merge_update_metadata(
                            &record_from_map.metadata_to_be_merged,
                            &log_record.record.metadata,
                        );
                        if log_record.record.document.is_some() {
                            record_from_map.final_document =
                                Some(log_record.record.document.as_ref().unwrap().as_str());
                        }
                        if log_record.record.embedding.is_some() {
                            record_from_map.final_embedding =
                                Some(log_record.record.embedding.as_ref().unwrap().as_slice());
                        }
                    } else {
                        // Insert.
                        let next_offset_id = self
                            .curr_max_offset_id
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        new_id_to_materialized.insert(
                            log_record.record.id.as_str(),
                            MaterializedLogRecordV2::from_operation_record(
                                &log_record.record,
                                next_offset_id,
                                log_record.record.id.as_str(),
                            ),
                        );
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
        Chunk::new(res.into())
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

pub(crate) trait SegmentWriter {
    fn apply_materialized_log_chunk(&self, records: Chunk<MaterializedLogRecord>);
    fn apply_log_chunk(&self, records: Chunk<LogRecord>);
    fn commit(self) -> Result<impl SegmentFlusher, Box<dyn ChromaError>>;
}

#[async_trait]
pub(crate) trait SegmentFlusher {
    async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>>;
}

#[async_trait]
pub(crate) trait LogMaterializer: SegmentWriter {
    async fn materialize<'chunk>(
        &self,
        records: &'chunk Chunk<LogRecord>,
    ) -> Chunk<MaterializedLogRecord<'chunk>>;
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::{
        blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider},
        segment::record_segment::RecordSegmentWriter,
        storage::{local::LocalStorage, Storage},
        types::{MetadataValue, Operation, OperationRecord, UpdateMetadataValue},
    };
    use std::{collections::HashMap, str::FromStr};

    #[tokio::test]
    async fn test_materializer_v2() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(storage);
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        // let in_memory_provider = BlockfileProvider::new_memory();
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
            segment_writer.materialize(&data).await;
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
        let curr_max_offset_id = Arc::new(AtomicU32::new(2));
        let materializer = LogMaterializerV2 {
            record_segment_reader: reader,
            logs: data,
            curr_max_offset_id,
        };
        let res = materializer.materializeV2().await;
        assert_eq!(3, res.len());
        for (log, _) in res.iter() {
            // Embedding 3.
            if log.user_id.is_some() {
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
            }
        }
    }

    // This is just a POC test to show how the materialize method could be tested, we can
    // remove it later
    #[test]
    fn test_materialize() {
        let mut metadata_1 = HashMap::new();
        metadata_1.insert("key".to_string(), MetadataValue::Str("value".to_string()));
        let metadata_1 = Some(metadata_1);

        let data = vec![
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
        ];
        let data: Chunk<LogRecord> = Chunk::new(data.into());

        let materialized_data = data
            .iter()
            .map(|record| MaterializedLogRecord {
                segment_offset_id: 0,
                log_record: record.0,
                materialized_record: DataRecord {
                    id: &record.0.record.id,
                    embedding: &[],
                    metadata: metadata_1.clone(),
                    document: None,
                },
            })
            .collect::<Vec<_>>();

        let materialized_chunk = Chunk::new(materialized_data.into());
        drop(materialized_chunk);
        drop(data);
    }
}
