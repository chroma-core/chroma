use core::panic;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU16, AtomicU32},
        Arc,
    },
};

use futures::stream::Count;
use roaring::RoaringBitmap;
use tonic::async_trait;

use crate::{
    blockstore::{key::KeyWrapper, provider::BlockfileProvider},
    chroma_proto::r#where,
    execution::{
        data::data_chunk::Chunk, operator::Operator,
        operators::write_segments::WriteSegmentsOperatorError,
    },
    index::metadata::types::process_where_clause,
    segment::{
        record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
        LogMaterializer, MaterializedLogRecord,
    },
    types::{LogRecord, Metadata, MetadataValue, Operation, Segment, Where, WhereClauseComparator},
};

use super::count_records::CountRecordsError;

#[derive(Debug)]
pub(crate) struct BruteForceMetadataFilteringOperator {}

impl BruteForceMetadataFilteringOperator {
    pub(crate) fn new() -> Box<Self> {
        Box::new(BruteForceMetadataFilteringOperator {})
    }
}

#[derive(Debug)]
pub(crate) struct BruteForceMetadataFilteringInput {
    log_record: Chunk<LogRecord>,
    record_segment: Segment,
    blockfile_provider: BlockfileProvider,
    curr_max_offset_id: Arc<AtomicU32>,
    where_clause: Where,
    // TODO(Sanket): Add Where document.
}

impl BruteForceMetadataFilteringInput {
    pub(crate) fn new(
        log_record: Chunk<LogRecord>,
        record_segment: Segment,
        blockfile_provider: BlockfileProvider,
        where_clause: Where,
        curr_max_offset_id: Arc<AtomicU32>,
    ) -> Self {
        Self {
            log_record,
            record_segment,
            blockfile_provider,
            where_clause,
            curr_max_offset_id,
        }
    }
}

#[derive(Debug)]
pub(crate) struct BruteForceMetadataFilteringOutput {
    // Offset Ids.
    filtered_documents: Vec<usize>,
}

#[async_trait]
impl Operator<BruteForceMetadataFilteringInput, BruteForceMetadataFilteringOutput>
    for BruteForceMetadataFilteringOperator
{
    type Error = CountRecordsError;
    async fn run(
        &self,
        input: &BruteForceMetadataFilteringInput,
    ) -> Result<BruteForceMetadataFilteringOutput, CountRecordsError> {
        let record_segment_reader: Option<RecordSegmentReader>;
        match RecordSegmentReader::from_segment(&input.record_segment, &input.blockfile_provider)
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
                        return Err(CountRecordsError::RecordSegmentCreateError(
                            RecordSegmentReaderCreationError::BlockfileOpenError(e),
                        ));
                    }
                    RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                        tracing::error!("Error creating record segment reader {}", e);
                        return Err(CountRecordsError::RecordSegmentCreateError(
                            RecordSegmentReaderCreationError::InvalidNumberOfFiles,
                        ));
                    }
                };
            }
        };
        let materializer = LogMaterializer::new(
            record_segment_reader,
            input.log_record.clone(),
            input.curr_max_offset_id.clone(),
        );
        let mat_records = match materializer.materialize().await {
            Ok(records) => records,
            Err(e) => {
                return Err(CountRecordsError::RecordSegmentReadError(Box::new(e)));
            }
        };
        let mut ids_to_metadata: HashMap<u32, HashMap<&String, &MetadataValue>> = HashMap::new();
        for (records, _) in mat_records.iter() {
            if records.final_operation == Operation::Delete {
                continue;
            }
            if !ids_to_metadata.contains_key(&records.offset_id) {
                ids_to_metadata.insert(records.offset_id, HashMap::new());
            }
            let map_pointer = ids_to_metadata
                .get_mut(&records.offset_id)
                .expect("Not possible.");
            match &records.data_record {
                Some(data_record) => match &data_record.metadata {
                    Some(meta) => {
                        for (meta_key, meta_val) in meta {
                            map_pointer.insert(&meta_key, &meta_val);
                        }
                    }
                    None => (),
                },
                None => (),
            };
            match &records.metadata_to_be_merged {
                Some(meta) => {
                    for (meta_key, meta_val) in meta {
                        map_pointer.insert(meta_key, meta_val);
                    }
                }
                None => (),
            };
        }
        // Time now to perform a metadata search based on where clause.
        let clo = |metadata_key: &str,
                   metadata_value: &crate::blockstore::key::KeyWrapper,
                   metadata_type: crate::types::MetadataType,
                   comparator: WhereClauseComparator| {
            match metadata_type {
                crate::types::MetadataType::StringType => match comparator {
                    WhereClauseComparator::Equal => {
                        let mut result = RoaringBitmap::new();
                        // Construct a bitmap consisting of all offset ids
                        // that have this key equal to this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(&metadata_key.to_string()) {
                                match *val {
                                    MetadataValue::Str(string_value) => {
                                        if let KeyWrapper::String(where_value) = metadata_value {
                                            if *string_value == *where_value {
                                                result.insert(*offset_id);
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                        return result;
                    }
                    WhereClauseComparator::NotEqual => {
                        todo!();
                    }
                    // We don't allow these comparators for strings.
                    WhereClauseComparator::LessThan => {
                        unimplemented!();
                    }
                    WhereClauseComparator::LessThanOrEqual => {
                        unimplemented!();
                    }
                    WhereClauseComparator::GreaterThan => {
                        unimplemented!();
                    }
                    WhereClauseComparator::GreaterThanOrEqual => {
                        unimplemented!();
                    }
                },
                crate::types::MetadataType::IntType => match comparator {
                    WhereClauseComparator::Equal => {
                        let mut result = RoaringBitmap::new();
                        // Construct a bitmap consisting of all offset ids
                        // that have this key equal to this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(&metadata_key.to_string()) {
                                match *val {
                                    MetadataValue::Int(int_value) => {
                                        if let KeyWrapper::Uint32(where_value) = metadata_value {
                                            if *int_value as u32 == *where_value {
                                                result.insert(*offset_id);
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                        return result;
                    }
                    WhereClauseComparator::NotEqual => {
                        todo!();
                    }
                    WhereClauseComparator::LessThan => {
                        let mut result = RoaringBitmap::new();
                        // Construct a bitmap consisting of all offset ids
                        // that have this key equal to this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(&metadata_key.to_string()) {
                                match *val {
                                    MetadataValue::Int(int_value) => {
                                        if let KeyWrapper::Uint32(where_value) = metadata_value {
                                            if ((*int_value) as u32) < (*where_value) {
                                                result.insert(*offset_id);
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                        return result;
                    }
                    WhereClauseComparator::LessThanOrEqual => {
                        let mut result = RoaringBitmap::new();
                        // Construct a bitmap consisting of all offset ids
                        // that have this key equal to this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(&metadata_key.to_string()) {
                                match *val {
                                    MetadataValue::Int(int_value) => {
                                        if let KeyWrapper::Uint32(where_value) = metadata_value {
                                            if ((*int_value) as u32) <= (*where_value) {
                                                result.insert(*offset_id);
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                        return result;
                    }
                    WhereClauseComparator::GreaterThan => {
                        let mut result = RoaringBitmap::new();
                        // Construct a bitmap consisting of all offset ids
                        // that have this key equal to this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(&metadata_key.to_string()) {
                                match *val {
                                    MetadataValue::Int(int_value) => {
                                        if let KeyWrapper::Uint32(where_value) = metadata_value {
                                            if ((*int_value) as u32) > (*where_value) {
                                                result.insert(*offset_id);
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                        return result;
                    }
                    WhereClauseComparator::GreaterThanOrEqual => {
                        let mut result = RoaringBitmap::new();
                        // Construct a bitmap consisting of all offset ids
                        // that have this key equal to this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(&metadata_key.to_string()) {
                                match *val {
                                    MetadataValue::Int(int_value) => {
                                        if let KeyWrapper::Uint32(where_value) = metadata_value {
                                            if ((*int_value) as u32) >= (*where_value) {
                                                result.insert(*offset_id);
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                        return result;
                    }
                },
                crate::types::MetadataType::DoubleType => match comparator {
                    WhereClauseComparator::Equal => {
                        let mut result = RoaringBitmap::new();
                        // Construct a bitmap consisting of all offset ids
                        // that have this key equal to this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(&metadata_key.to_string()) {
                                match *val {
                                    MetadataValue::Float(float_value) => {
                                        if let KeyWrapper::Float32(where_value) = metadata_value {
                                            if ((*float_value) as f32) == (*where_value) {
                                                result.insert(*offset_id);
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                        return result;
                    }
                    WhereClauseComparator::NotEqual => {
                        todo!();
                    }
                    WhereClauseComparator::LessThan => {
                        let mut result = RoaringBitmap::new();
                        // Construct a bitmap consisting of all offset ids
                        // that have this key equal to this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(&metadata_key.to_string()) {
                                match *val {
                                    MetadataValue::Float(float_value) => {
                                        if let KeyWrapper::Float32(where_value) = metadata_value {
                                            if ((*float_value) as f32) < (*where_value) {
                                                result.insert(*offset_id);
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                        return result;
                    }
                    WhereClauseComparator::LessThanOrEqual => {
                        let mut result = RoaringBitmap::new();
                        // Construct a bitmap consisting of all offset ids
                        // that have this key equal to this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(&metadata_key.to_string()) {
                                match *val {
                                    MetadataValue::Float(float_value) => {
                                        if let KeyWrapper::Float32(where_value) = metadata_value {
                                            if ((*float_value) as f32) <= (*where_value) {
                                                result.insert(*offset_id);
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                        return result;
                    }
                    WhereClauseComparator::GreaterThan => {
                        let mut result = RoaringBitmap::new();
                        // Construct a bitmap consisting of all offset ids
                        // that have this key equal to this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(&metadata_key.to_string()) {
                                match *val {
                                    MetadataValue::Float(float_value) => {
                                        if let KeyWrapper::Float32(where_value) = metadata_value {
                                            if ((*float_value) as f32) > (*where_value) {
                                                result.insert(*offset_id);
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                        return result;
                    }
                    WhereClauseComparator::GreaterThanOrEqual => {
                        let mut result = RoaringBitmap::new();
                        // Construct a bitmap consisting of all offset ids
                        // that have this key equal to this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(&metadata_key.to_string()) {
                                match *val {
                                    MetadataValue::Float(float_value) => {
                                        if let KeyWrapper::Float32(where_value) = metadata_value {
                                            if ((*float_value) as f32) >= (*where_value) {
                                                result.insert(*offset_id);
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                        return result;
                    }
                },
                crate::types::MetadataType::StringListType => {
                    todo!();
                }
                crate::types::MetadataType::IntListType => {
                    todo!();
                }
                crate::types::MetadataType::DoubleListType => {
                    todo!();
                }
            }
        };
        let res = match process_where_clause(&input.where_clause, &clo) {
            Ok(r) => r,
            Err(e) => panic!("Failed parsing where clause"),
        };
        return Ok(BruteForceMetadataFilteringOutput {
            filtered_documents: res,
        });
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
            operators::brute_force_metadata_filtering::{
                BruteForceMetadataFilteringInput, BruteForceMetadataFilteringOperator,
            },
        },
        segment::{
            record_segment::{
                RecordSegmentReader, RecordSegmentReaderCreationError, RecordSegmentWriter,
            },
            types::SegmentFlusher,
            LogMaterializer, SegmentWriter,
        },
        storage::{local::LocalStorage, Storage},
        types::{
            DirectComparison, LogRecord, Operation, OperationRecord, UpdateMetadataValue, Where,
            WhereComparison,
        },
    };

    #[tokio::test]
    async fn test_where_clause() {
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
                    embedding: Some(vec![10.0, 11.0, 12.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Update,
                },
            },
        ];
        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let operator = BruteForceMetadataFilteringOperator::new();
        let curr_max_offset_id = Arc::new(AtomicU32::new(3));
        let where_clause: Where = Where::DirectWhereComparison(DirectComparison {
            key: String::from("hello"),
            comparison: WhereComparison::SingleStringComparison(
                String::from("new_world"),
                crate::types::WhereClauseComparator::Equal,
            ),
        });
        let input = BruteForceMetadataFilteringInput::new(
            data,
            record_segment,
            blockfile_provider,
            where_clause,
            curr_max_offset_id,
        );
        let mut res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(2, res.filtered_documents.len());
        res.filtered_documents.sort();
        assert_eq!(1, *res.filtered_documents.get(0).expect("Expect not none"));
        assert_eq!(3, *res.filtered_documents.get(1).expect("Expect not none"));
    }
}
