use crate::{
    blockstore::{key::KeyWrapper, provider::BlockfileProvider},
    errors::{ChromaError, ErrorCodes},
    execution::{data::data_chunk::Chunk, operator::Operator},
    index::{
        fulltext::types::process_where_document_clause_with_callback,
        metadata::types::{process_where_clause_with_callback, MetadataIndexError},
    },
    segment::{
        metadata_segment::{MetadataSegmentError, MetadataSegmentReader},
        record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
        LogMaterializer, LogMaterializerError,
    },
    types::{
        LogRecord, MetadataValue, Operation, Segment, Where, WhereClauseComparator, WhereDocument,
        WhereDocumentOperator,
    },
    utils::{merge_sorted_vecs_conjunction, merge_sorted_vecs_disjunction},
};
use core::panic;
use roaring::RoaringBitmap;
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use tonic::async_trait;

#[derive(Debug)]
pub(crate) struct MetadataFilteringOperator {}

impl MetadataFilteringOperator {
    pub(crate) fn new() -> Box<Self> {
        Box::new(MetadataFilteringOperator {})
    }
}

#[derive(Debug)]
pub(crate) struct MetadataFilteringInput {
    log_record: Chunk<LogRecord>,
    record_segment: Segment,
    metadata_segment: Segment,
    blockfile_provider: BlockfileProvider,
    where_clause: Option<Where>,
    where_document_clause: Option<WhereDocument>,
    query_ids: Option<Vec<String>>,
}

impl MetadataFilteringInput {
    pub(crate) fn new(
        log_record: Chunk<LogRecord>,
        record_segment: Segment,
        metadata_segment: Segment,
        blockfile_provider: BlockfileProvider,
        where_clause: Option<Where>,
        where_document_clause: Option<WhereDocument>,
        query_ids: Option<Vec<String>>,
    ) -> Self {
        Self {
            log_record,
            record_segment,
            metadata_segment,
            blockfile_provider,
            where_clause,
            where_document_clause,
            query_ids,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MetadataFilteringOutput {
    pub(crate) log_records: Chunk<LogRecord>,
    // Offset Ids of documents that match the where and where_document clauses.
    pub(crate) where_condition_filtered_offset_ids: Option<Vec<u32>>,
    // Offset ids of documents that the user specified in the query directly.
    pub(crate) user_supplied_filtered_offset_ids: Option<Vec<u32>>,
}

#[derive(Error, Debug)]
pub(crate) enum MetadataFilteringError {
    #[error("Error creating record segment reader {0}")]
    MetadataFilteringRecordSegmentReaderCreationError(#[from] RecordSegmentReaderCreationError),
    #[error("Error materializing logs {0}")]
    MetadataFilteringLogMaterializationError(#[from] LogMaterializerError),
    #[error("Error filtering documents by where or where_document clauses {0}")]
    MetadataFilteringIndexError(#[from] MetadataIndexError),
    #[error("Error from metadata segment reader {0}")]
    MetadataFilteringMetadataSegmentReaderError(#[from] MetadataSegmentError),
    #[error("Error reading from record segment")]
    MetadataFilteringRecordSegmentReaderError,
    #[error("Invalid input")]
    MetadataFilteringInvalidInput,
}

impl ChromaError for MetadataFilteringError {
    fn code(&self) -> ErrorCodes {
        match self {
            MetadataFilteringError::MetadataFilteringRecordSegmentReaderCreationError(e) => {
                e.code()
            }
            MetadataFilteringError::MetadataFilteringLogMaterializationError(e) => e.code(),
            MetadataFilteringError::MetadataFilteringIndexError(e) => e.code(),
            MetadataFilteringError::MetadataFilteringMetadataSegmentReaderError(e) => e.code(),
            MetadataFilteringError::MetadataFilteringRecordSegmentReaderError => {
                ErrorCodes::Internal
            }
            MetadataFilteringError::MetadataFilteringInvalidInput => ErrorCodes::InvalidArgument,
        }
    }
}

#[async_trait]
impl Operator<MetadataFilteringInput, MetadataFilteringOutput> for MetadataFilteringOperator {
    type Error = MetadataFilteringError;
    async fn run(
        &self,
        input: &MetadataFilteringInput,
    ) -> Result<MetadataFilteringOutput, MetadataFilteringError> {
        // Step 0: Create the record segment reader.
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
                        return Err(MetadataFilteringError::MetadataFilteringRecordSegmentReaderCreationError(
                            RecordSegmentReaderCreationError::BlockfileOpenError(e),
                        ));
                    }
                    RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                        tracing::error!("Error creating record segment reader {}", e);
                        return Err(MetadataFilteringError::MetadataFilteringRecordSegmentReaderCreationError(
                            RecordSegmentReaderCreationError::InvalidNumberOfFiles,
                        ));
                    }
                };
            }
        };
        // Step 1: Materialize the logs.
        let materializer =
            LogMaterializer::new(record_segment_reader, input.log_record.clone(), None);
        let mat_records = match materializer.materialize().await {
            Ok(records) => records,
            Err(e) => {
                return Err(MetadataFilteringError::MetadataFilteringLogMaterializationError(e));
            }
        };
        // Step 2: Apply where and where_document clauses on the materialized logs.
        let mut ids_to_metadata: HashMap<u32, HashMap<&str, &MetadataValue>> = HashMap::new();
        let mut ids_in_mat_log = HashSet::new();
        for (records, _) in mat_records.iter() {
            // It's important to account for even the deleted records here
            // so that they can be ignored when reading from the segment later.
            ids_in_mat_log.insert(records.offset_id);
            // Skip deleted records.
            if records.final_operation == Operation::Delete {
                continue;
            }
            ids_to_metadata.insert(records.offset_id, records.merged_metadata_ref());
        }
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
                            if let Some(val) = meta_map.get(metadata_key) {
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
                crate::types::MetadataType::BoolType => match comparator {
                    WhereClauseComparator::Equal => {
                        let mut result = RoaringBitmap::new();
                        // Construct a bitmap consisting of all offset ids
                        // that have this key equal to this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(metadata_key) {
                                match *val {
                                    MetadataValue::Bool(bool_value) => {
                                        if let KeyWrapper::Bool(where_value) = metadata_value {
                                            if *bool_value == *where_value {
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
                    // We don't allow these comparators for booleans.
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
                            if let Some(val) = meta_map.get(metadata_key) {
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
                        // that have this key less than this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(metadata_key) {
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
                        // that have this key <= this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(metadata_key) {
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
                        // that have this key > this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(metadata_key) {
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
                        // that have this key >= this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(metadata_key) {
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
                            if let Some(val) = meta_map.get(metadata_key) {
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
                        // that have this key < this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(metadata_key) {
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
                        // that have this key <= this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(metadata_key) {
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
                        // that have this key > this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(metadata_key) {
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
                        // that have this key >= this value.
                        for (offset_id, meta_map) in &ids_to_metadata {
                            if let Some(val) = meta_map.get(metadata_key) {
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
                crate::types::MetadataType::BoolListType => {
                    todo!();
                }
            }
        };
        // This will be sorted by offset ids since rbms.insert() insert in sorted order.
        let mtsearch_res = match &input.where_clause {
            Some(where_clause) => match process_where_clause_with_callback(where_clause, &clo) {
                Ok(r) => {
                    let ids_as_u32: Vec<u32> = r.into_iter().map(|index| index as u32).collect();
                    tracing::info!(
                        "Filtered {} results from log based on where clause filtering",
                        ids_as_u32.len()
                    );
                    Some(ids_as_u32)
                }
                Err(e) => {
                    tracing::error!("Error filtering logs based on where clause {:?}", e);
                    return Err(MetadataFilteringError::MetadataFilteringIndexError(e));
                }
            },
            None => {
                tracing::info!("Where clause not supplied by the user");
                None
            }
        };
        // AND this with where_document clause.
        let cb = |query: &str, op: WhereDocumentOperator| {
            match op {
                WhereDocumentOperator::Contains => {
                    // Note matching_contains is sorted (which is needed for correctness)
                    // because materialized log record is sorted by offset id.
                    let mut matching_contains = vec![];
                    // Upstream sorts materialized records by offset id so matching_contains
                    // will be sorted.
                    // Note: Uncomment this out when adding FTS support for queries
                    // containing _ or %. Currently, we disable such scenarios in tests
                    // for distributed version.
                    // Emulate sqlite behavior. _ and % match to any character in sqlite.
                    // let normalized_query = query.replace("_", ".").replace("%", ".");
                    // let re = Regex::new(normalized_query.as_str()).unwrap();
                    for (record, _) in mat_records.iter() {
                        if record.final_operation == Operation::Delete {
                            continue;
                        }
                        match record.merged_document_ref() {
                            Some(doc) => {
                                /* if re.is_match(doc) { */
                                if doc.contains(query) {
                                    matching_contains.push(record.offset_id as i32);
                                }
                            }
                            None => {}
                        };
                    }
                    return matching_contains;
                }
                WhereDocumentOperator::NotContains => {
                    todo!()
                }
            }
        };
        // fts_result will be sorted by offset id.
        let fts_result = match &input.where_document_clause {
            Some(where_doc_clause) => {
                match process_where_document_clause_with_callback(where_doc_clause, &cb) {
                    Ok(res) => {
                        let ids_as_u32: Vec<u32> =
                            res.into_iter().map(|index| index as u32).collect();
                        tracing::info!(
                            "Filtered {} results from log based on where document filtering",
                            ids_as_u32.len()
                        );
                        Some(ids_as_u32)
                    }
                    Err(e) => {
                        tracing::error!("Error filtering logs based on where document {:?}", e);
                        return Err(MetadataFilteringError::MetadataFilteringIndexError(e));
                    }
                }
            }
            None => {
                tracing::info!("Where document not supplied by the user");
                None
            }
        };
        let mut merged_result: Option<Vec<u32>> = None;
        if mtsearch_res.is_none() && fts_result.is_some() {
            merged_result = fts_result;
        } else if mtsearch_res.is_some() && fts_result.is_none() {
            merged_result = mtsearch_res;
        } else if mtsearch_res.is_some() && fts_result.is_some() {
            merged_result = Some(merge_sorted_vecs_conjunction(
                &mtsearch_res.expect("Already validated that it is not none"),
                &fts_result.expect("Already validated that it is not none"),
            ));
        }
        // Get offset ids that satisfy where conditions from storage.
        let metadata_segment_reader =
            MetadataSegmentReader::from_segment(&input.metadata_segment, &input.blockfile_provider)
                .await;

        let filtered_index_offset_ids = match metadata_segment_reader {
            Ok(reader) => {
                reader
                    .query(
                        input.where_clause.as_ref(),
                        input.where_document_clause.as_ref(),
                        Some(&vec![]),
                        0,
                        0,
                    )
                    .await
            }
            Err(e) => {
                tracing::error!("Error querying metadata segment: {:?}", e);
                return Err(MetadataFilteringError::MetadataFilteringMetadataSegmentReaderError(e));
            }
        };
        // This will be sorted by offset id.
        let filter_from_mt_segment = match filtered_index_offset_ids {
            Ok(res) => {
                match res {
                    Some(r) => {
                        // convert to u32 and also filter out the ones present in the
                        // materialized log. This is strictly needed for correctness as
                        // the ids that satisfy the predicate in the metadata segment
                        // could have been updated more recently (in the log) to NOT
                        // satisfy the predicate, hence we treat the materialized log
                        // as the source of truth for ids that are present in both the
                        // places.
                        let ids_as_u32: Vec<u32> = r
                            .into_iter()
                            .map(|index| index as u32)
                            .filter(|x| !ids_in_mat_log.contains(x))
                            .collect();
                        Some(ids_as_u32)
                    }
                    None => None,
                }
            }
            Err(e) => {
                return Err(MetadataFilteringError::MetadataFilteringMetadataSegmentReaderError(e));
            }
        };
        // It cannot happen that one is none and other is some.
        if (filter_from_mt_segment.is_some() && merged_result.is_none())
            || (filter_from_mt_segment.is_none() && merged_result.is_some())
        {
            panic!("Invariant violation. Both should either be none or some");
        }
        let mut where_condition_filtered_offset_ids = None;
        if filter_from_mt_segment.is_some() && merged_result.is_some() {
            where_condition_filtered_offset_ids = Some(merge_sorted_vecs_disjunction(
                &filter_from_mt_segment.expect("Already checked that should be some"),
                &merged_result.expect("Already checked that should be some"),
            ));
        }

        // Hydrate offset ids for user supplied ids.
        // First from the log.
        let mut user_supplied_offset_ids: Vec<u32> = vec![];
        let mut remaining_id_set: HashSet<String> = HashSet::new();
        let mut query_ids_present = false;
        match &input.query_ids {
            Some(query_ids) => {
                let query_ids_set: HashSet<String> = HashSet::from_iter(query_ids.iter().cloned());
                query_ids_present = true;
                remaining_id_set = query_ids.iter().cloned().collect();
                for (log_records, _) in mat_records.iter() {
                    let user_id = log_records.merged_user_id_ref();
                    if query_ids_set.contains(user_id) {
                        remaining_id_set.remove(user_id);
                        if log_records.final_operation != Operation::Delete {
                            user_supplied_offset_ids.push(log_records.offset_id);
                        }
                    }
                }
                tracing::info!(
                    "For user supplied query ids, filtered {} records from log, {} ids remain",
                    user_supplied_offset_ids.len(),
                    remaining_id_set.len()
                );
                let record_segment_reader_2: Option<RecordSegmentReader>;
                match RecordSegmentReader::from_segment(
                    &input.record_segment,
                    &input.blockfile_provider,
                )
                .await
                {
                    Ok(reader) => {
                        record_segment_reader_2 = Some(reader);
                    }
                    Err(e) => {
                        match *e {
                            // Uninitialized segment is fine and means that the record
                            // segment is not yet initialized in storage.
                            RecordSegmentReaderCreationError::UninitializedSegment => {
                                record_segment_reader_2 = None;
                            }
                            RecordSegmentReaderCreationError::BlockfileOpenError(e) => {
                                tracing::error!("Error creating record segment reader {}", e);
                                return Err(MetadataFilteringError::MetadataFilteringRecordSegmentReaderCreationError(
                            RecordSegmentReaderCreationError::BlockfileOpenError(e),
                        ));
                            }
                            RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                                tracing::error!("Error creating record segment reader {}", e);
                                return Err(MetadataFilteringError::MetadataFilteringRecordSegmentReaderCreationError(
                            RecordSegmentReaderCreationError::InvalidNumberOfFiles,
                        ));
                            }
                        };
                    }
                };
                match &record_segment_reader_2 {
                    Some(r) => {
                        // Now read the remaining ids from storage.
                        for ids in remaining_id_set {
                            match r.get_offset_id_for_user_id(ids.as_str()).await {
                                Ok(offset_id) => {
                                    user_supplied_offset_ids.push(offset_id);
                                }
                                // It's ok for the user to supply a non existent id.
                                Err(_) => (),
                            }
                        }
                    }
                    // It's ok for the user to supply a non existent id.
                    None => (),
                }
            }
            None => {
                query_ids_present = false;
            }
        }
        // need to sort user_supplied_offset_ids by offset id.
        user_supplied_offset_ids.sort();
        let mut filtered_offset_ids = None;
        if query_ids_present {
            tracing::info!(
                "Filtered {} records (log + segment) based on user supplied ids",
                user_supplied_offset_ids.len()
            );
            filtered_offset_ids = Some(user_supplied_offset_ids);
        }
        return Ok(MetadataFilteringOutput {
            log_records: input.log_record.clone(),
            where_condition_filtered_offset_ids: where_condition_filtered_offset_ids,
            user_supplied_filtered_offset_ids: filtered_offset_ids,
        });
    }
}

#[cfg(test)]
mod test {
    use crate::{
        blockstore::{
            arrow::{config::TEST_MAX_BLOCK_SIZE_BYTES, provider::ArrowBlockfileProvider},
            provider::BlockfileProvider,
        },
        execution::{
            data::data_chunk::Chunk,
            operator::Operator,
            operators::metadata_filtering::{MetadataFilteringInput, MetadataFilteringOperator},
        },
        segment::{
            metadata_segment::MetadataSegmentWriter,
            record_segment::{
                RecordSegmentReader, RecordSegmentReaderCreationError, RecordSegmentWriter,
            },
            types::SegmentFlusher,
            LogMaterializer, SegmentWriter,
        },
        storage::{local::LocalStorage, Storage},
        types::{
            DirectComparison, DirectDocumentComparison, LogRecord, Operation, OperationRecord,
            UpdateMetadataValue, Where, WhereComparison, WhereDocument,
        },
    };
    use std::{collections::HashMap, str::FromStr};
    use uuid::Uuid;

    #[tokio::test]
    async fn where_and_where_document_from_log() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let arrow_blockfile_provider =
            ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut record_segment = crate::types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: crate::types::SegmentType::BlockfileRecord,
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
                    document: Some(String::from("This is a document about dogs.")),
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
        let operator = MetadataFilteringOperator::new();
        let where_clause: Where = Where::DirectWhereComparison(DirectComparison {
            key: String::from("hello"),
            comparison: WhereComparison::SingleStringComparison(
                String::from("new_world"),
                crate::types::WhereClauseComparator::Equal,
            ),
        });
        let where_document_clause =
            WhereDocument::DirectWhereDocumentComparison(DirectDocumentComparison {
                document: String::from("about dogs"),
                operator: crate::types::WhereDocumentOperator::Contains,
            });
        let input = MetadataFilteringInput::new(
            data.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            blockfile_provider.clone(),
            Some(where_clause),
            Some(where_document_clause),
            None,
        );
        let mut res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(None, res.user_supplied_filtered_offset_ids);

        assert_eq!(
            1,
            res.where_condition_filtered_offset_ids
                .clone()
                .expect("Expected one document")
                .len()
        );
        assert_eq!(
            3,
            *res.where_condition_filtered_offset_ids
                .expect("Expected one document")
                .get(0)
                .expect("Expect not none")
        );
    }

    #[tokio::test]
    async fn where_from_metadata_segment() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let arrow_blockfile_provider =
            ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut record_segment = crate::types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: crate::types::SegmentType::BlockfileRecord,
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
            let materializer = LogMaterializer::new(record_segment_reader, data, None);
            let mat_records = materializer
                .materialize()
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(mat_records.clone())
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .write_to_blockfiles()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
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
        let data = vec![LogRecord {
            log_offset: 3,
            record: OperationRecord {
                id: "embedding_id_3".to_string(),
                embedding: Some(vec![7.0, 8.0, 9.0]),
                encoding: None,
                metadata: Some(update_metadata),
                document: Some(String::from("This is a document about dogs.")),
                operation: Operation::Add,
            },
        }];
        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let operator = MetadataFilteringOperator::new();
        let where_clause: Where = Where::DirectWhereComparison(DirectComparison {
            key: String::from("bye"),
            comparison: WhereComparison::SingleStringComparison(
                String::from("world"),
                crate::types::WhereClauseComparator::Equal,
            ),
        });
        let input = MetadataFilteringInput::new(
            data.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            blockfile_provider.clone(),
            Some(where_clause),
            None,
            None,
        );
        let mut res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(None, res.user_supplied_filtered_offset_ids);

        assert_eq!(
            2,
            res.where_condition_filtered_offset_ids
                .clone()
                .expect("Expected one document")
                .len()
        );
        let mut where_res = res
            .where_condition_filtered_offset_ids
            .expect("Expect not none")
            .clone();
        // Already sorted.
        assert_eq!(1, *where_res.get(0).expect("Expected not none value"));
        assert_eq!(2, *where_res.get(1).expect("Expected not none value"));
    }

    #[tokio::test]
    async fn query_ids_only() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let arrow_blockfile_provider =
            ArrowBlockfileProvider::new(storage, TEST_MAX_BLOCK_SIZE_BYTES);
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut record_segment = crate::types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: crate::types::SegmentType::BlockfileRecord,
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
            let metadata_writer =
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
                    document: Some(String::from("This is a document about dogs.")),
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
        let operator = MetadataFilteringOperator::new();
        let input = MetadataFilteringInput::new(
            data.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            blockfile_provider.clone(),
            None,
            None,
            Some(vec![
                String::from("embedding_id_1"),
                String::from("embedding_id_3"),
            ]),
        );
        let mut res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(None, res.where_condition_filtered_offset_ids);
        let mut query_offset_id_vec = res
            .user_supplied_filtered_offset_ids
            .expect("Expected not none")
            .clone();
        // Already sorted.
        assert_eq!(2, query_offset_id_vec.len());
        assert_eq!(
            1,
            *query_offset_id_vec.get(0).expect("Expect not none value")
        );
        assert_eq!(
            3,
            *query_offset_id_vec.get(1).expect("Expect not none value")
        );
    }
}
