use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{
    logical_size_of_metadata, Chunk, DataRecord, DeletedMetadata, LogRecord,
    MaterializedLogOperation, Metadata, MetadataDelta, MetadataValue, MetadataValueConversionError,
    Operation, Schema, SegmentUuid, UpdateMetadata, UpdateMetadataValue,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;
use tracing::{Instrument, Span};
use uuid::Uuid;

use crate::distributed_spann::{SpannSegmentFlusher, SpannSegmentWriter};

use super::blockfile_metadata::{MetadataSegmentFlusher, MetadataSegmentWriter};
use super::blockfile_record::{
    ApplyMaterializedLogError, RecordSegmentFlusher, RecordSegmentReader,
    RecordSegmentReaderCreationError, RecordSegmentWriter,
};
use super::distributed_hnsw::DistributedHNSWSegmentWriter;

// Materializes metadata from update metadata, populating the delete list
// and upsert list.
fn materialize_update_metadata(
    update_metdata: &UpdateMetadata,
) -> Result<(Metadata, DeletedMetadata), MetadataValueConversionError> {
    let mut metadata = Metadata::new();
    let mut deleted_metadata = DeletedMetadata::new();
    for (key, value) in update_metdata {
        if *value == UpdateMetadataValue::None {
            deleted_metadata.insert(key.clone());
            continue;
        }
        // Should be a valid conversion for not None values.
        let res = value.try_into();
        match res {
            Ok(value) => {
                metadata.insert(key.clone(), value);
            }
            Err(err) => {
                return Err(err);
            }
        }
    }
    Ok((metadata, deleted_metadata))
}

// Merges update metadata to base metadata, updating
// the delete list and upsert list.
fn merge_update_metadata(
    base_metadata: (&Option<Metadata>, &Option<DeletedMetadata>),
    update_metadata: &Option<UpdateMetadata>,
) -> Result<(Option<Metadata>, Option<DeletedMetadata>), MetadataValueConversionError> {
    let mut merged_metadata = HashMap::new();
    let mut deleted_metadata = DeletedMetadata::new();
    if let Some(base_mt) = base_metadata.0 {
        merged_metadata = base_mt.clone();
    }
    if let Some(deleted_mt) = base_metadata.1 {
        deleted_metadata = deleted_mt.clone();
    }
    if let Some(update_metadata) = update_metadata {
        match materialize_update_metadata(update_metadata) {
            Ok((metadata, deleted_mt)) => {
                // Overwrite with new kv.
                for (key, value) in metadata {
                    merged_metadata.insert(key.clone(), value);
                    // Also remove from deleted list. This is important
                    // because it can happen that the user deleted and then
                    // reinserted the key.
                    deleted_metadata.remove(&key);
                }
                // apply the deletes.
                for key in deleted_mt {
                    deleted_metadata.insert(key.clone());
                    // Again important to remove from this map since the user
                    // could have previously update the key (and is now deleting it).
                    merged_metadata.remove(&key);
                }
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    let final_mt = if merged_metadata.is_empty() {
        None
    } else {
        Some(merged_metadata)
    };
    let final_deleted = if deleted_metadata.is_empty() {
        None
    } else {
        Some(deleted_metadata)
    };
    Ok((final_mt, final_deleted))
}

#[derive(Error, Debug)]
pub enum LogMaterializerError {
    #[error("Error materializing document metadata {0}")]
    MetadataMaterialization(#[from] MetadataValueConversionError),
    #[error("Error materializing document embedding")]
    EmbeddingMaterialization,
    #[error("Error reading record segment {0}")]
    RecordSegment(#[from] Box<dyn ChromaError>),
}

impl ChromaError for LogMaterializerError {
    fn code(&self) -> ErrorCodes {
        match self {
            LogMaterializerError::MetadataMaterialization(e) => e.code(),
            LogMaterializerError::EmbeddingMaterialization => ErrorCodes::Internal,
            LogMaterializerError::RecordSegment(e) => e.code(),
        }
    }
}

/// This struct is used internally. It is not exposed to materialized log consumers.
///
/// Instead of cloning or holding references to log records/segment data, this struct contains owned values that can be resolved to the referenced data.
/// E.x. `final_document_at_log_index: Option<usize>` is used instead of `final_document: Option<&str>` to avoid holding references to the data.
/// This allows `MaterializedLogRecord` (and types above it) to be trivially Send'able.
#[derive(Debug)]
struct MaterializedLogRecord {
    // False if the record exists only in the log, otherwise true.
    offset_id_exists_in_segment: bool,
    // If present in the record segment then it is the offset id
    // in the record segment at which the record was found.
    // If not present in the segment then it is the offset id
    // at which it should be inserted.
    offset_id: u32,
    // Set only for the records that are being inserted for the first time
    // in the log since data_record will be None in such cases. For other
    // cases, just read from data record.
    user_id_at_log_index: Option<usize>,
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
    final_operation: MaterializedLogOperation,
    // This is the metadata obtained by combining all the operations
    // present in the log for this id.
    // E.g. if has log has [Insert(a: h), Update(a: b, c: d), Update(a: e, f: g)] then this
    // will contain (a: e, c: d, f: g).
    metadata_to_be_merged: Option<Metadata>,
    // Keys from the metadata that the user wants to delete. This is guaranteed
    // to be disjoint from metadata_to_be_merged i.e. there won't be keys
    // present in both the places.
    metadata_to_be_deleted: Option<HashSet<String>>,
    // This is the log index containing the final document obtained from the last non null operation.
    // E.g. if log has [Insert(str0), Update(str1), Update(str2), Update()] then this will contain
    // str2. None if final operation is Delete.
    final_document_at_log_index: Option<usize>,

    // Similar to above, this is the log index containing the final embedding obtained
    // from the last non null operation.
    // E.g. if log has [Insert(emb0), Update(emb1), Update(emb2), Update()]
    // then this will contain emb2. None if final operation is Delete.
    final_embedding_at_log_index: Option<usize>,
}

impl MaterializedLogRecord {
    fn from_segment_offset_id(offset_id: u32) -> Self {
        Self {
            offset_id_exists_in_segment: true,
            offset_id,
            user_id_at_log_index: None,
            final_operation: MaterializedLogOperation::Initial,
            metadata_to_be_merged: None,
            metadata_to_be_deleted: None,
            final_document_at_log_index: None,
            final_embedding_at_log_index: None,
        }
    }

    fn from_log_record(
        offset_id: u32,
        log_index: usize,
        log_record: &LogRecord,
    ) -> Result<Self, LogMaterializerError> {
        let final_document_at_log_index = if log_record.record.document.is_some() {
            Some(log_index)
        } else {
            None
        };

        let final_embedding_at_log_index = if log_record.record.embedding.is_some() {
            Some(log_index)
        } else {
            return Err(LogMaterializerError::EmbeddingMaterialization);
        };

        let merged_metadata;
        let deleted_metadata;
        match &log_record.record.metadata {
            Some(metadata) => match materialize_update_metadata(metadata) {
                Ok(m) => {
                    merged_metadata = Some(m.0);
                    deleted_metadata = Some(m.1);
                }
                Err(e) => {
                    return Err(LogMaterializerError::MetadataMaterialization(e));
                }
            },
            None => {
                merged_metadata = None;
                deleted_metadata = None;
            }
        };

        Ok(Self {
            offset_id_exists_in_segment: false,
            offset_id,
            user_id_at_log_index: Some(log_index),
            final_operation: MaterializedLogOperation::AddNew,
            metadata_to_be_merged: merged_metadata,
            metadata_to_be_deleted: deleted_metadata,
            final_document_at_log_index,
            final_embedding_at_log_index,
        })
    }
}

/// Obtained from a `MaterializeLogsResult`. Provides a borrowed view of a single materialized log record.
/// You will probably need to call `.hydrate()` on this struct for most use cases, although you can view the offset ID and operation without hydrating.
/// BorrowedMaterializedLogRecord and HydratedMaterializedLogRecord are separate types as some use cases may not need to fully hydrate the record.
pub struct BorrowedMaterializedLogRecord<'log_data> {
    materialized_log_record: &'log_data MaterializedLogRecord,
    logs: &'log_data Chunk<LogRecord>,
}

impl<'log_data> BorrowedMaterializedLogRecord<'log_data> {
    pub fn get_offset_id(&self) -> u32 {
        self.materialized_log_record.offset_id
    }

    pub fn get_operation(&self) -> MaterializedLogOperation {
        self.materialized_log_record.final_operation
    }

    /// Reads any record segment data that this log record may reference and returns a hydrated version of this record.
    /// The record segment reader passed here **must be over the same set of blockfiles** as the reader that was originally passed to `materialize_logs()`. If the two readers are different, the behavior is undefined.
    pub async fn hydrate<'segment_data>(
        &self,
        record_segment_reader: Option<&'segment_data RecordSegmentReader<'segment_data>>,
    ) -> Result<HydratedMaterializedLogRecord<'log_data, 'segment_data>, LogMaterializerError> {
        let segment_data_record = match self.materialized_log_record.offset_id_exists_in_segment {
            true => match record_segment_reader {
                Some(reader) => {
                    reader
                        .get_data_for_offset_id(self.materialized_log_record.offset_id)
                        .await?
                }
                None => None,
            },
            false => None,
        };

        Ok(HydratedMaterializedLogRecord {
            materialized_log_record: self.materialized_log_record,
            segment_data_record,
            logs: self.logs,
        })
    }
}

/// Obtained from `BorrowedMaterializedLogRecord::hydrate()`. Provides a fully-hydrated view of a single materialized log record.
pub struct HydratedMaterializedLogRecord<'log_data, 'segment_data> {
    materialized_log_record: &'log_data MaterializedLogRecord,
    segment_data_record: Option<DataRecord<'segment_data>>,
    logs: &'log_data Chunk<LogRecord>,
}

impl<'log_data, 'segment_data: 'log_data> HydratedMaterializedLogRecord<'log_data, 'segment_data> {
    pub fn get_offset_id(&self) -> u32 {
        self.materialized_log_record.offset_id
    }

    pub fn get_operation(&self) -> MaterializedLogOperation {
        self.materialized_log_record.final_operation
    }

    pub fn get_user_id(&self) -> &'log_data str {
        if let Some(id) = self.materialized_log_record.user_id_at_log_index {
            return self.logs.get(id).unwrap().record.id.as_str();
        }

        if let Some(data_record) = self.segment_data_record.as_ref() {
            data_record.id
        } else {
            panic!("Expected at least one source of user id")
        }
    }

    pub fn document_ref_from_log(&self) -> Option<&'log_data str> {
        match self.materialized_log_record.final_document_at_log_index {
            Some(offset) => Some(self.logs.get(offset).unwrap().record.document.as_ref()?),
            None => None,
        }
    }

    pub fn document_ref_from_segment(&self) -> Option<&'segment_data str> {
        self.segment_data_record
            .as_ref()
            .map(|data_record| data_record.document)?
    }

    pub fn merged_document_ref(&self) -> Option<&'log_data str> {
        if self
            .materialized_log_record
            .final_document_at_log_index
            .is_some()
        {
            return self.document_ref_from_log();
        }

        if self.materialized_log_record.final_operation
            == MaterializedLogOperation::OverwriteExisting
            || self.materialized_log_record.final_operation == MaterializedLogOperation::AddNew
        {
            None
        } else {
            self.document_ref_from_segment()
        }
    }

    /// Performs a deep copy of the metadata so only use this if really needed.
    pub fn merged_metadata(&self) -> HashMap<String, MetadataValue> {
        let mut final_metadata;
        if self.materialized_log_record.final_operation
            == MaterializedLogOperation::OverwriteExisting
            || self.materialized_log_record.final_operation == MaterializedLogOperation::AddNew
        {
            final_metadata = HashMap::new();
        } else {
            final_metadata = match self.segment_data_record.as_ref() {
                Some(data_record) => match &data_record.metadata {
                    Some(ref map) => map.clone(),
                    None => HashMap::new(),
                },
                None => HashMap::new(),
            };
        }
        if let Some(metadata) = self.materialized_log_record.metadata_to_be_merged.as_ref() {
            for (key, value) in metadata {
                final_metadata.insert(key.clone(), value.clone());
            }
        }
        if let Some(metadata) = self.materialized_log_record.metadata_to_be_deleted.as_ref() {
            for key in metadata {
                final_metadata.remove(key);
            }
        }
        final_metadata
    }

    pub fn embeddings_ref_from_log(&self) -> Option<&'log_data [f32]> {
        match self.materialized_log_record.final_embedding_at_log_index {
            Some(index) => Some(self.logs.get(index).unwrap().record.embedding.as_ref()?),
            None => None,
        }
    }

    pub fn embeddings_ref_from_segment(&self) -> Option<&'segment_data [f32]> {
        self.segment_data_record
            .as_ref()
            .map(|data_record| data_record.embedding)
    }

    pub fn merged_embeddings_ref(&self) -> &'log_data [f32] {
        if self
            .materialized_log_record
            .final_embedding_at_log_index
            .is_some()
        {
            return self.embeddings_ref_from_log().unwrap();
        }

        if self.materialized_log_record.final_operation
            == MaterializedLogOperation::OverwriteExisting
            || self.materialized_log_record.final_operation == MaterializedLogOperation::AddNew
        {
            panic!("Expected at least once source of embedding")
        } else {
            self.embeddings_ref_from_segment().unwrap()
        }
    }

    pub fn get_data_record(&self) -> Option<&DataRecord> {
        self.segment_data_record.as_ref()
    }

    pub fn get_metadata_to_be_merged(&self) -> Option<&Metadata> {
        self.materialized_log_record.metadata_to_be_merged.as_ref()
    }

    pub fn compute_metadata_delta(&self) -> MetadataDelta<'_> {
        let mut metadata_delta = MetadataDelta::new();
        let mut base_metadata: HashMap<&str, &MetadataValue> = HashMap::new();
        if let Some(data_record) = &self.segment_data_record {
            if let Some(meta) = &data_record.metadata {
                for (meta_key, meta_val) in meta {
                    base_metadata.insert(meta_key, meta_val);
                }
            }
        }
        // Populate updates.
        if let Some(meta) = &self.materialized_log_record.metadata_to_be_merged {
            for (meta_key, meta_val) in meta {
                match base_metadata.get(meta_key.as_str()) {
                    Some(old_value) => {
                        metadata_delta
                            .metadata_to_update
                            .insert(meta_key.as_str(), (old_value, meta_val));
                    }
                    None => {
                        metadata_delta
                            .metadata_to_insert
                            .insert(meta_key.as_str(), meta_val);
                    }
                }
            }
        };
        // Populate deletes.
        if let Some(meta) = &self.materialized_log_record.metadata_to_be_deleted {
            for key in meta {
                if let Some(old_value) = base_metadata.get(key.as_str()) {
                    metadata_delta
                        .metadata_to_delete
                        .insert(key.as_str(), old_value);
                }
            }
        }
        metadata_delta
    }

    pub fn compute_logical_size_delta_bytes(&self) -> i64 {
        let old_size = self
            .get_data_record()
            .map(|rec| {
                rec.id.len()
                    + size_of_val(rec.embedding)
                    + rec
                        .metadata
                        .as_ref()
                        .map(logical_size_of_metadata)
                        .unwrap_or_default()
                    + rec.document.map(|doc| doc.len()).unwrap_or_default()
            })
            .unwrap_or_default() as i64;
        let merged_metadata = self.merged_metadata();
        let new_size = match self.get_operation() {
            MaterializedLogOperation::AddNew
            | MaterializedLogOperation::OverwriteExisting
            | MaterializedLogOperation::UpdateExisting => {
                (self.get_user_id().len()
                    + size_of_val(self.merged_embeddings_ref())
                    + logical_size_of_metadata(&merged_metadata)
                    + self
                        .merged_document_ref()
                        .map(|doc| doc.len())
                        .unwrap_or_default()) as i64
            }
            _ => 0,
        };
        new_size - old_size
    }
}

#[derive(Debug, Clone)]
pub struct MaterializeLogsResult {
    logs: Chunk<LogRecord>,
    materialized: Chunk<MaterializedLogRecord>,
}

impl MaterializeLogsResult {
    pub fn is_empty(&self) -> bool {
        self.materialized.is_empty()
    }

    pub fn len(&self) -> usize {
        self.materialized.len()
    }

    pub fn iter(&self) -> MaterializeLogsResultIter {
        MaterializeLogsResultIter {
            logs: &self.logs,
            chunk: &self.materialized,
            index: 0,
        }
    }
}

// IntoIterator is implemented for &'a MaterializeLogsResult rather than MaterializeLogsResult because the iterator needs to hand out values with a lifetime of 'a.
impl<'log_data> IntoIterator for &'log_data MaterializeLogsResult {
    type Item = BorrowedMaterializedLogRecord<'log_data>;
    type IntoIter = MaterializeLogsResultIter<'log_data>;

    fn into_iter(self) -> Self::IntoIter {
        MaterializeLogsResultIter {
            logs: &self.logs,
            chunk: &self.materialized,
            index: 0,
        }
    }
}

pub struct MaterializeLogsResultIter<'log_data> {
    logs: &'log_data Chunk<LogRecord>,
    chunk: &'log_data Chunk<MaterializedLogRecord>,
    index: usize,
}

impl<'log_data> Iterator for MaterializeLogsResultIter<'log_data> {
    type Item = BorrowedMaterializedLogRecord<'log_data>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.chunk.len() {
            let item = BorrowedMaterializedLogRecord {
                materialized_log_record: self.chunk.get(self.index).unwrap(),
                logs: self.logs,
            };
            self.index += 1;
            Some(item)
        } else {
            None
        }
    }
}

static TOTAL_LOGS_PRE_MATERIALIZED: std::sync::LazyLock<opentelemetry::metrics::Counter<u64>> =
    std::sync::LazyLock::new(|| {
        let meter = opentelemetry::global::meter("chroma");
        meter
            .u64_counter("total_logs_pre_materialization")
            .with_description("The total number of log records provided to materialize_logs()")
            .build()
    });

static TOTAL_LOGS_POST_MATERIALIZED: std::sync::LazyLock<opentelemetry::metrics::Counter<u64>> =
    std::sync::LazyLock::new(|| {
        let meter = opentelemetry::global::meter("chroma");
        meter
            .u64_counter("total_logs_post_materialized")
            .with_description("The total number of log records materialized by materialize_logs()")
            .build()
    });

/// Materializes a chunk of log records.
/// - `record_segment_reader` can be `None` if the record segment is uninitialized.
/// - `next_offset_id` must be provided if the log was partitioned and `materialize_logs()` is called for each partition: if it is not provided, generated offset IDs will conflict between partitions. When it is not provided, it is initialized from the max offset ID in the record segment.
pub async fn materialize_logs(
    record_segment_reader: &Option<RecordSegmentReader<'_>>,
    logs: Chunk<LogRecord>,
    next_offset_id: Option<Arc<AtomicU32>>,
) -> Result<MaterializeLogsResult, LogMaterializerError> {
    // Trace the total_len since len() iterates over the entire chunk
    // and we don't want to do that just to trace the length.
    TOTAL_LOGS_PRE_MATERIALIZED.add(logs.len() as u64, &[]);

    // The offset ID that should be used for the next record
    let next_offset_id = match next_offset_id.as_ref() {
        Some(next_offset_id) => next_offset_id.clone(),
        None => {
            match record_segment_reader.as_ref() {
                Some(reader) => {
                    let offset_id = Arc::new(AtomicU32::new(reader.get_max_offset_id()));
                    offset_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    offset_id
                }
                // This means that the segment is uninitialized so counting starts from 1.
                None => Arc::new(AtomicU32::new(1)),
            }
        }
    };

    // Populate entries that are present in the record segment.
    let mut existing_id_to_materialized: HashMap<&str, MaterializedLogRecord> = HashMap::new();
    let mut new_id_to_materialized: HashMap<&str, MaterializedLogRecord> = HashMap::new();
    if let Some(reader) = &record_segment_reader {
        async {
            for (log_record, _) in logs.iter() {
                let exists = match reader
                    .data_exists_for_user_id(log_record.record.id.as_str())
                    .await
                {
                    Ok(res) => res,
                    Err(e) => {
                        return Err(LogMaterializerError::RecordSegment(e));
                    }
                };
                if exists {
                    match reader
                        .get_offset_id_for_user_id(log_record.record.id.as_str())
                        .await
                    {
                        Ok(Some(offset_id)) => {
                            existing_id_to_materialized.insert(
                                log_record.record.id.as_str(),
                                MaterializedLogRecord::from_segment_offset_id(offset_id),
                            );
                        }
                        Ok(None) => {
                            return Err(LogMaterializerError::RecordSegment(Box::new(
                                RecordSegmentReaderCreationError::UserRecordNotFound(format!(
                                    "not found: {}",
                                    log_record.record.id,
                                )),
                            )
                                as _));
                        }
                        Err(e) => {
                            return Err(LogMaterializerError::RecordSegment(e));
                        }
                    }
                }
            }
            Ok(())
        }
        .instrument(Span::current())
        .await?;
    }
    // Populate updates to these and fresh records that are being
    // inserted for the first time.
    async {
        for (log_record, log_index) in logs.iter() {
            match log_record.record.operation {
                Operation::Add => {
                    // If this is an add of a record present in the segment then add
                    // only if it has been previously deleted in the log.
                    if existing_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                        // safe to unwrap
                        let operation = existing_id_to_materialized
                            .get(log_record.record.id.as_str())
                            .unwrap()
                            .final_operation;
                        match operation {
                            MaterializedLogOperation::DeleteExisting => {
                                let curr_val = existing_id_to_materialized
                                    .remove(log_record.record.id.as_str())
                                    .unwrap();
                                // Overwrite.
                                let mut materialized_record =
                                    match MaterializedLogRecord::from_log_record(
                                        curr_val.offset_id,
                                        log_index,
                                        log_record,
                                    ) {
                                        Ok(record) => record,
                                        Err(e) => {
                                            return Err(e);
                                        }
                                    };
                                materialized_record.offset_id_exists_in_segment = true;
                                materialized_record.final_operation =
                                    MaterializedLogOperation::OverwriteExisting;
                                existing_id_to_materialized
                                    .insert(log_record.record.id.as_str(), materialized_record);
                            }
                            MaterializedLogOperation::AddNew => panic!(
                                "Invariant violation. Existing record can never have an Add new state"
                            ),
                            MaterializedLogOperation::Initial
                            | MaterializedLogOperation::OverwriteExisting
                            | MaterializedLogOperation::UpdateExisting => {
                                // Invalid add so skip.
                                continue;
                            }
                        }
                    }
                    // Adding an entry that does not exist on the segment yet.
                    // Only add if it hasn't been added before in the log.
                    else if !new_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                        let next_offset_id =
                            next_offset_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let materialized_record = match MaterializedLogRecord::from_log_record(
                            next_offset_id,
                            log_index,
                            log_record,
                        ) {
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
                    // in segment then we'll have to pass it as a delete
                    // to the compactor so that it can be deleted.
                    if new_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                        new_id_to_materialized.remove(log_record.record.id.as_str());
                    } else if existing_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                        // Mark state as deleted. Other fields become noop after such a delete.
                        let record_from_map = existing_id_to_materialized
                            .get_mut(log_record.record.id.as_str())
                            .unwrap();
                        record_from_map.final_operation = MaterializedLogOperation::DeleteExisting;
                        record_from_map.final_document_at_log_index = None;
                        record_from_map.final_embedding_at_log_index = None;
                        record_from_map.metadata_to_be_merged = None;
                        record_from_map.metadata_to_be_deleted = None;
                        record_from_map.user_id_at_log_index = None;
                    }
                }
                Operation::Update => {
                    let record_from_map = match existing_id_to_materialized
                        .get_mut(log_record.record.id.as_str())
                    {
                        Some(res) => {
                            match res.final_operation {
                                        // Ignore the update if deleted.
                                        MaterializedLogOperation::DeleteExisting => {
                                            continue;
                                        },
                                        MaterializedLogOperation::AddNew => panic!("Invariant violation. AddNew state not expected for an entry that exists on the segment"),
                                        MaterializedLogOperation::Initial | MaterializedLogOperation::OverwriteExisting | MaterializedLogOperation::UpdateExisting => {}
                                    }
                            res
                        }
                        None => match new_id_to_materialized.get_mut(log_record.record.id.as_str()) {
                            Some(res) => res,
                            None => {
                                // Does not exist in either maps. Ignore this update.
                                continue;
                            }
                        },
                    };

                    match merge_update_metadata(
                        (
                            &record_from_map.metadata_to_be_merged,
                            &record_from_map.metadata_to_be_deleted,
                        ),
                        &log_record.record.metadata,
                    ) {
                        Ok(meta) => {
                            record_from_map.metadata_to_be_merged = meta.0;
                            record_from_map.metadata_to_be_deleted = meta.1;
                        }
                        Err(e) => {
                            return Err(LogMaterializerError::MetadataMaterialization(e));
                        }
                    };

                    if log_record.record.document.is_some() {
                        record_from_map.final_document_at_log_index = Some(log_index);
                    }

                    if log_record.record.embedding.is_some() {
                        record_from_map.final_embedding_at_log_index = Some(log_index);
                    }

                    match record_from_map.final_operation {
                        MaterializedLogOperation::Initial => {
                            record_from_map.final_operation = MaterializedLogOperation::UpdateExisting;
                        }
                        // State remains as is.
                        MaterializedLogOperation::AddNew
                        | MaterializedLogOperation::OverwriteExisting
                        | MaterializedLogOperation::UpdateExisting => {}
                        // Not expected.
                        MaterializedLogOperation::DeleteExisting => {
                            panic!("Invariant violation. Should not be updating a deleted record")
                        }
                    }
                }
                Operation::Upsert => {
                    if existing_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                        // safe to unwrap here.
                        let operation = existing_id_to_materialized
                            .get(log_record.record.id.as_str())
                            .unwrap()
                            .final_operation;
                        match operation {
                                    MaterializedLogOperation::DeleteExisting => {
                                        let curr_val = existing_id_to_materialized.remove(log_record.record.id.as_str()).unwrap();
                                        // Overwrite.
                                        let mut materialized_record =
                                            match MaterializedLogRecord::from_log_record(curr_val.offset_id, log_index, log_record) {
                                                Ok(record) => record,
                                                Err(e) => {
                                                    return Err(e);
                                                }
                                            };
                                        materialized_record.offset_id_exists_in_segment = true;
                                        materialized_record.final_operation =
                                            MaterializedLogOperation::OverwriteExisting;
                                        existing_id_to_materialized
                                            .insert(log_record.record.id.as_str(), materialized_record);
                                    },
                                    MaterializedLogOperation::AddNew => panic!("Invariant violation. AddNew state not expected for records that exist in the segment"),
                                    MaterializedLogOperation::Initial | MaterializedLogOperation::OverwriteExisting | MaterializedLogOperation::UpdateExisting => {
                                        // Update.
                                        let record_from_map = existing_id_to_materialized.get_mut(log_record.record.id.as_str()).unwrap();
                                        match merge_update_metadata((&record_from_map.metadata_to_be_merged, &record_from_map.metadata_to_be_deleted,),&log_record.record.metadata,) {
                                            Ok(meta) => {
                                                record_from_map.metadata_to_be_merged = meta.0;
                                                record_from_map.metadata_to_be_deleted = meta.1;
                                            }
                                            Err(e) => {
                                                return Err(LogMaterializerError::MetadataMaterialization(e));
                                            }
                                        };

                                        if log_record.record.document.is_some() {
                                            record_from_map.final_document_at_log_index = Some(log_index);
                                        }

                                        if log_record.record.embedding.is_some() {
                                            record_from_map.final_embedding_at_log_index = Some(log_index);
                                        }

                                        match record_from_map.final_operation {
                                            MaterializedLogOperation::Initial => {
                                                record_from_map.final_operation =
                                                    MaterializedLogOperation::UpdateExisting;
                                            }
                                            // State remains as is.
                                            MaterializedLogOperation::AddNew
                                            | MaterializedLogOperation::OverwriteExisting
                                            | MaterializedLogOperation::UpdateExisting => {}
                                            // Not expected.
                                            MaterializedLogOperation::DeleteExisting => {
                                                panic!("Invariant violation. Should not be updating a deleted record")
                                            }
                                        }
                                    }
                                }
                    } else if new_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                        // Update.
                        let record_from_map = new_id_to_materialized
                            .get_mut(log_record.record.id.as_str())
                            .unwrap();
                        match merge_update_metadata(
                            (
                                &record_from_map.metadata_to_be_merged,
                                &record_from_map.metadata_to_be_deleted,
                            ),
                            &log_record.record.metadata,
                        ) {
                            Ok(meta) => {
                                record_from_map.metadata_to_be_merged = meta.0;
                                record_from_map.metadata_to_be_deleted = meta.1;
                            }
                            Err(e) => {
                                return Err(LogMaterializerError::MetadataMaterialization(e));
                            }
                        };

                        if log_record.record.document.is_some() {
                            record_from_map.final_document_at_log_index = Some(log_index);
                        }

                        if log_record.record.embedding.is_some() {
                            record_from_map.final_embedding_at_log_index = Some(log_index);
                        }

                        // This record is not present on storage yet hence final operation is
                        // AddNew and not UpdateExisting.
                        record_from_map.final_operation = MaterializedLogOperation::AddNew;
                    } else {
                        // Insert.
                        let next_offset =
                            next_offset_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let materialized_record = match MaterializedLogRecord::from_log_record(
                            next_offset,
                            log_index,
                            log_record,
                        ) {
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
        Ok(())
    }.instrument(Span::current()).await?;
    let mut res = vec![];
    for (_key, value) in existing_id_to_materialized {
        // Ignore records that only had invalid ADDS on the log.
        if value.final_operation == MaterializedLogOperation::Initial {
            continue;
        }
        res.push(value);
    }
    for (_key, value) in new_id_to_materialized {
        res.push(value);
    }
    res.sort_by(|x, y| x.offset_id.cmp(&y.offset_id));

    tracing::info!(
        "Log count before materialization: {}, after materialization: {}. Total number of logs in chunk: {}",
        logs.len(),
        res.len(),
        logs.total_len()
    );
    TOTAL_LOGS_POST_MATERIALIZED.add(res.len() as u64, &[]);

    Ok(MaterializeLogsResult {
        logs,
        materialized: Chunk::new(res.into()),
    })
}

#[derive(Clone, Debug)]
pub enum VectorSegmentWriter {
    Hnsw(Box<DistributedHNSWSegmentWriter>),
    Spann(SpannSegmentWriter),
}

impl VectorSegmentWriter {
    pub fn get_id(&self) -> SegmentUuid {
        match self {
            VectorSegmentWriter::Hnsw(writer) => writer.id,
            VectorSegmentWriter::Spann(writer) => writer.id,
        }
    }

    pub fn get_name(&self) -> &'static str {
        match self {
            VectorSegmentWriter::Hnsw(_) => "DistributedHNSWSegmentWriter",
            VectorSegmentWriter::Spann(_) => "SpannSegmentWriter",
        }
    }

    pub async fn apply_materialized_log_chunk(
        &self,
        record_segment_reader: &Option<RecordSegmentReader<'_>>,
        materialized: &MaterializeLogsResult,
    ) -> Result<(), ApplyMaterializedLogError> {
        match self {
            VectorSegmentWriter::Hnsw(writer) => {
                writer
                    .apply_materialized_log_chunk(record_segment_reader, materialized)
                    .await
            }
            VectorSegmentWriter::Spann(writer) => {
                writer
                    .apply_materialized_log_chunk(record_segment_reader, materialized)
                    .await
            }
        }
    }

    pub async fn finish(&mut self) -> Result<(), Box<dyn ChromaError>> {
        match self {
            VectorSegmentWriter::Hnsw(_) => Ok(()),
            VectorSegmentWriter::Spann(writer) => writer.garbage_collect().await,
        }
    }

    pub async fn commit(self) -> Result<ChromaSegmentFlusher, Box<dyn ChromaError>> {
        match self {
            VectorSegmentWriter::Hnsw(writer) => writer.commit().await.map(|w| {
                ChromaSegmentFlusher::VectorSegment(VectorSegmentFlusher::Hnsw(Box::new(w)))
            }),
            VectorSegmentWriter::Spann(writer) => Box::pin(writer.commit())
                .await
                .map(|w| ChromaSegmentFlusher::VectorSegment(VectorSegmentFlusher::Spann(w))),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ChromaSegmentWriter<'bf> {
    RecordSegment(RecordSegmentWriter),
    MetadataSegment(MetadataSegmentWriter<'bf>),
    VectorSegment(VectorSegmentWriter),
}

impl ChromaSegmentWriter<'_> {
    pub fn get_id(&self) -> SegmentUuid {
        match self {
            ChromaSegmentWriter::RecordSegment(writer) => writer.id,
            ChromaSegmentWriter::MetadataSegment(writer) => writer.id,
            ChromaSegmentWriter::VectorSegment(writer) => writer.get_id(),
        }
    }

    pub fn get_name(&self) -> &'static str {
        match self {
            ChromaSegmentWriter::RecordSegment(_) => "RecordSegmentWriter",
            ChromaSegmentWriter::MetadataSegment(_) => "MetadataSegmentWriter",
            ChromaSegmentWriter::VectorSegment(writer) => writer.get_name(),
        }
    }

    pub async fn apply_materialized_log_chunk(
        &self,
        record_segment_reader: &Option<RecordSegmentReader<'_>>,
        materialized: &MaterializeLogsResult,
        schema: Option<Schema>,
    ) -> Result<Option<Schema>, ApplyMaterializedLogError> {
        match self {
            ChromaSegmentWriter::RecordSegment(writer) => writer
                .apply_materialized_log_chunk(record_segment_reader, materialized)
                .await
                .map(|_| None),
            ChromaSegmentWriter::MetadataSegment(writer) => {
                writer
                    .apply_materialized_log_chunk(record_segment_reader, materialized, schema)
                    .await
            }
            ChromaSegmentWriter::VectorSegment(writer) => writer
                .apply_materialized_log_chunk(record_segment_reader, materialized)
                .await
                .map(|_| None),
        }
    }

    pub async fn finish(&mut self) -> Result<(), Box<dyn ChromaError>> {
        match self {
            ChromaSegmentWriter::RecordSegment(_) => Ok(()),
            ChromaSegmentWriter::MetadataSegment(writer) => writer.finish().await,
            ChromaSegmentWriter::VectorSegment(writer) => writer.finish().await,
        }
    }

    pub async fn commit(self) -> Result<ChromaSegmentFlusher, Box<dyn ChromaError>> {
        match self {
            ChromaSegmentWriter::RecordSegment(writer) => Box::pin(writer.commit())
                .await
                .map(ChromaSegmentFlusher::RecordSegment),
            ChromaSegmentWriter::MetadataSegment(writer) => Box::pin(writer.commit())
                .await
                .map(ChromaSegmentFlusher::MetadataSegment),
            ChromaSegmentWriter::VectorSegment(writer) => Box::pin(writer.commit()).await,
        }
    }
}

#[derive(Debug)]
pub enum VectorSegmentFlusher {
    Hnsw(Box<DistributedHNSWSegmentWriter>),
    Spann(SpannSegmentFlusher),
}

#[derive(Debug)]
pub enum ChromaSegmentFlusher {
    RecordSegment(RecordSegmentFlusher),
    MetadataSegment(MetadataSegmentFlusher),
    VectorSegment(VectorSegmentFlusher),
}

impl ChromaSegmentFlusher {
    pub fn flush_key(prefix_path: &str, id: &Uuid) -> String {
        // For legacy collections, prefix_path will be empty.
        if prefix_path.is_empty() {
            return id.to_string();
        }
        format!("{}/{}", prefix_path, id)
    }
    pub fn get_id(&self) -> SegmentUuid {
        match self {
            ChromaSegmentFlusher::RecordSegment(flusher) => flusher.id,
            ChromaSegmentFlusher::MetadataSegment(flusher) => flusher.id,
            ChromaSegmentFlusher::VectorSegment(flusher) => match flusher {
                VectorSegmentFlusher::Hnsw(writer) => writer.id,
                VectorSegmentFlusher::Spann(writer) => writer.id,
            },
        }
    }

    pub fn get_name(&self) -> &'static str {
        match self {
            ChromaSegmentFlusher::RecordSegment(_) => "RecordSegmentFlusher",
            ChromaSegmentFlusher::MetadataSegment(_) => "MetadataSegmentFlusher",
            ChromaSegmentFlusher::VectorSegment(flusher) => match flusher {
                VectorSegmentFlusher::Hnsw(_) => "DistributedHNSWSegmentFlusher",
                VectorSegmentFlusher::Spann(_) => "SpannSegmentFlusher",
            },
        }
    }

    pub async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        match self {
            ChromaSegmentFlusher::RecordSegment(flusher) => Box::pin(flusher.flush()).await,
            ChromaSegmentFlusher::MetadataSegment(flusher) => Box::pin(flusher.flush()).await,
            ChromaSegmentFlusher::VectorSegment(flusher) => match flusher {
                VectorSegmentFlusher::Hnsw(flusher) => flusher.flush().await,
                VectorSegmentFlusher::Spann(flusher) => Box::pin(flusher.flush()).await,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        blockfile_metadata::{MetadataSegmentReader, MetadataSegmentWriter},
        blockfile_record::{RecordSegmentReaderCreationError, RecordSegmentWriter},
    };
    use chroma_blockstore::{
        arrow::{
            config::{BlockManagerConfig, TEST_MAX_BLOCK_SIZE_BYTES},
            provider::ArrowBlockfileProvider,
        },
        provider::BlockfileProvider,
    };
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{CollectionUuid, DatabaseUuid, OperationRecord, SegmentUuid};
    use std::{collections::HashMap, str::FromStr};

    #[tokio::test]
    async fn test_materializer_add_delete_upsert() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();
        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let segment_writer = RecordSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &record_segment,
                &blockfile_provider,
            )
            .await
            .expect("Error creating segment writer");
            let mut metadata_writer = MetadataSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &metadata_segment,
                &blockfile_provider,
            )
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
            let data = vec![LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: Some(update_metadata.clone()),
                    document: Some(String::from("doc1")),
                    operation: Operation::Add,
                },
            }];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReader> = match Box::pin(
                RecordSegmentReader::from_segment(&record_segment, &blockfile_provider),
            )
            .await
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
                        RecordSegmentReaderCreationError::DataRecordNotFound(_) => {
                            panic!("Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::UserRecordNotFound(_) => {
                            panic!("Error creating record segment reader");
                        }
                        _ => {
                            panic!("Unexpected error creating record segment reader: {:?}", e);
                        }
                    }
                }
            };
            let mat_records = materialize_logs(&record_segment_reader, data, None)
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records, None)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            let flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");
            record_segment.file_path = Box::pin(flusher.flush())
                .await
                .expect("Flush segment writer failed");
        }
        let mut update_metadata = HashMap::new();
        update_metadata.insert(
            String::from("hello"),
            UpdateMetadataValue::Str(String::from("new_world")),
        );
        let data = vec![
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Delete,
                },
            },
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![7.0, 8.0, 9.0]),
                    encoding: None,
                    metadata: Some(update_metadata),
                    document: Some(String::from("number")),
                    operation: Operation::Upsert,
                },
            },
        ];
        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Error creating segment reader");
        let some_reader = Some(reader);
        let res = materialize_logs(&some_reader, data, None)
            .await
            .expect("Error materializing logs");
        let mut res_vec = vec![];
        for record in &res {
            let record = record.hydrate(some_reader.as_ref()).await.unwrap();
            res_vec.push(record);
        }
        res_vec.sort_by(|x, y| x.get_user_id().cmp(y.get_user_id()));
        assert_eq!(1, res_vec.len());
        let emb_1 = &res_vec[0];
        assert_eq!(1, emb_1.get_offset_id());
        assert_eq!("number", emb_1.merged_document_ref().unwrap());
        assert_eq!(&[7.0, 8.0, 9.0], emb_1.merged_embeddings_ref());
        assert_eq!("embedding_id_1", emb_1.get_user_id());
        let mut res_metadata = HashMap::new();
        res_metadata.insert(
            String::from("hello"),
            MetadataValue::Str(String::from("new_world")),
        );
        assert_eq!(res_metadata, emb_1.merged_metadata());
        assert_eq!(
            MaterializedLogOperation::OverwriteExisting,
            emb_1.get_operation()
        );
        // Now write this, read again and validate.
        let segment_writer = RecordSegmentWriter::from_segment(
            &tenant,
            &database_id,
            &record_segment,
            &blockfile_provider,
        )
        .await
        .expect("Error creating segment writer");
        let mut metadata_writer = MetadataSegmentWriter::from_segment(
            &tenant,
            &database_id,
            &metadata_segment,
            &blockfile_provider,
        )
        .await
        .expect("Error creating segment writer");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &res)
            .await
            .expect("Error applying materialized log chunk");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &res, None)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        let flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        record_segment.file_path = Box::pin(flusher.flush())
            .await
            .expect("Flush segment writer failed");
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        // Read.
        let segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Error creating segment reader");
        let all_data = segment_reader
            .get_all_data()
            .await
            .expect("Get all data failed")
            .collect::<Vec<_>>();
        assert_eq!(all_data.len(), 1);
        let record = &all_data[0];
        assert_eq!(record.1.id, "embedding_id_1");
        assert_eq!(record.1.document, Some("number"));
        assert_eq!(record.1.embedding, &[7.0, 8.0, 9.0]);
        assert_eq!(record.1.metadata, Some(res_metadata));
        // Search by metadata filter.
        let metadata_segment_reader = Box::pin(MetadataSegmentReader::from_segment(
            &metadata_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Metadata segment reader construction failed");
        let res = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("hello", &"new_world".into())
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        let res = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("hello", &"world".into())
            .await
            .unwrap();
        assert_eq!(res.len(), 0);
        let res = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("bye", &"world".into())
            .await
            .unwrap();
        assert_eq!(res.len(), 0);
        let res = metadata_segment_reader
            .full_text_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .search("number")
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        let res = metadata_segment_reader
            .full_text_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .search("doc")
            .await
            .unwrap();
        assert_eq!(res.len(), 0);
    }

    #[tokio::test]
    async fn test_materializer_add_upsert() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();
        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let segment_writer = RecordSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &record_segment,
                &blockfile_provider,
            )
            .await
            .expect("Error creating segment writer");
            let mut metadata_writer = MetadataSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &metadata_segment,
                &blockfile_provider,
            )
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
            let data = vec![LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: Some(update_metadata.clone()),
                    document: Some(String::from("doc1")),
                    operation: Operation::Add,
                },
            }];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReader> = match Box::pin(
                RecordSegmentReader::from_segment(&record_segment, &blockfile_provider),
            )
            .await
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
                        RecordSegmentReaderCreationError::DataRecordNotFound(_) => {
                            panic!("Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::UserRecordNotFound(_) => {
                            panic!("Error creating record segment reader");
                        }
                        _ => {
                            panic!("Unexpected error creating record segment reader: {:?}", e);
                        }
                    }
                }
            };
            let mat_records = materialize_logs(&record_segment_reader, data, None)
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records, None)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            let flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");
            record_segment.file_path = Box::pin(flusher.flush())
                .await
                .expect("Flush segment writer failed");
        }
        let mut update_metadata = HashMap::new();
        update_metadata.insert(
            String::from("hello"),
            UpdateMetadataValue::Str(String::from("new_world")),
        );
        let data = vec![LogRecord {
            log_offset: 2,
            record: OperationRecord {
                id: "embedding_id_1".to_string(),
                embedding: Some(vec![7.0, 8.0, 9.0]),
                encoding: None,
                metadata: Some(update_metadata),
                document: None,
                operation: Operation::Upsert,
            },
        }];
        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Error creating segment reader");
        let some_reader = Some(reader);
        let res = materialize_logs(&some_reader, data, None)
            .await
            .expect("Error materializing logs");
        let mut res_vec = vec![];
        for record in &res {
            let record = record.hydrate(some_reader.as_ref()).await.unwrap();
            res_vec.push(record);
        }
        res_vec.sort_by(|x, y| x.get_user_id().cmp(y.get_user_id()));
        assert_eq!(1, res_vec.len());
        let emb_1 = &res_vec[0];
        assert_eq!(1, emb_1.get_offset_id());
        assert_eq!("doc1", emb_1.merged_document_ref().unwrap());
        assert_eq!(&[7.0, 8.0, 9.0], emb_1.merged_embeddings_ref());
        assert_eq!("embedding_id_1", emb_1.get_user_id());
        let mut res_metadata = HashMap::new();
        res_metadata.insert(
            String::from("hello"),
            MetadataValue::Str(String::from("new_world")),
        );
        res_metadata.insert(
            String::from("bye"),
            MetadataValue::Str(String::from("world")),
        );
        assert_eq!(res_metadata, emb_1.merged_metadata());
        assert_eq!(
            MaterializedLogOperation::UpdateExisting,
            emb_1.get_operation()
        );
        // Now write this, read again and validate.
        let segment_writer = RecordSegmentWriter::from_segment(
            &tenant,
            &database_id,
            &record_segment,
            &blockfile_provider,
        )
        .await
        .expect("Error creating segment writer");
        let mut metadata_writer = MetadataSegmentWriter::from_segment(
            &tenant,
            &database_id,
            &metadata_segment,
            &blockfile_provider,
        )
        .await
        .expect("Error creating segment writer");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &res)
            .await
            .expect("Error applying materialized log chunk");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &res, None)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        let flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        record_segment.file_path = Box::pin(flusher.flush())
            .await
            .expect("Flush segment writer failed");
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        // Read.
        let segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Error creating segment reader");
        let all_data = segment_reader
            .get_all_data()
            .await
            .expect("Get all data failed")
            .collect::<Vec<_>>();
        assert_eq!(all_data.len(), 1);
        let record = &all_data[0];
        assert_eq!(record.1.id, "embedding_id_1");
        assert_eq!(record.1.document, Some("doc1"));
        assert_eq!(record.1.embedding, &[7.0, 8.0, 9.0]);
        assert_eq!(record.1.metadata, Some(res_metadata));
        // Search by metadata filter.
        let metadata_segment_reader = Box::pin(MetadataSegmentReader::from_segment(
            &metadata_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Metadata segment reader construction failed");
        let res = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("hello", &"new_world".into())
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        let res = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("hello", &"world".into())
            .await
            .unwrap();
        assert_eq!(res.len(), 0);
        let res = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("bye", &"world".into())
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        let res = metadata_segment_reader
            .full_text_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .search("doc1")
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        let res = metadata_segment_reader
            .full_text_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .search("number")
            .await
            .unwrap();
        assert_eq!(res.len(), 0);
    }

    #[tokio::test]
    async fn test_materializer_add_delete_upsert_update() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();
        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let segment_writer = RecordSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &record_segment,
                &blockfile_provider,
            )
            .await
            .expect("Error creating segment writer");
            let mut metadata_writer = MetadataSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &metadata_segment,
                &blockfile_provider,
            )
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
            let data = vec![LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: Some(update_metadata.clone()),
                    document: Some(String::from("doc1")),
                    operation: Operation::Add,
                },
            }];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReader> = match Box::pin(
                RecordSegmentReader::from_segment(&record_segment, &blockfile_provider),
            )
            .await
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
                        RecordSegmentReaderCreationError::DataRecordNotFound(_) => {
                            panic!("Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::UserRecordNotFound(_) => {
                            panic!("Error creating record segment reader");
                        }
                        _ => {
                            panic!("Unexpected error creating record segment reader: {:?}", e);
                        }
                    }
                }
            };
            let mat_records = materialize_logs(&record_segment_reader, data, None)
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records, None)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            let flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");
            record_segment.file_path = Box::pin(flusher.flush())
                .await
                .expect("Flush segment writer failed");
        }
        let mut update_metadata = HashMap::new();
        update_metadata.insert(
            String::from("hello"),
            UpdateMetadataValue::Str(String::from("new_world")),
        );
        let data = vec![
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Delete,
                },
            },
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![7.0, 8.0, 9.0]),
                    encoding: None,
                    metadata: Some(update_metadata),
                    document: None,
                    operation: Operation::Upsert,
                },
            },
            LogRecord {
                log_offset: 4,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: Some(String::from("number")),
                    operation: Operation::Update,
                },
            },
        ];
        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Error creating segment reader");
        let some_reader = Some(reader);
        let res = materialize_logs(&some_reader, data, None)
            .await
            .expect("Error materializing logs");
        let mut res_vec = vec![];
        for record in &res {
            let record = record.hydrate(some_reader.as_ref()).await.unwrap();
            res_vec.push(record);
        }
        res_vec.sort_by(|x, y| x.get_user_id().cmp(y.get_user_id()));
        assert_eq!(1, res_vec.len());
        let emb_1 = &res_vec[0];
        assert_eq!(1, emb_1.get_offset_id());
        assert_eq!("number", emb_1.merged_document_ref().unwrap());
        assert_eq!(&[7.0, 8.0, 9.0], emb_1.merged_embeddings_ref());
        assert_eq!("embedding_id_1", emb_1.get_user_id());
        let mut res_metadata = HashMap::new();
        res_metadata.insert(
            String::from("hello"),
            MetadataValue::Str(String::from("new_world")),
        );
        assert_eq!(res_metadata, emb_1.merged_metadata());
        assert_eq!(
            MaterializedLogOperation::OverwriteExisting,
            emb_1.get_operation()
        );
        // Now write this, read again and validate.
        let segment_writer = RecordSegmentWriter::from_segment(
            &tenant,
            &database_id,
            &record_segment,
            &blockfile_provider,
        )
        .await
        .expect("Error creating segment writer");
        let mut metadata_writer = MetadataSegmentWriter::from_segment(
            &tenant,
            &database_id,
            &metadata_segment,
            &blockfile_provider,
        )
        .await
        .expect("Error creating segment writer");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &res)
            .await
            .expect("Error applying materialized log chunk");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &res, None)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        let flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        record_segment.file_path = Box::pin(flusher.flush())
            .await
            .expect("Flush segment writer failed");
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        // Read.
        let segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Error creating segment reader");
        let all_data = segment_reader
            .get_all_data()
            .await
            .expect("Get all data failed")
            .collect::<Vec<_>>();
        assert_eq!(all_data.len(), 1);
        let record = &all_data[0];
        assert_eq!(record.1.id, "embedding_id_1");
        assert_eq!(record.1.document, Some("number"));
        assert_eq!(record.1.embedding, &[7.0, 8.0, 9.0]);
        assert_eq!(record.1.metadata, Some(res_metadata));
        // Search by metadata filter.
        let metadata_segment_reader = Box::pin(MetadataSegmentReader::from_segment(
            &metadata_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Metadata segment reader construction failed");
        let res = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("hello", &"new_world".into())
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        let res = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("hello", &"world".into())
            .await
            .unwrap();
        assert_eq!(res.len(), 0);
        let res = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("bye", &"world".into())
            .await
            .unwrap();
        assert_eq!(res.len(), 0);
        let res = metadata_segment_reader
            .full_text_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .search("number")
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        let res = metadata_segment_reader
            .full_text_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .search("doc1")
            .await
            .unwrap();
        assert_eq!(res.len(), 0);
    }

    #[tokio::test]
    async fn test_materializer_basic() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();
        {
            let segment_writer = RecordSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &record_segment,
                &blockfile_provider,
            )
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
            let record_segment_reader: Option<RecordSegmentReader> = match Box::pin(
                RecordSegmentReader::from_segment(&record_segment, &blockfile_provider),
            )
            .await
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
                        RecordSegmentReaderCreationError::DataRecordNotFound(_) => {
                            panic!("Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::UserRecordNotFound(_) => {
                            panic!("Error creating record segment reader");
                        }
                        _ => {
                            panic!("Unexpected error creating record segment reader: {:?}", e);
                        }
                    }
                }
            };
            let mat_records = materialize_logs(&record_segment_reader, data, None)
                .await
                .expect("Log materialization failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log failed");
            let flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            record_segment.file_path = Box::pin(flusher.flush())
                .await
                .expect("Flush segment writer failed");
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
        let reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Error creating segment reader");
        let some_reader = Some(reader);
        let res = materialize_logs(&some_reader, data, None)
            .await
            .expect("Error materializing logs");
        assert_eq!(3, res.len());
        let mut id1_found = 0;
        let mut id2_found = 0;
        let mut id3_found = 0;
        for log in &res {
            let log = log.hydrate(some_reader.as_ref()).await.unwrap();

            // Embedding 3.
            if log.get_user_id() == "embedding_id_3" {
                id3_found += 1;
                assert_eq!("embedding_id_3", log.get_user_id());
                assert!(log.get_data_record().is_none());
                assert_eq!("doc3", log.document_ref_from_log().unwrap());
                assert_eq!(vec![7.0, 8.0, 9.0], log.merged_embeddings_ref());
                assert_eq!(3, log.get_offset_id());
                assert_eq!(MaterializedLogOperation::AddNew, log.get_operation());
                let mut hello_found = 0;
                let mut hello_again_found = 0;
                for (key, value) in log.get_metadata_to_be_merged().unwrap() {
                    if key == "hello" {
                        assert_eq!(MetadataValue::Str(String::from("new_world")), *value);
                        hello_found += 1;
                    } else if key == "hello_again" {
                        assert_eq!(MetadataValue::Str(String::from("new_world")), *value);
                        hello_again_found += 1;
                    } else {
                        panic!("Not expecting any other key");
                    }
                }
                assert_eq!(hello_found, 1);
                assert_eq!(hello_again_found, 1);
            } else if log.get_data_record().as_ref().unwrap().id == "embedding_id_2" {
                id2_found += 1;
                assert_eq!(
                    MaterializedLogOperation::DeleteExisting,
                    log.get_operation()
                );
                assert_eq!(2, log.get_offset_id());
                assert_eq!(None, log.document_ref_from_log());
                assert_eq!(None, log.embeddings_ref_from_log());
                assert_eq!(None, log.get_metadata_to_be_merged());
                assert!(log.get_data_record().is_some());
            } else if log.get_data_record().as_ref().unwrap().id == "embedding_id_1" {
                id1_found += 1;
                assert_eq!(
                    MaterializedLogOperation::UpdateExisting,
                    log.get_operation()
                );
                assert_eq!(1, log.get_offset_id());
                assert_eq!(None, log.document_ref_from_log());
                assert_eq!(None, log.embeddings_ref_from_log());
                let mut hello_found = 0;
                let mut hello_again_found = 0;
                for (key, value) in log.get_metadata_to_be_merged().unwrap() {
                    if key == "hello" {
                        assert_eq!(MetadataValue::Str(String::from("new_world")), *value);
                        hello_found += 1;
                    } else if key == "hello_again" {
                        assert_eq!(MetadataValue::Str(String::from("new_world")), *value);
                        hello_again_found += 1;
                    } else {
                        panic!("Not expecting any other key");
                    }
                }
                assert_eq!(hello_found, 1);
                assert_eq!(hello_again_found, 1);
                assert!(log.get_data_record().is_some());
                assert_eq!(
                    log.get_data_record().as_ref().unwrap().document,
                    Some("doc1")
                );
                assert_eq!(
                    log.get_data_record().as_ref().unwrap().embedding,
                    vec![1.0, 2.0, 3.0].as_slice()
                );
                hello_found = 0;
                let mut bye_found = 0;
                for (key, value) in log
                    .get_data_record()
                    .as_ref()
                    .unwrap()
                    .metadata
                    .as_ref()
                    .unwrap()
                {
                    if key == "hello" {
                        assert_eq!(MetadataValue::Str(String::from("world")), *value);
                        hello_found += 1;
                    } else if key == "bye" {
                        assert_eq!(MetadataValue::Str(String::from("world")), *value);
                        bye_found += 1;
                    } else {
                        panic!("Not expecting any other key");
                    }
                }
                assert_eq!(hello_found, 1);
                assert_eq!(bye_found, 1);
            } else {
                panic!("Not expecting any other materialized record");
            }
        }
        assert_eq!(1, id1_found);
        assert_eq!(1, id2_found);
        assert_eq!(1, id3_found);
        // Now write this, read again and validate.
        let segment_writer = RecordSegmentWriter::from_segment(
            &tenant,
            &database_id,
            &record_segment,
            &blockfile_provider,
        )
        .await
        .expect("Error creating segment writer");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &res)
            .await
            .expect("Error applying materialized log chunk");
        let flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        record_segment.file_path = Box::pin(flusher.flush())
            .await
            .expect("Flush segment writer failed");
        // Read.
        let segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Error creating segment reader");
        let all_data = segment_reader
            .get_all_data()
            .await
            .expect("Get all data failed")
            .collect::<Vec<_>>();
        for data in all_data {
            assert_ne!(data.1.id, "embedding_id_2");
            if data.1.id == "embedding_id_1" {
                assert!(data
                    .1
                    .metadata
                    .clone()
                    .expect("Metadata is empty")
                    .contains_key("hello"),);
                assert_eq!(
                    data.1
                        .metadata
                        .clone()
                        .expect("Metadata is empty")
                        .get("hello"),
                    Some(&MetadataValue::Str(String::from("new_world")))
                );
                assert!(data
                    .1
                    .metadata
                    .clone()
                    .expect("Metadata is empty")
                    .contains_key("bye"),);
                assert_eq!(
                    data.1
                        .metadata
                        .clone()
                        .expect("Metadata is empty")
                        .get("bye"),
                    Some(&MetadataValue::Str(String::from("world")))
                );
                assert!(data
                    .1
                    .metadata
                    .clone()
                    .expect("Metadata is empty")
                    .contains_key("hello_again"),);
                assert_eq!(
                    data.1
                        .metadata
                        .clone()
                        .expect("Metadata is empty")
                        .get("hello_again"),
                    Some(&MetadataValue::Str(String::from("new_world")))
                );
                assert_eq!(data.1.document.expect("Non empty document"), "doc1");
                assert_eq!(data.1.embedding, vec![1.0, 2.0, 3.0]);
            } else if data.1.id == "embedding_id_3" {
                assert!(data
                    .1
                    .metadata
                    .clone()
                    .expect("Metadata is empty")
                    .contains_key("hello"),);
                assert_eq!(
                    data.1
                        .metadata
                        .clone()
                        .expect("Metadata is empty")
                        .get("hello"),
                    Some(&MetadataValue::Str(String::from("new_world")))
                );
                assert!(data
                    .1
                    .metadata
                    .clone()
                    .expect("Metadata is empty")
                    .contains_key("hello_again"),);
                assert_eq!(
                    data.1
                        .metadata
                        .clone()
                        .expect("Metadata is empty")
                        .get("hello_again"),
                    Some(&MetadataValue::Str(String::from("new_world")))
                );
                assert_eq!(data.1.document.expect("Non empty document"), "doc3");
                assert_eq!(data.1.embedding, vec![7.0, 8.0, 9.0]);
            }
        }
    }
}
