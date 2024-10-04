use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{
    Chunk, DataRecord, DeletedMetadata, LogRecord, MaterializedLogOperation, Metadata,
    MetadataDelta, MetadataValue, MetadataValueConversionError, Operation, OperationRecord,
    UpdateMetadata, UpdateMetadataValue,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;
use tracing::{Instrument, Span};

use super::record_segment::{ApplyMaterializedLogError, RecordSegmentReader};

// Materializes metadata from update metadata, populating the delete list
// and upsert list.
pub(crate) fn materialize_update_metadata(
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
pub(crate) fn merge_update_metadata(
    base_metadata: (&Option<Metadata>, &Option<DeletedMetadata>),
    update_metadata: &Option<UpdateMetadata>,
) -> Result<(Option<Metadata>, Option<DeletedMetadata>), MetadataValueConversionError> {
    let mut merged_metadata = HashMap::new();
    let mut deleted_metadata = DeletedMetadata::new();
    match base_metadata.0 {
        Some(base_mt) => {
            merged_metadata = base_mt.clone();
        }
        None => (),
    }
    match base_metadata.1 {
        Some(deleted_mt) => {
            deleted_metadata = deleted_mt.clone();
        }
        None => (),
    }
    match update_metadata {
        Some(update_metadata) => {
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
            };
        }
        None => (),
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

#[derive(Debug, Clone)]
pub(crate) struct MaterializedLogRecord<'referred_data> {
    // This is the data record read from the record segment for this id.
    // None if the record exists only in the log.
    pub(crate) data_record: Option<DataRecord<'referred_data>>,
    // If present in the record segment then it is the offset id
    // in the record segment at which the record was found.
    // If not present in the segment then it is the offset id
    // at which it should be inserted.
    pub(crate) offset_id: u32,
    // Set only for the records that are being inserted for the first time
    // in the log since data_record will be None in such cases. For other
    // cases, just read from data record.
    pub(crate) user_id: Option<&'referred_data str>,
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
    pub(crate) final_operation: MaterializedLogOperation,
    // This is the metadata obtained by combining all the operations
    // present in the log for this id.
    // E.g. if has log has [Insert(a: h), Update(a: b, c: d), Update(a: e, f: g)] then this
    // will contain (a: e, c: d, f: g).
    pub(crate) metadata_to_be_merged: Option<Metadata>,
    // Keys from the metadata that the user wants to delete. This is guaranteed
    // to be disjoint from metadata_to_be_merged i.e. there won't be keys
    // present in both the places.
    pub(crate) metadata_to_be_deleted: Option<HashSet<String>>,
    // This is the final document obtained from the last non null operation.
    // E.g. if log has [Insert(str0), Update(str1), Update(str2), Update()] then this will contain
    // str2. None if final operation is Delete.
    pub(crate) final_document: Option<&'referred_data str>,
    // Similar to above, this is the final embedding obtained
    // from the last non null operation.
    // E.g. if log has [Insert(emb0), Update(emb1), Update(emb2), Update()]
    // then this will contain emb2. None if final operation is Delete.
    pub(crate) final_embedding: Option<&'referred_data [f32]>,
}

impl<'referred_data> MaterializedLogRecord<'referred_data> {
    // Performs a deep copy of the document so only use it if really
    // needed. If you only need a reference then use merged_document_ref
    // defined below.
    pub(crate) fn merged_document(&self) -> Option<String> {
        if self.final_operation == MaterializedLogOperation::OverwriteExisting
            || self.final_operation == MaterializedLogOperation::AddNew
        {
            return self.final_document.map(|doc| doc.to_string());
        }
        return match self.final_document {
            Some(doc) => Some(doc.to_string()),
            None => match self.data_record.as_ref() {
                Some(data_record) => data_record.document.map(|doc| doc.to_string()),
                None => None,
            },
        };
    }

    pub(crate) fn merged_document_ref(&self) -> Option<&str> {
        if self.final_operation == MaterializedLogOperation::OverwriteExisting
            || self.final_operation == MaterializedLogOperation::AddNew
        {
            return match self.final_document {
                Some(doc) => Some(doc),
                None => None,
            };
        }
        return match self.final_document {
            Some(doc) => Some(doc),
            None => match self.data_record.as_ref() {
                Some(data_record) => match data_record.document {
                    Some(doc) => Some(doc),
                    None => None,
                },
                None => None,
            },
        };
    }

    // Performs a deep copy of the user id so only use it if really
    // needed. If you only need reference then use merged_user_id_ref below.
    pub(crate) fn merged_user_id(&self) -> String {
        match self.user_id {
            Some(id) => id.to_string(),
            None => match &self.data_record {
                Some(data_record) => data_record.id.to_string(),
                None => panic!("Expected at least one user id to be set"),
            },
        }
    }

    pub(crate) fn merged_user_id_ref(&self) -> &str {
        match self.user_id {
            Some(id) => id,
            None => match &self.data_record {
                Some(data_record) => data_record.id,
                None => panic!("Expected at least one user id to be set"),
            },
        }
    }

    // Performs a deep copy of the metadata so only use it if really
    // needed. If you only need reference then use merged_metadata_ref below.
    pub(crate) fn merged_metadata(&self) -> HashMap<String, MetadataValue> {
        let mut final_metadata;
        if self.final_operation == MaterializedLogOperation::OverwriteExisting
            || self.final_operation == MaterializedLogOperation::AddNew
        {
            final_metadata = HashMap::new();
        } else {
            final_metadata = match self.data_record.as_ref() {
                Some(data_record) => match data_record.metadata {
                    Some(ref map) => map.clone(), // auto deref here.
                    None => HashMap::new(),
                },
                None => HashMap::new(),
            };
        }
        if let Some(metadata) = self.metadata_to_be_merged.as_ref() {
            for (key, value) in metadata {
                final_metadata.insert(key.clone(), value.clone());
            }
        }
        if let Some(metadata) = self.metadata_to_be_deleted.as_ref() {
            for key in metadata {
                final_metadata.remove(key);
            }
        }
        final_metadata
    }

    pub(crate) fn metadata_delta(&'referred_data self) -> MetadataDelta<'referred_data> {
        let mut metadata_delta = MetadataDelta::new();
        let mut base_metadata: HashMap<&str, &MetadataValue> = HashMap::new();
        match &self.data_record {
            Some(data_record) => match &data_record.metadata {
                Some(meta) => {
                    for (meta_key, meta_val) in meta {
                        base_metadata.insert(meta_key, meta_val);
                    }
                }
                None => (),
            },
            None => (),
        };
        // Populate updates.
        match &self.metadata_to_be_merged {
            Some(meta) => {
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
            }
            None => (),
        };
        // Populate deletes.
        match &self.metadata_to_be_deleted {
            Some(meta) => {
                for key in meta {
                    if let Some(old_value) = base_metadata.get(key.as_str()) {
                        metadata_delta
                            .metadata_to_delete
                            .insert(key.as_str(), old_value);
                    }
                }
            }
            None => (),
        }
        metadata_delta
    }

    // Returns references to metadata present in the materialized log record.
    pub(crate) fn merged_metadata_ref(&self) -> HashMap<&str, &MetadataValue> {
        let mut final_metadata: HashMap<&str, &MetadataValue> = HashMap::new();
        if self.final_operation != MaterializedLogOperation::OverwriteExisting
            && self.final_operation != MaterializedLogOperation::AddNew
        {
            match &self.data_record {
                Some(data_record) => match &data_record.metadata {
                    Some(meta) => {
                        for (meta_key, meta_val) in meta {
                            final_metadata.insert(meta_key, meta_val);
                        }
                    }
                    None => (),
                },
                None => (),
            };
        }
        match &self.metadata_to_be_merged {
            Some(meta) => {
                for (meta_key, meta_val) in meta {
                    final_metadata.insert(meta_key, meta_val);
                }
            }
            None => (),
        };
        // Remove the deleted metadatas.
        match &self.metadata_to_be_deleted {
            Some(meta) => {
                for key in meta {
                    final_metadata.remove(key.as_str());
                }
            }
            None => (),
        }
        final_metadata
    }

    pub(crate) fn merged_embeddings(&self) -> &[f32] {
        if self.final_operation == MaterializedLogOperation::OverwriteExisting
            || self.final_operation == MaterializedLogOperation::AddNew
        {
            return match self.final_embedding {
                Some(embed) => embed,
                None => panic!("Expected source of embedding"),
            };
        }
        return match self.final_embedding {
            Some(embed) => embed,
            None => match self.data_record.as_ref() {
                Some(data_record) => data_record.embedding,
                None => panic!("Expected at least one source of embedding"),
            },
        };
    }
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
            final_operation: MaterializedLogOperation::Initial,
            metadata_to_be_merged: None,
            metadata_to_be_deleted: None,
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
        let merged_metadata;
        let deleted_metadata;
        match &log_record.metadata {
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

        let document = log_record.document.as_deref();
        let embedding = match &log_record.embedding {
            Some(embedding) => Some(embedding.as_slice()),
            None => {
                return Err(LogMaterializerError::EmbeddingMaterialization);
            }
        };

        Ok(Self {
            data_record: None,
            offset_id,
            user_id: Some(user_id),
            final_operation: MaterializedLogOperation::AddNew,
            metadata_to_be_merged: merged_metadata,
            metadata_to_be_deleted: deleted_metadata,
            final_document: document,
            final_embedding: embedding,
        })
    }
}

pub(crate) struct LogMaterializer<'me> {
    // Is None when record segment is uninitialized.
    pub(crate) record_segment_reader: Option<RecordSegmentReader<'me>>,
    pub(crate) logs: Chunk<LogRecord>,
    // Is None for readers. In that case, the materializer reads
    // the current maximum from the record segment and uses that
    // for materializing. Writers pass this value to the materializer
    // because they need to share this across all log partitions.
    pub(crate) curr_offset_id: Option<Arc<AtomicU32>>,
}

impl<'me> LogMaterializer<'me> {
    pub(crate) fn new(
        record_segment_reader: Option<RecordSegmentReader<'me>>,
        logs: Chunk<LogRecord>,
        curr_offset_id: Option<Arc<AtomicU32>>,
    ) -> Self {
        Self {
            record_segment_reader,
            logs,
            curr_offset_id,
        }
    }
    pub(crate) async fn materialize(
        &'me self,
    ) -> Result<Chunk<MaterializedLogRecord<'me>>, LogMaterializerError> {
        // Trace the total_len since len() iterates over the entire chunk
        // and we don't want to do that just to trace the length.
        tracing::info!(
            "Total length of logs in materializer: {}",
            self.logs.total_len()
        );
        let next_offset_id;
        match self.curr_offset_id.as_ref() {
            Some(curr_offset_id) => {
                next_offset_id = curr_offset_id.clone();
                next_offset_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            None => {
                match self.record_segment_reader.as_ref() {
                    Some(reader) => {
                        next_offset_id = reader.get_current_max_offset_id();
                        next_offset_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    // This means that the segment is uninitialized so counting starts
                    // from 1.
                    None => {
                        next_offset_id = Arc::new(AtomicU32::new(1));
                    }
                };
            }
        }
        // Populate entries that are present in the record segment.
        let mut existing_id_to_materialized: HashMap<&str, MaterializedLogRecord> = HashMap::new();
        let mut new_id_to_materialized: HashMap<&str, MaterializedLogRecord> = HashMap::new();
        match &self.record_segment_reader {
            Some(reader) => {
                async {
                    for (log_record, _) in self.logs.iter() {
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
                                    return Err(LogMaterializerError::RecordSegment(e));
                                }
                            }
                        }
                    }
                    Ok(())
                }.instrument(tracing::info_span!(parent: Span::current(), "Materialization read from stroage")).await?;
            }
            // If record segment is uninitialized then there's nothing
            // in the record segment yet.
            None => (),
        }
        // Populate updates to these and fresh records that are being
        // inserted for the first time.
        async {
            for (log_record, _) in self.logs.iter() {
                match log_record.record.operation {
                    Operation::Add => {
                        // If this is an add of a record present in the segment then add
                        // only if it has been previously deleted in the log.
                        if existing_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                            // safe to unwrap
                            let operation = existing_id_to_materialized
                                .get(log_record.record.id.as_str())
                                .unwrap()
                                .final_operation
                                .clone();
                            match operation {
                                MaterializedLogOperation::DeleteExisting => {
                                    let curr_val = existing_id_to_materialized.remove(log_record.record.id.as_str()).unwrap();
                                    // Overwrite.
                                    let mut materialized_record =
                                        match MaterializedLogRecord::try_from((
                                            &log_record.record,
                                            curr_val.offset_id,
                                            log_record.record.id.as_str(),
                                        )) {
                                            Ok(record) => record,
                                            Err(e) => {
                                                return Err(e);
                                            }
                                        };
                                    materialized_record.data_record = curr_val.data_record;
                                    materialized_record.final_operation =
                                        MaterializedLogOperation::OverwriteExisting;
                                    existing_id_to_materialized
                                        .insert(log_record.record.id.as_str(), materialized_record);
                                },
                                MaterializedLogOperation::AddNew => panic!("Invariant violation. Existing record can never have an Add new state"),
                                MaterializedLogOperation::Initial | MaterializedLogOperation::OverwriteExisting | MaterializedLogOperation::UpdateExisting => {
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
                        // in segment then we'll have to pass it as a delete
                        // to the compactor so that it can be deleted.
                        if new_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                            new_id_to_materialized.remove(log_record.record.id.as_str());
                        } else if existing_id_to_materialized
                            .contains_key(log_record.record.id.as_str())
                        {
                            // Mark state as deleted. Other fields become noop after such a delete.
                            let record_from_map = existing_id_to_materialized
                                .get_mut(log_record.record.id.as_str())
                                .unwrap();
                            record_from_map.final_operation = MaterializedLogOperation::DeleteExisting;
                            record_from_map.final_document = None;
                            record_from_map.final_embedding = None;
                            record_from_map.metadata_to_be_merged = None;
                            record_from_map.metadata_to_be_deleted = None;
                            record_from_map.user_id = None;
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
                            None => match new_id_to_materialized.get_mut(log_record.record.id.as_str())
                            {
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
                        if let Some(doc) = log_record.record.document.as_ref() {
                            record_from_map.final_document = Some(doc);
                        }
                        if let Some(emb) = log_record.record.embedding.as_ref() {
                            record_from_map.final_embedding = Some(emb.as_slice());
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
                    Operation::Upsert => {
                        if existing_id_to_materialized.contains_key(log_record.record.id.as_str()) {
                            // safe to unwrap here.
                            let operation = existing_id_to_materialized
                                .get(log_record.record.id.as_str())
                                .unwrap()
                                .final_operation
                                .clone();
                            match operation {
                                MaterializedLogOperation::DeleteExisting => {
                                    let curr_val = existing_id_to_materialized.remove(log_record.record.id.as_str()).unwrap();
                                    // Overwrite.
                                    let mut materialized_record =
                                        match MaterializedLogRecord::try_from((
                                            &log_record.record,
                                            curr_val.offset_id,
                                            log_record.record.id.as_str(),
                                        )) {
                                            Ok(record) => record,
                                            Err(e) => {
                                                return Err(e);
                                            }
                                        };
                                    materialized_record.data_record = curr_val.data_record;
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
                                    if let Some(doc) = log_record.record.document.as_ref() {
                                        record_from_map.final_document = Some(doc);
                                    }
                                    if let Some(emb) = log_record.record.embedding.as_ref() {
                                        record_from_map.final_embedding = Some(emb.as_slice());
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
                            if let Some(doc) = log_record.record.document.as_ref() {
                                record_from_map.final_document = Some(doc);
                            }
                            if let Some(emb) = log_record.record.embedding.as_ref() {
                                record_from_map.final_embedding = Some(emb.as_slice());
                            }
                            // This record is not present on storage yet hence final operation is
                            // AddNew and not UpdateExisting.
                            record_from_map.final_operation = MaterializedLogOperation::AddNew;
                        } else {
                            // Insert.
                            let next_offset =
                                next_offset_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            let materialized_record = match MaterializedLogRecord::try_from((
                                &log_record.record,
                                next_offset,
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
            Ok(())
        }.instrument(tracing::info_span!(parent: Span::current(), "Materialization main iteration")).await?;
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
        Ok(Chunk::new(res.into()))
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
    #![allow(deprecated)]

    use super::*;
    use crate::segment::{
        metadata_segment::{MetadataSegmentReader, MetadataSegmentWriter},
        record_segment::{RecordSegmentReaderCreationError, RecordSegmentWriter},
    };
    use chroma_blockstore::{
        arrow::{config::TEST_MAX_BLOCK_SIZE_BYTES, provider::ArrowBlockfileProvider},
        provider::BlockfileProvider,
    };
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{
        DirectDocumentComparison, DirectWhereComparison, PrimitiveOperator, Where, WhereComparison,
    };
    use std::{collections::HashMap, str::FromStr};
    use uuid::Uuid;

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
            metadata_writer
                .write_to_blockfiles()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(mat_records)
                .await
                .expect("Apply materialized log failed");
            let metadata_flusher = metadata_writer
                .commit()
                .expect("Commit for metadata writer failed");
            let flusher = segment_writer
                .commit()
                .expect("Commit for segment writer failed");
            metadata_segment.file_path = metadata_flusher
                .flush()
                .await
                .expect("Flush metadata segment writer failed");
            record_segment.file_path = flusher.flush().await.expect("Flush segment writer failed");
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
        let reader = RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
            .await
            .expect("Error creating segment reader");
        let materializer = LogMaterializer {
            record_segment_reader: Some(reader),
            logs: data,
            curr_offset_id: None,
        };
        let res = materializer
            .materialize()
            .await
            .expect("Error materializing logs");
        let mut res_vec = vec![];
        for (record, _) in res.iter() {
            res_vec.push(record);
        }
        res_vec.sort_by(|x, y| x.merged_user_id_ref().cmp(y.merged_user_id_ref()));
        assert_eq!(1, res_vec.len());
        let emb_1 = res_vec[0];
        assert_eq!(1, emb_1.offset_id);
        assert_eq!("number", emb_1.merged_document_ref().unwrap());
        assert_eq!(&[7.0, 8.0, 9.0], emb_1.merged_embeddings());
        assert_eq!("embedding_id_1", emb_1.merged_user_id_ref());
        let mut res_metadata = HashMap::new();
        res_metadata.insert(
            String::from("hello"),
            MetadataValue::Str(String::from("new_world")),
        );
        assert_eq!(res_metadata, emb_1.merged_metadata());
        assert_eq!(
            MaterializedLogOperation::OverwriteExisting,
            emb_1.final_operation
        );
        // Now write this, read again and validate.
        let segment_writer =
            RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        let mut metadata_writer =
            MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        segment_writer
            .apply_materialized_log_chunk(res.clone())
            .await
            .expect("Error applying materialized log chunk");
        metadata_writer
            .apply_materialized_log_chunk(res.clone())
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .write_to_blockfiles()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        let flusher = segment_writer
            .commit()
            .expect("Commit for segment writer failed");
        record_segment.file_path = flusher.flush().await.expect("Flush segment writer failed");
        let metadata_flusher = metadata_writer
            .commit()
            .expect("Commit for metadata writer failed");
        metadata_segment.file_path = metadata_flusher
            .flush()
            .await
            .expect("Flush metadata segment writer failed");
        // Read.
        let segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Error creating segment reader");
        let all_data = segment_reader
            .get_all_data()
            .await
            .expect("Get all data failed");
        assert_eq!(all_data.len(), 1);
        let record = &all_data[0];
        assert_eq!(record.id, "embedding_id_1");
        assert_eq!(record.document, Some("number"));
        assert_eq!(record.embedding, &[7.0, 8.0, 9.0]);
        assert_eq!(record.metadata, Some(res_metadata));
        // Search by metadata filter.
        let metadata_segment_reader =
            MetadataSegmentReader::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Metadata segment reader construction failed");
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: String::from("hello"),
            comparison: WhereComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str(String::from("new_world")),
            ),
        });
        let res = metadata_segment_reader
            .query(Some(&where_clause), None, None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first(), Some(&(1_usize)));
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: String::from("hello"),
            comparison: WhereComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str(String::from("world")),
            ),
        });
        let res = metadata_segment_reader
            .query(Some(&where_clause), None, None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 0);
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: String::from("bye"),
            comparison: WhereComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str(String::from("world")),
            ),
        });
        let res = metadata_segment_reader
            .query(Some(&where_clause), None, None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 0);
        let where_document_clause =
            Where::DirectWhereDocumentComparison(DirectDocumentComparison {
                document: String::from("number"),
                operator: chroma_types::DocumentOperator::Contains,
            });
        let res = metadata_segment_reader
            .query(None, Some(&where_document_clause), None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first(), Some(&(1_usize)));
        let where_document_clause =
            Where::DirectWhereDocumentComparison(DirectDocumentComparison {
                document: String::from("doc"),
                operator: chroma_types::DocumentOperator::Contains,
            });
        let res = metadata_segment_reader
            .query(None, Some(&where_document_clause), None, 0, 0)
            .await
            .expect("Metadata segment query failed")
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
            metadata_writer
                .write_to_blockfiles()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(mat_records)
                .await
                .expect("Apply materialized log failed");
            let metadata_flusher = metadata_writer
                .commit()
                .expect("Commit for metadata writer failed");
            let flusher = segment_writer
                .commit()
                .expect("Commit for segment writer failed");
            metadata_segment.file_path = metadata_flusher
                .flush()
                .await
                .expect("Flush metadata segment writer failed");
            record_segment.file_path = flusher.flush().await.expect("Flush segment writer failed");
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
        let reader = RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
            .await
            .expect("Error creating segment reader");
        let materializer = LogMaterializer {
            record_segment_reader: Some(reader),
            logs: data,
            curr_offset_id: None,
        };
        let res = materializer
            .materialize()
            .await
            .expect("Error materializing logs");
        let mut res_vec = vec![];
        for (record, _) in res.iter() {
            res_vec.push(record);
        }
        res_vec.sort_by(|x, y| x.merged_user_id_ref().cmp(y.merged_user_id_ref()));
        assert_eq!(1, res_vec.len());
        let emb_1 = res_vec[0];
        assert_eq!(1, emb_1.offset_id);
        assert_eq!("doc1", emb_1.merged_document_ref().unwrap());
        assert_eq!(&[7.0, 8.0, 9.0], emb_1.merged_embeddings());
        assert_eq!("embedding_id_1", emb_1.merged_user_id_ref());
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
            emb_1.final_operation
        );
        // Now write this, read again and validate.
        let segment_writer =
            RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        let mut metadata_writer =
            MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        segment_writer
            .apply_materialized_log_chunk(res.clone())
            .await
            .expect("Error applying materialized log chunk");
        metadata_writer
            .apply_materialized_log_chunk(res.clone())
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .write_to_blockfiles()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        let flusher = segment_writer
            .commit()
            .expect("Commit for segment writer failed");
        record_segment.file_path = flusher.flush().await.expect("Flush segment writer failed");
        let metadata_flusher = metadata_writer
            .commit()
            .expect("Commit for metadata writer failed");
        metadata_segment.file_path = metadata_flusher
            .flush()
            .await
            .expect("Flush metadata segment writer failed");
        // Read.
        let segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Error creating segment reader");
        let all_data = segment_reader
            .get_all_data()
            .await
            .expect("Get all data failed");
        assert_eq!(all_data.len(), 1);
        let record = &all_data[0];
        assert_eq!(record.id, "embedding_id_1");
        assert_eq!(record.document, Some("doc1"));
        assert_eq!(record.embedding, &[7.0, 8.0, 9.0]);
        assert_eq!(record.metadata, Some(res_metadata));
        // Search by metadata filter.
        let metadata_segment_reader =
            MetadataSegmentReader::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Metadata segment reader construction failed");
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: String::from("hello"),
            comparison: WhereComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str(String::from("new_world")),
            ),
        });
        let res = metadata_segment_reader
            .query(Some(&where_clause), None, None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first(), Some(&(1_usize)));
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: String::from("hello"),
            comparison: WhereComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str(String::from("world")),
            ),
        });
        let res = metadata_segment_reader
            .query(Some(&where_clause), None, None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 0);
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: String::from("bye"),
            comparison: WhereComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str(String::from("world")),
            ),
        });
        let res = metadata_segment_reader
            .query(Some(&where_clause), None, None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first(), Some(&(1_usize)));
        let where_document_clause =
            Where::DirectWhereDocumentComparison(DirectDocumentComparison {
                document: String::from("doc1"),
                operator: chroma_types::DocumentOperator::Contains,
            });
        let res = metadata_segment_reader
            .query(None, Some(&where_document_clause), None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first(), Some(&(1_usize)));
        let where_document_clause =
            Where::DirectWhereDocumentComparison(DirectDocumentComparison {
                document: String::from("number"),
                operator: chroma_types::DocumentOperator::Contains,
            });
        let res = metadata_segment_reader
            .query(None, Some(&where_document_clause), None, 0, 0)
            .await
            .expect("Metadata segment query failed")
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
            metadata_writer
                .write_to_blockfiles()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(mat_records)
                .await
                .expect("Apply materialized log failed");
            let metadata_flusher = metadata_writer
                .commit()
                .expect("Commit for metadata writer failed");
            let flusher = segment_writer
                .commit()
                .expect("Commit for segment writer failed");
            metadata_segment.file_path = metadata_flusher
                .flush()
                .await
                .expect("Flush metadata segment writer failed");
            record_segment.file_path = flusher.flush().await.expect("Flush segment writer failed");
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
        let reader = RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
            .await
            .expect("Error creating segment reader");
        let materializer = LogMaterializer {
            record_segment_reader: Some(reader),
            logs: data,
            curr_offset_id: None,
        };
        let res = materializer
            .materialize()
            .await
            .expect("Error materializing logs");
        let mut res_vec = vec![];
        for (record, _) in res.iter() {
            res_vec.push(record);
        }
        res_vec.sort_by(|x, y| x.merged_user_id_ref().cmp(y.merged_user_id_ref()));
        assert_eq!(1, res_vec.len());
        let emb_1 = res_vec[0];
        assert_eq!(1, emb_1.offset_id);
        assert_eq!("number", emb_1.merged_document_ref().unwrap());
        assert_eq!(&[7.0, 8.0, 9.0], emb_1.merged_embeddings());
        assert_eq!("embedding_id_1", emb_1.merged_user_id_ref());
        let mut res_metadata = HashMap::new();
        res_metadata.insert(
            String::from("hello"),
            MetadataValue::Str(String::from("new_world")),
        );
        assert_eq!(res_metadata, emb_1.merged_metadata());
        assert_eq!(
            MaterializedLogOperation::OverwriteExisting,
            emb_1.final_operation
        );
        // Now write this, read again and validate.
        let segment_writer =
            RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        let mut metadata_writer =
            MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        segment_writer
            .apply_materialized_log_chunk(res.clone())
            .await
            .expect("Error applying materialized log chunk");
        metadata_writer
            .apply_materialized_log_chunk(res.clone())
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .write_to_blockfiles()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        let flusher = segment_writer
            .commit()
            .expect("Commit for segment writer failed");
        record_segment.file_path = flusher.flush().await.expect("Flush segment writer failed");
        let metadata_flusher = metadata_writer
            .commit()
            .expect("Commit for metadata writer failed");
        metadata_segment.file_path = metadata_flusher
            .flush()
            .await
            .expect("Flush metadata segment writer failed");
        // Read.
        let segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Error creating segment reader");
        let all_data = segment_reader
            .get_all_data()
            .await
            .expect("Get all data failed");
        assert_eq!(all_data.len(), 1);
        let record = &all_data[0];
        assert_eq!(record.id, "embedding_id_1");
        assert_eq!(record.document, Some("number"));
        assert_eq!(record.embedding, &[7.0, 8.0, 9.0]);
        assert_eq!(record.metadata, Some(res_metadata));
        // Search by metadata filter.
        let metadata_segment_reader =
            MetadataSegmentReader::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Metadata segment reader construction failed");
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: String::from("hello"),
            comparison: WhereComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str(String::from("new_world")),
            ),
        });
        let res = metadata_segment_reader
            .query(Some(&where_clause), None, None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first(), Some(&(1_usize)));
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: String::from("hello"),
            comparison: WhereComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str(String::from("world")),
            ),
        });
        let res = metadata_segment_reader
            .query(Some(&where_clause), None, None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 0);
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: String::from("bye"),
            comparison: WhereComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str(String::from("world")),
            ),
        });
        let res = metadata_segment_reader
            .query(Some(&where_clause), None, None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 0);
        let where_document_clause =
            Where::DirectWhereDocumentComparison(DirectDocumentComparison {
                document: String::from("number"),
                operator: chroma_types::DocumentOperator::Contains,
            });
        let res = metadata_segment_reader
            .query(None, Some(&where_document_clause), None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first(), Some(&(1_usize)));
        let where_document_clause =
            Where::DirectWhereDocumentComparison(DirectDocumentComparison {
                document: String::from("doc1"),
                operator: chroma_types::DocumentOperator::Contains,
            });
        let res = metadata_segment_reader
            .query(None, Some(&where_document_clause), None, 0, 0)
            .await
            .expect("Metadata segment query failed")
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
        let materializer = LogMaterializer {
            record_segment_reader: Some(reader),
            logs: data,
            curr_offset_id: None,
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
                assert!(log.data_record.is_none());
                assert_eq!("doc3", log.final_document.unwrap());
                assert_eq!(vec![7.0, 8.0, 9.0], log.final_embedding.unwrap());
                assert_eq!(3, log.offset_id);
                assert_eq!(MaterializedLogOperation::AddNew, log.final_operation);
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
                        panic!("Not expecting any other key");
                    }
                }
                assert_eq!(hello_found, 1);
                assert_eq!(hello_again_found, 1);
            } else if log.data_record.as_ref().unwrap().id == "embedding_id_2" {
                id2_found += 1;
                assert_eq!(
                    MaterializedLogOperation::DeleteExisting,
                    log.final_operation
                );
                assert_eq!(2, log.offset_id);
                assert_eq!(None, log.final_document);
                assert_eq!(None, log.final_embedding);
                assert_eq!(None, log.user_id);
                assert_eq!(None, log.metadata_to_be_merged);
                assert!(log.data_record.is_some());
            } else if log.data_record.as_ref().unwrap().id == "embedding_id_1" {
                id1_found += 1;
                assert_eq!(
                    MaterializedLogOperation::UpdateExisting,
                    log.final_operation
                );
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
                        panic!("Not expecting any other key");
                    }
                }
                assert_eq!(hello_found, 1);
                assert_eq!(hello_again_found, 1);
                assert!(log.data_record.is_some());
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
                assert!(data
                    .metadata
                    .clone()
                    .expect("Metadata is empty")
                    .contains_key("hello"),);
                assert_eq!(
                    data.metadata
                        .clone()
                        .expect("Metadata is empty")
                        .get("hello"),
                    Some(&MetadataValue::Str(String::from("new_world")))
                );
                assert!(data
                    .metadata
                    .clone()
                    .expect("Metadata is empty")
                    .contains_key("bye"),);
                assert_eq!(
                    data.metadata.clone().expect("Metadata is empty").get("bye"),
                    Some(&MetadataValue::Str(String::from("world")))
                );
                assert!(data
                    .metadata
                    .clone()
                    .expect("Metadata is empty")
                    .contains_key("hello_again"),);
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
                assert!(data
                    .metadata
                    .clone()
                    .expect("Metadata is empty")
                    .contains_key("hello"),);
                assert_eq!(
                    data.metadata
                        .clone()
                        .expect("Metadata is empty")
                        .get("hello"),
                    Some(&MetadataValue::Str(String::from("new_world")))
                );
                assert!(data
                    .metadata
                    .clone()
                    .expect("Metadata is empty")
                    .contains_key("hello_again"),);
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
