use super::blockfile_record::ApplyMaterializedLogError;
use super::blockfile_record::RecordSegmentReader;
use super::types::MaterializeLogsResult;
use chroma_blockstore::provider::{BlockfileProvider, CreateError, OpenError};
use chroma_blockstore::BlockfileWriterOptions;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::fulltext::types::{
    DocumentMutation, FullTextIndexError, FullTextIndexFlusher, FullTextIndexReader,
    FullTextIndexWriter,
};
use chroma_index::metadata::types::{
    MetadataIndexError, MetadataIndexFlusher, MetadataIndexReader, MetadataIndexWriter,
};
use chroma_types::SegmentType;
use chroma_types::BOOL_METADATA;
use chroma_types::F32_METADATA;
use chroma_types::FULL_TEXT_PLS;
use chroma_types::STRING_METADATA;
use chroma_types::U32_METADATA;
use chroma_types::{MaterializedLogOperation, MetadataValue, Segment, SegmentUuid};
use core::panic;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use tantivy::tokenizer::NgramTokenizer;
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone)]
pub struct MetadataSegmentWriter<'me> {
    pub(crate) full_text_index_writer: Option<FullTextIndexWriter>,
    pub(crate) string_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) bool_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) f32_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) u32_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub id: SegmentUuid,
}

impl Debug for MetadataSegmentWriter<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MetadataSegmentWriter")
    }
}

#[derive(Debug, Error)]
pub enum MetadataSegmentError {
    #[error("Invalid segment type")]
    InvalidSegmentType,
    // TODO turn this into index creation error
    #[error("Failed to create full text index writer")]
    FullTextIndexWriterError(#[from] FullTextIndexError),
    #[error("Blockfile creation error")]
    BlockfileError(#[from] CreateError),
    #[error("Blockfile open error")]
    BlockfileOpenError(#[from] OpenError),
    #[error("Only one of posting lists and frequencies files found")]
    FullTextIndexFilesIntegrityError,
    #[error("Incorrect number of files")]
    IncorrectNumberOfFiles,
    #[error("Missing file {0}")]
    MissingFile(String),
    #[error("Count not parse UUID {0}")]
    UuidParseError(String),
    #[error("No writer found")]
    NoWriter,
    #[error("Path vector exists but is empty?")]
    EmptyPathVector,
    #[error("Failed to write to blockfile")]
    BlockfileWriteError,
    #[error("Limit and offset are not currently supported")]
    LimitOffsetNotSupported,
    #[error("Could not query metadata index {0}")]
    MetadataIndexQueryError(#[from] MetadataIndexError),
}

impl ChromaError for MetadataSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            MetadataSegmentError::InvalidSegmentType => ErrorCodes::Internal,
            MetadataSegmentError::FullTextIndexWriterError(e) => e.code(),
            MetadataSegmentError::BlockfileError(e) => e.code(),
            MetadataSegmentError::BlockfileOpenError(e) => e.code(),
            MetadataSegmentError::FullTextIndexFilesIntegrityError => ErrorCodes::Internal,
            MetadataSegmentError::IncorrectNumberOfFiles => ErrorCodes::Internal,
            MetadataSegmentError::MissingFile(_) => ErrorCodes::Internal,
            MetadataSegmentError::UuidParseError(_) => ErrorCodes::Internal,
            MetadataSegmentError::NoWriter => ErrorCodes::Internal,
            MetadataSegmentError::EmptyPathVector => ErrorCodes::Internal,
            MetadataSegmentError::BlockfileWriteError => ErrorCodes::Internal,
            MetadataSegmentError::LimitOffsetNotSupported => ErrorCodes::Internal,
            MetadataSegmentError::MetadataIndexQueryError(_) => ErrorCodes::Internal,
        }
    }
}

impl<'me> MetadataSegmentWriter<'me> {
    pub async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<MetadataSegmentWriter<'me>, MetadataSegmentError> {
        if segment.r#type != SegmentType::BlockfileMetadata {
            return Err(MetadataSegmentError::InvalidSegmentType);
        }
        let pls_writer = match segment.file_path.get(FULL_TEXT_PLS) {
            Some(pls_path) => match pls_path.first() {
                Some(pls_uuid) => {
                    let pls_uuid = match Uuid::parse_str(pls_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(pls_uuid.to_string()))
                        }
                    };

                    blockfile_provider
                        .write::<u32, Vec<u32>>(
                            BlockfileWriterOptions::new()
                                .fork(pls_uuid)
                                .ordered_mutations(),
                        )
                        .await
                        .map_err(|e| MetadataSegmentError::BlockfileError(*e))?
                }
                None => return Err(MetadataSegmentError::EmptyPathVector),
            },
            None => match blockfile_provider
                .write::<u32, Vec<u32>>(BlockfileWriterOptions::new().ordered_mutations())
                .await
            {
                Ok(writer) => writer,
                Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
            },
        };

        let full_text_writer_tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
        let full_text_index_writer =
            FullTextIndexWriter::new(pls_writer, full_text_writer_tokenizer);

        let (string_metadata_writer, string_metadata_index_reader) =
            match segment.file_path.get(STRING_METADATA) {
                Some(string_metadata_path) => match string_metadata_path.first() {
                    Some(string_metadata_uuid) => {
                        let string_metadata_uuid = match Uuid::parse_str(string_metadata_uuid) {
                            Ok(uuid) => uuid,
                            Err(_) => {
                                return Err(MetadataSegmentError::UuidParseError(
                                    string_metadata_uuid.to_string(),
                                ))
                            }
                        };
                        let string_metadata_writer = match blockfile_provider
                            .write::<&str, RoaringBitmap>(
                                BlockfileWriterOptions::new().fork(string_metadata_uuid),
                            )
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        };
                        let string_metadata_index_reader = match blockfile_provider
                            .read::<&str, RoaringBitmap>(&string_metadata_uuid)
                            .await
                        {
                            Ok(reader) => MetadataIndexReader::new_string(reader),
                            Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                        };
                        (string_metadata_writer, Some(string_metadata_index_reader))
                    }
                    None => return Err(MetadataSegmentError::EmptyPathVector),
                },
                None => match blockfile_provider
                    .write::<&str, RoaringBitmap>(BlockfileWriterOptions::new())
                    .await
                {
                    Ok(writer) => (writer, None),
                    Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                },
            };
        let string_metadata_index_writer =
            MetadataIndexWriter::new_string(string_metadata_writer, string_metadata_index_reader);

        let (bool_metadata_writer, bool_metadata_index_reader) =
            match segment.file_path.get(BOOL_METADATA) {
                Some(bool_metadata_path) => match bool_metadata_path.first() {
                    Some(bool_metadata_uuid) => {
                        let bool_metadata_uuid = match Uuid::parse_str(bool_metadata_uuid) {
                            Ok(uuid) => uuid,
                            Err(_) => {
                                return Err(MetadataSegmentError::UuidParseError(
                                    bool_metadata_uuid.to_string(),
                                ))
                            }
                        };
                        let bool_metadata_writer = match blockfile_provider
                            .write::<bool, RoaringBitmap>(
                                BlockfileWriterOptions::new().fork(bool_metadata_uuid),
                            )
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        };
                        let bool_metadata_index_writer = match blockfile_provider
                            .read::<bool, RoaringBitmap>(&bool_metadata_uuid)
                            .await
                        {
                            Ok(reader) => MetadataIndexReader::new_bool(reader),
                            Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                        };
                        (bool_metadata_writer, Some(bool_metadata_index_writer))
                    }
                    None => return Err(MetadataSegmentError::EmptyPathVector),
                },
                None => match blockfile_provider
                    .write::<bool, RoaringBitmap>(BlockfileWriterOptions::default())
                    .await
                {
                    Ok(writer) => (writer, None),
                    Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                },
            };
        let bool_metadata_index_writer =
            MetadataIndexWriter::new_bool(bool_metadata_writer, bool_metadata_index_reader);

        let (f32_metadata_writer, f32_metadata_index_reader) =
            match segment.file_path.get(F32_METADATA) {
                Some(f32_metadata_path) => match f32_metadata_path.first() {
                    Some(f32_metadata_uuid) => {
                        let f32_metadata_uuid = match Uuid::parse_str(f32_metadata_uuid) {
                            Ok(uuid) => uuid,
                            Err(_) => {
                                return Err(MetadataSegmentError::UuidParseError(
                                    f32_metadata_uuid.to_string(),
                                ))
                            }
                        };
                        let f32_metadata_writer = match blockfile_provider
                            .write::<f32, RoaringBitmap>(
                                BlockfileWriterOptions::new().fork(f32_metadata_uuid),
                            )
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        };
                        let f32_metadata_index_reader = match blockfile_provider
                            .read::<f32, RoaringBitmap>(&f32_metadata_uuid)
                            .await
                        {
                            Ok(reader) => MetadataIndexReader::new_f32(reader),
                            Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                        };
                        (f32_metadata_writer, Some(f32_metadata_index_reader))
                    }
                    None => return Err(MetadataSegmentError::EmptyPathVector),
                },
                None => match blockfile_provider
                    .write::<f32, RoaringBitmap>(BlockfileWriterOptions::default())
                    .await
                {
                    Ok(writer) => (writer, None),
                    Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                },
            };
        let f32_metadata_index_writer =
            MetadataIndexWriter::new_f32(f32_metadata_writer, f32_metadata_index_reader);

        let (u32_metadata_writer, u32_metadata_index_reader) =
            match segment.file_path.get(U32_METADATA) {
                Some(u32_metadata_path) => match u32_metadata_path.first() {
                    Some(u32_metadata_uuid) => {
                        let u32_metadata_uuid = match Uuid::parse_str(u32_metadata_uuid) {
                            Ok(uuid) => uuid,
                            Err(_) => {
                                return Err(MetadataSegmentError::UuidParseError(
                                    u32_metadata_uuid.to_string(),
                                ))
                            }
                        };
                        let u32_metadata_writer = match blockfile_provider
                            .write::<u32, RoaringBitmap>(
                                BlockfileWriterOptions::new().fork(u32_metadata_uuid),
                            )
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        };
                        let u32_metadata_index_reader = match blockfile_provider
                            .read::<u32, RoaringBitmap>(&u32_metadata_uuid)
                            .await
                        {
                            Ok(reader) => MetadataIndexReader::new_u32(reader),
                            Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                        };
                        (u32_metadata_writer, Some(u32_metadata_index_reader))
                    }
                    None => return Err(MetadataSegmentError::EmptyPathVector),
                },
                None => match blockfile_provider
                    .write::<u32, RoaringBitmap>(BlockfileWriterOptions::default())
                    .await
                {
                    Ok(writer) => (writer, None),
                    Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                },
            };
        let u32_metadata_index_writer =
            MetadataIndexWriter::new_u32(u32_metadata_writer, u32_metadata_index_reader);

        Ok(MetadataSegmentWriter {
            full_text_index_writer: Some(full_text_index_writer),
            string_metadata_index_writer: Some(string_metadata_index_writer),
            bool_metadata_index_writer: Some(bool_metadata_index_writer),
            f32_metadata_index_writer: Some(f32_metadata_index_writer),
            u32_metadata_index_writer: Some(u32_metadata_index_writer),
            id: segment.id,
        })
    }

    pub(crate) async fn set_metadata(
        &self,
        prefix: &str,
        key: &MetadataValue,
        offset_id: u32,
    ) -> Result<(), MetadataIndexError> {
        match key {
            MetadataValue::Str(v) => {
                match &self.string_metadata_index_writer {
                    Some(writer) => {
                        match writer.set(prefix, v.as_str(), offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error inserting into str metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. String metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::Int(v) => {
                match &self.u32_metadata_index_writer {
                    Some(writer) => {
                        match writer.set(prefix, *v as u32, offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error inserting into u32 metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. u32 metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::Float(v) => {
                match &self.f32_metadata_index_writer {
                    Some(writer) => {
                        match writer.set(prefix, *v as f32, offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error inserting into f32 metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. f32 metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::Bool(v) => {
                match &self.bool_metadata_index_writer {
                    Some(writer) => {
                        match writer.set(prefix, *v, offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error inserting into bool metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. bool metadata index writer should be set for metadata segment"),
                }
            }
        }
    }

    pub(crate) async fn delete_metadata(
        &self,
        prefix: &str,
        key: &MetadataValue,
        offset_id: u32,
    ) -> Result<(), MetadataIndexError> {
        match key {
            MetadataValue::Str(v) => {
                match &self.string_metadata_index_writer {
                    Some(writer) => {
                        match writer.delete(prefix, v.as_str(), offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error deleting from str metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. String metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::Int(v) => {
                match &self.u32_metadata_index_writer {
                    Some(writer) => {
                        match writer.delete(prefix, *v as u32, offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error deleting from u32 metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. u32 metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::Float(v) => {
                match &self.f32_metadata_index_writer {
                    Some(writer) => {
                        match writer.delete(prefix, *v as f32, offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error deleting from f32 metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. f32 metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::Bool(v) => {
                match &self.bool_metadata_index_writer {
                    Some(writer) => {
                        match writer.delete(prefix, *v, offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error deleting from bool metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. bool metadata index writer should be set for metadata segment"),
                }
            }
        }
    }

    pub(crate) async fn update_metadata(
        &self,
        key: &str,
        old_value: &MetadataValue,
        new_value: &MetadataValue,
        offset_id: u32,
    ) -> Result<(), MetadataSegmentError> {
        // Delete old value.
        self.delete_metadata(key, old_value, offset_id).await?;
        // Insert new value.
        Ok(self.set_metadata(key, new_value, offset_id).await?)
    }

    pub async fn apply_materialized_log_chunk(
        &self,
        record_segment_reader: &Option<RecordSegmentReader<'_>>,
        materialized: &MaterializeLogsResult,
    ) -> Result<(), ApplyMaterializedLogError> {
        let mut count = 0u64;

        let mut full_text_writer_batch = vec![];
        for record in materialized {
            let record = record
                .hydrate(record_segment_reader.as_ref())
                .await
                .map_err(ApplyMaterializedLogError::Materialization)?;
            let offset_id = record.get_offset_id();
            let old_document = record.document_ref_from_segment();
            let new_document = record.document_ref_from_log();

            if matches!(
                record.get_operation(),
                MaterializedLogOperation::UpdateExisting
            ) && new_document.is_none()
            {
                continue;
            }

            match (old_document, new_document) {
                (None, None) => continue,
                (Some(old_document), Some(new_document)) => {
                    full_text_writer_batch.push(DocumentMutation::Update {
                        offset_id,
                        old_document,
                        new_document,
                    })
                }
                (None, Some(new_document)) => {
                    full_text_writer_batch.push(DocumentMutation::Create {
                        offset_id,
                        new_document,
                    })
                }
                (Some(old_document), None) => {
                    full_text_writer_batch.push(DocumentMutation::Delete {
                        offset_id,
                        old_document,
                    })
                }
            }
        }

        self.full_text_index_writer
            .as_ref()
            .unwrap()
            .handle_batch(full_text_writer_batch)
            .map_err(ApplyMaterializedLogError::FullTextIndex)?;

        for record in materialized {
            count += 1;

            let record = record
                .hydrate(record_segment_reader.as_ref())
                .await
                .map_err(ApplyMaterializedLogError::Materialization)?;
            let segment_offset_id = record.get_offset_id();
            match record.get_operation() {
                MaterializedLogOperation::AddNew => {
                    // We can ignore record.0.metadata_to_be_deleted
                    // for fresh adds. TODO on whether to propagate error.
                    if let Some(metadata) = record.get_metadata_to_be_merged() {
                            for (key, value) in metadata.iter() {
                                match self.set_metadata(key, value, segment_offset_id).await {
                                    Ok(()) => {}
                                    Err(_) => {
                                        return Err(ApplyMaterializedLogError::BlockfileSet);
                                    }
                                }
                            }
                        }

                }
                MaterializedLogOperation::DeleteExisting => match record.get_data_record() {
                    Some(data_record) => {
                        if let Some(metadata) = &data_record.metadata {
                                for (key, value) in metadata.iter() {
                                    match self.delete_metadata(key, value, segment_offset_id).await
                                    {
                                        Ok(()) => {}
                                        Err(_) => {
                                            return Err(
                                                ApplyMaterializedLogError::BlockfileDelete,
                                            );
                                        }
                                    }
                                }
                            }

                    }
                    None => panic!("Invariant violation. Data record should be set by materializer in case of Deletes")
                },
                MaterializedLogOperation::UpdateExisting => {
                    let metadata_delta = record.compute_metadata_delta();
                    // Metadata updates.
                    for (update_key, (old_value, new_value)) in metadata_delta.metadata_to_update {
                        match self
                            .update_metadata(update_key, old_value, new_value, segment_offset_id)
                            .await
                        {
                            Ok(()) => {}
                            Err(_) => {
                                return Err(ApplyMaterializedLogError::BlockfileUpdate);
                            }
                        }
                    }
                    // Metadata inserts.
                    for (insert_key, new_value) in metadata_delta.metadata_to_insert {
                        match self
                            .set_metadata(insert_key, new_value, segment_offset_id)
                            .await
                        {
                            Ok(()) => {}
                            Err(_) => {
                                return Err(ApplyMaterializedLogError::BlockfileSet);
                            }
                        }
                    }
                    // Metadata deletes.
                    for (delete_key, old_value) in metadata_delta.metadata_to_delete {
                        match self
                            .delete_metadata(delete_key, old_value, segment_offset_id)
                            .await
                        {
                            Ok(()) => {}
                            Err(_) => {
                                return Err(ApplyMaterializedLogError::BlockfileDelete);
                            }
                        }
                    }

                }
                MaterializedLogOperation::OverwriteExisting => {
                    // Delete existing.
                    match record.get_data_record() {
                        Some(data_record) => {
                            if let Some(metadata) = &data_record.metadata {
                                    for (key, value) in metadata.iter() {
                                        match self.delete_metadata(key, value, segment_offset_id).await
                                        {
                                            Ok(()) => {}
                                            Err(_) => {
                                                return Err(
                                                    ApplyMaterializedLogError::BlockfileDelete,
                                                );
                                            }
                                        }
                                    }
                                }

                        },
                        None => panic!("Invariant violation. Data record should be set by materializer in case of Deletes")
                    };
                    // Add new.
                    if let Some(metadata) = record.get_metadata_to_be_merged() {
                            for (key, value) in metadata.iter() {
                                match self.set_metadata(key, value, segment_offset_id).await {
                                    Ok(()) => {}
                                    Err(_) => {
                                        return Err(ApplyMaterializedLogError::BlockfileSet);
                                    }
                                }
                            }
                        }

                },
                MaterializedLogOperation::Initial => panic!("Not expected mat records in the initial state")
            }
        }
        tracing::info!("Applied {} records to metadata segment", count,);
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<(), Box<dyn ChromaError>> {
        let mut full_text_index_writer = match self.full_text_index_writer.take() {
            Some(writer) => writer,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };
        let res = full_text_index_writer.write_to_blockfiles().await;
        self.full_text_index_writer = Some(full_text_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(Box::new(MetadataSegmentError::BlockfileWriteError)),
        }

        let mut string_metadata_index_writer = match self.string_metadata_index_writer.take() {
            Some(writer) => writer,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };
        let res = string_metadata_index_writer.write_to_blockfile().await;
        self.string_metadata_index_writer = Some(string_metadata_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(Box::new(MetadataSegmentError::BlockfileWriteError)),
        }

        let mut bool_metadata_index_writer = match self.bool_metadata_index_writer.take() {
            Some(writer) => writer,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };
        let res = bool_metadata_index_writer.write_to_blockfile().await;
        self.bool_metadata_index_writer = Some(bool_metadata_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(Box::new(MetadataSegmentError::BlockfileWriteError)),
        }

        let mut f32_metadata_index_writer = match self.f32_metadata_index_writer.take() {
            Some(writer) => writer,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };
        let res = f32_metadata_index_writer.write_to_blockfile().await;
        self.f32_metadata_index_writer = Some(f32_metadata_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(Box::new(MetadataSegmentError::BlockfileWriteError)),
        }

        let mut u32_metadata_index_writer = match self.u32_metadata_index_writer.take() {
            Some(writer) => writer,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };
        let res = u32_metadata_index_writer.write_to_blockfile().await;
        self.u32_metadata_index_writer = Some(u32_metadata_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(Box::new(MetadataSegmentError::BlockfileWriteError)),
        }

        Ok(())
    }

    pub async fn commit(self) -> Result<MetadataSegmentFlusher, Box<dyn ChromaError>> {
        let full_text_flusher = match self.full_text_index_writer {
            Some(flusher) => match flusher.commit().await {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let string_metadata_flusher = match self.string_metadata_index_writer {
            Some(flusher) => match flusher.commit().await {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let bool_metadata_flusher = match self.bool_metadata_index_writer {
            Some(flusher) => match flusher.commit().await {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let f32_metadata_flusher = match self.f32_metadata_index_writer {
            Some(flusher) => match flusher.commit().await {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let u32_metadata_flusher = match self.u32_metadata_index_writer {
            Some(flusher) => match flusher.commit().await {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        Ok(MetadataSegmentFlusher {
            id: self.id,
            full_text_index_flusher: full_text_flusher,
            string_metadata_index_flusher: string_metadata_flusher,
            bool_metadata_index_flusher: bool_metadata_flusher,
            f32_metadata_index_flusher: f32_metadata_flusher,
            u32_metadata_index_flusher: u32_metadata_flusher,
        })
    }
}

pub struct MetadataSegmentFlusher {
    pub id: SegmentUuid,
    pub(crate) full_text_index_flusher: FullTextIndexFlusher,
    pub(crate) string_metadata_index_flusher: MetadataIndexFlusher,
    pub(crate) bool_metadata_index_flusher: MetadataIndexFlusher,
    pub(crate) f32_metadata_index_flusher: MetadataIndexFlusher,
    pub(crate) u32_metadata_index_flusher: MetadataIndexFlusher,
}

impl Debug for MetadataSegmentFlusher {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataSegmentFlusher")
            .field("id", &self.id)
            .finish()
    }
}

impl MetadataSegmentFlusher {
    pub async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let full_text_pls_id = self.full_text_index_flusher.pls_id();
        let string_metadata_id = self.string_metadata_index_flusher.id();
        let bool_metadata_id = self.bool_metadata_index_flusher.id();
        let f32_metadata_id = self.f32_metadata_index_flusher.id();
        let u32_metadata_id = self.u32_metadata_index_flusher.id();

        let mut flushed = HashMap::new();

        match self.full_text_index_flusher.flush().await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            FULL_TEXT_PLS.to_string(),
            vec![full_text_pls_id.to_string()],
        );

        match self.bool_metadata_index_flusher.flush().await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            BOOL_METADATA.to_string(),
            vec![bool_metadata_id.to_string()],
        );

        match self.f32_metadata_index_flusher.flush().await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(F32_METADATA.to_string(), vec![f32_metadata_id.to_string()]);

        match self.u32_metadata_index_flusher.flush().await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(U32_METADATA.to_string(), vec![u32_metadata_id.to_string()]);

        match self.string_metadata_index_flusher.flush().await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            STRING_METADATA.to_string(),
            vec![string_metadata_id.to_string()],
        );

        Ok(flushed)
    }
}

pub struct MetadataSegmentReader<'me> {
    pub full_text_index_reader: Option<FullTextIndexReader<'me>>,
    pub string_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    pub bool_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    pub f32_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    pub u32_metadata_index_reader: Option<MetadataIndexReader<'me>>,
}

impl MetadataSegmentReader<'_> {
    pub async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Self, MetadataSegmentError> {
        if segment.r#type != SegmentType::BlockfileMetadata {
            return Err(MetadataSegmentError::InvalidSegmentType);
        }

        let pls_reader = match segment.file_path.get(FULL_TEXT_PLS) {
            Some(pls_path) => match pls_path.first() {
                Some(pls_uuid) => {
                    let pls_uuid = match Uuid::parse_str(pls_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(pls_uuid.to_string()))
                        }
                    };

                    match blockfile_provider.read::<u32, &[u32]>(&pls_uuid).await {
                        Ok(reader) => Some(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    }
                }
                None => None,
            },
            None => None,
        };

        let full_text_index_reader = pls_reader.map(|reader| {
            let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
            FullTextIndexReader::new(reader, tokenizer)
        });

        let string_metadata_reader = match segment.file_path.get(STRING_METADATA) {
            Some(string_metadata_path) => match string_metadata_path.first() {
                Some(string_metadata_uuid) => {
                    let string_metadata_uuid = match Uuid::parse_str(string_metadata_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(
                                string_metadata_uuid.to_string(),
                            ))
                        }
                    };
                    match blockfile_provider
                        .read::<&str, RoaringBitmap>(&string_metadata_uuid)
                        .await
                    {
                        Ok(reader) => Some(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    }
                }
                None => None,
            },
            None => None,
        };
        let string_metadata_index_reader =
            string_metadata_reader.map(MetadataIndexReader::new_string);

        let bool_metadata_reader = match segment.file_path.get(BOOL_METADATA) {
            Some(bool_metadata_path) => match bool_metadata_path.first() {
                Some(bool_metadata_uuid) => {
                    let bool_metadata_uuid = match Uuid::parse_str(bool_metadata_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(
                                bool_metadata_uuid.to_string(),
                            ))
                        }
                    };
                    match blockfile_provider
                        .read::<bool, RoaringBitmap>(&bool_metadata_uuid)
                        .await
                    {
                        Ok(reader) => Some(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    }
                }
                None => None,
            },
            None => None,
        };
        let bool_metadata_index_reader = bool_metadata_reader.map(MetadataIndexReader::new_bool);
        let u32_metadata_reader = match segment.file_path.get(U32_METADATA) {
            Some(u32_metadata_path) => match u32_metadata_path.first() {
                Some(u32_metadata_uuid) => {
                    let u32_metadata_uuid = match Uuid::parse_str(u32_metadata_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(
                                u32_metadata_uuid.to_string(),
                            ))
                        }
                    };
                    match blockfile_provider
                        .read::<u32, RoaringBitmap>(&u32_metadata_uuid)
                        .await
                    {
                        Ok(reader) => Some(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    }
                }
                None => None,
            },
            None => None,
        };
        let u32_metadata_index_reader = u32_metadata_reader.map(MetadataIndexReader::new_u32);
        let f32_metadata_reader = match segment.file_path.get(F32_METADATA) {
            Some(f32_metadata_path) => match f32_metadata_path.first() {
                Some(f32_metadata_uuid) => {
                    let f32_metadata_uuid = match Uuid::parse_str(f32_metadata_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(
                                f32_metadata_uuid.to_string(),
                            ))
                        }
                    };
                    match blockfile_provider
                        .read::<f32, RoaringBitmap>(&f32_metadata_uuid)
                        .await
                    {
                        Ok(reader) => Some(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    }
                }
                None => None,
            },
            None => None,
        };
        let f32_metadata_index_reader = f32_metadata_reader.map(MetadataIndexReader::new_f32);

        Ok(MetadataSegmentReader {
            full_text_index_reader,
            string_metadata_index_reader,
            bool_metadata_index_reader,
            f32_metadata_index_reader,
            u32_metadata_index_reader,
        })
    }
}

#[cfg(test)]
mod test {

    use crate::{
        blockfile_metadata::{MetadataSegmentReader, MetadataSegmentWriter},
        blockfile_record::{
            RecordSegmentReader, RecordSegmentReaderCreationError, RecordSegmentWriter,
        },
        test::TestDistributedSegment,
        types::materialize_logs,
    };
    use chroma_blockstore::{
        arrow::{config::TEST_MAX_BLOCK_SIZE_BYTES, provider::ArrowBlockfileProvider},
        provider::BlockfileProvider,
    };
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{
        regex::literal_expr::{LiteralExpr, NgramLiteralProvider},
        strategies::{ArbitraryChromaRegexTestDocumentsParameters, ChromaRegexTestDocuments},
        Chunk, CollectionUuid, LogRecord, MetadataValue, Operation, OperationRecord,
        ScalarEncoding, SegmentUuid, UpdateMetadataValue,
    };
    use proptest::prelude::any_with;
    use roaring::RoaringBitmap;
    use std::{collections::HashMap, str::FromStr};
    use tokio::runtime::Runtime;

    #[tokio::test]
    async fn empty_blocks() {
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
                            RecordSegmentReaderCreationError::DataRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderCreationError::UserRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                        }
                    }
                };
            let mat_records = materialize_logs(&record_segment_reader, data, None)
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
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
        let data = vec![
            LogRecord {
                log_offset: 3,
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
                log_offset: 4,
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
        let record_segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Reader should be initialized by now");
        let segment_writer =
            RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        let mut metadata_writer =
            MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(&some_reader, data, None)
            .await
            .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to record segment failed");
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
        // No data should be present.
        let record_segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Record segment reader should be initialized by now");
        let res = record_segment_reader
            .get_all_data()
            .await
            .expect("Error getting all data from record segment");
        assert_eq!(res.len(), 0);
        // Add a few records and they should exist.
        let data = vec![
            LogRecord {
                log_offset: 5,
                record: OperationRecord {
                    id: "embedding_id_3".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: None,
                    document: Some(String::from("This is a document about cats.")),
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 6,
                record: OperationRecord {
                    id: "embedding_id_4".to_string(),
                    embedding: Some(vec![4.0, 5.0, 6.0]),
                    encoding: None,
                    metadata: None,
                    document: Some(String::from("This is a document about dogs.")),
                    operation: Operation::Add,
                },
            },
        ];

        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let record_segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Reader should be initialized by now");
        let segment_writer =
            RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        let mut metadata_writer =
            MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(&some_reader, data, None)
            .await
            .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to record segment failed");
        let record_flusher = segment_writer
            .commit()
            .await
            .expect("Commit for segment writer failed");
        let count = record_flusher.count();
        assert_eq!(count, 2_u64);
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
        // No data should be present.
        let record_segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Record segment reader should be initialized by now");
        let res = record_segment_reader
            .get_all_data()
            .await
            .expect("Error getting all data from record segment");
        assert_eq!(res.len(), 2);
    }

    #[tokio::test]
    async fn metadata_update_same_key_different_type() {
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
                            RecordSegmentReaderCreationError::DataRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderCreationError::UserRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                        }
                    }
                };
            let mat_records = materialize_logs(&record_segment_reader, data, None)
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
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
        let mut update_metadata_id1 = HashMap::new();
        update_metadata_id1.insert(
            String::from("hello"),
            UpdateMetadataValue::Str(String::from("new world")),
        );
        let mut update_metadata_id2 = HashMap::new();
        update_metadata_id2.insert(String::from("hello"), UpdateMetadataValue::Float(1.0));
        let data = vec![
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: Some(update_metadata_id1.clone()),
                    document: None,
                    operation: Operation::Update,
                },
            },
            LogRecord {
                log_offset: 4,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: Some(update_metadata_id2.clone()),
                    document: None,
                    operation: Operation::Update,
                },
            },
        ];

        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let record_segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Reader should be initialized by now");
        let segment_writer =
            RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        let mut metadata_writer =
            MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(&some_reader, data, None)
            .await
            .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to record segment failed");
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
        // Search by f32 metadata value first.
        let metadata_segment_reader =
            MetadataSegmentReader::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Metadata segment reader construction failed");
        let res = metadata_segment_reader
            .f32_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("hello", &1.0.into())
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(2));
        let res = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("hello", &"new world".into())
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        // Record segment should also have the updated values.
        let record_segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Reader should be initialized by now");
        let mut res = record_segment_reader
            .get_all_data()
            .await
            .expect("Record segment get all data failed");
        assert_eq!(res.len(), 2);
        res.sort_by(|x, y| x.id.cmp(y.id));
        let mut id1_mt = HashMap::new();
        id1_mt.insert(
            String::from("hello"),
            MetadataValue::Str(String::from("new world")),
        );
        assert_eq!(res.first().as_ref().unwrap().metadata, Some(id1_mt));
        let mut id2_mt = HashMap::new();
        id2_mt.insert(String::from("hello"), MetadataValue::Float(1.0));
        assert_eq!(res.get(1).as_ref().unwrap().metadata, Some(id2_mt));
    }

    #[tokio::test]
    async fn metadata_deletes() {
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
                    document: Some(String::from("This is a document about cats.")),
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
                            RecordSegmentReaderCreationError::DataRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderCreationError::UserRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                        }
                    }
                };
            let mat_records = materialize_logs(&record_segment_reader, data, None)
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
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
        let mut update_metadata_id1 = HashMap::new();
        update_metadata_id1.insert(String::from("hello"), UpdateMetadataValue::None);
        let data = vec![LogRecord {
            log_offset: 2,
            record: OperationRecord {
                id: "embedding_id_1".to_string(),
                embedding: None,
                encoding: None,
                metadata: Some(update_metadata_id1.clone()),
                document: None,
                operation: Operation::Update,
            },
        }];

        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let record_segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Reader should be initialized by now");
        let segment_writer =
            RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        let mut metadata_writer =
            MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(&some_reader, data, None)
            .await
            .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to record segment failed");
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
        // Only one key should be present.
        let metadata_segment_reader =
            MetadataSegmentReader::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Metadata segment reader construction failed");
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
        // Record segment should also have the updated values.
        let record_segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Reader should be initialized by now");
        let mut res = record_segment_reader
            .get_all_data()
            .await
            .expect("Record segment get all data failed");
        assert_eq!(res.len(), 1);
        res.sort_by(|x, y| x.id.cmp(y.id));
        let mut id1_mt = HashMap::new();
        id1_mt.insert(
            String::from("bye"),
            MetadataValue::Str(String::from("world")),
        );
        assert_eq!(res.first().as_ref().unwrap().metadata, Some(id1_mt));
    }

    #[tokio::test]
    async fn document_updates() {
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
            let segment_writer =
                RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                    .await
                    .expect("Error creating segment writer");
            let mut metadata_writer =
                MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                    .await
                    .expect("Error creating segment writer");
            let data = vec![LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: None,
                    document: Some(String::from("hello")),
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
                            RecordSegmentReaderCreationError::DataRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderCreationError::UserRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                        }
                    }
                };
            let mat_records = materialize_logs(&record_segment_reader, data, None)
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
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
        let data = vec![LogRecord {
            log_offset: 2,
            record: OperationRecord {
                id: "embedding_id_1".to_string(),
                embedding: None,
                encoding: None,
                metadata: None,
                document: Some(String::from("bye")),
                operation: Operation::Update,
            },
        }];

        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let record_segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Reader should be initialized by now");
        let segment_writer =
            RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        let mut metadata_writer =
            MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Error creating segment writer");
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(&some_reader, data, None)
            .await
            .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to record segment failed");
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
        // FTS for hello should return empty.
        let metadata_segment_reader =
            MetadataSegmentReader::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Metadata segment reader construction failed");
        let res = metadata_segment_reader
            .full_text_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .search("hello")
            .await
            .unwrap();
        assert_eq!(res.len(), 0);
        // FTS for bye should return the lone document.
        let res = metadata_segment_reader
            .full_text_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .search("bye")
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        // Record segment should also have the updated values.
        let record_segment_reader =
            RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                .await
                .expect("Reader should be initialized by now");
        let mut res = record_segment_reader
            .get_all_data()
            .await
            .expect("Record segment get all data failed");
        assert_eq!(res.len(), 1);
        res.sort_by(|x, y| x.id.cmp(y.id));
        assert_eq!(
            res.first().as_ref().unwrap().document,
            Some(String::from("bye").as_str())
        );
    }

    async fn run_regex_test(test_case: ChromaRegexTestDocuments) {
        let pattern = String::from(test_case.hir.clone());
        let regex = regex::Regex::new(&pattern).unwrap();
        let reference_results = test_case
            .documents
            .iter()
            .enumerate()
            .filter_map(|(id, doc)| regex.is_match(doc).then_some(id as u32))
            .collect::<RoaringBitmap>();
        let logs = test_case
            .documents
            .into_iter()
            .enumerate()
            .map(|(id, doc)| LogRecord {
                log_offset: id as i64,
                record: OperationRecord {
                    id: format!("<{id}>"),
                    embedding: Some(vec![id as f32; 2]),
                    encoding: Some(ScalarEncoding::FLOAT32),
                    metadata: None,
                    document: Some(doc),
                    operation: Operation::Add,
                },
            })
            .collect::<Vec<_>>();
        let mut segments = TestDistributedSegment::new_with_dimension(2);
        segments.compact_log(Chunk::new(logs.into()), 0).await;
        let metadata_segment_reader = MetadataSegmentReader::from_segment(
            &segments.metadata_segment,
            &segments.blockfile_provider,
        )
        .await
        .expect("Metadata segment reader should be constructable");
        let fts_reader = metadata_segment_reader
            .full_text_index_reader
            .as_ref()
            .expect("Full text index reader should be present");
        let literal_expression = LiteralExpr::from(test_case.hir);
        let regex_results = fts_reader
            .match_literal_expression(&literal_expression)
            .await
            .expect("Literal evaluation should not fail");
        if let Some(res) = regex_results {
            assert_eq!(res, reference_results);
        }
    }

    proptest::proptest! {
        #[test]
        fn test_simple_regex(
            test_case in any_with::<ChromaRegexTestDocuments>(ArbitraryChromaRegexTestDocumentsParameters {
                recursive_hir: false,
                total_document_count: 10,
            })
        ) {
            let runtime = Runtime::new().unwrap();
            runtime.block_on(async {
                run_regex_test(test_case).await
            });
        }

        #[test]
        fn test_composite_regex(
            test_case in any_with::<ChromaRegexTestDocuments>(ArbitraryChromaRegexTestDocumentsParameters {
                recursive_hir: true,
                total_document_count: 50,
            })
        ) {
            let runtime = Runtime::new().unwrap();
            runtime.block_on(async {
                run_regex_test(test_case).await
            });
        }
    }
}
