use crate::types::ChromaSegmentFlusher;

use super::blockfile_record::ApplyMaterializedLogError;
use super::blockfile_record::RecordSegmentReader;
use super::types::MaterializeLogsResult;
use chroma_blockstore::arrow::provider::BlockfileReaderOptions;
use chroma_blockstore::provider::{BlockfileProvider, CreateError, OpenError, ReadKey, ReadValue};
use chroma_blockstore::BlockfileReader;
use chroma_blockstore::BlockfileWriterOptions;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::fulltext::types::{
    DocumentMutation, FullTextIndexError, FullTextIndexFlusher, FullTextIndexReader,
    FullTextIndexWriter,
};
use chroma_index::metadata::types::{
    MetadataIndexError, MetadataIndexFlusher, MetadataIndexReader, MetadataIndexWriter,
};
use chroma_index::sparse::reader::SparseReader;
use chroma_index::sparse::types::DEFAULT_BLOCK_SIZE;
use chroma_index::sparse::writer::SparseFlusher;
use chroma_index::sparse::writer::SparseWriter;
use chroma_types::DatabaseUuid;
use chroma_types::Schema;
use chroma_types::SegmentType;
use chroma_types::BOOL_METADATA;
use chroma_types::F32_METADATA;
use chroma_types::FULL_TEXT_PLS;
use chroma_types::SPARSE_MAX;
use chroma_types::SPARSE_OFFSET_VALUE;
use chroma_types::STRING_METADATA;
use chroma_types::U32_METADATA;
use chroma_types::{MaterializedLogOperation, MetadataValue, Segment, SegmentUuid};
use core::panic;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use tantivy::tokenizer::NgramTokenizer;
use thiserror::Error;
use tracing::Instrument;
use tracing::Span;

#[derive(Clone)]
pub struct MetadataSegmentWriter<'me> {
    pub(crate) full_text_index_writer: Option<FullTextIndexWriter>,
    pub(crate) string_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) bool_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) f32_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) u32_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) sparse_index_writer: Option<SparseWriter<'me>>,
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
        tenant: &str,
        database_id: &DatabaseUuid,
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<MetadataSegmentWriter<'me>, MetadataSegmentError> {
        if segment.r#type != SegmentType::BlockfileMetadata {
            return Err(MetadataSegmentError::InvalidSegmentType);
        }
        // NOTE: We hope that all blockfiles of the same collection should live under the same prefix.
        // The implementation below implies all collections in the fork tree share the same prefix for
        // blockfiles. Although this is not a desired behavior, as a temporary fix we create the sparse
        // vector index blockfiles under the same prefix as other blockfiles if they are present.
        let prefix_path =
            if let Some(existing_file_path) = segment.file_path.values().flatten().next() {
                let (existing_prefix, _) = Segment::extract_prefix_and_id(existing_file_path)
                    .map_err(|_| {
                        MetadataSegmentError::UuidParseError(existing_file_path.to_string())
                    })?;
                existing_prefix.to_string()
            } else {
                segment.construct_prefix_path(tenant, database_id)
            };
        let pls_writer = match segment.file_path.get(FULL_TEXT_PLS) {
            Some(pls_paths) => match pls_paths.first() {
                Some(pls_path) => {
                    let (prefix, pls_uuid) = Segment::extract_prefix_and_id(pls_path)
                        .map_err(|_| MetadataSegmentError::UuidParseError(pls_path.to_string()))?;

                    blockfile_provider
                        .write::<u32, Vec<u32>>(
                            BlockfileWriterOptions::new(prefix.to_string())
                                .fork(pls_uuid)
                                .ordered_mutations(),
                        )
                        .await
                        .map_err(|e| MetadataSegmentError::BlockfileError(*e))?
                }
                None => return Err(MetadataSegmentError::EmptyPathVector),
            },
            None => match blockfile_provider
                .write::<u32, Vec<u32>>(
                    BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations(),
                )
                .await
            {
                Ok(writer) => writer,
                Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
            },
        };

        let full_text_writer_tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
        let full_text_index_writer =
            FullTextIndexWriter::new(pls_writer, full_text_writer_tokenizer);

        let (string_metadata_writer, string_metadata_index_reader) = match segment
            .file_path
            .get(STRING_METADATA)
        {
            Some(string_metadata_paths) => match string_metadata_paths.first() {
                Some(string_metadata_path) => {
                    let (prefix, string_metadata_uuid) =
                        Segment::extract_prefix_and_id(string_metadata_path).map_err(|_| {
                            MetadataSegmentError::UuidParseError(string_metadata_path.to_string())
                        })?;
                    let string_metadata_writer = match blockfile_provider
                        .write::<&str, RoaringBitmap>(
                            BlockfileWriterOptions::new(prefix.to_string())
                                .fork(string_metadata_uuid),
                        )
                        .await
                    {
                        Ok(writer) => writer,
                        Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                    };
                    let read_options =
                        BlockfileReaderOptions::new(string_metadata_uuid, prefix.to_string());
                    let string_metadata_index_reader = match blockfile_provider
                        .read::<&str, RoaringBitmap>(read_options)
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
                .write::<&str, RoaringBitmap>(BlockfileWriterOptions::new(prefix_path.clone()))
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
                Some(bool_metadata_paths) => match bool_metadata_paths.first() {
                    Some(bool_metadata_path) => {
                        let (prefix, bool_metadata_uuid) =
                            Segment::extract_prefix_and_id(bool_metadata_path).map_err(|_| {
                                MetadataSegmentError::UuidParseError(bool_metadata_path.to_string())
                            })?;
                        let bool_metadata_writer = match blockfile_provider
                            .write::<bool, RoaringBitmap>(
                                BlockfileWriterOptions::new(prefix.to_string())
                                    .fork(bool_metadata_uuid),
                            )
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        };
                        let read_options =
                            BlockfileReaderOptions::new(bool_metadata_uuid, prefix.to_string());
                        let bool_metadata_index_writer = match blockfile_provider
                            .read::<bool, RoaringBitmap>(read_options)
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
                    .write::<bool, RoaringBitmap>(BlockfileWriterOptions::new(prefix_path.clone()))
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
                Some(f32_metadata_paths) => match f32_metadata_paths.first() {
                    Some(f32_metadata_path) => {
                        let (prefix, f32_metadata_uuid) =
                            Segment::extract_prefix_and_id(f32_metadata_path).map_err(|_| {
                                MetadataSegmentError::UuidParseError(f32_metadata_path.to_string())
                            })?;
                        let f32_metadata_writer = match blockfile_provider
                            .write::<f32, RoaringBitmap>(
                                BlockfileWriterOptions::new(prefix.to_string())
                                    .fork(f32_metadata_uuid),
                            )
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        };
                        let read_options =
                            BlockfileReaderOptions::new(f32_metadata_uuid, prefix.to_string());
                        let f32_metadata_index_reader = match blockfile_provider
                            .read::<f32, RoaringBitmap>(read_options)
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
                    .write::<f32, RoaringBitmap>(BlockfileWriterOptions::new(prefix_path.clone()))
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
                Some(u32_metadata_paths) => match u32_metadata_paths.first() {
                    Some(u32_metadata_path) => {
                        let (prefix, u32_metadata_uuid) =
                            Segment::extract_prefix_and_id(u32_metadata_path).map_err(|_| {
                                MetadataSegmentError::UuidParseError(u32_metadata_path.to_string())
                            })?;
                        let u32_metadata_writer = match blockfile_provider
                            .write::<u32, RoaringBitmap>(
                                BlockfileWriterOptions::new(prefix.to_string())
                                    .fork(u32_metadata_uuid),
                            )
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        };
                        let read_options =
                            BlockfileReaderOptions::new(u32_metadata_uuid, prefix.to_string());
                        let u32_metadata_index_reader = match blockfile_provider
                            .read::<u32, RoaringBitmap>(read_options)
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
                    .write::<u32, RoaringBitmap>(BlockfileWriterOptions::new(prefix_path.clone()))
                    .await
                {
                    Ok(writer) => (writer, None),
                    Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                },
            };
        let u32_metadata_index_writer =
            MetadataIndexWriter::new_u32(u32_metadata_writer, u32_metadata_index_reader);

        let max_file_path = segment
            .file_path
            .get(SPARSE_MAX)
            .and_then(|paths| paths.first());
        let offset_value_file_path = segment
            .file_path
            .get(SPARSE_OFFSET_VALUE)
            .and_then(|paths| paths.first());
        let sparse_index_writer = if let (Some(max_file_path), Some(offset_value_file_path)) =
            (max_file_path, offset_value_file_path)
        {
            let (max_prefix, max_uuid) = Segment::extract_prefix_and_id(max_file_path)
                .map_err(|_| MetadataSegmentError::UuidParseError(max_file_path.to_string()))?;
            let max_reader = blockfile_provider
                .read::<u32, f32>(BlockfileReaderOptions::new(
                    max_uuid,
                    max_prefix.to_string(),
                ))
                .await
                .map_err(|e| MetadataSegmentError::BlockfileOpenError(*e))?;
            let max_writer = blockfile_provider
                .write::<u32, f32>(
                    BlockfileWriterOptions::new(max_prefix.to_string()).fork(max_uuid),
                )
                .await
                .map_err(|e| MetadataSegmentError::BlockfileError(*e))?;
            let (offset_value_prefix, offset_value_uuid) =
                Segment::extract_prefix_and_id(offset_value_file_path).map_err(|_| {
                    MetadataSegmentError::UuidParseError(offset_value_file_path.to_string())
                })?;
            let offset_value_reader = blockfile_provider
                .read::<u32, f32>(BlockfileReaderOptions::new(
                    offset_value_uuid,
                    offset_value_prefix.to_string(),
                ))
                .await
                .map_err(|e| MetadataSegmentError::BlockfileOpenError(*e))?;
            let offset_value_writer = blockfile_provider
                .write::<u32, f32>(
                    BlockfileWriterOptions::new(offset_value_prefix.to_string())
                        .fork(offset_value_uuid),
                )
                .await
                .map_err(|e| MetadataSegmentError::BlockfileError(*e))?;
            Some(SparseWriter::new(
                DEFAULT_BLOCK_SIZE,
                max_writer,
                offset_value_writer,
                Some(SparseReader::new(max_reader, offset_value_reader)),
            ))
        } else {
            let max_writer = blockfile_provider
                .write::<u32, f32>(BlockfileWriterOptions::new(prefix_path.clone()))
                .await
                .map_err(|e| MetadataSegmentError::BlockfileError(*e))?;
            let offset_value_writer = blockfile_provider
                .write::<u32, f32>(BlockfileWriterOptions::new(prefix_path.clone()))
                .await
                .map_err(|e| MetadataSegmentError::BlockfileError(*e))?;
            Some(SparseWriter::new(
                DEFAULT_BLOCK_SIZE,
                max_writer,
                offset_value_writer,
                None,
            ))
        };

        Ok(MetadataSegmentWriter {
            full_text_index_writer: Some(full_text_index_writer),
            string_metadata_index_writer: Some(string_metadata_index_writer),
            bool_metadata_index_writer: Some(bool_metadata_index_writer),
            f32_metadata_index_writer: Some(f32_metadata_index_writer),
            u32_metadata_index_writer: Some(u32_metadata_index_writer),
            sparse_index_writer,
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
            MetadataValue::SparseVector(offset_value) => {
                match &self.sparse_index_writer {
                    Some(writer) => {
                        writer.set(offset_id, offset_value.iter()).await;
                        Ok(())
                    }
                    None => panic!("Invariant violation. sparse index writer should be set for metadata segment"),
                }
            },
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
            MetadataValue::SparseVector(offset_value) => match &self.sparse_index_writer {
                Some(writer) => {
                    writer.delete(offset_id, offset_value.indices.iter().cloned()).await;
                    Ok(())
                }
                    None => panic!("Invariant violation. sparse index writer should be set for metadata segment"),
            },
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
        schema: Option<Schema>,
    ) -> Result<Option<Schema>, ApplyMaterializedLogError> {
        let mut count = 0u64;
        let mut schema = schema;
        let mut schema_modified = false;

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
                            if let Some(schema_mut) = schema.as_mut() {
                                if schema_mut.ensure_key_from_metadata(key, value.value_type()) {
                                    schema_modified = true;
                                }
                                if !schema_mut.is_metadata_type_index_enabled(key, value.value_type())? {
                                    continue;
                                }
                            }
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
                                if let Some(ref schema) = schema {
                                    if !schema.is_metadata_type_index_enabled(key, value.value_type())? {
                                        continue;
                                    }
                                }
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
                        if let Some(schema_mut) = schema.as_mut() {
                            if schema_mut.ensure_key_from_metadata(update_key, new_value.value_type()) {
                                schema_modified = true;
                            }
                            // theres basically 4 cases:
                            // 1.old value & new value are not indexed -> noop
                            // 2.old value is indexed & new value is not indexed -> delete old value
                            // 3.old value is not indexed & new value is indexed -> insert new value
                            // 4.old value is indexed & new value is indexed -> update old value
                            let old_is_indexed = schema_mut.is_metadata_type_index_enabled(update_key, old_value.value_type())?;
                            let new_is_indexed = schema_mut.is_metadata_type_index_enabled(update_key, new_value.value_type())?;
                            if !old_is_indexed && !new_is_indexed {
                                continue;
                            }
                            else if old_is_indexed && !new_is_indexed {
                                match self.delete_metadata(update_key, old_value, segment_offset_id).await {
                                    Ok(()) => {}
                                    Err(_) => {
                                        return Err(ApplyMaterializedLogError::BlockfileDelete);
                                    }
                                }
                            }
                            else if !old_is_indexed && new_is_indexed {
                                match self.set_metadata(update_key, new_value, segment_offset_id).await {
                                    Ok(()) => {}
                                    Err(_) => {
                                        return Err(ApplyMaterializedLogError::BlockfileSet);
                                    }
                                }
                            }
                            else if old_is_indexed && new_is_indexed {
                                match self.update_metadata(update_key, old_value, new_value, segment_offset_id).await {
                                    Ok(()) => {}
                                    Err(_) => {
                                        return Err(ApplyMaterializedLogError::BlockfileUpdate);
                                    }
                                }
                            }
                        } else {
                            match self
                                .update_metadata(
                                    update_key,
                                    old_value,
                                    new_value,
                                    segment_offset_id,
                                )
                                .await
                            {
                                Ok(()) => {}
                                Err(_) => {
                                    return Err(ApplyMaterializedLogError::BlockfileUpdate);
                                }
                            }
                        }
                    }

                    // Metadata inserts.
                    for (insert_key, new_value) in metadata_delta.metadata_to_insert {
                        if let Some(schema_mut) = schema.as_mut() {
                            if schema_mut.ensure_key_from_metadata(insert_key, new_value.value_type()) {
                                schema_modified = true;
                            }
                            if !schema_mut.is_metadata_type_index_enabled(insert_key, new_value.value_type())? {
                                continue;
                            }
                        }
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
                        if let Some(ref schema) = schema {
                            if !schema.is_metadata_type_index_enabled(delete_key, old_value.value_type())? {
                                continue;
                            }
                        }
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
                                    if let Some(ref schema) = schema {
                                        if !schema.is_metadata_type_index_enabled(key, value.value_type())? {
                                            continue;
                                        }
                                    }
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
                            if let Some(schema_mut) = schema.as_mut() {
                                if schema_mut.ensure_key_from_metadata(key, value.value_type()) {
                                    schema_modified = true;
                                }
                                if !schema_mut.is_metadata_type_index_enabled(key, value.value_type())? {
                                    continue;
                                }
                            }
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
        // return the schema only if it was modified (so will not affect legacy paths)
        Ok(if schema_modified { schema } else { None })
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

        let sparse_index_flusher = match self.sparse_index_writer {
            Some(sparse_index_writer) => match Box::pin(sparse_index_writer.commit()).await {
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
            sparse_index_flusher,
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
    pub(crate) sparse_index_flusher: SparseFlusher,
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
        let prefix_path = self.full_text_index_flusher.prefix_path().to_string();
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
            vec![ChromaSegmentFlusher::flush_key(
                &prefix_path,
                &full_text_pls_id,
            )],
        );

        match self.bool_metadata_index_flusher.flush().await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            BOOL_METADATA.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &prefix_path,
                &bool_metadata_id,
            )],
        );

        match self.f32_metadata_index_flusher.flush().await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            F32_METADATA.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &prefix_path,
                &f32_metadata_id,
            )],
        );

        match self.u32_metadata_index_flusher.flush().await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            U32_METADATA.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &prefix_path,
                &u32_metadata_id,
            )],
        );

        match self.string_metadata_index_flusher.flush().await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            STRING_METADATA.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &prefix_path,
                &string_metadata_id,
            )],
        );

        let max_id = self.sparse_index_flusher.max_id();
        let offset_value_id = self.sparse_index_flusher.offset_value_id();
        match Box::pin(self.sparse_index_flusher.flush()).await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            SPARSE_MAX.to_string(),
            vec![ChromaSegmentFlusher::flush_key(&prefix_path, &max_id)],
        );
        flushed.insert(
            SPARSE_OFFSET_VALUE.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &prefix_path,
                &offset_value_id,
            )],
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
    pub sparse_index_reader: Option<SparseReader<'me>>,
}

impl MetadataSegmentReader<'_> {
    async fn load_index_reader<'new, K: ReadKey<'new>, V: ReadValue<'new>>(
        segment: &Segment,
        file_path_string: &str,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Option<BlockfileReader<'new, K, V>>, MetadataSegmentError> {
        match segment.file_path.get(file_path_string) {
            Some(file_paths) => match file_paths.first() {
                Some(file_path) => {
                    let (prefix_path, index_uuid) = Segment::extract_prefix_and_id(file_path)
                        .map_err(|_| MetadataSegmentError::UuidParseError(file_path.to_string()))?;
                    let reader_options =
                        BlockfileReaderOptions::new(index_uuid, prefix_path.to_string());
                    match blockfile_provider.read::<K, V>(reader_options).await {
                        Ok(reader) => Ok(Some(reader)),
                        Err(e) => Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    }
                }
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    pub async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Self, MetadataSegmentError> {
        if segment.r#type != SegmentType::BlockfileMetadata {
            return Err(MetadataSegmentError::InvalidSegmentType);
        }

        // Create async tasks for all reader operations
        let pls_future = Self::load_index_reader(segment, FULL_TEXT_PLS, blockfile_provider);

        let string_metadata_future =
            Self::load_index_reader(segment, STRING_METADATA, blockfile_provider)
                .instrument(Span::current());

        let bool_metadata_future =
            Self::load_index_reader(segment, BOOL_METADATA, blockfile_provider)
                .instrument(Span::current());

        let f32_metadata_future =
            Self::load_index_reader(segment, F32_METADATA, blockfile_provider)
                .instrument(Span::current());

        let u32_metadata_future =
            Self::load_index_reader(segment, U32_METADATA, blockfile_provider)
                .instrument(Span::current());

        let sparse_max_future = Self::load_index_reader(segment, SPARSE_MAX, blockfile_provider)
            .instrument(Span::current());

        let sparse_offset_value_future =
            Self::load_index_reader(segment, SPARSE_OFFSET_VALUE, blockfile_provider)
                .instrument(Span::current());

        let (
            pls_reader,
            string_metadata_reader,
            bool_metadata_reader,
            f32_metadata_reader,
            u32_metadata_reader,
            sparse_max_reader,
            sparse_offset_value_reader,
        ) = tokio::join!(
            pls_future,
            string_metadata_future,
            bool_metadata_future,
            f32_metadata_future,
            u32_metadata_future,
            sparse_max_future,
            sparse_offset_value_future,
        );

        // Handle results and create index readers
        let pls_reader = pls_reader?;
        let full_text_index_reader = pls_reader.map(|reader| {
            let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
            FullTextIndexReader::new(reader, tokenizer)
        });

        let string_metadata_reader = string_metadata_reader?;
        let string_metadata_index_reader =
            string_metadata_reader.map(MetadataIndexReader::new_string);

        let bool_metadata_reader = bool_metadata_reader?;
        let bool_metadata_index_reader = bool_metadata_reader.map(MetadataIndexReader::new_bool);

        let u32_metadata_reader = u32_metadata_reader?;
        let u32_metadata_index_reader = u32_metadata_reader.map(MetadataIndexReader::new_u32);

        let f32_metadata_reader = f32_metadata_reader?;
        let f32_metadata_index_reader = f32_metadata_reader.map(MetadataIndexReader::new_f32);

        let sparse_index_reader =
            if let (Some(sparse_max_reader), Some(sparse_offset_value_reader)) =
                (sparse_max_reader?, sparse_offset_value_reader?)
            {
                Some(SparseReader::new(
                    sparse_max_reader,
                    sparse_offset_value_reader,
                ))
            } else {
                None
            };

        Ok(MetadataSegmentReader {
            full_text_index_reader,
            string_metadata_index_reader,
            bool_metadata_index_reader,
            f32_metadata_index_reader,
            u32_metadata_index_reader,
            sparse_index_reader,
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
        arrow::{
            config::{BlockManagerConfig, TEST_MAX_BLOCK_SIZE_BYTES},
            provider::ArrowBlockfileProvider,
        },
        provider::BlockfileProvider,
    };
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{
        regex::literal_expr::{LiteralExpr, NgramLiteralProvider},
        strategies::{ArbitraryChromaRegexTestDocumentsParameters, ChromaRegexTestDocuments},
        Chunk, CollectionUuid, DatabaseUuid, LogRecord, MetadataValue, Operation, OperationRecord,
        ScalarEncoding, SegmentUuid, UpdateMetadataValue, SPARSE_MAX, SPARSE_OFFSET_VALUE,
    };
    use proptest::prelude::any_with;
    use roaring::RoaringBitmap;
    use std::{collections::HashMap, str::FromStr};
    use tokio::runtime::Runtime;

    #[tokio::test]
    async fn empty_blocks() {
        // Run the actual test logic in a separate thread with increased stack size
        let handle = std::thread::Builder::new()
            .name("empty_blocks_test".to_string())
            .stack_size(8 * 1024 * 1024) // 8MB stack size
            .spawn(|| {
                // Create a new tokio runtime within the thread
                let runtime = tokio::runtime::Runtime::new().unwrap();
                runtime.block_on(async {
                    Box::pin(empty_blocks_impl()).await;
                });
            })
            .expect("Failed to spawn thread");

        handle.join().expect("Test thread panicked");
    }

    async fn empty_blocks_impl() {
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
                .expect("Apply materialized log to record segment failed");
            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
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
        let record_segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Reader should be initialized by now");
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
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(&some_reader, data, None)
            .await
            .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records, None)
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
        let record_flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        record_segment.file_path = Box::pin(record_flusher.flush())
            .await
            .expect("Flush record segment writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        // No data should be present.
        let record_segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Record segment reader should be initialized by now");
        let res = record_segment_reader
            .get_all_data()
            .await
            .expect("Error getting all data from record segment")
            .collect::<Vec<_>>();
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
        let record_segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Reader should be initialized by now");
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
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(&some_reader, data, None)
            .await
            .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records, None)
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
        let record_flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        let count = record_flusher.count();
        assert_eq!(count, 2_u64);
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        record_segment.file_path = Box::pin(record_flusher.flush())
            .await
            .expect("Flush record segment writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        // No data should be present.
        let record_segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Record segment reader should be initialized by now");
        let res = record_segment_reader
            .get_all_data()
            .await
            .expect("Error getting all data from record segment")
            .collect::<Vec<_>>();
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
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();
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
                .expect("Apply materialized log to record segment failed");
            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
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
        let record_segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Reader should be initialized by now");
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
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(&some_reader, data, None)
            .await
            .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records, None)
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
        let record_flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        record_segment.file_path = Box::pin(record_flusher.flush())
            .await
            .expect("Flush record segment writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        // Search by f32 metadata value first.
        let metadata_segment_reader = Box::pin(MetadataSegmentReader::from_segment(
            &metadata_segment,
            &blockfile_provider,
        ))
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
        let record_segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Reader should be initialized by now");
        let mut res = record_segment_reader
            .get_all_data()
            .await
            .expect("Record segment get all data failed")
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 2);
        res.sort_by(|x, y| x.1.id.cmp(y.1.id));
        let mut id1_mt = HashMap::new();
        id1_mt.insert(
            String::from("hello"),
            MetadataValue::Str(String::from("new world")),
        );
        assert_eq!(res.first().as_ref().unwrap().1.metadata, Some(id1_mt));
        let mut id2_mt = HashMap::new();
        id2_mt.insert(String::from("hello"), MetadataValue::Float(1.0));
        assert_eq!(res.get(1).as_ref().unwrap().1.metadata, Some(id2_mt));
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
                    document: Some(String::from("This is a document about cats.")),
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
                .expect("Apply materialized log to record segment failed");
            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
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
        let record_segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Reader should be initialized by now");
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
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(&some_reader, data, None)
            .await
            .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records, None)
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
        let record_flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        record_segment.file_path = Box::pin(record_flusher.flush())
            .await
            .expect("Flush record segment writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        // Only one key should be present.
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
        let record_segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Reader should be initialized by now");
        let mut res = record_segment_reader
            .get_all_data()
            .await
            .expect("Record segment get all data failed")
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 1);
        res.sort_by(|x, y| x.1.id.cmp(y.1.id));
        let mut id1_mt = HashMap::new();
        id1_mt.insert(
            String::from("bye"),
            MetadataValue::Str(String::from("world")),
        );
        assert_eq!(res.first().as_ref().unwrap().1.metadata, Some(id1_mt));
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
                .expect("Apply materialized log to record segment failed");
            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
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
        let record_segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Reader should be initialized by now");
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
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(&some_reader, data, None)
            .await
            .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records, None)
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
        let record_flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        record_segment.file_path = Box::pin(record_flusher.flush())
            .await
            .expect("Flush record segment writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        // FTS for hello should return empty.
        let metadata_segment_reader = Box::pin(MetadataSegmentReader::from_segment(
            &metadata_segment,
            &blockfile_provider,
        ))
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
        let record_segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Reader should be initialized by now");
        let mut res = record_segment_reader
            .get_all_data()
            .await
            .expect("Record segment get all data failed")
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 1);
        res.sort_by(|x, y| x.1.id.cmp(y.1.id));
        assert_eq!(
            res.first().as_ref().unwrap().1.document,
            Some(String::from("bye").as_str())
        );
    }

    #[tokio::test]
    async fn test_storage_prefix_path() {
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
            let data = vec![
                LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: None,
                        document: Some(String::from("hello")),
                        operation: Operation::Add,
                    },
                },
                LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: None,
                        document: Some(String::from("world")),
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
                .expect("Apply materialized log to record segment failed");
            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");
        }
        let prefix = format!(
            "tenant/{}/database/{}/collection/{}/segment/{}",
            tenant, database_id, record_segment.collection, record_segment.id,
        );
        assert_eq!(record_segment.file_path.len(), 4);
        for (_, file_path) in record_segment.file_path.iter() {
            assert_eq!(file_path.len(), 1);
            assert!(file_path
                .first()
                .expect("File path should have at least one entry")
                .starts_with(&prefix));
        }
        let prefix = format!(
            "tenant/{}/database/{}/collection/{}/segment/{}",
            tenant, database_id, record_segment.collection, metadata_segment.id,
        );
        assert_eq!(metadata_segment.file_path.len(), 7);
        for (_, file_path) in metadata_segment.file_path.iter() {
            assert_eq!(file_path.len(), 1);
            assert!(file_path
                .first()
                .expect("File path should have at least one entry")
                .starts_with(&prefix));
        }
        // FTS for hello should return 1 document
        let metadata_segment_reader = Box::pin(MetadataSegmentReader::from_segment(
            &metadata_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Metadata segment reader construction failed");
        let res = metadata_segment_reader
            .full_text_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .search("hello")
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        // FTS for world should return the other document.
        let res = metadata_segment_reader
            .full_text_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .search("world")
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(2));
        // Record segment should also have the updated values.
        let record_segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Reader should be initialized by now");
        let mut res = record_segment_reader
            .get_all_data()
            .await
            .expect("Record segment get all data failed")
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 2);
        res.sort_by(|x, y| x.1.id.cmp(y.1.id));
        assert_eq!(
            res.first().as_ref().unwrap().1.document,
            Some(String::from("hello").as_str())
        );
        assert_eq!(
            res.get(1).as_ref().unwrap().1.document,
            Some(String::from("world").as_str())
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
        let mut segments = TestDistributedSegment::new_with_dimension(2).await;
        Box::pin(segments.compact_log(Chunk::new(logs.into()), 0)).await;
        let metadata_segment_reader = Box::pin(MetadataSegmentReader::from_segment(
            &segments.metadata_segment,
            &segments.blockfile_provider,
        ))
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
                Box::pin(run_regex_test(test_case)).await
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
                Box::pin(run_regex_test(test_case)).await
            });
        }
    }

    #[tokio::test]
    async fn test_metadata_sparse_vector() {
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
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();
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

        // Create segments and add records with sparse vectors
        {
            let segment_writer = RecordSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &record_segment,
                &blockfile_provider,
            )
            .await
            .expect("Error creating segment writer");

            let metadata_writer = MetadataSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &metadata_segment,
                &blockfile_provider,
            )
            .await
            .expect("Error creating segment writer");

            // Verify that sparse index writer is created
            assert!(
                metadata_writer.sparse_index_writer.is_some(),
                "Sparse index writer should be created"
            );

            // Create metadata with sparse vectors
            let mut update_metadata1 = HashMap::new();
            update_metadata1.insert(
                String::from("sparse_vec"),
                UpdateMetadataValue::SparseVector(chroma_types::SparseVector::new(
                    vec![0, 5, 10],
                    vec![0.1, 0.5, 0.9],
                )),
            );
            update_metadata1.insert(
                String::from("category"),
                UpdateMetadataValue::Str(String::from("science")),
            );

            let data = vec![LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: Some(update_metadata1.clone()),
                    document: Some(String::from("Document with sparse vector 1")),
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
                Err(e) => match *e {
                    RecordSegmentReaderCreationError::UninitializedSegment => None,
                    _ => panic!("Error creating record segment reader"),
                },
            };

            let materialized_logs = materialize_logs(&record_segment_reader, data, None)
                .await
                .expect("Error materializing logs");

            // Apply logs - this should handle sparse vectors
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &materialized_logs)
                .await
                .expect("Error applying materialized log chunk");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &materialized_logs, None)
                .await
                .expect("Error applying materialized log chunk");

            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit record segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit metadata segment writer failed");

            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");

            // Verify that sparse index files are created
            assert!(
                metadata_segment.file_path.contains_key(SPARSE_MAX),
                "Sparse max file should be created"
            );
            assert!(
                metadata_segment.file_path.contains_key(SPARSE_OFFSET_VALUE),
                "Sparse offset value file should be created"
            );
        }

        // Verify we can read the segment back
        {
            let metadata_segment_reader = Box::pin(MetadataSegmentReader::from_segment(
                &metadata_segment,
                &blockfile_provider,
            ))
            .await
            .expect("Error creating metadata segment reader");

            // Verify sparse index reader is created
            assert!(
                metadata_segment_reader.sparse_index_reader.is_some(),
                "Sparse index reader should be created"
            );
        }
    }

    #[tokio::test]
    async fn test_sparse_index_recreated_with_existing_prefix() {
        // This test verifies that when sparse index files are missing (e.g., deleted)
        // and need to be recreated, they use the same prefix as existing blockfiles
        // This tests the bug fix for incorrect blockfile paths

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

        // Original collection ID
        let original_collection_id =
            CollectionUuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error");

        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000002").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: original_collection_id,
            metadata: None,
            file_path: HashMap::new(),
        };

        // First flush: create initial blockfiles
        {
            let metadata_writer = MetadataSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &metadata_segment,
                &blockfile_provider,
            )
            .await
            .expect("Error creating metadata writer");

            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Error committing metadata");

            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Error flushing metadata");
        }

        // Verify sparse index files were created
        assert!(metadata_segment.file_path.contains_key(SPARSE_MAX));
        assert!(metadata_segment.file_path.contains_key(SPARSE_OFFSET_VALUE));

        // Extract the original prefix
        let original_prefix = {
            let existing_file_path = metadata_segment
                .file_path
                .values()
                .next()
                .and_then(|paths| paths.first())
                .expect("Should have at least one blockfile");

            let (prefix, _) = chroma_types::Segment::extract_prefix_and_id(existing_file_path)
                .expect("Should be able to extract prefix");
            prefix.to_string()
        };

        // Simulate missing sparse index files (e.g., from older version or deleted)
        metadata_segment.file_path.remove(SPARSE_MAX);
        metadata_segment.file_path.remove(SPARSE_OFFSET_VALUE);

        // Change collection ID to simulate a forked collection
        let forked_collection_id =
            CollectionUuid::from_str("00000000-0000-0000-0000-000000000003").expect("parse error");
        metadata_segment.collection = forked_collection_id;

        // Second flush: recreate sparse index files
        // The bug fix ensures they use the existing prefix, not a new one
        {
            let metadata_writer = MetadataSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &metadata_segment,
                &blockfile_provider,
            )
            .await
            .expect("Error creating metadata writer");

            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Error committing metadata");

            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Error flushing metadata");
        }

        // Verify sparse index files were recreated
        assert!(
            metadata_segment.file_path.contains_key(SPARSE_MAX),
            "Sparse max should be recreated"
        );
        assert!(
            metadata_segment.file_path.contains_key(SPARSE_OFFSET_VALUE),
            "Sparse offset value should be recreated"
        );

        // Verify ALL blockfiles use the original prefix
        for (key, paths) in &metadata_segment.file_path {
            for path in paths {
                let (prefix, _) = chroma_types::Segment::extract_prefix_and_id(path)
                    .expect("Should be able to extract prefix");
                assert_eq!(
                    prefix, original_prefix,
                    "All blockfiles should use original prefix. Key: {}, Path: {}",
                    key, path
                );
                // Verify the prefix contains the original collection ID, not the forked one
                assert!(
                    prefix.contains(&original_collection_id.to_string()),
                    "Prefix should contain original collection ID"
                );
                assert!(
                    !prefix.contains(&forked_collection_id.to_string()),
                    "Prefix should NOT contain forked collection ID"
                );
            }
        }

        // Verify we can read from the segment with recreated sparse indices
        {
            let metadata_reader = Box::pin(MetadataSegmentReader::from_segment(
                &metadata_segment,
                &blockfile_provider,
            ))
            .await
            .expect("Should be able to read from segment with recreated sparse indices");

            assert!(
                metadata_reader.sparse_index_reader.is_some(),
                "Sparse index reader should be created, verifying files exist and are readable"
            );
        }
        // Simulate legacy files without prefix
        metadata_segment.file_path.drain();
        metadata_segment.file_path.insert(
            "legacy_file".to_string(),
            vec!["11111111-1111-1111-1111-111111111111".to_string()],
        );

        // Change collection ID to simulate a forked collection
        let forked_collection_id =
            CollectionUuid::from_str("00000000-0000-0000-0000-000000000004").expect("parse error");
        metadata_segment.collection = forked_collection_id;

        // Third flush: recreate all index files
        // The bug fix ensures they use the existing prefix, not a new one
        {
            let metadata_writer = MetadataSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &metadata_segment,
                &blockfile_provider,
            )
            .await
            .expect("Error creating metadata writer");

            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Error committing metadata");

            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Error flushing metadata");
        }

        // Verify sparse index files were recreated
        assert!(
            metadata_segment.file_path.contains_key(SPARSE_MAX),
            "Sparse max should be recreated"
        );
        assert!(
            metadata_segment.file_path.contains_key(SPARSE_OFFSET_VALUE),
            "Sparse offset value should be recreated"
        );

        // Verify ALL blockfiles use the original prefix
        for (key, paths) in &metadata_segment.file_path {
            for path in paths {
                let (prefix, _) = chroma_types::Segment::extract_prefix_and_id(path)
                    .expect("Should be able to extract prefix");
                assert!(
                    prefix.is_empty(),
                    "All blockfiles should use empty prefix. Key: {}, Path: {}",
                    key,
                    path
                );
            }
        }

        // Verify we can read from the segment with recreated sparse indices
        {
            let metadata_reader = Box::pin(MetadataSegmentReader::from_segment(
                &metadata_segment,
                &blockfile_provider,
            ))
            .await
            .expect("Should be able to read from segment with recreated sparse indices");

            assert!(
                metadata_reader.sparse_index_reader.is_some(),
                "Sparse index reader should be created, verifying files exist and are readable"
            );
        }
    }
}
