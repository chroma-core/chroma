use super::record_segment::ApplyMaterializedLogError;
use super::types::{MaterializedLogRecord, SegmentWriter};
use super::SegmentFlusher;
use arrow::array::Int32Array;
use async_trait::async_trait;
use chroma_blockstore::provider::{BlockfileProvider, CreateError, OpenError};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::fulltext::tokenizer::TantivyChromaTokenizer;
use chroma_index::fulltext::types::{
    FullTextIndexError, FullTextIndexFlusher, FullTextIndexReader, FullTextIndexWriter,
};
use chroma_index::metadata::types::{
    MetadataIndexError, MetadataIndexFlusher, MetadataIndexReader, MetadataIndexWriter,
};
use chroma_index::utils::{merge_sorted_vecs_conjunction, merge_sorted_vecs_disjunction};
use chroma_types::{
    BooleanOperator, Chunk, MaterializedLogOperation, MetadataValue, Segment, Where,
    WhereClauseComparator, WhereDocument, WhereDocumentOperator,
};
use chroma_types::{SegmentType, WhereComparison};
use core::panic;
use futures::future::BoxFuture;
use futures::FutureExt;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::u32;
use tantivy::tokenizer::NgramTokenizer;
use thiserror::Error;
use uuid::Uuid;

const FULL_TEXT_PLS: &str = "full_text_pls";
const FULL_TEXT_FREQS: &str = "full_text_freqs";
const STRING_METADATA: &str = "string_metadata";
const BOOL_METADATA: &str = "bool_metadata";
const F32_METADATA: &str = "f32_metadata";
const U32_METADATA: &str = "u32_metadata";

#[derive(Clone)]
pub(crate) struct MetadataSegmentWriter<'me> {
    pub(crate) full_text_index_writer: Option<FullTextIndexWriter<'me>>,
    pub(crate) string_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) bool_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) f32_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) u32_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) id: Uuid,
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
    #[error("Could not write to fulltext index blockfiles {0}")]
    FullTextIndexWriteError(Box<dyn ChromaError>),
    #[error("Path vector exists but is empty?")]
    EmptyPathVector,
    #[error("Failed to write to blockfile")]
    BlockfileWriteError,
    #[error("Limit and offset are not currently supported")]
    LimitOffsetNotSupported,
    #[error("Could not query metadata index {0}")]
    MetadataIndexQueryError(#[from] MetadataIndexError),
    #[error("Attempted to delete a document that does not exist")]
    DocumentDoesNotExist,
}

impl ChromaError for MetadataSegmentError {
    fn code(&self) -> ErrorCodes {
        // TODO
        ErrorCodes::Internal
    }
}

impl<'me> MetadataSegmentWriter<'me> {
    pub(crate) async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<MetadataSegmentWriter<'me>, MetadataSegmentError> {
        if segment.r#type != SegmentType::BlockfileMetadata {
            return Err(MetadataSegmentError::InvalidSegmentType);
        }
        if segment.file_path.contains_key(FULL_TEXT_FREQS)
            && !segment.file_path.contains_key(FULL_TEXT_PLS)
        {
            return Err(MetadataSegmentError::MissingFile(
                (*FULL_TEXT_PLS).to_string(),
            ));
        }
        if segment.file_path.contains_key(FULL_TEXT_PLS)
            && !segment.file_path.contains_key(FULL_TEXT_FREQS)
        {
            return Err(MetadataSegmentError::MissingFile(
                (*FULL_TEXT_FREQS).to_string(),
            ));
        }
        let (pls_writer, pls_reader) = match segment.file_path.get(FULL_TEXT_PLS) {
            Some(pls_path) => match pls_path.get(0) {
                Some(pls_uuid) => {
                    let pls_uuid = match Uuid::parse_str(pls_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(pls_uuid.to_string()))
                        }
                    };
                    let pls_writer =
                        match blockfile_provider.fork::<u32, &Int32Array>(&pls_uuid).await {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        };
                    let pls_reader =
                        match blockfile_provider.open::<u32, Int32Array>(&pls_uuid).await {
                            Ok(reader) => reader,
                            Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                        };
                    (pls_writer, Some(pls_reader))
                }
                None => return Err(MetadataSegmentError::EmptyPathVector),
            },
            None => match blockfile_provider.create::<u32, &Int32Array>() {
                Ok(writer) => (writer, None),
                Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
            },
        };
        let (freqs_writer, freqs_reader) = match segment.file_path.get(FULL_TEXT_FREQS) {
            Some(freqs_path) => match freqs_path.get(0) {
                Some(freqs_uuid) => {
                    let freqs_uuid = match Uuid::parse_str(freqs_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(
                                freqs_uuid.to_string(),
                            ))
                        }
                    };
                    let freqs_writer = match blockfile_provider.fork::<u32, u32>(&freqs_uuid).await
                    {
                        Ok(writer) => writer,
                        Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                    };
                    let freqs_reader = match blockfile_provider.open::<u32, u32>(&freqs_uuid).await
                    {
                        Ok(reader) => reader,
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    };
                    (freqs_writer, Some(freqs_reader))
                }
                None => return Err(MetadataSegmentError::EmptyPathVector),
            },
            None => match blockfile_provider.create::<u32, u32>() {
                Ok(writer) => (writer, None),
                Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
            },
        };
        let full_text_index_reader = match (pls_reader, freqs_reader) {
            (Some(pls_reader), Some(freqs_reader)) => {
                let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
                    NgramTokenizer::new(3, 3, false).unwrap(),
                )));
                Some(FullTextIndexReader::new(
                    pls_reader,
                    freqs_reader,
                    tokenizer,
                ))
            }
            (None, None) => None,
            _ => return Err(MetadataSegmentError::IncorrectNumberOfFiles),
        };

        let full_text_writer_tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(3, 3, false).unwrap(),
        )));
        let full_text_index_writer = FullTextIndexWriter::new(
            full_text_index_reader,
            pls_writer,
            freqs_writer,
            full_text_writer_tokenizer,
        );

        let (string_metadata_writer, string_metadata_index_reader) =
            match segment.file_path.get(STRING_METADATA) {
                Some(string_metadata_path) => match string_metadata_path.get(0) {
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
                            .fork::<&str, &RoaringBitmap>(&string_metadata_uuid)
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        };
                        let string_metadata_index_reader = match blockfile_provider
                            .open::<&str, RoaringBitmap>(&string_metadata_uuid)
                            .await
                        {
                            Ok(reader) => MetadataIndexReader::new_string(reader),
                            Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                        };
                        (string_metadata_writer, Some(string_metadata_index_reader))
                    }
                    None => return Err(MetadataSegmentError::EmptyPathVector),
                },
                None => match blockfile_provider.create::<&str, &RoaringBitmap>() {
                    Ok(writer) => (writer, None),
                    Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                },
            };
        let string_metadata_index_writer =
            MetadataIndexWriter::new_string(string_metadata_writer, string_metadata_index_reader);

        let (bool_metadata_writer, bool_metadata_index_reader) =
            match segment.file_path.get(BOOL_METADATA) {
                Some(bool_metadata_path) => match bool_metadata_path.get(0) {
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
                            .fork::<bool, &RoaringBitmap>(&bool_metadata_uuid)
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        };
                        let bool_metadata_index_writer = match blockfile_provider
                            .open::<bool, RoaringBitmap>(&bool_metadata_uuid)
                            .await
                        {
                            Ok(reader) => MetadataIndexReader::new_bool(reader),
                            Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                        };
                        (bool_metadata_writer, Some(bool_metadata_index_writer))
                    }
                    None => return Err(MetadataSegmentError::EmptyPathVector),
                },
                None => match blockfile_provider.create::<bool, &RoaringBitmap>() {
                    Ok(writer) => (writer, None),
                    Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                },
            };
        let bool_metadata_index_writer =
            MetadataIndexWriter::new_bool(bool_metadata_writer, bool_metadata_index_reader);

        let (f32_metadata_writer, f32_metadata_index_reader) =
            match segment.file_path.get(F32_METADATA) {
                Some(f32_metadata_path) => match f32_metadata_path.get(0) {
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
                            .fork::<f32, &RoaringBitmap>(&f32_metadata_uuid)
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        };
                        let f32_metadata_index_reader = match blockfile_provider
                            .open::<f32, RoaringBitmap>(&f32_metadata_uuid)
                            .await
                        {
                            Ok(reader) => MetadataIndexReader::new_f32(reader),
                            Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                        };
                        (f32_metadata_writer, Some(f32_metadata_index_reader))
                    }
                    None => return Err(MetadataSegmentError::EmptyPathVector),
                },
                None => match blockfile_provider.create::<f32, &RoaringBitmap>() {
                    Ok(writer) => (writer, None),
                    Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                },
            };
        let f32_metadata_index_writer =
            MetadataIndexWriter::new_f32(f32_metadata_writer, f32_metadata_index_reader);

        let (u32_metadata_writer, u32_metadata_index_reader) =
            match segment.file_path.get(U32_METADATA) {
                Some(u32_metadata_path) => match u32_metadata_path.get(0) {
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
                            .fork::<u32, &RoaringBitmap>(&u32_metadata_uuid)
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        };
                        let u32_metadata_index_reader = match blockfile_provider
                            .open::<u32, RoaringBitmap>(&u32_metadata_uuid)
                            .await
                        {
                            Ok(reader) => MetadataIndexReader::new_u32(reader),
                            Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                        };
                        (u32_metadata_writer, Some(u32_metadata_index_reader))
                    }
                    None => return Err(MetadataSegmentError::EmptyPathVector),
                },
                None => match blockfile_provider.create::<u32, &RoaringBitmap>() {
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

    pub async fn write_to_blockfiles(&mut self) -> Result<(), MetadataSegmentError> {
        let mut full_text_index_writer = self
            .full_text_index_writer
            .take()
            .ok_or_else(|| MetadataSegmentError::NoWriter)?;
        let res = full_text_index_writer.write_to_blockfiles().await;
        self.full_text_index_writer = Some(full_text_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(MetadataSegmentError::BlockfileWriteError),
        }

        let mut string_metadata_index_writer = self
            .string_metadata_index_writer
            .take()
            .ok_or_else(|| MetadataSegmentError::NoWriter)?;
        let res = string_metadata_index_writer.write_to_blockfile().await;
        self.string_metadata_index_writer = Some(string_metadata_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(MetadataSegmentError::BlockfileWriteError),
        }

        let mut bool_metadata_index_writer = self
            .bool_metadata_index_writer
            .take()
            .ok_or_else(|| MetadataSegmentError::NoWriter)?;
        let res = bool_metadata_index_writer.write_to_blockfile().await;
        self.bool_metadata_index_writer = Some(bool_metadata_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(MetadataSegmentError::BlockfileWriteError),
        }

        let mut f32_metadata_index_writer = self
            .f32_metadata_index_writer
            .take()
            .ok_or_else(|| MetadataSegmentError::NoWriter)?;
        let res = f32_metadata_index_writer.write_to_blockfile().await;
        self.f32_metadata_index_writer = Some(f32_metadata_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(MetadataSegmentError::BlockfileWriteError),
        }

        let mut u32_metadata_index_writer = self
            .u32_metadata_index_writer
            .take()
            .ok_or_else(|| MetadataSegmentError::NoWriter)?;
        let res = u32_metadata_index_writer.write_to_blockfile().await;
        self.u32_metadata_index_writer = Some(u32_metadata_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(MetadataSegmentError::BlockfileWriteError),
        }

        Ok(())
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
                                return Err(e);
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
                                return Err(e);
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
                                return Err(e);
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
                                return Err(e);
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
                                return Err(e);
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
                                return Err(e);
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
                                return Err(e);
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
                                return Err(e);
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
}

impl<'log_records> SegmentWriter<'log_records> for MetadataSegmentWriter<'_> {
    async fn apply_materialized_log_chunk(
        &self,
        records: Chunk<MaterializedLogRecord<'log_records>>,
    ) -> Result<(), ApplyMaterializedLogError> {
        for record in records.iter() {
            let segment_offset_id = record.0.offset_id;
            match record.0.final_operation {
                MaterializedLogOperation::AddNew => {
                    // We can ignore record.0.metadata_to_be_deleted
                    // for fresh adds. TODO on whether to propagate error.
                    match &record.0.metadata_to_be_merged {
                        Some(metadata) => {
                            for (key, value) in metadata.iter() {
                                match self.set_metadata(key, value, segment_offset_id).await {
                                    Ok(()) => {}
                                    Err(e) => {
                                        return Err(ApplyMaterializedLogError::BlockfileSetError);
                                    }
                                }
                            }
                        }
                        None => {}
                    };
                    match &record.0.final_document {
                        Some(document) => match &self.full_text_index_writer {
                            Some(writer) => {
                                let _ = writer
                                    .add_document(document, segment_offset_id as i32)
                                    .await;
                            }
                            None => panic!(
                                "Invariant violation. Expected full text index writer to be set"
                            ),
                        },
                        // It is ok for the user to not pass in any document.
                        None => {}
                    };
                }
                MaterializedLogOperation::DeleteExisting => match &record.0.data_record {
                    Some(data_record) => {
                        match &data_record.metadata {
                            Some(metadata) => {
                                for (key, value) in metadata.iter() {
                                    match self.delete_metadata(key, value, segment_offset_id).await
                                    {
                                        Ok(()) => {}
                                        Err(e) => {
                                            return Err(
                                                ApplyMaterializedLogError::BlockfileDeleteError,
                                            );
                                        }
                                    }
                                }
                            }
                            // Ok to not have any metadata to delete.
                            None => {}
                        };
                        match &data_record.document {
                            Some(document) => match &self.full_text_index_writer {
                                Some(writer) => {
                                    let err =
                                        writer.delete_document(document, segment_offset_id).await;
                                    match err {
                                        Ok(_) => {}
                                        Err(e) => {
                                            tracing::error!("Error deleting document {:?}", e);
                                            return Err(
                                                ApplyMaterializedLogError::FTSDocumentDeleteError,
                                            );
                                        }
                                    }
                                }
                                None => {
                                    panic!("Invariant violation. FTS index writer should be set")
                                }
                            },
                            // The record that is to be deleted might not have
                            // a document, it is fine and should not be an error.
                            None => {}
                        };
                    }
                    None => panic!("Invariant violation. Data record should be set by materializer in case of Deletes")
                },
                MaterializedLogOperation::UpdateExisting => {
                    let metadata_delta = record.0.metadata_delta();
                    // Metadata updates.
                    for (update_key, (old_value, new_value)) in metadata_delta.metadata_to_update {
                        match self
                            .update_metadata(update_key, old_value, new_value, segment_offset_id)
                            .await
                        {
                            Ok(()) => {}
                            Err(e) => {
                                return Err(ApplyMaterializedLogError::BlockfileUpdateError);
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
                            Err(e) => {
                                return Err(ApplyMaterializedLogError::BlockfileSetError);
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
                            Err(e) => {
                                return Err(ApplyMaterializedLogError::BlockfileDeleteError);
                            }
                        }
                    }
                    // Update the document if present.
                    match record.0.final_document {
                        Some(doc) => match &self.full_text_index_writer {
                            Some(writer) => match &record.0.data_record {
                                Some(record) => match record.document {
                                    Some(old_doc) => {
                                        match writer
                                            .update_document(&old_doc, doc, segment_offset_id)
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(e) => {
                                                tracing::error!(
                                                    "FTS Update document failed {:?}",
                                                    e
                                                );
                                                return Err(
                                                    ApplyMaterializedLogError::FTSDocumentUpdateError,
                                                );
                                            }
                                        }
                                    }
                                    // Previous version of record does not contain document string.
                                    None => match writer
                                        .add_document(doc, segment_offset_id as i32)
                                        .await
                                    {
                                        Ok(_) => {}
                                        Err(e) => {
                                            tracing::error!(
                                                "Add document for an update failed {:?}",
                                                e
                                            );
                                            return Err(
                                                ApplyMaterializedLogError::FTSDocumentAddError,
                                            );
                                        }
                                    },
                                },
                                None => panic!("Invariant violation. Record should be set by materializer for an update")
                            },
                            None => panic!("Invariant violation. FTS index writer should be set"),
                        },
                        // Ok to not have any update for the document. Do not error.
                        None => {}
                    }
                }
                MaterializedLogOperation::OverwriteExisting => {
                    // Delete existing.
                    match &record.0.data_record {
                        Some(data_record) => {
                            match &data_record.metadata {
                                Some(metadata) => {
                                    for (key, value) in metadata.iter() {
                                        match self.delete_metadata(key, value, segment_offset_id).await
                                        {
                                            Ok(()) => {}
                                            Err(e) => {
                                                return Err(
                                                    ApplyMaterializedLogError::BlockfileDeleteError,
                                                );
                                            }
                                        }
                                    }
                                }
                                // Ok to not have any metadata to delete.
                                None => {}
                            };
                            match &data_record.document {
                                Some(document) => match &self.full_text_index_writer {
                                    Some(writer) => {
                                        let err =
                                            writer.delete_document(document, segment_offset_id).await;
                                        match err {
                                            Ok(_) => {}
                                            Err(e) => {
                                                tracing::error!("Error deleting document {:?}", e);
                                                return Err(
                                                    ApplyMaterializedLogError::FTSDocumentDeleteError,
                                                );
                                            }
                                        }
                                    }
                                    None => {
                                        panic!("Invariant violation. FTS index writer should be set")
                                    }
                                },
                                // The record that is to be deleted might not have
                                // a document, it is fine and should not be an error.
                                None => {}
                            };
                        },
                        None => panic!("Invariant violation. Data record should be set by materializer in case of Deletes")
                    };
                    // Add new.
                    match &record.0.metadata_to_be_merged {
                        Some(metadata) => {
                            for (key, value) in metadata.iter() {
                                match self.set_metadata(key, value, segment_offset_id).await {
                                    Ok(()) => {}
                                    Err(e) => {
                                        return Err(ApplyMaterializedLogError::BlockfileSetError);
                                    }
                                }
                            }
                        }
                        None => {}
                    };
                    match &record.0.final_document {
                        Some(document) => match &self.full_text_index_writer {
                            Some(writer) => {
                                let _ = writer
                                    .add_document(document, segment_offset_id as i32)
                                    .await;
                            }
                            None => panic!(
                                "Invariant violation. Expected full text index writer to be set"
                            ),
                        },
                        // It is ok for the user to not pass in any document.
                        None => {}
                    };
                },
                MaterializedLogOperation::Initial => panic!("Not expected mat records in the initial state")
            }
        }
        Ok(())
    }

    fn commit(self) -> Result<impl SegmentFlusher, Box<dyn ChromaError>> {
        let full_text_flusher = match self.full_text_index_writer {
            Some(flusher) => match flusher.commit() {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let string_metadata_flusher = match self.string_metadata_index_writer {
            Some(flusher) => match flusher.commit() {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let bool_metadata_flusher = match self.bool_metadata_index_writer {
            Some(flusher) => match flusher.commit() {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let f32_metadata_flusher = match self.f32_metadata_index_writer {
            Some(flusher) => match flusher.commit() {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let u32_metadata_flusher = match self.u32_metadata_index_writer {
            Some(flusher) => match flusher.commit() {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        Ok(MetadataSegmentFlusher {
            full_text_index_flusher: full_text_flusher,
            string_metadata_index_flusher: string_metadata_flusher,
            bool_metadata_index_flusher: bool_metadata_flusher,
            f32_metadata_index_flusher: f32_metadata_flusher,
            u32_metadata_index_flusher: u32_metadata_flusher,
        })
    }
}

pub(crate) struct MetadataSegmentFlusher {
    pub(crate) full_text_index_flusher: FullTextIndexFlusher,
    pub(crate) string_metadata_index_flusher: MetadataIndexFlusher,
    pub(crate) bool_metadata_index_flusher: MetadataIndexFlusher,
    pub(crate) f32_metadata_index_flusher: MetadataIndexFlusher,
    pub(crate) u32_metadata_index_flusher: MetadataIndexFlusher,
}

#[async_trait]
impl SegmentFlusher for MetadataSegmentFlusher {
    async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let full_text_pls_id = self.full_text_index_flusher.pls_id();
        let full_text_freqs_id = self.full_text_index_flusher.freqs_id();
        let string_metadata_id = self.string_metadata_index_flusher.id();
        let bool_metadata_id = self.bool_metadata_index_flusher.id();
        let f32_metadata_id = self.f32_metadata_index_flusher.id();
        let u32_metadata_id = self.u32_metadata_index_flusher.id();

        let mut flushed = HashMap::new();

        match self.full_text_index_flusher.flush().await.map_err(|e| e) {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            FULL_TEXT_PLS.to_string(),
            vec![full_text_pls_id.to_string()],
        );
        flushed.insert(
            FULL_TEXT_FREQS.to_string(),
            vec![full_text_freqs_id.to_string()],
        );

        match self
            .bool_metadata_index_flusher
            .flush()
            .await
            .map_err(|e| e)
        {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            BOOL_METADATA.to_string(),
            vec![bool_metadata_id.to_string()],
        );

        match self.f32_metadata_index_flusher.flush().await.map_err(|e| e) {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(F32_METADATA.to_string(), vec![f32_metadata_id.to_string()]);

        match self.u32_metadata_index_flusher.flush().await.map_err(|e| e) {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(U32_METADATA.to_string(), vec![u32_metadata_id.to_string()]);

        match self
            .string_metadata_index_flusher
            .flush()
            .await
            .map_err(|e| e)
        {
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

pub(crate) struct MetadataSegmentReader<'me> {
    pub(crate) full_text_index_reader: Option<FullTextIndexReader<'me>>,
    pub(crate) string_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    pub(crate) bool_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    pub(crate) f32_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    pub(crate) u32_metadata_index_reader: Option<MetadataIndexReader<'me>>,
}

impl MetadataSegmentReader<'_> {
    pub(crate) async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Self, MetadataSegmentError> {
        if segment.r#type != SegmentType::BlockfileMetadata {
            return Err(MetadataSegmentError::InvalidSegmentType);
        }
        if segment.file_path.contains_key(FULL_TEXT_FREQS)
            && !segment.file_path.contains_key(FULL_TEXT_PLS)
        {
            return Err(MetadataSegmentError::MissingFile(
                (*FULL_TEXT_PLS).to_string(),
            ));
        }
        if segment.file_path.contains_key(FULL_TEXT_PLS)
            && !segment.file_path.contains_key(FULL_TEXT_FREQS)
        {
            return Err(MetadataSegmentError::MissingFile(
                (*FULL_TEXT_FREQS).to_string(),
            ));
        }
        let pls_reader = match segment.file_path.get(FULL_TEXT_PLS) {
            Some(pls_path) => match pls_path.get(0) {
                Some(pls_uuid) => {
                    let pls_uuid = match Uuid::parse_str(pls_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(pls_uuid.to_string()))
                        }
                    };
                    let pls_reader =
                        match blockfile_provider.open::<u32, Int32Array>(&pls_uuid).await {
                            Ok(reader) => Some(reader),
                            Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                        };
                    pls_reader
                }
                None => None,
            },
            None => None,
        };
        let freqs_reader = match segment.file_path.get(FULL_TEXT_FREQS) {
            Some(freqs_path) => match freqs_path.get(0) {
                Some(freqs_uuid) => {
                    let freqs_uuid = match Uuid::parse_str(freqs_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(
                                freqs_uuid.to_string(),
                            ))
                        }
                    };
                    let freqs_reader = match blockfile_provider.open::<u32, u32>(&freqs_uuid).await
                    {
                        Ok(reader) => Some(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    };
                    freqs_reader
                }
                None => None,
            },
            None => None,
        };
        let full_text_index_reader = match (pls_reader, freqs_reader) {
            (Some(pls_reader), Some(freqs_reader)) => {
                let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
                    NgramTokenizer::new(3, 3, false).unwrap(),
                )));
                Some(FullTextIndexReader::new(
                    pls_reader,
                    freqs_reader,
                    tokenizer,
                ))
            }
            (Some(_), None) => return Err(MetadataSegmentError::FullTextIndexFilesIntegrityError),
            (None, Some(_)) => return Err(MetadataSegmentError::FullTextIndexFilesIntegrityError),
            _ => None,
        };

        let string_metadata_reader = match segment.file_path.get(STRING_METADATA) {
            Some(string_metadata_path) => match string_metadata_path.get(0) {
                Some(string_metadata_uuid) => {
                    let string_metadata_uuid = match Uuid::parse_str(string_metadata_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(
                                string_metadata_uuid.to_string(),
                            ))
                        }
                    };
                    let string_metadata_reader = match blockfile_provider
                        .open::<&str, RoaringBitmap>(&string_metadata_uuid)
                        .await
                    {
                        Ok(reader) => Some(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    };
                    string_metadata_reader
                }
                None => None,
            },
            None => None,
        };
        let string_metadata_index_reader = match string_metadata_reader {
            Some(reader) => Some(MetadataIndexReader::new_string(reader)),
            None => None,
        };

        let bool_metadata_reader = match segment.file_path.get(BOOL_METADATA) {
            Some(bool_metadata_path) => match bool_metadata_path.get(0) {
                Some(bool_metadata_uuid) => {
                    let bool_metadata_uuid = match Uuid::parse_str(bool_metadata_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(
                                bool_metadata_uuid.to_string(),
                            ))
                        }
                    };
                    let bool_metadata_reader = match blockfile_provider
                        .open::<bool, RoaringBitmap>(&bool_metadata_uuid)
                        .await
                    {
                        Ok(reader) => Some(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    };
                    bool_metadata_reader
                }
                None => None,
            },
            None => None,
        };
        let bool_metadata_index_reader = match bool_metadata_reader {
            Some(reader) => Some(MetadataIndexReader::new_bool(reader)),
            None => None,
        };

        let u32_metadata_reader = match segment.file_path.get(U32_METADATA) {
            Some(u32_metadata_path) => match u32_metadata_path.get(0) {
                Some(u32_metadata_uuid) => {
                    let u32_metadata_uuid = match Uuid::parse_str(u32_metadata_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(
                                u32_metadata_uuid.to_string(),
                            ))
                        }
                    };
                    let u32_metadata_reader = match blockfile_provider
                        .open::<u32, RoaringBitmap>(&u32_metadata_uuid)
                        .await
                    {
                        Ok(reader) => Some(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    };
                    u32_metadata_reader
                }
                None => None,
            },
            None => None,
        };
        let u32_metadata_index_reader = match u32_metadata_reader {
            Some(reader) => Some(MetadataIndexReader::new_u32(reader)),
            None => None,
        };

        let f32_metadata_reader = match segment.file_path.get(F32_METADATA) {
            Some(f32_metadata_path) => match f32_metadata_path.get(0) {
                Some(f32_metadata_uuid) => {
                    let f32_metadata_uuid = match Uuid::parse_str(f32_metadata_uuid) {
                        Ok(uuid) => uuid,
                        Err(_) => {
                            return Err(MetadataSegmentError::UuidParseError(
                                f32_metadata_uuid.to_string(),
                            ))
                        }
                    };
                    let f32_metadata_reader = match blockfile_provider
                        .open::<f32, RoaringBitmap>(&f32_metadata_uuid)
                        .await
                    {
                        Ok(reader) => Some(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    };
                    f32_metadata_reader
                }
                None => None,
            },
            None => None,
        };
        let f32_metadata_index_reader = match f32_metadata_reader {
            Some(reader) => Some(MetadataIndexReader::new_f32(reader)),
            None => None,
        };

        Ok(MetadataSegmentReader {
            full_text_index_reader,
            string_metadata_index_reader,
            bool_metadata_index_reader,
            f32_metadata_index_reader,
            u32_metadata_index_reader,
        })
    }

    pub async fn query(
        &self,
        where_clause: Option<&Where>,
        where_document_clause: Option<&WhereDocument>,
        _allowed_ids: Option<&Vec<usize>>,
        limit: usize,
        offset: usize,
    ) -> Result<Option<Vec<usize>>, MetadataSegmentError> {
        if limit != 0 || offset != 0 {
            return Err(MetadataSegmentError::LimitOffsetNotSupported);
        }
        // TODO we can do lots of clever query planning here. For now, just
        // run through the Where and WhereDocument clauses sequentially.
        let where_results = match where_clause {
            Some(where_clause) => {
                match self.process_where_clause(where_clause).await.map_err(|e| e) {
                    Ok(results) => {
                        tracing::info!(
                            "Filtered {} records from metadata segment based on where clause",
                            results.len()
                        );
                        Some(results)
                    }
                    Err(e) => {
                        tracing::error!(
                            "Error fetching results from metadata segment based on where clause {:?}",
                            e
                        );
                        return Err(MetadataSegmentError::MetadataIndexQueryError(e));
                    }
                }
            }
            None => {
                tracing::info!("No where clause to filter anything from metadata segment");
                None
            }
        };
        // Where and WhereDocument are implicitly ANDed, so if we have nothing
        // for the Where query we can just return.
        match &where_results {
            Some(results) => {
                if results.is_empty() {
                    return Ok(where_results);
                }
            }
            None => (),
        };
        let where_document_results = match where_document_clause {
            Some(where_document_clause) => {
                match self
                    .process_where_document_clause(where_document_clause)
                    .await
                {
                    Ok(results) => {
                        tracing::info!(
                            "Filtered {} records from metadata segment based on where document",
                            results.len()
                        );
                        Some(results)
                    }
                    Err(e) => {
                        tracing::error!(
                            "Error fetching results from metadata segment based on where clause {:?}",
                            e
                        );
                        return Err(MetadataSegmentError::MetadataIndexQueryError(e));
                    }
                }
            }
            None => {
                tracing::info!("No where document to filter anything from metadata segment");
                None
            }
        };
        match &where_document_results {
            Some(results) => {
                if results.is_empty() {
                    return Ok(where_document_results);
                }
            }
            None => (),
        };

        if where_results.is_none() && where_document_results.is_none() {
            return Ok(None);
        } else if where_results.is_none() && where_document_results.is_some() {
            return Ok(where_document_results);
        } else if where_results.is_some() && where_document_results.is_none() {
            return Ok(where_results);
        } else {
            return Ok(Some(merge_sorted_vecs_conjunction(
                &where_results.expect("Checked just now that it is not none"),
                &where_document_results.expect("Checked just now that it is not none"),
            )));
        }
    }

    fn process_where_clause<'me>(
        &'me self,
        where_clause: &'me Where,
    ) -> BoxFuture<Result<Vec<usize>, MetadataIndexError>> {
        async move {
            let mut results = vec![];
            match where_clause {
                Where::DirectWhereComparison(direct_where_comparison) => {
                    match &direct_where_comparison.comparison {
                        WhereComparison::SingleStringComparison(operand, comparator) => {
                            match comparator {
                                WhereClauseComparator::Equal => {
                                    let metadata_value_keywrapper = operand.as_str().try_into();
                                    match metadata_value_keywrapper {
                                        Ok(keywrapper) => {
                                            match &self.string_metadata_index_reader {
                                                Some(reader) => {
                                                    let result = reader
                                                        .get(
                                                            &direct_where_comparison.key,
                                                            &keywrapper,
                                                        )
                                                        .await;
                                                    match result {
                                                        Ok(r) => {
                                                            results = r
                                                                .iter()
                                                                .map(|x| x as usize)
                                                                .collect();
                                                        }
                                                        Err(e) => {
                                                            return Err(e);
                                                        }
                                                    }
                                                }
                                                // This is expected. Before the first ever compaction
                                                // the reader will be uninitialized, hence an empty vector
                                                // here since nothing has been written to storage yet.
                                                None => results = vec![],
                                            }
                                        }
                                        Err(_) => {
                                            panic!("Error converting string to keywrapper")
                                        }
                                    }
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
                            }
                        }
                        WhereComparison::SingleBoolComparison(operand, comparator) => {
                            match comparator {
                                WhereClauseComparator::Equal => {
                                    let metadata_value_keywrapper = (*operand).try_into();
                                    match metadata_value_keywrapper {
                                        Ok(keywrapper) => {
                                            match &self.bool_metadata_index_reader {
                                                Some(reader) => {
                                                    let result = reader
                                                        .get(
                                                            &direct_where_comparison.key,
                                                            &keywrapper,
                                                        )
                                                        .await;
                                                    match result {
                                                        Ok(r) => {
                                                            results = r
                                                                .iter()
                                                                .map(|x| x as usize)
                                                                .collect();
                                                        }
                                                        Err(e) => {
                                                            return Err(e);
                                                        }
                                                    }
                                                }
                                                // This is expected. Before the first ever compaction
                                                // the reader will be uninitialized, hence an empty vector
                                                // here since nothing has been written to storage yet.
                                                None => results = vec![],
                                            }
                                        }
                                        Err(_) => {
                                            panic!("Error converting bool to keywrapper")
                                        }
                                    }
                                }
                                WhereClauseComparator::NotEqual => {
                                    todo!();
                                }
                                // We don't allow these comparators for bools.
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
                            }
                        }
                        WhereComparison::SingleIntComparison(operand, comparator) => {
                            match comparator {
                                WhereClauseComparator::Equal => {
                                    let metadata_value_keywrapper = (*operand).try_into();
                                    match metadata_value_keywrapper {
                                        Ok(keywrapper) => {
                                            match &self.u32_metadata_index_reader {
                                                Some(reader) => {
                                                    let result = reader
                                                        .get(
                                                            &direct_where_comparison.key,
                                                            &keywrapper,
                                                        )
                                                        .await;
                                                    match result {
                                                        Ok(r) => {
                                                            results = r
                                                                .iter()
                                                                .map(|x| x as usize)
                                                                .collect();
                                                        }
                                                        Err(e) => {
                                                            return Err(e);
                                                        }
                                                    }
                                                }
                                                // This is expected. Before the first ever compaction
                                                // the reader will be uninitialized, hence an empty vector
                                                // here since nothing has been written to storage yet.
                                                None => results = vec![],
                                            }
                                        }
                                        Err(_) => {
                                            panic!("Error converting int to keywrapper")
                                        }
                                    }
                                }
                                WhereClauseComparator::NotEqual => {
                                    todo!();
                                }
                                WhereClauseComparator::LessThan => {
                                    let metadata_value_keywrapper = (*operand).try_into();
                                    match metadata_value_keywrapper {
                                        Ok(keywrapper) => {
                                            match &self.u32_metadata_index_reader {
                                                Some(reader) => {
                                                    let result = reader
                                                        .lt(
                                                            &direct_where_comparison.key,
                                                            &keywrapper,
                                                        )
                                                        .await;
                                                    match result {
                                                        Ok(r) => {
                                                            results = r
                                                                .iter()
                                                                .map(|x| x as usize)
                                                                .collect();
                                                        }
                                                        Err(e) => {
                                                            return Err(e);
                                                        }
                                                    }
                                                }
                                                // This is expected. Before the first ever compaction
                                                // the reader will be uninitialized, hence an empty vector
                                                // here since nothing has been written to storage yet.
                                                None => results = vec![],
                                            }
                                        }
                                        Err(_) => {
                                            panic!("Error converting int to keywrapper")
                                        }
                                    }
                                }
                                WhereClauseComparator::LessThanOrEqual => {
                                    let metadata_value_keywrapper = (*operand).try_into();
                                    match metadata_value_keywrapper {
                                        Ok(keywrapper) => {
                                            match &self.u32_metadata_index_reader {
                                                Some(reader) => {
                                                    let result = reader
                                                        .lte(
                                                            &direct_where_comparison.key,
                                                            &keywrapper,
                                                        )
                                                        .await;
                                                    match result {
                                                        Ok(r) => {
                                                            results = r
                                                                .iter()
                                                                .map(|x| x as usize)
                                                                .collect();
                                                        }
                                                        Err(e) => {
                                                            return Err(e);
                                                        }
                                                    }
                                                }
                                                // This is expected. Before the first ever compaction
                                                // the reader will be uninitialized, hence an empty vector
                                                // here since nothing has been written to storage yet.
                                                None => results = vec![],
                                            }
                                        }
                                        Err(_) => {
                                            panic!("Error converting int to keywrapper")
                                        }
                                    }
                                }
                                WhereClauseComparator::GreaterThan => {
                                    let metadata_value_keywrapper = (*operand).try_into();
                                    match metadata_value_keywrapper {
                                        Ok(keywrapper) => {
                                            match &self.u32_metadata_index_reader {
                                                Some(reader) => {
                                                    let result = reader
                                                        .gt(
                                                            &direct_where_comparison.key,
                                                            &keywrapper,
                                                        )
                                                        .await;
                                                    match result {
                                                        Ok(r) => {
                                                            results = r
                                                                .iter()
                                                                .map(|x| x as usize)
                                                                .collect();
                                                        }
                                                        Err(e) => {
                                                            return Err(e);
                                                        }
                                                    }
                                                }
                                                // This is expected. Before the first ever compaction
                                                // the reader will be uninitialized, hence an empty vector
                                                // here since nothing has been written to storage yet.
                                                None => results = vec![],
                                            }
                                        }
                                        Err(_) => {
                                            panic!("Error converting int to keywrapper")
                                        }
                                    }
                                }
                                WhereClauseComparator::GreaterThanOrEqual => {
                                    let metadata_value_keywrapper = (*operand).try_into();
                                    match metadata_value_keywrapper {
                                        Ok(keywrapper) => {
                                            match &self.u32_metadata_index_reader {
                                                Some(reader) => {
                                                    let result = reader
                                                        .gte(
                                                            &direct_where_comparison.key,
                                                            &keywrapper,
                                                        )
                                                        .await;
                                                    match result {
                                                        Ok(r) => {
                                                            results = r
                                                                .iter()
                                                                .map(|x| x as usize)
                                                                .collect();
                                                        }
                                                        Err(e) => {
                                                            return Err(e);
                                                        }
                                                    }
                                                }
                                                // This is expected. Before the first ever compaction
                                                // the reader will be uninitialized, hence an empty vector
                                                // here since nothing has been written to storage yet.
                                                None => results = vec![],
                                            }
                                        }
                                        Err(_) => {
                                            panic!("Error converting int to keywrapper")
                                        }
                                    }
                                }
                            }
                        }
                        WhereComparison::SingleDoubleComparison(operand, comparator) => {
                            match comparator {
                                WhereClauseComparator::Equal => {
                                    let metadata_value_keywrapper = (*operand as f32).try_into();
                                    match metadata_value_keywrapper {
                                        Ok(keywrapper) => {
                                            match &self.f32_metadata_index_reader {
                                                Some(reader) => {
                                                    let result = reader
                                                        .get(
                                                            &direct_where_comparison.key,
                                                            &keywrapper,
                                                        )
                                                        .await;
                                                    match result {
                                                        Ok(r) => {
                                                            results = r
                                                                .iter()
                                                                .map(|x| x as usize)
                                                                .collect();
                                                        }
                                                        Err(e) => {
                                                            return Err(e);
                                                        }
                                                    }
                                                }
                                                // This is expected. Before the first ever compaction
                                                // the reader will be uninitialized, hence an empty vector
                                                // here since nothing has been written to storage yet.
                                                None => results = vec![],
                                            }
                                        }
                                        Err(_) => {
                                            panic!("Error converting double to keywrapper")
                                        }
                                    }
                                }
                                WhereClauseComparator::NotEqual => {
                                    todo!();
                                }
                                WhereClauseComparator::LessThan => {
                                    let metadata_value_keywrapper = (*operand as f32).try_into();
                                    match metadata_value_keywrapper {
                                        Ok(keywrapper) => {
                                            match &self.f32_metadata_index_reader {
                                                Some(reader) => {
                                                    let result = reader
                                                        .lt(
                                                            &direct_where_comparison.key,
                                                            &keywrapper,
                                                        )
                                                        .await;
                                                    match result {
                                                        Ok(r) => {
                                                            results = r
                                                                .iter()
                                                                .map(|x| x as usize)
                                                                .collect();
                                                        }
                                                        Err(e) => {
                                                            return Err(e);
                                                        }
                                                    }
                                                }
                                                // This is expected. Before the first ever compaction
                                                // the reader will be uninitialized, hence an empty vector
                                                // here since nothing has been written to storage yet.
                                                None => results = vec![],
                                            }
                                        }
                                        Err(_) => {
                                            panic!("Error converting double to keywrapper")
                                        }
                                    }
                                }
                                WhereClauseComparator::LessThanOrEqual => {
                                    let metadata_value_keywrapper = (*operand as f32).try_into();
                                    match metadata_value_keywrapper {
                                        Ok(keywrapper) => {
                                            match &self.f32_metadata_index_reader {
                                                Some(reader) => {
                                                    let result = reader
                                                        .lte(
                                                            &direct_where_comparison.key,
                                                            &keywrapper,
                                                        )
                                                        .await;
                                                    match result {
                                                        Ok(r) => {
                                                            results = r
                                                                .iter()
                                                                .map(|x| x as usize)
                                                                .collect();
                                                        }
                                                        Err(e) => {
                                                            return Err(e);
                                                        }
                                                    }
                                                }
                                                // This is expected. Before the first ever compaction
                                                // the reader will be uninitialized, hence an empty vector
                                                // here since nothing has been written to storage yet.
                                                None => results = vec![],
                                            }
                                        }
                                        Err(_) => {
                                            panic!("Error converting double to keywrapper")
                                        }
                                    }
                                }
                                WhereClauseComparator::GreaterThan => {
                                    let metadata_value_keywrapper = (*operand as f32).try_into();
                                    match metadata_value_keywrapper {
                                        Ok(keywrapper) => {
                                            match &self.f32_metadata_index_reader {
                                                Some(reader) => {
                                                    let result = reader
                                                        .gt(
                                                            &direct_where_comparison.key,
                                                            &keywrapper,
                                                        )
                                                        .await;
                                                    match result {
                                                        Ok(r) => {
                                                            results = r
                                                                .iter()
                                                                .map(|x| x as usize)
                                                                .collect();
                                                        }
                                                        Err(e) => {
                                                            return Err(e);
                                                        }
                                                    }
                                                }
                                                // This is expected. Before the first ever compaction
                                                // the reader will be uninitialized, hence an empty vector
                                                // here since nothing has been written to storage yet.
                                                None => results = vec![],
                                            }
                                        }
                                        Err(_) => {
                                            panic!("Error converting double to keywrapper")
                                        }
                                    }
                                }
                                WhereClauseComparator::GreaterThanOrEqual => {
                                    let metadata_value_keywrapper = (*operand as f32).try_into();
                                    match metadata_value_keywrapper {
                                        Ok(keywrapper) => {
                                            match &self.f32_metadata_index_reader {
                                                Some(reader) => {
                                                    let result = reader
                                                        .gte(
                                                            &direct_where_comparison.key,
                                                            &keywrapper,
                                                        )
                                                        .await;
                                                    match result {
                                                        Ok(r) => {
                                                            results = r
                                                                .iter()
                                                                .map(|x| x as usize)
                                                                .collect();
                                                        }
                                                        Err(e) => {
                                                            return Err(e);
                                                        }
                                                    }
                                                }
                                                // This is expected. Before the first ever compaction
                                                // the reader will be uninitialized, hence an empty vector
                                                // here since nothing has been written to storage yet.
                                                None => results = vec![],
                                            }
                                        }
                                        Err(_) => {
                                            panic!("Error converting double to keywrapper")
                                        }
                                    }
                                }
                            }
                        }
                        WhereComparison::StringListComparison(operand, list_operator) => {
                            todo!();
                        }
                        WhereComparison::IntListComparison(..) => {
                            todo!();
                        }
                        WhereComparison::DoubleListComparison(..) => {
                            todo!();
                        }
                        WhereComparison::BoolListComparison(..) => {
                            todo!();
                        }
                    }
                }
                Where::WhereChildren(where_children) => {
                    let mut first_iteration = true;
                    for child in where_children.children.iter() {
                        let child_results: Vec<usize> =
                            match self.process_where_clause(&child).await {
                                Ok(result) => result,
                                Err(_) => vec![],
                            };
                        if first_iteration {
                            results = child_results;
                            first_iteration = false;
                        } else {
                            match where_children.operator {
                                BooleanOperator::And => {
                                    results =
                                        merge_sorted_vecs_conjunction(&results, &child_results);
                                }
                                BooleanOperator::Or => {
                                    results =
                                        merge_sorted_vecs_disjunction(&results, &child_results);
                                }
                            }
                        }
                    }
                }
            }
            return Ok(results);
        }
        .boxed()
    }

    fn process_where_document_clause<'me>(
        &'me self,
        where_document_clause: &'me WhereDocument,
    ) -> BoxFuture<Result<Vec<usize>, MetadataIndexError>> {
        async move {
            let mut results = vec![];
            match where_document_clause {
                WhereDocument::DirectWhereDocumentComparison(direct_document_comparison) => {
                    match &direct_document_comparison.operator {
                        WhereDocumentOperator::Contains => {
                            match &self.full_text_index_reader {
                                Some(reader) => {
                                    let result =
                                        reader.search(&direct_document_comparison.document).await;
                                    match result {
                                        Ok(r) => {
                                            results = r.iter().map(|x| *x as usize).collect();
                                        }
                                        Err(e) => {
                                            return Err(MetadataIndexError::FullTextError(e));
                                        }
                                    }
                                }
                                // This is expected. Before the first ever compaction
                                // the reader will be uninitialized, hence an empty vector
                                // here since nothing has been written to storage yet.
                                None => results = vec![],
                            }
                        }
                        WhereDocumentOperator::NotContains => {
                            todo!();
                        }
                    }
                }
                WhereDocument::WhereDocumentChildren(where_document_children) => {
                    let mut first_iteration = true;
                    for child in where_document_children.children.iter() {
                        let child_results: Vec<usize> =
                            match self.process_where_document_clause(&child).await {
                                Ok(result) => result,
                                Err(_) => vec![],
                            };
                        if first_iteration {
                            results = child_results;
                            first_iteration = false;
                        } else {
                            match where_document_children.operator {
                                BooleanOperator::And => {
                                    results =
                                        merge_sorted_vecs_conjunction(&results, &child_results);
                                }
                                BooleanOperator::Or => {
                                    results =
                                        merge_sorted_vecs_disjunction(&results, &child_results);
                                }
                            }
                        }
                    }
                }
            }
            results.sort();
            return Ok(results);
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use crate::segment::{
        metadata_segment::{MetadataSegmentReader, MetadataSegmentWriter},
        record_segment::{
            RecordSegmentReader, RecordSegmentReaderCreationError, RecordSegmentWriter,
        },
        LogMaterializer, SegmentFlusher, SegmentWriter,
    };
    use chroma_blockstore::{
        arrow::{config::TEST_MAX_BLOCK_SIZE_BYTES, provider::ArrowBlockfileProvider},
        provider::BlockfileProvider,
    };
    use chroma_cache::{
        cache::Cache,
        config::{CacheConfig, UnboundedCacheConfig},
    };
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{
        Chunk, DirectComparison, DirectDocumentComparison, LogRecord, MetadataValue, Operation,
        OperationRecord, UpdateMetadataValue, Where, WhereComparison, WhereDocument,
    };
    use std::{collections::HashMap, str::FromStr};
    use uuid::Uuid;

    #[tokio::test]
    async fn empty_blocks() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let sparse_index_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
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
        let materializer = LogMaterializer::new(Some(record_segment_reader), data, None);
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
        let materializer = LogMaterializer::new(Some(record_segment_reader), data, None);
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
        let block_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let sparse_index_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
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
        let materializer = LogMaterializer::new(Some(record_segment_reader), data, None);
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
        // Search by f32 metadata value first.
        let metadata_segment_reader =
            MetadataSegmentReader::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Metadata segment reader construction failed");
        let where_clause = Where::DirectWhereComparison(DirectComparison {
            key: String::from("hello"),
            comparison: WhereComparison::SingleDoubleComparison(
                1.0,
                chroma_types::WhereClauseComparator::Equal,
            ),
        });
        let res = metadata_segment_reader
            .query(Some(&where_clause), None, None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0), Some(&(2 as usize)));
        let where_clause = Where::DirectWhereComparison(DirectComparison {
            key: String::from("hello"),
            comparison: WhereComparison::SingleStringComparison(
                String::from("new world"),
                chroma_types::WhereClauseComparator::Equal,
            ),
        });
        let res = metadata_segment_reader
            .query(Some(&where_clause), None, None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0), Some(&(1 as usize)));
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
        assert_eq!(res.get(0).as_ref().unwrap().metadata, Some(id1_mt));
        let mut id2_mt = HashMap::new();
        id2_mt.insert(String::from("hello"), MetadataValue::Float(1.0));
        assert_eq!(res.get(1).as_ref().unwrap().metadata, Some(id2_mt));
    }

    #[tokio::test]
    async fn metadata_deletes() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let sparse_index_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
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
                    document: Some(String::from("This is a document about cats.")),
                    operation: Operation::Add,
                },
            }];
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
        let materializer = LogMaterializer::new(Some(record_segment_reader), data, None);
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
        // Only one key should be present.
        let metadata_segment_reader =
            MetadataSegmentReader::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Metadata segment reader construction failed");
        let where_clause = Where::DirectWhereComparison(DirectComparison {
            key: String::from("hello"),
            comparison: WhereComparison::SingleStringComparison(
                String::from("world"),
                chroma_types::WhereClauseComparator::Equal,
            ),
        });
        let res = metadata_segment_reader
            .query(Some(&where_clause), None, None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 0);
        let where_clause = Where::DirectWhereComparison(DirectComparison {
            key: String::from("bye"),
            comparison: WhereComparison::SingleStringComparison(
                String::from("world"),
                chroma_types::WhereClauseComparator::Equal,
            ),
        });
        let res = metadata_segment_reader
            .query(Some(&where_clause), None, None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0), Some(&(1 as usize)));
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
        assert_eq!(res.get(0).as_ref().unwrap().metadata, Some(id1_mt));
    }

    #[tokio::test]
    async fn document_updates() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let sparse_index_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
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
        let materializer = LogMaterializer::new(Some(record_segment_reader), data, None);
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
        // FTS for hello should return empty.
        let metadata_segment_reader =
            MetadataSegmentReader::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .expect("Metadata segment reader construction failed");
        let where_document_clause =
            WhereDocument::DirectWhereDocumentComparison(DirectDocumentComparison {
                document: String::from("hello"),
                operator: chroma_types::WhereDocumentOperator::Contains,
            });
        let res = metadata_segment_reader
            .query(None, Some(&where_document_clause), None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 0);
        // FTS for bye should return the lone document.
        let where_document_clause =
            WhereDocument::DirectWhereDocumentComparison(DirectDocumentComparison {
                document: String::from("bye"),
                operator: chroma_types::WhereDocumentOperator::Contains,
            });
        let res = metadata_segment_reader
            .query(None, Some(&where_document_clause), None, 0, 0)
            .await
            .expect("Metadata segment query failed")
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0), Some(&(1 as usize)));
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
            res.get(0).as_ref().unwrap().document,
            Some(String::from("bye").as_str())
        );
    }
}
