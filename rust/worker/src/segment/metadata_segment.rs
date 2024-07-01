use arrow::array::Int32Array;
use async_trait::async_trait;
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

use super::record_segment::ApplyMaterializedLogError;
use super::types::{MaterializedLogRecord, SegmentWriter};
use super::SegmentFlusher;
use crate::blockstore::key::KeyWrapper;
use crate::blockstore::provider::{BlockfileProvider, CreateError, OpenError};
use crate::errors::{ChromaError, ErrorCodes};
use crate::index::fulltext::tokenizer::TantivyChromaTokenizer;
use crate::index::fulltext::types::{
    process_where_document_clause_with_callback, FullTextIndexError, FullTextIndexFlusher,
    FullTextIndexReader, FullTextIndexWriter,
};
use crate::index::metadata::types::{
    process_where_clause_with_callback, MetadataIndexError, MetadataIndexFlusher,
    MetadataIndexReader, MetadataIndexWriter,
};
use crate::types::{
    BooleanOperator, MetadataValue, Operation, Segment, Where, WhereClauseComparator,
    WhereDocument, WhereDocumentOperator,
};
use crate::types::{SegmentType, WhereComparison};
use crate::utils::{merge_sorted_vecs_conjunction, merge_sorted_vecs_disjunction};

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
}

impl<'log_records> SegmentWriter<'log_records> for MetadataSegmentWriter<'_> {
    async fn apply_materialized_log_chunk(
        &self,
        records: crate::execution::data::data_chunk::Chunk<MaterializedLogRecord<'log_records>>,
    ) -> Result<(), ApplyMaterializedLogError> {
        for record in records.iter() {
            let segment_offset_id = record.0.offset_id;
            match record.0.final_operation {
                Operation::Add => {
                    // We can ignore record.0.metadata_to_be_deleted
                    // for fresh adds. TODO on whether to propagate error.
                    match &record.0.metadata_to_be_merged {
                        Some(metadata) => {
                            for (key, value) in metadata.iter() {
                                match value {
                                    MetadataValue::Str(value) => {
                                        match &self.string_metadata_index_writer {
                                            Some(writer) => {
                                                let a = writer
                                                    .set(key, value.as_str(), segment_offset_id)
                                                    .await;
                                            }
                                            None => {}
                                        }
                                    }
                                    MetadataValue::Float(value) => {
                                        match &self.f32_metadata_index_writer {
                                            Some(writer) => {
                                                let _ = writer
                                                    .set(key, *value as f32, segment_offset_id)
                                                    .await;
                                            }
                                            None => {}
                                        }
                                    }
                                    MetadataValue::Int(value) => {
                                        match &self.u32_metadata_index_writer {
                                            Some(writer) => {
                                                let _ = writer
                                                    .set(key, *value as u32, segment_offset_id)
                                                    .await;
                                            }
                                            None => {}
                                        }
                                    }
                                    MetadataValue::Bool(value) => {
                                        match &self.bool_metadata_index_writer {
                                            Some(writer) => {
                                                let _ = writer
                                                    .set(key, *value, segment_offset_id)
                                                    .await;
                                            }
                                            None => {}
                                        }
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
                            None => {}
                        },
                        None => {}
                    };
                }
                Operation::Delete => match &record.0.data_record {
                    Some(data_record) => {
                        match &data_record.metadata {
                            Some(metadata) => {
                                for (key, value) in metadata.iter() {
                                    match value {
                                        MetadataValue::Str(value) => {
                                            match &self.string_metadata_index_writer {
                                                Some(writer) => {
                                                    let _ = writer
                                                        .delete(
                                                            key,
                                                            value.as_str(),
                                                            segment_offset_id,
                                                        )
                                                        .await;
                                                }
                                                None => {
                                                    return Err(ApplyMaterializedLogError::BlockfileDeleteError);
                                                }
                                            }
                                        }
                                        MetadataValue::Float(value) => {
                                            match &self.f32_metadata_index_writer {
                                                Some(writer) => {
                                                    let _ = writer
                                                        .delete(
                                                            key,
                                                            *value as f32,
                                                            segment_offset_id,
                                                        )
                                                        .await;
                                                }
                                                None => {
                                                    return Err(ApplyMaterializedLogError::BlockfileDeleteError);
                                                }
                                            }
                                        }
                                        MetadataValue::Int(value) => {
                                            match &self.u32_metadata_index_writer {
                                                Some(writer) => {
                                                    let _ = writer
                                                        .delete(
                                                            key,
                                                            *value as u32,
                                                            segment_offset_id,
                                                        )
                                                        .await;
                                                }
                                                None => {
                                                    return Err(ApplyMaterializedLogError::BlockfileDeleteError);
                                                }
                                            }
                                        }
                                        MetadataValue::Bool(value) => {
                                            match &self.bool_metadata_index_writer {
                                                Some(writer) => {
                                                    let _ = writer
                                                        .delete(key, *value, segment_offset_id)
                                                        .await;
                                                }
                                                None => {
                                                    return Err(ApplyMaterializedLogError::BlockfileDeleteError);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            None => {}
                        };
                        match &data_record.document {
                            Some(document) => match &self.full_text_index_writer {
                                Some(writer) => {
                                    let _ =
                                        writer.delete_document(document, segment_offset_id).await;
                                }
                                None => {
                                    return Err(ApplyMaterializedLogError::BlockfileDeleteError);
                                }
                            },
                            None => {
                                return Err(ApplyMaterializedLogError::BlockfileDeleteError);
                            }
                        };
                    }
                    None => {}
                },
                Operation::Update => {
                    let metadata_delta = record.0.metadata_delta();
                    // Updates.
                    for (update_key, (old_value, new_value)) in metadata_delta.metadata_to_update {
                        match new_value {
                            MetadataValue::Str(new_val_str) => match old_value {
                                MetadataValue::Str(old_val_str) => {
                                    match &self.string_metadata_index_writer {
                                        Some(writer) => {
                                            match writer
                                                .update(
                                                    update_key,
                                                    old_val_str.as_str().into(),
                                                    new_val_str.as_str().into(),
                                                    segment_offset_id,
                                                )
                                                .await
                                            {
                                                Ok(()) => {}
                                                Err(e) => {
                                                    return Err(ApplyMaterializedLogError::BlockfileUpdateError);
                                                }
                                            }
                                        }
                                        None => {
                                            panic!("Invariant violation. String metadata index writer should be set");
                                        }
                                    }
                                }
                                _ => {
                                    return Err(ApplyMaterializedLogError::MetadataUpdateNotValid);
                                }
                            },
                            MetadataValue::Float(new_val_float) => match old_value {
                                MetadataValue::Float(old_val_float) => {
                                    match &self.f32_metadata_index_writer {
                                        Some(writer) => {
                                            match writer
                                                .update(
                                                    update_key,
                                                    (*old_val_float as f32).into(),
                                                    (*new_val_float as f32).into(),
                                                    segment_offset_id,
                                                )
                                                .await
                                            {
                                                Ok(()) => {}
                                                Err(e) => {
                                                    return Err(ApplyMaterializedLogError::BlockfileUpdateError);
                                                }
                                            }
                                        }
                                        None => {
                                            panic!("Invariant violation. Float metadata index writer should be set");
                                        }
                                    }
                                }
                                _ => {
                                    return Err(ApplyMaterializedLogError::MetadataUpdateNotValid);
                                }
                            },
                            MetadataValue::Int(new_val_int) => match old_value {
                                MetadataValue::Int(old_val_int) => {
                                    match &self.u32_metadata_index_writer {
                                        Some(writer) => {
                                            match writer
                                                .update(
                                                    update_key,
                                                    (*old_val_int as u32).into(),
                                                    (*new_val_int as u32).into(),
                                                    segment_offset_id,
                                                )
                                                .await
                                            {
                                                Ok(()) => {}
                                                Err(e) => {
                                                    return Err(ApplyMaterializedLogError::BlockfileUpdateError);
                                                }
                                            }
                                        }
                                        None => {
                                            panic!("Invariant violation. u32 metadata index writer should be set");
                                        }
                                    }
                                }
                                _ => {
                                    return Err(ApplyMaterializedLogError::MetadataUpdateNotValid);
                                }
                            },
                            MetadataValue::Bool(new_val_bool) => match old_value {
                                MetadataValue::Bool(old_val_bool) => {
                                    match &self.bool_metadata_index_writer {
                                        Some(writer) => {
                                            match writer
                                                .update(
                                                    update_key,
                                                    (*old_val_bool).into(),
                                                    (*new_val_bool).into(),
                                                    segment_offset_id,
                                                )
                                                .await
                                            {
                                                Ok(()) => {}
                                                Err(e) => {
                                                    return Err(ApplyMaterializedLogError::BlockfileUpdateError);
                                                }
                                            }
                                        }
                                        None => {
                                            panic!("Invariant violation. Bool metadata index writer should be set");
                                        }
                                    }
                                }
                                _ => {
                                    return Err(ApplyMaterializedLogError::MetadataUpdateNotValid);
                                }
                            },
                        }
                    }
                    // Inserts.
                    for (insert_key, new_value) in metadata_delta.metadata_to_insert {
                        match new_value {
                            MetadataValue::Str(new_val_str) => {
                                match &self.string_metadata_index_writer {
                                    Some(writer) => {
                                        match writer
                                            .set(
                                                insert_key,
                                                new_val_str.as_str(),
                                                segment_offset_id,
                                            )
                                            .await
                                        {
                                            Ok(()) => {}
                                            Err(e) => {
                                                return Err(
                                                    ApplyMaterializedLogError::BlockfileSetError,
                                                );
                                            }
                                        }
                                    }
                                    None => {
                                        panic!("Invariant violation. String metadata index writer should be set");
                                    }
                                }
                            }
                            MetadataValue::Float(new_val_float) => {
                                match &self.f32_metadata_index_writer {
                                    Some(writer) => {
                                        match writer
                                            .set(
                                                insert_key,
                                                *new_val_float as f32,
                                                segment_offset_id,
                                            )
                                            .await
                                        {
                                            Ok(()) => {}
                                            Err(e) => {
                                                return Err(
                                                    ApplyMaterializedLogError::BlockfileSetError,
                                                );
                                            }
                                        }
                                    }
                                    None => {
                                        panic!("Invariant violation. Float metadata index writer should be set");
                                    }
                                }
                            }
                            MetadataValue::Int(new_val_int) => {
                                match &self.u32_metadata_index_writer {
                                    Some(writer) => {
                                        match writer
                                            .set(insert_key, *new_val_int as u32, segment_offset_id)
                                            .await
                                        {
                                            Ok(()) => {}
                                            Err(e) => {
                                                return Err(
                                                    ApplyMaterializedLogError::BlockfileSetError,
                                                );
                                            }
                                        }
                                    }
                                    None => {
                                        panic!("Invariant violation. Int metadata index writer should be set");
                                    }
                                }
                            }
                            MetadataValue::Bool(new_val_bool) => {
                                match &self.bool_metadata_index_writer {
                                    Some(writer) => {
                                        match writer
                                            .set(insert_key, *new_val_bool, segment_offset_id)
                                            .await
                                        {
                                            Ok(()) => {}
                                            Err(e) => {
                                                return Err(
                                                    ApplyMaterializedLogError::BlockfileSetError,
                                                );
                                            }
                                        }
                                    }
                                    None => {
                                        panic!("Invariant violation. Bool metadata index writer should be set");
                                    }
                                }
                            }
                        }
                    }
                    // Deletes.
                    for (delete_key, old_value) in metadata_delta.metadata_to_delete {
                        match old_value {
                            MetadataValue::Str(old_val_str) => {
                                match &self.string_metadata_index_writer {
                                    Some(writer) => {
                                        match writer
                                            .delete(
                                                delete_key,
                                                old_val_str.as_str(),
                                                segment_offset_id,
                                            )
                                            .await
                                        {
                                            Ok(()) => {}
                                            Err(e) => {
                                                return Err(
                                                    ApplyMaterializedLogError::BlockfileDeleteError,
                                                );
                                            }
                                        }
                                    }
                                    None => {
                                        panic!("Invariant violation. String metadata index writer should be set");
                                    }
                                }
                            }
                            MetadataValue::Float(old_val_float) => {
                                match &self.f32_metadata_index_writer {
                                    Some(writer) => {
                                        match writer
                                            .delete(
                                                delete_key,
                                                *old_val_float as f32,
                                                segment_offset_id,
                                            )
                                            .await
                                        {
                                            Ok(()) => {}
                                            Err(e) => {
                                                return Err(
                                                    ApplyMaterializedLogError::BlockfileDeleteError,
                                                );
                                            }
                                        }
                                    }
                                    None => {
                                        panic!("Invariant violation. Float metadata index writer should be set");
                                    }
                                }
                            }
                            MetadataValue::Int(old_val_int) => {
                                match &self.u32_metadata_index_writer {
                                    Some(writer) => {
                                        match writer
                                            .delete(
                                                delete_key,
                                                *old_val_int as u32,
                                                segment_offset_id,
                                            )
                                            .await
                                        {
                                            Ok(()) => {}
                                            Err(e) => {
                                                return Err(
                                                    ApplyMaterializedLogError::BlockfileDeleteError,
                                                );
                                            }
                                        }
                                    }
                                    None => {
                                        panic!("Invariant violation. Int metadata index writer should be set");
                                    }
                                }
                            }
                            MetadataValue::Bool(old_val_bool) => {
                                match &self.bool_metadata_index_writer {
                                    Some(writer) => {
                                        match writer
                                            .set(delete_key, *old_val_bool, segment_offset_id)
                                            .await
                                        {
                                            Ok(()) => {}
                                            Err(e) => {
                                                return Err(
                                                    ApplyMaterializedLogError::BlockfileDeleteError,
                                                );
                                            }
                                        }
                                    }
                                    None => {
                                        panic!("Invariant violation. Bool metadata index writer should be set");
                                    }
                                }
                            }
                        }
                    }
                }
                Operation::Upsert => {
                    panic!("Invariant violation. There should be no upserts in materialized log");
                }
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
