use arrow::array::Int32Array;
use async_trait::async_trait;
use core::panic;
use futures::future::BoxFuture;
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
use crate::blockstore::provider::{BlockfileProvider, CreateError, OpenError};
use crate::errors::{ChromaError, ErrorCodes};
use crate::index::fulltext::tokenizer::TantivyChromaTokenizer;
use crate::index::fulltext::types::{
    process_where_document_clause_with_callback, FullTextIndexError, FullTextIndexFlusher, FullTextIndexReader,
    FullTextIndexWriter,
};
use crate::index::metadata::types::{
    process_where_clause_with_callback, MetadataIndexError, MetadataIndexFlusher, MetadataIndexReader,
    MetadataIndexWriter,
};
use crate::types::SegmentType;
use crate::types::{
    BooleanOperator, DirectComparison, MetadataValue, Operation, Segment, Where, WhereChildren,
    WhereClauseComparator, WhereClauseListOperator, WhereComparison, WhereDocument,
    WhereDocumentOperator,
};
use crate::utils::{merge_sorted_vecs_conjunction, merge_sorted_vecs_disjunction};

const FULL_TEXT_PLS: &str = "full_text_pls";
const FULL_TEXT_FREQS: &str = "full_text_freqs";
const STRING_METADATA: &str = "string_metadata";
const BOOL_METADATA: &str = "bool_metadata";
const F32_METADATA: &str = "f32_metadata";
const U32_METADATA: &str = "u32_metadata";

pub(crate) struct MetadataSegmentWriter {
    pub(crate) full_text_index_writer: Option<FullTextIndexWriter>,
    // TODO this needs a real lifetime. However doing it breaks the commit() method
    // for some reason? This works for now.
    pub(crate) string_metadata_index_writer: Option<MetadataIndexWriter>,
    pub(crate) bool_metadata_index_writer: Option<MetadataIndexWriter>,
    pub(crate) f32_metadata_index_writer: Option<MetadataIndexWriter>,
    pub(crate) u32_metadata_index_writer: Option<MetadataIndexWriter>,
}

impl Debug for MetadataSegmentWriter {
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
}

impl ChromaError for MetadataSegmentError {
    fn code(&self) -> ErrorCodes {
        // TODO
        ErrorCodes::Internal
    }
}

impl MetadataSegmentWriter {
    pub(crate) async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<MetadataSegmentWriter, MetadataSegmentError> {
        println!("Creating MetadataSegmentWriter from Segment");
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
        let pls_writer = match segment.file_path.get(FULL_TEXT_PLS) {
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
                    pls_writer
                }
                None => return Err(MetadataSegmentError::EmptyPathVector),
            },
            None => match blockfile_provider.create::<u32, &Int32Array>() {
                Ok(writer) => writer,
                Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
            },
        };
        let freqs_writer = match segment.file_path.get(FULL_TEXT_FREQS) {
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
                    freqs_writer
                }
                None => return Err(MetadataSegmentError::EmptyPathVector),
            },
            None => match blockfile_provider.create::<u32, u32>() {
                Ok(writer) => writer,
                Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
            },
        };
        let full_text_tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 3, false).unwrap(),
        )));
        let full_text_index_writer =
            FullTextIndexWriter::new(pls_writer, freqs_writer, full_text_tokenizer);

        let string_metadata_writer = match segment.file_path.get(STRING_METADATA) {
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
                    string_metadata_writer
                }
                None => return Err(MetadataSegmentError::EmptyPathVector),
            },
            None => match blockfile_provider.create::<&str, &RoaringBitmap>() {
                Ok(writer) => writer,
                Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
            },
        };
        let string_metadata_index_writer = MetadataIndexWriter::new_string(string_metadata_writer);

        let bool_metadata_writer = match segment.file_path.get(BOOL_METADATA) {
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
                    bool_metadata_writer
                }
                None => return Err(MetadataSegmentError::EmptyPathVector),
            },
            None => match blockfile_provider.create::<bool, &RoaringBitmap>() {
                Ok(writer) => writer,
                Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
            },
        };
        let bool_metadata_index_writer = MetadataIndexWriter::new_bool(bool_metadata_writer);

        let f32_metadata_writer = match segment.file_path.get(F32_METADATA) {
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
                    f32_metadata_writer
                }
                None => return Err(MetadataSegmentError::EmptyPathVector),
            },
            None => match blockfile_provider.create::<f32, &RoaringBitmap>() {
                Ok(writer) => writer,
                Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
            },
        };
        let f32_metadata_index_writer = MetadataIndexWriter::new_f32(f32_metadata_writer);

        let u32_metadata_writer = match segment.file_path.get(U32_METADATA) {
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
                    u32_metadata_writer
                }
                None => return Err(MetadataSegmentError::EmptyPathVector),
            },
            None => match blockfile_provider.create::<u32, &RoaringBitmap>() {
                Ok(writer) => writer,
                Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
            },
        };
        let u32_metadata_index_writer = MetadataIndexWriter::new_u32(u32_metadata_writer);

        Ok(MetadataSegmentWriter {
            full_text_index_writer: Some(full_text_index_writer),
            string_metadata_index_writer: Some(string_metadata_index_writer),
            bool_metadata_index_writer: Some(bool_metadata_index_writer),
            f32_metadata_index_writer: Some(f32_metadata_index_writer),
            u32_metadata_index_writer: Some(u32_metadata_index_writer),
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

// TODO(Sanket): Implement this for updates/upserts/deletes.
impl<'a> SegmentWriter<'a> for MetadataSegmentWriter {
    async fn apply_materialized_log_chunk(
        &self,
        records: crate::execution::data::data_chunk::Chunk<MaterializedLogRecord<'a>>,
    ) -> Result<(), ApplyMaterializedLogError> {
        for record in records.iter() {
            let segment_offset_id = record.0.offset_id;
            match record.0.final_operation {
                Operation::Add => {
                    match &record.0.metadata_to_be_merged {
                        Some(metadata) => {
                            for (key, value) in metadata.iter() {
                                match value {
                                    MetadataValue::Str(value) => {
                                        match &self.string_metadata_index_writer {
                                            Some(writer) => {
                                                let _ = writer.set(
                                                    key,
                                                    value.as_str(),
                                                    segment_offset_id,
                                                );
                                            }
                                            None => {}
                                        }
                                    }
                                    MetadataValue::Float(value) => {
                                        match &self.f32_metadata_index_writer {
                                            Some(writer) => {
                                                let _ = writer.set(
                                                    key,
                                                    *value as f32,
                                                    segment_offset_id,
                                                );
                                            }
                                            None => {}
                                        }
                                    }
                                    MetadataValue::Int(value) => {
                                        match &self.u32_metadata_index_writer {
                                            Some(writer) => {
                                                let _ = writer.set(
                                                    key,
                                                    *value as u32,
                                                    segment_offset_id,
                                                );
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
                                let _ = writer.add_document(document, segment_offset_id as i32);
                            }
                            None => {}
                        },
                        None => {}
                    }
                }
                _ => todo!(),
            }
        }
        Ok(())
    }

    fn commit(self) -> Result<impl SegmentFlusher, Box<dyn ChromaError>> {
        let full_text_flusher = match self.full_text_index_writer {
            Some(flusher) => flusher.commit()?,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let string_metadata_flusher = match self.string_metadata_index_writer {
            Some(flusher) => flusher.commit()?,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let bool_metadata_flusher = match self.bool_metadata_index_writer {
            Some(flusher) => flusher.commit()?,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let f32_metadata_flusher = match self.f32_metadata_index_writer {
            Some(flusher) => flusher.commit()?,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let u32_metadata_flusher = match self.u32_metadata_index_writer {
            Some(flusher) => flusher.commit()?,
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

        self.full_text_index_flusher.flush().await.map_err(|e| e)?;
        flushed.insert(
            FULL_TEXT_PLS.to_string(),
            vec![full_text_pls_id.to_string()],
        );
        flushed.insert(
            FULL_TEXT_FREQS.to_string(),
            vec![full_text_freqs_id.to_string()],
        );

        self.bool_metadata_index_flusher
            .flush()
            .await
            .map_err(|e| e)?;
        flushed.insert(
            BOOL_METADATA.to_string(),
            vec![bool_metadata_id.to_string()],
        );

        self.f32_metadata_index_flusher
            .flush()
            .await
            .map_err(|e| e)?;
        flushed.insert(F32_METADATA.to_string(), vec![f32_metadata_id.to_string()]);

        self.u32_metadata_index_flusher
            .flush()
            .await
            .map_err(|e| e)?;
        flushed.insert(U32_METADATA.to_string(), vec![u32_metadata_id.to_string()]);

        self.string_metadata_index_flusher
            .flush()
            .await
            .map_err(|e| e)?;
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
                    NgramTokenizer::new(1, 3, false).unwrap(),
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
        allowed_ids: Option<&Vec<usize>>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<usize>, MetadataSegmentError> {
        if limit != 0 || offset != 0 {
            return Err(MetadataSegmentError::LimitOffsetNotSupported);
        }
        // TODO we can do lots of clever query planning here. For now, just
        // run through the Where and WhereDocument clauses sequentially.
        let where_results = match where_clause {
            Some(where_clause) => match self.process_where_clause(where_clause).map_err(|e| e) {
                Ok(results) => results,
                Err(e) => return Err(MetadataSegmentError::MetadataIndexQueryError(e)),
            },
            None => {
                vec![]
            }
        };
        // Where and WhereDocument are implicitly ANDed, so if we have nothing
        // for the Where query we can just return.
        if where_results.is_empty() {
            return Ok(where_results);
        }

        let where_document_results = match where_document_clause {
            Some(where_document_clause) => {
                match self.process_where_document_clause(where_document_clause) {
                    Ok(results) => results,
                    Err(e) => return Err(MetadataSegmentError::MetadataIndexQueryError(e)),
                }
            }
            None => {
                vec![]
            }
        };
        if where_document_results.is_empty() {
            return Ok(where_document_results);
        }

        Ok(merge_sorted_vecs_conjunction(
            where_results,
            where_document_results,
        ))
    }

    fn process_where_clause(&self, where_clause: &Where) -> Result<Vec<usize>, MetadataIndexError> {
        let clo = |metadata_key: &str,
                   metadata_value: &crate::blockstore::key::KeyWrapper,
                   metadata_type: crate::types::MetadataType,
                   comparator: WhereClauseComparator| {
            match metadata_type {
                crate::types::MetadataType::StringType => match comparator {
                    WhereClauseComparator::Equal => {
                        let result = match &self.string_metadata_index_reader {
                            Some(reader) => futures::executor::block_on(
                                reader.get(metadata_key, metadata_value),
                            ),
                            None => Ok(RoaringBitmap::new()),
                        };
                        match result {
                            Ok(result) => {
                                return result;
                            }
                            Err(_) => {
                                panic!("Error querying metadata index")
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
                },
                crate::types::MetadataType::IntType => match comparator {
                    WhereClauseComparator::Equal => {
                        let result = match &self.u32_metadata_index_reader {
                            Some(reader) => futures::executor::block_on(
                                reader.get(metadata_key, metadata_value),
                            ),
                            None => Ok(RoaringBitmap::new()),
                        };
                        match result {
                            Ok(result) => {
                                return result;
                            }
                            Err(_) => {
                                panic!("Error querying metadata index")
                            }
                        }
                    }
                    WhereClauseComparator::NotEqual => {
                        todo!();
                    }
                    WhereClauseComparator::LessThan => {
                        let result = match &self.u32_metadata_index_reader {
                            Some(reader) => {
                                futures::executor::block_on(reader.lt(metadata_key, metadata_value))
                            }
                            None => Ok(RoaringBitmap::new()),
                        };
                        match result {
                            Ok(result) => {
                                return result;
                            }
                            Err(_) => {
                                panic!("Error querying metadata index")
                            }
                        }
                    }
                    WhereClauseComparator::LessThanOrEqual => {
                        let result = match &self.u32_metadata_index_reader {
                            Some(reader) => futures::executor::block_on(
                                reader.lte(metadata_key, metadata_value),
                            ),
                            None => Ok(RoaringBitmap::new()),
                        };
                        match result {
                            Ok(result) => {
                                return result;
                            }
                            Err(_) => {
                                panic!("Error querying metadata index")
                            }
                        }
                    }
                    WhereClauseComparator::GreaterThan => {
                        let result = match &self.u32_metadata_index_reader {
                            Some(reader) => {
                                futures::executor::block_on(reader.gt(metadata_key, metadata_value))
                            }
                            None => Ok(RoaringBitmap::new()),
                        };
                        match result {
                            Ok(result) => {
                                return result;
                            }
                            Err(_) => {
                                panic!("Error querying metadata index")
                            }
                        }
                    }
                    WhereClauseComparator::GreaterThanOrEqual => {
                        let result = match &self.u32_metadata_index_reader {
                            Some(reader) => futures::executor::block_on(
                                reader.gte(metadata_key, metadata_value),
                            ),
                            None => Ok(RoaringBitmap::new()),
                        };
                        match result {
                            Ok(result) => {
                                return result;
                            }
                            Err(_) => {
                                panic!("Error querying metadata index")
                            }
                        }
                    }
                },
                crate::types::MetadataType::DoubleType => match comparator {
                    WhereClauseComparator::Equal => {
                        let result = match &self.f32_metadata_index_reader {
                            Some(reader) => futures::executor::block_on(
                                reader.get(metadata_key, metadata_value),
                            ),
                            None => Ok(RoaringBitmap::new()),
                        };
                        match result {
                            Ok(result) => {
                                return result;
                            }
                            Err(_) => {
                                panic!("Error querying metadata index")
                            }
                        }
                    }
                    WhereClauseComparator::NotEqual => {
                        todo!();
                    }
                    WhereClauseComparator::LessThan => {
                        let result = match &self.f32_metadata_index_reader {
                            Some(reader) => {
                                futures::executor::block_on(reader.lt(metadata_key, metadata_value))
                            }
                            None => Ok(RoaringBitmap::new()),
                        };
                        match result {
                            Ok(result) => {
                                return result;
                            }
                            Err(_) => {
                                panic!("Error querying metadata index")
                            }
                        }
                    }
                    WhereClauseComparator::LessThanOrEqual => {
                        let result = match &self.f32_metadata_index_reader {
                            Some(reader) => futures::executor::block_on(
                                reader.lte(metadata_key, metadata_value),
                            ),
                            None => Ok(RoaringBitmap::new()),
                        };
                        match result {
                            Ok(result) => {
                                return result;
                            }
                            Err(_) => {
                                panic!("Error querying metadata index")
                            }
                        }
                    }
                    WhereClauseComparator::GreaterThan => {
                        let result = match &self.f32_metadata_index_reader {
                            Some(reader) => {
                                futures::executor::block_on(reader.gt(metadata_key, metadata_value))
                            }
                            None => Ok(RoaringBitmap::new()),
                        };
                        match result {
                            Ok(result) => {
                                return result;
                            }
                            Err(_) => {
                                panic!("Error querying metadata index")
                            }
                        }
                    }
                    WhereClauseComparator::GreaterThanOrEqual => {
                        let result = match &self.f32_metadata_index_reader {
                            Some(reader) => futures::executor::block_on(
                                reader.gte(metadata_key, metadata_value),
                            ),
                            None => Ok(RoaringBitmap::new()),
                        };
                        match result {
                            Ok(result) => {
                                return result;
                            }
                            Err(_) => {
                                panic!("Error querying metadata index")
                            }
                        }
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
        return process_where_clause_with_callback(where_clause, &clo);
    }

    fn process_where_document_clause(
        &self,
        where_document_clause: &WhereDocument,
    ) -> Result<Vec<usize>, MetadataIndexError> {
        let cb = |doc: &str, comparison: WhereDocumentOperator| match comparison {
            WhereDocumentOperator::Contains => {
                let result = match &self.full_text_index_reader {
                    Some(reader) => futures::executor::block_on(reader.search(doc)),
                    None => Ok(vec![]),
                };
                match result {
                    Ok(result) => {
                        return result;
                    }
                    Err(_) => {
                        panic!("Error querying metadata index")
                    }
                }
            }
            WhereDocumentOperator::NotContains => {
                todo!()
            }
        };
        process_where_document_clause_with_callback(where_document_clause, &cb)
    }
}
