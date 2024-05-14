use arrow::array::Int32Array;
use std::fmt::{self, Debug, Formatter};
use tantivy::tokenizer::NgramTokenizer;
use thiserror::Error;
use uuid::Uuid;

use super::types::{LogMaterializer, MaterializedLogRecord, SegmentWriter};
use super::{DataRecord, SegmentFlusher};
use crate::blockstore::provider::{BlockfileProvider, CreateError, OpenError};
use crate::blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use crate::errors::{ChromaError, ErrorCodes};
use crate::index::fulltext::tokenizer::TantivyChromaTokenizer;
use crate::index::fulltext::types::{
    FullTextIndexError, FullTextIndexFlusher, FullTextIndexWriter,
};
use crate::types::{Segment, SegmentType};

const FULL_TEXT: &str = "full_text";
const METADATA: &str = "metadata";

pub(crate) struct MetadataSegmentWriter {
    pub(crate) full_text_index_writer: Option<FullTextIndexWriter>,
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
    #[error("Failed to create full text index writer")]
    FullTextIndexWriterError(#[from] FullTextIndexError),
    #[error("Blockfile creation error")]
    BlockfileError(#[from] CreateError),
    #[error("Incorrect number of files")]
    IncorrectNumberOfFiles,
    #[error("Missing file {0}")]
    MissingFile(String),
    #[error("Count not parse UUID {0}")]
    UuidParseError(String),
}

impl MetadataSegmentWriter {
    pub(crate) async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<MetadataSegmentWriter, MetadataSegmentError> {
        println!("Creating MetadataSegmentWriter from Segment");
        if segment.r#type != SegmentType::Metadata {
            return Err(MetadataSegmentError::InvalidSegmentType);
        }
        let full_text_index_writer = match segment.file_path.len() {
            0 => {
                println!("No files found, creating new blockfiles for metadata segment");
                let pl_blockfile = match blockfile_provider.create::<u32, &Int32Array>() {
                    Ok(blockfile) => blockfile,
                    Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                };
                let freq_blockfile = match blockfile_provider.create::<u32, u32>() {
                    Ok(blockfile) => blockfile,
                    Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                };
                let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
                    NgramTokenizer::new(1, 3, false).unwrap(),
                )));
                let full_text_index_writer =
                    FullTextIndexWriter::new(pl_blockfile, freq_blockfile, tokenizer);
                full_text_index_writer
            }
            2 => {
                println!("Found 2 files, opening blockfiles for metadata segment");
                let pl_blockfile_id = match segment
                    .file_path
                    .get(FULL_TEXT)
                    .ok_or_else(|| MetadataSegmentError::MissingFile(FULL_TEXT.to_string()))?
                    .get(0)
                {
                    Some(id) => id,
                    None => return Err(MetadataSegmentError::MissingFile(FULL_TEXT.to_string())),
                };
                let freq_blockfile_id = match segment
                    .file_path
                    .get(METADATA)
                    .ok_or_else(|| MetadataSegmentError::MissingFile(METADATA.to_string()))?
                    .get(0)
                {
                    Some(id) => id,
                    None => return Err(MetadataSegmentError::MissingFile(METADATA.to_string())),
                };

                let pl_blockfile_uuid = match Uuid::parse_str(&freq_blockfile_id) {
                    Ok(uuid) => uuid,
                    Err(_) => {
                        return Err(MetadataSegmentError::UuidParseError(
                            pl_blockfile_id.clone(),
                        ))
                    }
                };
                let freq_blockfile_uuid = match Uuid::parse_str(&pl_blockfile_id) {
                    Ok(uuid) => uuid,
                    Err(_) => {
                        return Err(MetadataSegmentError::UuidParseError(
                            freq_blockfile_id.clone(),
                        ))
                    }
                };

                let pl_blockfile = match blockfile_provider
                    .fork::<u32, &Int32Array>(&pl_blockfile_uuid)
                    .await
                {
                    Ok(blockfile) => blockfile,
                    Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                };
                let freq_blockfile = match blockfile_provider
                    .fork::<u32, u32>(&freq_blockfile_uuid)
                    .await
                {
                    Ok(blockfile) => blockfile,
                    Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                };

                let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
                    NgramTokenizer::new(1, 3, false).unwrap(),
                )));
                let full_text_index_writer =
                    FullTextIndexWriter::new(pl_blockfile, freq_blockfile, tokenizer);
                full_text_index_writer
            }
            _ => return Err(MetadataSegmentError::IncorrectNumberOfFiles),
        };
        Ok(MetadataSegmentWriter {
            full_text_index_writer: Some(full_text_index_writer),
        })
    }
}

impl SegmentWriter for MetadataSegmentWriter {
    fn apply_materialized_log_chunk(
        &self,
        records: crate::execution::data::data_chunk::Chunk<MaterializedLogRecord>,
    ) {
        unreachable!();
    }

    fn apply_log_chunk(
        &self,
        records: crate::execution::data::data_chunk::Chunk<crate::types::LogRecord>,
    ) {
        unreachable!();
    }

    fn commit(mut self) -> Result<impl SegmentFlusher, Box<dyn ChromaError>> {
        let writer = async {
            match self
                .full_text_index_writer
                .take()
                .unwrap()
                .write_to_blockfiles()
                .await
            {
                Ok(writer) => writer,
                Err(e) => return Err(Box::new(e)),
            };
        }
        Ok(MetadataSegmentFlusher {
            full_text_index_flusher: Some(writer),
        })
    }
}

pub(crate) struct MetadataSegmentFlusher {
    pub(crate) full_text_index_flusher: Option<FullTextIndexFlusher>,
}
