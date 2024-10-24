use super::types::{MaterializedLogRecord, SegmentWriter};
use super::SegmentFlusher;
use async_trait::async_trait;
use chroma_blockstore::provider::{BlockfileProvider, CreateError, OpenError};
use chroma_blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Chunk, DataRecord, MaterializedLogOperation, Segment, SegmentType};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

const USER_ID_TO_OFFSET_ID: &str = "user_id_to_offset_id";
const OFFSET_ID_TO_USER_ID: &str = "offset_id_to_user_id";
const OFFSET_ID_TO_DATA: &str = "offset_id_to_data";
const MAX_OFFSET_ID: &str = "max_offset_id";

#[derive(Clone)]
pub struct RecordSegmentWriter {
    // These are Option<> so that we can take() them when we commit
    user_id_to_id: Option<BlockfileWriter>,
    id_to_user_id: Option<BlockfileWriter>,
    id_to_data: Option<BlockfileWriter>,
    // TODO: for now we store the max offset ID in a separate blockfile, this is not ideal
    // we should store it in metadata of one of the blockfiles
    max_offset_id: Option<BlockfileWriter>,
    pub(crate) id: Uuid,
}

impl Debug for RecordSegmentWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordSegmentWriter")
            .field("id", &self.id)
            .finish()
    }
}

#[derive(Error, Debug)]
pub enum RecordSegmentWriterCreationError {
    #[error("Invalid segment type")]
    InvalidSegmentType,
    #[error("Missing file: {0}")]
    MissingFile(String),
    #[error("Incorrect number of files")]
    IncorrectNumberOfFiles,
    #[error("Invalid Uuid for file: {0}")]
    InvalidUuid(String),
    #[error("Blockfile Creation Error")]
    BlockfileCreateError(#[from] Box<CreateError>),
    #[error("Blockfile Open Error")]
    BlockfileOpenError(#[from] Box<OpenError>),
}

impl RecordSegmentWriter {
    async fn construct_and_set_data_record<'a>(
        &self,
        mat_record: &MaterializedLogRecord<'a>,
        user_id: &str,
        offset_id: u32,
    ) -> Result<(), ApplyMaterializedLogError> {
        // Merge data record with updates.
        let updated_document = mat_record.merged_document_ref();
        let updated_embeddings = mat_record.merged_embeddings();
        let final_metadata = mat_record.merged_metadata();
        let mut final_metadata_opt = None;
        if !final_metadata.is_empty() {
            final_metadata_opt = Some(final_metadata);
        }
        // Time to create a data record now.
        let data_record = DataRecord {
            id: user_id,
            embedding: updated_embeddings,
            metadata: final_metadata_opt,
            document: updated_document,
        };
        match self
            .id_to_data
            .as_ref()
            .unwrap()
            .set("", offset_id, &data_record)
            .await
        {
            Ok(_) => (),
            Err(_) => {
                return Err(ApplyMaterializedLogError::BlockfileSet);
            }
        };
        Ok(())
    }

    pub async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Self, RecordSegmentWriterCreationError> {
        tracing::debug!("Creating RecordSegmentWriter from Segment");
        if segment.r#type != SegmentType::BlockfileRecord {
            return Err(RecordSegmentWriterCreationError::InvalidSegmentType);
        }

        let (user_id_to_id, id_to_user_id, id_to_data, max_offset_id) =
            match segment.file_path.len() {
                0 => {
                    tracing::debug!("No files found, creating new blockfiles for record segment");
                    let user_id_to_id = match blockfile_provider.create::<&str, u32>() {
                        Ok(user_id_to_id) => user_id_to_id,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };
                    let id_to_user_id = match blockfile_provider.create::<u32, String>() {
                        Ok(id_to_user_id) => id_to_user_id,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };
                    let id_to_data = match blockfile_provider.create::<u32, &DataRecord>() {
                        Ok(id_to_data) => id_to_data,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };
                    let max_offset_id = match blockfile_provider.create::<&str, u32>() {
                        Ok(max_offset_id) => max_offset_id,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };

                    (user_id_to_id, id_to_user_id, id_to_data, max_offset_id)
                }
                4 => {
                    tracing::debug!("Found files, loading blockfiles for record segment");
                    let user_id_to_id_bf_id = match segment.file_path.get(USER_ID_TO_OFFSET_ID) {
                        Some(user_id_to_id_bf_id) => match user_id_to_id_bf_id.first() {
                            Some(user_id_to_id_bf_id) => user_id_to_id_bf_id,
                            None => {
                                return Err(RecordSegmentWriterCreationError::MissingFile(
                                    USER_ID_TO_OFFSET_ID.to_string(),
                                ))
                            }
                        },
                        None => {
                            return Err(RecordSegmentWriterCreationError::MissingFile(
                                USER_ID_TO_OFFSET_ID.to_string(),
                            ))
                        }
                    };
                    let id_to_user_id_bf_id = match segment.file_path.get(OFFSET_ID_TO_USER_ID) {
                        Some(id_to_user_id_bf_id) => match id_to_user_id_bf_id.first() {
                            Some(id_to_user_id_bf_id) => id_to_user_id_bf_id,
                            None => {
                                return Err(RecordSegmentWriterCreationError::MissingFile(
                                    OFFSET_ID_TO_USER_ID.to_string(),
                                ))
                            }
                        },
                        None => {
                            return Err(RecordSegmentWriterCreationError::MissingFile(
                                OFFSET_ID_TO_USER_ID.to_string(),
                            ))
                        }
                    };
                    let id_to_data_bf_id = match segment.file_path.get(OFFSET_ID_TO_DATA) {
                        Some(id_to_data_bf_id) => match id_to_data_bf_id.first() {
                            Some(id_to_data_bf_id) => id_to_data_bf_id,
                            None => {
                                return Err(RecordSegmentWriterCreationError::MissingFile(
                                    OFFSET_ID_TO_DATA.to_string(),
                                ))
                            }
                        },
                        None => {
                            return Err(RecordSegmentWriterCreationError::MissingFile(
                                OFFSET_ID_TO_DATA.to_string(),
                            ))
                        }
                    };
                    let max_offset_id_bf_id = match segment.file_path.get(MAX_OFFSET_ID) {
                        Some(max_offset_id_file_id) => match max_offset_id_file_id.first() {
                            Some(max_offset_id_file_id) => max_offset_id_file_id,
                            None => {
                                return Err(RecordSegmentWriterCreationError::MissingFile(
                                    MAX_OFFSET_ID.to_string(),
                                ))
                            }
                        },
                        None => {
                            return Err(RecordSegmentWriterCreationError::MissingFile(
                                MAX_OFFSET_ID.to_string(),
                            ))
                        }
                    };

                    let user_id_to_bf_uuid = match Uuid::parse_str(user_id_to_id_bf_id) {
                        Ok(user_id_to_bf_uuid) => user_id_to_bf_uuid,
                        Err(_) => {
                            return Err(RecordSegmentWriterCreationError::InvalidUuid(
                                USER_ID_TO_OFFSET_ID.to_string(),
                            ))
                        }
                    };
                    let id_to_user_id_bf_uuid = match Uuid::parse_str(id_to_user_id_bf_id) {
                        Ok(id_to_user_id_bf_uuid) => id_to_user_id_bf_uuid,
                        Err(_) => {
                            return Err(RecordSegmentWriterCreationError::InvalidUuid(
                                OFFSET_ID_TO_USER_ID.to_string(),
                            ))
                        }
                    };
                    let id_to_data_bf_uuid = match Uuid::parse_str(id_to_data_bf_id) {
                        Ok(id_to_data_bf_uuid) => id_to_data_bf_uuid,
                        Err(_) => {
                            return Err(RecordSegmentWriterCreationError::InvalidUuid(
                                OFFSET_ID_TO_DATA.to_string(),
                            ))
                        }
                    };
                    let max_offset_id_bf_uuid = match Uuid::parse_str(max_offset_id_bf_id) {
                        Ok(max_offset_id_bf_uuid) => max_offset_id_bf_uuid,
                        Err(_) => {
                            return Err(RecordSegmentWriterCreationError::InvalidUuid(
                                MAX_OFFSET_ID.to_string(),
                            ))
                        }
                    };

                    let user_id_to_id = match blockfile_provider
                        .fork::<&str, u32>(&user_id_to_bf_uuid)
                        .await
                    {
                        Ok(user_id_to_id) => user_id_to_id,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };
                    let id_to_user_id = match blockfile_provider
                        .fork::<u32, String>(&id_to_user_id_bf_uuid)
                        .await
                    {
                        Ok(id_to_user_id) => id_to_user_id,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };
                    let id_to_data = match blockfile_provider
                        .fork::<u32, &DataRecord>(&id_to_data_bf_uuid)
                        .await
                    {
                        Ok(id_to_data) => id_to_data,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };
                    let max_offset_id_bf = match blockfile_provider
                        .fork::<&str, u32>(&max_offset_id_bf_uuid)
                        .await
                    {
                        Ok(max_offset_id) => max_offset_id,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };
                    (user_id_to_id, id_to_user_id, id_to_data, max_offset_id_bf)
                }
                _ => return Err(RecordSegmentWriterCreationError::IncorrectNumberOfFiles),
            };

        Ok(RecordSegmentWriter {
            user_id_to_id: Some(user_id_to_id),
            id_to_user_id: Some(id_to_user_id),
            id_to_data: Some(id_to_data),
            max_offset_id: Some(max_offset_id),
            id: segment.id,
        })
    }
}

#[derive(Error, Debug)]
// TODO(Sanket): Should compose errors here but can't currently because
// of Box<dyn ChromaError>.
// Since blockfile does not support read then write semantics natively
// all write operations to it are either set or delete.
pub enum ApplyMaterializedLogError {
    #[error("Error setting to blockfile")]
    BlockfileSet,
    #[error("Error deleting from blockfile")]
    BlockfileDelete,
    #[error("Error updating blockfile")]
    BlockfileUpdate,
    #[error("Allocation error")]
    Allocation,
    #[error("FTS Document add error")]
    FTSDocumentAdd,
    #[error("FTS Document delete error")]
    FTSDocumentDelete,
    #[error("FTS Document update error")]
    FTSDocumentUpdate,
    #[error("Error writing to hnsw index")]
    HnswIndex(#[from] Box<dyn ChromaError>),
}

impl ChromaError for ApplyMaterializedLogError {
    fn code(&self) -> ErrorCodes {
        match self {
            ApplyMaterializedLogError::BlockfileSet => ErrorCodes::Internal,
            ApplyMaterializedLogError::BlockfileDelete => ErrorCodes::Internal,
            ApplyMaterializedLogError::BlockfileUpdate => ErrorCodes::Internal,
            ApplyMaterializedLogError::Allocation => ErrorCodes::Internal,
            ApplyMaterializedLogError::FTSDocumentAdd => ErrorCodes::Internal,
            ApplyMaterializedLogError::FTSDocumentDelete => ErrorCodes::Internal,
            ApplyMaterializedLogError::FTSDocumentUpdate => ErrorCodes::Internal,
            ApplyMaterializedLogError::HnswIndex(_) => ErrorCodes::Internal,
        }
    }
}

impl<'a> SegmentWriter<'a> for RecordSegmentWriter {
    async fn apply_materialized_log_chunk(
        &self,
        records: Chunk<MaterializedLogRecord<'a>>,
    ) -> Result<(), ApplyMaterializedLogError> {
        for (log_record, _) in records.iter() {
            match log_record.final_operation {
                MaterializedLogOperation::AddNew => {
                    // Set all four.
                    // Set user id to offset id.
                    match self
                        .user_id_to_id
                        .as_ref()
                        .unwrap()
                        .set::<&str, u32>("", log_record.user_id.unwrap(), log_record.offset_id)
                        .await
                    {
                        Ok(()) => (),
                        Err(_) => {
                            return Err(ApplyMaterializedLogError::BlockfileSet);
                        }
                    };
                    // Set offset id to user id.
                    match self
                        .id_to_user_id
                        .as_ref()
                        .unwrap()
                        .set::<u32, String>("", log_record.offset_id, log_record.user_id.unwrap().to_string())
                        .await
                    {
                        Ok(()) => (),
                        Err(_) => {
                            return Err(ApplyMaterializedLogError::BlockfileSet);
                        }
                    };
                    // Set data record.
                    match self
                        .construct_and_set_data_record(
                            log_record,
                            log_record.user_id.unwrap(),
                            log_record.offset_id,
                        )
                        .await
                    {
                        Ok(()) => (),
                        Err(e) => {
                            return Err(e);
                        }
                    }
                    // Set max offset id.
                    match self
                        .max_offset_id
                        .as_ref()
                        .unwrap()
                        .set("", MAX_OFFSET_ID, log_record.offset_id)
                        .await
                    {
                        Ok(()) => (),
                        Err(_) => {
                            return Err(ApplyMaterializedLogError::BlockfileSet);
                        }
                    }
                }
                MaterializedLogOperation::UpdateExisting | MaterializedLogOperation::OverwriteExisting => {
                    // Offset id and user id do not need to change. Only data
                    // needs to change. Blockfile does not have Read then write
                    // semantics so we'll delete and insert.
                    match self
                        .id_to_data
                        .as_ref()
                        .unwrap()
                        .delete::<u32, &DataRecord>("", log_record.offset_id)
                        .await
                    {
                        Ok(()) => (),
                        Err(e) => {
                            tracing::error!("Error deleting from user_id_to_id {:?}", e);
                            return Err(ApplyMaterializedLogError::BlockfileDelete);
                        }
                    }
                    match self
                        .construct_and_set_data_record(
                            log_record,
                            log_record.data_record.as_ref().unwrap().id,
                            log_record.offset_id,
                        )
                        .await
                    {
                        Ok(()) => (),
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
                MaterializedLogOperation::DeleteExisting => {
                    // Delete user id to offset id.
                    match self
                        .user_id_to_id
                        .as_ref()
                        .unwrap()
                        .delete::<&str, u32>("", log_record.data_record.as_ref().unwrap().id)
                        .await
                    {
                        Ok(()) => (),
                        Err(e) => {
                            tracing::error!("Error deleting from user_id_to_id {:?}", e);
                            return Err(ApplyMaterializedLogError::BlockfileDelete);
                        }
                    };
                    // Delete offset id to user id.
                    match self
                        .id_to_user_id
                        .as_ref()
                        .unwrap()
                        .delete::<u32, String>("", log_record.offset_id)
                        .await
                    {
                        Ok(()) => (),
                        Err(e) => {
                            tracing::error!("Error deleting from id_to_user_id {:?}", e);
                            return Err(ApplyMaterializedLogError::BlockfileDelete);
                        }
                    };
                    // Delete data record.
                    match self
                        .id_to_data
                        .as_ref()
                        .unwrap()
                        .delete::<u32, &DataRecord>("", log_record.offset_id)
                        .await
                    {
                        Ok(()) => (),
                        Err(e) => {
                            tracing::error!("Error deleting from id_to_data {:?}", e);
                            return Err(ApplyMaterializedLogError::BlockfileDelete);
                        }
                    }
                }
                MaterializedLogOperation::Initial => panic!("Invariant violation. Materialized logs should not have any logs in the initial state")
            }
        }
        Ok(())
    }

    async fn commit(mut self) -> Result<impl SegmentFlusher, Box<dyn ChromaError>> {
        // Commit all the blockfiles
        let flusher_user_id_to_id = self
            .user_id_to_id
            .take()
            .unwrap()
            .commit::<&str, u32>()
            .await;
        let flusher_id_to_user_id = self
            .id_to_user_id
            .take()
            .unwrap()
            .commit::<u32, String>()
            .await;
        let flusher_id_to_data = self
            .id_to_data
            .take()
            .unwrap()
            .commit::<u32, &DataRecord>()
            .await;
        let flusher_max_offset_id = self
            .max_offset_id
            .take()
            .unwrap()
            .commit::<&str, u32>()
            .await;

        let flusher_user_id_to_id = match flusher_user_id_to_id {
            Ok(f) => f,
            Err(e) => {
                return Err(e);
            }
        };

        let flusher_id_to_user_id = match flusher_id_to_user_id {
            Ok(f) => f,
            Err(e) => {
                return Err(e);
            }
        };

        let flusher_id_to_data = match flusher_id_to_data {
            Ok(f) => f,
            Err(e) => {
                return Err(e);
            }
        };

        let flusher_max_offset_id = match flusher_max_offset_id {
            Ok(f) => f,
            Err(e) => {
                return Err(e);
            }
        };

        // Return a flusher that can be used to flush the blockfiles
        Ok(RecordSegmentFlusher {
            user_id_to_id_flusher: flusher_user_id_to_id,
            id_to_user_id_flusher: flusher_id_to_user_id,
            id_to_data_flusher: flusher_id_to_data,
            max_offset_id_flusher: flusher_max_offset_id,
        })
    }
}

pub(crate) struct RecordSegmentFlusher {
    user_id_to_id_flusher: BlockfileFlusher,
    id_to_user_id_flusher: BlockfileFlusher,
    id_to_data_flusher: BlockfileFlusher,
    max_offset_id_flusher: BlockfileFlusher,
}

impl Debug for RecordSegmentFlusher {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordSegmentFlusher").finish()
    }
}

#[async_trait]
impl SegmentFlusher for RecordSegmentFlusher {
    async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let user_id_to_id_bf_id = self.user_id_to_id_flusher.id();
        let id_to_user_id_bf_id = self.id_to_user_id_flusher.id();
        let id_to_data_bf_id = self.id_to_data_flusher.id();
        let max_offset_id_bf_id = self.max_offset_id_flusher.id();
        let res_user_id_to_id = self.user_id_to_id_flusher.flush::<&str, u32>().await;
        let res_id_to_user_id = self.id_to_user_id_flusher.flush::<u32, String>().await;
        let res_id_to_data = self.id_to_data_flusher.flush::<u32, &DataRecord>().await;
        let res_max_offset_id = self.max_offset_id_flusher.flush::<&str, u32>().await;

        let mut flushed_files = HashMap::new();

        match res_user_id_to_id {
            Ok(_) => {
                flushed_files.insert(
                    USER_ID_TO_OFFSET_ID.to_string(),
                    vec![user_id_to_id_bf_id.to_string()],
                );
            }
            Err(e) => {
                return Err(e);
            }
        }

        match res_id_to_user_id {
            Ok(_) => {
                flushed_files.insert(
                    OFFSET_ID_TO_USER_ID.to_string(),
                    vec![id_to_user_id_bf_id.to_string()],
                );
            }
            Err(e) => {
                return Err(e);
            }
        }

        match res_id_to_data {
            Ok(_) => {
                flushed_files.insert(
                    OFFSET_ID_TO_DATA.to_string(),
                    vec![id_to_data_bf_id.to_string()],
                );
            }
            Err(e) => {
                return Err(e);
            }
        }

        match res_max_offset_id {
            Ok(_) => {
                flushed_files.insert(
                    MAX_OFFSET_ID.to_string(),
                    vec![max_offset_id_bf_id.to_string()],
                );
            }
            Err(e) => {
                return Err(e);
            }
        }

        Ok(flushed_files)
    }
}

#[derive(Clone)]
pub struct RecordSegmentReader<'me> {
    user_id_to_id: BlockfileReader<'me, &'me str, u32>,
    id_to_user_id: BlockfileReader<'me, u32, &'me str>,
    id_to_data: BlockfileReader<'me, u32, DataRecord<'me>>,
    curr_max_offset_id: Arc<AtomicU32>,
}

#[derive(Error, Debug)]
pub enum RecordSegmentReaderCreationError {
    #[error("Segment uninitialized")]
    UninitializedSegment,
    #[error("Blockfile Open Error")]
    BlockfileOpenError(#[from] Box<OpenError>),
    #[error("Segment has invalid number of files")]
    InvalidNumberOfFiles,
}

impl ChromaError for RecordSegmentReaderCreationError {
    fn code(&self) -> ErrorCodes {
        match self {
            RecordSegmentReaderCreationError::BlockfileOpenError(e) => e.code(),
            RecordSegmentReaderCreationError::InvalidNumberOfFiles => ErrorCodes::InvalidArgument,
            RecordSegmentReaderCreationError::UninitializedSegment => ErrorCodes::InvalidArgument,
        }
    }
}

impl RecordSegmentReader<'_> {
    pub(crate) async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Self, Box<RecordSegmentReaderCreationError>> {
        let (user_id_to_id, id_to_user_id, id_to_data, existing_max_offset_id) = match segment
            .file_path
            .len()
        {
            4 => {
                let user_id_to_id_bf_id = &segment.file_path.get(USER_ID_TO_OFFSET_ID).unwrap()[0];
                let id_to_user_id_bf_id = &segment.file_path.get(OFFSET_ID_TO_USER_ID).unwrap()[0];
                let id_to_data_bf_id = &segment.file_path.get(OFFSET_ID_TO_DATA).unwrap()[0];

                let max_offset_id_bf_id = match segment.file_path.get(MAX_OFFSET_ID) {
                    Some(max_offset_id_file_id) => max_offset_id_file_id.first(),
                    None => None,
                };
                let max_offset_id_bf_uuid = match max_offset_id_bf_id {
                    Some(id) => Uuid::parse_str(id).ok(),
                    None => None,
                };

                let max_offset_id_bf_reader = match max_offset_id_bf_uuid {
                    Some(bf_uuid) => match blockfile_provider.open::<&str, u32>(&bf_uuid).await {
                        Ok(max_offset_id_bf_reader) => Some(max_offset_id_bf_reader),
                        Err(_) => None,
                    },
                    None => None,
                };
                let exising_max_offset_id = match max_offset_id_bf_reader {
                    Some(reader) => match reader.get("", MAX_OFFSET_ID).await {
                        Ok(max_offset_id) => Arc::new(AtomicU32::new(max_offset_id)),
                        Err(_) => Arc::new(AtomicU32::new(0)),
                    },
                    None => Arc::new(AtomicU32::new(0)),
                };

                let user_id_to_id = match blockfile_provider
                    .open::<&str, u32>(&Uuid::parse_str(user_id_to_id_bf_id).unwrap())
                    .await
                {
                    Ok(user_id_to_id) => user_id_to_id,
                    Err(e) => {
                        return Err(Box::new(
                            RecordSegmentReaderCreationError::BlockfileOpenError(e),
                        ))
                    }
                };

                let id_to_user_id = match blockfile_provider
                    .open::<u32, &str>(&Uuid::parse_str(id_to_user_id_bf_id).unwrap())
                    .await
                {
                    Ok(id_to_user_id) => id_to_user_id,
                    Err(e) => {
                        return Err(Box::new(
                            RecordSegmentReaderCreationError::BlockfileOpenError(e),
                        ))
                    }
                };

                let id_to_data = match blockfile_provider
                    .open::<u32, DataRecord>(&Uuid::parse_str(id_to_data_bf_id).unwrap())
                    .await
                {
                    Ok(id_to_data) => id_to_data,
                    Err(e) => {
                        return Err(Box::new(
                            RecordSegmentReaderCreationError::BlockfileOpenError(e),
                        ))
                    }
                };

                (
                    user_id_to_id,
                    id_to_user_id,
                    id_to_data,
                    exising_max_offset_id,
                )
            }
            0 => {
                return Err(Box::new(
                    RecordSegmentReaderCreationError::UninitializedSegment,
                ));
            }
            _ => {
                return Err(Box::new(
                    RecordSegmentReaderCreationError::InvalidNumberOfFiles,
                ));
            }
        };

        Ok(RecordSegmentReader {
            user_id_to_id,
            id_to_user_id,
            id_to_data,
            curr_max_offset_id: existing_max_offset_id,
        })
    }

    pub(crate) fn get_current_max_offset_id(&self) -> Arc<AtomicU32> {
        self.curr_max_offset_id.clone()
    }

    pub(crate) async fn get_user_id_for_offset_id(
        &self,
        offset_id: u32,
    ) -> Result<&str, Box<dyn ChromaError>> {
        self.id_to_user_id.get("", offset_id).await
    }

    pub(crate) async fn get_offset_id_for_user_id(
        &self,
        user_id: &str,
    ) -> Result<u32, Box<dyn ChromaError>> {
        self.user_id_to_id.get("", user_id).await
    }

    pub(crate) async fn get_data_for_offset_id(
        &self,
        offset_id: u32,
    ) -> Result<DataRecord, Box<dyn ChromaError>> {
        self.id_to_data.get("", offset_id).await
    }

    pub(crate) async fn get_data_and_offset_id_for_user_id(
        &self,
        user_id: &str,
    ) -> Result<(DataRecord, u32), Box<dyn ChromaError>> {
        let offset_id = match self.user_id_to_id.get("", user_id).await {
            Ok(id) => id,
            Err(e) => {
                return Err(e);
            }
        };
        match self.id_to_data.get("", offset_id).await {
            Ok(data_record) => Ok((data_record, offset_id)),
            Err(e) => Err(e),
        }
    }

    pub(crate) async fn data_exists_for_user_id(
        &self,
        user_id: &str,
    ) -> Result<bool, Box<dyn ChromaError>> {
        if !self.user_id_to_id.contains("", user_id).await? {
            return Ok(false);
        }
        let offset_id = match self.user_id_to_id.get("", user_id).await {
            Ok(id) => id,
            Err(e) => {
                return Err(e);
            }
        };
        self.id_to_data.contains("", offset_id).await
    }

    /// Returns all data in the record segment, sorted by
    /// embedding id
    #[allow(dead_code)]
    pub(crate) async fn get_all_data(&self) -> Result<Vec<DataRecord>, Box<dyn ChromaError>> {
        let mut data = Vec::new();
        let max_size = self.user_id_to_id.count().await?;
        for i in 0..max_size {
            let res = self.user_id_to_id.get_at_index(i).await;
            match res {
                Ok((_, _, offset_id)) => {
                    let data_record = self.id_to_data.get("", offset_id).await?;
                    data.push(data_record);
                }
                Err(e) => {
                    tracing::error!(
                        "[GetAllData] Error getting data record for index {:?}: {:?}",
                        i,
                        e
                    );
                    return Err(e);
                }
            }
        }
        Ok(data)
    }

    pub(crate) async fn get_offset_id_at_index(
        &self,
        index: usize,
    ) -> Result<u32, Box<dyn ChromaError>> {
        match self.id_to_user_id.get_at_index(index).await {
            Ok((_, oid, _)) => Ok(oid),
            Err(e) => {
                tracing::error!(
                    "[GetAllData] Error getting offset id for index {}: {}",
                    index,
                    e
                );
                Err(e)
            }
        }
    }

    // Find the rank of the given offset id in the record segment
    // The implemention is based on std binary search
    pub(crate) async fn get_offset_id_rank(
        &self,
        target_oid: u32,
    ) -> Result<usize, Box<dyn ChromaError>> {
        let mut size = self.count().await?;
        if size == 0 {
            return Ok(0);
        }
        let mut base = 0;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;

            let cmp = self.get_offset_id_at_index(mid).await?.cmp(&target_oid);
            base = if cmp == Ordering::Greater { base } else { mid };
            size -= half;
        }

        Ok(
            match self.get_offset_id_at_index(base).await?.cmp(&target_oid) {
                Ordering::Equal => base,
                Ordering::Less => base + 1,
                Ordering::Greater => base,
            },
        )
    }

    pub(crate) async fn count(&self) -> Result<usize, Box<dyn ChromaError>> {
        // We query using the id_to_user_id blockfile since it is likely to be the smallest
        // and count loads all the data
        // In the future, we can optimize this by making the underlying blockfile
        // store counts in the sparse index.
        self.id_to_user_id.count().await
    }

    pub(crate) async fn prefetch_id_to_data(&self, keys: &[u32]) {
        let prefixes = vec![""; keys.len()];
        self.id_to_data.load_blocks_for_keys(&prefixes, keys).await
    }

    #[allow(dead_code)]
    pub(crate) async fn prefetch_user_id_to_id(&self, keys: Vec<&str>) {
        let prefixes = vec![""; keys.len()];
        self.user_id_to_id
            .load_blocks_for_keys(&prefixes, &keys)
            .await
    }

    pub(crate) async fn prefetch_id_to_user_id(&self, keys: &[u32]) {
        let prefixes = vec![""; keys.len()];
        self.id_to_user_id
            .load_blocks_for_keys(&prefixes, keys)
            .await
    }
}
