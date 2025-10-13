use crate::types::ChromaSegmentFlusher;

use super::distributed_spann::SpannSegmentWriterError;
use super::types::{HydratedMaterializedLogRecord, LogMaterializerError, MaterializeLogsResult};
use chroma_blockstore::arrow::provider::BlockfileReaderOptions;
use chroma_blockstore::provider::ReadKey;
use chroma_blockstore::provider::ReadValue;
use chroma_blockstore::provider::{BlockfileProvider, CreateError, OpenError};
use chroma_blockstore::{
    BlockfileFlusher, BlockfileReader, BlockfileWriter, BlockfileWriterOptions,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::fulltext::types::FullTextIndexError;
use chroma_types::{
    DataRecord, DatabaseUuid, MaterializedLogOperation, SchemaError, Segment, SegmentType,
    SegmentUuid, MAX_OFFSET_ID, OFFSET_ID_TO_DATA, OFFSET_ID_TO_USER_ID, USER_ID_TO_OFFSET_ID,
};
use futures::{Stream, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::ops::RangeBounds;
use std::sync::atomic::{self, AtomicU32};
use std::sync::Arc;
use thiserror::Error;
use tracing::{Instrument, Span};

#[derive(Clone)]
pub struct RecordSegmentWriter {
    // These are Option<> so that we can take() them when we commit
    user_id_to_id: Option<BlockfileWriter>,
    id_to_user_id: Option<BlockfileWriter>,
    id_to_data: Option<BlockfileWriter>,
    // TODO: for now we store the max offset ID in a separate blockfile, this is not ideal
    // we should store it in metadata of one of the blockfiles
    max_offset_id: Option<BlockfileWriter>,
    max_new_offset_id: Arc<AtomicU32>,
    pub id: SegmentUuid,
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
    #[error("S3 prefix path wrong in file paths")]
    InvalidPrefixPath,
}

impl chroma_error::ChromaError for RecordSegmentWriterCreationError {
    fn code(&self) -> chroma_error::ErrorCodes {
        use chroma_error::ErrorCodes;
        match self {
            Self::InvalidSegmentType | Self::IncorrectNumberOfFiles => ErrorCodes::InvalidArgument,
            Self::BlockfileCreateError(e) => e.code(),
            Self::BlockfileOpenError(e) => e.code(),
            _ => ErrorCodes::Internal,
        }
    }
}

impl RecordSegmentWriter {
    async fn construct_and_set_data_record(
        &self,
        mat_record: &HydratedMaterializedLogRecord<'_, '_>,
    ) -> Result<(), ApplyMaterializedLogError> {
        // Merge data record with updates.
        let updated_document = mat_record.merged_document_ref();
        let updated_embeddings = mat_record.merged_embeddings_ref();
        let final_metadata = mat_record.merged_metadata();
        let mut final_metadata_opt = None;
        if !final_metadata.is_empty() {
            final_metadata_opt = Some(final_metadata);
        }
        // Time to create a data record now.
        let data_record = DataRecord {
            id: mat_record.get_user_id(),
            embedding: updated_embeddings,
            metadata: final_metadata_opt,
            document: updated_document,
        };
        match self
            .id_to_data
            .as_ref()
            .unwrap()
            .set("", mat_record.get_offset_id(), &data_record)
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
        tenant: &str,
        database_id: &DatabaseUuid,
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
                    let prefix_path = segment.construct_prefix_path(tenant, database_id);
                    tracing::debug!("No files found, creating new blockfiles for record segment");
                    let user_id_to_id = match blockfile_provider
                        .write::<&str, u32>(BlockfileWriterOptions::new(prefix_path.clone()))
                        .await
                    {
                        Ok(user_id_to_id) => user_id_to_id,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };
                    let id_to_user_id = match blockfile_provider
                        .write::<u32, String>(BlockfileWriterOptions::new(prefix_path.clone()))
                        .await
                    {
                        Ok(id_to_user_id) => id_to_user_id,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };
                    let id_to_data = match blockfile_provider
                        .write::<u32, &DataRecord>(BlockfileWriterOptions::new(prefix_path.clone()))
                        .await
                    {
                        Ok(id_to_data) => id_to_data,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };
                    let max_offset_id = match blockfile_provider
                        .write::<&str, u32>(BlockfileWriterOptions::new(prefix_path))
                        .await
                    {
                        Ok(max_offset_id) => max_offset_id,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };

                    (user_id_to_id, id_to_user_id, id_to_data, max_offset_id)
                }
                4 => {
                    tracing::debug!("Found files, loading blockfiles for record segment");
                    let user_id_to_id_bf_path = match segment.file_path.get(USER_ID_TO_OFFSET_ID) {
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
                    let id_to_user_id_bf_path = match segment.file_path.get(OFFSET_ID_TO_USER_ID) {
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
                    let id_to_data_bf_path = match segment.file_path.get(OFFSET_ID_TO_DATA) {
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
                    let max_offset_id_bf_path = match segment.file_path.get(MAX_OFFSET_ID) {
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

                    let (user_id_to_id_bf_prefix, user_id_to_id_bf_uuid) =
                        Segment::extract_prefix_and_id(user_id_to_id_bf_path).map_err(|_| {
                            RecordSegmentWriterCreationError::InvalidUuid(
                                user_id_to_id_bf_path.to_string(),
                            )
                        })?;
                    let (id_to_user_id_bf_prefix, id_to_user_id_bf_uuid) =
                        Segment::extract_prefix_and_id(id_to_user_id_bf_path).map_err(|_| {
                            RecordSegmentWriterCreationError::InvalidUuid(
                                id_to_user_id_bf_path.to_string(),
                            )
                        })?;
                    if user_id_to_id_bf_prefix != id_to_user_id_bf_prefix {
                        return Err(RecordSegmentWriterCreationError::InvalidPrefixPath);
                    }
                    let (id_to_data_bf_prefix, id_to_data_bf_uuid) =
                        Segment::extract_prefix_and_id(id_to_data_bf_path).map_err(|_| {
                            RecordSegmentWriterCreationError::InvalidUuid(
                                id_to_data_bf_path.to_string(),
                            )
                        })?;
                    if user_id_to_id_bf_prefix != id_to_data_bf_prefix {
                        return Err(RecordSegmentWriterCreationError::InvalidPrefixPath);
                    }
                    let (max_offset_id_bf_prefix, max_offset_id_bf_uuid) =
                        Segment::extract_prefix_and_id(max_offset_id_bf_path).map_err(|_| {
                            RecordSegmentWriterCreationError::InvalidUuid(
                                max_offset_id_bf_path.to_string(),
                            )
                        })?;
                    if user_id_to_id_bf_prefix != max_offset_id_bf_prefix {
                        return Err(RecordSegmentWriterCreationError::InvalidPrefixPath);
                    }

                    let user_id_to_id = match blockfile_provider
                        .write::<&str, u32>(
                            BlockfileWriterOptions::new(user_id_to_id_bf_prefix.to_string())
                                .fork(user_id_to_id_bf_uuid),
                        )
                        .await
                    {
                        Ok(user_id_to_id) => user_id_to_id,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };
                    let id_to_user_id = match blockfile_provider
                        .write::<u32, String>(
                            BlockfileWriterOptions::new(user_id_to_id_bf_prefix.to_string())
                                .fork(id_to_user_id_bf_uuid),
                        )
                        .await
                    {
                        Ok(id_to_user_id) => id_to_user_id,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };
                    let id_to_data = match blockfile_provider
                        .write::<u32, &DataRecord>(
                            BlockfileWriterOptions::new(user_id_to_id_bf_prefix.to_string())
                                .fork(id_to_data_bf_uuid),
                        )
                        .await
                    {
                        Ok(id_to_data) => id_to_data,
                        Err(e) => {
                            return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                        }
                    };
                    let max_offset_id_bf = match blockfile_provider
                        .write::<&str, u32>(
                            BlockfileWriterOptions::new(user_id_to_id_bf_prefix.to_string())
                                .fork(max_offset_id_bf_uuid),
                        )
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
            // The max new offset id introduced by materialized logs is initialized as zero
            // Since offset id should start from 1, we use this to indicate no new offset id
            // has been introduced in the materialized logs
            max_new_offset_id: AtomicU32::new(0).into(),
            id: segment.id,
        })
    }

    pub async fn apply_materialized_log_chunk(
        &self,
        record_segment_reader: &Option<RecordSegmentReader<'_>>,
        materialized: &MaterializeLogsResult,
    ) -> Result<(), ApplyMaterializedLogError> {
        // The max new offset id introduced by materialized logs is initialized as zero
        // Since offset id should start from 1, we use this to indicate no new offset id
        // has been introduced in the materialized logs
        let mut max_new_offset_id = 0;
        let mut count = 0u64;

        for log_record in materialized {
            count += 1;

            let log_record = log_record
                .hydrate(record_segment_reader.as_ref())
                .await
                .map_err(ApplyMaterializedLogError::Materialization)?;

            match log_record.get_operation() {
                MaterializedLogOperation::AddNew => {
                    // Set all four.
                    // Set user id to offset id.
                    match self
                        .user_id_to_id
                        .as_ref()
                        .unwrap()
                        .set::<&str, u32>("",  log_record.get_user_id(), log_record.get_offset_id())
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
                        .set::<u32, String>("", log_record.get_offset_id(), log_record.get_user_id().to_string())
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
                            &log_record,
                        )
                        .await
                    {
                        Ok(()) => (),
                        Err(e) => {
                            return Err(e);
                        }
                    }
                    // Set max offset id.
                    max_new_offset_id = max_new_offset_id.max(log_record.get_offset_id());
                }
                MaterializedLogOperation::UpdateExisting | MaterializedLogOperation::OverwriteExisting => {
                    // Offset id and user id do not need to change. Only data
                    // needs to change. Blockfile does not have Read then write
                    // semantics so we'll delete and insert.
                    match self
                        .id_to_data
                        .as_ref()
                        .unwrap()
                        .delete::<u32, &DataRecord>("", log_record.get_offset_id())
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
                            &log_record,
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
                        .delete::<&str, u32>("",  log_record.get_user_id())
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
                        .delete::<u32, String>("", log_record.get_offset_id())
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
                        .delete::<u32, &DataRecord>("", log_record.get_offset_id())
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
        self.max_new_offset_id
            .fetch_max(max_new_offset_id, atomic::Ordering::SeqCst);
        tracing::info!("Applied {} records to record segment", count,);
        Ok(())
    }

    pub async fn commit(mut self) -> Result<RecordSegmentFlusher, Box<dyn ChromaError>> {
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
        let max_offset_id = self.max_offset_id.take().unwrap();
        let max_new_offset_id = self.max_new_offset_id.load(atomic::Ordering::SeqCst);
        // The max new offset id is non zero if and only if new records are introduced
        if max_new_offset_id > 0 {
            max_offset_id
                .set::<&str, u32>("", MAX_OFFSET_ID, max_new_offset_id)
                .await
                .map_err(|_| {
                    Box::new(ApplyMaterializedLogError::BlockfileSet) as Box<dyn ChromaError>
                })?;
        }
        let flusher_max_offset_id = max_offset_id.commit::<&str, u32>().await;

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
            id: self.id,
            user_id_to_id_flusher: flusher_user_id_to_id,
            id_to_user_id_flusher: flusher_id_to_user_id,
            id_to_data_flusher: flusher_id_to_data,
            max_offset_id_flusher: flusher_max_offset_id,
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
    #[error("Schema error: {0}")]
    Schema(#[from] SchemaError),
    #[error("Error writing to the full text index: {0}")]
    FullTextIndex(#[from] FullTextIndexError),
    #[error("Error writing to hnsw index")]
    HnswIndex(#[from] Box<dyn ChromaError>),
    #[error("Log materialization error: {0}")]
    Materialization(#[from] LogMaterializerError),
    #[error("Error applying materialized records to spann segment: {0}")]
    SpannSegmentError(#[from] SpannSegmentWriterError),
}

impl ChromaError for ApplyMaterializedLogError {
    fn code(&self) -> ErrorCodes {
        match self {
            ApplyMaterializedLogError::BlockfileSet => ErrorCodes::Internal,
            ApplyMaterializedLogError::BlockfileDelete => ErrorCodes::Internal,
            ApplyMaterializedLogError::BlockfileUpdate => ErrorCodes::Internal,
            ApplyMaterializedLogError::Allocation => ErrorCodes::Internal,
            ApplyMaterializedLogError::Schema(e) => e.code(),
            ApplyMaterializedLogError::FullTextIndex(e) => e.code(),
            ApplyMaterializedLogError::HnswIndex(_) => ErrorCodes::Internal,
            ApplyMaterializedLogError::Materialization(e) => e.code(),
            ApplyMaterializedLogError::SpannSegmentError(e) => e.code(),
        }
    }
}

pub struct RecordSegmentFlusher {
    pub id: SegmentUuid,
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

impl RecordSegmentFlusher {
    pub async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let prefix_path = self.user_id_to_id_flusher.prefix_path().to_string();
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
                    vec![ChromaSegmentFlusher::flush_key(
                        &prefix_path,
                        &user_id_to_id_bf_id,
                    )],
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
                    vec![ChromaSegmentFlusher::flush_key(
                        &prefix_path,
                        &id_to_user_id_bf_id,
                    )],
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
                    vec![ChromaSegmentFlusher::flush_key(
                        &prefix_path,
                        &id_to_data_bf_id,
                    )],
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
                    vec![ChromaSegmentFlusher::flush_key(
                        &prefix_path,
                        &max_offset_id_bf_id,
                    )],
                );
            }
            Err(e) => {
                return Err(e);
            }
        }

        Ok(flushed_files)
    }

    pub fn count(&self) -> u64 {
        self.id_to_user_id_flusher.count()
    }
}

#[derive(Clone)]
pub struct RecordSegmentReader<'me> {
    user_id_to_id: BlockfileReader<'me, &'me str, u32>,
    id_to_user_id: BlockfileReader<'me, u32, &'me str>,
    id_to_data: BlockfileReader<'me, u32, DataRecord<'me>>,
    max_offset_id: u32,
}

impl Debug for RecordSegmentReader<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordSegmentReader").finish()
    }
}

#[derive(Error, Debug)]
pub enum RecordSegmentReaderCreationError {
    #[error("Segment uninitialized")]
    UninitializedSegment,
    #[error("Blockfile Open Error")]
    BlockfileOpenError(#[from] Box<OpenError>),
    #[error("Segment has invalid number of files")]
    InvalidNumberOfFiles,
    // This case should never happen, so it's internal, but until our APIs rule it out, we have it.
    #[error("Data record not found (offset id: {0})")]
    DataRecordNotFound(u32),
    // This case should never happen, so it's internal, but until our APIs rule it out, we have it.
    #[error("User record not found (user id: {0})")]
    UserRecordNotFound(String),
    #[error("Segment file path missing")]
    FilePathNotFound,
    #[error("Invalid Uuid for segment file: {0}")]
    InvalidUuid(String),
    #[error("Prefix paths across blockfiles do not match")]
    PrefixPathsMismatch,
}

impl ChromaError for RecordSegmentReaderCreationError {
    fn code(&self) -> ErrorCodes {
        match self {
            RecordSegmentReaderCreationError::BlockfileOpenError(e) => e.code(),
            RecordSegmentReaderCreationError::InvalidNumberOfFiles => ErrorCodes::InvalidArgument,
            RecordSegmentReaderCreationError::UninitializedSegment => ErrorCodes::InvalidArgument,
            RecordSegmentReaderCreationError::DataRecordNotFound(_) => ErrorCodes::Internal,
            RecordSegmentReaderCreationError::UserRecordNotFound(_) => ErrorCodes::Internal,
            RecordSegmentReaderCreationError::FilePathNotFound => ErrorCodes::Internal,
            RecordSegmentReaderCreationError::InvalidUuid(_) => ErrorCodes::Internal,
            RecordSegmentReaderCreationError::PrefixPathsMismatch => ErrorCodes::Internal,
        }
    }
}

impl RecordSegmentReader<'_> {
    async fn load_index_reader<'new, K: ReadKey<'new>, V: ReadValue<'new>>(
        segment: &Segment,
        file_path_string: &str,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<BlockfileReader<'new, K, V>, RecordSegmentReaderCreationError> {
        match segment.file_path.get(file_path_string) {
            Some(file_paths) => match file_paths.first() {
                Some(file_path) => {
                    let (prefix_path, index_uuid) = Segment::extract_prefix_and_id(file_path)
                        .map_err(|_| {
                            RecordSegmentReaderCreationError::InvalidUuid(file_path.to_string())
                        })?;
                    let reader_options =
                        BlockfileReaderOptions::new(index_uuid, prefix_path.to_string());
                    match blockfile_provider.read::<K, V>(reader_options).await {
                        Ok(reader) => Ok(reader),
                        Err(e) => Err(RecordSegmentReaderCreationError::BlockfileOpenError(e)),
                    }
                }
                None => Err(RecordSegmentReaderCreationError::FilePathNotFound),
            },
            None => Err(RecordSegmentReaderCreationError::FilePathNotFound),
        }
    }

    pub async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Self, Box<RecordSegmentReaderCreationError>> {
        let (user_id_to_id, id_to_user_id, id_to_data, existing_max_offset_id) =
            match segment.file_path.len() {
                4 => {
                    let user_id_to_id_future =
                        Self::load_index_reader(segment, USER_ID_TO_OFFSET_ID, blockfile_provider)
                            .instrument(Span::current());

                    let id_to_user_id_future =
                        Self::load_index_reader(segment, OFFSET_ID_TO_USER_ID, blockfile_provider)
                            .instrument(Span::current());

                    let id_to_data_future =
                        Self::load_index_reader(segment, OFFSET_ID_TO_DATA, blockfile_provider)
                            .instrument(Span::current());

                    let max_offset_id_future =
                        Self::load_index_reader(segment, MAX_OFFSET_ID, blockfile_provider)
                            .instrument(Span::current());

                    let (
                        max_offset_id_result,
                        user_id_to_id_result,
                        id_to_user_id_result,
                        id_to_data_result,
                    ) = tokio::join!(
                        max_offset_id_future,
                        user_id_to_id_future,
                        id_to_user_id_future,
                        id_to_data_future
                    );

                    let max_offset_id_bf_reader = max_offset_id_result?;
                    let user_id_to_id = user_id_to_id_result?;
                    let id_to_user_id = id_to_user_id_result?;
                    let id_to_data = id_to_data_result?;

                    let exising_max_offset_id =
                        match max_offset_id_bf_reader.get("", MAX_OFFSET_ID).await {
                            Ok(Some(max_offset_id)) => max_offset_id,
                            Ok(None) | Err(_) => 0,
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
            max_offset_id: existing_max_offset_id,
        })
    }

    pub fn get_max_offset_id(&self) -> u32 {
        self.max_offset_id
    }

    pub async fn get_offset_id_for_user_id(
        &self,
        user_id: &str,
    ) -> Result<Option<u32>, Box<dyn ChromaError>> {
        self.user_id_to_id.get("", user_id).await
    }

    pub async fn get_data_for_offset_id(
        &self,
        offset_id: u32,
    ) -> Result<Option<DataRecord>, Box<dyn ChromaError>> {
        self.id_to_data.get("", offset_id).await
    }

    pub async fn data_exists_for_user_id(
        &self,
        user_id: &str,
    ) -> Result<bool, Box<dyn ChromaError>> {
        if !self.user_id_to_id.contains("", user_id).await? {
            return Ok(false);
        }
        let offset_id = match self.user_id_to_id.get("", user_id).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                return Ok(false);
            }
            Err(e) => {
                return Err(e);
            }
        };
        self.id_to_data.contains("", offset_id).await
    }

    /// Returns all data in the record segment, sorted by their offset ids
    pub async fn get_all_data(
        &self,
    ) -> Result<impl Iterator<Item = (u32, DataRecord)> + '_, Box<dyn ChromaError>> {
        self.id_to_data
            .get_range(""..="", ..)
            .await
            .map(|iter| iter.map(|(_, offset, data)| (offset, data)))
    }

    pub async fn get_data_stream<'me>(
        &'me self,
        offset_range: impl RangeBounds<u32> + Clone + Send + 'me,
    ) -> impl Stream<Item = Result<(u32, DataRecord<'me>), Box<dyn ChromaError>>> + 'me {
        self.id_to_data
            .get_range_stream(""..="", offset_range)
            .map(|res| res.map(|(_, offset, rec)| (offset, rec)))
    }

    /// Get a stream of offset ids from the smallest to the largest in the given range
    pub fn get_offset_stream<'me>(
        &'me self,
        offset_range: impl RangeBounds<u32> + Clone + Send + 'me,
    ) -> impl Stream<Item = Result<u32, Box<dyn ChromaError>>> + 'me {
        self.id_to_user_id
            .get_range_stream(""..="", offset_range)
            .map(|res| res.map(|(_, offset_id, _)| offset_id))
    }

    /// Find the rank of the given offset id in the record segment
    /// The rank of an offset id is the number of offset ids strictly smaller than it
    /// In other words, it is the position where the given offset id can be inserted without breaking the order
    pub async fn get_offset_id_rank(&self, target_oid: u32) -> Result<usize, Box<dyn ChromaError>> {
        self.id_to_user_id.rank("", target_oid).await
    }

    pub async fn count(&self) -> Result<usize, Box<dyn ChromaError>> {
        // We query using the id_to_user_id blockfile since it is likely to be the smallest
        // and count loads all the data
        // In the future, we can optimize this by making the underlying blockfile
        // store counts in the sparse index.
        self.id_to_user_id.count().await
    }

    pub async fn prefetch_id_to_data(&self, keys: &[u32]) {
        self.id_to_data
            .load_blocks_for_keys(keys.iter().map(|k| ("".to_string(), *k)))
            .await
    }

    #[allow(dead_code)]
    pub(crate) async fn prefetch_user_id_to_id(&self, keys: Vec<&str>) {
        self.user_id_to_id
            .load_blocks_for_keys(keys.iter().map(|k| ("".to_string(), *k)))
            .await
    }

    pub async fn get_total_logical_size_bytes(&self) -> Result<u64, Box<dyn ChromaError>> {
        self.id_to_data
            .get_range_stream(""..="", ..)
            .map(|res| res.map(|(_, _, d)| d.get_size() as u64))
            .try_collect::<Vec<_>>()
            .await
            .map(|sizes| sizes.iter().sum())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicU32, Arc};

    use chroma_blockstore::BlockfileWriter;
    use chroma_log::test::{upsert_generator, LogGenerator};
    use chroma_types::Chunk;
    use shuttle::{future, thread};

    use crate::{
        blockfile_record::MAX_OFFSET_ID, test::TestDistributedSegment, types::materialize_logs,
    };

    use super::RecordSegmentWriter;

    // The same record segment writer should be able to run concurrently on different threads without conflict
    #[test]
    fn test_max_offset_id_shuttle() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Runtime creation should not fail");
        let test_segment = runtime.block_on(async { TestDistributedSegment::new().await });
        let record_segment_writer = runtime
            .block_on(RecordSegmentWriter::from_segment(
                &test_segment.collection.tenant,
                &test_segment.collection.database_id,
                &test_segment.record_segment,
                &test_segment.blockfile_provider,
            ))
            .expect("Should be able to initialize record segment writer");
        shuttle::check_random(
            move || {
                let log_partition_size = 100;
                let stack_size = 1 << 22;
                let thread_count = 4;
                let max_log_offset = thread_count * log_partition_size;
                let logs = upsert_generator.generate_vec(1..=max_log_offset);

                let batches = logs
                    .chunks(log_partition_size)
                    .map(|chunk| chunk.to_vec())
                    .collect::<Vec<_>>();

                let offset_id = Arc::new(AtomicU32::new(1));

                let mut handles = Vec::new();

                for batch in batches {
                    let curr_offset_id = offset_id.clone();
                    let record_writer = record_segment_writer.clone();

                    let handle = thread::Builder::new()
                        .stack_size(stack_size)
                        .spawn(move || {
                            let log_chunk = Chunk::new(batch.into());
                            let materialized_logs = future::block_on(materialize_logs(
                                &None,
                                log_chunk,
                                Some(curr_offset_id),
                            ))
                            .expect("Should be able to materialize log");
                            future::block_on(
                                record_writer
                                    .apply_materialized_log_chunk(&None, &materialized_logs),
                            )
                            .expect("Should be able to apply materialized log")
                        })
                        .expect("Should be able to spawn thread");

                    handles.push(handle);
                }

                handles
                    .into_iter()
                    .for_each(|handle| handle.join().expect("Writer should not fail"));

                let max_offset_id_writer =
                    if let Some(BlockfileWriter::ArrowUnorderedBlockfileWriter(writer)) =
                        &record_segment_writer.max_offset_id
                    {
                        writer.clone()
                    } else {
                        unreachable!(
                        "Please adjust how max offset id is extracted from record segment writer"
                    );
                    };

                let record_segment_writer = record_segment_writer.clone();
                thread::Builder::new()
                    .stack_size(stack_size)
                    .spawn(move || {
                        future::block_on(record_segment_writer.commit())
                            .expect("Should be able to commit applied logs")
                    })
                    .expect("Should be able to spawn thread")
                    .join()
                    .expect("Should be able to commit applied logs");
                let max_offset_id = future::block_on(
                    max_offset_id_writer.get_owned::<&str, u32>("", MAX_OFFSET_ID),
                )
                .expect("Get owned should not fail")
                .expect("Max offset id should exist");

                assert_eq!(max_offset_id, max_log_offset as u32);
            },
            60,
        );
    }
}
