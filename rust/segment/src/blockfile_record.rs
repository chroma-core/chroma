use crate::bloom_filter::{BloomFilter, BloomFilterError, BloomFilterFlusher, BloomFilterManager};
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
    Cmek, DataRecord, DatabaseUuid, MaterializedLogOperation, SchemaError, Segment, SegmentType,
    SegmentUuid, MAX_OFFSET_ID, OFFSET_ID_TO_DATA, OFFSET_ID_TO_USER_ID, USER_ID_BLOOM_FILTER,
    USER_ID_TO_OFFSET_ID,
};
use futures::{Stream, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::ops::RangeBounds;
use std::sync::atomic::{self, AtomicU32};
use std::sync::Arc;
use thiserror::Error;
use tracing::{Instrument, Span};

const DEFAULT_BLOOM_FILTER_CAPACITY: u64 = 100_000;

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
    #[allow(dead_code)]
    bloom_filter: Option<BloomFilter<str>>,
    bloom_filter_manager: Option<BloomFilterManager>,
    prefix_path: String,
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
    #[error("Bloom filter error: {0}")]
    BloomFilterError(#[from] BloomFilterError),
    #[error("Record segment reader error: {0}")]
    RecordSegmentReaderError(#[from] RecordSegmentReaderCreationError),
    #[error("Bloom filter rebuild error: {0}")]
    BloomFilterRebuildError(Box<dyn ChromaError>),
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
        cmek: Option<Cmek>,
        bloom_filter_manager: Option<BloomFilterManager>,
    ) -> Result<Self, RecordSegmentWriterCreationError> {
        tracing::debug!("Creating RecordSegmentWriter from Segment");
        if segment.r#type != SegmentType::BlockfileRecord {
            return Err(RecordSegmentWriterCreationError::InvalidSegmentType);
        }
        let (user_id_to_id, id_to_user_id, id_to_data, max_offset_id) = match segment
            .file_path
            .len()
        {
            0 => {
                let prefix_path = segment.construct_prefix_path(tenant, database_id);
                tracing::debug!("No files found, creating new blockfiles for record segment");

                let mut options = BlockfileWriterOptions::new(prefix_path.clone());
                if let Some(cmek) = &cmek {
                    options = options.with_cmek(cmek.clone());
                }
                let user_id_to_id = match blockfile_provider.write::<&str, u32>(options).await {
                    Ok(user_id_to_id) => user_id_to_id,
                    Err(e) => {
                        return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                    }
                };

                let mut options = BlockfileWriterOptions::new(prefix_path.clone());
                if let Some(cmek) = &cmek {
                    options = options.with_cmek(cmek.clone());
                }
                let id_to_user_id = match blockfile_provider.write::<u32, String>(options).await {
                    Ok(id_to_user_id) => id_to_user_id,
                    Err(e) => {
                        return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                    }
                };

                let mut options = BlockfileWriterOptions::new(prefix_path.clone());
                if let Some(cmek) = &cmek {
                    options = options.with_cmek(cmek.clone());
                }
                let id_to_data = match blockfile_provider.write::<u32, &DataRecord>(options).await {
                    Ok(id_to_data) => id_to_data,
                    Err(e) => {
                        return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                    }
                };

                let mut options = BlockfileWriterOptions::new(prefix_path.clone());
                if let Some(cmek) = cmek {
                    options = options.with_cmek(cmek);
                }
                let max_offset_id = match blockfile_provider.write::<&str, u32>(options).await {
                    Ok(max_offset_id) => max_offset_id,
                    Err(e) => {
                        return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                    }
                };

                (user_id_to_id, id_to_user_id, id_to_data, max_offset_id)
            }
            4 | 5 => {
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

                let mut options = BlockfileWriterOptions::new(user_id_to_id_bf_prefix.to_string())
                    .fork(user_id_to_id_bf_uuid);
                if let Some(cmek) = &cmek {
                    options = options.with_cmek(cmek.clone());
                }
                let user_id_to_id = match blockfile_provider.write::<&str, u32>(options).await {
                    Ok(user_id_to_id) => user_id_to_id,
                    Err(e) => {
                        return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                    }
                };

                let mut options = BlockfileWriterOptions::new(user_id_to_id_bf_prefix.to_string())
                    .fork(id_to_user_id_bf_uuid);
                if let Some(cmek) = &cmek {
                    options = options.with_cmek(cmek.clone());
                }
                let id_to_user_id = match blockfile_provider.write::<u32, String>(options).await {
                    Ok(id_to_user_id) => id_to_user_id,
                    Err(e) => {
                        return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                    }
                };

                let mut options = BlockfileWriterOptions::new(user_id_to_id_bf_prefix.to_string())
                    .fork(id_to_data_bf_uuid);
                if let Some(cmek) = &cmek {
                    options = options.with_cmek(cmek.clone());
                }
                let id_to_data = match blockfile_provider.write::<u32, &DataRecord>(options).await {
                    Ok(id_to_data) => id_to_data,
                    Err(e) => {
                        return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                    }
                };

                let mut options = BlockfileWriterOptions::new(user_id_to_id_bf_prefix.to_string())
                    .fork(max_offset_id_bf_uuid);
                if let Some(cmek) = cmek {
                    options = options.with_cmek(cmek);
                }
                let max_offset_id_bf = match blockfile_provider.write::<&str, u32>(options).await {
                    Ok(max_offset_id) => max_offset_id,
                    Err(e) => {
                        return Err(RecordSegmentWriterCreationError::BlockfileCreateError(e))
                    }
                };
                (user_id_to_id, id_to_user_id, id_to_data, max_offset_id_bf)
            }
            _ => return Err(RecordSegmentWriterCreationError::IncorrectNumberOfFiles),
        };

        // Having a bloom filter provider is overkill so we only have one abstraction
        // the bloomfilter manager, which is responsible for creating, caching, and committing bloom filters.
        let prefix_path = segment.construct_prefix_path(tenant, database_id);
        let bloom_filter = if let Some(manager) = &bloom_filter_manager {
            let forked = match segment.file_path.get(USER_ID_BLOOM_FILTER) {
                Some(paths) => {
                    // Unexpected state in the system, the key exists but the paths vector is empty.
                    if paths.is_empty() {
                        tracing::error!("Bloom filter key present but paths vector is empty");
                        return Err(RecordSegmentWriterCreationError::IncorrectNumberOfFiles);
                    }
                    Some(manager.fork(&paths[0]).await?)
                }
                // No bloom filter paths found, so rebuild from scratch.
                // This handles migrations from old segments where bloom filters were not used.
                None => None,
            };
            // Rebuild the bloom filter if it is either empty or is degraded.
            Some(
                Box::pin(Self::maybe_rebuild_bloom_filter(
                    segment,
                    blockfile_provider,
                    manager,
                    forked,
                ))
                .await?,
            )
        } else {
            // No bloom filter manager provided, so no bloom filter will be used.
            None
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
            bloom_filter,
            bloom_filter_manager,
            prefix_path,
        })
    }

    /// Return `existing` if it does not need a rebuild, otherwise build a fresh
    /// bloom filter by scanning all user IDs from the record segment.
    async fn maybe_rebuild_bloom_filter(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
        manager: &BloomFilterManager,
        existing: Option<BloomFilter<str>>,
    ) -> Result<BloomFilter<str>, RecordSegmentWriterCreationError> {
        if let Some(bf) = existing {
            if !bf.needs_rebuild() {
                tracing::info!(
                    live_count = bf.live_count(),
                    stale_count = bf.stale_count(),
                    "Reusing existing bloom filter"
                );
                return Ok(bf);
            }
        }
        tracing::info!("Bloom filter needs rebuild, will rebuild from reader");

        let reader = match Box::pin(RecordSegmentReader::from_segment(
            segment,
            blockfile_provider,
            None,
        ))
        .await
        {
            Ok(reader) => reader,
            // Uninitialized segment means no records in the segment, so create an empty bloom filter.
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                return Ok(manager.create(DEFAULT_BLOOM_FILTER_CAPACITY));
            }
            // Other errors are propagated.
            Err(e) => {
                return Err(RecordSegmentWriterCreationError::RecordSegmentReaderError(
                    *e,
                ));
            }
        };
        let count = reader
            .count()
            .await
            .map_err(RecordSegmentWriterCreationError::BloomFilterRebuildError)?;
        let capacity = ((count * 2) as u64).max(DEFAULT_BLOOM_FILTER_CAPACITY);
        let bloom_filter = manager.create(capacity);
        let mut stream = std::pin::pin!(reader.get_user_id_stream());
        while let Some(result) = stream.next().await {
            match result {
                Ok(user_id) => bloom_filter.insert(user_id),
                Err(e) => {
                    return Err(RecordSegmentWriterCreationError::BloomFilterRebuildError(e));
                }
            }
        }
        tracing::info!(
            count,
            capacity,
            "Rebuilt bloom filter from existing records"
        );
        Ok(bloom_filter)
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
                    if let Some(bf) = &self.bloom_filter {
                        bf.insert(log_record.get_user_id());
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
                    if let Some(bf) = &self.bloom_filter {
                        bf.mark_deleted();
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

        let serialized_bloom_filter = match (self.bloom_filter.take(), &self.bloom_filter_manager) {
            (Some(bf), Some(manager)) => {
                Some(manager.commit(bf, &self.prefix_path).await.map_err(|e| {
                    Box::new(ApplyMaterializedLogError::BloomFilterSerializationError(e))
                        as Box<dyn ChromaError>
                })?)
            }
            _ => None,
        };

        // Return a flusher that can be used to flush the blockfiles
        Ok(RecordSegmentFlusher {
            id: self.id,
            user_id_to_id_flusher: flusher_user_id_to_id,
            id_to_user_id_flusher: flusher_id_to_user_id,
            id_to_data_flusher: flusher_id_to_data,
            max_offset_id_flusher: flusher_max_offset_id,
            serialized_bloom_filter,
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
    #[error("Bloom filter serialization failed during commit: {0}")]
    BloomFilterSerializationError(BloomFilterError),
    #[cfg(feature = "usearch")]
    #[error(transparent)]
    QuantizedSpannSegmentError(#[from] crate::quantized_spann::QuantizedSpannSegmentError),
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
            ApplyMaterializedLogError::BloomFilterSerializationError(e) => e.code(),
            #[cfg(feature = "usearch")]
            ApplyMaterializedLogError::QuantizedSpannSegmentError(e) => e.code(),
        }
    }
}

pub struct RecordSegmentFlusher {
    pub id: SegmentUuid,
    user_id_to_id_flusher: BlockfileFlusher,
    id_to_user_id_flusher: BlockfileFlusher,
    id_to_data_flusher: BlockfileFlusher,
    max_offset_id_flusher: BlockfileFlusher,
    /// Serialized bloom filter ready for I/O during flush.
    serialized_bloom_filter: Option<BloomFilterFlusher>,
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

        if let Some(serialized_bloom_filter) = &self.serialized_bloom_filter {
            let bloom_filter_path = serialized_bloom_filter.path().to_string();
            serialized_bloom_filter
                .save()
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
            tracing::info!(path = %bloom_filter_path, "Persisted bloom filter to storage");
            flushed_files.insert(USER_ID_BLOOM_FILTER.to_string(), vec![bloom_filter_path]);
        }

        Ok(flushed_files)
    }

    pub fn count(&self) -> u64 {
        self.id_to_user_id_flusher.count()
    }
}

/// Controls how the record segment reader handles bloom-filter-based
/// pre-filtering during lookups.
#[derive(Debug, Clone, Copy, Default)]
pub struct RecordSegmentReaderOptions {
    pub use_bloom_filter: bool,
}

#[derive(Clone)]
pub struct RecordSegmentReader<'me> {
    user_id_to_id: BlockfileReader<'me, &'me str, u32>,
    id_to_user_id: BlockfileReader<'me, u32, &'me str>,
    id_to_data: BlockfileReader<'me, u32, DataRecord<'me>>,
    max_offset_id: u32,
    bloom_filter_manager: Option<BloomFilterManager>,
    bloom_filter_path: Option<String>,
    bloom_filter: Arc<tokio::sync::OnceCell<Option<BloomFilter<str>>>>,
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
        bloom_filter_manager: Option<BloomFilterManager>,
    ) -> Result<Self, Box<RecordSegmentReaderCreationError>> {
        let (user_id_to_id, id_to_user_id, id_to_data, existing_max_offset_id) =
            match segment.file_path.len() {
                4 | 5 => {
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

        let bloom_filter_path = segment
            .file_path
            .get(USER_ID_BLOOM_FILTER)
            .and_then(|paths| paths.first().cloned());

        Ok(RecordSegmentReader {
            user_id_to_id,
            id_to_user_id,
            id_to_data,
            max_offset_id: existing_max_offset_id,
            bloom_filter_manager,
            bloom_filter_path,
            bloom_filter: Arc::new(tokio::sync::OnceCell::new()),
        })
    }

    pub fn get_max_offset_id(&self) -> u32 {
        self.max_offset_id
    }

    /// Lazily loads the bloom filter using the two-tier heuristic.
    /// Called internally when a plan requests bloom filter usage.
    /// Fetch the bloom filter from storage (via the manager cache) and populate
    /// the local `OnceCell`. Only call when a storage fetch is acceptable.
    async fn fetch_bloom_filter(&self) {
        self.bloom_filter
            .get_or_init(|| async {
                let (manager, path) = match (&self.bloom_filter_manager, &self.bloom_filter_path) {
                    (Some(mgr), Some(p)) => (mgr, p.as_str()),
                    _ => return None,
                };
                manager.get(path).await.ok()
            })
            .await;
    }

    /// Try to populate the local `OnceCell` from the manager's in-memory cache
    /// without triggering a storage fetch. Returns quickly if already loaded or
    /// if the bloom filter isn't cached.
    async fn try_load_bloom_filter_from_cache(&self) {
        if self.bloom_filter.get().is_some() {
            return;
        }
        let (manager, path) = match (&self.bloom_filter_manager, &self.bloom_filter_path) {
            (Some(mgr), Some(p)) => (mgr, p.as_str()),
            _ => return,
        };
        if let Some(bf) = manager.get_if_cached(path).await {
            let _ = self.bloom_filter.set(Some(bf));
        }
    }

    pub async fn get_offset_id_for_user_id(
        &self,
        user_id: &str,
        plan: &RecordSegmentReaderOptions,
    ) -> Result<Option<u32>, Box<dyn ChromaError>> {
        if plan.use_bloom_filter {
            self.fetch_bloom_filter().await;
        } else {
            self.try_load_bloom_filter_from_cache().await;
        }
        if let Some(Some(bf)) = self.bloom_filter.get() {
            if !bf.contains(user_id) {
                return Ok(None);
            }
        }
        self.user_id_to_id.get("", user_id).await
    }

    pub async fn get_data_for_offset_id(
        &'_ self,
        offset_id: u32,
    ) -> Result<Option<DataRecord<'_>>, Box<dyn ChromaError>> {
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
        &'_ self,
    ) -> Result<impl Iterator<Item = (u32, DataRecord<'_>)> + '_, Box<dyn ChromaError>> {
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

    /// Stream all user IDs from the lightweight id_to_user_id blockfile.
    /// Used for bloom filter rebuild without loading full data records.
    fn get_user_id_stream<'me>(
        &'me self,
    ) -> impl Stream<Item = Result<&'me str, Box<dyn ChromaError>>> + 'me {
        self.id_to_user_id
            .get_range_stream(""..="", ..)
            .map(|res| res.map(|(_, _, user_id)| user_id))
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

    pub async fn load_id_to_data(&self, keys: impl Iterator<Item = u32>) {
        self.id_to_data
            .load_data_for_keys(keys.map(|k| ("".to_string(), k)))
            .await
    }

    pub async fn load_user_id_to_id(
        &self,
        keys: impl Iterator<Item = &str>,
        plan: &RecordSegmentReaderOptions,
    ) {
        // Lazy load the bloom filter if it is needed.
        if plan.use_bloom_filter {
            self.fetch_bloom_filter().await;
        } else {
            self.try_load_bloom_filter_from_cache().await;
        }

        let filtered: Vec<&str> = if let Some(Some(bf)) = self.bloom_filter.get() {
            keys.filter(|k| bf.contains(k)).collect()
        } else {
            keys.collect()
        };

        self.user_id_to_id
            .load_data_for_keys(filtered.into_iter().map(|k| ("".to_string(), k)))
            .await
    }

    /// Get the user id for a given offset id using the lightweight id_to_user_id blockfile.
    /// This avoids loading the full DataRecord (embedding, metadata, document).
    /// Returns an error if the offset id is not found.
    pub async fn get_user_id_for_offset_id(
        &self,
        offset_id: u32,
    ) -> Result<&str, Box<dyn ChromaError>> {
        self.id_to_user_id.get("", offset_id).await?.ok_or_else(|| {
            Box::new(RecordSegmentReaderCreationError::DataRecordNotFound(
                offset_id,
            )) as Box<dyn ChromaError>
        })
    }

    /// Bulk prefetch for the id_to_user_id blockfile.
    /// This is the lightweight alternative to load_id_to_data when only user IDs are needed.
    pub async fn load_id_to_user_id(&self, keys: impl Iterator<Item = u32>) {
        self.id_to_user_id
            .load_data_for_keys(keys.map(|k| ("".to_string(), k)))
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
    use chroma_log::test::{int_as_id, upsert_generator, LogGenerator};
    use chroma_types::{Chunk, USER_ID_BLOOM_FILTER};
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
                None,
                None,
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
                                &super::RecordSegmentReaderOptions::default(),
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

    #[tokio::test]
    async fn test_bloom_filter_persisted_after_flush() {
        let mut test_segment = TestDistributedSegment::new().await;
        let num_records = 20;
        let logs = upsert_generator.generate_chunk(1..=num_records);
        Box::pin(test_segment.compact_log(logs, 1)).await;

        assert!(
            test_segment
                .record_segment
                .file_path
                .contains_key(USER_ID_BLOOM_FILTER),
            "Flushed file_path should contain bloom filter key"
        );
        let paths = &test_segment.record_segment.file_path[USER_ID_BLOOM_FILTER];
        assert_eq!(paths.len(), 1, "Should have exactly one bloom filter path");
        assert!(
            !paths[0].is_empty(),
            "Bloom filter path should not be empty"
        );
    }

    #[tokio::test]
    async fn test_bloom_filter_loaded_on_next_compaction() {
        let mut test_segment = TestDistributedSegment::new().await;
        let num_records = 20;
        let logs = upsert_generator.generate_chunk(1..=num_records);
        Box::pin(test_segment.compact_log(logs, 1)).await;

        let writer = RecordSegmentWriter::from_segment(
            &test_segment.collection.tenant,
            &test_segment.collection.database_id,
            &test_segment.record_segment,
            &test_segment.blockfile_provider,
            None,
            test_segment.bloom_filter_manager.clone(),
        )
        .await
        .expect("Should be able to create writer from existing segment");

        let bf = writer
            .bloom_filter
            .as_ref()
            .expect("Bloom filter should be loaded");

        for i in 1..=num_records {
            assert!(
                bf.contains(&int_as_id(i)),
                "Bloom filter should contain {}",
                int_as_id(i)
            );
        }
        assert!(
            !bf.contains("id_nonexistent"),
            "Bloom filter should not contain a never-inserted ID"
        );
        assert_eq!(bf.live_count(), num_records as u64);
        assert_eq!(bf.stale_count(), 0);
    }

    #[tokio::test]
    async fn test_bloom_filter_rebuilt_for_legacy_segment() {
        let mut test_segment = TestDistributedSegment::new().await;
        let num_records = 20;
        let logs = upsert_generator.generate_chunk(1..=num_records);
        Box::pin(test_segment.compact_log(logs, 1)).await;

        // Simulate a legacy segment by removing the bloom filter key.
        test_segment
            .record_segment
            .file_path
            .remove(USER_ID_BLOOM_FILTER);
        assert!(!test_segment
            .record_segment
            .file_path
            .contains_key(USER_ID_BLOOM_FILTER),);

        let writer = RecordSegmentWriter::from_segment(
            &test_segment.collection.tenant,
            &test_segment.collection.database_id,
            &test_segment.record_segment,
            &test_segment.blockfile_provider,
            None,
            test_segment.bloom_filter_manager.clone(),
        )
        .await
        .expect("Should be able to create writer from legacy segment");

        let bf = writer
            .bloom_filter
            .as_ref()
            .expect("Bloom filter should be rebuilt from reader");

        for i in 1..=num_records {
            assert!(
                bf.contains(&int_as_id(i)),
                "Rebuilt bloom filter should contain {}",
                int_as_id(i)
            );
        }
        assert!(
            !bf.contains("id_nonexistent"),
            "Rebuilt bloom filter should not contain a never-inserted ID"
        );
        assert_eq!(bf.live_count(), num_records as u64);
    }

    #[tokio::test]
    async fn test_bloom_filter_updated_on_insert_and_delete() {
        let mut test_segment = TestDistributedSegment::new().await;

        // First compaction: add 10 records.
        let logs = upsert_generator.generate_chunk(1..=10);
        Box::pin(test_segment.compact_log(logs, 1)).await;

        // Second compaction: delete 2 records, materializing with a reader so
        // the deletes resolve to DeleteExisting.
        let reader = Box::pin(super::RecordSegmentReader::from_segment(
            &test_segment.record_segment,
            &test_segment.blockfile_provider,
            None,
        ))
        .await
        .expect("Should be able to create reader");

        let delete_logs: Vec<_> = [1usize, 2]
            .iter()
            .enumerate()
            .map(|(i, &id)| chroma_types::LogRecord {
                log_offset: (11 + i) as i64,
                record: chroma_types::OperationRecord {
                    id: int_as_id(id),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: chroma_types::Operation::Delete,
                },
            })
            .collect();

        let delete_chunk = Chunk::new(delete_logs.into());
        let materialized = materialize_logs(
            &Some(reader),
            delete_chunk.clone(),
            Some(AtomicU32::new(11).into()),
            &super::RecordSegmentReaderOptions::default(),
        )
        .await
        .expect("Should materialize delete logs");

        // Need a second reader for hydration during apply.
        let reader_for_apply = Box::pin(super::RecordSegmentReader::from_segment(
            &test_segment.record_segment,
            &test_segment.blockfile_provider,
            None,
        ))
        .await
        .expect("Should be able to create reader for apply");

        let writer = RecordSegmentWriter::from_segment(
            &test_segment.collection.tenant,
            &test_segment.collection.database_id,
            &test_segment.record_segment,
            &test_segment.blockfile_provider,
            None,
            test_segment.bloom_filter_manager.clone(),
        )
        .await
        .expect("Should be able to create writer");

        writer
            .apply_materialized_log_chunk(&Some(reader_for_apply), &materialized)
            .await
            .expect("Should apply materialized deletes");

        let bf = writer
            .bloom_filter
            .as_ref()
            .expect("Bloom filter should exist");

        // Deleted IDs are still in the bloom filter (can't remove from BF).
        assert!(bf.contains(&int_as_id(1)));
        assert!(bf.contains(&int_as_id(2)));

        // Live IDs should be present.
        for i in 3..=10 {
            assert!(
                bf.contains(&int_as_id(i)),
                "Bloom filter should contain live {}",
                int_as_id(i)
            );
        }

        assert_eq!(bf.stale_count(), 2, "Two deletes should be tracked");
        assert_eq!(bf.live_count(), 8, "8 records should remain live");
    }
}
