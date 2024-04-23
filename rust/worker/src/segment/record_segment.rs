use super::types::{LogMaterializer, MaterializedLogRecord, SegmentWriter};
use super::{DataRecord, SegmentFlusher};
use crate::blockstore::provider::{BlockfileProvider, CreateError};
use crate::blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use crate::errors::ChromaError;
use crate::execution::data::data_chunk::Chunk;
use crate::types::{
    update_metdata_to_metdata, LogRecord, Metadata, Operation, Segment, SegmentType,
};
use async_trait::async_trait;
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;

const USER_ID_TO_OFFSET_ID: &str = "user_id_to_offset_id";
const OFFSET_ID_TO_USER_ID: &str = "offset_id_to_user_id";
const OFFSET_ID_TO_DATA: &str = "offset_id_to_data";

#[derive(Clone)]
pub(crate) struct RecordSegmentWriter<'a> {
    // These are Option<> so that we can take() them when we commit
    user_id_to_id: Option<BlockfileWriter<&'a str, u32>>,
    id_to_user_id: Option<BlockfileWriter<u32, &'a str>>,
    id_to_data: Option<BlockfileWriter<u32, &'a DataRecord<'a>>>,
    // TODO: store current max offset id in the metadata of the id_to_data blockfile
    curr_max_offset_id: Arc<AtomicU32>,
    // If there is an old version of the data, we need to keep it around to be able to
    // materialize the log records
    // old_id_to_data: Option<BlockfileReader<'a, u32, DataRecord<'a>>>,
}

impl Debug for RecordSegmentWriter<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RecordSegmentWriter")
    }
}

#[derive(Error, Debug)]
pub enum RecordSegmentCreationError {
    #[error("Invalid segment type")]
    InvalidSegmentType,
    #[error("Missing file: {0}")]
    MissingFile(String),
    #[error("Blockfile Creation Error")]
    BlockfileCreateError(#[from] Box<CreateError>),
}

impl<'a> RecordSegmentWriter<'a> {
    pub(crate) fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Self, RecordSegmentCreationError> {
        if segment.r#type != SegmentType::Record {
            return Err(RecordSegmentCreationError::InvalidSegmentType);
        }
        // TODO: This only handles new segments. We need to handle existing segments as well
        // let files = segment.file_path;

        let user_id_to_id = match blockfile_provider.create::<&str, u32>() {
            Ok(user_id_to_id) => user_id_to_id,
            Err(e) => return Err(RecordSegmentCreationError::BlockfileCreateError(e)),
        };
        let id_to_user_id = match blockfile_provider.create::<u32, &str>() {
            Ok(id_to_user_id) => id_to_user_id,
            Err(e) => return Err(RecordSegmentCreationError::BlockfileCreateError(e)),
        };
        let id_to_data = match blockfile_provider.create::<u32, &DataRecord>() {
            Ok(id_to_data) => id_to_data,
            Err(e) => return Err(RecordSegmentCreationError::BlockfileCreateError(e)),
        };

        Ok(RecordSegmentWriter {
            user_id_to_id: Some(user_id_to_id),
            id_to_user_id: Some(id_to_user_id),
            id_to_data: Some(id_to_data),
            curr_max_offset_id: Arc::new(AtomicU32::new(0)),
        })
    }
}

impl SegmentWriter for RecordSegmentWriter<'_> {
    fn apply_materialized_log_chunk(&self, records: Chunk<MaterializedLogRecord>) {
        todo!()
    }

    fn apply_log_chunk(&self, records: Chunk<LogRecord>) {
        todo!()
    }

    fn commit(mut self) -> Result<impl SegmentFlusher, Box<dyn ChromaError>> {
        // Commit all the blockfiles
        let flusher_user_id_to_id = self.user_id_to_id.take().unwrap().commit();
        let flusher_id_to_user_id = self.id_to_user_id.take().unwrap().commit();
        let flusher_id_to_data = self.id_to_data.take().unwrap().commit();

        let flusher_user_id_to_id = match flusher_user_id_to_id {
            Ok(f) => f,
            Err(e) => {
                // TOOD: log and return error
                return Err(e);
            }
        };

        let flusher_id_to_user_id = match flusher_id_to_user_id {
            Ok(f) => f,
            Err(e) => {
                // TOOD: log and return error
                return Err(e);
            }
        };

        let flusher_id_to_data = match flusher_id_to_data {
            Ok(f) => f,
            Err(e) => {
                // TOOD: log and return error
                return Err(e);
            }
        };

        // Return a flusher that can be used to flush the blockfiles
        Ok(RecordSegmentFlusher {
            user_id_to_id_flusher: flusher_user_id_to_id,
            id_to_user_id_flusher: flusher_id_to_user_id,
            id_to_data_flusher: flusher_id_to_data,
        })
    }
}

pub(crate) struct RecordSegmentFlusher<'a> {
    user_id_to_id_flusher: BlockfileFlusher<&'a str, u32>,
    id_to_user_id_flusher: BlockfileFlusher<u32, &'a str>,
    id_to_data_flusher: BlockfileFlusher<u32, &'a DataRecord<'a>>,
}

impl Debug for RecordSegmentFlusher<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RecordSegmentFlusher")
    }
}

#[async_trait]
impl SegmentFlusher for RecordSegmentFlusher<'_> {
    async fn flush(self) -> Result<(), Box<dyn ChromaError>> {
        let res_user_id_to_id = self.user_id_to_id_flusher.flush().await;
        let res_id_to_user_id = self.id_to_user_id_flusher.flush().await;
        let res_id_to_data = self.id_to_data_flusher.flush().await;

        match res_user_id_to_id {
            Ok(_) => {}
            Err(e) => {
                return Err(e);
            }
        }

        match res_id_to_user_id {
            Ok(_) => {}
            Err(e) => {
                return Err(e);
            }
        }

        match res_id_to_data {
            Ok(_) => {}
            Err(e) => {
                return Err(e);
            }
        }

        Ok(())
    }
}

// TODO: remove log materializer, its needless abstraction and complexity
#[async_trait]
impl LogMaterializer for RecordSegmentWriter<'_> {
    async fn materialize<'chunk>(
        &self,
        log_records: &'chunk Chunk<LogRecord>,
    ) -> Chunk<MaterializedLogRecord<'chunk>> {
        let mut materialized_records = Vec::new();
        for (log_entry, index) in log_records.iter() {
            match log_entry.record.operation {
                Operation::Add => {
                    let next_offset_id = self
                        .curr_max_offset_id
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let metadata = log_entry.record.metadata.as_ref().unwrap();
                    // TODO: don't unwrap
                    let metadata = update_metdata_to_metdata(metadata).unwrap();
                    let data_record = DataRecord {
                        id: &log_entry.record.id,
                        embedding: log_entry.record.embedding.as_ref().unwrap(),
                        document: None, // TODO: document
                        metadata: Some(metadata),
                    };
                    let materialized =
                        MaterializedLogRecord::new(next_offset_id, log_entry, data_record);
                    let res = self
                        .id_to_data
                        .as_ref()
                        .unwrap()
                        .set("", index as u32, &materialized.materialized_record)
                        .await;
                    // TODO: use res
                    // RESUME POINT: ADD REVERSE MAPPING, THEN IMPLEMENT DOWNSTREAM SEGMENTS (HNSW, METADATA) AND FLUSHING
                    materialized_records.push(materialized);
                }
                Operation::Delete => {}
                Operation::Update => {}
                Operation::Upsert => {}
            }
        }

        Chunk::new(materialized_records.into())
    }
}
