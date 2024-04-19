use super::types::{LogMaterializer, MaterializedLogRecord, SegmentWriter};
use super::DataRecord;
use crate::blockstore::provider::{BlockfileProvider, CreateError};
use crate::blockstore::{BlockfileReader, BlockfileWriter};
use crate::execution::data::data_chunk::Chunk;
use crate::types::{
    update_metdata_to_metdata, LogRecord, Metadata, Operation, Segment, SegmentType,
};
use std::sync::atomic::AtomicU32;
use thiserror::Error;

const USER_ID_TO_OFFSET_ID: &str = "user_id_to_offset_id";
const OFFSET_ID_TO_USER_ID: &str = "offset_id_to_user_id";
const OFFSET_ID_TO_DATA: &str = "offset_id_to_data";

pub(crate) struct RecordSegmentWriter<'a> {
    user_id_to_id: BlockfileWriter<&'a str, u32>,
    id_to_user_id: BlockfileWriter<u32, &'a str>,
    id_to_data: BlockfileWriter<u32, &'a DataRecord<'a>>,
    curr_max_offset_id: AtomicU32,
    // If there is an old version of the data, we need to keep it around to be able to
    // materialize the log records
    // old_id_to_data: Option<BlockfileReader<'a, u32, DataRecord<'a>>>,
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
            user_id_to_id,
            id_to_user_id,
            id_to_data,
            curr_max_offset_id: AtomicU32::new(0),
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

    fn commit(&self) {
        todo!()
    }
}

impl LogMaterializer for RecordSegmentWriter<'_> {
    fn materialize<'chunk>(
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
                    let res =
                        self.id_to_data
                            .set("", index as u32, &materialized.materialized_record);
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
