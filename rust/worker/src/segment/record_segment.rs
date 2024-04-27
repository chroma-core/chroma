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
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

const USER_ID_TO_OFFSET_ID: &str = "user_id_to_offset_id";
const OFFSET_ID_TO_USER_ID: &str = "offset_id_to_user_id";
const OFFSET_ID_TO_DATA: &str = "offset_id_to_data";

#[derive(Clone)]
pub(crate) struct RecordSegmentWriter {
    // These are Option<> so that we can take() them when we commit
    user_id_to_id: Option<BlockfileWriter>,
    id_to_user_id: Option<BlockfileWriter>,
    id_to_data: Option<BlockfileWriter>,
    // TODO: store current max offset id in the metadata of the id_to_data blockfile
    curr_max_offset_id: Arc<AtomicU32>,
    pub(crate) id: Uuid,
    // If there is an old version of the data, we need to keep it around to be able to
    // materialize the log records
    // old_id_to_data: Option<BlockfileReader<'a, u32, DataRecord<'a>>>,
}

impl Debug for RecordSegmentWriter {
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
    #[error("Incorrect number of files")]
    IncorrectNumberOfFiles,
    #[error("Invalid Uuid for file: {0}")]
    InvalidUuid(String),
    #[error("Blockfile Creation Error")]
    BlockfileCreateError(#[from] Box<CreateError>),
}

impl RecordSegmentWriter {
    pub(crate) async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Self, RecordSegmentCreationError> {
        println!("Creating RecordSegmentWriter from Segment");
        if segment.r#type != SegmentType::Record {
            return Err(RecordSegmentCreationError::InvalidSegmentType);
        }
        // RESUME POINT = HANDLE EXISTING FILES AND ALSO PORT HSNW TO FORK() NOT LOAD()

        let (user_id_to_id, id_to_user_id, id_to_data) = match segment.file_path.len() {
            0 => {
                println!("No files found, creating new blockfiles for record segment");
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
                (user_id_to_id, id_to_user_id, id_to_data)
            }
            3 => {
                println!("Found files, loading blockfiles for record segment");
                let user_id_to_id_bf_id = match segment.file_path.get(USER_ID_TO_OFFSET_ID) {
                    Some(user_id_to_id_bf_id) => match user_id_to_id_bf_id.get(0) {
                        Some(user_id_to_id_bf_id) => user_id_to_id_bf_id,
                        None => {
                            return Err(RecordSegmentCreationError::MissingFile(
                                USER_ID_TO_OFFSET_ID.to_string(),
                            ))
                        }
                    },
                    None => {
                        return Err(RecordSegmentCreationError::MissingFile(
                            USER_ID_TO_OFFSET_ID.to_string(),
                        ))
                    }
                };
                let id_to_user_id_bf_id = match segment.file_path.get(OFFSET_ID_TO_USER_ID) {
                    Some(id_to_user_id_bf_id) => match id_to_user_id_bf_id.get(0) {
                        Some(id_to_user_id_bf_id) => id_to_user_id_bf_id,
                        None => {
                            return Err(RecordSegmentCreationError::MissingFile(
                                OFFSET_ID_TO_USER_ID.to_string(),
                            ))
                        }
                    },
                    None => {
                        return Err(RecordSegmentCreationError::MissingFile(
                            OFFSET_ID_TO_USER_ID.to_string(),
                        ))
                    }
                };
                let id_to_data_bf_id = match segment.file_path.get(OFFSET_ID_TO_DATA) {
                    Some(id_to_data_bf_id) => match id_to_data_bf_id.get(0) {
                        Some(id_to_data_bf_id) => id_to_data_bf_id,
                        None => {
                            return Err(RecordSegmentCreationError::MissingFile(
                                OFFSET_ID_TO_DATA.to_string(),
                            ))
                        }
                    },
                    None => {
                        return Err(RecordSegmentCreationError::MissingFile(
                            OFFSET_ID_TO_DATA.to_string(),
                        ))
                    }
                };

                let user_id_to_bf_uuid = match Uuid::parse_str(user_id_to_id_bf_id) {
                    Ok(user_id_to_bf_uuid) => user_id_to_bf_uuid,
                    Err(e) => {
                        return Err(RecordSegmentCreationError::InvalidUuid(
                            USER_ID_TO_OFFSET_ID.to_string(),
                        ))
                    }
                };

                let id_to_user_id_bf_uuid = match Uuid::parse_str(id_to_user_id_bf_id) {
                    Ok(id_to_user_id_bf_uuid) => id_to_user_id_bf_uuid,
                    Err(e) => {
                        return Err(RecordSegmentCreationError::InvalidUuid(
                            OFFSET_ID_TO_USER_ID.to_string(),
                        ))
                    }
                };

                let id_to_data_bf_uuid = match Uuid::parse_str(id_to_data_bf_id) {
                    Ok(id_to_data_bf_uuid) => id_to_data_bf_uuid,
                    Err(e) => {
                        return Err(RecordSegmentCreationError::InvalidUuid(
                            OFFSET_ID_TO_DATA.to_string(),
                        ))
                    }
                };

                let user_id_to_id = match blockfile_provider
                    .fork::<&str, u32>(&user_id_to_bf_uuid)
                    .await
                {
                    Ok(user_id_to_id) => user_id_to_id,
                    Err(e) => return Err(RecordSegmentCreationError::BlockfileCreateError(e)),
                };

                let id_to_user_id = match blockfile_provider
                    .fork::<u32, &str>(&id_to_user_id_bf_uuid)
                    .await
                {
                    Ok(id_to_user_id) => id_to_user_id,
                    Err(e) => return Err(RecordSegmentCreationError::BlockfileCreateError(e)),
                };

                let id_to_data = match blockfile_provider
                    .fork::<u32, &DataRecord>(&id_to_data_bf_uuid)
                    .await
                {
                    Ok(id_to_data) => id_to_data,
                    Err(e) => return Err(RecordSegmentCreationError::BlockfileCreateError(e)),
                };

                (user_id_to_id, id_to_user_id, id_to_data)
            }
            _ => return Err(RecordSegmentCreationError::IncorrectNumberOfFiles),
        };

        Ok(RecordSegmentWriter {
            user_id_to_id: Some(user_id_to_id),
            id_to_user_id: Some(id_to_user_id),
            id_to_data: Some(id_to_data),
            curr_max_offset_id: Arc::new(AtomicU32::new(0)),
            id: segment.id,
        })
    }
}

impl SegmentWriter for RecordSegmentWriter {
    fn apply_materialized_log_chunk(&self, records: Chunk<MaterializedLogRecord>) {
        todo!()
    }

    fn apply_log_chunk(&self, records: Chunk<LogRecord>) {
        todo!()
    }

    fn commit(mut self) -> Result<impl SegmentFlusher, Box<dyn ChromaError>> {
        // Commit all the blockfiles
        let flusher_user_id_to_id = self.user_id_to_id.take().unwrap().commit::<&str, u32>();
        let flusher_id_to_user_id = self.id_to_user_id.take().unwrap().commit::<u32, &str>();
        let flusher_id_to_data = self.id_to_data.take().unwrap().commit::<u32, &DataRecord>();

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

pub(crate) struct RecordSegmentFlusher {
    user_id_to_id_flusher: BlockfileFlusher,
    id_to_user_id_flusher: BlockfileFlusher,
    id_to_data_flusher: BlockfileFlusher,
}

impl Debug for RecordSegmentFlusher {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RecordSegmentFlusher")
    }
}

#[async_trait]
impl SegmentFlusher for RecordSegmentFlusher {
    async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let user_id_to_id_bf_id = self.user_id_to_id_flusher.id();
        let id_to_user_id_bf_id = self.id_to_user_id_flusher.id();
        let id_to_data_bf_id = self.id_to_data_flusher.id();
        let res_user_id_to_id = self.user_id_to_id_flusher.flush::<&str, u32>().await;
        let res_id_to_user_id = self.id_to_user_id_flusher.flush::<u32, &str>().await;
        let res_id_to_data = self.id_to_data_flusher.flush::<u32, &DataRecord>().await;

        let mut flushed_files = HashMap::new();

        match res_user_id_to_id {
            Ok(f) => {
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
            Ok(f) => {
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
            Ok(f) => {
                flushed_files.insert(
                    OFFSET_ID_TO_DATA.to_string(),
                    vec![id_to_data_bf_id.to_string()],
                );
            }
            Err(e) => {
                return Err(e);
            }
        }

        Ok(flushed_files)
    }
}

// TODO: remove log materializer, its needless abstraction and complexity
#[async_trait]
impl LogMaterializer for RecordSegmentWriter {
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
                    let metadata = match &log_entry.record.metadata {
                        Some(metadata) => match update_metdata_to_metdata(&metadata) {
                            Ok(metadata) => Some(metadata),
                            Err(e) => {
                                // TODO: this should error out and return an error
                                panic!("Error converting metadata: {}", e);
                            }
                        },
                        None => None,
                    };
                    let data_record = DataRecord {
                        id: &log_entry.record.id,
                        // TODO: don't unwrap here, it should never happen as Adds always have embeddings
                        // but we should handle this gracefully
                        embedding: log_entry.record.embedding.as_ref().unwrap(),
                        document: None, // TODO: document
                        metadata: metadata,
                    };
                    let materialized =
                        MaterializedLogRecord::new(next_offset_id, log_entry, data_record);
                    println!("Writing to id_to_data");
                    let res = self
                        .id_to_data
                        .as_ref()
                        .unwrap()
                        .set("", index as u32, &materialized.materialized_record)
                        .await;
                    println!("Writing to user_id_to_id");
                    let res = self
                        .user_id_to_id
                        .as_ref()
                        .unwrap()
                        .set::<&str, u32>("", log_entry.record.id.as_str(), next_offset_id)
                        .await;
                    println!("Writing to id_to_user_id");
                    let res = self
                        .id_to_user_id
                        .as_ref()
                        .unwrap()
                        .set("", next_offset_id, log_entry.record.id.as_str())
                        .await;
                    // TODO: use res
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

pub(crate) struct RecordSegmentReader<'me> {
    user_id_to_id: BlockfileReader<'me, &'me str, u32>,
    id_to_user_id: BlockfileReader<'me, u32, &'me str>,
    id_to_data: BlockfileReader<'me, u32, DataRecord<'me>>,
}

impl RecordSegmentReader<'_> {
    pub(crate) async fn from_segment(
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (user_id_to_id, id_to_user_id, id_to_data) = match segment.file_path.len() {
            3 => {
                let user_id_to_id_bf_id = &segment.file_path.get(USER_ID_TO_OFFSET_ID).unwrap()[0];
                let id_to_user_id_bf_id = &segment.file_path.get(OFFSET_ID_TO_USER_ID).unwrap()[0];
                let id_to_data_bf_id = &segment.file_path.get(OFFSET_ID_TO_DATA).unwrap()[0];

                // TODO: don't unwrap here
                let user_id_to_id = blockfile_provider
                    .open::<&str, u32>(&Uuid::parse_str(user_id_to_id_bf_id).unwrap())
                    .await
                    .unwrap();
                let id_to_user_id = blockfile_provider
                    .open::<u32, &str>(&Uuid::parse_str(id_to_user_id_bf_id).unwrap())
                    .await
                    .unwrap();
                let id_to_data = blockfile_provider
                    .open::<u32, DataRecord>(&Uuid::parse_str(id_to_data_bf_id).unwrap())
                    .await
                    .unwrap();

                (user_id_to_id, id_to_user_id, id_to_data)
            }
            _ => {
                // TODO: error
                panic!("Invalid number of files for RecordSegment");
            }
        };

        Ok(RecordSegmentReader {
            user_id_to_id,
            id_to_user_id,
            id_to_data,
        })
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
}
