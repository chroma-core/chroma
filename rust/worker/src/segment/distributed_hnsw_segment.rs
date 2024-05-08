use super::{SegmentFlusher, SegmentWriter};
use crate::errors::{ChromaError, ErrorCodes};
use crate::index::hnsw_provider::{
    HnswIndexProvider, HnswIndexProviderCommitError, HnswIndexProviderCreateError,
    HnswIndexProviderFlushError, HnswIndexProviderForkError, HnswIndexProviderOpenError,
};
use crate::index::{
    HnswIndex, HnswIndexConfig, HnswIndexFromSegmentError, Index, IndexConfig,
    IndexConfigFromSegmentError,
};
use crate::types::{LogRecord, Operation, Segment};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

const HNSW_INDEX: &str = "hnsw_index";

#[derive(Clone)]
pub(crate) struct DistributedHNSWSegmentWriter {
    index: Arc<RwLock<HnswIndex>>,
    hnsw_index_provider: HnswIndexProvider,
    pub(crate) id: Uuid,
}

impl Debug for DistributedHNSWSegmentWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DistributedHNSWSegment")
    }
}

#[derive(Error, Debug)]
pub enum DistributedHNSWSegmentFromSegmentError {
    #[error("No HNSW file found for segment")]
    NoHnswFileFound,
    #[error("HNSW file id not a valid uuid")]
    InvalidUUID,
    #[error("HNSW segment uninitialized")]
    Uninitialized,
    #[error("Index configuration error")]
    IndexConfigError(#[from] IndexConfigFromSegmentError),
    #[error("HNSW index configuration error")]
    HnswIndexConfigError(#[from] HnswIndexFromSegmentError),
    #[error("HNSW index provider open error")]
    HnswIndexProviderOpenError(#[from] HnswIndexProviderOpenError),
    #[error("HNSW index provider fork error")]
    HnswIndexProviderForkError(#[from] HnswIndexProviderForkError),
    #[error("HNSW index provider create error")]
    HnswIndexProviderCreateError(#[from] HnswIndexProviderCreateError),
}

impl ChromaError for DistributedHNSWSegmentFromSegmentError {
    fn code(&self) -> crate::errors::ErrorCodes {
        match self {
            DistributedHNSWSegmentFromSegmentError::NoHnswFileFound => ErrorCodes::NotFound,
            DistributedHNSWSegmentFromSegmentError::InvalidUUID => ErrorCodes::InvalidArgument,
            DistributedHNSWSegmentFromSegmentError::Uninitialized => ErrorCodes::InvalidArgument,
            DistributedHNSWSegmentFromSegmentError::IndexConfigError(e) => e.code(),
            DistributedHNSWSegmentFromSegmentError::HnswIndexConfigError(e) => e.code(),
            DistributedHNSWSegmentFromSegmentError::HnswIndexProviderOpenError(e) => e.code(),
            DistributedHNSWSegmentFromSegmentError::HnswIndexProviderForkError(e) => e.code(),
            DistributedHNSWSegmentFromSegmentError::HnswIndexProviderCreateError(e) => e.code(),
        }
    }
}

impl DistributedHNSWSegmentWriter {
    pub(crate) fn new(
        index: Arc<RwLock<HnswIndex>>,
        hnsw_index_provider: HnswIndexProvider,
        id: Uuid,
    ) -> Self {
        return DistributedHNSWSegmentWriter {
            index,
            hnsw_index_provider,
            id,
        };
    }

    pub(crate) async fn from_segment(
        segment: &Segment,
        dimensionality: usize,
        hnsw_index_provider: HnswIndexProvider,
    ) -> Result<Box<DistributedHNSWSegmentWriter>, Box<DistributedHNSWSegmentFromSegmentError>>
    {
        let index_config = match IndexConfig::from_segment(&segment, dimensionality as i32) {
            Ok(ic) => ic,
            Err(e) => {
                return Err(Box::new(
                    DistributedHNSWSegmentFromSegmentError::IndexConfigError(*e),
                ));
            }
        };
        let persist_path = &hnsw_index_provider.temporary_storage_path;

        let hnsw_config = match HnswIndexConfig::from_segment(segment, persist_path) {
            Ok(hc) => hc,
            Err(e) => {
                return Err(Box::new(
                    DistributedHNSWSegmentFromSegmentError::HnswIndexConfigError(*e),
                ));
            }
        };

        // TODO: this is hacky, we use the presence of files to determine if we need to load or create the index
        // ideally, an explicit state would be better. When we implement distributed HNSW segments,
        // we can introduce a state in the segment metadata for this
        if segment.file_path.len() > 0 {
            println!("Loading HNSW index from files");
            // Check if its in the providers cache, if not load the index from the files
            let index_id = match &segment.file_path.get(HNSW_INDEX) {
                None => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::NoHnswFileFound,
                    ));
                }
                Some(files) => {
                    if files.is_empty() {
                        return Err(Box::new(
                            DistributedHNSWSegmentFromSegmentError::NoHnswFileFound,
                        ));
                    } else {
                        &files[0]
                    }
                }
            };

            let index_uuid = match Uuid::parse_str(index_id.as_str()) {
                Ok(uuid) => uuid,
                Err(_) => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::InvalidUUID,
                    ))
                }
            };

            let index = match hnsw_index_provider
                .fork(&index_uuid, segment, dimensionality as i32)
                .await
            {
                Ok(index) => index,
                Err(e) => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::HnswIndexProviderForkError(*e),
                    ))
                }
            };

            Ok(Box::new(DistributedHNSWSegmentWriter::new(
                index,
                hnsw_index_provider,
                segment.id,
            )))
        } else {
            let index = match hnsw_index_provider.create(segment, dimensionality as i32) {
                Ok(index) => index,
                Err(e) => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::HnswIndexProviderCreateError(*e),
                    ))
                }
            };
            Ok(Box::new(DistributedHNSWSegmentWriter::new(
                index,
                hnsw_index_provider,
                segment.id,
            )))
        }
    }

    pub(crate) fn query(&self, vector: &[f32], k: usize) -> (Vec<usize>, Vec<f32>) {
        let index = self.index.read();
        index.query(vector, k)
    }
}

impl SegmentWriter for DistributedHNSWSegmentWriter {
    fn apply_materialized_log_chunk(
        &self,
        records: crate::execution::data::data_chunk::Chunk<super::MaterializedLogRecord>,
    ) {
        for record in records.iter() {
            match record.0.log_record.record.operation {
                Operation::Add => {
                    let segment_offset_id = record.0.segment_offset_id;
                    let embedding = record.0.log_record.record.embedding.as_ref().unwrap();
                    self.index
                        .read()
                        .add(segment_offset_id as usize, &embedding);
                }
                Operation::Upsert => {
                    // hnsw index behavior is to treat add() as upsert
                    let segment_offset_id = record.0.segment_offset_id;
                    // Assumption: Upserts must have embedding set
                    let embedding = record.0.log_record.record.embedding.as_ref().unwrap();
                    self.index
                        .read()
                        .add(segment_offset_id as usize, &embedding);
                }
                Operation::Update => {
                    // hnsw index behvaior is to treat add() as upsert so this
                    // will update the embedding
                    // The assumption is that materialized log records only contain
                    // valid updates.
                    let segment_offset_id = record.0.segment_offset_id;
                    match record.0.log_record.record.embedding.as_ref() {
                        Some(e) => {
                            self.index.read().add(segment_offset_id as usize, &e);
                        }
                        None => {
                            // An update may not necessarily update the embedding
                            continue;
                        }
                    };
                }
                Operation::Delete => {
                    // The assumption is that materialized log records only contain
                    // valid deletes
                    let segment_offset_id = record.0.segment_offset_id;
                    self.index.read().delete(segment_offset_id as usize);
                }
            }
        }
    }

    fn apply_log_chunk(&self, records: crate::execution::data::data_chunk::Chunk<LogRecord>) {
        todo!()
    }

    fn commit(self) -> Result<impl SegmentFlusher, Box<dyn ChromaError>> {
        let hnsw_index_id = self.index.read().id;
        let res = self.hnsw_index_provider.commit(&hnsw_index_id);
        match res {
            Ok(_) => Ok(self),
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl SegmentFlusher for DistributedHNSWSegmentWriter {
    async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let hnsw_index_id = self.index.read().id;
        match self.hnsw_index_provider.flush(&hnsw_index_id).await {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        let mut flushed_files = HashMap::new();
        flushed_files.insert(HNSW_INDEX.to_string(), vec![hnsw_index_id.to_string()]);
        Ok(flushed_files)
    }
}

#[derive(Clone)]
pub(crate) struct DistributedHNSWSegmentReader {
    index: Arc<RwLock<HnswIndex>>,
    hnsw_index_provider: HnswIndexProvider,
    pub(crate) id: Uuid,
}

impl Debug for DistributedHNSWSegmentReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DistributedHNSWSegmentReader")
    }
}

impl DistributedHNSWSegmentReader {
    fn new(
        index: Arc<RwLock<HnswIndex>>,
        hnsw_index_provider: HnswIndexProvider,
        id: Uuid,
    ) -> Self {
        return DistributedHNSWSegmentReader {
            index,
            hnsw_index_provider,
            id,
        };
    }

    pub(crate) async fn from_segment(
        segment: &Segment,
        dimensionality: usize,
        hnsw_index_provider: HnswIndexProvider,
    ) -> Result<Box<DistributedHNSWSegmentReader>, Box<DistributedHNSWSegmentFromSegmentError>>
    {
        let index_config = IndexConfig::from_segment(&segment, dimensionality as i32);
        let index_config = match index_config {
            Ok(ic) => ic,
            Err(e) => {
                return Err(Box::new(
                    DistributedHNSWSegmentFromSegmentError::IndexConfigError(*e),
                ));
            }
        };
        let persist_path = &hnsw_index_provider.temporary_storage_path;

        // TODO: this is hacky, we use the presence of files to determine if we need to load or create the index
        // ideally, an explicit state would be better. When we implement distributed HNSW segments,
        // we can introduce a state in the segment metadata for this
        if segment.file_path.len() > 0 {
            println!("Loading HNSW index from files");
            // Check if its in the providers cache, if not load the index from the files
            let index_id = match &segment.file_path.get(HNSW_INDEX) {
                None => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::NoHnswFileFound,
                    ));
                }
                Some(files) => {
                    if files.is_empty() {
                        return Err(Box::new(
                            DistributedHNSWSegmentFromSegmentError::NoHnswFileFound,
                        ));
                    } else {
                        &files[0]
                    }
                }
            };

            let index_uuid = match Uuid::parse_str(index_id.as_str()) {
                Ok(uuid) => uuid,
                Err(_) => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::InvalidUUID,
                    ))
                }
            };

            let index = match hnsw_index_provider
                .open(&index_uuid, segment, dimensionality as i32)
                .await
            {
                Ok(index) => index,
                Err(e) => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::HnswIndexProviderOpenError(*e),
                    ))
                }
            };

            Ok(Box::new(DistributedHNSWSegmentReader::new(
                index,
                hnsw_index_provider,
                segment.id,
            )))
        } else {
            return Err(Box::new(
                DistributedHNSWSegmentFromSegmentError::Uninitialized,
            ));
        }
    }

    pub(crate) fn query(&self, vector: &[f32], k: usize) -> (Vec<usize>, Vec<f32>) {
        let index = self.index.read();
        index.query(vector, k)
    }
}
