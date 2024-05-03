use super::{SegmentFlusher, SegmentWriter};
use crate::errors::{ChromaError, ErrorCodes};
use crate::index::hnsw_provider::HnswIndexProvider;
use crate::index::{HnswIndex, HnswIndexConfig, Index, IndexConfig};
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
pub(crate) struct DistributedHNSWSegment {
    index: Arc<RwLock<HnswIndex>>,
    hnsw_index_provider: HnswIndexProvider,
    pub(crate) id: Uuid,
}

impl Debug for DistributedHNSWSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DistributedHNSWSegment")
    }
}

#[derive(Error, Debug)]
pub enum DistributedHNSWSegmentFromSegmentError {
    #[error("No hnsw file found for segment")]
    NoHnswFileFound,
    #[error("Hnsw file id not a valid uuid")]
    InvalidUUID,
}

impl ChromaError for DistributedHNSWSegmentFromSegmentError {
    fn code(&self) -> crate::errors::ErrorCodes {
        match self {
            DistributedHNSWSegmentFromSegmentError::NoHnswFileFound => ErrorCodes::NotFound,
            DistributedHNSWSegmentFromSegmentError::InvalidUUID => ErrorCodes::InvalidArgument,
        }
    }
}

impl DistributedHNSWSegment {
    pub(crate) fn new(
        index: Arc<RwLock<HnswIndex>>,
        hnsw_index_provider: HnswIndexProvider,
        id: Uuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        return Ok(DistributedHNSWSegment {
            index,
            hnsw_index_provider,
            id,
        });
    }

    pub(crate) async fn from_segment(
        segment: &Segment,
        dimensionality: usize,
        hnsw_index_provider: HnswIndexProvider,
    ) -> Result<Box<DistributedHNSWSegment>, Box<dyn ChromaError>> {
        let index_config = IndexConfig::from_segment(&segment, dimensionality as i32)?;
        let persist_path = &hnsw_index_provider.temporary_storage_path;
        let hnsw_config = HnswIndexConfig::from_segment(segment, persist_path)?;

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
                    ))
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

            let index = match hnsw_index_provider.get(&index_uuid) {
                Some(index) => index,
                None => {
                    hnsw_index_provider
                        .load(&index_uuid, segment, dimensionality as i32)
                        .await?
                }
            };
            Ok(Box::new(DistributedHNSWSegment::new(
                index,
                hnsw_index_provider,
                segment.id,
            )?))
        } else {
            println!("Creating new HNSW index");
            let index = hnsw_index_provider.create(segment, dimensionality as i32)?;
            Ok(Box::new(DistributedHNSWSegment::new(
                index,
                hnsw_index_provider,
                segment.id,
            )?))
        }
    }

    pub(crate) fn query(&self, vector: &[f32], k: usize) -> (Vec<usize>, Vec<f32>) {
        let index = self.index.read();
        index.query(vector, k)
    }
}

impl SegmentWriter for DistributedHNSWSegment {
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
                Operation::Upsert => {}
                Operation::Update => {}
                Operation::Delete => {}
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
impl SegmentFlusher for DistributedHNSWSegment {
    async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let hnsw_index_id = self.index.read().id;
        self.hnsw_index_provider.flush(&hnsw_index_id).await?;
        let mut flushed_files = HashMap::new();
        flushed_files.insert(HNSW_INDEX.to_string(), vec![hnsw_index_id.to_string()]);
        Ok(flushed_files)
    }
}
