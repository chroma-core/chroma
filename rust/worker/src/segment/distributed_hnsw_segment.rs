use crate::errors::ChromaError;
use crate::index::{HnswIndex, HnswIndexConfig, Index, IndexConfig};
use crate::types::{LogRecord, Operation, Segment, VectorEmbeddingRecord};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use super::SegmentWriter;

pub(crate) struct DistributedHNSWSegment {
    index: Arc<RwLock<HnswIndex>>,
    index_config: IndexConfig,
    hnsw_config: HnswIndexConfig,
}

impl DistributedHNSWSegment {
    pub(crate) fn new(
        index_config: IndexConfig,
        hnsw_config: HnswIndexConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let hnsw_index = HnswIndex::init(&index_config, Some(&hnsw_config));
        let hnsw_index = match hnsw_index {
            Ok(index) => index,
            Err(e) => {
                // TODO: log + handle an error that we failed to init the index
                return Err(e);
            }
        };
        let index = Arc::new(RwLock::new(hnsw_index));
        return Ok(DistributedHNSWSegment {
            index: index,
            index_config: index_config,
            hnsw_config,
        });
    }

    pub(crate) fn from_segment(
        segment: &Segment,
        persist_path: &std::path::Path,
        dimensionality: usize,
    ) -> Result<Box<DistributedHNSWSegment>, Box<dyn ChromaError>> {
        let index_config = IndexConfig::from_segment(&segment, dimensionality as i32)?;
        let hnsw_config = HnswIndexConfig::from_segment(segment, persist_path)?;
        Ok(Box::new(DistributedHNSWSegment::new(
            index_config,
            hnsw_config,
        )?))
    }

    // pub(crate) fn get_records(&self, ids: Vec<String>) -> Vec<Box<VectorEmbeddingRecord>> {
    //     let mut records = Vec::new();
    //     let user_id_to_id = self.user_id_to_id.read();
    //     let index = self.index.read();
    //     for id in ids {
    //         let internal_id = match user_id_to_id.get(&id) {
    //             Some(internal_id) => internal_id,
    //             None => {
    //                 // TODO: Error
    //                 return records;
    //             }
    //         };
    //         let vector = index.get(*internal_id);
    //         match vector {
    //             Some(vector) => {
    //                 let record = VectorEmbeddingRecord { id: id, vector };
    //                 records.push(Box::new(record));
    //             }
    //             None => {
    //                 // TODO: error
    //             }
    //         }
    //     }
    //     return records;
    // }

    // pub(crate) fn query(&self, vector: &[f32], k: usize) -> (Vec<String>, Vec<f32>) {
    //     let index = self.index.read();
    //     let mut return_user_ids = Vec::new();
    //     let (ids, distances) = index.query(vector, k);
    //     let user_ids = self.id_to_user_id.read();
    //     for id in ids {
    //         match user_ids.get(&id) {
    //             Some(user_id) => return_user_ids.push(user_id.clone()),
    //             None => {
    //                 // TODO: error
    //             }
    //         };
    //     }
    //     return (return_user_ids, distances);
    // }
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

    fn commit(&self) {
        todo!()
    }
}
