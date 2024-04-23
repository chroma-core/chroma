use super::{SegmentFlusher, SegmentWriter};
use crate::errors::ChromaError;
use crate::index::hnsw_provider::HnswIndexProvider;
use crate::index::{HnswIndex, HnswIndexConfig, Index, IndexConfig};
use crate::types::{LogRecord, Operation, Segment};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct DistributedHNSWSegment {
    index: Arc<RwLock<HnswIndex>>,
    hnsw_index_provider: HnswIndexProvider,
    id: Uuid,
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

    pub(crate) fn from_segment(
        segment: &Segment,
        persist_path: &std::path::Path,
        dimensionality: usize,
        hnsw_index_provider: HnswIndexProvider,
    ) -> Result<Box<DistributedHNSWSegment>, Box<dyn ChromaError>> {
        let index_config = IndexConfig::from_segment(&segment, dimensionality as i32)?;
        let hnsw_config = HnswIndexConfig::from_segment(segment, persist_path)?;

        // TODO: this is hacky, we use the presence of files to determine if we need to load or create the index
        // ideally, an explicit state would be better. When we implement distributed HNSW segments,
        // we can introduce a state in the segment metadata for this
        if segment.file_path.len() > 0 {
            // Load the index from the files
            unimplemented!();
        } else {
            let index = hnsw_index_provider.create(segment, dimensionality as i32)?;
            Ok(Box::new(DistributedHNSWSegment::new(
                index,
                hnsw_index_provider,
                segment.id,
            )?))
        }
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
    async fn flush(self) -> Result<(), Box<dyn ChromaError>> {
        let hnsw_index_id = self.index.read().id;
        self.hnsw_index_provider.flush(&hnsw_index_id).await
    }
}
