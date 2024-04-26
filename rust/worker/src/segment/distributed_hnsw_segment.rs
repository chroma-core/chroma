use super::{SegmentFlusher, SegmentWriter};
use crate::errors::ChromaError;
use crate::index::hnsw_provider::HnswIndexProvider;
use crate::index::{HnswIndex, HnswIndexConfig, Index, IndexConfig};
use crate::types::{LogRecord, Operation, Segment};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
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
            // Check if its in the providers cache, if not load the index from the files
            // TODO: we should not unwrap here
            let index_id = &segment.file_path.get(HNSW_INDEX).unwrap()[0];
            let index_uuid = Uuid::parse_str(index_id.as_str()).unwrap();
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
