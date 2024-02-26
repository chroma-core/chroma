use num_bigint::BigInt;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::errors::ChromaError;
use crate::index::{HnswIndex, HnswIndexConfig, Index, IndexConfig};
use crate::types::{EmbeddingRecord, Operation, Segment, VectorEmbeddingRecord};

pub(crate) struct DistributedHNSWSegment {
    index: Arc<RwLock<HnswIndex>>,
    id: AtomicUsize,
    user_id_to_id: Arc<RwLock<HashMap<String, usize>>>,
    id_to_user_id: Arc<RwLock<HashMap<usize, String>>>,
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
            id: AtomicUsize::new(0),
            user_id_to_id: Arc::new(RwLock::new(HashMap::new())),
            id_to_user_id: Arc::new(RwLock::new(HashMap::new())),
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

    pub(crate) fn write_records(&self, records: &Vec<Box<EmbeddingRecord>>) {
        for record in records {
            let op = Operation::try_from(record.operation.clone());
            match op {
                Ok(Operation::Add) => {
                    // TODO: make lock xor lock
                    match &record.embedding {
                        Some(vector) => {
                            let next_id = self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            self.user_id_to_id
                                .write()
                                .insert(record.id.clone(), next_id);
                            self.id_to_user_id
                                .write()
                                .insert(next_id, record.id.clone());
                            println!("Segment adding item: {}", next_id);
                            self.index.read().add(next_id, &vector);
                        }
                        None => {
                            // TODO: log an error
                            println!("No vector found in record");
                        }
                    }
                }
                Ok(Operation::Upsert) => {}
                Ok(Operation::Update) => {}
                Ok(Operation::Delete) => {}
                Err(_) => {
                    println!("Error parsing operation");
                }
            }
        }
    }

    pub(crate) fn get_records(&self, ids: Vec<String>) -> Vec<Box<VectorEmbeddingRecord>> {
        let mut records = Vec::new();
        let user_id_to_id = self.user_id_to_id.read();
        let index = self.index.read();
        for id in ids {
            let internal_id = match user_id_to_id.get(&id) {
                Some(internal_id) => internal_id,
                None => {
                    // TODO: Error
                    return records;
                }
            };
            let vector = index.get(*internal_id);
            match vector {
                Some(vector) => {
                    let record = VectorEmbeddingRecord {
                        id: id,
                        seq_id: BigInt::from(0),
                        vector,
                    };
                    records.push(Box::new(record));
                }
                None => {
                    // TODO: error
                }
            }
        }
        return records;
    }

    pub(crate) fn query(&self, vector: &[f32], k: usize) -> (Vec<String>, Vec<f32>) {
        let index = self.index.read();
        let mut return_user_ids = Vec::new();
        let (ids, distances) = index.query(vector, k);
        let user_ids = self.id_to_user_id.read();
        for id in ids {
            match user_ids.get(&id) {
                Some(user_id) => return_user_ids.push(user_id.clone()),
                None => {
                    // TODO: error
                }
            };
        }
        return (return_user_ids, distances);
    }
}
