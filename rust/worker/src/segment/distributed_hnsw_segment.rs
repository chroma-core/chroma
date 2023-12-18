use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::errors::ChromaError;
use crate::index::{HnswIndex, HnswIndexConfig, Index, IndexConfig};
use crate::types::{EmbeddingRecord, Operation};

pub(crate) struct DistributedHNSWSegment {
    index: Arc<RwLock<HnswIndex>>,
    id: AtomicUsize,
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
            index_config: index_config,
            hnsw_config,
        });
    }

    pub(crate) fn write_records(&mut self, records: Vec<Box<EmbeddingRecord>>) {
        for record in records {
            let op = Operation::try_from(record.operation);
            match op {
                Ok(Operation::Add) => {
                    // TODO: make lock xor lock
                    match record.embedding {
                        Some(vector) => {
                            let next_id = self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            println!("Adding item: {}", next_id);
                            self.index.write().add(next_id, &vector);
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
}
