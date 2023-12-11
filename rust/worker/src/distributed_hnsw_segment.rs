use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::index::Index;
use crate::types::{EmbeddingRecord, Operation};

pub(crate) struct DistributedHNSWSegment {
    index: Arc<RwLock<Index>>,
    id: AtomicUsize,
    persist_path: String,
    persist_interval: usize,
    flush_interval: usize,
    max_records: usize,
    // TODO: make a segment trait (can use vector reader, metadata reader), and then implement it for hnsw segment
    // TODO: make the redudant data between index and segment implemented with nested gets
    // TODO: additional bookkeeping of the index
    // TODO: switch from Rwlock to xor lock that allows multiple readers or multiple writers
}

impl DistributedHNSWSegment {
    // TODO: load from path
    pub(crate) fn new(
        space_name: String,
        max_records: usize,
        persist_path: String,
        persist_interval: usize,
        flush_interval: usize,
    ) -> Self {
        let index = Arc::new(RwLock::new(Index::new(
            "ip", 1000, 16, 100, 0, false, true, "",
        )));
        return DistributedHNSWSegment {
            index: index,
            id: AtomicUsize::new(0),
            persist_path: persist_path,
            persist_interval: persist_interval,
            flush_interval: flush_interval,
            max_records: max_records,
        };
    }

    pub(crate) fn write_records(&self, records: Vec<Box<EmbeddingRecord>>) {
        for record in records {
            let op = Operation::try_from(record.operation);
            match op {
                Ok(Operation::Add) => {
                    // TODO: hold the lock for the shortest amount of time possible
                    // TODO: make lock xor lock
                    match record.embedding {
                        Some(vector) => {
                            let index = self.index.upgradable_read();
                            if index.initialized == false {
                                // The index is not initialized yet, we need to lazily initialize it
                                // so that we can infer the dimensionality
                                let dim = vector.len();
                                let mut index = RwLockUpgradableReadGuard::upgrade(index);
                                index.init(dim);
                                let next_id =
                                    self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                println!("Adding item: {}", next_id);
                                index.add_item(&vector, next_id, false);
                            } else {
                                let next_id =
                                    self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                println!("Adding item: {}", next_id);
                                index.add_item(&vector, next_id, false);
                            }
                        }
                        None => {
                            // TODO: log an error
                            println!("No vector found in record");
                        }
                    }
                }
                Ok(Operation::Upsert) => {
                    // self.index.upsert(record.id, record.vector.unwrap());
                }
                Ok(Operation::Update) => {}
                Ok(Operation::Delete) => {
                    // self.index.delete(record.id);
                }
                Err(_) => {
                    println!("Error parsing operation");
                }
            }
        }
    }
}

fn vec_to_f32(bytes: &[u8]) -> Option<&[f32]> {
    // Consumes a vector of bytes and returns a vector of f32s

    if bytes.len() % 4 != 0 {
        println!("Bytes length: {}", bytes.len());
        return None; // Return None if the length is not divisible by 4
    }

    unsafe {
        // WARNING: This will only work if the machine is little endian since
        // protobufs are little endian
        let (pre, mid, post) = bytes.align_to::<f32>();
        if pre.len() != 0 || post.len() != 0 {
            println!("Pre len: {}", pre.len());
            println!("Post len: {}", post.len());
            return None; // Return None if the bytes are not aligned
        }
        return Some(mid);
    }
}
