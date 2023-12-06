use std::sync::atomic::{AtomicI64, AtomicUsize};
use std::sync::{Arc, RwLock};

use crate::chroma_proto::{self, Operation};
use crate::index::Index;

pub(crate) struct DistributedHNSWSegment {
    index: Arc<RwLock<Index>>,
    id: AtomicUsize,
    // TODO: additional bookkeeping of the index
    // TODO: switch from Rwlock to xor lock that allows multiple readers or multiple writers
}

impl DistributedHNSWSegment {
    // TODO: load from path
    pub(crate) fn new(space_name: String, max_records: usize) -> Self {
        let index = Arc::new(RwLock::new(Index::new("ip", 1)));
        let index_guard = index.write();
        let index_handle = index_guard.unwrap();
        println!("Initializing index");
        index_handle.init(max_records, 16, 200, 0, true);
        return DistributedHNSWSegment {
            index: index.clone(),
            id: AtomicUsize::new(0),
        };
        // TODO: lazy init so we can track the dim correctly
    }

    pub(crate) fn write_records(&self, records: Vec<Box<chroma_proto::SubmitEmbeddingRecord>>) {
        for record in records {
            let op = Operation::try_from(record.operation);
            match op {
                Ok(Operation::Add) => {
                    // TODO: hold the lock for the shortest amount of time possible
                    // TODO: make lock xor lock
                    let index_res = self.index.read();
                    match index_res {
                        Ok(mut index) => match record.vector {
                            Some(vector) => {
                                // Parse the bytes into a vector
                                let vector = vector.vector;
                                let vector = vec_to_f32(&vector);
                                match vector {
                                    Some(vector) => {
                                        // TOOD: id management
                                        println!("Adding item to index with id: {}", record.id);
                                        let next_id = self
                                            .id
                                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                        // println!("Vector: {:?}", vector);
                                        index.add_item(vector, next_id, false);
                                    }
                                    None => {
                                        println!("Error parsing vector");
                                    }
                                }
                            }
                            None => {
                                // TODO: log an error
                                println!("No vector found in record");
                            }
                        },
                        Err(_) => {
                            println!("Error getting write lock on index");
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
