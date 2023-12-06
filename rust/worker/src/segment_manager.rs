use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::chroma_proto;
use crate::distributed_hnsw_segment::DistributedHNSWSegment;

#[derive(Clone)]
pub(crate) struct SegmentManager {
    inner: Arc<Inner>,
}

struct Inner {
    vector_segments: RwLock<HashMap<String, Box<DistributedHNSWSegment>>>,
}

impl SegmentManager {
    pub(crate) fn new() -> Self {
        SegmentManager {
            inner: Arc::new(Inner {
                vector_segments: RwLock::new(HashMap::new()),
            }),
        }
    }

    pub(crate) fn write_record(&self, record: Box<chroma_proto::SubmitEmbeddingRecord>) {
        let collection_id = &record.collection_id;
        // TODO: get segments for this collection from sysdb
        let segment_id = collection_id.clone(); // FOR NOW: just use the collection id as the segment id (1 segment per collection)

        match self.inner.vector_segments.read() {
            Ok(segment_map) => match segment_map.get(&segment_id) {
                Some(segment) => {
                    segment.write_records(vec![record]);
                }
                None => {
                    drop(segment_map); // explicitly drop the read lock so we can get a write lock
                    let res = self.inner.vector_segments.write();
                    match res {
                        Ok(mut segment_map) => {
                            let segment =
                                DistributedHNSWSegment::new(collection_id.clone(), 100000);
                            segment.write_records(vec![record]);
                            segment_map.insert(segment_id, Box::new(segment));
                        }
                        Err(_) => {}
                    }
                }
            },
            Err(_) => {}
        }
    }
}
