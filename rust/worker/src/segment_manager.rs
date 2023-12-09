use uuid::Uuid;

use crate::sysdb::SysDb;
use parking_lot::{
    MappedRwLockReadGuard, RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard,
};
use std::collections::HashMap;
use std::sync::Arc;

use crate::distributed_hnsw_segment::DistributedHNSWSegment;
use crate::types::{EmbeddingRecord, MetadataValue, Segment, SegmentScope};

#[derive(Clone)]
pub(crate) struct SegmentManager {
    inner: Arc<Inner>,
    sysdb: Box<dyn SysDb>,
}

struct Inner {
    vector_segments: RwLock<HashMap<Uuid, Box<DistributedHNSWSegment>>>,
    collection_to_segment_cache: RwLock<HashMap<Uuid, Vec<Segment>>>,
}

impl SegmentManager {
    pub(crate) fn new(sysdb: Box<dyn SysDb>) -> Self {
        SegmentManager {
            inner: Arc::new(Inner {
                vector_segments: RwLock::new(HashMap::new()),
                collection_to_segment_cache: RwLock::new(HashMap::new()),
            }),
            sysdb: sysdb,
        }
    }

    pub(crate) async fn write_record(&mut self, record: Box<EmbeddingRecord>) {
        let collection_id = record.collection_id;
        let mut target_segment_id = None;

        {
            let segments = self.get_segments(&collection_id).await;
            target_segment_id = match segments {
                Ok(segments) => {
                    // Find the active segment, load the segment, and write the record to it
                    let mut ret_id = None;
                    for segment in segments.iter() {
                        // Try to get the vector segment
                        if matches!(segment.scope, SegmentScope::VECTOR) {
                            let segment_metadata = segment.metadata.as_ref();
                            if segment_metadata.is_none() {
                                continue;
                            }
                            let segment_metadata = segment_metadata.unwrap();
                            let state = segment_metadata.get("segment_state");
                            match state {
                                Some(MetadataValue::Str(state)) => {
                                    if state == "ACTIVE" {
                                        println!("Found active segment: {}", segment.id);
                                        ret_id = Some(segment.id.clone());
                                        break;
                                    }
                                }
                                _ => {}
                            };
                        } else if matches!(segment.scope, SegmentScope::METADATA) {
                            todo!("Handle metadata segments");
                        }
                    }
                    ret_id
                }
                Err(_) => None,
            };
        }

        if target_segment_id.is_none() {
            return; // TODO: handle no segment found
        }
        let target_segment_id = target_segment_id.unwrap();
        println!("Writing record to segment: {}", target_segment_id);

        // TODO: don't assume 1:1 mapping between collection and segment
        let segment_cache = self.inner.vector_segments.upgradable_read();
        match segment_cache.get(&target_segment_id) {
            Some(segment) => {
                segment.write_records(vec![record]);
            }
            None => {
                let mut segment_cache = RwLockUpgradableReadGuard::upgrade(segment_cache);
                // Parse metadata from the segment and hydrate the params for the segment
                let new_segment = Box::new(DistributedHNSWSegment::new("ip".to_string(), 100000));
                segment_cache.insert(target_segment_id.clone(), new_segment);
                let segment_cache = RwLockWriteGuard::downgrade(segment_cache);
                let segment = RwLockReadGuard::map(segment_cache, |cache| {
                    return cache.get(&target_segment_id).unwrap();
                });
                segment.write_records(vec![record]);
            }
        }
    }

    async fn get_segments(
        &mut self,
        collection_uuid: &Uuid,
    ) -> Result<MappedRwLockReadGuard<Vec<Segment>>, &'static str> {
        let cache_guard = self.inner.collection_to_segment_cache.read();
        // This lets us return a reference to the segments with the lock. The caller is responsible
        // dropping the lock.
        let segments =
            RwLockReadGuard::try_map(cache_guard, |cache: &HashMap<Uuid, Vec<Segment>>| {
                return cache.get(&collection_uuid);
            });
        match segments {
            Ok(segments) => {
                return Ok(segments);
            }
            Err(_) => {
                // Data was not in the cache, so we need to get it from the database
                // Drop the lock since we need to upgrade it
                // Mappable locks cannot be upgraded, so we need to drop the lock and re-acquire it
                // https://github.com/Amanieu/parking_lot/issues/83
                drop(segments);

                let segments = self
                    .sysdb
                    .get_segments(
                        None,
                        None,
                        Some(SegmentScope::VECTOR), // TODO: allow getting other scopes
                        None,
                        Some(collection_uuid.clone()), // TODO: sysdb should not require a clone
                    )
                    .await;
                match segments {
                    Ok(segments) => {
                        let mut cache_guard = self.inner.collection_to_segment_cache.write();
                        cache_guard.insert(collection_uuid.clone(), segments);
                        let cache_guard = RwLockWriteGuard::downgrade(cache_guard);
                        let segments = RwLockReadGuard::map(cache_guard, |cache| {
                            // This unwrap is sage because we just inserted the segments into the cache and currently,
                            // there is no way to remove segments from the cache.
                            return cache.get(&collection_uuid).unwrap();
                        });
                        return Ok(segments);
                    }
                    Err(e) => {
                        return Err("Failed to get segments for collection from SysDB");
                    }
                }
            }
        }
    }
}
