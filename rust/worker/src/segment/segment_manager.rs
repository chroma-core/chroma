use crate::{
    config::{Configurable, WorkerConfig},
    errors::ChromaError,
    sysdb::sysdb::{GrpcSysDb, SysDb},
    types::VectorQueryResult,
};
use async_trait::async_trait;
use k8s_openapi::api::node;
use num_bigint::BigInt;
use parking_lot::{
    MappedRwLockReadGuard, RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard,
};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use super::distributed_hnsw_segment::DistributedHNSWSegment;
use crate::types::{EmbeddingRecord, MetadataValue, Segment, SegmentScope, VectorEmbeddingRecord};

#[derive(Clone)]
pub(crate) struct SegmentManager {
    inner: Arc<Inner>,
    sysdb: Box<dyn SysDb>,
}

///
struct Inner {
    vector_segments: RwLock<HashMap<Uuid, Box<DistributedHNSWSegment>>>,
    collection_to_segment_cache: RwLock<HashMap<Uuid, Vec<Arc<Segment>>>>,
    storage_path: Box<std::path::PathBuf>,
}

impl SegmentManager {
    pub(crate) fn new(sysdb: Box<dyn SysDb>, storage_path: &std::path::Path) -> Self {
        SegmentManager {
            inner: Arc::new(Inner {
                vector_segments: RwLock::new(HashMap::new()),
                collection_to_segment_cache: RwLock::new(HashMap::new()),
                storage_path: Box::new(storage_path.to_owned()),
            }),
            sysdb: sysdb,
        }
    }

    pub(crate) async fn write_record(&mut self, record: Box<EmbeddingRecord>) {
        let collection_id = record.collection_id;
        let mut target_segment = None;
        // TODO: don't assume 1:1 mapping between collection and segment
        {
            let segments = self.get_segments(&collection_id).await;
            target_segment = match segments {
                Ok(found_segments) => {
                    if found_segments.len() == 0 {
                        return; // TODO: handle no segment found
                    }
                    Some(found_segments[0].clone())
                }
                Err(_) => {
                    // TODO: throw an error and log no segment found
                    return;
                }
            };
        }

        let target_segment = match target_segment {
            Some(segment) => segment,
            None => {
                // TODO: throw an error and log no segment found
                return;
            }
        };

        println!("Writing to segment id {}", target_segment.id);

        let segment_cache = self.inner.vector_segments.upgradable_read();
        match segment_cache.get(&target_segment.id) {
            Some(segment) => {
                segment.write_records(vec![record]);
            }
            None => {
                let mut segment_cache = RwLockUpgradableReadGuard::upgrade(segment_cache);

                let new_segment = DistributedHNSWSegment::from_segment(
                    &target_segment,
                    &self.inner.storage_path,
                    // TODO: Don't unwrap - throw an error
                    record.embedding.as_ref().unwrap().len(),
                );

                match new_segment {
                    Ok(new_segment) => {
                        new_segment.write_records(vec![record]);
                        segment_cache.insert(target_segment.id, new_segment);
                    }
                    Err(e) => {
                        println!("Failed to create segment error {}", e);
                        // TODO: fail and log an error - failed to create/init segment
                    }
                }
            }
        }
    }

    pub(crate) async fn get_records(
        &self,
        segment_id: &Uuid,
        ids: Vec<String>,
    ) -> Result<Vec<Box<VectorEmbeddingRecord>>, &'static str> {
        // TODO: Load segment if not in cache
        let segment_cache = self.inner.vector_segments.read();
        match segment_cache.get(segment_id) {
            Some(segment) => {
                return Ok(segment.get_records(ids));
            }
            None => {
                return Err("No segment found");
            }
        }
    }

    pub(crate) async fn query_vector(
        &self,
        segment_id: &Uuid,
        vectors: &[f32],
        k: usize,
        include_vector: bool,
    ) -> Result<Vec<Box<VectorQueryResult>>, &'static str> {
        let segment_cache = self.inner.vector_segments.read();
        match segment_cache.get(segment_id) {
            Some(segment) => {
                let mut results = Vec::new();
                let (ids, distances) = segment.query(vectors, k);
                for (id, distance) in ids.iter().zip(distances.iter()) {
                    let fetched_vector = match include_vector {
                        true => Some(segment.get_records(vec![id.clone()])),
                        false => None,
                    };

                    let mut target_record = None;
                    if include_vector {
                        target_record = match fetched_vector {
                            Some(fetched_vectors) => {
                                if fetched_vectors.len() == 0 {
                                    return Err("No vector found");
                                }
                                let mut target_vec = None;
                                for vec in fetched_vectors.into_iter() {
                                    if vec.id == *id {
                                        target_vec = Some(vec);
                                        break;
                                    }
                                }
                                target_vec
                            }
                            None => {
                                return Err("No vector found");
                            }
                        };
                    }

                    let ret_vec = match target_record {
                        Some(target_record) => Some(target_record.vector),
                        None => None,
                    };

                    let result = Box::new(VectorQueryResult {
                        id: id.to_string(),
                        seq_id: BigInt::from(0),
                        distance: *distance,
                        vector: ret_vec,
                    });
                    results.push(result);
                }
                return Ok(results);
            }
            None => {
                return Err("No segment found");
            }
        }
    }

    async fn get_segments(
        &mut self,
        collection_uuid: &Uuid,
    ) -> Result<MappedRwLockReadGuard<Vec<Arc<Segment>>>, &'static str> {
        let cache_guard = self.inner.collection_to_segment_cache.read();
        // This lets us return a reference to the segments with the lock. The caller is responsible
        // dropping the lock.
        let segments = RwLockReadGuard::try_map(cache_guard, |cache| {
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
                        Some(SegmentScope::VECTOR),
                        Some(collection_uuid.clone()),
                    )
                    .await;
                match segments {
                    Ok(segments) => {
                        let mut cache_guard = self.inner.collection_to_segment_cache.write();
                        let mut arc_segments = Vec::new();
                        for segment in segments {
                            arc_segments.push(Arc::new(segment));
                        }
                        cache_guard.insert(collection_uuid.clone(), arc_segments);
                        let cache_guard = RwLockWriteGuard::downgrade(cache_guard);
                        let segments = RwLockReadGuard::map(cache_guard, |cache| {
                            // This unwrap is safe because we just inserted the segments into the cache and currently,
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

#[async_trait]
impl Configurable for SegmentManager {
    async fn try_from_config(worker_config: &WorkerConfig) -> Result<Self, Box<dyn ChromaError>> {
        // TODO: Sysdb should have a dynamic resolution in sysdb
        let sysdb = GrpcSysDb::try_from_config(worker_config).await;
        let sysdb = match sysdb {
            Ok(sysdb) => sysdb,
            Err(err) => {
                return Err(err);
            }
        };
        let path = std::path::Path::new(&worker_config.segment_manager.storage_path);
        Ok(SegmentManager::new(Box::new(sysdb), path))
    }
}
