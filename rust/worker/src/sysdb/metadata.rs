use crate::sysdb::sysdb::SysDb;
use crate::types::Segment;
use moka::future::Cache;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) struct Metadata {
    sysdb: Box<dyn SysDb>,
    segment_cache: Cache<Uuid, SegmentMetadata>,
}

impl Metadata {
    pub(crate) fn new(sysdb: Box<dyn SysDb>) -> Self {
        Self {
            sysdb,
            segment_cache: Cache::new(10000),
        }
    }

    pub(crate) async fn get_metadata(&mut self, segment_id: Uuid) -> Option<SegmentMetadata> {
        match self.segment_cache.get(&segment_id).await {
            Some(cached_entry) => {
                // Asynchronously fetch cache with Cache hit to avoid metastable failures
                self.update_metadata_on_cache_hit(segment_id);
                Some(cached_entry)
            }
            None => {
                let cached_entry = self.populate_metadata(segment_id).await.unwrap();
                self.segment_cache
                    .insert(segment_id, cached_entry.clone())
                    .await;
                Some(cached_entry)
            }
        }
    }

    async fn populate_metadata(&mut self, segment_id: Uuid) -> Option<SegmentMetadata> {
        // Query the sysdb for the cached entry
        let segments = self
            .sysdb
            .get_segments(Some(segment_id), None, None, None)
            .await;
        let segments = match segments {
            Ok(segments) => segments,
            Err(_) => {
                return None;
            }
        };
        let segment = match segments.first() {
            Some(segment) => segment,
            None => {
                return None;
            }
        };
        let collection_id = segment.collection.as_ref().unwrap();

        let collections = self
            .sysdb
            .get_collections(Some(collection_id.clone()), None, None, None)
            .await;

        let collections = match collections {
            Ok(collections) => collections,
            Err(_) => {
                return None;
            }
        };
        let collection = match collections.first() {
            Some(collection) => collection,
            None => {
                return None;
            }
        };

        let tenant = collection.tenant.clone();
        let database = collection.database.clone();
        let result = self
            .sysdb
            .get_last_compaction_time(vec![tenant.clone()])
            .await;
        let last_compaction_times = match result {
            Ok(last_compaction_times) => last_compaction_times,
            Err(_) => {
                return None;
            }
        };
        for last_compaction_time in last_compaction_times {
            if last_compaction_time.id == tenant {
                return Some(SegmentMetadata {
                    collection_id: collection_id.clone(),
                    collection_version: collection.version,
                    segment: segment.clone(),
                    tenant,
                    database,
                    last_compaction_time: last_compaction_time.last_compaction_time,
                });
            }
        }
        None
    }

    fn update_metadata_on_cache_hit(&self, segment_id: Uuid) {
        let mut self_clone = self.clone();
        tokio::spawn(async move {
            let cached_entry = self_clone.populate_metadata(segment_id).await.unwrap();
            self_clone
                .segment_cache
                .insert(segment_id, cached_entry.clone())
                .await;
            cached_entry
        });
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SegmentMetadata {
    pub(crate) collection_id: Uuid,
    pub(crate) collection_version: i32,
    pub(crate) segment: Segment,
    pub(crate) tenant: String,
    pub(crate) database: String,
    pub(crate) last_compaction_time: i64,
}
