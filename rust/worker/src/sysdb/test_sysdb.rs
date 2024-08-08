use chroma_types::{
    Collection, FlushCompactionResponse, Segment, SegmentFlushInfo, SegmentScope, SegmentType,
    Tenant,
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use super::sysdb::FlushCompactionError;
use super::sysdb::GetCollectionsError;
use super::sysdb::GetLastCompactionTimeError;
use super::sysdb::GetSegmentsError;

#[derive(Clone, Debug)]
pub(crate) struct TestSysDb {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
struct Inner {
    collections: HashMap<Uuid, Collection>,
    segments: HashMap<Uuid, Segment>,
    tenant_last_compaction_time: HashMap<String, i64>,
}

impl TestSysDb {
    pub(crate) fn new() -> Self {
        TestSysDb {
            inner: Arc::new(Mutex::new(Inner {
                collections: HashMap::new(),
                segments: HashMap::new(),
                tenant_last_compaction_time: HashMap::new(),
            })),
        }
    }

    pub(crate) fn add_collection(&mut self, collection: Collection) {
        let mut inner = self.inner.lock();
        inner.collections.insert(collection.id, collection);
    }

    pub(crate) fn add_segment(&mut self, segment: Segment) {
        let mut inner = self.inner.lock();
        inner.segments.insert(segment.id, segment);
    }

    pub(crate) fn add_tenant_last_compaction_time(
        &mut self,
        tenant: String,
        last_compaction_time: i64,
    ) {
        let mut inner = self.inner.lock();
        inner
            .tenant_last_compaction_time
            .insert(tenant, last_compaction_time);
    }

    fn filter_collections(
        collection: &Collection,
        collection_id: Option<Uuid>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
    ) -> bool {
        if collection_id.is_some() && collection_id.unwrap() != collection.id {
            return false;
        }
        if name.is_some() && name.unwrap() != collection.name {
            return false;
        }
        if tenant.is_some() && tenant.unwrap() != collection.tenant {
            return false;
        }
        if database.is_some() && database.unwrap() != collection.database {
            return false;
        }
        true
    }

    fn filter_segments(
        segment: &Segment,
        id: Option<Uuid>,
        r#type: Option<String>,
        scope: Option<SegmentScope>,
        collection: Option<Uuid>,
    ) -> bool {
        if id.is_some() && id.unwrap() != segment.id {
            return false;
        }
        if let Some(r#type) = r#type {
            return segment.r#type == SegmentType::try_from(r#type.as_str()).unwrap();
        }
        if scope.is_some() && scope.unwrap() != segment.scope {
            return false;
        }
        if collection.is_some() && (collection.unwrap() != segment.collection) {
            return false;
        }
        true
    }
}

impl TestSysDb {
    pub(crate) async fn get_collections(
        &mut self,
        collection_id: Option<Uuid>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
    ) -> Result<Vec<Collection>, GetCollectionsError> {
        let inner = self.inner.lock();
        let mut collections = Vec::new();
        for collection in inner.collections.values() {
            if !TestSysDb::filter_collections(
                &collection,
                collection_id,
                name.clone(),
                tenant.clone(),
                database.clone(),
            ) {
                continue;
            }
            collections.push(collection.clone());
        }
        Ok(collections)
    }

    pub(crate) async fn get_segments(
        &mut self,
        id: Option<Uuid>,
        r#type: Option<String>,
        scope: Option<SegmentScope>,
        collection: Option<Uuid>,
    ) -> Result<Vec<Segment>, GetSegmentsError> {
        let inner = self.inner.lock();
        let mut segments = Vec::new();
        for segment in inner.segments.values() {
            if !TestSysDb::filter_segments(&segment, id, r#type.clone(), scope.clone(), collection)
            {
                continue;
            }
            segments.push(segment.clone());
        }
        Ok(segments)
    }

    pub(crate) async fn get_last_compaction_time(
        &mut self,
        tenant_ids: Vec<String>,
    ) -> Result<Vec<Tenant>, GetLastCompactionTimeError> {
        let inner = self.inner.lock();
        let mut tenants = Vec::new();
        for tenant_id in tenant_ids {
            let last_compaction_time = match inner.tenant_last_compaction_time.get(&tenant_id) {
                Some(last_compaction_time) => *last_compaction_time,
                None => {
                    // TODO: Log an error
                    return Err(GetLastCompactionTimeError::TenantNotFound);
                }
            };
            tenants.push(Tenant {
                id: tenant_id,
                last_compaction_time,
            });
        }
        Ok(tenants)
    }

    pub(crate) async fn flush_compaction(
        &mut self,
        tenant_id: String,
        collection_id: Uuid,
        log_position: i64,
        collection_version: i32,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
    ) -> Result<FlushCompactionResponse, FlushCompactionError> {
        let mut inner = self.inner.lock();
        let collection = inner.collections.get(&collection_id);
        if collection.is_none() {
            return Err(FlushCompactionError::CollectionNotFound);
        }
        let collection = collection.unwrap();
        let mut collection = collection.clone();
        collection.log_position = log_position;
        let new_collection_version = collection_version + 1;
        collection.version = new_collection_version;
        inner.collections.insert(collection.id, collection);
        let mut last_compaction_time = match inner.tenant_last_compaction_time.get(&tenant_id) {
            Some(last_compaction_time) => *last_compaction_time,
            None => 0,
        };
        last_compaction_time += 1;

        // update segments
        for segment_flush_info in segment_flush_info.iter() {
            let segment = inner.segments.get(&segment_flush_info.segment_id);
            if segment.is_none() {
                return Err(FlushCompactionError::SegmentNotFound);
            }
            let mut segment = segment.unwrap().clone();
            segment.file_path = segment_flush_info.file_paths.clone();
            inner.segments.insert(segment.id, segment);
        }

        Ok(FlushCompactionResponse::new(
            collection_id,
            new_collection_version,
            last_compaction_time,
        ))
    }
}
