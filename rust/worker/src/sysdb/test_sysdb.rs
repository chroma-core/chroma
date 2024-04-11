use crate::sysdb::sysdb::GetCollectionsError;
use crate::sysdb::sysdb::GetSegmentsError;
use crate::sysdb::sysdb::SysDb;
use crate::types::Collection;
use crate::types::Segment;
use crate::types::SegmentScope;
use crate::types::Tenant;
use async_trait::async_trait;
use std::collections::HashMap;
use uuid::Uuid;

use super::sysdb::GetLastCompactionTimeError;

#[derive(Clone, Debug)]
pub(crate) struct TestSysDb {
    collections: HashMap<Uuid, Collection>,
    tenant_last_compaction_time: HashMap<String, i64>,
}

impl TestSysDb {
    pub(crate) fn new() -> Self {
        TestSysDb {
            collections: HashMap::new(),
            tenant_last_compaction_time: HashMap::new(),
        }
    }

    pub(crate) fn add_collection(&mut self, collection: Collection) {
        self.collections.insert(collection.id, collection);
    }

    pub(crate) fn add_tenant_last_compaction_time(
        &mut self,
        tenant: String,
        last_compaction_time: i64,
    ) {
        self.tenant_last_compaction_time
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
}

#[async_trait]
impl SysDb for TestSysDb {
    async fn get_collections(
        &mut self,
        collection_id: Option<Uuid>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
    ) -> Result<Vec<Collection>, GetCollectionsError> {
        let mut collections = Vec::new();
        for collection in self.collections.values() {
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

    async fn get_segments(
        &mut self,
        _id: Option<Uuid>,
        _type: Option<String>,
        _scope: Option<SegmentScope>,
        _collection: Option<Uuid>,
    ) -> Result<Vec<Segment>, GetSegmentsError> {
        Ok(Vec::new())
    }

    async fn get_last_compaction_time(
        &mut self,
        tenant_ids: Vec<String>,
    ) -> Result<Vec<Tenant>, GetLastCompactionTimeError> {
        let mut tenants = Vec::new();
        for tenant_id in tenant_ids {
            let last_compaction_time = match self.tenant_last_compaction_time.get(&tenant_id) {
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
}
