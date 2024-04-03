use crate::sysdb::sysdb::GetCollectionsError;
use crate::sysdb::sysdb::GetSegmentsError;
use crate::sysdb::sysdb::SysDb;
use crate::types::Collection;
use crate::types::Segment;
use crate::types::SegmentScope;
use async_trait::async_trait;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub(crate) struct TestSysDb {
    collections: HashMap<Uuid, Collection>,
}

impl TestSysDb {
    pub(crate) fn new() -> Self {
        TestSysDb {
            collections: HashMap::new(),
        }
    }

    pub(crate) fn add_collection(&mut self, collection: Collection) {
        self.collections.insert(collection.id, collection);
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
}
