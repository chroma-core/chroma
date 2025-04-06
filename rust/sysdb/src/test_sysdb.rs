use chroma_types::{
    Collection, CollectionAndSegments, CollectionUuid, Database, FlushCompactionResponse,
    GetCollectionSizeError, GetCollectionWithSegmentsError, GetSegmentsError, ListDatabasesError,
    ListDatabasesResponse, Segment, SegmentFlushInfo, SegmentScope, SegmentType, Tenant,
};
use chroma_types::{GetCollectionsError, SegmentUuid};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

use super::sysdb::FlushCompactionError;
use super::sysdb::GetLastCompactionTimeError;
use crate::sysdb::VERSION_FILE_S3_PREFIX;
use chroma_storage::PutOptions;
use chroma_types::chroma_proto::collection_version_info::VersionChangeReason;
use chroma_types::chroma_proto::CollectionInfoImmutable;
use chroma_types::chroma_proto::CollectionSegmentInfo;
use chroma_types::chroma_proto::CollectionVersionFile;
use chroma_types::chroma_proto::CollectionVersionHistory;
use chroma_types::chroma_proto::CollectionVersionInfo;
use chroma_types::chroma_proto::FlushSegmentCompactionInfo;
use chroma_types::chroma_proto::VersionListForCollection;
use chroma_types::ListCollectionVersionsError;
use chrono;
use derivative::Derivative;
use prost::Message;

#[derive(Clone, Debug)]
pub struct TestSysDb {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct Inner {
    collections: HashMap<CollectionUuid, Collection>,
    segments: HashMap<SegmentUuid, Segment>,
    tenant_last_compaction_time: HashMap<String, i64>,
    collection_to_version_file: HashMap<CollectionUuid, CollectionVersionFile>,
    collection_to_version_file_name: HashMap<CollectionUuid, String>,
    #[derivative(Debug = "ignore")]
    storage: Option<chroma_storage::Storage>,
    mock_time: u64,
}

impl TestSysDb {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        TestSysDb {
            inner: Arc::new(Mutex::new(Inner {
                collections: HashMap::new(),
                segments: HashMap::new(),
                tenant_last_compaction_time: HashMap::new(),
                collection_to_version_file: HashMap::new(),
                collection_to_version_file_name: HashMap::new(),
                storage: None,
                mock_time: 0,
            })),
        }
    }

    pub fn set_mock_time(&mut self, mock_time: u64) {
        let mut inner = self.inner.lock();
        inner.mock_time = mock_time;
    }

    pub fn add_collection(&mut self, collection: Collection) {
        let mut inner = self.inner.lock();
        inner
            .collections
            .insert(collection.collection_id, collection);
    }

    pub fn update_collection_size(&mut self, collection_id: CollectionUuid, collection_size: u64) {
        let mut inner = self.inner.lock();
        let coll = inner
            .collections
            .get_mut(&collection_id)
            .expect("Expected collection");
        coll.total_records_post_compaction = collection_size;
    }

    pub fn add_segment(&mut self, segment: Segment) {
        let mut inner = self.inner.lock();
        inner.segments.insert(segment.id, segment);
    }

    pub fn add_tenant_last_compaction_time(&mut self, tenant: String, last_compaction_time: i64) {
        let mut inner = self.inner.lock();
        inner
            .tenant_last_compaction_time
            .insert(tenant, last_compaction_time);
    }

    fn filter_collections(
        collection: &Collection,
        collection_id: Option<CollectionUuid>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
    ) -> bool {
        if collection_id.is_some() && collection_id.unwrap() != collection.collection_id {
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
        id: Option<SegmentUuid>,
        r#type: Option<String>,
        scope: Option<SegmentScope>,
        collection: CollectionUuid,
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
        if collection != segment.collection {
            return false;
        }
        true
    }

    pub(crate) async fn get_collections(
        &mut self,
        collection_id: Option<CollectionUuid>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
    ) -> Result<Vec<Collection>, GetCollectionsError> {
        let inner = self.inner.lock();
        let mut collections = Vec::new();
        for collection in inner.collections.values() {
            if !TestSysDb::filter_collections(
                collection,
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
        id: Option<SegmentUuid>,
        r#type: Option<String>,
        scope: Option<SegmentScope>,
        collection: CollectionUuid,
    ) -> Result<Vec<Segment>, GetSegmentsError> {
        let inner = self.inner.lock();
        let mut segments = Vec::new();
        for segment in inner.segments.values() {
            if !TestSysDb::filter_segments(segment, id, r#type.clone(), scope.clone(), collection) {
                continue;
            }
            segments.push(segment.clone());
        }
        Ok(segments)
    }

    pub(crate) async fn list_databases(
        &self,
        tenant: String,
        limit: Option<u32>,
        _offset: u32,
    ) -> Result<ListDatabasesResponse, ListDatabasesError> {
        let inner = self.inner.lock();
        let mut databases = Vec::new();
        let mut seen_db_names = std::collections::HashSet::new();

        for collection in inner.collections.values() {
            if collection.tenant == tenant && !seen_db_names.contains(&collection.database) {
                seen_db_names.insert(collection.database.clone());

                let db = Database {
                    id: uuid::Uuid::new_v4(),
                    name: collection.database.clone(),
                    tenant: tenant.clone(),
                };

                databases.push(db);
            }
        }

        if let Some(limit_value) = limit {
            if limit_value > 0 && databases.len() > limit_value as usize {
                databases.truncate(limit_value as usize);
            }
        }

        Ok(databases)
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

    fn update_collection_version_file(
        &mut self,
        collection_id: CollectionUuid,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
    ) -> Result<(), String> {
        // Check if version file already exists for the collection.
        // If it does not, then create a new one with version 0.
        // Update the version file by adding a new entry for the new version.
        // The new entry should have the correct file paths for the segment,
        // and the version number should be the next number in the sequence.

        let mut inner = self.inner.lock();
        // Get the current version file for the collection or create a new one
        let mut version_file = match inner.collection_to_version_file.get(&collection_id) {
            Some(existing_file) => existing_file.clone(),
            None => {
                // Initialize new CollectionVersionFile with version 0
                let mut new_file = CollectionVersionFile::default();
                let collection_info = CollectionInfoImmutable {
                    collection_id: collection_id.to_string(),
                    ..Default::default()
                };
                new_file.collection_info_immutable = Some(collection_info);

                let mut version_history = CollectionVersionHistory::default();
                let version_info = CollectionVersionInfo {
                    version: 0,
                    created_at_secs: if inner.mock_time > 0 {
                        inner.mock_time as i64
                    } else {
                        chrono::Utc::now().timestamp()
                    },
                    version_change_reason: VersionChangeReason::DataCompaction as i32,
                    ..Default::default()
                };
                version_history.versions = vec![version_info];
                new_file.version_history = Some(version_history);
                new_file
            }
        };

        // Get current version history
        let mut version_history = version_file.version_history.unwrap_or_default();
        let next_version = version_history
            .versions
            .last()
            .map(|v| v.version + 1)
            .unwrap_or(0);

        // Create new version info with segment file paths
        let mut version_info = CollectionVersionInfo {
            version: next_version,
            created_at_secs: if inner.mock_time > 0 {
                inner.mock_time as i64
            } else {
                chrono::Utc::now().timestamp()
            },
            version_change_reason: VersionChangeReason::DataCompaction as i32,
            ..Default::default()
        };

        let mut segment_info = CollectionSegmentInfo::default();
        let mut flush_compaction_infos = Vec::new();

        // Iterate through all segment flush infos
        for segment_flush_info in segment_flush_info.iter() {
            let flush_compaction_info: FlushSegmentCompactionInfo = segment_flush_info
                .try_into()
                .expect("Failed to convert SegmentFlushInfo");
            flush_compaction_infos.push(flush_compaction_info);
        }

        segment_info.segment_compaction_info = flush_compaction_infos;
        version_info.segment_info = Some(segment_info);

        // Add new version to history
        version_history.versions.push(version_info);
        version_file.version_history = Some(version_history);

        tracing::debug!(line = line!(), "version_file: \n{:#?}", version_file);

        // Update the version file name.
        let version_file_name = format!(
            "{}{}/{}",
            VERSION_FILE_S3_PREFIX, collection_id, next_version
        );

        inner
            .collection_to_version_file_name
            .insert(collection_id, version_file_name.clone());

        // Serialize the version file to bytes and write to storage
        let version_file_bytes = version_file.encode_to_vec();

        // Extract storage reference before unlocking
        let storage = inner.storage.clone();
        drop(inner);

        // Write the serialized bytes to storage
        if let Some(storage) = storage {
            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(storage.put_bytes(
                    &version_file_name,
                    version_file_bytes,
                    PutOptions::default(),
                ))
            });
            if result.is_err() {
                return Err("Failed to write version file to storage".to_string());
            }
        }

        let mut inner = self.inner.lock();
        // Update the version file in the collection_to_version_file map
        inner
            .collection_to_version_file
            .insert(collection_id, version_file);
        Ok(())
    }

    pub(crate) async fn list_collection_versions(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<Vec<CollectionVersionInfo>, ListCollectionVersionsError> {
        let inner = self.inner.lock();
        let version_file = inner.collection_to_version_file.get(&collection_id);
        if version_file.is_none() {
            return Err(ListCollectionVersionsError::NotFound(
                collection_id.to_string(),
            ));
        }
        let version_file = version_file.unwrap();
        Ok(version_file
            .version_history
            .as_ref()
            .unwrap()
            .versions
            .clone())
    }

    // For testing purposes, set the storage for the sysdb.
    pub fn set_storage(&mut self, storage: Option<chroma_storage::Storage>) {
        let mut inner = self.inner.lock();
        inner.storage = storage;
    }

    pub fn get_version_file_name(&self, collection_id: CollectionUuid) -> String {
        let inner = self.inner.lock();
        inner
            .collection_to_version_file_name
            .get(&collection_id)
            .unwrap()
            .clone()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn flush_compaction(
        &mut self,
        tenant_id: String,
        collection_id: CollectionUuid,
        log_position: i64,
        collection_version: i32,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
        total_records_post_compaction: u64,
        size_bytes_post_compaction: u64,
    ) -> Result<FlushCompactionResponse, FlushCompactionError> {
        // Print the segment flush info
        let new_collection_version: i32;
        let mut last_compaction_time: i64;
        {
            let mut inner = self.inner.lock();
            let collection = inner.collections.get(&collection_id);
            if collection.is_none() {
                return Err(FlushCompactionError::CollectionNotFound);
            }
            let collection = collection.unwrap();
            let mut collection = collection.clone();
            collection.log_position = log_position;
            new_collection_version = collection_version + 1;
            collection.version = new_collection_version;
            collection.total_records_post_compaction = total_records_post_compaction;
            collection.size_bytes_post_compaction = size_bytes_post_compaction;
            inner
                .collections
                .insert(collection.collection_id, collection);
            last_compaction_time = match inner.tenant_last_compaction_time.get(&tenant_id) {
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
        }

        // Update the in-memory version file
        let result = self.update_collection_version_file(collection_id, segment_flush_info);
        if result.is_err() {
            return Err(FlushCompactionError::FailedToFlushCompaction(
                tonic::Status::internal("Failed to update version file"),
            ));
        }

        Ok(FlushCompactionResponse::new(
            collection_id,
            new_collection_version,
            last_compaction_time,
        ))
    }

    pub(crate) async fn mark_version_for_deletion(
        &self,
        _epoch_id: i64,
        versions: Vec<VersionListForCollection>,
    ) -> Result<(), String> {
        // For testing success case, return Ok when versions are not empty
        if !versions.is_empty() && !versions[0].versions.is_empty() {
            // Simulate error case when version is 0
            if versions[0].versions.contains(&0) {
                return Err("Failed to mark version for deletion".to_string());
            }
            Ok(())
        } else {
            Ok(())
        }
    }

    pub async fn delete_collection_version(
        &self,
        _versions: Vec<VersionListForCollection>,
    ) -> HashMap<String, bool> {
        // For testing, return success for all collections
        let mut results = HashMap::new();
        for version_list in _versions {
            results.insert(version_list.collection_id, true);
        }
        results
    }

    pub(crate) async fn get_collection_size(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<usize, GetCollectionSizeError> {
        let inner = self.inner.lock();
        let collection = inner.collections.get(&collection_id);
        match collection {
            Some(collection) => Ok(collection.total_records_post_compaction as usize),
            None => Err(GetCollectionSizeError::NotFound(
                "Collection not found".to_string(),
            )),
        }
    }

    pub(crate) async fn get_collection_with_segments(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<CollectionAndSegments, GetCollectionWithSegmentsError> {
        let inner = self.inner.lock();
        let collection = inner.collections.get(&collection_id).cloned().ok_or(
            GetCollectionWithSegmentsError::NotFound(
                "Collection not found in TestSysDB".to_string(),
            ),
        )?;
        let segments = inner
            .segments
            .values()
            .filter_map(|seg| {
                (seg.collection == collection_id).then_some((seg.r#type, seg.clone()))
            })
            .collect::<HashMap<_, _>>();
        let record_segment = segments
            .get(&SegmentType::BlockfileRecord)
            .or(segments.get(&SegmentType::Sqlite))
            .cloned()
            .ok_or(GetCollectionWithSegmentsError::NotFound(
                "Record segment not found in TestSysDB".to_string(),
            ))?;
        let metadata_segment = segments
            .get(&SegmentType::BlockfileMetadata)
            .or(segments.get(&SegmentType::Sqlite))
            .cloned()
            .ok_or(GetCollectionWithSegmentsError::NotFound(
                "Metadata segment not found in TestSysDB".to_string(),
            ))?;
        let vector_segment = segments
            .get(&SegmentType::HnswDistributed)
            .or(segments.get(&SegmentType::HnswLocalMemory))
            .or(segments.get(&SegmentType::HnswLocalPersisted))
            .or(segments.get(&SegmentType::Spann))
            .cloned()
            .ok_or(GetCollectionWithSegmentsError::NotFound(
                "Vector segment not found in TestSysDB".to_string(),
            ))?;

        Ok(CollectionAndSegments {
            collection,
            metadata_segment,
            record_segment,
            vector_segment,
        })
    }
}
