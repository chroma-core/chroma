use chroma_types::{
    BatchGetCollectionSoftDeleteStatusError, BatchGetCollectionVersionFilePathsError, Collection,
    CollectionAndSegments, CollectionUuid, CountForksError, Database, FlushCompactionResponse,
    GetCollectionByCrnError, GetCollectionSizeError, GetCollectionWithSegmentsError,
    GetSegmentsError, ListAttachedFunctionsError, ListDatabasesError, ListDatabasesResponse,
    Segment, SegmentFlushInfo, SegmentScope, SegmentType, Tenant, UpdateTenantError,
    UpdateTenantResponse,
};
use chroma_types::{GetCollectionsError, SegmentUuid};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use super::sysdb::json_to_prost_value;
use super::sysdb::FlushCompactionError;
use super::sysdb::GetLastCompactionTimeError;
use crate::sysdb::VERSION_FILE_S3_PREFIX;
use crate::GetCollectionsOptions;
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
use serde_json::Value as JsonValue;

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
    tenant_resource_names: HashMap<String, String>,
    collection_to_version_file: HashMap<CollectionUuid, CollectionVersionFile>,
    soft_deleted_collections: HashSet<CollectionUuid>,
    tasks: HashMap<chroma_types::AttachedFunctionUuid, chroma_types::AttachedFunction>,
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
                tenant_resource_names: HashMap::new(),
                collection_to_version_file: HashMap::new(),
                soft_deleted_collections: HashSet::new(),
                tasks: HashMap::new(),
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

    pub fn set_collection_version_file_path(
        &mut self,
        collection_id: CollectionUuid,
        version_file_path: String,
    ) {
        let mut inner = self.inner.lock();

        let collection = inner.collections.get_mut(&collection_id).unwrap();
        collection.version_file_path = Some(version_file_path);
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
        options: GetCollectionsOptions,
    ) -> Result<Vec<Collection>, GetCollectionsError> {
        let GetCollectionsOptions {
            collection_id,
            collection_ids: _,
            include_soft_deleted: _,
            name,
            tenant,
            database,
            limit: _,
            offset: _,
        } = options;

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

    pub(crate) async fn get_collection_by_crn(
        &mut self,
        tenant_resource_name: String,
        database: String,
        name: String,
    ) -> Result<Collection, GetCollectionByCrnError> {
        let inner = self.inner.lock();
        let tenant = inner.tenant_resource_names.get(&tenant_resource_name);
        if tenant.is_none() {
            return Err(GetCollectionByCrnError::NotFound(tenant_resource_name));
        }
        let tenant = tenant.unwrap();
        let collection = inner
            .collections
            .values()
            .find(|c| c.tenant == *tenant && c.database == database && c.name == name);
        if collection.is_none() {
            return Err(GetCollectionByCrnError::NotFound(format!(
                "{}:{}:{}",
                tenant_resource_name, database, name
            )));
        }
        Ok(collection.unwrap().clone())
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
            let Some(last_compaction_time) =
                inner.tenant_last_compaction_time.get(&tenant_id).cloned()
            else {
                continue;
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
        log_position: i64,
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
                    collection_info_mutable: Some(Default::default()),
                    ..Default::default()
                };
                version_history.versions = vec![version_info];
                new_file.version_history = Some(version_history);
                new_file
            }
        };

        let time_secs = if inner.mock_time > 0 {
            inner.mock_time as i64
        } else {
            chrono::Utc::now().timestamp()
        };

        // Get current version history
        let mut version_history = version_file.version_history.unwrap_or_default();
        let last_version_info = version_history
            .versions
            .last()
            .cloned()
            .unwrap_or_default()
            .clone();
        let mut collection_info = last_version_info
            .collection_info_mutable
            .unwrap_or_default();
        collection_info.current_collection_version = last_version_info.version + 1;
        collection_info.current_log_position = log_position;
        collection_info.updated_at_secs = time_secs;
        collection_info.last_compaction_time_secs = time_secs;

        // Create new version info with segment file paths
        let next_version = last_version_info.version + 1;
        let mut version_info = CollectionVersionInfo {
            version: next_version,
            created_at_secs: time_secs,
            version_change_reason: VersionChangeReason::DataCompaction as i32,
            collection_info_mutable: Some(collection_info),
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

        let collection = inner
            .collections
            .get_mut(&collection_id)
            .expect("Expected collection");
        collection.version_file_path = Some(version_file_name.clone());

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
        let collection = inner.collections.get(&collection_id).unwrap();
        collection.version_file_path.clone().unwrap()
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
        let result =
            self.update_collection_version_file(collection_id, segment_flush_info, log_position);
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

    pub(crate) async fn count_forks(
        &mut self,
        _source_collection_id: CollectionUuid,
    ) -> Result<usize, CountForksError> {
        Ok(10)
    }

    pub(crate) async fn list_attached_functions(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<Vec<chroma_types::chroma_proto::AttachedFunction>, ListAttachedFunctionsError> {
        let inner = self.inner.lock();
        let functions = inner
            .tasks
            .values()
            .filter(|af| af.input_collection_id == collection_id)
            .map(attached_function_to_proto)
            .collect();
        Ok(functions)
    }

    pub(crate) async fn batch_get_collection_version_file_paths(
        &self,
        collection_ids: Vec<CollectionUuid>,
    ) -> Result<HashMap<CollectionUuid, String>, BatchGetCollectionVersionFilePathsError> {
        let inner = self.inner.lock();
        let mut paths = HashMap::new();
        for collection_id in collection_ids {
            if let Some(path) = &inner
                .collections
                .get(&collection_id)
                .unwrap()
                .version_file_path
            {
                paths.insert(collection_id, path.clone());
            } else {
                return Err(BatchGetCollectionVersionFilePathsError::Grpc(
                    tonic::Status::not_found(format!(
                        "Version file not found for collection: {}",
                        collection_id
                    )),
                ));
            }
        }
        Ok(paths)
    }

    pub(crate) async fn batch_get_collection_soft_delete_status(
        &mut self,
        collection_ids: Vec<CollectionUuid>,
    ) -> Result<HashMap<CollectionUuid, bool>, BatchGetCollectionSoftDeleteStatusError> {
        let inner = self.inner.lock();
        let mut statuses = HashMap::new();
        for collection_id in collection_ids {
            if inner.soft_deleted_collections.contains(&collection_id) {
                statuses.insert(collection_id, true);
            } else if inner.collections.contains_key(&collection_id) {
                statuses.insert(collection_id, false);
            }
        }
        Ok(statuses)
    }

    pub(crate) async fn update_tenant(
        &mut self,
        tenant_id: String,
        resource_name: String,
    ) -> Result<UpdateTenantResponse, UpdateTenantError> {
        let mut inner = self.inner.lock();
        inner.tenant_resource_names.insert(tenant_id, resource_name);
        Ok(UpdateTenantResponse {})
    }

    pub(crate) async fn peek_schedule_by_collection_id(
        &mut self,
        _collection_ids: &[CollectionUuid],
    ) -> Result<Vec<chroma_types::ScheduleEntry>, crate::sysdb::PeekScheduleError> {
        Ok(vec![])
    }

    pub(crate) async fn finish_attached_function(
        &mut self,
        task_id: chroma_types::AttachedFunctionUuid,
    ) -> Result<(), chroma_types::FinishAttachedFunctionError> {
        let mut inner = self.inner.lock();
        let attached_function = inner
            .tasks
            .get_mut(&task_id)
            .ok_or(chroma_types::FinishAttachedFunctionError::AttachedFunctionNotFound)?;

        // Update lowest_live_nonce to equal next_nonce
        // This marks the current epoch as verified and complete
        attached_function.lowest_live_nonce = Some(attached_function.next_nonce);
        Ok(())
    }
}

fn attached_function_to_proto(
    attached_function: &chroma_types::AttachedFunction,
) -> chroma_types::chroma_proto::AttachedFunction {
    chroma_types::chroma_proto::AttachedFunction {
        id: attached_function.id.0.to_string(),
        name: attached_function.name.clone(),
        function_name: attached_function.function_id.to_string(),
        function_id: attached_function.function_id.to_string(),
        input_collection_id: attached_function.input_collection_id.0.to_string(),
        output_collection_name: attached_function.output_collection_name.clone(),
        output_collection_id: attached_function
            .output_collection_id
            .as_ref()
            .map(|id| id.0.to_string()),
        params: parse_params(attached_function.params.as_deref()),
        completion_offset: attached_function.completion_offset,
        min_records_for_invocation: attached_function.min_records_for_invocation,
        tenant_id: attached_function.tenant_id.clone(),
        database_id: attached_function.database_id.clone(),
        next_run_at: system_time_to_micros(attached_function.next_run),
        lowest_live_nonce: attached_function
            .lowest_live_nonce
            .as_ref()
            .map(|nonce| nonce.0.to_string()),
        next_nonce: attached_function.next_nonce.0.to_string(),
        created_at: system_time_to_micros(attached_function.created_at),
        updated_at: system_time_to_micros(attached_function.updated_at),
    }
}

fn parse_params(params: Option<&str>) -> Option<prost_types::Struct> {
    let json = params?;
    let value: JsonValue = serde_json::from_str(json).ok()?;
    match value {
        JsonValue::Object(map) => Some(prost_types::Struct {
            fields: map
                .into_iter()
                .map(|(k, v)| (k, json_to_prost_value(v)))
                .collect(),
        }),
        _ => None,
    }
}

fn system_time_to_micros(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_micros() as u64
}
