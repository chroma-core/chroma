use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use chroma_config::assignment::assignment_policy::AssignmentPolicy;
use chroma_log::{CollectionInfo, CollectionRecord, Log};
use chroma_memberlist::memberlist_provider::{Member, Memberlist};
use chroma_sysdb::{DatabaseOrTopology, GetCollectionsOptions, SysDb};
use chroma_types::{CollectionUuid, DatabaseName, JobId, TopologyName};
use figment::providers::Env;
use figment::Figment;
use opentelemetry::metrics::{Counter, Gauge};
use serde::Deserialize;
use uuid::Uuid;

use crate::compactor::scheduler_policy::{ScheduleContext, SchedulerPolicy};
use crate::compactor::types::CompactionJob;

#[derive(Debug, Clone)]
pub(crate) struct SchedulerMetrics {
    job_failure_count: Counter<u64>,
    unpenalized_job_failure_count: Counter<u64>,
    unaddressable_jobs_count: Gauge<u64>,
}

impl Default for SchedulerMetrics {
    fn default() -> Self {
        let meter = opentelemetry::global::meter("chroma_compactor");
        let job_failure_count = meter
            .u64_counter("compactor_job_failure_count")
            .with_description(
                "Compaction job failures charged to the collection, which count toward \
                 max_failure_count and can eventually dead-letter it. Failures the \
                 collection could not have caused are counted separately, under \
                 compactor_unpenalized_job_failure_count",
            )
            .build();
        let unpenalized_job_failure_count = meter
            .u64_counter("compactor_unpenalized_job_failure_count")
            .with_description(
                "Compaction job failures not counted against the collection, because the \
                 cause was node-local rather than anything about the collection",
            )
            .build();
        let unaddressable_jobs_count = meter
            .u64_gauge("compactor_unaddressable_jobs_count")
            .with_description("Number of jobs skipped due to scheduler capacity limits")
            .build();

        Self {
            job_failure_count,
            unpenalized_job_failure_count,
            unaddressable_jobs_count,
        }
    }
}

impl SchedulerMetrics {
    fn increment_job_failure_count(&self) {
        self.job_failure_count.add(1, &[]);
    }

    fn increment_unpenalized_job_failure_count(&self) {
        self.unpenalized_job_failure_count.add(1, &[]);
    }

    fn set_unaddressable_jobs_count(&self, count: u64) {
        self.unaddressable_jobs_count.record(count, &[]);
    }
}

pub(crate) struct InProgressJob {
    pub(crate) expires_at: SystemTime,
    pub(crate) database_name: DatabaseName,
    /// The size of the collection in bytes, used for memory-bounded scheduling.
    pub(crate) collection_size_bytes: u64,
}

impl InProgressJob {
    fn new(
        job_expiry_seconds: u64,
        database_name: DatabaseName,
        collection_size_bytes: u64,
    ) -> Self {
        Self {
            expires_at: SystemTime::now() + Duration::from_secs(job_expiry_seconds),
            database_name,
            collection_size_bytes,
        }
    }

    fn is_expired(&self) -> bool {
        SystemTime::now() >= self.expires_at
    }
}

pub(crate) struct Scheduler {
    my_member_id: String,
    log: Log,
    sysdb: SysDb,
    policy: Box<dyn SchedulerPolicy>,
    job_queue: Vec<CompactionJob>,
    max_concurrent_jobs: usize,
    min_compaction_size: usize,
    memberlist: Option<Memberlist>,
    assignment_policy: Box<dyn AssignmentPolicy>,
    oneoff_collections: HashMap<CollectionUuid, DatabaseName>,
    pending_oneoff_ids: Vec<CollectionUuid>,
    disabled_collections: HashSet<CollectionUuid>,
    deleted_collections: HashMap<CollectionUuid, Option<TopologyName>>,
    collections_needing_repair: HashMap<CollectionUuid, (DatabaseName, i64)>,
    in_progress_jobs: HashMap<JobId, InProgressJob>,
    job_expiry_seconds: u64,
    max_failure_count: i32,
    metrics: SchedulerMetrics,
}

#[derive(Deserialize, Debug)]
struct RunTimeConfig {
    disabled_collections: Vec<String>,
}

impl Scheduler {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        my_ip: String,
        log: Log,
        sysdb: SysDb,
        policy: Box<dyn SchedulerPolicy>,
        max_concurrent_jobs: usize,
        min_compaction_size: usize,
        assignment_policy: Box<dyn AssignmentPolicy>,
        disabled_collections: HashSet<CollectionUuid>,
        job_expiry_seconds: u64,
        max_failure_count: i32,
    ) -> Scheduler {
        Scheduler {
            my_member_id: my_ip,
            log,
            sysdb,
            min_compaction_size,
            policy,
            job_queue: Vec::with_capacity(max_concurrent_jobs),
            max_concurrent_jobs,
            memberlist: None,
            assignment_policy,
            oneoff_collections: HashMap::new(),
            pending_oneoff_ids: Vec::new(),
            disabled_collections,
            deleted_collections: HashMap::new(),
            collections_needing_repair: HashMap::new(),
            in_progress_jobs: HashMap::new(),
            job_expiry_seconds,
            max_failure_count,
            metrics: SchedulerMetrics::default(),
        }
    }

    /// Returns the total size in bytes of all collections currently being compacted.
    ///
    /// Expired jobs are excluded: they are treated as no longer in progress
    /// (consistent with `is_job_in_progress`), so a stale entry cannot
    /// permanently eat into the memory budget.
    fn current_in_flight_size_bytes(&self) -> u64 {
        self.in_progress_jobs
            .values()
            .filter(|job| !job.is_expired())
            .map(|job| job.collection_size_bytes)
            .sum()
    }

    pub(crate) async fn add_oneoff_collections(&mut self, ids: Vec<CollectionUuid>) {
        if ids.is_empty() {
            return;
        }

        const BATCH_SIZE: usize = 1_000;
        for batch in ids.chunks(BATCH_SIZE) {
            let collections = match self
                .sysdb
                .get_collections(GetCollectionsOptions {
                    collection_ids: Some(batch.to_vec()),
                    database_or_topology: None,
                    limit: Some(batch.len() as u32),
                    offset: 0,
                    include_soft_deleted: false,
                    collection_id: None,
                    name: None,
                    tenant: None,
                })
                .await
            {
                Ok(collections) => collections,
                Err(e) => {
                    tracing::error!(
                        error = ?e,
                        "Error fetching one-off collections from sysdb"
                    );
                    self.pending_oneoff_ids.extend(batch.iter().copied());
                    continue;
                }
            };

            let found_ids: HashSet<_> = collections.iter().map(|c| c.collection_id).collect();
            for collection_id in batch {
                if !found_ids.contains(collection_id) {
                    tracing::warn!(
                        collection_id = %collection_id,
                        "Requested one-off compaction for collection not found in sysdb"
                    );
                }
            }

            for collection in collections {
                let Some(database_name) = DatabaseName::new(collection.database) else {
                    tracing::warn!(
                        collection_id = %collection.collection_id,
                        "Invalid database name for one-off collection"
                    );
                    continue;
                };
                self.oneoff_collections
                    .insert(collection.collection_id, database_name);
            }
        }
    }

    pub(crate) fn get_oneoff_collections(&self) -> Vec<CollectionUuid> {
        self.oneoff_collections.keys().cloned().collect()
    }

    pub(crate) fn drain_deleted_collections(
        &mut self,
    ) -> Vec<(CollectionUuid, Option<TopologyName>)> {
        self.deleted_collections.drain().collect()
    }

    pub(crate) fn drain_collections_requiring_repair(
        &mut self,
    ) -> Vec<(DatabaseName, CollectionUuid, i64)> {
        self.collections_needing_repair
            .drain()
            .map(|(k, (d, o))| (d, k, o))
            .collect()
    }

    pub(crate) fn require_repair(
        &mut self,
        collection_id: CollectionUuid,
        database_name: DatabaseName,
        offset_in_sysdb: i64,
    ) {
        self.collections_needing_repair
            .insert(collection_id, (database_name, offset_in_sysdb));
    }

    async fn get_collections_with_new_data(&mut self) -> Vec<CollectionInfo> {
        let collections = self
            .log
            .get_collections_with_new_data(self.min_compaction_size as u64)
            .await;
        let one_off_collections = self
            .oneoff_collections
            .iter()
            .map(|x| CollectionInfo {
                collection_id: *x.0,
                topology_name: x.1.topology().and_then(|t| TopologyName::new(t).ok()),
                first_log_offset: 0,
                first_log_ts: 0,
            })
            .collect::<Vec<_>>();

        match collections {
            Ok(mut collections) => {
                tracing::info!("Collections with new data: {collections:?}");
                let collection_ids: HashSet<_> =
                    collections.iter().map(|c| c.collection_id).collect();
                let one_off_collections = one_off_collections
                    .into_iter()
                    .filter(|c| !collection_ids.contains(&c.collection_id));
                collections.extend(one_off_collections);
                collections
            }
            Err(e) => {
                tracing::error!("Error: {:?}", e);
                one_off_collections
            }
        }
    }

    async fn verify_and_enrich_collections(
        &mut self,
        collections: Vec<CollectionInfo>,
    ) -> Vec<CollectionRecord> {
        let mut by_topology: HashMap<Option<DatabaseOrTopology>, Vec<CollectionInfo>> =
            HashMap::new();
        for collection_info in collections {
            let entry = by_topology
                .entry(
                    collection_info
                        .topology_name
                        .clone()
                        .map(DatabaseOrTopology::Topology),
                )
                .or_default();
            entry.push(collection_info);
        }
        let mut collection_records = Vec::new();
        for (topology, collection_infos) in by_topology {
            const BATCH_SIZE: usize = 1_000;
            let ids: Vec<CollectionUuid> =
                collection_infos.iter().map(|c| c.collection_id).collect();
            let mut all_collections = Vec::new();
            let mut had_error = false;
            for batch in ids.chunks(BATCH_SIZE) {
                let result = self
                    .sysdb
                    .get_collections(GetCollectionsOptions {
                        collection_ids: Some(batch.to_vec()),
                        database_or_topology: topology.clone(),
                        limit: Some(batch.len() as u32),
                        offset: 0,
                        include_soft_deleted: false,
                        collection_id: None,
                        name: None,
                        tenant: None,
                    })
                    .await;
                match result {
                    Ok(collections) => {
                        all_collections.extend(collections);
                    }
                    Err(e) => {
                        tracing::error!("error fetching for topo = {topology:?}: {e}");
                        had_error = true;
                        break;
                    }
                }
            }
            if had_error {
                continue;
            }
            if all_collections.len() != collection_infos.len() {
                tracing::warn!(
                    "returned collection info does not match number of input collections"
                );
            }
            let mut info_map: HashMap<_, _> = collection_infos
                .into_iter()
                .map(|c| (c.collection_id, c))
                .collect();
            let mut with_infos = vec![];
            for collection in all_collections.into_iter() {
                if let Some(info) = info_map.remove(&collection.collection_id) {
                    // One-off (manually requested) compactions skip the failure-count
                    // gate: a manual request is the operator's way to retry a
                    // collection that has been dead-lettered.
                    if collection.compaction_failure_count >= self.max_failure_count
                        && !self
                            .oneoff_collections
                            .contains_key(&collection.collection_id)
                    {
                        tracing::info!(
                            "Ignoring collection {} - too many compaction failures ({}/{})",
                            collection.collection_id,
                            collection.compaction_failure_count,
                            self.max_failure_count
                        );
                    } else {
                        with_infos.push((collection, info));
                    }
                }
            }
            for (id, info) in info_map {
                self.oneoff_collections.remove(&id);
                self.deleted_collections.insert(id, info.topology_name);
            }
            for (collection, info) in with_infos.into_iter() {
                // offset in log is the first offset in the log that has not been compacted. Note that
                // since the offset is the first offset of log we get from the log service, we should
                // use this offset to pull data from the log service.
                if collection.log_position + 1 < info.first_log_offset {
                    tracing::error!(
                        collection = collection.collection_id.to_string(),
                        sysdb_log_position = (collection.log_position + 1),
                        collection_log_position = info.first_log_offset,
                        name = "offset in sysdb is less than offset in log"
                    )
                } else {
                    collection_records.push(CollectionRecord {
                        collection_id: collection.collection_id,
                        tenant_id: collection.tenant.clone(),
                        database_name: collection.database.clone(),
                        last_compaction_time: Default::default(),
                        first_record_time: info.first_log_ts,
                        offset: collection.log_position + 1,
                        collection_version: collection.version,
                        collection_logical_size_bytes: collection.size_bytes_post_compaction,
                    });
                }
            }
        }
        collection_records
    }

    async fn filter_collections(
        &mut self,
        collections: Vec<CollectionInfo>,
    ) -> Vec<CollectionInfo> {
        let mut filtered_collections = Vec::new();
        let members = self.memberlist.as_ref().unwrap();
        let members_as_string = members
            .iter()
            .map(|member| member.member_id.clone())
            .collect();
        self.assignment_policy.set_members(members_as_string);

        for collection in collections {
            if self
                .disabled_collections
                .contains(&collection.collection_id)
            {
                if self
                    .oneoff_collections
                    .contains_key(&collection.collection_id)
                {
                    tracing::warn!(
                        "Skipping one-off compaction for {:?} because it is disabled for compaction",
                        collection.collection_id
                    );
                } else {
                    tracing::info!(
                        "Ignoring collection: {:?} because it is disabled for compaction",
                        collection.collection_id
                    );
                }
                continue;
            }

            if self.is_job_in_progress(&collection.collection_id).await {
                tracing::info!(
                    "Compaction for {} is already in progress, skipping",
                    collection.collection_id
                );
                continue;
            }

            // One-off collections were explicitly requested on this node, so run
            // them here even if the assignment policy would give them to another
            // member. The disabled_collections check above still applies to them.
            if self
                .oneoff_collections
                .contains_key(&collection.collection_id)
            {
                filtered_collections.push(collection);
                continue;
            }

            let result = self
                .assignment_policy
                // NOTE(rescrv):  Need to use the untyped uuid here.
                .assign_one(collection.collection_id.0.to_string().as_str());

            match result {
                Ok(member) => {
                    if member == self.my_member_id {
                        filtered_collections.push(collection);
                    }
                }
                Err(e) => {
                    tracing::error!("Error: {:?}", e);
                    continue;
                }
            }
        }
        filtered_collections
    }

    pub(crate) async fn schedule_internal(&mut self, collection_records: Vec<CollectionRecord>) {
        self.job_queue.clear();
        let mut oneoff_collections = Vec::with_capacity(collection_records.len());
        let mut regular_collections = Vec::with_capacity(collection_records.len());
        for record in collection_records {
            let database_name = match DatabaseName::new(record.database_name.clone()) {
                Some(db_name) => db_name,
                None => {
                    tracing::warn!(
                        collection_id = %record.collection_id,
                        database_name = %record.database_name,
                        "Invalid database name for collection",
                    );
                    continue;
                }
            };
            if self.is_job_in_progress(&record.collection_id).await {
                tracing::info!(
                    collection_id = record.collection_id.to_string(),
                    "Compaction is already in progress, skipping",
                );
            } else if let Some(database_name) = self.oneoff_collections.get(&record.collection_id) {
                oneoff_collections.push((database_name.clone(), record));
            } else {
                regular_collections.push((database_name, record));
            }
        }
        let mut dropped_jobs_count = 0;
        let mut rem_capacity = self
            .max_concurrent_jobs
            .saturating_sub(self.in_progress_jobs.len());
        dropped_jobs_count += oneoff_collections.len().saturating_sub(rem_capacity);
        for (database_name, record) in oneoff_collections.into_iter().take(rem_capacity) {
            tracing::info!(
                collection_version = record.collection_version,
                "Creating one-off compaction job for collection"
            );
            self.job_queue.push(CompactionJob {
                collection_id: record.collection_id,
                database_name: database_name.clone(),
                tenant_id: record.tenant_id.clone(),
                collection_size_bytes: record.collection_logical_size_bytes,
            });
            self.oneoff_collections.remove(&record.collection_id);
            rem_capacity -= 1;
        }
        dropped_jobs_count += regular_collections.len().saturating_sub(rem_capacity);
        let records: Vec<CollectionRecord> = regular_collections
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        let mut selected = self.policy.determine(
            records.clone(),
            ScheduleContext {
                max_jobs: rem_capacity as i32,
                in_flight_size_bytes: self.current_in_flight_size_bytes(),
            },
        );
        selected.truncate(rem_capacity);
        let seen: HashSet<CollectionUuid> = selected.iter().map(|r| r.collection_id).collect();
        for record in &records {
            if !seen.contains(&record.collection_id) {
                tracing::info!(
                    collection_id = %record.collection_id,
                    "Max concurrent jobs reached, skipping compaction"
                );
            }
        }
        for job in &selected {
            tracing::info!(
                collection_id = %job.collection_id,
                "Enqueuing compaction job"
            );
        }
        self.job_queue.extend(selected);
        self.metrics
            .set_unaddressable_jobs_count(dropped_jobs_count as u64);
        // At this point, nobody should modify the job queue and every collection
        // in the job queue will definitely be compacted. It is now safe to add
        // them to the in-progress set.
        let job_info: Vec<_> = self
            .job_queue
            .iter()
            .map(|j| {
                (
                    j.collection_id,
                    j.database_name.clone(),
                    j.collection_size_bytes,
                )
            })
            .collect();
        for (collection_id, database_name, collection_size_bytes) in job_info {
            self.add_in_progress(collection_id, database_name, collection_size_bytes);
        }
    }

    async fn is_job_in_progress(&mut self, collection_id: &CollectionUuid) -> bool {
        let job_id = (*collection_id).into();
        match self.in_progress_jobs.get(&job_id) {
            Some(job) if job.is_expired() => {
                tracing::info!(
                    "Compaction for {} is expired, removing from dedup set.",
                    collection_id
                );
                self.fail_job(job_id).await;
                false
            }
            Some(_) => true,
            None => false,
        }
    }

    fn add_in_progress(
        &mut self,
        collection_id: CollectionUuid,
        database_name: DatabaseName,
        collection_size_bytes: u64,
    ) {
        self.in_progress_jobs.insert(
            collection_id.into(),
            InProgressJob::new(
                self.job_expiry_seconds,
                database_name,
                collection_size_bytes,
            ),
        );
    }

    pub(crate) fn succeed_job(&mut self, job_id: JobId) {
        tracing::info!("Compaction for {} just successfully finished", job_id);
        if self.in_progress_jobs.remove(&job_id).is_none() {
            tracing::warn!(
                "Expired compaction for {} just successfully finished.",
                job_id
            );
        }
    }

    /// Releases a job that failed for a reason the collection cannot influence —
    /// a dependency this node could not reach, say. The job is cleared so it can
    /// be scheduled again, but the collection's failure count is left untouched.
    ///
    /// Counting these would be actively harmful: five such failures dead-letter
    /// the collection permanently (`verify_and_enrich_collections` then drops it
    /// on every tick), so a transient node-local outage would take a healthy
    /// collection out of compaction forever and nothing would put it back.
    pub(crate) fn release_job_without_penalty(&mut self, job_id: JobId) {
        tracing::info!(
            "Releasing compaction for {} without counting it against the collection",
            job_id
        );
        self.metrics.increment_unpenalized_job_failure_count();
        if self.in_progress_jobs.remove(&job_id).is_none() {
            tracing::warn!("Expired compaction for {} was released.", job_id);
        }
    }

    /// Marks a job as failed and persists the failure count to sysdb.
    pub(crate) async fn fail_job(&mut self, job_id: JobId) {
        tracing::info!("Failing compaction for {}", job_id.0);
        // Get the database_name and remove the job in one operation
        let db_entry = self
            .in_progress_jobs
            .remove(&job_id)
            .map(|job| job.database_name);

        self.metrics.increment_job_failure_count();

        match db_entry {
            Some(database_name) => {
                // Increment failure count in sysdb for persistent tracking across nodes
                let collection_id = CollectionUuid(job_id.0);

                if let Err(e) = self
                    .sysdb
                    .increment_compaction_failure_count(collection_id, &database_name)
                    .await
                {
                    tracing::warn!(
                        "Failed to increment compaction failure count in sysdb for {}: {:?}.",
                        job_id,
                        e
                    );
                }
            }
            None => {
                tracing::warn!(
                    "Expired compaction for {} just unsuccessfully finished.",
                    job_id
                );
            }
        }
    }

    pub(crate) fn recompute_disabled_collections(&mut self) {
        let config = Figment::new()
            .merge(
                Env::prefixed("CHROMA_")
                    .map(|k| k.as_str().replace("__", ".").into())
                    .map(|k| {
                        if k == "COMPACTION_SERVICE.COMPACTOR.DISABLED_COLLECTIONS" {
                            k["COMPACTION_SERVICE.COMPACTOR.".len()..].into()
                        } else {
                            k.into()
                        }
                    })
                    .only(&["DISABLED_COLLECTIONS"]),
            )
            .extract::<RunTimeConfig>();
        if let Ok(config) = config {
            self.disabled_collections = config
                .disabled_collections
                .iter()
                .map(|collection| CollectionUuid(Uuid::from_str(collection).unwrap()))
                .collect();
        }
    }

    pub(crate) async fn schedule(&mut self) {
        // For now, we clear the job queue every time, assuming we will not have any pending jobs running
        self.job_queue.clear();

        if self.memberlist.is_none() || self.memberlist.as_ref().unwrap().is_empty() {
            tracing::error!("Memberlist is not set or empty. Cannot schedule compaction jobs.");
            return;
        }

        // Retry any one-off collection IDs whose sysdb lookup failed previously.
        if !self.pending_oneoff_ids.is_empty() {
            let pending = std::mem::take(&mut self.pending_oneoff_ids);
            self.add_oneoff_collections(pending).await;
        }

        // Recompute disabled list.
        self.recompute_disabled_collections();
        let collections = self.get_collections_with_new_data().await;
        if collections.is_empty() {
            return;
        }
        let filtered_collections = self.filter_collections(collections).await;
        let collection_records = self
            .verify_and_enrich_collections(filtered_collections)
            .await;
        self.schedule_internal(collection_records).await;
    }

    pub(crate) fn get_jobs(&self) -> impl Iterator<Item = &CompactionJob> {
        self.job_queue.iter()
    }

    pub(crate) fn get_in_progress_jobs(&self) -> Vec<(JobId, &InProgressJob)> {
        self.in_progress_jobs
            .iter()
            .map(|(id, job)| (*id, job))
            .collect()
    }

    pub(crate) fn set_memberlist(&mut self, memberlist: Memberlist) {
        self.memberlist = Some(memberlist);
    }

    pub(crate) fn has_memberlist(&self) -> bool {
        self.memberlist.is_some()
    }

    pub(crate) fn get_memberlist(&self) -> Vec<Member> {
        self.memberlist.as_ref().cloned().unwrap_or_default()
    }

    pub(crate) fn get_assignment_policy(&mut self) -> &mut Box<dyn AssignmentPolicy> {
        &mut self.assignment_policy
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serial_test::serial;

    use super::*;
    use crate::compactor::scheduler_policy::LasCompactionTimeSchedulerPolicy;
    use chroma_config::assignment::assignment_policy::RendezvousHashingAssignmentPolicy;
    use chroma_log::in_memory_log::{InMemoryLog, InternalLogRecord};
    use chroma_memberlist::memberlist_provider::Member;
    use chroma_sysdb::TestSysDb;
    use chroma_types::{Collection, LogRecord, Operation, OperationRecord};

    /// Shared setup for scheduler tests.
    struct SchedulerFixture {
        scheduler: Scheduler,
        collection_uuid_1: CollectionUuid,
        collection_uuid_2: CollectionUuid,
        my_member: Member,
    }

    impl SchedulerFixture {
        fn new() -> Self {
            Self::with_max_failure_count(3)
        }

        fn with_max_failure_count(max_failure_count: i32) -> Self {
            let mut log = Log::InMemory(InMemoryLog::new());
            let in_memory_log = match log {
                Log::InMemory(ref mut in_memory_log) => in_memory_log,
                _ => panic!("Invalid log type"),
            };

            let tenant_1 = "tenant_1".to_string();
            let collection_1 = Collection {
                collection_id: CollectionUuid::from_str("00000000-0000-0000-0000-000000000001")
                    .unwrap(),
                name: "collection_1".to_string(),
                dimension: Some(1),
                tenant: tenant_1.clone(),
                database: "database_1".to_string(),
                ..Default::default()
            };
            let collection_uuid_1 = collection_1.collection_id;

            in_memory_log.add_log(
                collection_uuid_1,
                InternalLogRecord {
                    collection_id: collection_uuid_1,
                    log_offset: 0,
                    log_ts: 1,
                    record: LogRecord {
                        log_offset: 0,
                        record: OperationRecord {
                            id: "embedding_id_1".to_string(),
                            embedding: None,
                            encoding: None,
                            metadata: None,
                            document: None,
                            operation: Operation::Add,
                        },
                    },
                },
            );

            let tenant_2 = "tenant_2".to_string();
            let collection_2 = Collection {
                collection_id: CollectionUuid::from_str("00000000-0000-0000-0000-000000000002")
                    .unwrap(),
                name: "collection_2".to_string(),
                dimension: Some(1),
                tenant: tenant_2.clone(),
                database: "database_2".to_string(),
                ..Default::default()
            };
            let collection_uuid_2 = collection_2.collection_id;

            in_memory_log.add_log(
                collection_uuid_2,
                InternalLogRecord {
                    collection_id: collection_uuid_2,
                    log_offset: 0,
                    log_ts: 2,
                    record: LogRecord {
                        log_offset: 0,
                        record: OperationRecord {
                            id: "embedding_id_2".to_string(),
                            embedding: None,
                            encoding: None,
                            metadata: None,
                            document: None,
                            operation: Operation::Add,
                        },
                    },
                },
            );

            let mut sysdb = SysDb::Test(TestSysDb::new());
            match sysdb {
                SysDb::Test(ref mut test_sysdb) => {
                    test_sysdb.add_collection(collection_1);
                    test_sysdb.add_collection(collection_2);
                    test_sysdb.add_tenant_last_compaction_time(tenant_1, 2);
                }
                _ => panic!("Invalid sysdb type"),
            }

            let my_member = Member {
                member_id: "member_1".to_string(),
                member_ip: "10.0.0.1".to_string(),
                member_node_name: "node_1".to_string(),
            };

            let mut assignment_policy = Box::new(RendezvousHashingAssignmentPolicy::default());
            assignment_policy.set_members(vec![my_member.member_id.clone()]);

            let scheduler = Scheduler::new(
                my_member.member_id.clone(),
                log,
                sysdb.clone(),
                Box::new(LasCompactionTimeSchedulerPolicy {}),
                1000,
                1,
                assignment_policy,
                HashSet::new(),
                3600,
                max_failure_count,
            );

            Self {
                scheduler,
                collection_uuid_1,
                collection_uuid_2,
                my_member,
            }
        }

        /// Clear env vars that may leak between tests.
        fn clear_env_vars() {
            std::env::remove_var("CHROMA_COMPACTION_SERVICE__COMPACTOR__DISABLED_COLLECTIONS");
            std::env::remove_var("CHROMA_COMPACTION_SERVICE.COMPACTOR.DISABLED_COLLECTIONS");
            std::env::remove_var("CHROMA_COMPACTION_SERVICE.IRRELEVANT");
        }
    }

    #[tokio::test]
    #[serial]
    async fn schedule_without_memberlist_produces_no_jobs() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        f.scheduler.schedule().await;
        assert_eq!(f.scheduler.get_jobs().count(), 0, "no memberlist set");
    }

    #[tokio::test]
    #[serial]
    async fn schedule_with_empty_memberlist_produces_no_jobs() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        f.scheduler.set_memberlist(vec![]);
        f.scheduler.schedule().await;
        assert_eq!(f.scheduler.get_jobs().count(), 0, "empty memberlist");
    }

    #[tokio::test]
    #[serial]
    async fn schedule_with_memberlist_produces_jobs_for_all_collections() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        f.scheduler.set_memberlist(vec![f.my_member.clone()]);
        f.scheduler.schedule().await;

        let mut jobs: Vec<&CompactionJob> = f.scheduler.get_jobs().collect();
        jobs.sort_by_key(|j| j.collection_id);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].collection_id, f.collection_uuid_1);
        assert_eq!(jobs[1].collection_id, f.collection_uuid_2);
    }

    #[tokio::test]
    #[serial]
    async fn disabled_collections_via_double_underscore_env_var() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        f.scheduler.set_memberlist(vec![f.my_member.clone()]);

        std::env::set_var(
            "CHROMA_COMPACTION_SERVICE__COMPACTOR__DISABLED_COLLECTIONS",
            format!("[\"{}\"]", f.collection_uuid_1.0),
        );

        f.scheduler.schedule().await;
        let jobs: Vec<&CompactionJob> = f.scheduler.get_jobs().collect();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].collection_id, f.collection_uuid_2);

        SchedulerFixture::clear_env_vars();
    }

    #[tokio::test]
    #[serial]
    async fn disabled_collections_via_dot_env_var() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        f.scheduler.set_memberlist(vec![f.my_member.clone()]);

        std::env::set_var(
            "CHROMA_COMPACTION_SERVICE.COMPACTOR.DISABLED_COLLECTIONS",
            format!("[\"{}\"]", f.collection_uuid_2.0),
        );
        // Irrelevant env var should not affect the result.
        std::env::set_var(
            "CHROMA_COMPACTION_SERVICE.IRRELEVANT",
            format!("[\"{}\"]", f.collection_uuid_1.0),
        );

        f.scheduler.schedule().await;
        let jobs: Vec<&CompactionJob> = f.scheduler.get_jobs().collect();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].collection_id, f.collection_uuid_1);

        SchedulerFixture::clear_env_vars();
    }

    #[tokio::test]
    #[serial]
    async fn filter_collections_with_multiple_members() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        let member_2 = Member {
            member_id: "member_2".to_string(),
            member_ip: "10.0.0.2".to_string(),
            member_node_name: "node_2".to_string(),
        };

        f.scheduler
            .set_memberlist(vec![f.my_member.clone(), member_2]);
        f.scheduler.schedule().await;

        // With two members, rendezvous hashing assigns a subset to this node.
        assert_eq!(f.scheduler.get_jobs().count(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn collections_exceeding_failure_count_are_skipped() {
        SchedulerFixture::clear_env_vars();
        let max_failure_count = 3;
        let mut f = SchedulerFixture::with_max_failure_count(max_failure_count);

        f.scheduler.set_memberlist(vec![f.my_member.clone()]);

        for _ in 0..max_failure_count {
            f.scheduler.schedule().await;
            let jobs: Vec<&CompactionJob> = f.scheduler.get_jobs().collect();
            assert_eq!(
                jobs.len(),
                2,
                "both collections scheduled before reaching max failures"
            );
            f.scheduler.fail_job(f.collection_uuid_1.into()).await;
            f.scheduler.succeed_job(f.collection_uuid_2.into());
        }

        f.scheduler.schedule().await;
        let jobs: Vec<&CompactionJob> = f.scheduler.get_jobs().collect();
        assert_eq!(
            jobs.len(),
            1,
            "collection_1 should be excluded after max failures"
        );
        assert_eq!(jobs[0].collection_id, f.collection_uuid_2);
    }

    #[tokio::test]
    #[serial]
    async fn released_jobs_never_dead_letter_the_collection() {
        SchedulerFixture::clear_env_vars();
        let max_failure_count = 3;
        let mut f = SchedulerFixture::with_max_failure_count(max_failure_count);

        f.scheduler.set_memberlist(vec![f.my_member.clone()]);

        // Well past the dead-letter threshold. A node-local fault must never
        // exhaust a collection's retry budget, however often it recurs.
        for _ in 0..(max_failure_count * 3) {
            f.scheduler.schedule().await;
            let jobs: Vec<&CompactionJob> = f.scheduler.get_jobs().collect();
            assert_eq!(jobs.len(), 2, "both collections stay schedulable");
            f.scheduler
                .release_job_without_penalty(f.collection_uuid_1.into());
            f.scheduler.succeed_job(f.collection_uuid_2.into());
        }

        f.scheduler.schedule().await;
        let jobs: Vec<&CompactionJob> = f.scheduler.get_jobs().collect();
        assert_eq!(
            jobs.len(),
            2,
            "collection_1 must still be scheduled after repeated releases"
        );
    }

    #[tokio::test]
    #[serial]
    async fn enriched_offset_advances_past_sysdb_log_position() {
        SchedulerFixture::clear_env_vars();

        // Construct a scheduler with a single collection whose sysdb log_position
        // is ahead of the log service's first_log_offset.  The enriched
        // CollectionRecord.offset must be log_position + 1 (the next un-compacted
        // offset), NOT first_log_offset (which would rewind compaction).
        let mut log = Log::InMemory(InMemoryLog::new());
        let in_memory_log = match log {
            Log::InMemory(ref mut in_memory_log) => in_memory_log,
            _ => panic!("Invalid log type"),
        };

        let tenant = "tenant_1".to_string();
        let sysdb_log_position: i64 = 10;
        let collection = Collection {
            collection_id: CollectionUuid::from_str("00000000-0000-0000-0000-000000000001")
                .unwrap(),
            name: "collection_1".to_string(),
            dimension: Some(1),
            tenant: tenant.clone(),
            database: "database_1".to_string(),
            log_position: sysdb_log_position,
            ..Default::default()
        };
        let collection_id = collection.collection_id;

        in_memory_log.add_log(
            collection_id,
            InternalLogRecord {
                collection_id,
                log_offset: 0,
                log_ts: 1,
                record: LogRecord {
                    log_offset: 0,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: None,
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                },
            },
        );

        let mut sysdb = SysDb::Test(TestSysDb::new());
        match sysdb {
            SysDb::Test(ref mut test_sysdb) => {
                test_sysdb.add_collection(collection);
                test_sysdb.add_tenant_last_compaction_time(tenant, 1);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let my_member = Member {
            member_id: "member_1".to_string(),
            member_ip: "10.0.0.1".to_string(),
            member_node_name: "node_1".to_string(),
        };
        let mut assignment_policy = Box::new(RendezvousHashingAssignmentPolicy::default());
        assignment_policy.set_members(vec![my_member.member_id.clone()]);

        let mut scheduler = Scheduler::new(
            my_member.member_id.clone(),
            log,
            sysdb.clone(),
            Box::new(LasCompactionTimeSchedulerPolicy {}),
            1000,
            1,
            assignment_policy,
            HashSet::new(),
            3600,
            3,
        );

        // The log service reports first_log_offset = 0, but sysdb says
        // log_position = 10 (offsets 0..=10 already compacted).
        let first_log_offset: i64 = 0;
        let collection_infos = vec![CollectionInfo {
            collection_id,
            topology_name: None,
            first_log_offset,
            first_log_ts: 1,
        }];

        let records = scheduler
            .verify_and_enrich_collections(collection_infos)
            .await;

        assert_eq!(records.len(), 1, "should produce exactly one record");
        let expected_offset = sysdb_log_position + 1;
        assert_eq!(
            records[0].offset, expected_offset,
            "offset must be log_position + 1 ({expected_offset}), not first_log_offset ({first_log_offset}); \
             using first_log_offset would rewind compaction and reprocess already-compacted records"
        );
    }

    #[tokio::test]
    #[serial]
    async fn oneoff_collection_with_negative_log_position_not_dropped() {
        SchedulerFixture::clear_env_vars();

        // Set up a collection whose log_position is -1 (never compacted).
        let mut log = Log::InMemory(InMemoryLog::new());
        let in_memory_log = match log {
            Log::InMemory(ref mut in_memory_log) => in_memory_log,
            _ => panic!("Invalid log type"),
        };

        let tenant = "tenant_1".to_string();
        let sysdb_log_position: i64 = -1;
        let collection = Collection {
            collection_id: CollectionUuid::from_str("00000000-0000-0000-0000-000000000099")
                .unwrap(),
            name: "oneoff_collection".to_string(),
            dimension: Some(1),
            tenant: tenant.clone(),
            database: "database_1".to_string(),
            log_position: sysdb_log_position,
            ..Default::default()
        };
        let collection_id = collection.collection_id;

        in_memory_log.add_log(
            collection_id,
            InternalLogRecord {
                collection_id,
                log_offset: 0,
                log_ts: 1,
                record: LogRecord {
                    log_offset: 0,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: None,
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                },
            },
        );

        let mut sysdb = SysDb::Test(TestSysDb::new());
        match sysdb {
            SysDb::Test(ref mut test_sysdb) => {
                test_sysdb.add_collection(collection);
                test_sysdb.add_tenant_last_compaction_time(tenant, 1);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let my_member = Member {
            member_id: "member_1".to_string(),
            member_ip: "10.0.0.1".to_string(),
            member_node_name: "node_1".to_string(),
        };
        let mut assignment_policy = Box::new(RendezvousHashingAssignmentPolicy::default());
        assignment_policy.set_members(vec![my_member.member_id.clone()]);

        let mut scheduler = Scheduler::new(
            my_member.member_id.clone(),
            log,
            sysdb.clone(),
            Box::new(LasCompactionTimeSchedulerPolicy {}),
            1000,
            1,
            assignment_policy,
            HashSet::new(),
            3600,
            3,
        );

        // Simulate a one-off collection entry with the values that
        // get_collections_with_new_data produces (first_log_offset=0).
        let collection_infos = vec![CollectionInfo {
            collection_id,
            topology_name: None,
            first_log_offset: 0,
            first_log_ts: 0,
        }];

        let records = scheduler
            .verify_and_enrich_collections(collection_infos)
            .await;

        // The collection must not be dropped by the invariant check.
        // log_position + 1 = -1 + 1 = 0, and first_log_offset = 0,
        // so the condition (0 < 0) is false and the collection is kept.
        assert_eq!(
            records.len(),
            1,
            "one-off collection with log_position=-1 must not be dropped; \
             first_log_offset=0 avoids false positive in the invariant check"
        );
    }

    #[tokio::test]
    #[serial]
    async fn oneoff_collection_does_not_overwrite_log_metadata() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        f.scheduler
            .add_oneoff_collections(vec![f.collection_uuid_1])
            .await;

        let collections = f.scheduler.get_collections_with_new_data().await;
        let matching: Vec<_> = collections
            .into_iter()
            .filter(|c| c.collection_id == f.collection_uuid_1)
            .collect();

        assert_eq!(
            matching.len(),
            1,
            "one-off collections should be filtered out when log-derived data already exists"
        );
        assert_eq!(
            matching[0].first_log_ts, 1,
            "log-derived metadata must be preserved instead of being reset by the one-off entry"
        );
    }

    #[tokio::test]
    async fn missing_sysdb_collections_marked_as_deleted() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        // Ask to enrich two collections, but remove collection_2 from sysdb
        // so it won't be returned.
        match f.scheduler.sysdb {
            SysDb::Test(ref mut test_sysdb) => {
                test_sysdb.remove_collection(f.collection_uuid_2);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let collection_infos = vec![
            CollectionInfo {
                collection_id: f.collection_uuid_1,
                topology_name: None,
                first_log_offset: 0,
                first_log_ts: 1,
            },
            CollectionInfo {
                collection_id: f.collection_uuid_2,
                topology_name: None,
                first_log_offset: 0,
                first_log_ts: 2,
            },
        ];

        let records = f
            .scheduler
            .verify_and_enrich_collections(collection_infos)
            .await;

        assert_eq!(records.len(), 1, "only collection_1 should be enriched");
        assert_eq!(records[0].collection_id, f.collection_uuid_1);

        let deleted = f.scheduler.drain_deleted_collections();
        assert_eq!(deleted.len(), 1, "collection_2 should be marked as deleted");
        assert_eq!(
            deleted[0].0, f.collection_uuid_2,
            "the deleted collection should be collection_2"
        );
    }

    #[tokio::test]
    async fn missing_sysdb_oneoff_collections_removed_from_oneoff_tracking() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        f.scheduler
            .add_oneoff_collections(vec![f.collection_uuid_2])
            .await;
        assert!(
            f.scheduler
                .oneoff_collections
                .contains_key(&f.collection_uuid_2),
            "one-off collection should be tracked before sysdb deletion"
        );

        match f.scheduler.sysdb {
            SysDb::Test(ref mut test_sysdb) => {
                test_sysdb.remove_collection(f.collection_uuid_2);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let records = f
            .scheduler
            .verify_and_enrich_collections(vec![CollectionInfo {
                collection_id: f.collection_uuid_2,
                topology_name: None,
                first_log_offset: 0,
                first_log_ts: 1,
            }])
            .await;

        assert!(
            records.is_empty(),
            "deleted one-off collection should not be enriched"
        );
        assert!(
            !f.scheduler
                .oneoff_collections
                .contains_key(&f.collection_uuid_2),
            "deleted one-off collection must be removed from oneoff_collections"
        );

        let deleted = f.scheduler.drain_deleted_collections();
        assert_eq!(deleted.len(), 1, "collection should be marked as deleted");
        assert_eq!(deleted[0].0, f.collection_uuid_2);
    }

    #[tokio::test]
    async fn test_k8s_integration_scheduler_invariant_violation() {
        let mut log = Log::InMemory(InMemoryLog::new());
        let in_memory_log = match log {
            Log::InMemory(ref mut in_memory_log) => in_memory_log,
            _ => panic!("Invalid log type"),
        };

        let tenant_1 = "tenant_1".to_string();
        let collection_1 = Collection {
            name: "collection_1".to_string(),
            dimension: Some(1),
            tenant: tenant_1.clone(),
            database: "database_1".to_string(),
            ..Default::default()
        };

        let collection_uuid_1 = collection_1.collection_id;

        in_memory_log.add_log(
            collection_uuid_1,
            InternalLogRecord {
                collection_id: collection_uuid_1,
                log_offset: 0,
                log_ts: 1,
                record: LogRecord {
                    log_offset: 0,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: None,
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                },
            },
        );
        in_memory_log.add_log(
            collection_uuid_1,
            InternalLogRecord {
                collection_id: collection_uuid_1,
                log_offset: 1,
                log_ts: 2,
                record: LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: None,
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                },
            },
        );
        in_memory_log.add_log(
            collection_uuid_1,
            InternalLogRecord {
                collection_id: collection_uuid_1,
                log_offset: 2,
                log_ts: 3,
                record: LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: None,
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                },
            },
        );
        in_memory_log.add_log(
            collection_uuid_1,
            InternalLogRecord {
                collection_id: collection_uuid_1,
                log_offset: 3,
                log_ts: 4,
                record: LogRecord {
                    log_offset: 3,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: None,
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                },
            },
        );
        let _ = log
            .update_collection_log_offset(
                &tenant_1,
                chroma_types::DatabaseName::new("test_db").unwrap(),
                collection_uuid_1,
                2,
            )
            .await;

        let mut sysdb = SysDb::Test(TestSysDb::new());

        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_collection(collection_1);
                let last_compaction_time_1 = 2;
                sysdb.add_tenant_last_compaction_time(tenant_1, last_compaction_time_1);
            }
            _ => panic!("Invalid sysdb type"),
        }
        let my_member = Member {
            member_id: "member_1".to_string(),
            member_ip: "0.0.0.1".to_string(),
            member_node_name: "node_1".to_string(),
        };
        let scheduler_policy = Box::new(LasCompactionTimeSchedulerPolicy {});
        let max_concurrent_jobs = 1000;
        let max_failure_count = 3;

        // Set assignment policy
        let mut assignment_policy = Box::new(RendezvousHashingAssignmentPolicy::default());
        assignment_policy.set_members(vec![my_member.member_id.clone()]);

        let mut scheduler = Scheduler::new(
            my_member.member_id.clone(),
            log,
            sysdb.clone(),
            scheduler_policy,
            max_concurrent_jobs,
            1,
            assignment_policy,
            HashSet::new(),
            3600,              // job_expiry_seconds
            max_failure_count, // max_failure_count
        );

        scheduler.set_memberlist(vec![my_member.clone()]);
        scheduler.schedule().await;
        let jobs = scheduler.get_jobs();
        let jobs = jobs.collect::<Vec<&CompactionJob>>();
        assert!(
            jobs.is_empty(),
            "Expected no jobs when log offset precedes sysdb position, but got {} jobs",
            jobs.len()
        );
    }

    #[tokio::test]
    #[serial]
    async fn in_progress_collections_are_skipped_by_filter() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();
        f.scheduler.set_memberlist(vec![f.my_member.clone()]);

        f.scheduler.schedule().await;
        assert_eq!(f.scheduler.get_jobs().count(), 2);

        f.scheduler.schedule().await;
        assert_eq!(
            f.scheduler.get_jobs().count(),
            0,
            "in-progress collections should be filtered out"
        );
    }

    #[tokio::test]
    #[serial]
    async fn only_in_progress_collection_is_skipped() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();
        f.scheduler.set_memberlist(vec![f.my_member.clone()]);

        f.scheduler.schedule().await;
        assert_eq!(f.scheduler.get_jobs().count(), 2);

        f.scheduler.succeed_job(f.collection_uuid_1.into());

        f.scheduler.schedule().await;
        let jobs: Vec<&CompactionJob> = f.scheduler.get_jobs().collect();
        assert_eq!(jobs.len(), 1);
        assert_eq!(
            jobs[0].collection_id, f.collection_uuid_1,
            "only the completed collection should be re-scheduled"
        );
    }

    #[tokio::test]
    #[serial]
    async fn filter_collections_removes_disabled_collections() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();
        f.scheduler.set_memberlist(vec![f.my_member.clone()]);

        f.scheduler.disabled_collections.insert(f.collection_uuid_1);

        let input = vec![
            CollectionInfo {
                collection_id: f.collection_uuid_1,
                topology_name: None,
                first_log_offset: 0,
                first_log_ts: 1,
            },
            CollectionInfo {
                collection_id: f.collection_uuid_2,
                topology_name: None,
                first_log_offset: 0,
                first_log_ts: 2,
            },
        ];

        let filtered = f.scheduler.filter_collections(input).await;
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].collection_id, f.collection_uuid_2);
    }

    #[tokio::test]
    #[serial]
    async fn disabled_and_in_progress_both_filtered() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();
        f.scheduler.set_memberlist(vec![f.my_member.clone()]);

        f.scheduler.schedule().await;
        assert_eq!(f.scheduler.get_jobs().count(), 2);

        // Complete collection_2, leave collection_1 in-progress.
        f.scheduler.succeed_job(f.collection_uuid_2.into());
        // Disable collection_2.
        f.scheduler.disabled_collections.insert(f.collection_uuid_2);

        f.scheduler.schedule().await;
        assert_eq!(
            f.scheduler.get_jobs().count(),
            0,
            "collection_1 is in-progress, collection_2 is disabled"
        );
    }

    #[tokio::test]
    #[serial]
    async fn sysdb_error_preserves_oneoff_ids_for_retry() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        // Enable error injection so add_oneoff_collections fails.
        match f.scheduler.sysdb {
            SysDb::Test(ref mut test_sysdb) => {
                test_sysdb.set_get_collections_error(true);
            }
            _ => panic!("Invalid sysdb type"),
        }

        f.scheduler
            .add_oneoff_collections(vec![f.collection_uuid_1])
            .await;

        // The collection must not have been resolved into oneoff_collections.
        assert!(
            f.scheduler.oneoff_collections.is_empty(),
            "sysdb error should prevent insertion into oneoff_collections"
        );
        // The ID must be retained in pending_oneoff_ids for retry.
        assert_eq!(
            f.scheduler.pending_oneoff_ids.len(),
            1,
            "failed IDs must be preserved in pending_oneoff_ids"
        );
        assert_eq!(f.scheduler.pending_oneoff_ids[0], f.collection_uuid_1);

        // Clear the error so the retry in schedule() succeeds.
        match f.scheduler.sysdb {
            SysDb::Test(ref mut test_sysdb) => {
                test_sysdb.set_get_collections_error(false);
            }
            _ => panic!("Invalid sysdb type"),
        }

        f.scheduler.set_memberlist(vec![f.my_member.clone()]);
        f.scheduler.schedule().await;

        // After schedule(), pending_oneoff_ids should have been drained and
        // the collection resolved into oneoff_collections (and then scheduled).
        assert!(
            f.scheduler.pending_oneoff_ids.is_empty(),
            "pending_oneoff_ids must be empty after successful retry"
        );

        // The one-off collection should have been scheduled as a job.
        let jobs: Vec<&CompactionJob> = f.scheduler.get_jobs().collect();
        let has_oneoff = jobs.iter().any(|j| j.collection_id == f.collection_uuid_1);
        assert!(
            has_oneoff,
            "one-off collection should appear in the job queue after retry; jobs: {:?}",
            jobs.iter().map(|j| j.collection_id).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    #[serial]
    async fn oneoff_collection_assigned_elsewhere_is_scheduled() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        // The memberlist does not contain this node, so every collection is
        // assigned to another member.
        let other_member = Member {
            member_id: "member_2".to_string(),
            member_ip: "10.0.0.2".to_string(),
            member_node_name: "node_2".to_string(),
        };
        f.scheduler.set_memberlist(vec![other_member]);

        f.scheduler
            .add_oneoff_collections(vec![f.collection_uuid_1])
            .await;
        f.scheduler.schedule().await;

        let jobs: Vec<&CompactionJob> = f.scheduler.get_jobs().collect();
        assert_eq!(
            jobs.len(),
            1,
            "one-off collection must run on the node that received the request \
             even when assigned to another member"
        );
        assert_eq!(jobs[0].collection_id, f.collection_uuid_1);
    }

    #[tokio::test]
    #[serial]
    async fn regular_collection_assigned_elsewhere_is_filtered() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        // The memberlist does not contain this node, so every collection is
        // assigned to another member.
        let other_member = Member {
            member_id: "member_2".to_string(),
            member_ip: "10.0.0.2".to_string(),
            member_node_name: "node_2".to_string(),
        };
        f.scheduler.set_memberlist(vec![other_member]);

        f.scheduler.schedule().await;

        assert_eq!(
            f.scheduler.get_jobs().count(),
            0,
            "regular collections assigned to another member must not be scheduled here"
        );
    }

    #[tokio::test]
    #[serial]
    async fn oneoff_collection_bypasses_failure_count_gate() {
        SchedulerFixture::clear_env_vars();
        let max_failure_count = 3;
        let mut f = SchedulerFixture::with_max_failure_count(max_failure_count);

        f.scheduler.set_memberlist(vec![f.my_member.clone()]);

        // Drive both collections to max_failure_count failures.
        for _ in 0..max_failure_count {
            f.scheduler.schedule().await;
            assert_eq!(f.scheduler.get_jobs().count(), 2);
            f.scheduler.fail_job(f.collection_uuid_1.into()).await;
            f.scheduler.fail_job(f.collection_uuid_2.into()).await;
        }

        f.scheduler
            .add_oneoff_collections(vec![f.collection_uuid_1])
            .await;
        f.scheduler.schedule().await;

        let jobs: Vec<&CompactionJob> = f.scheduler.get_jobs().collect();
        assert_eq!(
            jobs.len(),
            1,
            "the one-off collection must be scheduled despite exceeding \
             max_failure_count, while the regular collection is dropped"
        );
        assert_eq!(jobs[0].collection_id, f.collection_uuid_1);
    }

    #[tokio::test]
    #[serial]
    async fn oneoff_collection_still_respects_disabled_collections() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        f.scheduler.set_memberlist(vec![f.my_member.clone()]);
        f.scheduler.disabled_collections.insert(f.collection_uuid_1);

        f.scheduler
            .add_oneoff_collections(vec![f.collection_uuid_1])
            .await;
        f.scheduler.schedule().await;

        let jobs: Vec<&CompactionJob> = f.scheduler.get_jobs().collect();
        assert_eq!(
            jobs.len(),
            1,
            "only the non-disabled collection should be scheduled"
        );
        assert_eq!(
            jobs[0].collection_id, f.collection_uuid_2,
            "a one-off collection in disabled_collections must not be scheduled"
        );
    }

    // =========================================================================
    // Memory-Bounded Scheduling Integration Tests
    // =========================================================================

    /// Create a scheduler with memory bounding enabled via MemoryBoundedSchedulerPolicy.
    ///
    /// Collections 1-3 are registered in the test sysdb (database `test_db`) so
    /// that one-off compaction requests can be resolved.
    fn memory_bounded_fixture(
        max_concurrent_jobs: usize,
        max_total_size_bytes: u64,
    ) -> (Scheduler, CollectionUuid, CollectionUuid, CollectionUuid) {
        use crate::compactor::scheduler_policy::MemoryBoundedSchedulerPolicy;

        let log = Log::InMemory(InMemoryLog::new());

        let uuid_1 = CollectionUuid::from_str("00000000-0000-0000-0000-000000000001").unwrap();
        let uuid_2 = CollectionUuid::from_str("00000000-0000-0000-0000-000000000002").unwrap();
        let uuid_3 = CollectionUuid::from_str("00000000-0000-0000-0000-000000000003").unwrap();

        let mut sysdb = SysDb::Test(TestSysDb::new());
        match sysdb {
            SysDb::Test(ref mut test_sysdb) => {
                for (uuid, name) in [
                    (uuid_1, "collection_1"),
                    (uuid_2, "collection_2"),
                    (uuid_3, "collection_3"),
                ] {
                    test_sysdb.add_collection(Collection {
                        collection_id: uuid,
                        name: name.to_string(),
                        dimension: Some(1),
                        tenant: "test".to_string(),
                        database: "test_db".to_string(),
                        ..Default::default()
                    });
                }
            }
            _ => panic!("Invalid sysdb type"),
        }

        let my_member = Member {
            member_id: "member_1".to_string(),
            member_ip: "10.0.0.1".to_string(),
            member_node_name: "node_1".to_string(),
        };
        let mut assignment_policy = Box::new(RendezvousHashingAssignmentPolicy::default());
        assignment_policy.set_members(vec![my_member.member_id.clone()]);

        let scheduler = Scheduler::new(
            my_member.member_id.clone(),
            log,
            sysdb,
            Box::new(MemoryBoundedSchedulerPolicy::new(max_total_size_bytes)),
            max_concurrent_jobs,
            1,
            assignment_policy,
            HashSet::new(),
            3600,
            3,
        );

        (scheduler, uuid_1, uuid_2, uuid_3)
    }

    fn make_collection_record(id: CollectionUuid, size_bytes: u64) -> CollectionRecord {
        CollectionRecord {
            collection_id: id,
            tenant_id: "test".to_string(),
            database_name: "test_db".to_string(),
            last_compaction_time: 0,
            first_record_time: 0,
            offset: 0,
            collection_version: 0,
            collection_logical_size_bytes: size_bytes,
        }
    }

    #[tokio::test]
    #[serial]
    async fn schedule_internal_respects_memory_limit() {
        SchedulerFixture::clear_env_vars();
        let (mut scheduler, uuid_1, uuid_2, _) = memory_bounded_fixture(10, 500);

        // Two collections, each 400 bytes. Only one should fit within 500 byte limit.
        let records = vec![
            make_collection_record(uuid_1, 400),
            make_collection_record(uuid_2, 400),
        ];

        scheduler.schedule_internal(records).await;
        let jobs: Vec<_> = scheduler.get_jobs().collect();

        // Due to random shuffling in the memory policy, we can't predict which one
        // is selected, but only one should fit
        assert_eq!(
            jobs.len(),
            1,
            "Only one collection should fit within 500 byte limit"
        );
        assert_eq!(
            jobs[0].collection_size_bytes, 400,
            "Selected job should have correct size"
        );
    }

    #[tokio::test]
    #[serial]
    async fn schedule_internal_tracks_in_flight_size() {
        SchedulerFixture::clear_env_vars();
        let (mut scheduler, uuid_1, uuid_2, _) = memory_bounded_fixture(10, 1000);

        // First batch: one 600 byte collection
        let records = vec![make_collection_record(uuid_1, 600)];
        scheduler.schedule_internal(records).await;

        assert_eq!(scheduler.get_jobs().count(), 1);
        assert_eq!(
            scheduler.current_in_flight_size_bytes(),
            600,
            "In-flight size should be tracked"
        );

        // Second batch: another 600 byte collection shouldn't fit
        // (600 + 600 = 1200 > 1000)
        let records = vec![make_collection_record(uuid_2, 600)];
        scheduler.schedule_internal(records).await;

        // The new job shouldn't be added because it would exceed the limit
        // Note: schedule_internal clears the job queue, so we check in-flight jobs
        let in_progress = scheduler.get_in_progress_jobs();
        assert_eq!(
            in_progress.len(),
            1,
            "Second collection should not fit within remaining budget"
        );
    }

    #[tokio::test]
    #[serial]
    async fn schedule_internal_frees_size_on_job_completion() {
        SchedulerFixture::clear_env_vars();
        let (mut scheduler, uuid_1, uuid_2, _) = memory_bounded_fixture(10, 1000);

        // Schedule a 600 byte collection
        let records = vec![make_collection_record(uuid_1, 600)];
        scheduler.schedule_internal(records).await;

        assert_eq!(scheduler.current_in_flight_size_bytes(), 600);

        // Complete the job
        scheduler.succeed_job(uuid_1.into());

        assert_eq!(
            scheduler.current_in_flight_size_bytes(),
            0,
            "In-flight size should be 0 after job completion"
        );

        // Now a 600 byte collection should fit again
        let records = vec![make_collection_record(uuid_2, 600)];
        scheduler.schedule_internal(records).await;

        assert_eq!(scheduler.get_jobs().count(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn schedule_internal_concurrent_jobs_and_size_both_enforced() {
        SchedulerFixture::clear_env_vars();
        // Test that both max_concurrent_jobs and max_total_size are enforced
        let (mut scheduler, uuid_1, uuid_2, _) = memory_bounded_fixture(
            1,    // only 1 concurrent job
            2000, // but 2000 bytes allowed
        );

        // Two small collections that would fit size-wise
        let records = vec![
            make_collection_record(uuid_1, 100),
            make_collection_record(uuid_2, 100),
        ];

        scheduler.schedule_internal(records).await;
        let jobs: Vec<_> = scheduler.get_jobs().collect();

        // Should be limited by concurrent job count, not size
        assert_eq!(jobs.len(), 1, "Should respect max_concurrent_jobs limit");
    }

    #[tokio::test]
    #[serial]
    async fn schedule_internal_size_limit_stricter_than_job_limit() {
        SchedulerFixture::clear_env_vars();
        let (mut scheduler, uuid_1, uuid_2, uuid_3) = memory_bounded_fixture(
            10,  // up to 10 concurrent jobs
            250, // but only 250 bytes
        );

        // Three collections at 100 bytes each (300 total, exceeds 250)
        let records = vec![
            make_collection_record(uuid_1, 100),
            make_collection_record(uuid_2, 100),
            make_collection_record(uuid_3, 100),
        ];

        scheduler.schedule_internal(records).await;
        let jobs: Vec<_> = scheduler.get_jobs().collect();

        // Should be limited by size (at most 2 fit within 250 bytes)
        let total_size: u64 = jobs.iter().map(|j| j.collection_size_bytes).sum();
        assert!(
            total_size <= 250,
            "Total size {} should not exceed 250",
            total_size
        );
        assert!(jobs.len() <= 2, "At most 2 collections should fit");
    }

    #[tokio::test]
    #[serial]
    async fn in_progress_job_tracks_collection_size() {
        SchedulerFixture::clear_env_vars();
        let (mut scheduler, uuid_1, _, _) = memory_bounded_fixture(10, 1000);

        let records = vec![make_collection_record(uuid_1, 500)];
        scheduler.schedule_internal(records).await;

        let in_progress = scheduler.get_in_progress_jobs();
        assert_eq!(in_progress.len(), 1);

        let (_, job) = &in_progress[0];
        assert_eq!(
            job.collection_size_bytes, 500,
            "InProgressJob should track collection size"
        );
    }

    #[tokio::test]
    #[serial]
    async fn fail_job_frees_size() {
        SchedulerFixture::clear_env_vars();
        let (mut scheduler, uuid_1, uuid_2, _) = memory_bounded_fixture(10, 1000);

        // Schedule a 600 byte collection
        let records = vec![make_collection_record(uuid_1, 600)];
        scheduler.schedule_internal(records).await;

        assert_eq!(scheduler.current_in_flight_size_bytes(), 600);

        // Fail the job
        scheduler.fail_job(uuid_1.into()).await;

        assert_eq!(
            scheduler.current_in_flight_size_bytes(),
            0,
            "In-flight size should be 0 after job failure"
        );

        // Now a 600 byte collection should fit again
        let records = vec![make_collection_record(uuid_2, 600)];
        scheduler.schedule_internal(records).await;

        assert_eq!(scheduler.get_jobs().count(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn memory_bounded_policy_allows_one_large_job_when_empty() {
        SchedulerFixture::clear_env_vars();
        let (mut scheduler, uuid_1, _, _) = memory_bounded_fixture(10, 100);

        // A collection larger than the limit should still be scheduled
        // to prevent starvation when nothing is in flight
        let records = vec![make_collection_record(uuid_1, 500)];
        scheduler.schedule_internal(records).await;

        let jobs: Vec<_> = scheduler.get_jobs().collect();
        assert_eq!(
            jobs.len(),
            1,
            "Should allow at least one job even if it exceeds the limit"
        );
    }

    // =========================================================================
    // One-off compaction tests with memory-bounded policy
    // =========================================================================

    #[tokio::test]
    #[serial]
    async fn oneoff_compaction_bypasses_memory_limit() {
        SchedulerFixture::clear_env_vars();
        // One-off compactions are admin-initiated and should bypass memory limits
        let (mut scheduler, uuid_1, _, _) = memory_bounded_fixture(10, 100);

        // Add a one-off collection that exceeds the memory limit
        scheduler.add_oneoff_collections(vec![uuid_1]).await;

        // Schedule with a large collection
        let records = vec![make_collection_record(uuid_1, 500)]; // 500 > 100 limit
        scheduler.schedule_internal(records).await;

        let jobs: Vec<_> = scheduler.get_jobs().collect();
        assert_eq!(
            jobs.len(),
            1,
            "One-off compaction should bypass memory limit"
        );
        assert_eq!(jobs[0].collection_id, uuid_1);
    }

    #[tokio::test]
    #[serial]
    async fn oneoff_compaction_tracks_in_flight_size() {
        SchedulerFixture::clear_env_vars();
        // One-off jobs should still be tracked in in_flight_size
        let (mut scheduler, uuid_1, _, _) = memory_bounded_fixture(10, 100);

        scheduler.add_oneoff_collections(vec![uuid_1]).await;

        let records = vec![make_collection_record(uuid_1, 500)];
        scheduler.schedule_internal(records).await;

        // Should track the size even though it bypassed the limit
        assert_eq!(
            scheduler.current_in_flight_size_bytes(),
            500,
            "One-off job should be tracked in in_flight_size"
        );
    }

    #[tokio::test]
    #[serial]
    async fn scheduling_after_oneoff_respects_in_flight_size() {
        SchedulerFixture::clear_env_vars();
        // After a one-off job is scheduled, subsequent scheduling should
        // respect the current in-flight size
        let (mut scheduler, uuid_1, uuid_2, uuid_3) = memory_bounded_fixture(10, 1000);

        // First, schedule a one-off 800-byte job
        scheduler.add_oneoff_collections(vec![uuid_1]).await;
        let records = vec![make_collection_record(uuid_1, 800)];
        scheduler.schedule_internal(records).await;

        assert_eq!(scheduler.current_in_flight_size_bytes(), 800);

        // Now try to schedule more collections - only 200 bytes left
        // uuid_2 at 300 bytes should not fit, uuid_3 at 150 bytes should fit
        let records = vec![
            make_collection_record(uuid_2, 300),
            make_collection_record(uuid_3, 150),
        ];
        scheduler.schedule_internal(records).await;

        let jobs: Vec<_> = scheduler.get_jobs().collect();
        // Should only schedule the 150-byte collection
        assert_eq!(jobs.len(), 1, "Only smaller collection should fit");
        assert_eq!(jobs[0].collection_id, uuid_3);
    }

    #[tokio::test]
    #[serial]
    async fn oneoff_compaction_respects_max_concurrent_jobs() {
        SchedulerFixture::clear_env_vars();
        // One-off compactions should still respect max_concurrent_jobs
        let (mut scheduler, uuid_1, uuid_2, _) = memory_bounded_fixture(
            1,     // only 1 concurrent job allowed
            10000, // high memory limit
        );

        // Try to schedule two one-off compactions
        scheduler.add_oneoff_collections(vec![uuid_1, uuid_2]).await;

        let records = vec![
            make_collection_record(uuid_1, 100),
            make_collection_record(uuid_2, 100),
        ];
        scheduler.schedule_internal(records).await;

        let jobs: Vec<_> = scheduler.get_jobs().collect();
        assert_eq!(
            jobs.len(),
            1,
            "One-off compactions should respect max_concurrent_jobs"
        );
    }

    #[tokio::test]
    #[serial]
    async fn oneoff_completion_frees_size_for_regular_jobs() {
        SchedulerFixture::clear_env_vars();
        // After a one-off job completes, the freed size should allow regular jobs
        let (mut scheduler, uuid_1, uuid_2, _) = memory_bounded_fixture(10, 500);

        // Schedule a large one-off job that takes up most of the budget
        scheduler.add_oneoff_collections(vec![uuid_1]).await;
        let records = vec![make_collection_record(uuid_1, 400)];
        scheduler.schedule_internal(records).await;

        assert_eq!(scheduler.current_in_flight_size_bytes(), 400);

        // Complete the one-off job
        scheduler.succeed_job(uuid_1.into());
        assert_eq!(scheduler.current_in_flight_size_bytes(), 0);

        // Now a regular 400-byte job should fit
        let records = vec![make_collection_record(uuid_2, 400)];
        scheduler.schedule_internal(records).await;

        let jobs: Vec<_> = scheduler.get_jobs().collect();
        assert_eq!(
            jobs.len(),
            1,
            "Regular job should fit after one-off completes"
        );
    }
}
