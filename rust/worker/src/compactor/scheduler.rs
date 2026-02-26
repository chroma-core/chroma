use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use chroma_config::assignment::assignment_policy::AssignmentPolicy;
use chroma_log::{CollectionInfo, CollectionRecord, Log};
use chroma_memberlist::memberlist_provider::Memberlist;
use chroma_sysdb::{DatabaseOrTopology, GetCollectionsOptions, SysDb};
use chroma_types::{CollectionUuid, DatabaseName, JobId};
use figment::providers::Env;
use figment::Figment;
use opentelemetry::metrics::Counter;
use serde::Deserialize;
use uuid::Uuid;

use crate::compactor::scheduler_policy::SchedulerPolicy;
use crate::compactor::types::CompactionJob;

#[derive(Debug, Clone)]
pub(crate) struct SchedulerMetrics {
    job_failure_count: Counter<u64>,
}

impl Default for SchedulerMetrics {
    fn default() -> Self {
        let meter = opentelemetry::global::meter("chroma_compactor");
        let job_failure_count = meter
            .u64_counter("compactor_job_failure_count")
            .with_description("Number of compaction job failures")
            .build();

        Self { job_failure_count }
    }
}

impl SchedulerMetrics {
    fn increment_job_failure_count(&self) {
        self.job_failure_count.add(1, &[]);
    }
}

struct InProgressJob {
    expires_at: SystemTime,
    database_name: DatabaseName,
}

impl InProgressJob {
    fn new(job_expiry_seconds: u64, database_name: DatabaseName) -> Self {
        Self {
            expires_at: SystemTime::now() + Duration::from_secs(job_expiry_seconds),
            database_name,
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
    oneoff_collections: HashSet<CollectionUuid>,
    disabled_collections: HashSet<CollectionUuid>,
    deleted_collections: HashSet<CollectionUuid>,
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
            oneoff_collections: HashSet::new(),
            disabled_collections,
            deleted_collections: HashSet::new(),
            collections_needing_repair: HashMap::new(),
            in_progress_jobs: HashMap::new(),
            job_expiry_seconds,
            max_failure_count,
            metrics: SchedulerMetrics::default(),
        }
    }

    pub(crate) fn add_oneoff_collections(&mut self, ids: Vec<CollectionUuid>) {
        self.oneoff_collections.extend(ids);
    }

    pub(crate) fn get_oneoff_collections(&self) -> Vec<CollectionUuid> {
        self.oneoff_collections.iter().cloned().collect()
    }

    pub(crate) fn drain_deleted_collections(&mut self) -> Vec<CollectionUuid> {
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

        match collections {
            Ok(collections) => {
                tracing::info!("Collections with new data: {collections:?}");
                collections
            }
            Err(e) => {
                tracing::error!("Error: {:?}", e);
                Vec::new()
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
        for (topology, mut collection_infos) in by_topology {
            for collection_info in std::mem::take(&mut collection_infos).into_iter() {
                if self
                    .disabled_collections
                    .contains(&collection_info.collection_id)
                {
                    tracing::info!(
                        "Ignoring collection: {:?} because it is disabled for compaction",
                        collection_info.collection_id
                    );
                    continue;
                }
                collection_infos.push(collection_info);
            }
            let mut ids = Vec::with_capacity(collection_infos.len());
            ids.extend(collection_infos.iter().map(|c| c.collection_id));
            let result = self
                .sysdb
                .get_collections(GetCollectionsOptions {
                    collection_ids: Some(ids),
                    database_or_topology: topology.clone(),
                    limit: Some(collection_infos.len() as u32),
                    offset: 0,
                    include_soft_deleted: false,
                    collection_id: None,
                    name: None,
                    tenant: None,
                })
                .await;

            match result {
                Ok(collections) => {
                    if collections.len() != collection_infos.len() {
                        tracing::warn!(
                            "returned collection info does not match number of input collections"
                        );
                    }
                    let mut with_infos = vec![];
                    for collection in collections.into_iter() {
                        let Some(info) = collection_infos
                            .iter()
                            .find(|c| c.collection_id == collection.collection_id)
                        else {
                            self.deleted_collections.insert(collection.collection_id);
                            continue;
                        };
                        if collection.compaction_failure_count >= self.max_failure_count {
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
                                offset: info.first_log_offset,
                                collection_version: collection.version,
                                collection_logical_size_bytes: collection
                                    .size_bytes_post_compaction,
                            });
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("error fetching for topo = {topology:?}: {e}");
                }
            }
        }
        collection_records
    }

    fn filter_collections(&mut self, collections: Vec<CollectionInfo>) -> Vec<CollectionInfo> {
        let mut filtered_collections = Vec::new();
        let members = self.memberlist.as_ref().unwrap();
        let members_as_string = members
            .iter()
            .map(|member| member.member_id.clone())
            .collect();
        self.assignment_policy.set_members(members_as_string);

        for collection in collections {
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
        let mut scheduled_collections = Vec::new();
        for record in collection_records {
            tracing::info!("Processing collection: {}", record.collection_id);
            let database_name = match DatabaseName::new(record.database_name.clone()) {
                Some(db_name) => db_name,
                None => {
                    tracing::warn!(
                        "Invalid database name for collection {}: {}",
                        record.collection_id,
                        record.database_name
                    );
                    continue;
                }
            };

            if self.is_job_in_progress(&record.collection_id).await {
                tracing::info!(
                    "Compaction for {} is already in progress, skipping",
                    record.collection_id
                );
                continue;
            }
            if self.oneoff_collections.contains(&record.collection_id) {
                tracing::info!(
                    "Creating one-off compaction job for collection: {}",
                    record.collection_version
                );
                self.job_queue.push(CompactionJob {
                    collection_id: record.collection_id,
                    database_name,
                });
                self.oneoff_collections.remove(&record.collection_id);
                if self.job_queue.len() == self.max_concurrent_jobs {
                    return;
                }
            } else {
                if self.in_progress_jobs.len() >= self.max_concurrent_jobs {
                    tracing::info!(
                        "Max concurrent jobs reached, skipping compaction for {}",
                        record.collection_id
                    );
                    return;
                }
                scheduled_collections.push(record);
            }
        }

        self.job_queue.extend(
            self.policy
                .determine(scheduled_collections, self.max_concurrent_jobs as i32),
        );
        self.job_queue
            .truncate(self.max_concurrent_jobs - self.in_progress_jobs.len());

        // At this point, nobody should modify the job queue and every collection
        // in the job queue will definitely be compacted. It is now safe to add
        // them to the in-progress set.
        let job_ids: Vec<_> = self
            .job_queue
            .iter()
            .map(|j| (j.collection_id, j.database_name.clone()))
            .collect();
        for (collection_id, database_name) in job_ids {
            self.add_in_progress(collection_id, database_name);
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

    fn add_in_progress(&mut self, collection_id: CollectionUuid, database_name: DatabaseName) {
        self.in_progress_jobs.insert(
            collection_id.into(),
            InProgressJob::new(self.job_expiry_seconds, database_name),
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

        // Recompute disabled list.
        self.recompute_disabled_collections();
        let collections = self.get_collections_with_new_data().await;
        if collections.is_empty() {
            return;
        }
        let filtered_collections = self.filter_collections(collections);
        let collection_records = self
            .verify_and_enrich_collections(filtered_collections)
            .await;
        self.schedule_internal(collection_records).await;
    }

    pub(crate) fn get_jobs(&self) -> impl Iterator<Item = &CompactionJob> {
        self.job_queue.iter()
    }

    pub(crate) fn set_memberlist(&mut self, memberlist: Memberlist) {
        self.memberlist = Some(memberlist);
    }

    pub(crate) fn has_memberlist(&self) -> bool {
        self.memberlist.is_some()
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
        sysdb: SysDb,
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
                sysdb,
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
    async fn schedule_orders_by_last_compaction_time() {
        SchedulerFixture::clear_env_vars();
        let mut f = SchedulerFixture::new();

        // tenant_1 already has last_compaction_time = 2 from the fixture.
        // Add tenant_2 with last_compaction_time = 1 (older).
        match f.sysdb {
            SysDb::Test(ref mut test_sysdb) => {
                test_sysdb.add_tenant_last_compaction_time("tenant_2".to_string(), 1);
            }
            _ => panic!("Invalid sysdb type"),
        }

        f.scheduler.set_memberlist(vec![f.my_member.clone()]);
        f.scheduler.schedule().await;

        let mut jobs: Vec<&CompactionJob> = f.scheduler.get_jobs().collect();
        jobs.sort_by_key(|j| j.collection_id);
        assert_eq!(jobs.len(), 2);
        // Both should appear; ordering is tested in scheduler_policy tests.
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
}
