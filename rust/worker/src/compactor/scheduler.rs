use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use chroma_config::assignment::assignment_policy::AssignmentPolicy;
use chroma_log::{CollectionInfo, CollectionRecord, Log};
use chroma_memberlist::memberlist_provider::Memberlist;
use chroma_sysdb::{GetCollectionsOptions, SysDb};
use chroma_types::CollectionUuid;
use figment::providers::Env;
use figment::Figment;
use serde::Deserialize;
use uuid::Uuid;

use crate::compactor::scheduler_policy::SchedulerPolicy;
use crate::compactor::types::CompactionJob;

#[derive(Debug, Clone)]
pub(crate) struct SchedulerMetrics {
    dead_jobs_count: opentelemetry::metrics::Gauge<u64>,
}

impl Default for SchedulerMetrics {
    fn default() -> Self {
        let meter = opentelemetry::global::meter("chroma_compactor");
        let dead_jobs_count = meter
            .u64_gauge("compactor_dead_jobs_count")
            .with_description("Number of collections with failed jobs")
            .build();

        Self { dead_jobs_count }
    }
}

impl SchedulerMetrics {
    fn update_dead_jobs_count(&self, count: usize) {
        // Create a callback that will be called when metrics are collected
        self.dead_jobs_count.record(count.try_into().unwrap(), &[]);
    }
}

#[derive(Debug)]
struct FailedJob {
    failure_count: u8,
}

impl FailedJob {
    fn new() -> Self {
        Self { failure_count: 1 }
    }

    fn increment_failure(&mut self, max_failure_count: u8) {
        if self.failure_count >= max_failure_count {
            return;
        }
        self.failure_count += 1;
    }

    fn failure_count(&self) -> u8 {
        self.failure_count
    }
}

struct InProgressJob {
    expires_at: SystemTime,
}

impl InProgressJob {
    fn new(job_expiry_seconds: u64) -> Self {
        Self {
            expires_at: SystemTime::now() + Duration::from_secs(job_expiry_seconds),
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
    collections_needing_repair: HashMap<CollectionUuid, i64>,
    in_progress_jobs: HashMap<CollectionUuid, InProgressJob>,
    job_expiry_seconds: u64,
    failing_jobs: HashMap<CollectionUuid, FailedJob>,
    dead_jobs: HashSet<CollectionUuid>,
    max_failure_count: u8,
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
        max_failure_count: u8,
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
            failing_jobs: HashMap::new(),
            max_failure_count,
            dead_jobs: HashSet::new(),
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

    pub(crate) fn drain_collections_requiring_repair(&mut self) -> Vec<(CollectionUuid, i64)> {
        self.collections_needing_repair.drain().collect()
    }

    pub(crate) fn require_repair(&mut self, collection_id: CollectionUuid, offset_in_sysdb: i64) {
        self.collections_needing_repair
            .insert(collection_id, offset_in_sysdb);
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
        let mut collection_records = Vec::new();
        for collection_info in collections {
            if self
                .disabled_collections
                .contains(&collection_info.collection_id)
                || self.dead_jobs.contains(&collection_info.collection_id)
            {
                tracing::info!(
                    "Ignoring collection: {:?} because it disabled for compaction",
                    collection_info.collection_id
                );
                continue;
            }
            // TODO: add a cache to avoid fetching the same collection multiple times
            let result = self
                .sysdb
                .get_collections(GetCollectionsOptions {
                    collection_id: Some(collection_info.collection_id),
                    ..Default::default()
                })
                .await;

            match result {
                Ok(collection) => {
                    if collection.is_empty() {
                        self.deleted_collections
                            .insert(collection_info.collection_id);
                        continue;
                    }

                    // TODO: make querying the last compaction time in batch
                    let log_position_in_collection = collection[0].log_position;
                    let tenant_ids = vec![collection[0].tenant.clone()];
                    let tenant = self.sysdb.get_last_compaction_time(tenant_ids).await;

                    let last_compaction_time = match tenant {
                        Ok(tenant) => {
                            if tenant.is_empty() {
                                tracing::info!(
                                    "Ignoring collection: {:?}",
                                    collection_info.collection_id
                                );
                                continue;
                            }
                            tenant[0].last_compaction_time
                        }
                        Err(e) => {
                            tracing::error!("Error: {:?}", e);
                            // Ignore this collection id for this compaction iteration
                            tracing::info!(
                                "Ignoring collection: {:?}",
                                collection_info.collection_id
                            );
                            continue;
                        }
                    };

                    let mut offset = collection_info.first_log_offset;
                    // offset in log is the first offset in the log that has not been compacted. Note that
                    // since the offset is the first offset of log we get from the log service, we should
                    // use this offset to pull data from the log service.
                    if log_position_in_collection + 1 < offset {
                        panic!(
                            "offset in sysdb ({}) is less than offset in log ({}) for {}",
                            log_position_in_collection + 1,
                            offset,
                            collection[0].collection_id,
                        )
                    } else {
                        // The offset in sysdb is the last offset that has been compacted.
                        // We need to start from the next offset.
                        offset = log_position_in_collection + 1;
                    }

                    collection_records.push(CollectionRecord {
                        collection_id: collection[0].collection_id,
                        tenant_id: collection[0].tenant.clone(),
                        last_compaction_time,
                        first_record_time: collection_info.first_log_ts,
                        offset,
                        collection_version: collection[0].version,
                        collection_logical_size_bytes: collection[0].size_bytes_post_compaction,
                    });
                }
                Err(e) => {
                    tracing::error!("Error: {:?}", e);
                }
            }
        }
        collection_records
    }

    fn filter_collections(&mut self, collections: Vec<CollectionRecord>) -> Vec<CollectionRecord> {
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
            if self.is_job_in_progress(&record.collection_id) {
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

        let filtered_collections = self.filter_collections(scheduled_collections);
        self.job_queue.extend(
            self.policy
                .determine(filtered_collections, self.max_concurrent_jobs as i32),
        );
        self.job_queue
            .truncate(self.max_concurrent_jobs - self.in_progress_jobs.len());

        // At this point, nobody should modify the job queue and every collection
        // in the job queue will definitely be compacted. It is now safe to add
        // them to the in-progress set.
        let job_ids: Vec<_> = self.job_queue.iter().map(|j| j.collection_id).collect();
        for collection_id in job_ids {
            self.add_in_progress(collection_id);
        }
    }

    fn is_job_in_progress(&mut self, collection_id: &CollectionUuid) -> bool {
        match self.in_progress_jobs.get(collection_id) {
            Some(job) if job.is_expired() => {
                tracing::info!(
                    "Compaction for {} is expired, removing from dedup set.",
                    collection_id
                );
                self.fail_collection(*collection_id);
                false
            }
            Some(_) => true,
            None => false,
        }
    }

    fn add_in_progress(&mut self, collection_id: CollectionUuid) {
        self.in_progress_jobs
            .insert(collection_id, InProgressJob::new(self.job_expiry_seconds));
    }

    pub(crate) fn succeed_collection(&mut self, collection_id: CollectionUuid) {
        if self.in_progress_jobs.remove(&collection_id).is_none() {
            tracing::warn!(
                "Expired compaction for {} just successfully finished.",
                collection_id
            );
            return;
        }
        self.failing_jobs.remove(&collection_id);
    }

    pub(crate) fn fail_collection(&mut self, collection_id: CollectionUuid) {
        if self.in_progress_jobs.remove(&collection_id).is_none() {
            tracing::warn!(
                "Expired compaction for {} just unsuccessfully finished.",
                collection_id
            );
            return;
        }
        match self.failing_jobs.get_mut(&collection_id) {
            Some(failed_job) => {
                failed_job.increment_failure(self.max_failure_count);
                tracing::warn!(
                    "Job for collection {} failed {}/{} times",
                    collection_id,
                    failed_job.failure_count(),
                    self.max_failure_count
                );

                if failed_job.failure_count() >= self.max_failure_count {
                    tracing::warn!(
                        "Job for collection {} failed {} times, moving this to dead jobs",
                        collection_id,
                        failed_job.failure_count()
                    );
                    self.kill_collection(collection_id);
                }
            }
            None => {
                self.failing_jobs.insert(collection_id, FailedJob::new());
                tracing::warn!(
                    "Job for collection {} failed for the first time",
                    collection_id
                );
            }
        }
    }

    pub(crate) fn kill_collection(&mut self, collection_id: CollectionUuid) {
        self.failing_jobs.remove(&collection_id);
        self.dead_jobs.insert(collection_id);
        self.metrics.update_dead_jobs_count(self.dead_jobs.len());
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
        let mut collections = self.get_collections_with_new_data().await;
        if collections.is_empty() {
            return;
        }
        let blessed = vec![
            CollectionUuid::from_str("665e9b5e-87d3-4431-8b87-60b5a299b85c").unwrap(),
            CollectionUuid::from_str("6a661627-1072-4cc8-8fd4-362613fc5eca").unwrap(),
            CollectionUuid::from_str("6c01554d-21b2-4ed2-848a-15bbd5ca19f0").unwrap(),
            CollectionUuid::from_str("6c211f7b-1c64-4813-a255-ccd740b50a47").unwrap(),
            CollectionUuid::from_str("70918b85-5d2b-4992-ae1c-7adac0a37bfc").unwrap(),
            CollectionUuid::from_str("713d3aec-19b7-418a-9c83-ff5aaa67aa52").unwrap(),
            CollectionUuid::from_str("716f1d0f-024a-48d5-af3c-709dba0e7236").unwrap(),
            CollectionUuid::from_str("76b28e00-f484-4a26-b96e-277e97197279").unwrap(),
            CollectionUuid::from_str("79316392-c98d-4753-90c5-9a862211a86a").unwrap(),
            CollectionUuid::from_str("7ce00fe7-e372-4ff9-89ba-907916033ed3").unwrap(),
            CollectionUuid::from_str("7d54d717-a84e-4119-9e3b-1ad92796e0bb").unwrap(),
            CollectionUuid::from_str("80514844-63d1-4ff6-8f75-75b639c7fbef").unwrap(),
            CollectionUuid::from_str("83ef30ea-ebf6-49e4-a2b5-bd13a4e7a22b").unwrap(),
            CollectionUuid::from_str("8502c642-13b1-4226-8fc7-ab3a04e8cb28").unwrap(),
            CollectionUuid::from_str("89b3d033-6210-46d8-922b-3fcdb92d404e").unwrap(),
            CollectionUuid::from_str("89e133cf-9adf-47c4-b5d6-15ef9940ef01").unwrap(),
            CollectionUuid::from_str("8bea788c-b714-490b-85ff-f57dab95d7bb").unwrap(),
            CollectionUuid::from_str("8bebfa1d-643e-467f-a276-3bc6c38bbf0a").unwrap(),
            CollectionUuid::from_str("8e889076-4674-4d99-9087-3d21bbc2ae3a").unwrap(),
            CollectionUuid::from_str("8efe6407-4b12-4661-ae3c-b91963cb5275").unwrap(),
            CollectionUuid::from_str("913119f5-f33c-421e-8df6-bed1720bab15").unwrap(),
            CollectionUuid::from_str("9413e5b6-6066-419b-a890-7ebc1024c762").unwrap(),
            CollectionUuid::from_str("9564123b-d4aa-46af-aef8-6cbf7d331974").unwrap(),
            CollectionUuid::from_str("95c570e4-86d8-4d16-81cd-2e8ee3f21c74").unwrap(),
            CollectionUuid::from_str("9644bfda-d98e-41b9-af11-92cbb4bfc463").unwrap(),
            CollectionUuid::from_str("96a15c56-75ad-4b02-a77c-b329134f59b3").unwrap(),
            CollectionUuid::from_str("977cc4ec-5d74-4775-9351-8a81160cdb48").unwrap(),
            CollectionUuid::from_str("98690bb6-4c1d-4014-a3b4-b8b1091fe00f").unwrap(),
            CollectionUuid::from_str("99dcaf59-4a10-4089-86cf-d7400ec16069").unwrap(),
            CollectionUuid::from_str("9bac9475-2453-438c-b8f5-b7d00265cb43").unwrap(),
            CollectionUuid::from_str("9bb76607-12e5-43ea-a385-b046f86a38bb").unwrap(),
            CollectionUuid::from_str("9e7d71bb-ac9c-4012-b491-6dfbf10cb7a6").unwrap(),
            CollectionUuid::from_str("9f24aeaf-f49e-475f-8e78-a78009084c8a").unwrap(),
            CollectionUuid::from_str("a549e02f-b364-4ada-88df-e10bedeb14f8").unwrap(),
            CollectionUuid::from_str("a7cbbcb3-8ef6-4d69-8683-5a5242e1a39f").unwrap(),
            CollectionUuid::from_str("aa20920c-2e92-4154-989d-ccfe5bdf905e").unwrap(),
            CollectionUuid::from_str("ab20d2b9-d5f1-4161-b6ba-4c2303f18614").unwrap(),
            CollectionUuid::from_str("ac0b688f-9ea2-4efb-aa21-c409af0b2847").unwrap(),
            CollectionUuid::from_str("ad44418e-cd1b-475a-8084-bc62243e70d9").unwrap(),
            CollectionUuid::from_str("ae4e758e-df66-447d-ac8c-65341f8315c3").unwrap(),
            CollectionUuid::from_str("b028b9ee-948e-4297-b091-a4732cfd09da").unwrap(),
            CollectionUuid::from_str("b78a0633-1a50-4e4d-9689-2d065ef22c0b").unwrap(),
            CollectionUuid::from_str("b7f15094-7f51-476b-835d-7da55596e98f").unwrap(),
            CollectionUuid::from_str("b819d183-09a3-4555-a2b6-652e4e72138a").unwrap(),
            CollectionUuid::from_str("bb317f42-95a1-460b-ba99-c1b0e042f51b").unwrap(),
            CollectionUuid::from_str("bcd6f7be-2c5c-4da0-a1d3-fa206aea3e4c").unwrap(),
            CollectionUuid::from_str("bd643ec9-8744-40b9-8604-0d330cca7bc8").unwrap(),
            CollectionUuid::from_str("c1c65b63-4249-49d7-99c6-b27fd3bfa1d2").unwrap(),
            CollectionUuid::from_str("c1d70058-8c4b-49df-8b49-e3e47557d162").unwrap(),
            CollectionUuid::from_str("c64d7157-7a43-4386-802d-e3d091f0f29d").unwrap(),
            CollectionUuid::from_str("c6e27471-d550-4b06-8898-53ab97a8e05f").unwrap(),
            CollectionUuid::from_str("c96049ad-a231-40ec-9f7a-872de5652e00").unwrap(),
            CollectionUuid::from_str("cd541d84-c4fe-4071-b6c3-ab8ec8f264b0").unwrap(),
            CollectionUuid::from_str("cdd05786-adf6-4cf8-9dee-700a4e65903d").unwrap(),
            CollectionUuid::from_str("cf812f6b-aff5-42a8-b62c-4ffaf0ca06db").unwrap(),
            CollectionUuid::from_str("cfd0d5b6-83c7-47f9-96c6-1afbcea74540").unwrap(),
            CollectionUuid::from_str("cff1aa66-1005-4c6b-b7bc-5096fea02ba5").unwrap(),
            CollectionUuid::from_str("cffdf0b9-ada1-4047-9e7e-7bfb1cd63996").unwrap(),
            CollectionUuid::from_str("d2083c44-54aa-4a64-b0e5-6ddc62cedf81").unwrap(),
            CollectionUuid::from_str("d3949100-306f-4bb0-9680-496922c04e16").unwrap(),
            CollectionUuid::from_str("d5221847-4a16-4230-8c73-f79d4a1cdd7d").unwrap(),
            CollectionUuid::from_str("d563f5ee-020c-41de-9667-fcc42e3e96ec").unwrap(),
            CollectionUuid::from_str("d601d4e3-5a61-45e3-9eac-46f4f7378024").unwrap(),
            CollectionUuid::from_str("d64878f9-3aee-4ccd-86c9-60ab642f1400").unwrap(),
            CollectionUuid::from_str("d68df0f8-5af0-4223-91aa-fdfd9ef0cd02").unwrap(),
            CollectionUuid::from_str("d95f6971-988c-4675-adc4-f1efc567a2f7").unwrap(),
            CollectionUuid::from_str("ddf2d754-a5e0-4efe-b843-25f22bfbf8e8").unwrap(),
            CollectionUuid::from_str("e034acc5-bf2f-49e8-aee9-9df96e358881").unwrap(),
            CollectionUuid::from_str("e2f08e5e-ee07-4c94-a45d-8e8e586103f2").unwrap(),
            CollectionUuid::from_str("e5576f13-9ebc-47c3-8e33-bc775ade801a").unwrap(),
            CollectionUuid::from_str("e66690a6-a756-40a2-8313-1742d1d730cd").unwrap(),
            CollectionUuid::from_str("e693e433-cdca-47ff-ab14-656dc9faa2d5").unwrap(),
            CollectionUuid::from_str("e7831427-0714-48e4-8a1f-e33cc62f469b").unwrap(),
            CollectionUuid::from_str("ebf5e6e6-0dcd-49c4-aa7f-c5668d746957").unwrap(),
            CollectionUuid::from_str("ed1e38e8-c910-4c3f-9e3d-a5ee9270aa52").unwrap(),
            CollectionUuid::from_str("ef53a48b-1488-4ceb-a4dc-b3aa1858a866").unwrap(),
            CollectionUuid::from_str("f0f4c6e3-492f-4e03-ae9d-d09e399fc074").unwrap(),
            CollectionUuid::from_str("f7bbf5eb-1816-4e4b-9dbe-09b3468d5682").unwrap(),
            CollectionUuid::from_str("f7d96c65-3db4-470e-b88b-088e8e27a8b9").unwrap(),
            CollectionUuid::from_str("f9cbbb70-725a-438c-8b84-aeb38afe8f63").unwrap(),
            CollectionUuid::from_str("fb5b2da6-240f-4894-a98f-70faa6caf4e2").unwrap(),
            CollectionUuid::from_str("ff136c00-d47c-456c-b115-aeae0125a01e").unwrap(),
        ];
        collections.retain(|x| blessed.contains(&x.collection_id));
        let collection_records = self.verify_and_enrich_collections(collections).await;
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

    use super::*;
    use crate::compactor::scheduler_policy::LasCompactionTimeSchedulerPolicy;
    use chroma_config::assignment::assignment_policy::RendezvousHashingAssignmentPolicy;
    use chroma_log::in_memory_log::{InMemoryLog, InternalLogRecord};
    use chroma_memberlist::memberlist_provider::Member;
    use chroma_sysdb::TestSysDb;
    use chroma_types::{Collection, LogRecord, Operation, OperationRecord};

    #[tokio::test]
    async fn test_scheduler() {
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
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_collection(collection_1);
                sysdb.add_collection(collection_2);
                let last_compaction_time_1 = 2;
                sysdb.add_tenant_last_compaction_time(tenant_1, last_compaction_time_1);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let my_member = Member {
            member_id: "member_1".to_string(),
            member_ip: "10.0.0.1".to_string(),
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
        // Scheduler does nothing without memberlist
        scheduler.schedule().await;
        let jobs = scheduler.get_jobs();
        assert_eq!(jobs.count(), 0);

        // Set empty memberlist
        // Scheduler does nothing with empty memberlist
        scheduler.set_memberlist(vec![]);
        scheduler.schedule().await;
        let jobs = scheduler.get_jobs();
        assert_eq!(jobs.count(), 0);

        // Set memberlist
        scheduler.set_memberlist(vec![my_member.clone()]);
        scheduler.schedule().await;
        let jobs = scheduler.get_jobs();
        let jobs = jobs.collect::<Vec<&CompactionJob>>();
        // Scheduler ignores collection that failed to fetch last compaction time
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].collection_id, collection_uuid_1,);
        scheduler.succeed_collection(collection_uuid_1);

        // Add last compaction time for tenant_2
        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                let last_compaction_time_2 = 1;
                sysdb.add_tenant_last_compaction_time(tenant_2, last_compaction_time_2);
            }
            _ => panic!("Invalid sysdb type"),
        }
        scheduler.schedule().await;
        let jobs = scheduler.get_jobs();
        let jobs = jobs.collect::<Vec<&CompactionJob>>();
        // Scheduler schedules collections based on last compaction time
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].collection_id, collection_uuid_2,);
        assert_eq!(jobs[1].collection_id, collection_uuid_1,);
        scheduler.succeed_collection(collection_uuid_1);
        scheduler.succeed_collection(collection_uuid_2);

        // Set disable list.
        std::env::set_var(
            "CHROMA_COMPACTION_SERVICE__COMPACTOR__DISABLED_COLLECTIONS",
            format!("[\"{}\"]", collection_uuid_1.0),
        );
        scheduler.schedule().await;
        let jobs = scheduler.get_jobs();
        let jobs = jobs.collect::<Vec<&CompactionJob>>();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].collection_id, collection_uuid_2,);
        scheduler.succeed_collection(collection_uuid_2);
        std::env::set_var(
            "CHROMA_COMPACTION_SERVICE__COMPACTOR__DISABLED_COLLECTIONS",
            "[]",
        );
        // Even . should work.
        std::env::set_var(
            "CHROMA_COMPACTION_SERVICE.COMPACTOR.DISABLED_COLLECTIONS",
            format!("[\"{}\"]", collection_uuid_2.0),
        );
        std::env::set_var(
            "CHROMA_COMPACTION_SERVICE.IRRELEVANT",
            format!("[\"{}\"]", collection_uuid_1.0),
        );
        scheduler.schedule().await;
        let jobs = scheduler.get_jobs();
        let jobs = jobs.collect::<Vec<&CompactionJob>>();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].collection_id, collection_uuid_1,);
        scheduler.succeed_collection(collection_uuid_1);
        scheduler.succeed_collection(collection_uuid_2);
        std::env::set_var(
            "CHROMA_COMPACTION_SERVICE.COMPACTOR.DISABLED_COLLECTIONS",
            "[]",
        );

        // Test filter_collections
        let member_1 = Member {
            member_id: "member_1".to_string(),
            member_ip: "10.0.0.1".to_string(),
            member_node_name: "node_1".to_string(),
        };
        let member_2 = Member {
            member_id: "member_2".to_string(),
            member_ip: "10.0.0.2".to_string(),
            member_node_name: "node_2".to_string(),
        };
        let members = vec![member_1.clone(), member_2.clone()];
        scheduler.set_memberlist(members);
        scheduler.schedule().await;
        let jobs = scheduler.get_jobs();
        assert_eq!(jobs.count(), 1);
        scheduler.succeed_collection(collection_uuid_2);

        let members = vec![member_1.clone()];
        scheduler.set_memberlist(members);
        // Test dead jobs
        std::env::set_var(
            "CHROMA_COMPACTION_SERVICE.COMPACTOR.DISABLED_COLLECTIONS",
            "[]",
        );
        std::env::set_var("CHROMA_COMPACTION_SERVICE.IRRELEVANT", "[]");
        for _ in 0..max_failure_count {
            scheduler.schedule().await;
            let jobs = scheduler.get_jobs();
            let jobs = jobs.collect::<Vec<&CompactionJob>>();
            assert_eq!(jobs.len(), 2);
            scheduler.fail_collection(collection_uuid_1);
            scheduler.succeed_collection(collection_uuid_2);
        }
        scheduler.schedule().await;
        let jobs = scheduler.get_jobs();
        let jobs = jobs.collect::<Vec<&CompactionJob>>();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].collection_id, collection_uuid_2);
        scheduler.succeed_collection(collection_uuid_2);
    }

    #[tokio::test]
    #[should_panic(expected = "is less than offset")]
    async fn test_scheduler_panic() {
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
            .update_collection_log_offset(&tenant_1, collection_uuid_1, 2)
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
    }
}
