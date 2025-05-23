use std::collections::HashSet;
use std::str::FromStr;

use chroma_config::assignment::assignment_policy::AssignmentPolicy;
use chroma_log::{CollectionInfo, CollectionRecord, Log};
use chroma_memberlist::memberlist_provider::Memberlist;
use chroma_sysdb::SysDb;
use chroma_types::CollectionUuid;
use figment::providers::Env;
use figment::Figment;
use serde::Deserialize;
use uuid::Uuid;

use crate::compactor::scheduler_policy::SchedulerPolicy;
use crate::compactor::types::CompactionJob;

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
        }
    }

    pub(crate) fn add_oneoff_collections(&mut self, ids: Vec<CollectionUuid>) {
        self.oneoff_collections.extend(ids);
    }

    pub(crate) fn get_oneoff_collections(&self) -> Vec<CollectionUuid> {
        self.oneoff_collections.iter().cloned().collect()
    }

    async fn get_collections_with_new_data(&mut self) -> Vec<CollectionInfo> {
        let collections = self
            .log
            .get_collections_with_new_data(self.min_compaction_size as u64)
            .await;

        match collections {
            Ok(collections) => collections,
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
            {
                tracing::info!(
                    "Ignoring collection: {:?} because it disabled for compaction",
                    collection_info.collection_id
                );
                continue;
            }
            let collection_id = Some(collection_info.collection_id);
            // TODO: add a cache to avoid fetching the same collection multiple times
            let result = self
                .sysdb
                .get_collections(collection_id, None, None, None, None, 0)
                .await;

            match result {
                Ok(collection) => {
                    if collection.is_empty() {
                        tracing::info!(
                            "Collection not found, purging: {:?}",
                            collection_info.collection_id
                        );
                        if let Err(err) = self
                            .log
                            .purge_dirty_for_collection(collection_info.collection_id)
                            .await
                        {
                            // NOTE(rescrv):  This is something we ideally want to know about, but
                            // cannot act on except to skip the collection.  Some day we'll have
                            // something by which we can say, "Watch for this condition."
                            tracing::warn!(
                                "Error purging dirty records for collection: {:?}, error: {:?}",
                                collection_info.collection_id,
                                err
                            );
                        }
                        continue;
                    }

                    // TODO: make querying the last compaction time in batch
                    let log_position_in_collecion = collection[0].log_position;
                    let tenant_ids = vec![collection[0].tenant.clone()];
                    let tenant = self.sysdb.get_last_compaction_time(tenant_ids).await;

                    let last_compaction_time = match tenant {
                        Ok(tenant) => tenant[0].last_compaction_time,
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
                    if log_position_in_collecion + 1 < offset {
                        panic!(
                            "offset in sysdb is less than offset in log, this should not happen!"
                        )
                    } else {
                        // The offset in sysdb is the last offset that has been compacted.
                        // We need to start from the next offset.
                        offset = log_position_in_collecion + 1;
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
                scheduled_collections.push(record);
            }
        }

        let filtered_collections = self.filter_collections(scheduled_collections);
        self.job_queue.extend(
            self.policy
                .determine(filtered_collections, self.max_concurrent_jobs as i32),
        );
        self.job_queue.truncate(self.max_concurrent_jobs);
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
    }

    #[tokio::test]
    #[should_panic(
        expected = "offset in sysdb is less than offset in log, this should not happen!"
    )]
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
        );

        scheduler.set_memberlist(vec![my_member.clone()]);
        scheduler.schedule().await;
    }
}
