use std::collections::HashMap;

use chroma_sysdb::SysDb;
use chroma_types::{CollectionUuid, ScheduleEntry};
use s3heap::{HeapScheduler, Schedule, Triggerable};
use uuid::Uuid;

/// Scheduler that integrates with SysDb to manage task scheduling.
pub struct SysDbScheduler {
    sysdb: SysDb,
}

impl SysDbScheduler {
    pub fn new(sysdb: SysDb) -> SysDbScheduler {
        Self { sysdb }
    }
}

#[async_trait::async_trait]
impl HeapScheduler for SysDbScheduler {
    async fn are_done(&self, items: &[(Triggerable, Uuid)]) -> Result<Vec<bool>, s3heap::Error> {
        let collection_ids = items
            .iter()
            .map(|item| CollectionUuid(*item.0.partitioning.as_uuid()))
            .collect::<Vec<_>>();
        let schedules = self
            .sysdb
            .clone()
            .peek_schedule_by_collection_id(&collection_ids)
            .await
            .map_err(|e| s3heap::Error::Internal(format!("sysdb error: {}", e)))?;
        let mut by_triggerable: HashMap<Triggerable, ScheduleEntry> = HashMap::default();
        for schedule in schedules.into_iter() {
            by_triggerable.insert(
                Triggerable {
                    partitioning: schedule.collection_id.0.into(),
                    scheduling: schedule.attached_function_id.into(),
                },
                schedule,
            );
        }
        let mut results = Vec::with_capacity(items.len());
        for (triggerable, nonce) in items.iter() {
            let Some(schedule) = by_triggerable.get(triggerable) else {
                // No schedule found - task is done/completed
                // TODO(tanujnay112): This has to be reconsidered when task templates are lazily created
                results.push(true);
                continue;
            };

            // Check if this nonce is done based on lowest_live_nonce
            let is_done = match schedule.lowest_live_nonce {
                None => false,
                Some(lowest_live) => *nonce < lowest_live,
            };
            results.push(is_done);
        }
        Ok(results)
    }

    async fn get_schedules(&self, ids: &[Uuid]) -> Result<Vec<Schedule>, s3heap::Error> {
        let collection_ids = ids.iter().cloned().map(CollectionUuid).collect::<Vec<_>>();
        let schedules = self
            .sysdb
            .clone()
            .peek_schedule_by_collection_id(&collection_ids)
            .await
            .map_err(|e| s3heap::Error::Internal(format!("sysdb error: {}", e)))?;
        let mut results = Vec::new();
        tracing::info!("schedules {schedules:?}");
        for schedule in schedules.into_iter() {
            if let Some(when_to_run) = schedule.when_to_run {
                results.push(Schedule {
                    triggerable: Triggerable {
                        partitioning: schedule.collection_id.0.into(),
                        scheduling: schedule.attached_function_id.into(),
                    },
                    nonce: schedule.attached_function_run_nonce.0,
                    next_scheduled: when_to_run,
                });
            }
        }
        Ok(results)
    }
}
