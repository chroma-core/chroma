use std::sync::Arc;

use chrono::{DateTime, Utc};

use chroma_storage::Storage;
use chroma_types::{CollectionUuid, NonceUuid, TaskUuid};
use s3heap::{heap_path_from_hostname, Error, HeapReader, HeapScheduler, Limits};

/// A task that has been scheduled for execution.
#[derive(Clone, Debug)]
pub struct SchedulableTask {
    pub collection_id: CollectionUuid,
    pub task_id: TaskUuid,
    pub nonce: NonceUuid,
    pub bucket: DateTime<Utc>,
}

/// Reader for fetching scheduled tasks from multiple heap instances.
pub struct TaskHeapReader {
    storage: Storage,
    heap_scheduler: Arc<dyn HeapScheduler>,
}

impl TaskHeapReader {
    /// Create a new TaskHeapReader with the given dependencies.
    pub fn new(storage: Storage, heap_scheduler: Arc<dyn HeapScheduler>) -> Self {
        Self {
            storage,
            heap_scheduler,
        }
    }

    /// Get tasks scheduled for execution across all rust-log-service heaps.
    ///
    /// This method queries heap/rust-log-service-0, heap/rust-log-service-1, etc.,
    /// until it encounters an empty heap or error, collecting up to `limit` tasks.
    pub async fn get_tasks_scheduled_for_execution(&self, limits: Limits) -> Vec<SchedulableTask> {
        let mut all_tasks = Vec::new();
        let mut service_index = 0;
        let max_items = limits.max_items.unwrap_or(1000);

        loop {
            if all_tasks.len() >= max_items {
                break;
            }

            let heap_prefix =
                heap_path_from_hostname(&format!("rust-log-service-{}", service_index));

            let reader_result = HeapReader::new(
                self.storage.clone(),
                heap_prefix.clone(),
                Arc::clone(&self.heap_scheduler),
            )
            .await;

            let reader = match reader_result {
                Ok(r) => r,
                Err(Error::UninitializedHeap(_)) => {
                    break;
                }
                Err(e) => {
                    tracing::error!("Error creating heap reader for {}: {:?}", heap_prefix, e);
                    service_index += 1;
                    continue;
                }
            };

            match reader.peek(|_, _| true, limits).await {
                Ok(items) => {
                    tracing::trace!("Found {} tasks in {}", items.len(), heap_prefix);
                    for (bucket, item) in items {
                        let collection_id = CollectionUuid(*item.trigger.partitioning.as_uuid());
                        all_tasks.push(SchedulableTask {
                            collection_id,
                            task_id: TaskUuid(*item.trigger.scheduling.as_uuid()),
                            nonce: NonceUuid(item.nonce),
                            bucket,
                        });
                    }
                }
                Err(e) => {
                    tracing::trace!("Error reading from {}: {:?}", heap_prefix, e);
                }
            }

            service_index += 1;
        }

        all_tasks.truncate(max_items);
        all_tasks
    }
}
