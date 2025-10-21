use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_sysdb::SysDb;
use chroma_system::Operator;
use chroma_types::{FinishTaskError as SysDbFinishTaskError, Task, TaskUuid};
use thiserror::Error;

/// The finish task operator is responsible for updating task state in SysDB
/// after a successful task execution run.
#[derive(Debug, Clone)]
pub struct FinishTaskOperator {
    log_client: Log,
    sysdb: SysDb,
}

impl FinishTaskOperator {
    /// Create a new finish task operator.
    #[allow(dead_code)]
    pub fn new(log_client: Log, sysdb: SysDb) -> Box<Self> {
        Box::new(FinishTaskOperator { log_client, sysdb })
    }
}

#[derive(Debug)]
/// The input for the finish task operator.
/// # Parameters
/// * `updated_task` - The updated task record from sysdb.
/// * `records_processed` - The number of records processed in this run.
/// * `sysdb` - The sysdb client.
pub struct FinishTaskInput {
    // Updated Task record from sysdb
    updated_task: Task,
}

impl FinishTaskInput {
    /// Create a new finish task input.
    pub fn new(updated_task: Task) -> Self {
        FinishTaskInput { updated_task }
    }
}

/// The output for the finish task operator.
#[derive(Debug)]
pub struct FinishTaskOutput {
    pub _task_id: TaskUuid,
    pub _new_completion_offset: u64,
}

#[derive(Error, Debug)]
pub enum FinishTaskError {
    #[error("Failed to scout logs: {0}")]
    ScoutLogs(String),
    #[error("Failed to finish task in SysDB: {0}")]
    SysDb(#[from] SysDbFinishTaskError),
}

impl ChromaError for FinishTaskError {
    fn code(&self) -> ErrorCodes {
        match self {
            FinishTaskError::ScoutLogs(_) => ErrorCodes::Internal,
            FinishTaskError::SysDb(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<FinishTaskInput, FinishTaskOutput> for FinishTaskOperator {
    type Error = FinishTaskError;

    fn get_name(&self) -> &'static str {
        "FinishTaskOperator"
    }

    async fn run(&self, input: &FinishTaskInput) -> Result<FinishTaskOutput, FinishTaskError> {
        // Step 1: Scout the logs to see if there are any new records written since we started processing
        // This recheck ensures we don't miss any records that were written during our task execution
        tracing::info!(
            "Rechecking logs for task {} with completion offset {}",
            input.updated_task.id.0,
            input.updated_task.completion_offset
        );

        // Scout the logs to check for new records written since we started processing
        // This catches any records that were written during our task execution
        // scout_logs returns the offset of the next record to be inserted
        let mut log_client = self.log_client.clone();
        let next_log_offset = log_client
            .scout_logs(
                &input.updated_task.tenant_id,
                input.updated_task.input_collection_id,
                input.updated_task.completion_offset,
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    task_id = %input.updated_task.id.0,
                    error = %e,
                    "Failed to scout logs during finish_task recheck"
                );
                FinishTaskError::ScoutLogs(format!("Failed to scout logs: {}", e))
            })?;

        // Calculate how many new records were written since we started processing
        let new_records_count =
            next_log_offset.saturating_sub(input.updated_task.completion_offset);
        let new_records_found = new_records_count >= input.updated_task.min_records_for_task;

        if new_records_found {
            tracing::info!(
                task_id = %input.updated_task.id.0,
                new_records_count = new_records_count,
                min_records_threshold = input.updated_task.min_records_for_task,
                "Detected new records written during task execution that exceed threshold"
            );

            // TODO: Schedule a new task for next nonce by pushing to the heap
        }

        // Step 2: Update lowest_live_nonce to equal next_nonce
        // This indicates that finish_task completed successfully and this epoch is verified
        // If this fails, lowest_live_nonce < next_nonce will indicate
        // that we should skip execution next time and only do the recheck phase
        let mut sysdb = self.sysdb.clone();
        sysdb.finish_task(input.updated_task.id).await?;

        tracing::info!(
            "Task {} finish_task completed. lowest_live_nonce updated",
            input.updated_task.id.0,
        );

        Ok(FinishTaskOutput {
            _task_id: input.updated_task.id,
            _new_completion_offset: input.updated_task.completion_offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_config::Configurable;
    use chroma_log::in_memory_log::InMemoryLog;
    use chroma_log::Log;
    use chroma_sysdb::{GrpcSysDb, GrpcSysDbConfig, SysDb};
    use chroma_types::CollectionUuid;
    use uuid::Uuid;

    async fn get_grpc_sysdb() -> SysDb {
        let registry = chroma_config::registry::Registry::default();
        let config = GrpcSysDbConfig {
            host: "localhost".to_string(),
            port: 50051,
            ..Default::default()
        };
        SysDb::Grpc(
            GrpcSysDb::try_from_config(&config, &registry)
                .await
                .unwrap(),
        )
    }

    async fn setup_tenant_and_database(
        sysdb: &mut SysDb,
        tenant: &str,
        database: &str,
    ) -> CollectionUuid {
        // Create tenant (ignore error if exists)
        let _ = sysdb.create_tenant(tenant.to_string()).await;

        // Create database (ignore error if exists)
        let _ = sysdb
            .create_database(
                uuid::Uuid::new_v4(),
                database.to_string(),
                tenant.to_string(),
            )
            .await;

        // Create an input collection
        let collection_id = CollectionUuid::new();
        let collection = sysdb
            .create_collection(
                tenant.to_string(),
                database.to_string(),
                collection_id,
                format!("test_input_collection_{}", uuid::Uuid::new_v4()),
                vec![],    // segments
                None,      // configuration
                None,      // schema
                None,      // metadata
                Some(128), // dimension
                true,      // get_or_create
            )
            .await
            .unwrap();

        collection.collection_id
    }

    #[tokio::test]
    async fn test_k8s_integration_finish_task_updates_lowest_live_nonce() {
        // Setup: Create a task and advance it once
        let mut sysdb = get_grpc_sysdb().await;
        let log = Log::InMemory(InMemoryLog::new());

        let collection_id = setup_tenant_and_database(&mut sysdb, "test_tenant", "test_db").await;

        // Create a task via SysDB
        let task_id = sysdb
            .create_task(
                format!("test_task_{}", Uuid::new_v4()),
                "record_counter".to_string(),
                collection_id,
                format!("test_output_{}", Uuid::new_v4()),
                serde_json::Value::Null,
                "test_tenant".to_string(),
                "test_db".to_string(),
                10,
            )
            .await
            .unwrap();

        let task_initial = sysdb.get_task_by_uuid(task_id).await.unwrap();
        let initial_nonce = task_initial.next_nonce;

        // Advance the task once to set lowest_live_nonce
        sysdb
            .advance_task(task_id, initial_nonce.0, 0, 60)
            .await
            .unwrap();

        let task_advanced = sysdb.get_task_by_uuid(task_id).await.unwrap();

        // Verify: lowest_live_nonce is set, next_nonce has advanced
        assert_eq!(task_advanced.lowest_live_nonce, Some(initial_nonce));
        assert_ne!(task_advanced.next_nonce, initial_nonce);

        let input = FinishTaskInput::new(task_advanced.clone());
        let operator = FinishTaskOperator::new(log.clone(), sysdb.clone());

        // Run finish_task - should move lowest_live_nonce up to match next_nonce
        let result = operator.run(&input).await;

        // Assert: Operation succeeded
        assert!(result.is_ok());

        // Assert: lowest_live_nonce was updated to equal next_nonce
        let task_after = sysdb.get_task_by_uuid(task_id).await.unwrap();
        assert_eq!(task_after.lowest_live_nonce, Some(task_advanced.next_nonce));
        assert_eq!(task_after.next_nonce, task_advanced.next_nonce);
    }

    #[tokio::test]
    async fn test_k8s_integration_finish_task_updates_lowest_live_nonce_from_old_value() {
        // Setup: Task with lowest_live_nonce = A and next_nonce = B
        let mut sysdb = get_grpc_sysdb().await;
        let log = Log::InMemory(InMemoryLog::new());

        let collection_id = setup_tenant_and_database(&mut sysdb, "test_tenant", "test_db").await;

        // Create a task
        let task_id = sysdb
            .create_task(
                format!("test_task_{}", Uuid::new_v4()),
                "record_counter".to_string(),
                collection_id,
                format!("test_output_{}", Uuid::new_v4()),
                serde_json::Value::Null,
                "test_tenant".to_string(),
                "test_db".to_string(),
                10,
            )
            .await
            .unwrap();

        // Advance task once: lowest_live_nonce = A, next_nonce = B
        let task_initial = sysdb.get_task_by_uuid(task_id).await.unwrap();
        let nonce_a = task_initial.next_nonce;

        sysdb.advance_task(task_id, nonce_a.0, 0, 60).await.unwrap();

        let task_after_advance = sysdb.get_task_by_uuid(task_id).await.unwrap();
        let nonce_b = task_after_advance.next_nonce;

        // Verify initial state: lowest_live_nonce is at A, next_nonce is at B
        assert_eq!(task_after_advance.lowest_live_nonce, Some(nonce_a));
        assert_eq!(task_after_advance.next_nonce, nonce_b);
        assert_ne!(nonce_a, nonce_b);

        let input = FinishTaskInput::new(task_after_advance.clone());
        let operator = FinishTaskOperator::new(log.clone(), sysdb.clone());

        // Run finish_task
        let result = operator.run(&input).await;

        // Assert: Operation succeeded
        assert!(result.is_ok());

        // Assert: lowest_live_nonce was moved from A to B (now equals next_nonce)
        let task_after = sysdb.get_task_by_uuid(task_id).await.unwrap();
        assert_eq!(task_after.lowest_live_nonce, Some(nonce_b));
        assert_eq!(task_after.next_nonce, nonce_b);
    }

    #[tokio::test]
    async fn test_k8s_integration_finish_task_error_when_task_not_found() {
        // Setup: Use a task ID that doesn't exist
        let sysdb = get_grpc_sysdb().await;
        let log = Log::InMemory(InMemoryLog::new());

        let collection_id = CollectionUuid::new();

        // Create a fake task that's not in the database
        use chroma_types::{NonceUuid, Task};
        use std::time::SystemTime;

        let fake_task = Task {
            id: TaskUuid(Uuid::new_v4()),
            name: "fake_task".to_string(),
            operator_id: "record_counter".to_string(),
            input_collection_id: collection_id,
            output_collection_name: format!("test_output_{}", Uuid::new_v4()),
            output_collection_id: None,
            params: None,
            tenant_id: "test_tenant".to_string(),
            database_id: "test_db".to_string(),
            last_run: None,
            next_run: SystemTime::now(),
            completion_offset: 0,
            min_records_for_task: 10,
            is_deleted: false,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            next_nonce: NonceUuid(Uuid::new_v4()),
            lowest_live_nonce: None,
        };

        let input = FinishTaskInput::new(fake_task.clone());
        let operator = FinishTaskOperator::new(log.clone(), sysdb.clone());

        // Run
        let result = operator.run(&input).await;

        // Assert: Operation should fail with TaskNotFound error
        assert!(result.is_err());
        match result.unwrap_err() {
            FinishTaskError::SysDb(_) => { /* expected */ }
            _ => panic!("Expected SysDbError"),
        }
    }
}
