use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_log::Log;
use chroma_sysdb::{CreateOutputCollectionForTaskError, GetTaskError, SysDb};
use chroma_system::{Operator, OperatorType};
use chroma_types::{AdvanceTaskError, CollectionUuid, Task};
use thiserror::Error;

/// The `PrepareTaskOperator` prepares a task execution by:
/// 1. Fetching the latest task state from SysDB using task_uuid
/// 2. Asserting that the input nonce matches next_nonce or lowest_live_nonce
/// 3. Determining state transition (waiting→scheduled or already scheduled)
/// 4. If transitioning to scheduled, call advance_task and scout_logs to check if we should skip
/// 5. Creating the output collection if it doesn't exist
///
/// # Parameters
/// - `sysdb`: The sysdb client
/// - `log`: The log client for scout_logs
/// - `task_uuid`: The UUID of the task
///
/// # Inputs
/// - `nonce`: The invocation nonce from the scheduler
///
/// # Outputs
/// - The task object with updated state, execution_nonce, and a flag indicating whether to skip execution
#[derive(Clone, Debug)]
pub struct PrepareTaskOperator {
    pub sysdb: SysDb,
    pub log: Log,
    pub task_uuid: chroma_types::TaskUuid,
}

#[derive(Clone, Debug)]
pub struct PrepareTaskInput {
    pub nonce: chroma_types::NonceUuid,
}

#[derive(Clone, Debug)]
// Fields are used in tests and will be used in orchestration code in a follow-up change
#[allow(dead_code)]
pub struct PrepareTaskOutput {
    /// The task object fetched from SysDB
    pub task: Task,
    /// The nonce to use for this task execution
    pub execution_nonce: chroma_types::NonceUuid,
    /// If true, skip execution and go directly to FinishTask
    /// This happens when there aren't enough new records to process
    pub should_skip_execution: bool,
    /// The output collection ID (created if it didn't exist)
    pub output_collection_id: CollectionUuid,
}

#[derive(Debug, Error)]
pub enum PrepareTaskError {
    #[error("Task not found in SysDB")]
    TaskNotFound,
    #[error("Failed to get task: {0}")]
    GetTask(#[from] GetTaskError),
    #[error("Failed to create output collection for task: {0}")]
    CreateOutputCollectionForTask(#[from] CreateOutputCollectionForTaskError),
    #[error("Invalid nonce: provided={provided}, expected next={expected_next} or lowest={expected_lowest}")]
    InvalidNonce {
        provided: chroma_types::NonceUuid,
        expected_next: chroma_types::NonceUuid,
        expected_lowest: chroma_types::NonceUuid,
    },
    #[error("Failed to advance task: {0}")]
    AdvanceTask(#[from] AdvanceTaskError),
    #[error("Failed to scout logs: {0}")]
    ScoutLogs(String),
}

impl ChromaError for PrepareTaskError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            PrepareTaskError::TaskNotFound => chroma_error::ErrorCodes::NotFound,
            PrepareTaskError::GetTask(e) => e.code(),
            PrepareTaskError::CreateOutputCollectionForTask(e) => e.code(),
            PrepareTaskError::InvalidNonce { .. } => chroma_error::ErrorCodes::InvalidArgument,
            PrepareTaskError::AdvanceTask(e) => e.code(),
            PrepareTaskError::ScoutLogs(_) => chroma_error::ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<PrepareTaskInput, PrepareTaskOutput> for PrepareTaskOperator {
    type Error = PrepareTaskError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(&self, input: &PrepareTaskInput) -> Result<PrepareTaskOutput, PrepareTaskError> {
        tracing::info!(
            "[{}]: Preparing task {} with nonce {}",
            self.get_name(),
            self.task_uuid.0,
            input.nonce
        );

        let mut sysdb = self.sysdb.clone();
        let mut log = self.log.clone();

        // 1. Fetch the task from SysDB using UUID
        let mut task = sysdb
            .get_task_by_uuid(self.task_uuid)
            .await
            .map_err(|e| match e {
                GetTaskError::NotFound => PrepareTaskError::TaskNotFound,
                other => PrepareTaskError::GetTask(other),
            })?;

        tracing::debug!(
            "[{}]: Retrieved task {} - next_nonce={}, lowest_live_nonce={:?}",
            self.get_name(),
            task.name,
            task.next_nonce,
            task.lowest_live_nonce
        );

        // 2. ASSERT: nonce must match either next_nonce or lowest_live_nonce
        let matches_lowest = task.lowest_live_nonce == Some(input.nonce);
        if input.nonce != task.next_nonce && !matches_lowest {
            tracing::error!(
                "[{}]: Invalid nonce for task {} - provided={}, expected next={} or lowest={:?}",
                self.get_name(),
                task.name,
                input.nonce,
                task.next_nonce,
                task.lowest_live_nonce
            );
            return Err(PrepareTaskError::InvalidNonce {
                provided: input.nonce,
                expected_next: task.next_nonce,
                expected_lowest: task.lowest_live_nonce.unwrap_or_default(),
            });
        }

        // 3. Determine state transition and whether to skip execution
        let execution_nonce = input.nonce;
        let mut should_skip_execution = false;

        if task
            .lowest_live_nonce
            .is_some_and(|lln| task.next_nonce != lln)
        {
            // Incomplete nonce exists - we are already **scheduled**
            tracing::info!(
                "[{}]: Task {} already in scheduled state (incomplete nonce exists)",
                self.get_name(),
                task.name
            );

            // Scout logs to see if we should skip execution (task may have already executed)
            let next_log_offset = log
                .scout_logs(
                    &task.tenant_id,
                    task.input_collection_id,
                    task.completion_offset,
                )
                .await
                .map_err(|e| {
                    tracing::error!(
                        "[{}]: Failed to scout logs for task {}: {}",
                        self.get_name(),
                        task.name,
                        e
                    );
                    PrepareTaskError::ScoutLogs(format!("Failed to scout logs: {}", e))
                })?;

            let new_records_count = next_log_offset.saturating_sub(task.completion_offset);
            should_skip_execution = new_records_count < task.min_records_for_task;

            if should_skip_execution {
                tracing::info!(
                    "[{}]: Skipping execution for task {} - not enough new records (new={}, min={})",
                    self.get_name(),
                    task.name,
                    new_records_count,
                    task.min_records_for_task
                );
            } else {
                tracing::info!(
                    "[{}]: Task {} will proceed with execution ({} new records available)",
                    self.get_name(),
                    task.name,
                    new_records_count
                );
            }
        } else {
            // Currently **waiting**, transition to **scheduled**
            tracing::info!(
                "[{}]: Task {} transitioning from waiting to scheduled",
                self.get_name(),
                task.name
            );

            // Call advance_task to increment next_nonce and set next_run (with nonce check for concurrency safety)
            // Set next_run to some reasonable delay (e.g., 60 seconds) since we're starting work
            const DEFAULT_THROTTLE_INTERVAL_SECS: u64 = 60;
            let advance_response = sysdb
                .advance_task(
                    task.id,
                    input.nonce.0,
                    task.completion_offset,
                    DEFAULT_THROTTLE_INTERVAL_SECS, // Set next_run since we're advancing nonce
                )
                .await?;

            tracing::debug!(
                "[{}]: Advanced task {} - new next_nonce={}",
                self.get_name(),
                task.name,
                advance_response.next_nonce
            );

            // Update task with the new nonce values
            task.next_nonce = chroma_types::NonceUuid(advance_response.next_nonce);
            task.next_run = advance_response.next_run;
        }

        // 4. Create output collection if it doesn't exist
        let output_collection_id = if let Some(output_id) = task.output_collection_id {
            // Output collection already exists
            output_id
        } else {
            // Create new output collection atomically with task update
            tracing::info!(
                "[{}]: Creating output collection '{}' for task {}",
                self.get_name(),
                task.output_collection_name,
                task.name
            );

            let collection_id = sysdb
                .create_output_collection_for_task(
                    task.id,
                    task.output_collection_name.clone(),
                    task.tenant_id.clone(),
                    task.database_id.clone(),
                )
                .await?;

            // Update local task object with the new output collection ID
            task.output_collection_id = Some(collection_id);

            collection_id
        };

        Ok(PrepareTaskOutput {
            task: task.clone(),
            execution_nonce,
            should_skip_execution,
            output_collection_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_config::Configurable;
    use chroma_log::in_memory_log::{InMemoryLog, InternalLogRecord};
    use chroma_log::Log;
    use chroma_sysdb::{GrpcSysDb, GrpcSysDbConfig, SysDb};
    use chroma_types::{CollectionUuid, LogRecord, NonceUuid, Operation};
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

    fn create_test_log_record(collection_id: CollectionUuid, offset: i64) -> InternalLogRecord {
        InternalLogRecord {
            collection_id,
            log_offset: offset,
            log_ts: offset,
            record: LogRecord {
                log_offset: offset,
                record: chroma_types::OperationRecord {
                    id: format!("id_{}", offset),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_prepare_task_invalid_nonce() {
        // Setup
        let mut sysdb = get_grpc_sysdb().await;
        let log = Log::InMemory(InMemoryLog::new());

        let input_collection_id =
            setup_tenant_and_database(&mut sysdb, "test_tenant", "test_db").await;

        // Create a task via SysDB with unique name
        let task_id = sysdb
            .create_task(
                format!("test_task_{}", Uuid::new_v4()),
                "record_counter".to_string(),
                input_collection_id,
                format!("test_output_{}", Uuid::new_v4()),
                serde_json::Value::Null,
                "test_tenant".to_string(),
                "test_db".to_string(),
                10, // min_records_for_task
            )
            .await
            .unwrap();

        // Get the task to find its next_nonce
        let task = sysdb.get_task_by_uuid(task_id).await.unwrap();

        // Create operator
        let operator = PrepareTaskOperator {
            sysdb: sysdb.clone(),
            log: log.clone(),
            task_uuid: task_id,
        };

        // Try to run with an invalid nonce (not matching next_nonce or lowest_live_nonce)
        let wrong_nonce = NonceUuid(Uuid::new_v4());
        let input = PrepareTaskInput { nonce: wrong_nonce };

        // Run - should fail with InvalidNonce
        let result = operator.run(&input).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            PrepareTaskError::InvalidNonce {
                provided,
                expected_next,
                ..
            } => {
                assert_eq!(provided, wrong_nonce);
                assert_eq!(expected_next, task.next_nonce);
            }
            _ => panic!("Expected InvalidNonce error"),
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_prepare_task_valid_next_nonce_transitions_to_scheduled() {
        // Setup
        let mut sysdb = get_grpc_sysdb().await;
        let log = Log::InMemory(InMemoryLog::new());

        let input_collection_id =
            setup_tenant_and_database(&mut sysdb, "test_tenant", "test_db").await;

        // Create a task
        let task_id = sysdb
            .create_task(
                format!("test_task_{}", Uuid::new_v4()),
                "record_counter".to_string(),
                input_collection_id,
                format!("test_output_{}", Uuid::new_v4()),
                serde_json::Value::Null,
                "test_tenant".to_string(),
                "test_db".to_string(),
                10,
            )
            .await
            .unwrap();

        // Get the task's next_nonce
        let task_before = sysdb.get_task_by_uuid(task_id).await.unwrap();
        let next_nonce = task_before.next_nonce;

        // Create operator
        let operator = PrepareTaskOperator {
            sysdb: sysdb.clone(),
            log: log.clone(),
            task_uuid: task_id,
        };

        // Run with valid next_nonce - should transition to scheduled
        let input = PrepareTaskInput { nonce: next_nonce };
        let result = operator.run(&input).await;

        // Assert: Operation succeeded
        assert!(result.is_ok());
        let output = result.unwrap();

        // Assert: execution_nonce matches input
        assert_eq!(output.execution_nonce, next_nonce);

        // Assert: should_skip_execution is false (we're transitioning, not skipping)
        assert!(!output.should_skip_execution);

        // Assert: Task was advanced - next_nonce should have changed
        let task_after = sysdb.get_task_by_uuid(task_id).await.unwrap();
        assert_ne!(task_after.next_nonce, task_before.next_nonce);

        // Assert: lowest_live_nonce should now be set to the nonce we used
        assert_eq!(task_after.lowest_live_nonce, Some(next_nonce));
    }

    #[tokio::test]
    async fn test_k8s_integration_prepare_task_with_lowest_live_nonce_skips_execution() {
        // Setup: Task that's already scheduled (lowest_live_nonce exists and != next_nonce)
        let mut sysdb = get_grpc_sysdb().await;
        let log = Log::InMemory(InMemoryLog::new());

        let input_collection_id =
            setup_tenant_and_database(&mut sysdb, "test_tenant", "test_db").await;

        // Create a task
        let task_id = sysdb
            .create_task(
                format!("test_task_{}", Uuid::new_v4()),
                "record_counter".to_string(),
                input_collection_id,
                format!("test_output_{}", Uuid::new_v4()),
                serde_json::Value::Null,
                "test_tenant".to_string(),
                "test_db".to_string(),
                10, // min_records_for_task = 10
            )
            .await
            .unwrap();

        // Advance the task once to set lowest_live_nonce
        let task_initial = sysdb.get_task_by_uuid(task_id).await.unwrap();
        let first_nonce = task_initial.next_nonce;

        sysdb
            .advance_task(task_id, first_nonce.0, 0, 60)
            .await
            .unwrap();

        // Now lowest_live_nonce = first_nonce, next_nonce = new value
        // No new log records, so should skip execution

        // Create operator
        let operator = PrepareTaskOperator {
            sysdb: sysdb.clone(),
            log: log.clone(),
            task_uuid: task_id,
        };

        // Run with the lowest_live_nonce (incomplete nonce)
        let input = PrepareTaskInput { nonce: first_nonce };
        let result = operator.run(&input).await;

        // Assert: Operation succeeded
        assert!(result.is_ok());
        let output = result.unwrap();

        // Assert: should_skip_execution is true (no new records)
        assert!(output.should_skip_execution);

        // Assert: execution_nonce is the lowest_live_nonce
        assert_eq!(output.execution_nonce, first_nonce);
    }

    #[tokio::test]
    async fn test_k8s_integration_prepare_task_with_lowest_live_nonce_and_new_records() {
        // Setup: Task that's already scheduled with new records available
        let mut sysdb = get_grpc_sysdb().await;

        let input_collection_id =
            setup_tenant_and_database(&mut sysdb, "test_tenant", "test_db").await;

        let mut in_memory_log = InMemoryLog::new();

        // Add log records (15 records, above the threshold of 10)
        for i in 0..15 {
            in_memory_log.add_log(
                input_collection_id,
                create_test_log_record(input_collection_id, i),
            );
        }

        let log = Log::InMemory(in_memory_log);

        // Create a task
        let task_id = sysdb
            .create_task(
                format!("test_task_{}", Uuid::new_v4()),
                "record_counter".to_string(),
                input_collection_id,
                format!("test_output_{}", Uuid::new_v4()),
                serde_json::Value::Null,
                "test_tenant".to_string(),
                "test_db".to_string(),
                10, // min_records_for_task = 10
            )
            .await
            .unwrap();

        // Advance the task to create lowest_live_nonce
        let task_initial = sysdb.get_task_by_uuid(task_id).await.unwrap();
        let first_nonce = task_initial.next_nonce;

        sysdb
            .advance_task(task_id, first_nonce.0, 0, 60)
            .await
            .unwrap();

        // Create operator
        let operator = PrepareTaskOperator {
            sysdb: sysdb.clone(),
            log: log.clone(),
            task_uuid: task_id,
        };

        // Run with the lowest_live_nonce
        let input = PrepareTaskInput { nonce: first_nonce };
        let result = operator.run(&input).await;

        // Assert: Operation succeeded
        assert!(result.is_ok());
        let output = result.unwrap();

        // Assert: should_skip_execution is false (we have 15 new records, above threshold)
        assert!(!output.should_skip_execution);

        // Assert: execution_nonce is the lowest_live_nonce
        assert_eq!(output.execution_nonce, first_nonce);
    }

    #[tokio::test]
    async fn test_k8s_integration_prepare_task_creates_output_collection() {
        // Setup
        let mut sysdb = get_grpc_sysdb().await;
        let log = Log::InMemory(InMemoryLog::new());

        let input_collection_id =
            setup_tenant_and_database(&mut sysdb, "test_tenant", "test_db").await;

        // Create a task (output collection doesn't exist yet)
        let task_id = sysdb
            .create_task(
                format!("test_task_{}", Uuid::new_v4()),
                "record_counter".to_string(),
                input_collection_id,
                format!("test_output_collection_{}", Uuid::new_v4()),
                serde_json::Value::Null,
                "test_tenant".to_string(),
                "test_db".to_string(),
                10,
            )
            .await
            .unwrap();

        let task_before = sysdb.get_task_by_uuid(task_id).await.unwrap();
        let next_nonce = task_before.next_nonce;

        // Verify output collection doesn't exist yet
        assert_eq!(task_before.output_collection_id, None);

        // Create operator
        let operator = PrepareTaskOperator {
            sysdb: sysdb.clone(),
            log: log.clone(),
            task_uuid: task_id,
        };

        // Run
        let input = PrepareTaskInput { nonce: next_nonce };
        let result = operator.run(&input).await;

        // Assert: Operation succeeded
        assert!(result.is_ok());
        let output = result.unwrap();

        // Assert: output_collection_id was created and returned
        // Verify output collection was created (not all zeros)
        assert_ne!(output.output_collection_id, CollectionUuid::new());

        // Assert: Task now has the output_collection_id set
        assert_eq!(
            output.task.output_collection_id,
            Some(output.output_collection_id)
        );
    }
}
