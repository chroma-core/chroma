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
    ScoutLogsError(String),
}

impl ChromaError for PrepareTaskError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            PrepareTaskError::TaskNotFound => chroma_error::ErrorCodes::NotFound,
            PrepareTaskError::GetTask(e) => e.code(),
            PrepareTaskError::CreateOutputCollectionForTask(e) => e.code(),
            PrepareTaskError::InvalidNonce { .. } => chroma_error::ErrorCodes::InvalidArgument,
            PrepareTaskError::AdvanceTask(e) => e.code(),
            PrepareTaskError::ScoutLogsError(_) => chroma_error::ErrorCodes::Internal,
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
                    PrepareTaskError::ScoutLogsError(format!("Failed to scout logs: {}", e))
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
                    task.completion_offset as i64,
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
