use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_sysdb::SysDb;
use chroma_system::Operator;
use chroma_types::{FinishTaskError as SysDbFinishTaskError, Task, TaskUuid};
use thiserror::Error;

/// The finish task operator is responsible for updating task state in SysDB
/// after a successful task execution run.
#[derive(Debug)]
pub struct FinishTaskOperator {
    log_client: Log,
    sysdb: SysDb,
}

impl FinishTaskOperator {
    /// Create a new finish task operator.
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
    ScoutLogsError(String),
    #[error("Failed to finish task in SysDB: {0}")]
    SysDbError(#[from] SysDbFinishTaskError),
}

impl ChromaError for FinishTaskError {
    fn code(&self) -> ErrorCodes {
        match self {
            FinishTaskError::ScoutLogsError(_) => ErrorCodes::Internal,
            FinishTaskError::SysDbError(e) => e.code(),
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
                FinishTaskError::ScoutLogsError(format!("Failed to scout logs: {}", e))
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

            // TODO: Schedule a new task for next nonce.
        }

        // Step 2: Update lowest_live_nonce to equal next_nonce
        // This indicates that finish_task completed successfully and this epoch is verified
        // If this fails, lowest_live_nonce < next_nonce will indicate
        // that we should skip execution next time and only do the recheck phase
        let mut sysdb = self.sysdb.clone();
        sysdb.finish_task(input.updated_task.id).await?;

        // TODO: delete old nonce from scheduler

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
