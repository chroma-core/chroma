use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_log::Log;
use chroma_sysdb::{
    CreateOutputCollectionForAttachedFunctionError, GetAttachedFunctionError, SysDb,
};
use chroma_system::{Operator, OperatorType};
use chroma_types::{AdvanceAttachedFunctionError, AttachedFunction, CollectionUuid};
use thiserror::Error;

/// The `PrepareAttachedFunctionOperator` prepares a attached function execution by:
/// 1. Fetching the latest attached function state from SysDB using attached_function_uuid
/// 2. Asserting that the input nonce matches next_nonce or lowest_live_nonce
/// 3. Determining state transition (waiting→scheduled or already scheduled)
/// 4. If transitioning to scheduled, call advance_attached_function and scout_logs to check if we should skip
/// 5. Creating the output collection if it doesn't exist
///
/// # Parameters
/// - `sysdb`: The sysdb client
/// - `log`: The log client for scout_logs
/// - `attached_function_uuid`: The UUID of the attached function
///
/// # Inputs
/// - `nonce`: The invocation nonce from the scheduler
///
/// # Outputs
/// - The attached function object with updated state, execution_nonce, and a flag indicating whether to skip execution
#[derive(Clone, Debug)]
pub struct PrepareAttachedFunctionOperator {
    pub sysdb: SysDb,
    pub log: Log,
    pub attached_function_uuid: chroma_types::AttachedFunctionUuid,
}

#[derive(Clone, Debug)]
pub struct PrepareAttachedFunctionInput {
    pub nonce: chroma_types::NonceUuid,
}

#[derive(Clone, Debug)]
pub struct PrepareAttachedFunctionOutput {
    /// The attached function object fetched from SysDB
    pub attached_function: AttachedFunction,
    /// The nonce to use for this attached function execution
    pub execution_nonce: chroma_types::NonceUuid,
    /// If true, skip execution and go directly to FinishAttachedFunction
    /// This happens when there aren't enough new records to process
    pub should_skip_execution: bool,
    /// The output collection ID (created if it didn't exist)
    pub output_collection_id: CollectionUuid,
}

#[derive(Debug, Error)]
pub enum PrepareAttachedFunctionError {
    #[error(" Attached Function not found in SysDB")]
    AttachedFunctionNotFound,
    #[error("Failed to get attached function: {0}")]
    GetAttachedFunction(#[from] GetAttachedFunctionError),
    #[error("Failed to create output collection for attached function: {0}")]
    CreateOutputCollectionForAttachedFunction(
        #[from] CreateOutputCollectionForAttachedFunctionError,
    ),
    #[error("Invalid nonce: provided={provided}, expected next={expected_next} or lowest={expected_lowest}")]
    InvalidNonce {
        provided: chroma_types::NonceUuid,
        expected_next: chroma_types::NonceUuid,
        expected_lowest: chroma_types::NonceUuid,
    },
    #[error("Failed to advance attached function: {0}")]
    AdvanceAttachedFunction(#[from] AdvanceAttachedFunctionError),
    #[error("Failed to scout logs: {0}")]
    ScoutLogs(String),
    #[error("Invariant violation: {0}")]
    InvariantViolation(String),
}

impl ChromaError for PrepareAttachedFunctionError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            PrepareAttachedFunctionError::AttachedFunctionNotFound => {
                chroma_error::ErrorCodes::NotFound
            }
            PrepareAttachedFunctionError::GetAttachedFunction(e) => e.code(),
            PrepareAttachedFunctionError::CreateOutputCollectionForAttachedFunction(e) => e.code(),
            PrepareAttachedFunctionError::InvalidNonce { .. } => {
                chroma_error::ErrorCodes::InvalidArgument
            }
            PrepareAttachedFunctionError::AdvanceAttachedFunction(e) => e.code(),
            PrepareAttachedFunctionError::ScoutLogs(_) => chroma_error::ErrorCodes::Internal,
            PrepareAttachedFunctionError::InvariantViolation(_) => {
                chroma_error::ErrorCodes::Internal
            }
        }
    }
}

#[async_trait]
impl Operator<PrepareAttachedFunctionInput, PrepareAttachedFunctionOutput>
    for PrepareAttachedFunctionOperator
{
    type Error = PrepareAttachedFunctionError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &PrepareAttachedFunctionInput,
    ) -> Result<PrepareAttachedFunctionOutput, PrepareAttachedFunctionError> {
        tracing::info!(
            "[{}]: Preparing attached function {} with nonce {}",
            self.get_name(),
            self.attached_function_uuid.0,
            input.nonce
        );

        let mut sysdb = self.sysdb.clone();
        let mut log = self.log.clone();

        // 1. Fetch the attached function from SysDB using UUID
        let mut attached_function = sysdb
            .get_attached_function_by_uuid(self.attached_function_uuid)
            .await
            .map_err(|e| match e {
                GetAttachedFunctionError::NotFound => {
                    PrepareAttachedFunctionError::AttachedFunctionNotFound
                }
                other => PrepareAttachedFunctionError::GetAttachedFunction(other),
            })?;

        tracing::debug!(
            "[{}]: Retrieved attached function {} - next_nonce={}, lowest_live_nonce={:?}",
            self.get_name(),
            attached_function.name,
            attached_function.next_nonce,
            attached_function.lowest_live_nonce
        );

        // 2. ASSERT: nonce must match either next_nonce or lowest_live_nonce
        let matches_lowest = attached_function.lowest_live_nonce == Some(input.nonce);
        if input.nonce != attached_function.next_nonce && !matches_lowest {
            tracing::error!(
                "[{}]: Invalid nonce for attached function {} - provided={}, expected next={} or lowest={:?}",
                self.get_name(),
                attached_function.name,
                input.nonce,
                attached_function.next_nonce,
                attached_function.lowest_live_nonce
            );
            return Err(PrepareAttachedFunctionError::InvalidNonce {
                provided: input.nonce,
                expected_next: attached_function.next_nonce,
                expected_lowest: attached_function.lowest_live_nonce.unwrap_or_default(),
            });
        }

        let execution_nonce = match attached_function.lowest_live_nonce {
            Some(nonce) => nonce,
            None => {
                return Err(PrepareAttachedFunctionError::InvariantViolation(format!(
                    "Attached function {} has no lowest_live_nonce",
                    attached_function.name
                )));
            }
        };

        // Scout logs to see if we should skip execution (task may have already executed)
        let next_log_offset = log
            .scout_logs(
                &attached_function.tenant_id,
                attached_function.input_collection_id,
                attached_function.completion_offset,
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    "[{}]: Failed to scout logs for attached function {}: {}",
                    self.get_name(),
                    attached_function.name,
                    e
                );
                PrepareAttachedFunctionError::ScoutLogs(format!("Failed to scout logs: {}", e))
            })?;

        // 3. Determine state transition and whether to skip execution
        let new_records_count = next_log_offset.saturating_sub(attached_function.completion_offset);
        let should_skip_execution =
            new_records_count < attached_function.min_records_for_invocation;

        if should_skip_execution {
            tracing::info!(
                "[{}]: Skipping execution for attached function {} - not enough new records (new={}, min={})",
                self.get_name(),
                attached_function.name,
                new_records_count,
                attached_function.min_records_for_invocation
            );
        } else {
            tracing::info!(
                "[{}]: Attached function {} will proceed with execution ({} new records available)",
                self.get_name(),
                attached_function.name,
                new_records_count
            );
        }

        if attached_function
            .lowest_live_nonce
            .is_some_and(|lln| attached_function.next_nonce == lln)
        {
            // Currently **waiting**, transition to **scheduled**
            tracing::info!(
                "[{}]: AttachedFunction {} transitioning from waiting to scheduled",
                self.get_name(),
                attached_function.name
            );

            // Call advance_attached_function to increment next_nonce and set next_run (with nonce check for concurrency safety)
            // Set next_run to some reasonable delay (e.g., 60 seconds) since we're starting work
            const DEFAULT_THROTTLE_INTERVAL_SECS: u64 = 60;
            let advance_response = sysdb
                .advance_attached_function(
                    attached_function.id,
                    input.nonce.0,
                    attached_function.completion_offset,
                    DEFAULT_THROTTLE_INTERVAL_SECS, // Set next_run since we're advancing nonce
                )
                .await?;

            tracing::debug!(
                "[{}]: Advanced attached function {} - new next_nonce={}",
                self.get_name(),
                attached_function.name,
                advance_response.next_nonce
            );

            // Update attached function with the new nonce values
            attached_function.next_nonce = chroma_types::NonceUuid(advance_response.next_nonce);
            attached_function.next_run = advance_response.next_run;
        }

        // 4. Create output collection if it doesn't exist
        let output_collection_id = if let Some(output_id) = attached_function.output_collection_id {
            // Output collection already exists
            output_id
        } else {
            // Create new output collection atomically with attached function update
            tracing::info!(
                "[{}]: Creating output collection '{}' for attached function {}",
                self.get_name(),
                attached_function.output_collection_name,
                attached_function.name
            );

            let collection_id = sysdb
                .create_output_collection_for_attached_function(
                    attached_function.id,
                    attached_function.output_collection_name.clone(),
                    attached_function.tenant_id.clone(),
                    attached_function.database_id.clone(),
                )
                .await?;

            // Update local attached function object with the new output collection ID
            attached_function.output_collection_id = Some(collection_id);

            collection_id
        };

        Ok(PrepareAttachedFunctionOutput {
            attached_function: attached_function.clone(),
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
    async fn test_k8s_integration_prepare_attached_function_invalid_nonce() {
        // Setup
        let mut sysdb = get_grpc_sysdb().await;
        let log = Log::InMemory(InMemoryLog::new());

        let input_collection_id =
            setup_tenant_and_database(&mut sysdb, "test_tenant", "test_db").await;

        // Attach a function via SysDB with unique name
        let attached_function_id = sysdb
            .create_attached_function(
                format!("test_attached_function_{}", Uuid::new_v4()),
                "record_counter".to_string(),
                input_collection_id,
                format!("test_output_{}", Uuid::new_v4()),
                serde_json::Value::Null,
                "test_tenant".to_string(),
                "test_db".to_string(),
                10, // min_records_for_invocation
            )
            .await
            .unwrap();

        // Get the attached function to find its next_nonce
        let attached_function = sysdb
            .get_attached_function_by_uuid(attached_function_id)
            .await
            .unwrap();

        // Create operator
        let operator = PrepareAttachedFunctionOperator {
            sysdb: sysdb.clone(),
            log: log.clone(),
            attached_function_uuid: attached_function_id,
        };

        // Try to run with an invalid nonce (not matching next_nonce or lowest_live_nonce)
        let wrong_nonce = NonceUuid(Uuid::new_v4());
        let input = PrepareAttachedFunctionInput { nonce: wrong_nonce };

        // Run - should fail with InvalidNonce
        let result = operator.run(&input).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            PrepareAttachedFunctionError::InvalidNonce {
                provided,
                expected_next,
                ..
            } => {
                assert_eq!(provided, wrong_nonce);
                assert_eq!(expected_next, attached_function.next_nonce);
            }
            _ => panic!("Expected InvalidNonce error"),
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_prepare_attached_function_valid_next_nonce_transitions_to_scheduled(
    ) {
        // Setup
        let mut sysdb = get_grpc_sysdb().await;
        let log = Log::InMemory(InMemoryLog::new());

        let input_collection_id =
            setup_tenant_and_database(&mut sysdb, "test_tenant", "test_db").await;

        // Attach a function
        let attached_function_id = sysdb
            .create_attached_function(
                format!("test_attached_function_{}", Uuid::new_v4()),
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

        // Get the attached function's next_nonce
        let attached_function_before = sysdb
            .get_attached_function_by_uuid(attached_function_id)
            .await
            .unwrap();
        let next_nonce = attached_function_before.next_nonce;
        sysdb
            .finish_attached_function(attached_function_id)
            .await
            .unwrap();

        // Create operator
        let operator = PrepareAttachedFunctionOperator {
            sysdb: sysdb.clone(),
            log: log.clone(),
            attached_function_uuid: attached_function_id,
        };

        // Run with valid next_nonce - should transition to scheduled
        let input = PrepareAttachedFunctionInput { nonce: next_nonce };
        let result = operator.run(&input).await;

        // Assert: Operation succeeded
        assert!(result.is_ok());
        let output = result.unwrap();

        // Assert: execution_nonce matches input
        assert_eq!(output.execution_nonce, next_nonce);

        // Assert:  Attached function was advanced - next_nonce should have changed
        let attached_function_after = sysdb
            .get_attached_function_by_uuid(attached_function_id)
            .await
            .unwrap();
        assert_ne!(
            attached_function_after.next_nonce,
            attached_function_before.next_nonce
        );

        // Assert: lowest_live_nonce should now be set to the nonce we used
        assert_eq!(attached_function_after.lowest_live_nonce, Some(next_nonce));
    }

    #[tokio::test]
    async fn test_k8s_integration_prepare_attached_function_with_lowest_live_nonce_skips_execution()
    {
        // Setup:  Attached function that's already scheduled (lowest_live_nonce exists and != next_nonce)
        let mut sysdb = get_grpc_sysdb().await;
        let log = Log::InMemory(InMemoryLog::new());

        let input_collection_id =
            setup_tenant_and_database(&mut sysdb, "test_tenant", "test_db").await;

        // Attach a function
        let attached_function_id = sysdb
            .create_attached_function(
                format!("test_attached_function_{}", Uuid::new_v4()),
                "record_counter".to_string(),
                input_collection_id,
                format!("test_output_{}", Uuid::new_v4()),
                serde_json::Value::Null,
                "test_tenant".to_string(),
                "test_db".to_string(),
                10, // min_records_for_invocation = 10
            )
            .await
            .unwrap();
        sysdb
            .finish_attached_function(attached_function_id)
            .await
            .unwrap();

        // Advance the attached function once to set lowest_live_nonce
        let attached_function_initial = sysdb
            .get_attached_function_by_uuid(attached_function_id)
            .await
            .unwrap();
        let first_nonce = attached_function_initial.next_nonce;

        // Now lowest_live_nonce = first_nonce, next_nonce = new value
        // No new log records, so should skip execution

        // Create operator
        let operator = PrepareAttachedFunctionOperator {
            sysdb: sysdb.clone(),
            log: log.clone(),
            attached_function_uuid: attached_function_id,
        };

        // Run with the lowest_live_nonce (incomplete nonce)
        let input = PrepareAttachedFunctionInput { nonce: first_nonce };
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
    async fn test_k8s_integration_prepare_attached_function_with_lowest_live_nonce_and_new_records()
    {
        // Setup:  Attached Function that's already scheduled with new records available
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

        // Attach a function
        let attached_function_id = sysdb
            .create_attached_function(
                format!("test_attached_function_{}", Uuid::new_v4()),
                "record_counter".to_string(),
                input_collection_id,
                format!("test_output_{}", Uuid::new_v4()),
                serde_json::Value::Null,
                "test_tenant".to_string(),
                "test_db".to_string(),
                10, // min_records_for_invocation = 10
            )
            .await
            .unwrap();
        sysdb
            .finish_attached_function(attached_function_id)
            .await
            .unwrap();

        // Advance the attached function to create lowest_live_nonce
        let attached_function_initial = sysdb
            .get_attached_function_by_uuid(attached_function_id)
            .await
            .unwrap();
        let first_nonce = attached_function_initial.next_nonce;

        sysdb
            .advance_attached_function(attached_function_id, first_nonce.0, 0, 60)
            .await
            .unwrap();

        // Create operator
        let operator = PrepareAttachedFunctionOperator {
            sysdb: sysdb.clone(),
            log: log.clone(),
            attached_function_uuid: attached_function_id,
        };

        // Run with the lowest_live_nonce
        let input = PrepareAttachedFunctionInput { nonce: first_nonce };
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
    async fn test_k8s_integration_prepare_attached_function_creates_output_collection() {
        // Setup
        let mut sysdb = get_grpc_sysdb().await;
        let log = Log::InMemory(InMemoryLog::new());

        let input_collection_id =
            setup_tenant_and_database(&mut sysdb, "test_tenant", "test_db").await;

        // Attach a function (output collection doesn't exist yet)
        let attached_function_id = sysdb
            .create_attached_function(
                format!("test_attached_function_{}", Uuid::new_v4()),
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

        let attached_function_before = sysdb
            .get_attached_function_by_uuid(attached_function_id)
            .await
            .unwrap();
        let next_nonce = attached_function_before.next_nonce;

        // Verify output collection doesn't exist yet
        assert_eq!(attached_function_before.output_collection_id, None);

        // Create operator
        let operator = PrepareAttachedFunctionOperator {
            sysdb: sysdb.clone(),
            log: log.clone(),
            attached_function_uuid: attached_function_id,
        };

        // Run
        let input = PrepareAttachedFunctionInput { nonce: next_nonce };
        let result = operator.run(&input).await;

        // Assert: Operation succeeded
        assert!(result.is_ok());
        let output = result.unwrap();

        // Assert: output_collection_id was created and returned
        // Verify output collection was created (not all zeros)
        assert_ne!(output.output_collection_id, CollectionUuid::new());

        // Assert:  Attached Function now has the output_collection_id set
        assert_eq!(
            output.attached_function.output_collection_id,
            Some(output.output_collection_id)
        );
    }
}
