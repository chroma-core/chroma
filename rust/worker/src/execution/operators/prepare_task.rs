use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_log::Log;
use chroma_sysdb::{
    CreateOutputCollectionForAttachedFunctionError, GetAttachedFunctionError, SysDb,
};
use chroma_system::{Operator, OperatorType};
use chroma_types::chroma_proto::AttachedFunction;
use chroma_types::ListAttachedFunctionsError;
use chroma_types::{AdvanceAttachedFunctionError, CollectionUuid};
use thiserror::Error;
use uuid;

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
}

#[derive(Clone, Debug)]
pub struct PrepareAttachedFunctionInput {
    pub input_collection_id: CollectionUuid,
}

#[derive(Clone, Debug)]
pub struct PrepareAttachedFunctionOutput {
    /// The attached function object fetched from SysDB
    pub attached_function: AttachedFunction,
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
    #[error("No attached function found for collection")]
    NoAttachedFunctionFound,
    #[error("Failed to get attached function: {0}")]
    GetAttachedFunction(#[from] GetAttachedFunctionError),
    #[error("Failed to list attached functions: {0}")]
    ListAttachedFunctions(#[from] ListAttachedFunctionsError),
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
            PrepareAttachedFunctionError::NoAttachedFunctionFound => {
                chroma_error::ErrorCodes::NotFound
            }
            PrepareAttachedFunctionError::GetAttachedFunction(e) => e.code(),
            PrepareAttachedFunctionError::ListAttachedFunctions(e) => e.code(),
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
            "[{}]: Preparing attached function for input collection {}",
            self.get_name(),
            input.input_collection_id.0,
        );

        let mut sysdb = self.sysdb.clone();

        // 1. Fetch attached functions from SysDB for the collection
        let attached_functions = sysdb
            .list_attached_functions(input.input_collection_id)
            .await
            .map_err(PrepareAttachedFunctionError::ListAttachedFunctions)?;

        // 2. Handle zero or one attached function
        let attached_function = match attached_functions.len() {
            0 => {
                tracing::info!(
                    "[{}]: No attached function found for collection {}",
                    self.get_name(),
                    input.input_collection_id.0
                );
                return Err(PrepareAttachedFunctionError::NoAttachedFunctionFound);
            }
            1 => {
                let attached_function = attached_functions.into_iter().next().unwrap();
                tracing::debug!(
                    "[{}]: Retrieved attached function {} - next_nonce={}, lowest_live_nonce={:?}",
                    self.get_name(),
                    attached_function.name,
                    attached_function.next_nonce,
                    attached_function.lowest_live_nonce
                );
                attached_function
            }
            _ => {
                return Err(PrepareAttachedFunctionError::InvariantViolation(format!(
                    "Expected 0 or 1 attached functions for collection {}, found {}",
                    input.input_collection_id.0,
                    attached_functions.len()
                )));
            }
        };

        // 5. Create output collection if it doesn't exist
        let output_collection_id =
            if let Some(output_id) = attached_function.output_collection_id.clone() {
                // Parse output_collection_id from string to CollectionUuid
                CollectionUuid(uuid::Uuid::parse_str(&output_id).map_err(|e| {
                    tracing::error!(
                    "[{}]: Failed to parse output_collection_id '{}' for attached function {}: {}",
                    self.get_name(),
                    output_id,
                    attached_function.name,
                    e
                );
                    PrepareAttachedFunctionError::InvariantViolation(format!(
                        "Invalid output_collection_id: {}",
                        output_id
                    ))
                })?)
            } else {
                // Create new output collection atomically with attached function update

                return Err(PrepareAttachedFunctionError::InvariantViolation(
                    "Not output_collection_id".to_string(),
                ));
            };

        Ok(PrepareAttachedFunctionOutput {
            attached_function: attached_function.clone(),
            should_skip_execution: false,
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
