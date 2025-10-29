use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_sysdb::SysDb;
use chroma_system::Operator;
use chroma_types::{
    AttachedFunction, AttachedFunctionUuid,
    FinishAttachedFunctionError as SysDbFinishAttachedFunctionError,
};
use thiserror::Error;

/// The finish attached function operator is responsible for updating attached function state in SysDB
/// after a successful attached function execution run.
#[derive(Debug, Clone)]
pub struct FinishAttachedFunctionOperator {
    log_client: Log,
    sysdb: SysDb,
    heap_service: s3heap_service::client::GrpcHeapService,
}

impl FinishAttachedFunctionOperator {
    /// Create a new finish attached function operator.
    ///
    /// # Parameters
    /// * `log_client` - Log client for scouting log records
    /// * `sysdb` - SysDB client for attached function state management
    /// * `heap_service` - Heap service client for scheduling next attached function runs (required)
    #[allow(dead_code)]
    pub fn new(
        log_client: Log,
        sysdb: SysDb,
        heap_service: s3heap_service::client::GrpcHeapService,
    ) -> Box<Self> {
        Box::new(FinishAttachedFunctionOperator {
            log_client,
            sysdb,
            heap_service,
        })
    }
}

#[derive(Debug)]
/// The input for the finish attached function operator.
/// # Parameters
/// * `updated_attached_function` - The updated attached function record from sysdb.
/// * `records_processed` - The number of records processed in this run.
/// * `sysdb` - The sysdb client.
pub struct FinishAttachedFunctionInput {
    // Updated  Attached Function record from sysdb
    updated_attached_function: AttachedFunction,
}

impl FinishAttachedFunctionInput {
    /// Create a new finish attached function input.
    pub fn new(updated_attached_function: AttachedFunction) -> Self {
        FinishAttachedFunctionInput {
            updated_attached_function,
        }
    }
}

/// The output for the finish attached function operator.
#[derive(Debug)]
pub struct FinishAttachedFunctionOutput {
    pub _attached_function_id: AttachedFunctionUuid,
    pub _new_completion_offset: u64,
}

#[derive(Error, Debug)]
pub enum FinishAttachedFunctionError {
    #[error("Failed to scout logs: {0}")]
    ScoutLogs(String),
    #[error("Failed to finish attached function in SysDB: {0}")]
    SysDb(#[from] SysDbFinishAttachedFunctionError),
    #[error("Failed to schedule attached function in heap service: {0}")]
    HeapService(#[from] s3heap_service::client::GrpcHeapServiceError),
}

impl ChromaError for FinishAttachedFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            FinishAttachedFunctionError::ScoutLogs(_) => ErrorCodes::Internal,
            FinishAttachedFunctionError::SysDb(e) => e.code(),
            FinishAttachedFunctionError::HeapService(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<FinishAttachedFunctionInput, FinishAttachedFunctionOutput>
    for FinishAttachedFunctionOperator
{
    type Error = FinishAttachedFunctionError;

    fn get_name(&self) -> &'static str {
        "FinishAttachedFunctionOperator"
    }

    async fn run(
        &self,
        input: &FinishAttachedFunctionInput,
    ) -> Result<FinishAttachedFunctionOutput, FinishAttachedFunctionError> {
        // Step 1: Scout the logs to see if there are any new records written since we started processing
        // This recheck ensures we don't miss any records that were written during our attached function execution
        tracing::info!(
            "Rechecking logs for attached function {} with completion offset {}",
            input.updated_attached_function.id.0,
            input.updated_attached_function.completion_offset
        );

        // Scout the logs to check for new records written since we started processing
        // This catches any records that were written during our attached function execution
        // scout_logs returns the offset of the next record to be inserted
        let mut log_client = self.log_client.clone();
        let next_log_offset = log_client
            .scout_logs(
                &input.updated_attached_function.tenant_id,
                input.updated_attached_function.input_collection_id,
                input.updated_attached_function.completion_offset,
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    attached_function_id = %input.updated_attached_function.id.0,
                    error = %e,
                    "Failed to scout logs during finish_attached_function recheck"
                );
                FinishAttachedFunctionError::ScoutLogs(format!("Failed to scout logs: {}", e))
            })?;

        // Calculate how many new records were written since we started processing
        let new_records_count =
            next_log_offset.saturating_sub(input.updated_attached_function.completion_offset);
        let new_records_found =
            new_records_count >= input.updated_attached_function.min_records_for_invocation;

        if new_records_found {
            tracing::info!(
                attached_function_id = %input.updated_attached_function.id.0,
                new_records_count = new_records_count,
                min_records_threshold = input.updated_attached_function.min_records_for_invocation,
                "Detected new records written during attached function execution that exceed threshold"
            );

            // Schedule a new attached function run for next nonce by pushing to the heap
            let mut heap_service = self.heap_service.clone();
            let schedule = chroma_types::chroma_proto::Schedule {
                triggerable: Some(chroma_types::chroma_proto::Triggerable {
                    partitioning_uuid: input
                        .updated_attached_function
                        .input_collection_id
                        .to_string(),
                    scheduling_uuid: input.updated_attached_function.id.0.to_string(),
                }),
                next_scheduled: Some(prost_types::Timestamp::from(
                    input.updated_attached_function.next_run,
                )),
                nonce: input.updated_attached_function.next_nonce.0.to_string(),
            };

            heap_service
                .push(
                    vec![schedule],
                    &input
                        .updated_attached_function
                        .input_collection_id
                        .to_string(),
                )
                .await?;

            tracing::info!(
                attached_function_id = %input.updated_attached_function.id.0,
                collection_id = %input.updated_attached_function.input_collection_id,
                next_nonce = %input.updated_attached_function.next_nonce.0,
                "Successfully scheduled next attached function run in heap"
            );
        }

        // Step 2: Update lowest_live_nonce to equal next_nonce
        // This indicates that finish_attached_function completed successfully and this epoch is verified
        // If this fails, lowest_live_nonce < next_nonce will indicate
        // that we should skip execution next time and only do the recheck phase
        let mut sysdb = self.sysdb.clone();
        sysdb
            .finish_attached_function(input.updated_attached_function.id)
            .await?;

        tracing::info!(
            " Attached Function {} finish_attached_function completed. lowest_live_nonce updated",
            input.updated_attached_function.id.0,
        );

        Ok(FinishAttachedFunctionOutput {
            _attached_function_id: input.updated_attached_function.id,
            _new_completion_offset: input.updated_attached_function.completion_offset,
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

    async fn get_test_heap_service() -> s3heap_service::client::GrpcHeapService {
        use chroma_system::System;

        let system = System::new();
        let registry = chroma_config::registry::Registry::default();
        let config = s3heap_service::client::GrpcHeapServiceConfig {
            enabled: true,
            port: 50052,
            connect_timeout_ms: 5000,
            request_timeout_ms: 5000,
            ..Default::default()
        };

        let port = config.port;
        s3heap_service::client::GrpcHeapService::try_from_config(
            &(config, system),
            &registry,
        )
        .await
        .unwrap_or_else(|_| {
            panic!("Failed to create test heap service client - ensure heap service is running on localhost:{}", port)
        })
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
    async fn test_k8s_integration_finish_attached_function_updates_lowest_live_nonce() {
        // Setup: Attach a function and advance it once
        let mut sysdb = get_grpc_sysdb().await;
        let log = Log::InMemory(InMemoryLog::new());

        let collection_id = setup_tenant_and_database(&mut sysdb, "test_tenant", "test_db").await;

        // Attach a function via SysDB
        let attached_function_id = sysdb
            .create_attached_function(
                format!("test_function_{}", Uuid::new_v4()),
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

        let heap_service = get_test_heap_service().await;
        let attached_function_initial = sysdb
            .get_attached_function_by_uuid(attached_function_id)
            .await
            .unwrap();

        let operator1 =
            FinishAttachedFunctionOperator::new(log.clone(), sysdb.clone(), heap_service.clone());
        let res1 = operator1
            .run(&FinishAttachedFunctionInput::new(
                attached_function_initial.clone(),
            ))
            .await;
        assert!(res1.is_ok());

        let attached_function_finished = sysdb
            .get_attached_function_by_uuid(attached_function_id)
            .await
            .unwrap();
        assert_eq!(
            attached_function_finished.lowest_live_nonce.unwrap(),
            attached_function_finished.next_nonce
        );

        let initial_nonce = attached_function_finished.next_nonce;

        // Advance the attached function once to set lowest_live_nonce
        sysdb
            .advance_attached_function(
                attached_function_id,
                attached_function_finished.next_nonce.0,
                0,
                60,
            )
            .await
            .unwrap();

        let attached_function_advanced = sysdb
            .get_attached_function_by_uuid(attached_function_id)
            .await
            .unwrap();

        // Verify: lowest_live_nonce is set, next_nonce has advanced
        assert_eq!(
            attached_function_advanced.lowest_live_nonce,
            Some(initial_nonce)
        );
        assert_ne!(attached_function_advanced.next_nonce, initial_nonce);

        let input = FinishAttachedFunctionInput::new(attached_function_advanced.clone());
        let operator =
            FinishAttachedFunctionOperator::new(log.clone(), sysdb.clone(), heap_service.clone());

        // Run finish_attached_function - should move lowest_live_nonce up to match next_nonce
        let result = operator.run(&input).await;

        // Assert: Operation succeeded
        assert!(result.is_ok());

        // Assert: lowest_live_nonce was updated to equal next_nonce
        let attached_function_after = sysdb
            .get_attached_function_by_uuid(attached_function_id)
            .await
            .unwrap();
        assert_eq!(
            attached_function_after.lowest_live_nonce,
            Some(attached_function_advanced.next_nonce)
        );
        assert_eq!(
            attached_function_after.next_nonce,
            attached_function_advanced.next_nonce
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_finish_attached_function_error_when_task_not_found() {
        // Setup: Use a task ID that doesn't exist
        let sysdb = get_grpc_sysdb().await;
        let log = Log::InMemory(InMemoryLog::new());

        let collection_id = CollectionUuid::new();

        // Create a fake attached function that's not in the database
        use chroma_types::{AttachedFunction, NonceUuid, FUNCTION_RECORD_COUNTER_ID};
        use std::time::SystemTime;

        let fake_attached_function = AttachedFunction {
            id: AttachedFunctionUuid(Uuid::new_v4()),
            name: "fake_function".to_string(),
            function_id: FUNCTION_RECORD_COUNTER_ID,
            input_collection_id: collection_id,
            output_collection_name: format!("test_output_{}", Uuid::new_v4()),
            output_collection_id: None,
            params: None,
            tenant_id: "test_tenant".to_string(),
            database_id: "test_db".to_string(),
            last_run: None,
            next_run: SystemTime::now(),
            completion_offset: 0,
            min_records_for_invocation: 10,
            is_deleted: false,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            next_nonce: NonceUuid(Uuid::new_v4()),
            lowest_live_nonce: None,
        };

        let input = FinishAttachedFunctionInput::new(fake_attached_function.clone());
        let heap_service = get_test_heap_service().await;
        let operator =
            FinishAttachedFunctionOperator::new(log.clone(), sysdb.clone(), heap_service);

        // Run
        let result = operator.run(&input).await;

        // Assert: Operation should fail with AttachedFunctionNotFound error
        assert!(result.is_err());
        match result.unwrap_err() {
            FinishAttachedFunctionError::SysDb(_) => { /* expected */ }
            _ => panic!("Expected SysDbError"),
        }
    }
}
