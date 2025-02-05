//! Garbage Collection Pipeline
//!
//! The garbage collection process follows these stages:
//!
//! 1. Fetch Version File (FetchVersionFileOperator)
//!    - Retrieves the collection version file from storage
//!    - Input: Version file path
//!    - Output: Version file content
//!
//! 2. Compute Versions to Delete (ComputeVersionsToDeleteOperator)
//!    - Identifies versions older than cutoff time while preserving minimum required versions
//!    - Input: Version file, cutoff time, minimum versions to keep
//!    - Output: List of versions to delete
//!
//! 3. Mark Versions at SysDB (MarkVersionsAtSysDbOperator)
//!    - Marks identified versions for deletion in the system database
//!    - Input: Version file, versions to delete, epoch ID
//!    - Output: Marked versions confirmation
//!
//! 4. Fetch Sparse Index Files (FetchSparseIndexFilesOperator)
//!    - Retrieves sparse index files for versions marked for deletion
//!    - Input: Version file, versions to delete
//!    - Output: Map of version IDs to file contents
//!
//! 5. Compute Unused Files (ComputeUnusedBetweenVersionsOperator)
//!    - Analyzes sparse index files to identify S3 files no longer referenced
//!    - Input: Version file, version contents
//!    - Output: Set of unused S3 file paths
//!
//! 6. Delete Versions (DeleteVersionsAtSysDbOperator)
//!    - Permanently deletes marked versions from the system database
//!    - Input: Version file, versions to delete, unused S3 files
//!    - Output: Deletion confirmation

use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_sysdb::SysDb;
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::chroma_proto::CollectionVersionFile;
use chroma_types::CollectionUuid;
use chrono::{Duration, Utc};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};

use crate::fetch_version_file::{
    FetchVersionFileError, FetchVersionFileInput, FetchVersionFileOperator, FetchVersionFileOutput,
};
use crate::operators::compute_unused_between_versions::{
    ComputeUnusedBetweenVersionsError, ComputeUnusedBetweenVersionsInput,
    ComputeUnusedBetweenVersionsOperator, ComputeUnusedBetweenVersionsOutput,
};
use crate::operators::compute_versions_to_delete::{
    ComputeVersionsToDeleteError, ComputeVersionsToDeleteInput, ComputeVersionsToDeleteOperator,
    ComputeVersionsToDeleteOutput,
};
use crate::operators::delete_versions_at_sysdb::{
    DeleteVersionsAtSysDbError, DeleteVersionsAtSysDbInput, DeleteVersionsAtSysDbOperator,
    DeleteVersionsAtSysDbOutput,
};
use crate::operators::fetch_sparse_index_files::{
    FetchSparseIndexFilesError, FetchSparseIndexFilesInput, FetchSparseIndexFilesOperator,
    FetchSparseIndexFilesOutput,
};
use crate::operators::mark_versions_at_sysdb::{
    MarkVersionsAtSysDbError, MarkVersionsAtSysDbInput, MarkVersionsAtSysDbOperator,
    MarkVersionsAtSysDbOutput,
};

use prost::Message;

pub struct GarbageCollectorOrchestrator {
    collection_id: CollectionUuid,
    version_file_path: String,
    cutoff_time_hours: u32,
    sysdb_client: SysDb,
    dispatcher: ComponentHandle<Dispatcher>,
    storage: Storage,
    result_channel: Option<Sender<Result<GarbageCollectorResponse, GarbageCollectorError>>>,
}

impl Debug for GarbageCollectorOrchestrator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GarbageCollector").finish()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct GarbageCollectorResponse {
    collection_id: CollectionUuid,
    version_file_path: String,
}

impl GarbageCollectorOrchestrator {
    pub fn new(
        collection_id: CollectionUuid,
        version_file_path: String,
        cutoff_time_hours: u32,
        sysdb_client: SysDb,
        dispatcher: ComponentHandle<Dispatcher>,
        storage: Storage,
    ) -> Self {
        Self {
            collection_id,
            version_file_path,
            cutoff_time_hours,
            sysdb_client,
            dispatcher,
            storage,
            result_channel: None,
        }
    }
}

#[derive(Error, Debug)]
pub enum GarbageCollectorError {
    #[error("FetchVersionFile error: {0}")]
    FetchVersionFile(#[from] FetchVersionFileError),
    #[error("Panic during compaction: {0}")]
    Panic(#[from] PanicError),
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("{0}")]
    Generic(#[from] Box<dyn ChromaError>),
    #[error("ComputeVersionsToDelete error: {0}")]
    ComputeVersionsToDelete(#[from] ComputeVersionsToDeleteError),
    #[error("MarkVersionsAtSysDb error: {0}")]
    MarkVersionsAtSysDb(#[from] MarkVersionsAtSysDbError),
    #[error("FetchSparseIndexFiles error: {0}")]
    FetchSparseIndexFiles(#[from] FetchSparseIndexFilesError),
    #[error("ComputeUnusedBetweenVersions error: {0}")]
    ComputeUnusedBetweenVersions(#[from] ComputeUnusedBetweenVersionsError),
    #[error("DeleteVersionsAtSysDb error: {0}")]
    DeleteVersionsAtSysDb(#[from] DeleteVersionsAtSysDbError),
    #[error("The task was aborted because resources were exhausted")]
    Aborted,
}

impl ChromaError for GarbageCollectorError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

impl<E> From<TaskError<E>> for GarbageCollectorError
where
    E: Into<GarbageCollectorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => GarbageCollectorError::Panic(e),
            TaskError::TaskFailed(e) => e.into(),
            TaskError::Aborted => GarbageCollectorError::Aborted,
        }
    }
}

#[async_trait]
impl Orchestrator for GarbageCollectorOrchestrator {
    type Output = GarbageCollectorResponse;
    type Error = GarbageCollectorError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    fn initial_tasks(&self, ctx: &ComponentContext<Self>) -> Vec<TaskMessage> {
        vec![wrap(
            Box::new(FetchVersionFileOperator {}),
            FetchVersionFileInput {
                version_file_path: self.version_file_path.clone(),
                storage: self.storage.clone(),
            },
            ctx.receiver(),
        )]
    }

    fn set_result_channel(
        &mut self,
        sender: Sender<Result<GarbageCollectorResponse, GarbageCollectorError>>,
    ) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(
        &mut self,
    ) -> Sender<Result<GarbageCollectorResponse, GarbageCollectorError>> {
        self.result_channel
            .take()
            .expect("The result channel should be set before take")
    }
}

#[async_trait]
impl Handler<TaskResult<FetchVersionFileOutput, FetchVersionFileError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchVersionFileOutput, FetchVersionFileError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        // Stage 1: Process fetched version file and initiate version computation
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        let cutoff_time = Utc::now() - Duration::hours(self.cutoff_time_hours as i64);

        let version_file =
            match CollectionVersionFile::decode(output.version_file_content().as_bytes()) {
                Ok(file) => file,
                Err(e) => {
                    let result: Result<FetchVersionFileOutput, GarbageCollectorError> =
                        Err(GarbageCollectorError::ComputeVersionsToDelete(
                            ComputeVersionsToDeleteError::ParseError(e),
                        ));
                    self.ok_or_terminate(result, ctx);
                    return;
                }
            };

        let compute_task = wrap(
            Box::new(ComputeVersionsToDeleteOperator {}),
            ComputeVersionsToDeleteInput {
                version_file,
                cutoff_time,
                min_versions_to_keep: 2,
            },
            ctx.receiver(),
        );

        if let Err(e) = self.dispatcher().send(compute_task, None).await {
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx);
            return;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<ComputeVersionsToDeleteOutput, ComputeVersionsToDeleteError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ComputeVersionsToDeleteOutput, ComputeVersionsToDeleteError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        // Stage 2: Process computed versions and initiate marking in SysDB
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        let mark_task = wrap(
            Box::new(MarkVersionsAtSysDbOperator {}),
            MarkVersionsAtSysDbInput {
                version_file: output.version_file,
                versions_to_delete: output.versions_to_delete,
                sysdb_client: self.sysdb_client.clone(),
                epoch_id: 0,
            },
            ctx.receiver(),
        );

        if let Err(e) = self.dispatcher().send(mark_task, None).await {
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx);
            return;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<MarkVersionsAtSysDbOutput, MarkVersionsAtSysDbError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<MarkVersionsAtSysDbOutput, MarkVersionsAtSysDbError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        // Stage 3: After marking versions, fetch their sparse index files
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        let fetch_task = wrap(
            Box::new(FetchSparseIndexFilesOperator {
                storage: self.storage.clone(),
            }),
            FetchSparseIndexFilesInput {
                version_file: output.version_file,
                epoch_id: output.epoch_id,
                sysdb_client: output.sysdb_client,
                versions_to_delete: output.versions_to_delete,
            },
            ctx.receiver(),
        );

        if let Err(e) = self.dispatcher().send(fetch_task, None).await {
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx);
            return;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<FetchSparseIndexFilesOutput, FetchSparseIndexFilesError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchSparseIndexFilesOutput, FetchSparseIndexFilesError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        // Stage 4: Process fetched sparse index files and compute unused files
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        let input = ComputeUnusedBetweenVersionsInput {
            version_file: output.version_file,
            epoch_id: output.epoch_id,
            sysdb_client: self.sysdb_client.clone(),
            versions_to_delete: output.versions_to_delete,
            version_to_content: output.version_to_content,
        };

        let compute_task = wrap(
            Box::new(ComputeUnusedBetweenVersionsOperator::new(
                self.storage.clone(),
            )),
            input,
            ctx.receiver(),
        );

        if let Err(e) = self.dispatcher().send(compute_task, None).await {
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx);
            return;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<ComputeUnusedBetweenVersionsOutput, ComputeUnusedBetweenVersionsError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ComputeUnusedBetweenVersionsOutput, ComputeUnusedBetweenVersionsError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        // Stage 5: After identifying unused files, initiate version deletion
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        let delete_task = wrap(
            Box::new(DeleteVersionsAtSysDbOperator {}),
            DeleteVersionsAtSysDbInput {
                version_file: output.version_file,
                epoch_id: output.epoch_id,
                sysdb_client: self.sysdb_client.clone(),
                versions_to_delete: output.versions_to_delete,
                unused_s3_files: output.unused_s3_files,
            },
            ctx.receiver(),
        );

        if let Err(e) = self.dispatcher().send(delete_task, None).await {
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx);
            return;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<DeleteVersionsAtSysDbOutput, DeleteVersionsAtSysDbError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<DeleteVersionsAtSysDbOutput, DeleteVersionsAtSysDbError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        // Stage 6: Final stage - versions deleted, complete the garbage collection process
        let _output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        let response = GarbageCollectorResponse {
            collection_id: self.collection_id,
            version_file_path: self.version_file_path.clone(),
        };

        self.terminate_with_result(Ok(response), ctx);
    }
}

#[cfg(test)]
mod tests {
    use crate::helper::ChromaGrpcClients;
    use chroma_types::chroma_proto::ListCollectionVersionsRequest;
    use std::time::Duration;
    use tracing_subscriber;

    // Add this helper function inside the tests module
    async fn wait_for_new_version(
        clients: &mut ChromaGrpcClients,
        collection_id: &str,
        tenant_id: &str,
        current_version_count: usize,
        max_attempts: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for attempt in 1..=max_attempts {
            tracing::info!(
                attempt,
                max_attempts,
                "Waiting for new version to be created..."
            );

            tokio::time::sleep(Duration::from_secs(2)).await;

            let versions = clients
                .list_collection_versions(collection_id, tenant_id, Some(100), None, None)
                .await?;

            if versions.versions.len() > current_version_count {
                tracing::info!(
                    previous_count = current_version_count,
                    new_count = versions.versions.len(),
                    "New version detected"
                );
                return Ok(());
            }
        }

        Err("Timeout waiting for new version to be created".into())
    }

    #[tokio::test]
    async fn test_direct_service_calls() -> Result<(), Box<dyn std::error::Error>> {
        tracing_subscriber::fmt::init();
        let mut clients = ChromaGrpcClients::new().await.map_err(|e| {
            tracing::error!(error = ?e, "Failed to create ChromaGrpcClients");
            e
        })?;

        // Create unique identifiers for tenant and database
        let test_uuid = uuid::Uuid::new_v4();
        let tenant_id = format!("test_tenant_{}", test_uuid);
        let database_name = format!("test_db_{}", test_uuid);
        let collection_name = format!("test_collection_{}", test_uuid);

        tracing::info!(
            tenant_id = %tenant_id,
            database = %database_name,
            collection = %collection_name,
            "Starting test with resources"
        );

        let collection_id = clients
            .create_database_and_collection(&tenant_id, &database_name, &collection_name)
            .await
            .map_err(|e| {
                tracing::error!(
                    error = ?e,
                    tenant_id = %tenant_id,
                    database = %database_name,
                    collection = %collection_name,
                    "Failed to create database and collection"
                );
                e
            })?;

        tracing::info!(collection_id = %collection_id, "Created collection");

        // Create 22 records
        let mut embeddings = Vec::with_capacity(22);
        let mut ids = Vec::with_capacity(22);

        for i in 0..22 {
            let mut embedding = vec![0.0; 3];
            embedding[i % 3] = 1.0;
            embeddings.push(embedding);
            ids.push(format!("id{}", i));
        }

        // Get initial version count
        let initial_versions = clients
            .list_collection_versions(&collection_id, &tenant_id, Some(100), None, None)
            .await?;
        let initial_version_count = initial_versions.versions.len();

        tracing::info!(
            initial_count = initial_version_count,
            "Initial version count"
        );

        // Add first batch of 11 records
        tracing::info!("Adding first batch of embeddings");
        clients
            .add_embeddings(
                &collection_id,
                embeddings[..11].to_vec(),
                ids[..11].to_vec(),
            )
            .await?;

        // Wait for new version after first batch
        wait_for_new_version(
            &mut clients,
            &collection_id,
            &tenant_id,
            initial_version_count,
            10,
        )
        .await?;

        // Add second batch of 11 records
        tracing::info!("Adding second batch of embeddings");
        clients
            .add_embeddings(
                &collection_id,
                embeddings[11..].to_vec(),
                ids[11..].to_vec(),
            )
            .await?;

        // Get current version count and wait for it to increase
        let mid_versions = clients
            .list_collection_versions(&collection_id, &tenant_id, Some(100), None, None)
            .await?;
        wait_for_new_version(
            &mut clients,
            &collection_id,
            &tenant_id,
            mid_versions.versions.len(),
            10,
        )
        .await?;

        // Get records from the collection
        tracing::info!(collection_id = %collection_id, "Getting records from collection");

        let results = clients
            .get_records(
                &collection_id,
                None,
                true,
                false,
                false,
            )
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, collection_id = %collection_id, "Failed to get records");
                e
            })?;

        // Verify results
        tracing::info!(
            num_results = results.ids.len(),
            "Get records results received"
        );
        assert_eq!(results.ids.len(), 22, "Expected 22 results");

        // Verify all IDs are present
        for i in 0..22 {
            let expected_id = format!("id{}", i);
            assert!(
                results.ids.contains(&expected_id),
                "Expected to find {}",
                expected_id
            );
        }

        // Verify embeddings
        if let Some(returned_embeddings) = results.embeddings {
            assert_eq!(returned_embeddings.len(), 22, "Expected 22 embeddings");

            for (i, embedding) in returned_embeddings.iter().enumerate() {
                let expected_index = ids
                    .iter()
                    .position(|id| id == &format!("id{}", i))
                    .expect("ID should exist");
                assert_eq!(
                    embedding, &embeddings[expected_index],
                    "Embedding mismatch for id{}",
                    i
                );
            }
        } else {
            panic!("Expected embeddings in results");
        }

        // Get final versions
        tracing::info!(collection_id = %collection_id, "Requesting final collection versions");

        let versions_response = clients
            .list_collection_versions(&collection_id, &tenant_id, Some(10), None, None)
            .await?;

        tracing::info!("Collection versions:");
        for version in versions_response.versions {
            tracing::info!(
                version = version.version,
                created_at = version.created_at_secs,
                change_reason = ?version.version_change_reason,
                marked_for_deletion = version.marked_for_deletion,
                "Version info"
            );

            if let Some(collection_info) = version.collection_info_mutable {
                tracing::info!(
                    log_position = collection_info.current_log_position,
                    collection_version = collection_info.current_collection_version,
                    last_compaction = collection_info.last_compaction_time_secs,
                    dimension = collection_info.dimension,
                    "Collection mutable info"
                );
            }

            if let Some(segment_info) = version.segment_info {
                tracing::info!(
                    num_segments = segment_info.segment_compaction_info.len(),
                    "Segment info"
                );
            }
        }

        tracing::info!(
            is_truncated = versions_response.list_is_truncated,
            "Version list complete"
        );

        Ok(())
    }
}
