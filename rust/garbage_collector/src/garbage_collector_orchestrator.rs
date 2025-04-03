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
//! 6. Delete Unused Files (DeleteUnusedFilesOperator)
//!    - Deletes unused S3 files
//!    - Input: Set of unused S3 file paths
//!    - Output: Deletion confirmation
//!
//! 7. Delete Versions (DeleteVersionsAtSysDbOperator)
//!    - Permanently deletes marked versions from the system database
//!    - Input: Version file, versions to delete, unused S3 files
//!    - Output: Deletion confirmation

use std::fmt::{Debug, Formatter};

use crate::types::CleanupMode;
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

use crate::operators::compute_unused_files::{
    ComputeUnusedFilesError, ComputeUnusedFilesInput, ComputeUnusedFilesOperator,
    ComputeUnusedFilesOutput,
};
use crate::operators::compute_versions_to_delete::{
    ComputeVersionsToDeleteError, ComputeVersionsToDeleteInput, ComputeVersionsToDeleteOperator,
    ComputeVersionsToDeleteOutput,
};
use crate::operators::delete_unused_files::{
    DeleteUnusedFilesError, DeleteUnusedFilesInput, DeleteUnusedFilesOperator,
    DeleteUnusedFilesOutput,
};
use crate::operators::delete_versions_at_sysdb::{
    DeleteVersionsAtSysDbError, DeleteVersionsAtSysDbInput, DeleteVersionsAtSysDbOperator,
    DeleteVersionsAtSysDbOutput,
};
use crate::operators::fetch_version_file::{
    FetchVersionFileError, FetchVersionFileInput, FetchVersionFileOperator, FetchVersionFileOutput,
};
use crate::operators::mark_versions_at_sysdb::{
    MarkVersionsAtSysDbError, MarkVersionsAtSysDbInput, MarkVersionsAtSysDbOperator,
    MarkVersionsAtSysDbOutput,
};

use prost::Message;

pub struct GarbageCollectorOrchestrator {
    collection_id: CollectionUuid,
    version_file_path: String,
    // TODO(rohitcp): Remove this parameter.
    cutoff_time_hours: u32,
    // Absolute cutoff time in seconds.
    // Any version created before this time will be deleted unless retained by the min_versions_to_keep parameter.
    cutoff_time_secs: u64,
    sysdb_client: SysDb,
    dispatcher: ComponentHandle<Dispatcher>,
    storage: Storage,
    result_channel: Option<Sender<Result<GarbageCollectorResponse, GarbageCollectorError>>>,
    pending_version_file: Option<CollectionVersionFile>,
    pending_versions_to_delete: Option<chroma_types::chroma_proto::VersionListForCollection>,
    pending_epoch_id: Option<i64>,
    num_versions_deleted: u32,
    deletion_list: Vec<String>,
    cleanup_mode: CleanupMode,
}

impl Debug for GarbageCollectorOrchestrator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GarbageCollector").finish()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct GarbageCollectorResponse {
    pub collection_id: CollectionUuid,
    pub version_file_path: String,
    pub num_versions_deleted: u32,
    pub deletion_list: Vec<String>,
}

#[allow(clippy::too_many_arguments)]
impl GarbageCollectorOrchestrator {
    pub fn new(
        collection_id: CollectionUuid,
        version_file_path: String,
        cutoff_time_secs: u64,
        cutoff_time_hours: u32,
        sysdb_client: SysDb,
        dispatcher: ComponentHandle<Dispatcher>,
        storage: Storage,
        cleanup_mode: CleanupMode,
    ) -> Self {
        Self {
            collection_id,
            version_file_path,
            cutoff_time_hours,
            cutoff_time_secs,
            sysdb_client,
            dispatcher,
            storage,
            cleanup_mode,
            result_channel: None,
            pending_version_file: None,
            pending_versions_to_delete: None,
            pending_epoch_id: None,
            num_versions_deleted: 0,
            deletion_list: Vec::new(),
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
    #[error("ComputeUnusedFiles error: {0}")]
    ComputeUnusedFiles(#[from] ComputeUnusedFilesError),
    #[error("DeleteVersionsAtSysDb error: {0}")]
    DeleteVersionsAtSysDb(#[from] DeleteVersionsAtSysDbError),
    #[error("The task was aborted because resources were exhausted")]
    Aborted,
    #[error("DeleteUnusedFiles error: {0}")]
    DeleteUnusedFiles(#[from] DeleteUnusedFilesError),
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
        tracing::info!(
            path = %self.version_file_path,
            "Creating initial fetch version file task"
        );

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
        tracing::info!("Processing FetchVersionFile result");

        // Stage 1: Process fetched version file and initiate version computation
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => {
                tracing::info!(
                    content_size = output.version_file_content().len(),
                    "Successfully got version file content"
                );
                output
            }
            None => {
                tracing::error!("Failed to get version file output");
                return;
            }
        };

        let cutoff_time = Utc::now() - Duration::hours(self.cutoff_time_hours as i64);
        tracing::info!(
            cutoff_time = ?cutoff_time,
            "Computed cutoff time for version deletion"
        );

        let version_file = match CollectionVersionFile::decode(output.version_file_content()) {
            Ok(file) => {
                tracing::info!("Successfully decoded version file");
                file
            }
            Err(e) => {
                tracing::error!(error = ?e, "Failed to decode version file");
                let result: Result<FetchVersionFileOutput, GarbageCollectorError> =
                    Err(GarbageCollectorError::ComputeVersionsToDelete(
                        ComputeVersionsToDeleteError::ParseError(e),
                    ));
                self.ok_or_terminate(result, ctx);
                return;
            }
        };

        tracing::info!("Creating compute versions task");
        let compute_task = wrap(
            Box::new(ComputeVersionsToDeleteOperator {}),
            ComputeVersionsToDeleteInput {
                version_file,
                cutoff_time,
                cutoff_time_secs: self.cutoff_time_secs,
                min_versions_to_keep: 2,
            },
            ctx.receiver(),
        );

        tracing::info!("Sending compute versions task to dispatcher");
        if let Err(e) = self.dispatcher().send(compute_task, None).await {
            tracing::error!(error = ?e, "Failed to send compute task to dispatcher");
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx);
            return;
        }
        tracing::info!("Successfully sent compute versions task");
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

        // If no versions to delete, terminate early with success
        if output.versions_to_delete.versions.is_empty() {
            tracing::info!("No versions to delete, terminating garbage collection early");
            let response = GarbageCollectorResponse {
                collection_id: self.collection_id,
                version_file_path: self.version_file_path.clone(),
                num_versions_deleted: 0,
                deletion_list: Vec::new(),
            };
            tracing::info!(?response, "Garbage collection completed early");
            self.terminate_with_result(Ok(response), ctx);
            // Signal the dispatcher to shut down
            return;
        }

        self.num_versions_deleted = output.versions_to_delete.versions.len() as u32;
        self.pending_versions_to_delete = Some(output.versions_to_delete.clone());
        self.pending_version_file = Some(output.version_file.clone());

        let mark_task = wrap(
            Box::new(MarkVersionsAtSysDbOperator {}),
            MarkVersionsAtSysDbInput {
                version_file: output.version_file,
                versions_to_delete: output.versions_to_delete,
                sysdb_client: self.sysdb_client.clone(),
                epoch_id: 0,
                oldest_version_to_keep: output.oldest_version_to_keep,
            },
            ctx.receiver(),
        );

        if let Err(e) = self.dispatcher().send(mark_task, None).await {
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx);
            // Signal the dispatcher to shut down
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
        // Stage 3: After marking versions, compute unused files
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        let compute_task = wrap(
            Box::new(ComputeUnusedFilesOperator::new(
                self.collection_id.to_string(),
                self.storage.clone(),
                2, // min_versions_to_keep
            )),
            ComputeUnusedFilesInput {
                version_file: output.version_file,
                versions_to_delete: output.versions_to_delete,
                oldest_version_to_keep: output.oldest_version_to_keep,
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
impl Handler<TaskResult<ComputeUnusedFilesOutput, ComputeUnusedFilesError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ComputeUnusedFilesOutput, ComputeUnusedFilesError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        // Stage 4: After identifying unused files, delete them
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        let delete_task = wrap(
            Box::new(DeleteUnusedFilesOperator::new(
                self.storage.clone(),
                self.cleanup_mode,
                self.collection_id.to_string(),
            )),
            DeleteUnusedFilesInput {
                unused_s3_files: output.unused_block_ids.into_iter().collect(),
                epoch_id: 0,
                hnsw_prefixes_for_deletion: output.unused_hnsw_prefixes,
            },
            ctx.receiver(),
        );

        if let Err(e) = self.dispatcher().send(delete_task, None).await {
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx);
            return;
        }

        // Store state needed for final deletion
        self.pending_epoch_id = Some(0); // TODO: Get this from somewhere
    }
}

#[async_trait]
impl Handler<TaskResult<DeleteUnusedFilesOutput, DeleteUnusedFilesError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<DeleteUnusedFilesOutput, DeleteUnusedFilesError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        // Stage 6: After deleting unused files, delete the versions
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        // Get stored state
        let version_file = self
            .pending_version_file
            .take()
            .expect("Version file should be set");
        let versions_to_delete = self
            .pending_versions_to_delete
            .take()
            .expect("Versions to delete should be set");
        let epoch_id = self
            .pending_epoch_id
            .take()
            .expect("Epoch ID should be set");

        let delete_versions_task = wrap(
            Box::new(DeleteVersionsAtSysDbOperator {
                storage: self.storage.clone(),
            }),
            DeleteVersionsAtSysDbInput {
                version_file,
                epoch_id,
                sysdb_client: self.sysdb_client.clone(),
                versions_to_delete,
                unused_s3_files: output.deleted_files.clone(),
            },
            ctx.receiver(),
        );

        // Update the deletion list so that GarbageCollectorOrchestrator can use it in the final stage.
        self.deletion_list = output.deleted_files.clone().into_iter().collect();

        if let Err(e) = self.dispatcher().send(delete_versions_task, None).await {
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
            num_versions_deleted: self.num_versions_deleted,
            deletion_list: self.deletion_list.clone(),
        };

        self.terminate_with_result(Ok(response), ctx);
    }
}

#[cfg(test)]
mod tests {
    use crate::helper::ChromaGrpcClients;
    use std::time::Duration;

    #[allow(dead_code)]
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

    // NOTE(hammadb): This test was added without consideration as to how to handle
    // the required configuration for the test. It expects configuration to be ON
    // for the sysdb that should not be on by default. I am disabling it until
    // we properly handle the config.

    // #[tokio::test]
    // async fn test_k8s_integration_check_end_to_end() -> Result<(), Box<dyn std::error::Error>> {
    //     // Initialize tracing subscriber once at the start of the test
    //     let _ = tracing_subscriber::fmt::try_init();

    //     tracing::info!("Starting direct service calls test");

    //     // Create storage config and storage client
    //     let storage_config = StorageConfig::ObjectStore(ObjectStoreConfig {
    //         bucket: ObjectStoreBucketConfig {
    //             name: "chroma-storage".to_string(),
    //             r#type: ObjectStoreType::Minio,
    //         },
    //         upload_part_size_bytes: 1024 * 1024,   // 1MB
    //         download_part_size_bytes: 1024 * 1024, // 1MB
    //         max_concurrent_requests: 10,
    //     });
    //     // Create registry for configuration
    //     let registry = Registry::new();
    //     // Initialize storage using config and registry
    //     let storage = Storage::try_from_config(&storage_config, &registry).await?;
    //     // Add check for HNSW prefixes before Tests.
    //     let hnsw_prefixes_before_tests: Vec<String> = storage
    //         .list_prefix("hnsw")
    //         .await?
    //         .into_iter()
    //         .filter(|path| path.contains("hnsw/"))
    //         .map(|path| {
    //             path.split("/")
    //                 .nth(1) // Get the prefix part after "hnsw/"
    //                 .unwrap_or("")
    //                 .to_string()
    //         })
    //         .collect::<std::collections::HashSet<_>>() // Collect into HashSet first
    //         .into_iter() // Convert HashSet back to iterator
    //         .collect(); // Collect into final Vec

    //     println!(
    //         "HNSW prefixes before Tests: {:?}",
    //         hnsw_prefixes_before_tests
    //     );
    //     let deleted_hnsw_files_before_tests: Vec<_> = storage
    //         .list_prefix("deleted")
    //         .await?
    //         .into_iter()
    //         .filter(|path| path.contains("deleted") && path.contains("header.bin"))
    //         .collect();

    //     let mut clients = ChromaGrpcClients::new().await.map_err(|e| {
    //         tracing::error!(error = ?e, "Failed to create ChromaGrpcClients");
    //         e
    //     })?;

    //     // Create unique identifiers for tenant and database
    //     let test_uuid = uuid::Uuid::new_v4();
    //     let tenant_id = format!("test_tenant_{}", test_uuid);
    //     let database_name = format!("test_db_{}", test_uuid);
    //     let collection_name = format!("test_collection_{}", test_uuid);

    //     tracing::info!(
    //         tenant_id = %tenant_id,
    //         database = %database_name,
    //         collection = %collection_name,
    //         "Starting test with resources"
    //     );

    //     let collection_id = clients
    //         .create_database_and_collection(&tenant_id, &database_name, &collection_name)
    //         .await
    //         .map_err(|e| {
    //             tracing::error!(
    //                 error = ?e,
    //                 tenant_id = %tenant_id,
    //                 database = %database_name,
    //                 collection = %collection_name,
    //                 "Failed to create database and collection"
    //             );
    //             e
    //         })?;

    //     tracing::info!(collection_id = %collection_id, "Created collection");
    //     println!("Created collection: {}", collection_id);

    //     // Create 33 records
    //     let mut embeddings = Vec::with_capacity(33);
    //     let mut ids = Vec::with_capacity(33);

    //     for i in 0..33 {
    //         let mut embedding = vec![0.0; 3];
    //         embedding[i % 3] = 1.0;
    //         embeddings.push(embedding);
    //         ids.push(format!("id{}", i));
    //     }

    //     // Get initial version count
    //     let initial_versions = clients
    //         .list_collection_versions(&collection_id, &tenant_id, Some(100), None, None)
    //         .await?;
    //     let initial_version_count = initial_versions.versions.len();

    //     tracing::info!(
    //         initial_count = initial_version_count,
    //         "Initial version count"
    //     );

    //     // Add first batch of 11 records
    //     tracing::info!("Adding first batch of embeddings");
    //     clients
    //         .add_embeddings(
    //             &collection_id,
    //             embeddings[..11].to_vec(),
    //             ids[..11].to_vec(),
    //         )
    //         .await?;

    //     // Wait for new version after first batch
    //     wait_for_new_version(
    //         &mut clients,
    //         &collection_id,
    //         &tenant_id,
    //         initial_version_count,
    //         10,
    //     )
    //     .await?;

    //     // Add second batch of 11 records
    //     tracing::info!("Adding second batch of embeddings");
    //     clients
    //         .add_embeddings(
    //             &collection_id,
    //             embeddings[11..22].to_vec(),
    //             ids[11..22].to_vec(),
    //         )
    //         .await?;
    //     // Wait for new version after first batch
    //     wait_for_new_version(
    //         &mut clients,
    //         &collection_id,
    //         &tenant_id,
    //         initial_version_count + 1,
    //         10,
    //     )
    //     .await?;

    //     // After adding second batch and waiting for version, add a third batch
    //     tracing::info!("Adding third batch of embeddings (modified records)");
    //     clients
    //         .add_embeddings(
    //             &collection_id,
    //             embeddings[22..].to_vec(),
    //             ids[22..].to_vec(),
    //         )
    //         .await?;

    //     wait_for_new_version(
    //         &mut clients,
    //         &collection_id,
    //         &tenant_id,
    //         initial_version_count + 2,
    //         10,
    //     )
    //     .await?;

    //     // Get version count before GC
    //     let versions_before_gc = clients
    //         .list_collection_versions(&collection_id, &tenant_id, Some(100), None, None)
    //         .await?;
    //     let unique_versions_before_gc = versions_before_gc
    //         .versions
    //         .iter()
    //         .map(|v| v.version)
    //         .collect::<std::collections::HashSet<_>>()
    //         .len();
    //     tracing::info!(
    //         count = unique_versions_before_gc,
    //         "Unique version count before GC"
    //     );

    //     // Get records from the collection
    //     tracing::info!(collection_id = %collection_id, "Getting records from collection");

    //     let results = clients
    //         .get_records(
    //             &collection_id,
    //             None,
    //             true,
    //             false,
    //             false,
    //         )
    //         .await
    //         .map_err(|e| {
    //             tracing::error!(error = ?e, collection_id = %collection_id, "Failed to get records");
    //             e
    //         })?;

    //     // Verify results
    //     tracing::info!(
    //         num_results = results.ids.len(),
    //         "Get records results received"
    //     );
    //     assert_eq!(results.ids.len(), 33, "Expected 33 results");

    //     // Verify all IDs are present
    //     for i in 0..33 {
    //         let expected_id = format!("id{}", i);
    //         assert!(
    //             results.ids.contains(&expected_id),
    //             "Expected to find {}",
    //             expected_id
    //         );
    //     }

    //     // Verify embeddings
    //     if let Some(returned_embeddings) = results.embeddings {
    //         assert_eq!(returned_embeddings.len(), 33, "Expected 33 embeddings");

    //         for (i, embedding) in returned_embeddings.iter().enumerate() {
    //             let expected_index = ids
    //                 .iter()
    //                 .position(|id| id == &format!("id{}", i))
    //                 .expect("ID should exist");
    //             assert_eq!(
    //                 embedding, &embeddings[expected_index],
    //                 "Embedding mismatch for id{}",
    //                 i
    //             );
    //         }
    //     } else {
    //         panic!("Expected embeddings in results");
    //     }

    //     // Get final versions
    //     tracing::info!(collection_id = %collection_id, "Requesting final collection versions");

    //     let versions_response = clients
    //         .list_collection_versions(&collection_id, &tenant_id, Some(10), None, None)
    //         .await?;

    //     tracing::info!("Collection versions:");
    //     let mut oldest_version_num = 0;
    //     let mut youngest_version_num = 0;
    //     for version_info in versions_response.versions.iter() {
    //         if version_info.version > oldest_version_num {
    //             oldest_version_num = version_info.version;
    //         }
    //         if version_info.version < youngest_version_num {
    //             youngest_version_num = version_info.version;
    //         }
    //     }
    //     println!(
    //         "Oldest version: {}, youngest version: {}",
    //         oldest_version_num, youngest_version_num
    //     );

    //     for version in versions_response.versions {
    //         tracing::info!(
    //             version = version.version,
    //             created_at = version.created_at_secs,
    //             change_reason = ?version.version_change_reason,
    //             marked_for_deletion = version.marked_for_deletion,
    //             "Version info"
    //         );

    //         if let Some(collection_info) = version.collection_info_mutable {
    //             tracing::info!(
    //                 log_position = collection_info.current_log_position,
    //                 collection_version = collection_info.current_collection_version,
    //                 last_compaction = collection_info.last_compaction_time_secs,
    //                 dimension = collection_info.dimension,
    //                 "Collection mutable info"
    //             );
    //         }
    //         println!("For Version: {}", version.version);
    //         if let Some(segment_info) = version.segment_info {
    //             println!(
    //                 "Segment info - Number of segments: {}",
    //                 segment_info.segment_compaction_info.len()
    //             );
    //             tracing::info!(
    //                 num_segments = segment_info.segment_compaction_info.len(),
    //                 "Segment info"
    //             );

    //             // Print detailed information for each segment
    //             for (idx, segment) in segment_info.segment_compaction_info.iter().enumerate() {
    //                 println!("Segment #{} - ID: {}", idx, segment.segment_id);
    //                 tracing::info!(
    //                     segment_number = idx,
    //                     segment_id = %segment.segment_id,
    //                     "Segment details"
    //                 );

    //                 // Log file paths for the segment
    //                 if !segment.file_paths.is_empty() {
    //                     println!(
    //                         "Segment #{} - ID: {} - File paths: {:?}",
    //                         idx, segment.segment_id, segment.file_paths
    //                     );
    //                     tracing::info!(
    //                         segment_number = idx,
    //                         segment_id = %segment.segment_id,
    //                         file_paths = ?segment.file_paths,
    //                         "Segment file paths"
    //                     );
    //                 }
    //             }
    //         }
    //     }

    //     tracing::info!(
    //         is_truncated = versions_response.list_is_truncated,
    //         "Version list complete"
    //     );

    //     // After creating versions and verifying records, add garbage collection:
    //     tracing::info!("Starting garbage collection process");

    //     // Create system first
    //     let system = System::new();

    //     // Create dispatcher and handle
    //     let dispatcher = Dispatcher::new(chroma_system::DispatcherConfig::default());
    //     let dispatcher_handle = system.start_component(dispatcher);

    //     // Create sysdb config and client
    //     let sysdb_config = SysDbConfig::Grpc(GrpcSysDbConfig {
    //         host: "localhost".to_string(),
    //         port: 50051,
    //         connect_timeout_ms: 5000,
    //         request_timeout_ms: 10000,
    //         num_channels: 1,
    //     });

    //     // Initialize sysdb client using config and registry
    //     let mut sysdb = SysDb::try_from_config(&sysdb_config, &registry).await?;

    //     // Get collection info for GC from sysdb
    //     let collections_to_gc = sysdb.get_collections_to_gc().await?;
    //     let collection_info = collections_to_gc
    //         .iter()
    //         .find(|c| c.id.0.to_string() == collection_id)
    //         .expect("Collection should be available for GC");

    //     tracing::info!(
    //         "Collection info: {:?} {:?}",
    //         collection_info.id,
    //         collection_info.version_file_path
    //     );

    //     // Verify the version file exists before proceeding by attempting to get it
    //     let version_file_exists = storage
    //         .get(&collection_info.version_file_path)
    //         .await
    //         .is_ok();

    //     if !version_file_exists {
    //         tracing::error!(
    //             path = ?collection_info.version_file_path,
    //             "Version file does not exist"
    //         );
    //         return Err("Version file not found".into());
    //     }

    //     // Create orchestrator with correct version file path
    //     let mut orchestrator = GarbageCollectorOrchestrator::new(
    //         CollectionUuid::from_str(&collection_id)?,
    //         collection_info.version_file_path.clone(),
    //         0,     // cutoff_time_hours: immediately expire versions
    //         sysdb, // sysdb is already a SysDb, will be boxed by new()
    //         dispatcher_handle,
    //         storage.clone(), // Clone storage since we'll use it again
    //     );

    //     // Create channel for receiving result
    //     let (sender, _receiver) = oneshot::channel();
    //     orchestrator.set_result_channel(sender);

    //     tracing::info!("Running orchestrator");
    //     // Run orchestrator with system
    //     orchestrator.run(system).await?;
    //     // let gc_result = receiver.await?; // Waiting here is giving error.

    //     // After running GC and waiting for result, verify versions were deleted
    //     tokio::time::sleep(Duration::from_secs(5)).await; // Give some time for GC to complete

    //     let versions_after_gc = clients
    //         .list_collection_versions(&collection_id, &tenant_id, Some(100), None, None)
    //         .await?;

    //     let unique_versions_after_gc = versions_after_gc
    //         .versions
    //         .iter()
    //         .map(|v| v.version)
    //         .collect::<std::collections::HashSet<_>>()
    //         .len();
    //     println!(
    //         "versions after GC: {:?}",
    //         versions_after_gc
    //             .versions
    //             .iter()
    //             .map(|v| v.version)
    //             .collect::<std::collections::HashSet<_>>()
    //     );

    //     tracing::info!(
    //         before = unique_versions_before_gc,
    //         after = unique_versions_after_gc,
    //         "Unique version counts before and after GC"
    //     );
    //     println!(
    //         "Unique version counts before and after GC: {} {}",
    //         unique_versions_before_gc, unique_versions_after_gc
    //     );

    //     // Add check for HNSW files
    //     // let hnsw_files_after_gc: Vec<_> = storage
    //     //     .list_prefix("hnsw")
    //     //     .await?
    //     //     .into_iter()
    //     //     .filter(|path| path.ends_with("header.bin"))
    //     //     .collect();
    //     let hnsw_prefixes_after_gc: Vec<String> = storage
    //         .list_prefix("hnsw")
    //         .await?
    //         .into_iter()
    //         .filter(|path| path.contains("hnsw/"))
    //         .map(|path| {
    //             path.split("/")
    //                 .nth(1) // Get the prefix part after "hnsw/"
    //                 .unwrap_or("")
    //                 .to_string()
    //         })
    //         .collect::<std::collections::HashSet<_>>() // Collect into HashSet first
    //         .into_iter() // Convert HashSet back to iterator
    //         .collect(); // Collect into final Vec

    //     tracing::info!(
    //         count = hnsw_prefixes_after_gc.len(),
    //         files = ?hnsw_prefixes_after_gc,
    //         "HNSW header files after GC"
    //     );

    //     assert_eq!(
    //         hnsw_prefixes_after_gc.len() - hnsw_prefixes_before_tests.len(),
    //         unique_versions_after_gc - 1, // unique_versions_before_gc - unique_versions_after_gc, //
    //         "Increase in HNSW prefixes should match the number of versions left behind after GC"
    //     );

    //     // Wait a bit for GC to complete
    //     tokio::time::sleep(Duration::from_secs(2)).await;

    //     // Verify that deleted files are renamed with the "deleted" prefix if using soft delete
    //     let deleted_hnsw_files: Vec<_> = storage
    //         .list_prefix("deleted")
    //         .await?
    //         .into_iter()
    //         .filter(|path| path.contains("deleted") && path.contains("header.bin"))
    //         .collect();

    //     tracing::info!(
    //         count = deleted_hnsw_files.len(),
    //         files = ?deleted_hnsw_files,
    //         "Soft-deleted HNSW header files"
    //     );

    //     // The number of deleted files should match the difference in versions
    //     assert_eq!(
    //         deleted_hnsw_files.len() - deleted_hnsw_files_before_tests.len(),
    //         unique_versions_before_gc - unique_versions_after_gc,
    //         "Expected deleted HNSW files to match the number of deleted unique versions"
    //     );

    //     assert!(
    //         unique_versions_after_gc >= 2,
    //         "Expected at least 2 unique versions to remain after garbage collection (min_versions_to_keep)"
    //     );

    //     tracing::info!("Verifying records are still accessible after GC");
    //     let results_after_gc = clients
    //         .get_records(
    //             &collection_id,
    //             None,
    //             true,  // include embeddings
    //             false, // include metadata
    //             false, // include documents
    //         )
    //         .await
    //         .map_err(|e| {
    //             tracing::error!(error = ?e, collection_id = %collection_id, "Failed to get records after GC");
    //             e
    //         })?;

    //     // Verify results count matches pre-GC
    //     assert_eq!(
    //         results_after_gc.ids.len(),
    //         results.ids.len(),
    //         "Expected same number of results after GC"
    //     );

    //     // Verify all IDs are still present
    //     for i in 0..33 {
    //         let expected_id = format!("id{}", i);
    //         assert!(
    //             results_after_gc.ids.contains(&expected_id),
    //             "Expected to find {} after GC",
    //             expected_id
    //         );
    //     }

    //     // Verify embeddings are unchanged
    //     if let Some(returned_embeddings) = results_after_gc.embeddings {
    //         assert_eq!(
    //             returned_embeddings.len(),
    //             33,
    //             "Expected 33 embeddings after GC"
    //         );

    //         // Compare with original embeddings
    //         for (i, embedding) in returned_embeddings.iter().enumerate() {
    //             let expected_index = ids
    //                 .iter()
    //                 .position(|id| id == &format!("id{}", i))
    //                 .expect("ID should exist");
    //             assert_eq!(
    //                 embedding, &embeddings[expected_index],
    //                 "Embedding mismatch for id{} after GC",
    //                 i
    //             );
    //         }
    //     } else {
    //         panic!("Expected embeddings in results after GC");
    //     }

    //     tracing::info!("Successfully verified all records are accessible after GC");

    //     Ok(())
    // }
}
