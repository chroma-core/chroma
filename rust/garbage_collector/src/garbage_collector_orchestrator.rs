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

use crate::types::{CleanupMode, GarbageCollectorResponse};
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
use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

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

pub struct GarbageCollectorOrchestrator {
    collection_id: CollectionUuid,
    version_file_path: String,
    absolute_cutoff_time: DateTime<Utc>,
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

#[allow(clippy::too_many_arguments)]
impl GarbageCollectorOrchestrator {
    pub fn new(
        collection_id: CollectionUuid,
        version_file_path: String,
        absolute_cutoff_time: DateTime<Utc>,
        sysdb_client: SysDb,
        dispatcher: ComponentHandle<Dispatcher>,
        storage: Storage,
        cleanup_mode: CleanupMode,
    ) -> Self {
        Self {
            collection_id,
            version_file_path,
            absolute_cutoff_time,
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

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        tracing::info!(
            path = %self.version_file_path,
            "Creating initial fetch version file task"
        );

        vec![(
            wrap(
                Box::new(FetchVersionFileOperator {}),
                FetchVersionFileInput::new(self.version_file_path.clone(), self.storage.clone()),
                ctx.receiver(),
            ),
            Some(Span::current()),
        )]
    }

    fn set_result_channel(
        &mut self,
        sender: Sender<Result<GarbageCollectorResponse, GarbageCollectorError>>,
    ) {
        self.result_channel = Some(sender);
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => {
                tracing::error!("Failed to get version file output");
                return;
            }
        };
        let version_file = output.file;

        tracing::info!("Creating compute versions task");
        let compute_task = wrap(
            Box::new(ComputeVersionsToDeleteOperator {}),
            ComputeVersionsToDeleteInput {
                version_file,
                cutoff_time: self.absolute_cutoff_time,
                min_versions_to_keep: 2,
            },
            ctx.receiver(),
        );

        tracing::info!("Sending compute versions task to dispatcher");
        if let Err(e) = self
            .dispatcher()
            .send(compute_task, Some(Span::current()))
            .await
        {
            tracing::error!(error = ?e, "Failed to send compute task to dispatcher");
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx)
                .await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        // If no versions to delete, terminate early with success
        if output.versions_to_delete.versions.is_empty() {
            tracing::info!("No versions to delete, terminating garbage collection early");
            let response = GarbageCollectorResponse {
                collection_id: self.collection_id,
                num_versions_deleted: 0,
                num_files_deleted: 0,
                ..Default::default()
            };
            tracing::info!(?response, "Garbage collection completed early");
            self.terminate_with_result(Ok(response), ctx).await;
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

        if let Err(e) = self
            .dispatcher()
            .send(mark_task, Some(Span::current()))
            .await
        {
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx)
                .await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
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

        if let Err(e) = self
            .dispatcher()
            .send(compute_task, Some(Span::current()))
            .await
        {
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx)
                .await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
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
                hnsw_prefixes_for_deletion: output.unused_hnsw_prefixes,
            },
            ctx.receiver(),
        );

        if let Err(e) = self
            .dispatcher()
            .send(delete_task, Some(Span::current()))
            .await
        {
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx)
                .await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        if self.cleanup_mode == CleanupMode::DryRun {
            tracing::info!("Dry run mode, skipping actual deletion");
            let response = GarbageCollectorResponse {
                collection_id: self.collection_id,
                num_versions_deleted: 0,
                num_files_deleted: 0,
                ..Default::default()
            };
            self.terminate_with_result(Ok(response), ctx).await;
            return;
        }

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

        if let Err(e) = self
            .dispatcher()
            .send(delete_versions_task, Some(Span::current()))
            .await
        {
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx)
                .await;
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
        let _output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        #[expect(deprecated)]
        let response = GarbageCollectorResponse {
            collection_id: self.collection_id,
            num_versions_deleted: self.num_versions_deleted,
            num_files_deleted: self.deletion_list.len() as u32,
            deletion_list: self.deletion_list.clone(),
        };

        self.terminate_with_result(Ok(response), ctx).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helper::ChromaGrpcClients;
    use chroma_config::registry::Registry;
    use chroma_config::Configurable;
    use chroma_storage::config::{
        ObjectStoreBucketConfig, ObjectStoreConfig, ObjectStoreType, StorageConfig,
    };
    use chroma_sysdb::{GrpcSysDbConfig, SysDbConfig};
    use chroma_system::System;
    use std::str::FromStr;
    use std::time::{Duration, SystemTime};
    use tracing_test::traced_test;
    use uuid::Uuid;

    #[allow(dead_code)]
    async fn wait_for_new_version(
        clients: &mut ChromaGrpcClients,
        collection_id: String,
        tenant_id: String,
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
                .list_collection_versions(
                    collection_id.clone(),
                    tenant_id.clone(),
                    Some(100),
                    None,
                    None,
                    None,
                )
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

    const TEST_COLLECTIONS_SIZE: usize = 33;

    async fn validate_test_collection(
        clients: &mut ChromaGrpcClients,
        collection_id: CollectionUuid,
    ) {
        let results = clients
            .get_records(collection_id.to_string(), None, true, false, false)
            .await
            .unwrap();

        // Verify all IDs are still present
        for i in 0..TEST_COLLECTIONS_SIZE {
            let expected_id = format!("id{}", i);
            assert!(
                results.ids.contains(&expected_id),
                "Expected to find {}",
                expected_id
            );
        }

        // Verify embeddings are unchanged
        if let Some(returned_embeddings) = results.embeddings {
            assert_eq!(
                returned_embeddings.len(),
                TEST_COLLECTIONS_SIZE,
                "Expected {} embeddings",
                TEST_COLLECTIONS_SIZE
            );

            // Compare with expected embeddings
            for (i, embedding) in returned_embeddings.iter().enumerate() {
                let mut expected_embedding = vec![0.0; 3];
                expected_embedding[i % 3] = 1.0;
                assert_eq!(
                    embedding, &expected_embedding,
                    "Expected embedding for ID {} to be {:?}",
                    i, expected_embedding
                );
            }
        } else {
            panic!("Expected embeddings in results");
        }
    }

    async fn create_test_collection(
        clients: &mut ChromaGrpcClients,
        enable_spann: bool,
    ) -> (CollectionUuid, String) {
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
            .create_database_and_collection(
                &tenant_id,
                &database_name,
                &collection_name,
                enable_spann,
            )
            .await
            .unwrap();

        tracing::info!(collection_id = %collection_id, "Created collection");

        let mut embeddings = vec![];
        let mut ids = vec![];

        for i in 0..TEST_COLLECTIONS_SIZE {
            let mut embedding = vec![0.0; 3];
            embedding[i % 3] = 1.0;
            embeddings.push(embedding);
            ids.push(format!("id{}", i));
        }

        // Get initial version count
        let initial_versions = clients
            .list_collection_versions(
                collection_id.clone(),
                tenant_id.clone(),
                Some(100),
                None,
                None,
                None,
            )
            .await
            .unwrap();
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
            .await
            .unwrap();

        // Wait for new version after first batch
        wait_for_new_version(
            clients,
            collection_id.clone(),
            tenant_id.clone(),
            initial_version_count,
            10,
        )
        .await
        .unwrap();

        // Add second batch of 11 records
        tracing::info!("Adding second batch of embeddings");
        clients
            .add_embeddings(
                &collection_id,
                embeddings[11..22].to_vec(),
                ids[11..22].to_vec(),
            )
            .await
            .unwrap();
        // Wait for new version after first batch
        wait_for_new_version(
            clients,
            collection_id.clone(),
            tenant_id.clone(),
            initial_version_count + 1,
            10,
        )
        .await
        .unwrap();

        // After adding second batch and waiting for version, add a third batch
        tracing::info!("Adding third batch of embeddings (modified records)");
        clients
            .add_embeddings(
                &collection_id,
                embeddings[22..].to_vec(),
                ids[22..].to_vec(),
            )
            .await
            .unwrap();

        wait_for_new_version(
            clients,
            collection_id.clone(),
            tenant_id.clone(),
            initial_version_count + 2,
            10,
        )
        .await
        .unwrap();

        let collection_id = CollectionUuid::from_str(&collection_id).unwrap();

        validate_test_collection(clients, collection_id).await;

        (collection_id, tenant_id)
    }

    async fn get_hnsw_index_ids(storage: &Storage) -> Vec<Uuid> {
        storage
            .list_prefix("hnsw")
            .await
            .unwrap()
            .into_iter()
            .filter(|path| path.contains("hnsw/"))
            .map(|path| {
                Uuid::from_str(
                    path.split("/")
                        .nth(1) // Get the prefix part after "hnsw/"
                        .unwrap(),
                )
                .unwrap()
            })
            .collect::<std::collections::HashSet<_>>() // de-dupe
            .into_iter()
            .collect()
    }

    async fn test_k8s_integration_check_end_to_end(use_spann: bool) {
        // Create storage config and storage client
        let storage_config = StorageConfig::ObjectStore(ObjectStoreConfig {
            bucket: ObjectStoreBucketConfig {
                name: "chroma-storage".to_string(),
                r#type: ObjectStoreType::Minio,
            },
            upload_part_size_bytes: 1024 * 1024,   // 1MB
            download_part_size_bytes: 1024 * 1024, // 1MB
            max_concurrent_requests: 10,
        });

        let registry = Registry::new();
        let storage = Storage::try_from_config(&storage_config, &registry)
            .await
            .unwrap();

        let mut clients = ChromaGrpcClients::new().await.unwrap();
        let (collection_id, tenant_id) = create_test_collection(&mut clients, use_spann).await;

        let hnsw_index_ids_before_gc = get_hnsw_index_ids(&storage).await;

        // Get version count before GC
        let versions_before_gc = clients
            .list_collection_versions(
                collection_id.to_string(),
                tenant_id.clone(),
                Some(100),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let unique_versions_before_gc = versions_before_gc
            .versions
            .iter()
            .map(|v| v.version)
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert_eq!(
            unique_versions_before_gc, 4,
            "Expected 4 unique versions before starting garbage collection"
        );

        // After creating versions and verifying records, start garbage collection:
        tracing::info!("Starting garbage collection process");

        let system = System::new();
        let dispatcher = Dispatcher::new(chroma_system::DispatcherConfig::default());
        let dispatcher_handle = system.start_component(dispatcher);
        let sysdb_config = SysDbConfig::Grpc(GrpcSysDbConfig {
            host: "localhost".to_string(),
            port: 50051,
            connect_timeout_ms: 5000,
            request_timeout_ms: 10000,
            num_channels: 1,
        });
        let mut sysdb = SysDb::try_from_config(&sysdb_config, &registry)
            .await
            .unwrap();

        // Get collection info for GC from sysdb
        let collections_to_gc = sysdb.get_collections_to_gc(None, None, None).await.unwrap();
        let collection_info = collections_to_gc
            .iter()
            .find(|c| c.id == collection_id)
            .expect("Collection should be available for GC");

        // Create orchestrator with correct version file path
        let orchestrator = GarbageCollectorOrchestrator::new(
            collection_id,
            collection_info.version_file_path.clone(),
            SystemTime::now().into(), //  immediately expire versions
            sysdb,
            dispatcher_handle,
            storage.clone(),
            CleanupMode::Delete,
        );

        tracing::info!("Running orchestrator");
        let result = orchestrator.run(system).await.unwrap();
        assert_eq!(result.num_versions_deleted, 1);

        // After running GC and waiting for result, verify versions were deleted
        let versions_after_gc = clients
            .list_collection_versions(
                collection_id.to_string(),
                tenant_id.clone(),
                Some(100),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let unique_versions_after_gc = versions_after_gc
            .versions
            .iter()
            .map(|v| v.version)
            .collect::<std::collections::HashSet<_>>()
            .len();

        tracing::info!(
            before = unique_versions_before_gc,
            after = unique_versions_after_gc,
            "Unique version counts before and after GC"
        );

        assert!(
            unique_versions_after_gc >= 2,
            "Expected at least 2 unique versions to remain after garbage collection (min_versions_to_keep)"
        );

        // Check HNSW indices
        let hnsw_index_ids_after_gc = get_hnsw_index_ids(&storage).await;
        tracing::info!(
            before = ?hnsw_index_ids_before_gc,
            after = ?hnsw_index_ids_after_gc,
            "HNSW index IDs before and after GC"
        );

        assert_eq!(
            hnsw_index_ids_before_gc.len() - hnsw_index_ids_after_gc.len(),
            result.num_versions_deleted as usize,
            "Expected {} HNSW indices to be deleted after garbage collection",
            result.num_versions_deleted
        );

        tracing::info!("Verifying records are still accessible after GC");
        validate_test_collection(&mut clients, collection_id).await;
    }

    #[tokio::test]
    #[traced_test]
    async fn test_k8s_integration_check_end_to_end_hnsw() {
        test_k8s_integration_check_end_to_end(false).await;
    }

    #[tokio::test]
    #[traced_test]
    async fn test_k8s_integration_check_end_to_end_spann() {
        test_k8s_integration_check_end_to_end(true).await;
    }

    #[tokio::test]
    #[traced_test]
    async fn test_k8s_integration_soft_delete() {
        // Create storage config and storage client
        let storage_config = StorageConfig::ObjectStore(ObjectStoreConfig {
            bucket: ObjectStoreBucketConfig {
                name: "chroma-storage".to_string(),
                r#type: ObjectStoreType::Minio,
            },
            upload_part_size_bytes: 1024 * 1024,   // 1MB
            download_part_size_bytes: 1024 * 1024, // 1MB
            max_concurrent_requests: 10,
        });

        let registry = Registry::new();
        let storage = Storage::try_from_config(&storage_config, &registry)
            .await
            .unwrap();

        let deleted_hnsw_files_before_test: Vec<_> = storage
            .list_prefix("gc")
            .await
            .unwrap()
            .into_iter()
            .filter(|path| path.contains("gc") && path.contains("header.bin"))
            .collect();

        let mut clients = ChromaGrpcClients::new().await.unwrap();
        let (collection_id, tenant_id) = create_test_collection(&mut clients, true).await;

        let hnsw_index_ids_before_gc = get_hnsw_index_ids(&storage).await;

        // Get version count before GC
        let versions_before_gc = clients
            .list_collection_versions(
                collection_id.to_string(),
                tenant_id.clone(),
                Some(100),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let unique_versions_before_gc = versions_before_gc
            .versions
            .iter()
            .map(|v| v.version)
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert_eq!(
            unique_versions_before_gc, 4,
            "Expected 4 unique versions before starting garbage collection"
        );

        // After creating versions and verifying records, start garbage collection:
        tracing::info!("Starting garbage collection process");

        let system = System::new();
        let dispatcher = Dispatcher::new(chroma_system::DispatcherConfig::default());
        let dispatcher_handle = system.start_component(dispatcher);
        let sysdb_config = SysDbConfig::Grpc(GrpcSysDbConfig {
            host: "localhost".to_string(),
            port: 50051,
            connect_timeout_ms: 5000,
            request_timeout_ms: 10000,
            num_channels: 1,
        });
        let mut sysdb = SysDb::try_from_config(&sysdb_config, &registry)
            .await
            .unwrap();

        // Get collection info for GC from sysdb
        let collections_to_gc = sysdb.get_collections_to_gc(None, None, None).await.unwrap();
        let collection_info = collections_to_gc
            .iter()
            .find(|c| c.id == collection_id)
            .expect("Collection should be available for GC");

        // Create orchestrator with correct version file path
        let orchestrator = GarbageCollectorOrchestrator::new(
            collection_id,
            collection_info.version_file_path.clone(),
            SystemTime::now().into(), //  immediately expire versions
            sysdb,
            dispatcher_handle,
            storage.clone(),
            CleanupMode::Rename,
        );

        tracing::info!("Running orchestrator");
        let result = orchestrator.run(system).await.unwrap();
        assert_eq!(result.num_versions_deleted, 1);

        // After running GC and waiting for result, verify versions were deleted
        let versions_after_gc = clients
            .list_collection_versions(
                collection_id.to_string(),
                tenant_id.clone(),
                Some(100),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let unique_versions_after_gc = versions_after_gc
            .versions
            .iter()
            .map(|v| v.version)
            .collect::<std::collections::HashSet<_>>()
            .len();

        tracing::info!(
            before = unique_versions_before_gc,
            after = unique_versions_after_gc,
            "Unique version counts before and after GC"
        );

        assert!(
            unique_versions_after_gc >= 2,
            "Expected at least 2 unique versions to remain after garbage collection (min_versions_to_keep)"
        );

        // Check HNSW indices
        let hnsw_index_ids_after_gc = get_hnsw_index_ids(&storage).await;
        tracing::info!(
            before = ?hnsw_index_ids_before_gc,
            after = ?hnsw_index_ids_after_gc,
            "HNSW index IDs before and after GC"
        );

        assert_eq!(
            hnsw_index_ids_before_gc.len() - hnsw_index_ids_after_gc.len(),
            result.num_versions_deleted as usize,
            "Expected {} HNSW indices to be deleted after garbage collection",
            result.num_versions_deleted
        );

        tracing::info!("Verifying records are still accessible after GC");
        validate_test_collection(&mut clients, collection_id).await;

        // Verify that "deleted" files are renamed with the "gc" prefix
        let deleted_hnsw_files: Vec<_> = storage
            .list_prefix("gc")
            .await
            .unwrap()
            .into_iter()
            .filter(|path| path.contains("gc") && path.contains("header.bin"))
            .collect();

        tracing::info!(
            count = deleted_hnsw_files.len(),
            files = ?deleted_hnsw_files,
            "Soft-deleted HNSW header files"
        );

        // The number of moved files should match the difference in versions
        assert_eq!(
            deleted_hnsw_files.len() - deleted_hnsw_files_before_test.len(),
            unique_versions_before_gc - unique_versions_after_gc,
            "Expected renamed HNSW files to match the number of deleted unique versions"
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn test_k8s_integration_dry_run() {
        // Create storage config and storage client
        let storage_config = StorageConfig::ObjectStore(ObjectStoreConfig {
            bucket: ObjectStoreBucketConfig {
                name: "chroma-storage".to_string(),
                r#type: ObjectStoreType::Minio,
            },
            upload_part_size_bytes: 1024 * 1024,   // 1MB
            download_part_size_bytes: 1024 * 1024, // 1MB
            max_concurrent_requests: 10,
        });

        let registry = Registry::new();
        let storage = Storage::try_from_config(&storage_config, &registry)
            .await
            .unwrap();

        let mut clients = ChromaGrpcClients::new().await.unwrap();
        let (collection_id, tenant_id) = create_test_collection(&mut clients, true).await;

        let hnsw_index_ids_before_gc = get_hnsw_index_ids(&storage).await;

        // Get version count before GC
        let versions_before_gc = clients
            .list_collection_versions(
                collection_id.to_string(),
                tenant_id.clone(),
                Some(100),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let unique_versions_before_gc = versions_before_gc
            .versions
            .iter()
            .map(|v| v.version)
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert_eq!(
            unique_versions_before_gc, 4,
            "Expected 4 unique versions before starting garbage collection"
        );

        // After creating versions and verifying records, start garbage collection:
        tracing::info!("Starting garbage collection process");

        let system = System::new();
        let dispatcher = Dispatcher::new(chroma_system::DispatcherConfig::default());
        let dispatcher_handle = system.start_component(dispatcher);
        let sysdb_config = SysDbConfig::Grpc(GrpcSysDbConfig {
            host: "localhost".to_string(),
            port: 50051,
            connect_timeout_ms: 5000,
            request_timeout_ms: 10000,
            num_channels: 1,
        });
        let mut sysdb = SysDb::try_from_config(&sysdb_config, &registry)
            .await
            .unwrap();

        // Get collection info for GC from sysdb
        let collections_to_gc = sysdb.get_collections_to_gc(None, None, None).await.unwrap();
        let collection_info = collections_to_gc
            .iter()
            .find(|c| c.id == collection_id)
            .expect("Collection should be available for GC");

        // Create orchestrator with correct version file path
        let orchestrator = GarbageCollectorOrchestrator::new(
            collection_id,
            collection_info.version_file_path.clone(),
            SystemTime::now().into(), //  immediately expire versions
            sysdb,
            dispatcher_handle,
            storage.clone(),
            CleanupMode::DryRun,
        );

        tracing::info!("Running orchestrator");
        let result = orchestrator.run(system).await.unwrap();
        assert_eq!(result.num_versions_deleted, 0);

        // After running GC and waiting for result, verify versions were deleted
        let versions_after_gc = clients
            .list_collection_versions(
                collection_id.to_string(),
                tenant_id.clone(),
                Some(100),
                None,
                None,
                Some(true), // include versions marked for deletion
            )
            .await
            .unwrap();

        // Expect 2 versions to be marked for deletion, but not actually deleted
        let num_versions_marked_for_deletion = versions_after_gc
            .versions
            .iter()
            .filter(|v| v.marked_for_deletion)
            .count();
        assert_eq!(
            num_versions_marked_for_deletion, 1,
            "Expected 1 version to be marked for deletion in dry run mode"
        );

        let unique_versions_after_gc = versions_after_gc
            .versions
            .iter()
            .map(|v| v.version)
            .collect::<std::collections::HashSet<_>>()
            .len();

        assert_eq!(
            unique_versions_after_gc, unique_versions_before_gc,
            "Expected no versions to be deleted in dry run mode"
        );

        // Check HNSW indices
        let hnsw_index_ids_after_gc = get_hnsw_index_ids(&storage).await;
        tracing::info!(
            before = ?hnsw_index_ids_before_gc,
            after = ?hnsw_index_ids_after_gc,
            "HNSW index IDs before and after GC"
        );

        assert_eq!(
            hnsw_index_ids_before_gc.len(),
            hnsw_index_ids_after_gc.len(),
            "Expected no HNSW indices to be deleted after garbage collection"
        );

        tracing::info!("Verifying records are still accessible after GC");
        validate_test_collection(&mut clients, collection_id).await;
    }
}
