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
    sysdb_client: Box<SysDb>,
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
        sysdb_client: Box<SysDb>,
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
            collection_id: self.collection_id.clone(),
            version_file_path: self.version_file_path.clone(),
        };

        self.terminate_with_result(Ok(response), ctx);
    }
}

#[cfg(test)]
mod tests {
    use crate::helper::ChromaGrpcClients;
    use chromadb::client::{
        ChromaAuthMethod, ChromaClient, ChromaClientOptions, ChromaTokenHeader,
    };

    async fn get_test_client() -> ChromaClient {
        let client = if let Ok(auth) = std::env::var("CHROMA_TOKEN") {
            println!("Creating ChromaClient with auth token");
            ChromaClient::new(ChromaClientOptions {
                url: Some("http://localhost:8000".to_string()),
                auth: ChromaAuthMethod::TokenAuth {
                    token: auth,
                    header: ChromaTokenHeader::Authorization,
                },
                database: "test".into(),
                connections: 32,
            })
            .await
        } else {
            println!("Creating ChromaClient with default settings");
            ChromaClient::new(Default::default()).await
        };

        match client {
            Ok(client) => client,
            Err(e) => {
                println!("Failed to create ChromaClient: {}", e);
                println!("Make sure ChromaDB is running at http://localhost:8000");
                panic!("ChromaDB connection failed");
            }
        }
    }

    // #[tokio::test]
    // async fn test_collection_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    //     println!("Starting collection lifecycle test");
    //     let client = get_test_client().await;
    //     let collection_name = format!("test_collection_{}", uuid::Uuid::new_v4());
    //     println!("Using collection name: {}", collection_name);

    //     // Get or create collection
    //     let collection: ChromaCollection = client
    //         .get_or_create_collection(&collection_name, None)
    //         .await
    //         .map_err(|e| {
    //             println!("Failed to create collection: {}", e);
    //             e
    //         })?;

    //     println!("Created collection with UUID: {}", collection.id());

    //     // Insert records with fixed embeddings
    //     let collection_entries = CollectionEntries {
    //         ids: vec!["id1".into(), "id2".into()],
    //         embeddings: Some(vec![
    //             vec![1.0_f32, 0.0_f32, 0.0_f32], // First document embedding
    //             vec![0.0_f32, 1.0_f32, 0.0_f32], // Second document embedding
    //         ]),
    //         metadatas: Some(vec![json!({"source": "test1"}), json!({"source": "test2"})]),
    //         documents: Some(vec![
    //             "This is document 1".into(),
    //             "This is document 2".into(),
    //         ]),
    //     };

    //     collection.upsert(collection_entries, None).await?;

    //     // Query the collection using vector search
    //     let query = QueryOptions {
    //         query_texts: None,
    //         query_embeddings: Some(vec![vec![1.0_f32, 0.0_f32, 0.0_f32]]), // Should match first document better
    //         where_metadata: None,
    //         where_document: None,
    //         n_results: Some(2),
    //         include: None,
    //     };

    //     let query_result = collection.query(query, None).await?;

    //     // Verify results
    //     let ids = query_result.ids.expect("Query should return IDs");
    //     assert_eq!(ids.len(), 2);
    //     assert_eq!(ids[0], "id1"); // First result should be id1 since it has the same embedding
    //     assert_eq!(ids[1], "id2");

    //     // Clean up - delete the collection
    //     client.delete_collection(&collection_name).await?;

    //     Ok(())
    // }

    #[tokio::test]
    async fn test_direct_service_calls() -> Result<(), Box<dyn std::error::Error>> {
        let mut clients = ChromaGrpcClients::new().await?;

        // Create database and collection
        let tenant_id = "test_tenant";
        let database_name = "test_db";
        let collection_name = format!("test_collection_{}", uuid::Uuid::new_v4());

        let collection_id = clients
            .create_database_and_collection(tenant_id, database_name, &collection_name)
            .await?;

        // Add embeddings
        let embeddings = vec![vec![1.0, 0.0, 0.0], vec![0.0, 1.0, 0.0]];
        let ids = vec!["id1".to_string(), "id2".to_string()];
        clients
            .add_embeddings(&collection_id, embeddings, ids)
            .await?;

        // Query the collection
        let query_embedding = vec![1.0, 0.0, 0.0];
        let results = clients
            .query_collection(&collection_id, query_embedding)
            .await?;

        // Verify results
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "id1"); // First result should be id1
        assert!(results[0].1 < results[1].1); // First result should have smaller distance

        Ok(())
    }
}
