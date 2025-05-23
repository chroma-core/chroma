use crate::construct_version_graph_orchestrator::{
    ConstructVersionGraphError, ConstructVersionGraphOrchestrator,
};
use crate::operators::compute_versions_to_delete_from_graph::{
    CollectionVersionAction, ComputeVersionsToDeleteError, ComputeVersionsToDeleteInput,
    ComputeVersionsToDeleteOperator, ComputeVersionsToDeleteOutput,
};
use crate::operators::delete_unused_files::{
    DeleteUnusedFilesError, DeleteUnusedFilesInput, DeleteUnusedFilesOperator,
    DeleteUnusedFilesOutput,
};
use crate::operators::delete_versions_at_sysdb::{
    DeleteVersionsAtSysDbError, DeleteVersionsAtSysDbInput, DeleteVersionsAtSysDbOperator,
    DeleteVersionsAtSysDbOutput,
};
use crate::operators::list_files_at_version::{
    ListFilesAtVersionError, ListFilesAtVersionInput, ListFilesAtVersionOutput,
    ListFilesAtVersionsOperator,
};
use crate::operators::mark_versions_at_sysdb::{
    MarkVersionsAtSysDbError, MarkVersionsAtSysDbInput, MarkVersionsAtSysDbOperator,
    MarkVersionsAtSysDbOutput,
};
use crate::types::CleanupMode;
use async_trait::async_trait;
use chroma_blockstore::RootManager;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_sysdb::SysDb;
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    PanicError, System, TaskError, TaskResult,
};
use chroma_types::chroma_proto::{CollectionVersionFile, VersionListForCollection};
use chroma_types::CollectionUuid;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

#[derive(Debug)]
pub struct GarbageCollectorOrchestrator {
    collection_id: CollectionUuid,
    version_file_path: String,
    lineage_file_path: Option<String>,
    absolute_cutoff_time: DateTime<Utc>,
    sysdb_client: SysDb,
    dispatcher: ComponentHandle<Dispatcher>,
    system: System,
    storage: Storage,
    root_manager: RootManager,
    result_channel: Option<Sender<Result<GarbageCollectorResponse, GarbageCollectorError>>>,
    cleanup_mode: CleanupMode,
    version_files: HashMap<CollectionUuid, CollectionVersionFile>,
    versions_to_delete_output: Option<ComputeVersionsToDeleteOutput>,
    pending_mark_versions_at_sysdb_tasks: HashSet<CollectionUuid>,
    pending_list_files_at_version_tasks: HashSet<(CollectionUuid, i64)>,
    file_ref_counts: HashMap<String, u32>,
    num_pending_tasks: usize,
    min_versions_to_keep: u32,

    num_files_deleted: u32,
    num_versions_deleted: u32,
}

#[derive(Debug)]
pub struct GarbageCollectorResponse {
    pub num_versions_deleted: u32,
    pub num_files_deleted: u32,
}

#[allow(clippy::too_many_arguments)]
impl GarbageCollectorOrchestrator {
    pub fn new(
        collection_id: CollectionUuid,
        version_file_path: String,
        lineage_file_path: Option<String>,
        absolute_cutoff_time: DateTime<Utc>,
        sysdb_client: SysDb,
        dispatcher: ComponentHandle<Dispatcher>,
        system: System,
        storage: Storage,
        root_manager: RootManager,
        cleanup_mode: CleanupMode,
        min_versions_to_keep: u32,
    ) -> Self {
        Self {
            collection_id,
            version_file_path,
            lineage_file_path,
            absolute_cutoff_time,
            sysdb_client,
            dispatcher,
            system,
            storage,
            root_manager,
            cleanup_mode,
            result_channel: None,
            version_files: HashMap::new(),
            file_ref_counts: HashMap::new(),
            versions_to_delete_output: None,
            pending_mark_versions_at_sysdb_tasks: HashSet::new(),
            pending_list_files_at_version_tasks: HashSet::new(),
            num_pending_tasks: 0,
            min_versions_to_keep,

            num_files_deleted: 0,
            num_versions_deleted: 0,
        }
    }
}

#[derive(Error, Debug)]
pub enum GarbageCollectorError {
    #[error("Panic during compaction: {0}")]
    Panic(#[from] PanicError),
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("{0}")]
    Generic(#[from] Box<dyn ChromaError>),
    #[error("The task was aborted because resources were exhausted")]
    Aborted,

    #[error("Failed to construct version graph: {0}")]
    ConstructVersionGraph(#[from] ConstructVersionGraphError),
    #[error("Failed to compute versions to delete: {0}")]
    ComputeVersionsToDelete(#[from] ComputeVersionsToDeleteError),
    #[error("Failed to mark versions at sysdb: {0}")]
    MarkVersionsAtSysDb(#[from] MarkVersionsAtSysDbError),
    #[error("Failed to list files at version: {0}")]
    ListFilesAtVersion(#[from] ListFilesAtVersionError),
    #[error("Failed to delete unused files: {0}")]
    DeleteUnusedFiles(#[from] DeleteUnusedFilesError),
    #[error("Failed to delete versions at sysdb: {0}")]
    DeleteVersionsAtSysDb(#[from] DeleteVersionsAtSysDbError),

    #[error("Expected version file missing for collection {0}")]
    MissingVersionFile(CollectionUuid),
    #[error("Invariant violation: {0}")]
    InvariantViolation(String),
    #[error("Could not parse UUID: {0}")]
    UnparsableUuid(#[from] uuid::Error),
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

#[derive(Debug)]
struct ConstructVersionGraphRequest;

#[async_trait]
impl Orchestrator for GarbageCollectorOrchestrator {
    type Output = GarbageCollectorResponse;
    type Error = GarbageCollectorError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        ctx.receiver()
            .send(ConstructVersionGraphRequest, Some(Span::current()))
            .await
            .expect("Failed to send ConstructVersionGraphRequest");
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

impl GarbageCollectorOrchestrator {
    async fn handle_construct_version_graph_request(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Result<(), GarbageCollectorError> {
        let orchestrator = ConstructVersionGraphOrchestrator::new(
            self.dispatcher(),
            self.storage.clone(),
            self.sysdb_client.clone(),
            self.collection_id,
            self.version_file_path.clone(),
            self.lineage_file_path.clone(),
        );
        let output = orchestrator.run(self.system.clone()).await?;
        self.version_files = output.version_files;

        let task = wrap(
            Box::new(ComputeVersionsToDeleteOperator {}),
            ComputeVersionsToDeleteInput {
                graph: output.graph,
                cutoff_time: self.absolute_cutoff_time,
                min_versions_to_keep: self.min_versions_to_keep,
            },
            ctx.receiver(),
        );

        self.dispatcher()
            .send(task, Some(Span::current()))
            .await
            .map_err(GarbageCollectorError::Channel)?;
        Ok(())
    }

    async fn handle_compute_versions_to_delete_output(
        &mut self,
        output: ComputeVersionsToDeleteOutput,
        ctx: &ComponentContext<Self>,
    ) -> Result<(), GarbageCollectorError> {
        if output.versions.is_empty() {
            tracing::debug!("No versions to delete");
            let response = GarbageCollectorResponse {
                num_versions_deleted: 0,
                num_files_deleted: 0,
            };
            self.terminate_with_result(Ok(response), ctx).await;
            return Ok(());
        }

        self.pending_list_files_at_version_tasks = output
            .versions
            .iter()
            .flat_map(|(collection_id, versions)| {
                versions
                    .keys()
                    .map(|version| (*collection_id, *version))
                    .collect::<HashSet<_>>()
            })
            .collect();

        for (collection_id, versions) in &output.versions {
            let version_file = self
                .version_files
                .get(collection_id)
                .ok_or(GarbageCollectorError::MissingVersionFile(*collection_id))?;

            let collection_info = version_file.collection_info_immutable.as_ref().ok_or(
                GarbageCollectorError::InvariantViolation(
                    "Expected collection_info_immutable to be set".to_string(),
                ),
            )?;

            // Spawn task to mark versions as deleted
            let versions_to_mark = versions
                .iter()
                .filter_map(|(version, action)| {
                    if *action == CollectionVersionAction::Delete {
                        Some(*version)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let mark_deleted_versions_task = wrap(
                Box::new(MarkVersionsAtSysDbOperator {}),
                MarkVersionsAtSysDbInput {
                    version_file: version_file.clone(),
                    versions_to_delete: VersionListForCollection {
                        collection_id: collection_id.to_string(),
                        versions: versions_to_mark,
                        tenant_id: collection_info.tenant_id.clone(),
                        database_id: collection_info.database_id.clone(),
                    },
                    // TODO(@codetheweb): remove unused fields
                    sysdb_client: self.sysdb_client.clone(),
                    epoch_id: 0,
                    oldest_version_to_keep: 0,
                },
                ctx.receiver(),
            );
            self.dispatcher()
                .send(mark_deleted_versions_task, Some(Span::current()))
                .await
                .map_err(GarbageCollectorError::Channel)?;

            for version in versions.keys() {
                let task = wrap(
                    Box::new(ListFilesAtVersionsOperator {}),
                    ListFilesAtVersionInput::new(
                        self.root_manager.clone(),
                        version_file.clone(),
                        *version,
                    ),
                    ctx.receiver(),
                );

                self.dispatcher()
                    .send(task, Some(Span::current()))
                    .await
                    .map_err(GarbageCollectorError::Channel)?;
            }
        }

        self.versions_to_delete_output = Some(output);

        Ok(())
    }

    async fn handle_mark_versions_at_sysdb_output(
        &mut self,
        output: MarkVersionsAtSysDbOutput,
        ctx: &ComponentContext<Self>,
    ) -> Result<(), GarbageCollectorError> {
        let collection_id = CollectionUuid::from_str(&output.versions_to_delete.collection_id)
            .map_err(GarbageCollectorError::UnparsableUuid)?;

        self.pending_mark_versions_at_sysdb_tasks
            .remove(&collection_id);

        self.advance_after_list_files_and_mark_version_tasks_complete(ctx)
            .await?;

        Ok(())
    }

    async fn handle_list_files_at_version_output(
        &mut self,
        output: ListFilesAtVersionOutput,
        ctx: &ComponentContext<Self>,
    ) -> Result<(), GarbageCollectorError> {
        let version_action = self
            .versions_to_delete_output
            .as_ref()
            .ok_or(GarbageCollectorError::InvariantViolation(
                "Expected versions_to_delete_output to be set".to_string(),
            ))?
            .versions
            .get(&output.collection_id)
            .ok_or(GarbageCollectorError::InvariantViolation(format!(
                "Expected versions_to_delete_output to contain collection {}",
                output.collection_id
            )))?
            .get(&output.version)
            .ok_or(GarbageCollectorError::InvariantViolation(format!(
                "Expected versions_to_delete_output to contain version {} for collection {}",
                output.version, output.collection_id
            )))?;

        tracing::trace!(
            "Received ListFilesAtVersionOutput for collection {} at version {}. Action: {:?}. File paths: {:#?}",
            output.collection_id,
            output.version,
            version_action,
            output.file_paths,
        );

        // Update the file ref counts. Counts in the map should:
        // - be 0 if we know about the file but it is unused
        // - be > 0 if we know about the file and it is used
        // We accomplish this by incrementing the count for files that are used and populating the map with 0 (if the entry does not exist) for files that are unused.
        match version_action {
            CollectionVersionAction::Keep => {
                tracing::debug!(
                    "Marking {} files as used for collection {} at version {}",
                    output.file_paths.len(),
                    output.collection_id,
                    output.version
                );

                for file_path in output.file_paths {
                    let count = self.file_ref_counts.entry(file_path).or_insert(0);
                    *count += 1;
                }
            }
            CollectionVersionAction::Delete => {
                tracing::debug!(
                    "Marking {} files as unused for collection {} at version {}",
                    output.file_paths.len(),
                    output.collection_id,
                    output.version
                );

                for file_path in output.file_paths {
                    self.file_ref_counts.entry(file_path).or_insert(0);
                }
            }
        }

        self.pending_list_files_at_version_tasks
            .remove(&(output.collection_id, output.version));

        self.advance_after_list_files_and_mark_version_tasks_complete(ctx)
            .await?;

        Ok(())
    }

    async fn advance_after_list_files_and_mark_version_tasks_complete(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Result<(), GarbageCollectorError> {
        if !self.pending_list_files_at_version_tasks.is_empty() {
            return Ok(());
        }
        if !self.pending_mark_versions_at_sysdb_tasks.is_empty() {
            return Ok(());
        }

        // We now have results for all ListFilesAtVersionsOperator tasks that we spawned
        tracing::trace!("File ref counts: {:#?}", self.file_ref_counts);
        let file_paths_to_delete = self
            .file_ref_counts
            .iter()
            .filter_map(|(path, count)| {
                if *count == 0 {
                    Some(path.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let delete_percentage =
            file_paths_to_delete.len() as f32 / self.file_ref_counts.len() as f32 * 100.0;

        tracing::debug!(
            delete_percentage = delete_percentage,
            "Deleting {} files out of a total of {}",
            file_paths_to_delete.len(),
            self.file_ref_counts.len()
        );

        if file_paths_to_delete.is_empty() {
            tracing::debug!("No files to delete.");
        }

        let version_file =
            self.version_files
                .values()
                .next()
                .ok_or(GarbageCollectorError::InvariantViolation(
                    "Expected there to be at least one version file".to_string(),
                ))?;
        // Assumes that all collections in a fork tree are under the same tenant
        let tenant_id = version_file
            .collection_info_immutable
            .as_ref()
            .ok_or(GarbageCollectorError::InvariantViolation(
                "Expected collection_info_immutable to be set".to_string(),
            ))?
            .tenant_id
            .clone();

        let task = wrap(
            Box::new(DeleteUnusedFilesOperator::new(
                self.storage.clone(),
                self.cleanup_mode,
                tenant_id,
            )),
            DeleteUnusedFilesInput {
                unused_s3_files: file_paths_to_delete,
                hnsw_prefixes_for_deletion: vec![],
            },
            ctx.receiver(),
        );
        self.dispatcher()
            .send(task, Some(Span::current()))
            .await
            .map_err(GarbageCollectorError::Channel)?;

        Ok(())
    }

    async fn handle_delete_unused_files_output(
        &mut self,
        output: DeleteUnusedFilesOutput,
        ctx: &ComponentContext<Self>,
    ) -> Result<(), GarbageCollectorError> {
        if self.cleanup_mode == CleanupMode::DryRun {
            tracing::info!("Dry run mode, skipping actual deletion");
            let response = GarbageCollectorResponse {
                num_versions_deleted: 0,
                num_files_deleted: 0,
            };
            self.terminate_with_result(Ok(response), ctx).await;
            return Ok(());
        }

        self.num_files_deleted += output.deleted_files.len() as u32;

        let versions_to_delete = self.versions_to_delete_output.as_ref().ok_or(
            GarbageCollectorError::InvariantViolation(
                "Expected versions_to_delete_output to be set".to_string(),
            ),
        )?;

        let versions_to_delete = versions_to_delete
            .versions
            .iter()
            .filter_map(|(collection_id, versions)| {
                let versions = versions
                    .iter()
                    .filter_map(|(version, action)| {
                        if *action == CollectionVersionAction::Delete {
                            Some(*version)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                if versions.is_empty() {
                    None
                } else {
                    Some((*collection_id, versions))
                }
            })
            .collect::<HashMap<_, _>>();

        let total_num_versions_to_delete = versions_to_delete
            .values()
            .map(|versions| versions.len())
            .sum::<usize>();

        if total_num_versions_to_delete == 0 {
            tracing::debug!("No versions to delete");
            let response = GarbageCollectorResponse {
                num_versions_deleted: 0,
                num_files_deleted: 0,
            };
            self.terminate_with_result(Ok(response), ctx).await;
            return Ok(());
        }

        tracing::debug!(
            "Deleting {} versions from sysdb across {} collections",
            total_num_versions_to_delete,
            versions_to_delete.len()
        );
        self.num_pending_tasks += versions_to_delete.len();

        for (collection_id, versions) in versions_to_delete {
            let version_file = self
                .version_files
                .get(&collection_id)
                .ok_or(GarbageCollectorError::MissingVersionFile(collection_id))?;

            let collection_info = version_file.collection_info_immutable.as_ref().ok_or(
                GarbageCollectorError::InvariantViolation(
                    "Expected collection_info_immutable to be set".to_string(),
                ),
            )?;

            let delete_versions_task = wrap(
                Box::new(DeleteVersionsAtSysDbOperator {
                    storage: self.storage.clone(),
                }),
                DeleteVersionsAtSysDbInput {
                    version_file: version_file.clone(),
                    epoch_id: 0,
                    sysdb_client: self.sysdb_client.clone(),
                    versions_to_delete: VersionListForCollection {
                        tenant_id: collection_info.tenant_id.clone(),
                        database_id: collection_info.database_id.clone(),
                        collection_id: collection_id.to_string(),
                        versions,
                    },
                    unused_s3_files: output.deleted_files.clone(),
                },
                ctx.receiver(),
            );

            self.dispatcher()
                .send(delete_versions_task, Some(Span::current()))
                .await
                .map_err(GarbageCollectorError::Channel)?;
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<ConstructVersionGraphRequest> for GarbageCollectorOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        _: ConstructVersionGraphRequest,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        let res = self.handle_construct_version_graph_request(ctx).await;
        self.ok_or_terminate(res, ctx).await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        let res = self
            .handle_compute_versions_to_delete_output(output, ctx)
            .await;
        self.ok_or_terminate(res, ctx).await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        let res = self.handle_mark_versions_at_sysdb_output(output, ctx).await;
        self.ok_or_terminate(res, ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<ListFilesAtVersionOutput, ListFilesAtVersionError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ListFilesAtVersionOutput, ListFilesAtVersionError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        let res = self.handle_list_files_at_version_output(output, ctx).await;
        self.ok_or_terminate(res, ctx).await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        let res = self.handle_delete_unused_files_output(output, ctx).await;
        self.ok_or_terminate(res, ctx).await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        tracing::trace!("Received DeleteVersionsAtSysDbOutput: {:#?}", output);
        self.num_versions_deleted += output.versions_to_delete.versions.len() as u32;

        self.num_pending_tasks -= 1;
        if self.num_pending_tasks == 0 {
            let response = GarbageCollectorResponse {
                num_files_deleted: self.num_files_deleted,
                num_versions_deleted: self.num_versions_deleted,
            };

            self.terminate_with_result(Ok(response), ctx).await;
        }
    }
}
