use crate::{
    operators::{
        fetch_lineage_file::{
            FetchLineageFileError, FetchLineageFileInput, FetchLineageFileOperator,
            FetchLineageFileOutput,
        },
        fetch_version_file::{
            FetchVersionFileError, FetchVersionFileInput, FetchVersionFileOperator,
            FetchVersionFileOutput,
        },
        get_version_file_paths::{
            GetVersionFilePathsError, GetVersionFilePathsInput, GetVersionFilePathsOperator,
            GetVersionFilePathsOutput,
        },
    },
    types::{VersionGraph, VersionGraphNode, VersionStatus},
};
use async_trait::async_trait;
use base64::{prelude::BASE64_STANDARD, Engine};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_sysdb::SysDb;
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{chroma_proto::CollectionVersionFile, CollectionUuid};
use chrono::DateTime;
use petgraph::{dot::Dot, graph::DiGraph};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

#[derive(Debug, Clone)]
struct VersionDependency {
    source_collection_id: CollectionUuid,
    source_collection_version: i64,
    target_collection_id: CollectionUuid,
}

#[derive(Debug)]
pub struct ConstructVersionGraphOrchestrator {
    context: OrchestratorContext,
    result_channel:
        Option<Sender<Result<ConstructVersionGraphResponse, ConstructVersionGraphError>>>,
    storage: Storage,
    sysdb: SysDb,

    collection_id: CollectionUuid,
    version_file_path: String,
    lineage_file_path: Option<String>,

    version_dependencies: Vec<VersionDependency>,
    version_files: HashMap<CollectionUuid, Arc<CollectionVersionFile>>,
    num_pending_tasks: usize,
}

impl ConstructVersionGraphOrchestrator {
    #[allow(dead_code)]
    pub fn new(
        dispatcher: ComponentHandle<Dispatcher>,
        storage: Storage,
        sysdb: SysDb,
        collection_id: CollectionUuid,
        version_file_path: String,
        lineage_file_path: Option<String>,
    ) -> Self {
        Self {
            context: OrchestratorContext::new(dispatcher),
            storage,
            sysdb,
            result_channel: None,
            collection_id,
            version_file_path,
            lineage_file_path,

            version_dependencies: Vec::new(),
            version_files: HashMap::new(),
            num_pending_tasks: 0,
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ConstructVersionGraphResponse {
    pub version_files: HashMap<CollectionUuid, Arc<CollectionVersionFile>>,
    pub graph: VersionGraph,
}

#[derive(Debug, Error)]
pub enum ConstructVersionGraphError {
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
    #[error("Aborted")]
    Aborted,

    #[error("Error fetching version file: {0}")]
    FetchVersionFile(#[from] FetchVersionFileError),
    #[error("Error fetching lineage file: {0}")]
    FetchLineageFile(#[from] FetchLineageFileError),
    #[error("Error fetching version file paths: {0}")]
    FetchVersionFilePaths(#[from] GetVersionFilePathsError),

    #[error("Invalid UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(i64),
    #[error("Expected node not found while constructing graph (collection {0}@v{1:?})")]
    ExpectedNodeNotFound(CollectionUuid, Option<i64>),
    #[error("Invariant violation: {0}")]
    InvariantViolation(String),
}

impl<E> From<TaskError<E>> for ConstructVersionGraphError
where
    E: Into<ConstructVersionGraphError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => ConstructVersionGraphError::Panic(e),
            TaskError::TaskFailed(e) => e.into(),
            TaskError::Aborted => ConstructVersionGraphError::Aborted,
        }
    }
}

impl ChromaError for ConstructVersionGraphError {
    fn code(&self) -> ErrorCodes {
        match self {
            ConstructVersionGraphError::Channel(_) => ErrorCodes::Internal,
            ConstructVersionGraphError::Result(_) => ErrorCodes::Internal,
            ConstructVersionGraphError::Panic(_) => ErrorCodes::Internal,
            ConstructVersionGraphError::Aborted => ErrorCodes::Aborted,
            ConstructVersionGraphError::FetchVersionFile(err) => err.code(),
            ConstructVersionGraphError::FetchLineageFile(err) => err.code(),
            ConstructVersionGraphError::FetchVersionFilePaths(err) => err.code(),
            ConstructVersionGraphError::InvalidUuid(_) => ErrorCodes::Internal,
            ConstructVersionGraphError::InvalidTimestamp(_) => ErrorCodes::InvalidArgument,
            ConstructVersionGraphError::ExpectedNodeNotFound(_, _) => ErrorCodes::Internal,
            ConstructVersionGraphError::InvariantViolation(_) => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Orchestrator for ConstructVersionGraphOrchestrator {
    type Output = ConstructVersionGraphResponse;
    type Error = ConstructVersionGraphError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.context.dispatcher.clone()
    }

    fn context(&self) -> &OrchestratorContext {
        &self.context
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        tracing::debug!(
            path = %self.version_file_path,
            "Creating initial fetch version file task"
        );

        let mut tasks = vec![(
            wrap(
                Box::new(FetchVersionFileOperator {}),
                FetchVersionFileInput::new(self.version_file_path.clone(), self.storage.clone()),
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            ),
            Some(Span::current()),
        )];

        if let Some(lineage_file_path) = &self.lineage_file_path {
            tasks.push((
                wrap(
                    Box::new(FetchLineageFileOperator {}),
                    FetchLineageFileInput::new(self.storage.clone(), lineage_file_path.clone()),
                    ctx.receiver(),
                    self.context.task_cancellation_token.clone(),
                ),
                Some(Span::current()),
            ));
        }

        self.num_pending_tasks = tasks.len();

        tasks
    }

    fn set_result_channel(&mut self, sender: Sender<Result<Self::Output, Self::Error>>) {
        self.result_channel = Some(sender);
    }

    fn take_result_channel(&mut self) -> Option<Sender<Result<Self::Output, Self::Error>>> {
        self.result_channel.take()
    }
}

impl ConstructVersionGraphOrchestrator {
    async fn finish_if_no_pending_tasks(
        &mut self,
        ctx: &ComponentContext<ConstructVersionGraphOrchestrator>,
    ) -> Result<(), ConstructVersionGraphError> {
        if self.num_pending_tasks == 0 {
            // This map will be used as a basis for building the graph
            let mut versions_by_collection_id: HashMap<CollectionUuid, Vec<(i64, VersionStatus)>> =
                HashMap::new();

            // Add all known versions from version files to map
            for (collection_id, version_file) in self.version_files.iter() {
                if let Some(versions) = &version_file.version_history {
                    for version in versions.versions.iter() {
                        versions_by_collection_id
                            .entry(*collection_id)
                            .or_default()
                            .push((
                                version.version,
                                VersionStatus::Alive {
                                    created_at: DateTime::from_timestamp(
                                        version.created_at_secs,
                                        0,
                                    )
                                    .ok_or(
                                        ConstructVersionGraphError::InvalidTimestamp(
                                            version.created_at_secs,
                                        ),
                                    )?,
                                },
                            ));
                    }
                }
            }

            // If any version appears as a version dependency (from the lineage file) but does not already exist in the map from the version files, the version must have been deleted.
            for dependency in self.version_dependencies.iter() {
                let source_collection_id = dependency.source_collection_id;
                let source_collection_version = dependency.source_collection_version;

                let source_versions = versions_by_collection_id
                    .entry(source_collection_id)
                    .or_default();

                // Add source version if it does not exist
                if !source_versions
                    .iter()
                    .any(|(v, _)| *v == source_collection_version)
                {
                    source_versions.push((source_collection_version, VersionStatus::Deleted));
                }

                // Add target version if it does not exist
                let target_versions = versions_by_collection_id
                    .entry(dependency.target_collection_id)
                    .or_default();

                if !target_versions.iter().any(|(v, _)| *v == 0) {
                    target_versions.push((0, VersionStatus::Deleted));
                }
            }

            // Sort
            for versions in versions_by_collection_id.values_mut() {
                versions.sort_unstable_by_key(|v| v.0);
            }

            tracing::trace!(
                "Versions by collection ID: {:#?}",
                versions_by_collection_id
            );

            let mut graph = DiGraph::new();
            for (collection_id, versions) in versions_by_collection_id.iter() {
                let mut prev_node = None;
                for (version, status) in versions.iter() {
                    let node = graph.add_node(VersionGraphNode {
                        collection_id: *collection_id,
                        version: *version,
                        status: *status,
                    });
                    if let Some(prev) = prev_node {
                        // Add edge between each successive pair of collection versions
                        graph.add_edge(prev, node, ());
                    }
                    prev_node = Some(node);
                }
            }

            // Add edges for forked collections
            for dependency in self.version_dependencies.iter() {
                let source_node = graph
                    .node_indices()
                    .find(|n| {
                        let node = graph.node_weight(*n).expect("node index should exist");
                        node.collection_id == dependency.source_collection_id
                            && node.version == dependency.source_collection_version
                    })
                    .ok_or_else(|| {
                        ConstructVersionGraphError::ExpectedNodeNotFound(
                            dependency.source_collection_id,
                            Some(dependency.source_collection_version),
                        )
                    })?;

                let target_node = graph
                    .node_indices()
                    .find(|n| {
                        let node = graph.node_weight(*n).expect("node index should exist");
                        node.collection_id == dependency.target_collection_id && node.version == 0
                    })
                    .ok_or_else(|| {
                        ConstructVersionGraphError::ExpectedNodeNotFound(
                            dependency.target_collection_id,
                            None,
                        )
                    })?;

                graph.add_edge(source_node, target_node, ());
            }

            if tracing::level_enabled!(tracing::Level::TRACE) {
                let dot_viz = Dot::with_config(&graph, &[petgraph::dot::Config::EdgeNoLabel]);
                let encoded = BASE64_STANDARD.encode(format!("{:?}", dot_viz));
                tracing::trace!(base64_encoded_dot_graph = ?encoded, "Constructed graph.");
            }

            tracing::trace!("Version files: {:#?}", self.version_files);

            let components = petgraph::algo::connected_components(&graph);
            if components != 1 {
                // This is a defensive check, it should never happen
                return Err(ConstructVersionGraphError::InvariantViolation(format!(
                    "Graph is not fully connected, found {} components",
                    components
                )));
            }

            self.terminate_with_result(
                Ok(ConstructVersionGraphResponse {
                    graph,
                    version_files: self.version_files.clone(),
                }),
                ctx,
            )
            .await;
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<TaskResult<FetchVersionFileOutput, FetchVersionFileError>>
    for ConstructVersionGraphOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchVersionFileOutput, FetchVersionFileError>,
        ctx: &ComponentContext<ConstructVersionGraphOrchestrator>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => {
                tracing::error!("Failed to get version file output");
                return;
            }
        };
        let collection_id = output.collection_id;
        self.version_files.insert(collection_id, output.file);

        self.num_pending_tasks -= 1;

        let res = self.finish_if_no_pending_tasks(ctx).await;
        self.ok_or_terminate(res, ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<FetchLineageFileOutput, FetchLineageFileError>>
    for ConstructVersionGraphOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchLineageFileOutput, FetchLineageFileError>,
        ctx: &ComponentContext<ConstructVersionGraphOrchestrator>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => {
                return;
            }
        };

        let mut collection_ids_to_fetch_version_files = HashSet::new();
        for dependency in output.0.dependencies {
            let source_collection_id = match self
                .ok_or_terminate(
                    CollectionUuid::from_str(&dependency.source_collection_id)
                        .map_err(ConstructVersionGraphError::InvalidUuid),
                    ctx,
                )
                .await
            {
                Some(id) => id,
                None => {
                    return;
                }
            };

            let target_collection_id = match self
                .ok_or_terminate(
                    CollectionUuid::from_str(&dependency.target_collection_id)
                        .map_err(ConstructVersionGraphError::InvalidUuid),
                    ctx,
                )
                .await
            {
                Some(id) => id,
                None => {
                    return;
                }
            };

            self.version_dependencies.push(VersionDependency {
                source_collection_id,
                source_collection_version: dependency.source_collection_version as i64,
                target_collection_id,
            });

            if source_collection_id != self.collection_id {
                collection_ids_to_fetch_version_files.insert(source_collection_id);
            }
            if target_collection_id != self.collection_id {
                collection_ids_to_fetch_version_files.insert(target_collection_id);
            }
        }

        if !collection_ids_to_fetch_version_files.is_empty() {
            self.num_pending_tasks += 1;
            let list_files_at_versions_task = wrap(
                Box::new(GetVersionFilePathsOperator {}),
                GetVersionFilePathsInput::new(
                    collection_ids_to_fetch_version_files.into_iter().collect(),
                    self.sysdb.clone(),
                ),
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );

            if let Err(e) = self
                .dispatcher()
                .send(list_files_at_versions_task, Some(Span::current()))
                .await
            {
                self.terminate_with_result(Err(ConstructVersionGraphError::Channel(e)), ctx)
                    .await;
                return;
            }
        }

        self.num_pending_tasks -= 1;
        let res = self.finish_if_no_pending_tasks(ctx).await;
        self.ok_or_terminate(res, ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<GetVersionFilePathsOutput, GetVersionFilePathsError>>
    for ConstructVersionGraphOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<GetVersionFilePathsOutput, GetVersionFilePathsError>,
        ctx: &ComponentContext<ConstructVersionGraphOrchestrator>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => {
                return;
            }
        };

        self.num_pending_tasks += output.0.len();

        for path in output.0.values() {
            let version_file = FetchVersionFileInput::new(path.clone(), self.storage.clone());
            let fetch_version_file_task = wrap(
                Box::new(FetchVersionFileOperator {}),
                version_file,
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );

            if let Err(e) = self
                .dispatcher()
                .send(fetch_version_file_task, Some(Span::current()))
                .await
            {
                self.terminate_with_result(Err(ConstructVersionGraphError::Channel(e)), ctx)
                    .await;
                return;
            }
        }

        self.num_pending_tasks -= 1;
        let res = self.finish_if_no_pending_tasks(ctx).await;
        self.ok_or_terminate(res, ctx).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_storage::test_storage;
    use chroma_sysdb::TestSysDb;
    use chroma_system::{DispatcherConfig, System};
    use chroma_types::chroma_proto::{
        CollectionInfoImmutable, CollectionLineageFile, CollectionVersionDependency,
        CollectionVersionFile, CollectionVersionHistory, CollectionVersionInfo,
    };
    use prost::Message;
    use tracing_test::traced_test;

    async fn create_version_file(
        collection_id: CollectionUuid,
        versions: Vec<i64>,
        storage: Storage,
    ) -> String {
        let version_file = CollectionVersionFile {
            collection_info_immutable: Some(CollectionInfoImmutable {
                tenant_id: "test_tenant".to_string(),
                database_id: "test_db".to_string(),
                collection_id: collection_id.to_string(),
                dimension: 0,
                ..Default::default()
            }),
            version_history: Some(CollectionVersionHistory {
                versions: versions
                    .into_iter()
                    .zip(0..)
                    .map(|(version, created_at_secs)| CollectionVersionInfo {
                        version,
                        created_at_secs,
                        marked_for_deletion: false,
                        ..Default::default()
                    })
                    .collect(),
            }),
        };

        let version_file_path = format!("test_version_file_{}.bin", collection_id);
        storage
            .put_bytes(
                &version_file_path,
                version_file.encode_to_vec(),
                chroma_storage::PutOptions::default(),
            )
            .await
            .unwrap();

        version_file_path
    }

    #[tokio::test]
    #[traced_test]
    async fn test_simple_graph() {
        let (_storage_dir, storage) = test_storage();

        let system = System::new();
        let sysdb = SysDb::Test(TestSysDb::new());
        let dispatcher = Dispatcher::new(DispatcherConfig::default());
        let dispatcher_handle = system.start_component(dispatcher);

        let collection_id = CollectionUuid::new();
        let version_file_path =
            create_version_file(collection_id, vec![1, 2], storage.clone()).await;

        let orchestrator = ConstructVersionGraphOrchestrator::new(
            dispatcher_handle,
            storage,
            sysdb,
            CollectionUuid::new(),
            version_file_path.to_string(),
            None,
        );

        let result = orchestrator.run(system).await.unwrap();

        assert_eq!(result.graph.node_count(), 2);
        assert_eq!(result.graph.edge_count(), 1);

        let edges: Vec<_> = result
            .graph
            .raw_edges()
            .iter()
            .map(|edge| {
                let source_node = result.graph.node_weight(edge.source()).unwrap();
                let target_node = result.graph.node_weight(edge.target()).unwrap();

                (source_node.version, target_node.version)
            })
            .collect();
        assert_eq!(edges, vec![(1, 2)]);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_graph_with_lineage() {
        let (_storage_dir, storage) = test_storage();

        let system = System::new();
        let mut sysdb = SysDb::Test(TestSysDb::new());
        let dispatcher = Dispatcher::new(DispatcherConfig::default());
        let dispatcher_handle = system.start_component(dispatcher);

        /*
         * Test graph:
         *                A v0
         *                 |
         *                A v1
         *                /   \
         *              B v0  C v0
         *               |
         *              B v1
         *               |
         *              D v0
         */

        let collection_id_a = CollectionUuid::new();
        let collection_id_b = CollectionUuid::new();
        let collection_id_c = CollectionUuid::new();
        let collection_id_d = CollectionUuid::new();

        for collection_id in [
            collection_id_a,
            collection_id_b,
            collection_id_c,
            collection_id_d,
        ] {
            sysdb
                .create_collection(
                    "test_tenant".to_string(),
                    "test_database".to_string(),
                    collection_id,
                    format!("test_collection_{}", collection_id),
                    vec![],
                    None,
                    None,
                    None,
                    None,
                    false,
                )
                .await
                .unwrap();
        }

        let version_file_a_path =
            create_version_file(collection_id_a, vec![0, 1], storage.clone()).await;
        let version_file_b_path =
            create_version_file(collection_id_b, vec![0, 1], storage.clone()).await;
        let version_file_c_path =
            create_version_file(collection_id_c, vec![0], storage.clone()).await;
        let version_file_d_path =
            create_version_file(collection_id_d, vec![0], storage.clone()).await;

        match sysdb {
            SysDb::Test(ref mut test) => {
                test.set_collection_version_file_path(collection_id_a, version_file_a_path.clone());
                test.set_collection_version_file_path(collection_id_b, version_file_b_path.clone());
                test.set_collection_version_file_path(collection_id_c, version_file_c_path.clone());
                test.set_collection_version_file_path(collection_id_d, version_file_d_path.clone());
            }
            _ => panic!("Invalid sysdb"),
        }

        let lineage_file_a = CollectionLineageFile {
            dependencies: vec![
                CollectionVersionDependency {
                    source_collection_id: collection_id_a.to_string(),
                    source_collection_version: 1,
                    target_collection_id: collection_id_b.to_string(),
                },
                CollectionVersionDependency {
                    source_collection_id: collection_id_b.to_string(),
                    source_collection_version: 1,
                    target_collection_id: collection_id_d.to_string(),
                },
                CollectionVersionDependency {
                    source_collection_id: collection_id_a.to_string(),
                    source_collection_version: 1,
                    target_collection_id: collection_id_c.to_string(),
                },
            ],
        };
        let lineage_file_a_path = format!("test_lineage_file_{}.bin", collection_id_a);
        storage
            .put_bytes(
                &lineage_file_a_path,
                lineage_file_a.encode_to_vec(),
                chroma_storage::PutOptions::default(),
            )
            .await
            .unwrap();

        let expected_nodes = vec![
            (collection_id_a, 0),
            (collection_id_a, 1),
            (collection_id_b, 0),
            (collection_id_b, 1),
            (collection_id_c, 0),
            (collection_id_d, 0),
        ];

        fn check_graph(graph: &VersionGraph, mut expected_nodes: Vec<(CollectionUuid, i64)>) {
            assert_eq!(graph.node_count(), 6);
            assert_eq!(graph.edge_count(), 5);

            let mut expected_edges = vec![
                (0, 1), // A
                (1, 0), // B
                (1, 0), // C
                (1, 0), // D
                (0, 1), // D
            ];
            expected_edges.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

            let mut edges: Vec<_> = graph
                .raw_edges()
                .iter()
                .map(|edge| {
                    let source_node = graph.node_weight(edge.source()).unwrap();
                    let target_node = graph.node_weight(edge.target()).unwrap();

                    (source_node.version, target_node.version)
                })
                .collect();
            edges.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            assert_eq!(edges, expected_edges,);

            let mut nodes: Vec<_> = graph
                .node_weights()
                .map(|node| (node.collection_id, node.version))
                .collect();
            nodes.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            expected_nodes.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            assert_eq!(nodes, expected_nodes,);
        }

        // Starting construction of the graph at any point in the graph should yield the same graph
        for collection_id in [
            collection_id_a,
            collection_id_b,
            collection_id_c,
            collection_id_d,
        ] {
            let version_file_path = match sysdb {
                SysDb::Test(ref mut test) => test.get_version_file_name(collection_id),
                _ => panic!("Invalid sysdb"),
            };

            let orchestrator = ConstructVersionGraphOrchestrator::new(
                dispatcher_handle.clone(),
                storage.clone(),
                sysdb.clone(),
                collection_id,
                version_file_path,
                Some(lineage_file_a_path.clone()),
            );

            let result = orchestrator.run(system.clone()).await.unwrap();
            check_graph(&result.graph, expected_nodes.clone());
        }
    }
}
