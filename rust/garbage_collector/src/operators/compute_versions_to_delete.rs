use crate::construct_version_graph_orchestrator::VersionGraph;
use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{Operator, OperatorType};
use chroma_types::CollectionUuid;
use chrono::{DateTime, Utc};
use petgraph::visit::Topo;
use std::collections::{HashMap, VecDeque};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct ComputeVersionsToDeleteOperator {}

#[derive(Debug)]
pub struct ComputeVersionsToDeleteInput {
    pub graph: VersionGraph,
    pub cutoff_time: DateTime<Utc>,
    pub min_versions_to_keep: u32,
}

#[derive(Debug)]
pub struct ComputeVersionsToDeleteOutput {
    pub versions_to_delete: HashMap<CollectionUuid, Vec<i64>>,
}

#[derive(Error, Debug)]
pub enum ComputeVersionsToDeleteError {
    #[error("Error computing versions to delete: {0}")]
    ComputeError(String),
    #[error("Invalid timestamp in version file")]
    InvalidTimestamp,
    #[error("Error parsing version file: {0}")]
    ParseError(#[from] prost::DecodeError),
    #[error("Graph is missing expected node")]
    MissingVersionGraphNode,
}

impl ChromaError for ComputeVersionsToDeleteError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<ComputeVersionsToDeleteInput, ComputeVersionsToDeleteOutput>
    for ComputeVersionsToDeleteOperator
{
    type Error = ComputeVersionsToDeleteError;

    fn get_type(&self) -> OperatorType {
        OperatorType::Other
    }

    async fn run(
        &self,
        input: &ComputeVersionsToDeleteInput,
    ) -> Result<ComputeVersionsToDeleteOutput, ComputeVersionsToDeleteError> {
        let mut visitor = Topo::new(&input.graph);

        let mut collection_versions_to_delete: HashMap<
            CollectionUuid,
            VecDeque<(i64, DateTime<Utc>)>,
        > = HashMap::new();
        while let Some(node_i) = visitor.next(&input.graph) {
            let node = input
                .graph
                .node_weight(node_i)
                .ok_or(ComputeVersionsToDeleteError::MissingVersionGraphNode)?;

            collection_versions_to_delete
                .entry(node.collection_id)
                .or_default()
                .push_back((node.version, node.created_at));
        }

        for versions in collection_versions_to_delete.values_mut() {
            if let Some((version, _)) = versions.front() {
                // Always keep version 0
                if *version == 0 {
                    versions.pop_front();
                }
            }

            for _ in 0..input.min_versions_to_keep {
                versions.pop_back();
            }

            *versions = versions
                .iter()
                .filter(|(_, created_at)| *created_at < input.cutoff_time)
                .map(|(version, created_at)| (*version, *created_at))
                .collect();
        }

        Ok(ComputeVersionsToDeleteOutput {
            versions_to_delete: collection_versions_to_delete
                .into_iter()
                .filter(|(_, versions)| !versions.is_empty())
                .map(|(collection_id, versions)| {
                    let versions: Vec<_> =
                        versions.into_iter().map(|(version, _)| version).collect();
                    let num_versions = versions.len();
                    tracing::debug!(
                        collection_id = %collection_id,
                        versions_to_delete = ?versions,
                        "Deleting {num_versions} versions from collection {collection_id}"
                    );

                    (collection_id, versions)
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::construct_version_graph_orchestrator::VersionGraphNode;
    use chrono::{Duration, Utc};
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_compute_versions_to_delete() {
        let now = Utc::now();

        let collection_id = CollectionUuid::new();

        let mut graph = VersionGraph::new();
        let v0 = graph.add_node(VersionGraphNode {
            collection_id,
            version: 0,
            created_at: (now - Duration::hours(48)),
        });
        let v1 = graph.add_node(VersionGraphNode {
            collection_id,
            version: 1,
            created_at: (now - Duration::hours(24)),
        });
        let v2 = graph.add_node(VersionGraphNode {
            collection_id,
            version: 2,
            created_at: (now - Duration::hours(12)),
        });
        let v3 = graph.add_node(VersionGraphNode {
            collection_id,
            version: 3,
            created_at: (now - Duration::hours(1)),
        });
        let v4 = graph.add_node(VersionGraphNode {
            collection_id,
            version: 4,
            created_at: now,
        });
        graph.add_edge(v0, v1, ());
        graph.add_edge(v1, v2, ());
        graph.add_edge(v2, v3, ());
        graph.add_edge(v3, v4, ());

        let input = ComputeVersionsToDeleteInput {
            graph,
            cutoff_time: now - Duration::hours(6),
            min_versions_to_keep: 1,
        };

        let result = ComputeVersionsToDeleteOperator {}
            .run(&input)
            .await
            .unwrap();

        // v0 is always kept, and the most recent version (v4) is kept. v3 is not eligible for deletion because it is after the cutoff time. So v1 and v2 are marked for deletion.
        assert_eq!(result.versions_to_delete.len(), 1);
        let versions_to_delete = result.versions_to_delete.get(&collection_id).unwrap();
        assert_eq!(versions_to_delete.len(), 2);
        assert!(versions_to_delete.contains(&1));
        assert!(versions_to_delete.contains(&2));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_compute_versions_to_delete_fork_tree() {
        let now = Utc::now();

        let a_collection_id = CollectionUuid::new();

        let mut graph = VersionGraph::new();
        let a_v0 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 0,
            created_at: (now - Duration::hours(48)),
        });
        let a_v1 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 1,
            created_at: (now - Duration::hours(24)),
        });
        let a_v2 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 2,
            created_at: (now - Duration::hours(12)),
        });
        let a_v3 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 3,
            created_at: (now - Duration::hours(1)),
        });
        let a_v4 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 4,
            created_at: now,
        });
        graph.add_edge(a_v0, a_v1, ());
        graph.add_edge(a_v1, a_v2, ());
        graph.add_edge(a_v2, a_v3, ());
        graph.add_edge(a_v3, a_v4, ());

        let b_collection_id = CollectionUuid::new();
        let b_v0 = graph.add_node(VersionGraphNode {
            collection_id: b_collection_id,
            version: 0,
            created_at: (now - Duration::hours(23)),
        });
        let b_v1 = graph.add_node(VersionGraphNode {
            collection_id: b_collection_id,
            version: 1,
            created_at: (now - Duration::hours(12)),
        });
        let b_v2 = graph.add_node(VersionGraphNode {
            collection_id: b_collection_id,
            version: 2,
            created_at: (now - Duration::hours(1)),
        });
        graph.add_edge(b_v0, b_v1, ());
        graph.add_edge(b_v1, b_v2, ());
        // B was forked from A
        graph.add_edge(a_v1, b_v0, ());

        let c_collection_id = CollectionUuid::new();
        let c_v0 = graph.add_node(VersionGraphNode {
            collection_id: c_collection_id,
            version: 0,
            created_at: (now - Duration::hours(1)),
        });
        // C was forked from B
        graph.add_edge(b_v2, c_v0, ());

        let input = ComputeVersionsToDeleteInput {
            graph,
            cutoff_time: now - Duration::hours(6),
            min_versions_to_keep: 1,
        };

        let result = ComputeVersionsToDeleteOperator {}
            .run(&input)
            .await
            .unwrap();

        // Only collections A and B should have versions to delete
        assert_eq!(result.versions_to_delete.len(), 2);

        // For collection A: v0 is always kept, and the most recent version (v4) is kept. v3 is not eligible for deletion because it is after the cutoff time. So v1 and v2 are marked for deletion.
        let a_versions_to_delete = result.versions_to_delete.get(&a_collection_id).unwrap();
        assert_eq!(a_versions_to_delete.len(), 2);
        assert!(a_versions_to_delete.contains(&1));
        assert!(a_versions_to_delete.contains(&2));

        // For collection B: v0 is always kept, and the most recent version (v2) is kept. So v1 is marked for deletion.
        let b_versions_to_delete = result.versions_to_delete.get(&b_collection_id).unwrap();
        assert_eq!(b_versions_to_delete.len(), 1);
        assert!(b_versions_to_delete.contains(&1));
    }
}
