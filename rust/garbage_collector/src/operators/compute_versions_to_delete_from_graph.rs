use crate::types::{VersionGraph, VersionStatus};
use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{Operator, OperatorType};
use chroma_types::{CollectionUuid, DatabaseName};
use chrono::{DateTime, Utc};
use petgraph::visit::Topo;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct ComputeVersionsToDeleteOperator {}

#[derive(Debug)]
pub struct ComputeVersionsToDeleteInput {
    pub graph: VersionGraph,
    pub soft_deleted_collections: HashSet<CollectionUuid>,
    pub cutoff_time: DateTime<Utc>,
    pub min_versions_to_keep: u32,
    pub database_names: HashMap<CollectionUuid, DatabaseName>,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum CollectionVersionAction {
    Keep,
    Delete,
}

#[derive(Debug, Clone)]
pub struct ComputeVersionsToDeleteOutput {
    pub versions: HashMap<CollectionUuid, (DatabaseName, HashMap<i64, CollectionVersionAction>)>,
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

        let mut versions_by_collection: HashMap<
            CollectionUuid,
            Vec<(i64, DateTime<Utc>, CollectionVersionAction)>,
        > = HashMap::new();

        while let Some(node_i) = visitor.next(&input.graph) {
            let node = input
                .graph
                .node_weight(node_i)
                .ok_or(ComputeVersionsToDeleteError::MissingVersionGraphNode)?;

            match node.status {
                VersionStatus::Alive { created_at } => {
                    versions_by_collection
                        .entry(node.collection_id)
                        .or_default()
                        .push((node.version, created_at, CollectionVersionAction::Keep));
                }
                VersionStatus::Deleted => {}
            }
        }

        for (_, versions) in versions_by_collection
            .iter_mut()
            .filter(|(collection_id, _)| !input.soft_deleted_collections.contains(collection_id))
        {
            for (version, created_at, mode) in versions
                .iter_mut()
                .rev()
                .skip(input.min_versions_to_keep as usize)
            {
                if *created_at < input.cutoff_time {
                    *mode = CollectionVersionAction::Delete;
                } else {
                    tracing::debug!(
                        version = *version,
                        created_at = %created_at,
                        cutoff_time = %input.cutoff_time,
                        "Keeping version {version} created at {created_at} after cutoff time {}", input.cutoff_time
                    );
                }
            }
        }

        for (_, versions) in versions_by_collection
            .iter_mut()
            .filter(|(collection_id, _)| input.soft_deleted_collections.contains(collection_id))
        {
            for (_, _, mode) in versions.iter_mut() {
                *mode = CollectionVersionAction::Delete;
            }
        }

        Ok(ComputeVersionsToDeleteOutput {
            versions: versions_by_collection
                .into_iter()
                .filter_map(|(collection_id, versions)| {
                    let database_name = input.database_names.get(&collection_id)?.clone();
                    let versions: HashMap<_, _> = versions
                        .into_iter()
                        .map(|(version, _, mode)| (version, mode))
                        .collect();
                    let num_versions = versions.len();

                    if tracing::enabled!(tracing::Level::DEBUG) {
                        let num_versions_to_keep = versions
                            .iter()
                            .filter(|(_, action)| **action == CollectionVersionAction::Keep)
                            .count();
                        let num_versions_to_delete = versions
                            .iter()
                            .filter(|(_, action)| **action == CollectionVersionAction::Delete)
                            .count();

                        tracing::debug!(
                            collection_id = %collection_id,
                            num_versions = num_versions,
                            num_versions_to_keep,
                            num_versions_to_delete,
                            "Computed versions to delete for collection {collection_id}",
                        );
                    }

                    Some((collection_id, (database_name, versions)))
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{VersionGraphNode, VersionStatus};
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
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(48)),
            },
        });
        let v1 = graph.add_node(VersionGraphNode {
            collection_id,
            version: 1,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(24)),
            },
        });
        let v2 = graph.add_node(VersionGraphNode {
            collection_id,
            version: 2,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(12)),
            },
        });
        let v3 = graph.add_node(VersionGraphNode {
            collection_id,
            version: 3,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(1)),
            },
        });
        let v4 = graph.add_node(VersionGraphNode {
            collection_id,
            version: 4,
            status: VersionStatus::Alive { created_at: now },
        });
        graph.add_edge(v0, v1, ());
        graph.add_edge(v1, v2, ());
        graph.add_edge(v2, v3, ());
        graph.add_edge(v3, v4, ());

        let database_name = DatabaseName::new("test_db").unwrap();
        let mut database_names = HashMap::new();
        database_names.insert(collection_id, database_name);

        let input = ComputeVersionsToDeleteInput {
            graph,
            cutoff_time: now - Duration::hours(6),
            min_versions_to_keep: 1,
            soft_deleted_collections: HashSet::new(),
            database_names,
        };

        let mut result = ComputeVersionsToDeleteOperator {}
            .run(&input)
            .await
            .unwrap();

        // v0 is always kept, and the most recent version (v4) is kept. v3 is not eligible for deletion because it is after the cutoff time. So v1 and v2 are marked for deletion.
        assert_eq!(result.versions.len(), 1);
        let (_database_name, versions) = result.versions.remove(&collection_id).unwrap();
        let mut versions = versions.into_iter().collect::<Vec<_>>();
        versions.sort_by_key(|(version, _)| *version);
        assert_eq!(
            versions,
            vec![
                (0, CollectionVersionAction::Delete),
                (1, CollectionVersionAction::Delete),
                (2, CollectionVersionAction::Delete),
                (3, CollectionVersionAction::Keep),
                (4, CollectionVersionAction::Keep)
            ]
        );
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
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(48)),
            },
        });
        let a_v1 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 1,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(24)),
            },
        });
        let a_v2 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 2,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(12)),
            },
        });
        let a_v3 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 3,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(1)),
            },
        });
        let a_v4 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 4,
            status: VersionStatus::Alive { created_at: now },
        });
        graph.add_edge(a_v0, a_v1, ());
        graph.add_edge(a_v1, a_v2, ());
        graph.add_edge(a_v2, a_v3, ());
        graph.add_edge(a_v3, a_v4, ());

        let b_collection_id = CollectionUuid::new();
        let b_v0 = graph.add_node(VersionGraphNode {
            collection_id: b_collection_id,
            version: 0,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(23)),
            },
        });
        let b_v1 = graph.add_node(VersionGraphNode {
            collection_id: b_collection_id,
            version: 1,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(12)),
            },
        });
        let b_v2 = graph.add_node(VersionGraphNode {
            collection_id: b_collection_id,
            version: 2,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(1)),
            },
        });
        graph.add_edge(b_v0, b_v1, ());
        graph.add_edge(b_v1, b_v2, ());
        // B was forked from A
        graph.add_edge(a_v1, b_v0, ());

        let c_collection_id = CollectionUuid::new();
        let c_v0 = graph.add_node(VersionGraphNode {
            collection_id: c_collection_id,
            version: 0,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(1)),
            },
        });
        // C was forked from B
        graph.add_edge(b_v2, c_v0, ());

        let mut database_names = HashMap::new();
        database_names.insert(a_collection_id, DatabaseName::new("test_db").unwrap());
        database_names.insert(b_collection_id, DatabaseName::new("test_db").unwrap());
        database_names.insert(c_collection_id, DatabaseName::new("test_db").unwrap());

        let input = ComputeVersionsToDeleteInput {
            graph,
            cutoff_time: now - Duration::hours(6),
            min_versions_to_keep: 1,
            soft_deleted_collections: HashSet::new(),
            database_names,
        };

        let mut result = ComputeVersionsToDeleteOperator {}
            .run(&input)
            .await
            .unwrap();

        // For collection A: v0 is always kept, and the most recent version (v4) is kept. v3 is not eligible for deletion because it is after the cutoff time. So v1 and v2 are marked for deletion.
        let (_database_name, a_versions) = result.versions.remove(&a_collection_id).unwrap();
        let mut a_versions = a_versions.into_iter().collect::<Vec<_>>();
        a_versions.sort_by_key(|(version, _)| *version);
        assert_eq!(
            a_versions,
            vec![
                (0, CollectionVersionAction::Delete),
                (1, CollectionVersionAction::Delete),
                (2, CollectionVersionAction::Delete),
                (3, CollectionVersionAction::Keep),
                (4, CollectionVersionAction::Keep)
            ]
        );

        // For collection B: v0 is always kept, and the most recent version (v2) is kept. So v1 is marked for deletion.
        let (_database_name, b_versions) = result.versions.remove(&b_collection_id).unwrap();
        let mut b_versions = b_versions.into_iter().collect::<Vec<_>>();
        b_versions.sort_by_key(|(version, _)| *version);
        assert_eq!(
            b_versions,
            vec![
                (0, CollectionVersionAction::Delete),
                (1, CollectionVersionAction::Delete),
                (2, CollectionVersionAction::Keep)
            ]
        );

        // For collection C: v0 is always kept.
        let (_database_name, c_versions) = result.versions.remove(&c_collection_id).unwrap();
        let mut c_versions = c_versions.into_iter().collect::<Vec<_>>();
        c_versions.sort_by_key(|(version, _)| *version);
        assert_eq!(c_versions, vec![(0, CollectionVersionAction::Keep)]);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_compute_versions_to_delete_fork_tree_soft_deleted_collection() {
        let now = Utc::now();

        let a_collection_id = CollectionUuid::new();

        let mut graph = VersionGraph::new();
        let a_v0 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 0,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(48)),
            },
        });
        let a_v1 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 1,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(24)),
            },
        });
        let a_v2 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 2,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(12)),
            },
        });
        let a_v3 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 3,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(1)),
            },
        });
        let a_v4 = graph.add_node(VersionGraphNode {
            collection_id: a_collection_id,
            version: 4,
            status: VersionStatus::Alive { created_at: now },
        });
        graph.add_edge(a_v0, a_v1, ());
        graph.add_edge(a_v1, a_v2, ());
        graph.add_edge(a_v2, a_v3, ());
        graph.add_edge(a_v3, a_v4, ());

        let b_collection_id = CollectionUuid::new();
        let b_v0 = graph.add_node(VersionGraphNode {
            collection_id: b_collection_id,
            version: 0,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(23)),
            },
        });
        let b_v1 = graph.add_node(VersionGraphNode {
            collection_id: b_collection_id,
            version: 1,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(12)),
            },
        });
        let b_v2 = graph.add_node(VersionGraphNode {
            collection_id: b_collection_id,
            version: 2,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(1)),
            },
        });
        graph.add_edge(b_v0, b_v1, ());
        graph.add_edge(b_v1, b_v2, ());
        // B was forked from A
        graph.add_edge(a_v1, b_v0, ());

        let c_collection_id = CollectionUuid::new();
        let c_v0 = graph.add_node(VersionGraphNode {
            collection_id: c_collection_id,
            version: 0,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(1)),
            },
        });
        // C was forked from B
        graph.add_edge(b_v2, c_v0, ());

        let mut database_names = HashMap::new();
        database_names.insert(a_collection_id, DatabaseName::new("test_db").unwrap());
        database_names.insert(b_collection_id, DatabaseName::new("test_db").unwrap());
        database_names.insert(c_collection_id, DatabaseName::new("test_db").unwrap());

        let input = ComputeVersionsToDeleteInput {
            graph,
            cutoff_time: now - Duration::hours(6),
            min_versions_to_keep: 1,
            soft_deleted_collections: HashSet::from([b_collection_id]), // B was soft deleted
            database_names,
        };

        let mut result = ComputeVersionsToDeleteOperator {}
            .run(&input)
            .await
            .unwrap();

        // For collection A: v0 is always kept, and the most recent version (v4) is kept. v3 is not eligible for deletion because it is after the cutoff time. So v1 and v2 are marked for deletion.
        let (_database_name, a_versions) = result.versions.remove(&a_collection_id).unwrap();
        let mut a_versions = a_versions.into_iter().collect::<Vec<_>>();
        a_versions.sort_by_key(|(version, _)| *version);
        assert_eq!(
            a_versions,
            vec![
                (0, CollectionVersionAction::Delete),
                (1, CollectionVersionAction::Delete),
                (2, CollectionVersionAction::Delete),
                (3, CollectionVersionAction::Keep),
                (4, CollectionVersionAction::Keep)
            ]
        );

        // Collection B was soft deleted, so all versions are marked for deletion.
        let (_database_name, b_versions) = result.versions.remove(&b_collection_id).unwrap();
        let mut b_versions = b_versions.into_iter().collect::<Vec<_>>();
        b_versions.sort_by_key(|(version, _)| *version);
        assert_eq!(
            b_versions,
            vec![
                (0, CollectionVersionAction::Delete),
                (1, CollectionVersionAction::Delete),
                (2, CollectionVersionAction::Delete)
            ]
        );

        // For collection C: v0 is always kept.
        let (_database_name, c_versions) = result.versions.remove(&c_collection_id).unwrap();
        let mut c_versions = c_versions.into_iter().collect::<Vec<_>>();
        c_versions.sort_by_key(|(version, _)| *version);
        assert_eq!(c_versions, vec![(0, CollectionVersionAction::Keep)]);
    }

    /// Collections missing from database_names are filtered out of the output entirely.
    #[tokio::test]
    #[traced_test]
    async fn collection_missing_from_database_names_is_filtered_out() {
        let now = Utc::now();

        let collection_with_db = CollectionUuid::new();
        let collection_without_db = CollectionUuid::new();

        let mut graph = VersionGraph::new();
        let v0_with = graph.add_node(VersionGraphNode {
            collection_id: collection_with_db,
            version: 0,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(48)),
            },
        });
        let v1_with = graph.add_node(VersionGraphNode {
            collection_id: collection_with_db,
            version: 1,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(24)),
            },
        });
        graph.add_edge(v0_with, v1_with, ());

        let v0_without = graph.add_node(VersionGraphNode {
            collection_id: collection_without_db,
            version: 0,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(48)),
            },
        });
        let v1_without = graph.add_node(VersionGraphNode {
            collection_id: collection_without_db,
            version: 1,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(24)),
            },
        });
        graph.add_edge(v0_without, v1_without, ());

        let mut database_names = HashMap::new();
        database_names.insert(collection_with_db, DatabaseName::new("test_db").unwrap());
        // Deliberately not inserting collection_without_db.

        let input = ComputeVersionsToDeleteInput {
            graph,
            cutoff_time: now - Duration::hours(6),
            min_versions_to_keep: 1,
            soft_deleted_collections: HashSet::new(),
            database_names,
        };

        let result = ComputeVersionsToDeleteOperator {}
            .run(&input)
            .await
            .unwrap();

        // Only collection_with_db should be in output; collection_without_db is filtered.
        assert_eq!(result.versions.len(), 1);
        assert!(result.versions.contains_key(&collection_with_db));
        assert!(!result.versions.contains_key(&collection_without_db));
    }

    /// Verify database name in input is propagated correctly to output.
    #[tokio::test]
    #[traced_test]
    async fn database_name_propagated_to_output() {
        let now = Utc::now();
        let collection_id = CollectionUuid::new();
        let expected_db_name = DatabaseName::new("my_special_database").unwrap();

        let mut graph = VersionGraph::new();
        let v0 = graph.add_node(VersionGraphNode {
            collection_id,
            version: 0,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(48)),
            },
        });
        let v1 = graph.add_node(VersionGraphNode {
            collection_id,
            version: 1,
            status: VersionStatus::Alive { created_at: now },
        });
        graph.add_edge(v0, v1, ());

        let mut database_names = HashMap::new();
        database_names.insert(collection_id, expected_db_name.clone());

        let input = ComputeVersionsToDeleteInput {
            graph,
            cutoff_time: now - Duration::hours(6),
            min_versions_to_keep: 1,
            soft_deleted_collections: HashSet::new(),
            database_names,
        };

        let result = ComputeVersionsToDeleteOperator {}
            .run(&input)
            .await
            .unwrap();

        let (actual_db_name, _versions) = result.versions.get(&collection_id).unwrap();
        assert_eq!(*actual_db_name, expected_db_name);
    }

    /// Empty database_names results in an empty output, even with collections in graph.
    #[tokio::test]
    #[traced_test]
    async fn empty_database_names_produces_empty_output() {
        let now = Utc::now();
        let collection_id = CollectionUuid::new();

        let mut graph = VersionGraph::new();
        let v0 = graph.add_node(VersionGraphNode {
            collection_id,
            version: 0,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(48)),
            },
        });
        let v1 = graph.add_node(VersionGraphNode {
            collection_id,
            version: 1,
            status: VersionStatus::Alive { created_at: now },
        });
        graph.add_edge(v0, v1, ());

        let database_names = HashMap::new();

        let input = ComputeVersionsToDeleteInput {
            graph,
            cutoff_time: now - Duration::hours(6),
            min_versions_to_keep: 1,
            soft_deleted_collections: HashSet::new(),
            database_names,
        };

        let result = ComputeVersionsToDeleteOperator {}
            .run(&input)
            .await
            .unwrap();

        assert!(result.versions.is_empty());
    }

    /// Different collections can have different database names.
    #[tokio::test]
    #[traced_test]
    async fn different_database_names_for_different_collections() {
        let now = Utc::now();

        let collection_a = CollectionUuid::new();
        let collection_b = CollectionUuid::new();
        let db_name_a = DatabaseName::new("database_alpha").unwrap();
        let db_name_b = DatabaseName::new("database_beta").unwrap();

        let mut graph = VersionGraph::new();
        let a_v0 = graph.add_node(VersionGraphNode {
            collection_id: collection_a,
            version: 0,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(48)),
            },
        });
        let a_v1 = graph.add_node(VersionGraphNode {
            collection_id: collection_a,
            version: 1,
            status: VersionStatus::Alive { created_at: now },
        });
        graph.add_edge(a_v0, a_v1, ());

        let b_v0 = graph.add_node(VersionGraphNode {
            collection_id: collection_b,
            version: 0,
            status: VersionStatus::Alive {
                created_at: (now - Duration::hours(48)),
            },
        });
        let b_v1 = graph.add_node(VersionGraphNode {
            collection_id: collection_b,
            version: 1,
            status: VersionStatus::Alive { created_at: now },
        });
        graph.add_edge(b_v0, b_v1, ());

        let mut database_names = HashMap::new();
        database_names.insert(collection_a, db_name_a.clone());
        database_names.insert(collection_b, db_name_b.clone());

        let input = ComputeVersionsToDeleteInput {
            graph,
            cutoff_time: now - Duration::hours(6),
            min_versions_to_keep: 1,
            soft_deleted_collections: HashSet::new(),
            database_names,
        };

        let result = ComputeVersionsToDeleteOperator {}
            .run(&input)
            .await
            .unwrap();

        assert_eq!(result.versions.len(), 2);

        let (actual_db_a, _) = result.versions.get(&collection_a).unwrap();
        assert_eq!(*actual_db_a, db_name_a);

        let (actual_db_b, _) = result.versions.get(&collection_b).unwrap();
        assert_eq!(*actual_db_b, db_name_b);
    }
}
