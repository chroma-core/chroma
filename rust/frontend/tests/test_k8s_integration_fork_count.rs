//! K8s integration tests for fork count functionality.
//!
//! These tests require a deployed Chroma cluster with gRPC sysdb.
//! They are designed to run in a Kubernetes environment where the
//! coordinator service is available.
//!
//! To run these tests locally, ensure you have a local cluster running
//! or configure the appropriate environment variables.

use chroma_config::{registry::Registry, Configurable};
use chroma_frontend::{config::FrontendServerConfig, Frontend};
use chroma_sqlite::config::SqliteDBConfig;
use chroma_system::System;
use chroma_types::{
    AddCollectionRecordsRequest, CollectionUuid, CountForksError, CreateCollectionRequest,
    DatabaseName, ForkCollectionRequest,
};
use uuid::Uuid;

const TENANT: &str = "default_tenant";
const DATABASE: &str = "default_database";

/// Check if the test is running in a K8s cluster environment.
fn is_k8s_environment() -> bool {
    // Check for kubernetes service host or a custom env var
    std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
        || std::env::var("CHROMA_K8S_TEST").is_ok()
}

/// Setup a frontend for testing.
/// Returns None if not in a K8s environment (for local SQLite which doesn't support fork_count).
async fn setup_k8s() -> Option<Frontend> {
    if !is_k8s_environment() {
        return None;
    }

    let system = System::new();
    let registry = Registry::new();
    let config = FrontendServerConfig::load();

    let frontend = Frontend::try_from_config(&(config.frontend, system), &registry)
        .await
        .expect("Failed to create frontend");

    Some(frontend)
}

/// Setup a frontend with SQLite for local testing (fork_count will return error).
async fn setup_local() -> Frontend {
    let system = System::new();
    let registry = Registry::new();
    let mut config = FrontendServerConfig::single_node_default();
    config.frontend.sqlitedb = Some(SqliteDBConfig {
        url: None,
        ..Default::default()
    });

    Frontend::try_from_config(&(config.frontend, system), &registry)
        .await
        .expect("Failed to create frontend")
}

async fn create_test_collection(
    frontend: &mut Frontend,
    name: &str,
) -> chroma_types::Collection {
    let database_name = DatabaseName::new(DATABASE).expect("database name should be valid");
    frontend
        .create_collection(
            CreateCollectionRequest::try_new(
                TENANT.to_string(),
                database_name,
                name.to_string(),
                None,
                None,
                None,
                false,
            )
            .unwrap(),
        )
        .await
        .expect("Failed to create collection")
}

async fn add_records(
    frontend: &mut Frontend,
    collection: &chroma_types::Collection,
    ids: Vec<&str>,
) {
    let num = ids.len();
    let embeddings: Vec<Vec<f32>> = (0..num).map(|i| vec![i as f32; 3]).collect();

    frontend
        .add(
            AddCollectionRecordsRequest::try_new(
                collection.tenant.clone(),
                collection.database.clone(),
                collection.collection_id,
                ids.into_iter().map(String::from).collect(),
                embeddings,
                None,
                None,
                None,
            )
            .unwrap(),
        )
        .await
        .expect("Failed to add records");
}

/// Test that fork_count returns an error in local/SQLite mode.
/// This is expected behavior since fork functionality requires the distributed sysdb.
#[tokio::test]
async fn test_fork_count_local_returns_error() {
    let mut frontend = setup_local().await;
    let collection = create_test_collection(&mut frontend, "test_fork_count_local").await;

    let result = frontend.fork_count(collection.collection_id).await;

    // In local SQLite mode, fork_count should return a Local error
    assert!(
        matches!(result, Err(CountForksError::Local)),
        "Expected CountForksError::Local, got {:?}",
        result
    );
}

/// Test that fork_count returns 0 for a collection with no forks.
/// This test only runs in a K8s environment with gRPC sysdb.
#[tokio::test]
async fn test_k8s_fork_count_no_forks() {
    let Some(mut frontend) = setup_k8s().await else {
        println!("Skipping test_k8s_fork_count_no_forks: not in K8s environment");
        return;
    };

    let collection_name = format!("test_fork_count_no_forks_{}", Uuid::new_v4());
    let collection = create_test_collection(&mut frontend, &collection_name).await;

    // Add some records
    add_records(&mut frontend, &collection, vec!["doc1", "doc2", "doc3"]).await;

    // Get fork count - should be 0
    let fork_count = frontend
        .fork_count(collection.collection_id)
        .await
        .expect("Failed to get fork count");

    assert_eq!(
        fork_count, 0,
        "Expected fork count to be 0 for collection with no forks"
    );
}

/// Test that fork_count returns correct count after creating forks.
/// This test only runs in a K8s environment with gRPC sysdb.
#[tokio::test]
async fn test_k8s_fork_count_after_forks() {
    let Some(mut frontend) = setup_k8s().await else {
        println!("Skipping test_k8s_fork_count_after_forks: not in K8s environment");
        return;
    };

    let collection_name = format!("test_fork_count_after_forks_{}", Uuid::new_v4());
    let collection = create_test_collection(&mut frontend, &collection_name).await;

    // Add some records
    add_records(&mut frontend, &collection, vec!["doc1"]).await;

    // Initial fork count should be 0
    let initial_count = frontend
        .fork_count(collection.collection_id)
        .await
        .expect("Failed to get initial fork count");
    assert_eq!(initial_count, 0);

    // Create 5 forks
    let num_forks = 5;
    let mut forked_collection_ids = Vec::new();

    for i in 0..num_forks {
        let fork_name = format!("{}_fork_{}", collection_name, i);

        let fork_request = ForkCollectionRequest::try_new(
            TENANT.to_string(),
            DATABASE.to_string(),
            collection.collection_id,
            fork_name,
        )
        .expect("Failed to create fork request");

        let fork_response = frontend
            .fork_collection(fork_request)
            .await
            .expect("Failed to fork collection");

        // ForkCollectionResponse is a Collection, so access collection_id directly
        forked_collection_ids.push(fork_response.collection_id);
    }

    // Fork count should now be 5
    let fork_count = frontend
        .fork_count(collection.collection_id)
        .await
        .expect("Failed to get fork count");

    assert_eq!(
        fork_count, num_forks,
        "Expected fork count to be {} after creating {} forks",
        num_forks, num_forks
    );

    // Each forked collection should also report the same fork count
    // (they share the same lineage)
    for forked_id in forked_collection_ids {
        let forked_count = frontend
            .fork_count(forked_id)
            .await
            .expect("Failed to get fork count for forked collection");

        assert_eq!(
            forked_count, num_forks,
            "Expected forked collection to have fork count {}",
            num_forks
        );
    }
}

/// Test fork_count with non-existent collection returns appropriate error.
/// This test only runs in a K8s environment with gRPC sysdb.
#[tokio::test]
async fn test_k8s_fork_count_nonexistent_collection() {
    let Some(mut frontend) = setup_k8s().await else {
        println!("Skipping test_k8s_fork_count_nonexistent_collection: not in K8s environment");
        return;
    };

    let nonexistent_id = CollectionUuid::new();

    let result = frontend.fork_count(nonexistent_id).await;

    // Should return NotFound error for non-existent collection
    assert!(
        matches!(result, Err(CountForksError::NotFound(_))),
        "Expected CountForksError::NotFound, got {:?}",
        result
    );
}
