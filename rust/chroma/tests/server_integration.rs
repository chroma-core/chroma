//! Integration tests for the embedded Chroma server.

#![cfg(feature = "server")]

use chroma::client::{ChromaAuthMethod, ChromaHttpClientOptions};
use chroma::server::ChromaServer;
use chroma::ChromaHttpClient;

#[tokio::test]
async fn test_local_server_starts_and_responds() {
    // Start a local server
    let server = ChromaServer::local().await.expect("Failed to start server");

    // Connect with the HTTP client
    let client = ChromaHttpClient::new(ChromaHttpClientOptions {
        endpoint: server.endpoint().parse().unwrap(),
        auth_method: ChromaAuthMethod::None,
        ..Default::default()
    });

    // Test heartbeat
    let heartbeat = client.heartbeat().await.expect("Heartbeat failed");
    assert!(heartbeat.nanosecond_heartbeat > 0);

    // Test listing collections (should be empty initially)
    let collections = client
        .list_collections(100, None)
        .await
        .expect("List collections failed");
    assert!(collections.is_empty());
}

#[tokio::test]
async fn test_create_and_delete_collection() {
    let server = ChromaServer::local().await.expect("Failed to start server");

    let client = ChromaHttpClient::new(ChromaHttpClientOptions {
        endpoint: server.endpoint().parse().unwrap(),
        auth_method: ChromaAuthMethod::None,
        ..Default::default()
    });

    // Create a collection
    let collection = client
        .create_collection("test_collection".to_string(), None, None)
        .await
        .expect("Failed to create collection");

    assert_eq!(collection.name(), "test_collection");

    // List should show one collection
    let collections = client
        .list_collections(100, None)
        .await
        .expect("List collections failed");
    assert_eq!(collections.len(), 1);

    // Delete the collection
    client
        .delete_collection("test_collection".to_string())
        .await
        .expect("Failed to delete collection");

    // Verify it's gone
    let collections = client
        .list_collections(100, None)
        .await
        .expect("List collections failed");
    assert!(collections.is_empty());
}

#[tokio::test]
async fn test_add_and_query_documents() {
    let server = ChromaServer::local().await.expect("Failed to start server");

    let client = ChromaHttpClient::new(ChromaHttpClientOptions {
        endpoint: server.endpoint().parse().unwrap(),
        auth_method: ChromaAuthMethod::None,
        ..Default::default()
    });

    // Create a collection
    let collection = client
        .create_collection("vector_test".to_string(), None, None)
        .await
        .expect("Failed to create collection");

    // Add some documents with embeddings
    let embeddings = vec![
        vec![1.0, 0.0, 0.0],
        vec![0.0, 1.0, 0.0],
        vec![0.0, 0.0, 1.0],
    ];
    let ids = vec![
        "doc1".to_string(),
        "doc2".to_string(),
        "doc3".to_string(),
    ];
    let documents = Some(vec![
        Some("First document".to_string()),
        Some("Second document".to_string()),
        Some("Third document".to_string()),
    ]);

    collection
        .add(ids, embeddings, documents, None, None)
        .await
        .expect("Failed to add documents");

    // Count documents
    let count = collection.count().await.expect("Failed to count");
    assert_eq!(count, 3);

    // Get a specific document
    let results = collection
        .get(Some(vec!["doc1".to_string()]), None, None, None, None)
        .await
        .expect("Failed to get documents");

    assert_eq!(results.ids.len(), 1);
    assert_eq!(results.ids[0], "doc1");

    // Query by vector similarity
    let query_results = collection
        .query(
            vec![vec![1.0, 0.1, 0.0]], // Query vector close to doc1
            Some(2),                   // n_results
            None,                      // where
            None,                      // where_document
            None,                      // include
        )
        .await
        .expect("Failed to query");

    // doc1 should be the closest match
    assert!(!query_results.ids.is_empty());
    assert_eq!(query_results.ids[0][0], "doc1");

    // Cleanup
    client
        .delete_collection("vector_test".to_string())
        .await
        .expect("Failed to delete collection");
}
