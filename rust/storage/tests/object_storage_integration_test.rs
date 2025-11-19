//! Integration tests for ObjectStorage implementations
//!
//! This test module can be configured to test different cloud storage providers
//! (GCS, S3, Azure Blob, etc.) by changing the storage configuration.
//!
//! ## Setup Instructions:
//!
//! ### For GCS (Google Cloud Storage):
//! 1. Set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON key:
//!    ```
//!    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
//!    ```
//! 2. Update `GCS_BUCKET_NAME` constant below with your bucket name
//!
//! ### For S3 (AWS):
//! 1. Set AWS credentials via environment or ~/.aws/credentials
//! 2. Update `S3_BUCKET_NAME` and `S3_REGION` constants below
//!
//! ## Running Tests:
//! ```
//! # Run all tests
//! cargo test --package chroma-storage --test object_storage_integration_test
//!
//! # Run specific test
//! cargo test --package chroma-storage --test object_storage_integration_test test_gcs_basic_operations
//! ```

mod utils;

use chroma_storage::config::{ObjectStorageConfig, ObjectStorageProvider};
use chroma_storage::object_storage::ObjectStorage;

// ============================================================================
// CONFIGURATION - UPDATE THESE VALUES FOR YOUR ENVIRONMENT
// ============================================================================

/// GCS bucket name for testing
const GCS_BUCKET_NAME: &str = "sandbox-sicheng";

/// Test prefix to namespace all test objects
const TEST_PREFIX: &str = "chroma-storage-test/";

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Creates an ObjectStorage instance configured for GCS
async fn create_gcs_storage() -> ObjectStorage {
    let config = ObjectStorageConfig {
        bucket: GCS_BUCKET_NAME.to_string(),
        provider: ObjectStorageProvider::GCS,
        connect_timeout_ms: 5000,
        request_timeout_ms: 60000,
        request_retry_count: 3,
        // GCS requires minimum 5MB part size for multipart uploads (except last part)
        upload_part_size_bytes: 5 * 1024 * 1024, // 5 MB
        // Keep download part size smaller for testing multipart downloads
        download_part_size_bytes: 256 * 1024, // 256 KB
    };

    ObjectStorage::new(&config)
        .await
        .expect("Failed to create GCS storage client")
}

/// Helper to generate a unique test prefix for each test to avoid conflicts
fn test_prefix(test_name: &str) -> String {
    format!("{}{}/", TEST_PREFIX, test_name)
}

// ============================================================================
// TESTS - GCS
// ============================================================================

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket access
async fn test_gcs_basic_operations() {
    let storage = create_gcs_storage().await;
    let prefix = test_prefix("gcs-basic");

    utils::test_basic_operations(&storage, &prefix).await;
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket access
async fn test_gcs_multipart_operations() {
    let storage = create_gcs_storage().await;
    let prefix = test_prefix("gcs-multipart");

    utils::test_multipart_operations(&storage, &prefix).await;
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket access
async fn test_gcs_conditional_operations() {
    let storage = create_gcs_storage().await;
    let prefix = test_prefix("gcs-conditional");

    utils::test_conditional_operations(&storage, &prefix).await;
}

// ============================================================================
// TESTS - S3 (Template - implement when needed)
// ============================================================================

// Uncomment and configure when testing S3:
//
// const S3_BUCKET_NAME: &str = "my-test-bucket";
// const S3_REGION: &str = "us-east-1";
//
// async fn create_s3_storage() -> ObjectStorage {
//     let config = ObjectStorageConfig {
//         bucket: S3_BUCKET_NAME.to_string(),
//         credentials: ObjectStorageCredentials::S3 {
//             access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
//             secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
//             region: S3_REGION.to_string(),
//         },
//         connect_timeout_ms: 5000,
//         request_timeout_ms: 60000,
//         request_retry_count: 3,
//         upload_part_size_bytes: 5 * 1024 * 1024,
//         download_part_size_bytes: 256 * 1024,
//     };
//
//     ObjectStorage::new(&config)
//         .await
//         .expect("Failed to create S3 storage client")
// }
//
// #[tokio::test]
// #[ignore]
// async fn test_s3_basic_operations() {
//     let storage = create_s3_storage().await;
//     let prefix = test_prefix("s3-basic");
//     utils::test_basic_operations(&storage, &prefix).await;
// }

// ============================================================================
// CLEANUP UTILITY (Optional - run manually to clean up test objects)
// ============================================================================

#[tokio::test]
#[ignore] // Only run manually when needed
async fn cleanup_all_test_objects() {
    let storage = create_gcs_storage().await;

    println!("Cleaning up all test objects under prefix: {}", TEST_PREFIX);

    let objects = storage
        .list_prefix(TEST_PREFIX)
        .await
        .expect("Failed to list objects");

    if objects.is_empty() {
        println!("No test objects found");
        return;
    }

    println!("Found {} test objects, deleting...", objects.len());

    let result = storage
        .delete_many(objects)
        .await
        .expect("Failed to delete objects");

    println!(
        "Cleanup complete: {} deleted, {} errors",
        result.deleted.len(),
        result.errors.len()
    );
}
