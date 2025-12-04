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
//! ### For CMEK (Customer-Managed Encryption Keys):
//! 1. Follow GCS setup above
//! 2. Update `GCS_CMEK_KEY_NAME` constant with your KMS key resource name
//! 3. Ensure your service account has `cloudkms.cryptoKeyVersions.useToEncrypt` permission
//!
//! ## Running Tests:
//! ```
//! # Run all tests
//! cargo test --package chroma-storage --test object_storage_integration_test
//!
//! # Run specific test
//! cargo test --package chroma-storage --test object_storage_integration_test test_gcs_basic_operations
//!
//! # Run CMEK tests
//! cargo test --package chroma-storage --test object_storage_integration_test cmek -- --ignored
//!
//! # Run negative CMEK test (proves header is sent)
//! cargo test --package chroma-storage --test object_storage_integration_test test_gcs_cmek_invalid_key_fails -- --ignored
//! ```

mod utils;

use chroma_storage::admissioncontrolleds3::AdmissionControlledS3Storage;
use chroma_storage::config::{ObjectStorageConfig, ObjectStorageProvider};
use chroma_storage::object_storage::ObjectStorage;
use chroma_storage::Storage;

// ============================================================================
// CONFIGURATION - UPDATE THESE VALUES FOR YOUR ENVIRONMENT
// ============================================================================

/// GCS bucket name for testing
const GCS_BUCKET_NAME: &str = "storage-client-test";

/// Test prefix to namespace all test objects
const TEST_PREFIX: &str = "object-store/";

/// CMEK key resource name for testing
/// Update this with your actual KMS key for CMEK tests
/// Format: projects/PROJECT_ID/locations/LOCATION/keyRings/RING/cryptoKeys/KEY
const GCS_CMEK_KEY_NAME: &str = GCS_INVALID_CMEK_KEY_NAME;

/// Fake CMEK key for negative testing (should always fail)
const GCS_INVALID_CMEK_KEY_NAME: &str =
    "projects/fake-project-12345/locations/us-central1/keyRings/fake-ring/cryptoKeys/fake-key";

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

/// Helper to create valid CMEK instance from constant
fn test_cmek() -> chroma_storage::Cmek {
    chroma_storage::Cmek::GCP(GCS_CMEK_KEY_NAME.to_string())
}

/// Helper to create invalid CMEK for negative testing
fn test_invalid_cmek() -> chroma_storage::Cmek {
    chroma_storage::Cmek::GCP(GCS_INVALID_CMEK_KEY_NAME.to_string())
}

// ============================================================================
// TESTS - GCS
// ============================================================================

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket access
async fn test_gcs_basic_operations() {
    let obj_storage = create_gcs_storage().await;
    let storage = Storage::Object(obj_storage);
    let prefix = test_prefix("gcs-basic");

    utils::test_basic_operations(&storage, &prefix, None).await;
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket access
async fn test_gcs_multipart_operations() {
    let obj_storage = create_gcs_storage().await;
    let storage = Storage::Object(obj_storage);
    let prefix = test_prefix("gcs-multipart");

    utils::test_multipart_operations(&storage, &prefix, None).await;
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket access
async fn test_gcs_conditional_operations() {
    let obj_storage = create_gcs_storage().await;
    let storage = Storage::Object(obj_storage);
    let prefix = test_prefix("gcs-conditional");

    utils::test_conditional_operations(&storage, &prefix, None).await;
}

#[tokio::test]
#[ignore] // Requires GCS credentials, bucket access, and valid CMEK setup
async fn test_gcs_cmek_basic_operations() {
    let obj_storage = create_gcs_storage().await;
    let storage = Storage::Object(obj_storage);
    let prefix = test_prefix("gcs-cmek-basic");

    utils::test_basic_operations(&storage, &prefix, Some(test_cmek())).await;
}

#[tokio::test]
#[ignore] // Requires GCS credentials, bucket access, and valid CMEK setup
async fn test_gcs_cmek_multipart_operations() {
    let obj_storage = create_gcs_storage().await;
    let storage = Storage::Object(obj_storage);
    let prefix = test_prefix("gcs-cmek-multipart");

    utils::test_multipart_operations(&storage, &prefix, Some(test_cmek())).await;
}

#[tokio::test]
#[ignore] // Requires GCS credentials, bucket access, and valid CMEK setup
async fn test_gcs_cmek_conditional_operations() {
    let obj_storage = create_gcs_storage().await;
    let storage = Storage::Object(obj_storage);
    let prefix = test_prefix("gcs-cmek-conditional");

    utils::test_conditional_operations(&storage, &prefix, Some(test_cmek())).await;
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket access
async fn test_gcs_cmek_invalid_key_fails() {
    let obj_storage = create_gcs_storage().await;
    let storage = Storage::Object(obj_storage);
    let prefix = test_prefix("gcs-cmek-invalid");

    utils::test_invalid_cmek_fails(&storage, &prefix, test_invalid_cmek()).await;
}

// ============================================================================
// TESTS - S3 Legacy (Minio - requires local minio server)
// ============================================================================

#[tokio::test]
#[ignore] // Requires local minio server running
async fn test_s3_basic_operations() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let prefix = test_prefix("s3-basic");

    utils::test_basic_operations(&storage, &prefix, None).await;
}

#[tokio::test]
#[ignore] // Requires local minio server running
async fn test_s3_multipart_operations() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let prefix = test_prefix("s3-multipart");

    utils::test_multipart_operations(&storage, &prefix, None).await;
}

#[tokio::test]
#[ignore] // Requires local minio server running
async fn test_s3_conditional_operations() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let prefix = test_prefix("s3-conditional");

    utils::test_conditional_operations(&storage, &prefix, None).await;
}

// ============================================================================
// TESTS - Admission Controlled GCS
// ============================================================================

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket access
async fn test_gcs_ac_basic_operations() {
    let obj_storage = create_gcs_storage().await;
    let ac_storage = AdmissionControlledS3Storage::new_object_with_default_policy(obj_storage);
    let storage = Storage::AdmissionControlledS3(ac_storage);
    let prefix = test_prefix("gcs-ac-basic");

    utils::test_basic_operations(&storage, &prefix, None).await;
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket access
async fn test_gcs_ac_multipart_operations() {
    let obj_storage = create_gcs_storage().await;
    let ac_storage = AdmissionControlledS3Storage::new_object_with_default_policy(obj_storage);
    let storage = Storage::AdmissionControlledS3(ac_storage);
    let prefix = test_prefix("gcs-ac-multipart");

    utils::test_multipart_operations(&storage, &prefix, None).await;
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket access
async fn test_gcs_ac_conditional_operations() {
    let obj_storage = create_gcs_storage().await;
    let ac_storage = AdmissionControlledS3Storage::new_object_with_default_policy(obj_storage);
    let storage = Storage::AdmissionControlledS3(ac_storage);
    let prefix = test_prefix("gcs-ac-conditional");

    utils::test_conditional_operations(&storage, &prefix, None).await;
}

#[tokio::test]
#[ignore] // Requires GCS credentials, bucket access, and valid CMEK setup
async fn test_gcs_ac_cmek_basic_operations() {
    let obj_storage = create_gcs_storage().await;
    let ac_storage = AdmissionControlledS3Storage::new_object_with_default_policy(obj_storage);
    let storage = Storage::AdmissionControlledS3(ac_storage);
    let prefix = test_prefix("gcs-ac-cmek-basic");

    utils::test_basic_operations(&storage, &prefix, Some(test_cmek())).await;
}

#[tokio::test]
#[ignore] // Requires GCS credentials, bucket access, and valid CMEK setup
async fn test_gcs_ac_cmek_multipart_operations() {
    let obj_storage = create_gcs_storage().await;
    let ac_storage = AdmissionControlledS3Storage::new_object_with_default_policy(obj_storage);
    let storage = Storage::AdmissionControlledS3(ac_storage);
    let prefix = test_prefix("gcs-ac-cmek-multipart");

    utils::test_multipart_operations(&storage, &prefix, Some(test_cmek())).await;
}

#[tokio::test]
#[ignore] // Requires GCS credentials and bucket access
async fn test_gcs_ac_cmek_invalid_key_fails() {
    let obj_storage = create_gcs_storage().await;
    let ac_storage = AdmissionControlledS3Storage::new_object_with_default_policy(obj_storage);
    let storage = Storage::AdmissionControlledS3(ac_storage);
    let prefix = test_prefix("gcs-ac-cmek-invalid");

    utils::test_invalid_cmek_fails(&storage, &prefix, test_invalid_cmek()).await;
}

// ============================================================================
// TESTS - Admission Controlled S3 Legacy (Minio)
// ============================================================================

#[tokio::test]
#[ignore] // Requires local minio server running
async fn test_s3_ac_basic_operations() {
    let base_storage = chroma_storage::s3_client_for_test_with_new_bucket().await;

    // Extract S3Storage from Storage enum to wrap in AdmissionControlled
    let s3_storage = match base_storage {
        Storage::S3(s3) => s3,
        _ => panic!("Expected S3 storage"),
    };

    let ac_storage = AdmissionControlledS3Storage::new_s3_with_default_policy(s3_storage);
    let storage = Storage::AdmissionControlledS3(ac_storage);
    let prefix = test_prefix("s3-ac-basic");

    utils::test_basic_operations(&storage, &prefix, None).await;
}

#[tokio::test]
#[ignore] // Requires local minio server running
async fn test_s3_ac_multipart_operations() {
    let base_storage = chroma_storage::s3_client_for_test_with_new_bucket().await;

    let s3_storage = match base_storage {
        Storage::S3(s3) => s3,
        _ => panic!("Expected S3 storage"),
    };

    let ac_storage = AdmissionControlledS3Storage::new_s3_with_default_policy(s3_storage);
    let storage = Storage::AdmissionControlledS3(ac_storage);
    let prefix = test_prefix("s3-ac-multipart");

    utils::test_multipart_operations(&storage, &prefix, None).await;
}

#[tokio::test]
#[ignore] // Requires local minio server running
async fn test_s3_ac_conditional_operations() {
    let base_storage = chroma_storage::s3_client_for_test_with_new_bucket().await;

    let s3_storage = match base_storage {
        Storage::S3(s3) => s3,
        _ => panic!("Expected S3 storage"),
    };

    let ac_storage = AdmissionControlledS3Storage::new_s3_with_default_policy(s3_storage);
    let storage = Storage::AdmissionControlledS3(ac_storage);
    let prefix = test_prefix("s3-ac-conditional");

    utils::test_conditional_operations(&storage, &prefix, None).await;
}
