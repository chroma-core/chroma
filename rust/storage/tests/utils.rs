//! Test utilities for ObjectStorage implementations
//!
//! This module provides reusable test functions that can be used to test
//! any ObjectStorage backend (GCS, S3, Azure, etc.) by simply passing in
//! a configured storage instance.

use chroma_storage::object_storage::ObjectStorage;
use chroma_storage::{GetOptions, PutOptions};

/// Test Group 1: Basic operations (non-concurrency aware APIs)
///
/// Tests: list_prefix, put, get, copy, rename, delete, delete_many
pub async fn test_basic_operations(storage: &ObjectStorage, test_prefix: &str) {
    // Cleanup any leftover objects from previous test runs
    let existing = storage
        .list_prefix(test_prefix)
        .await
        .expect("Failed to list for cleanup");
    if !existing.is_empty() {
        storage
            .delete_many(existing)
            .await
            .expect("Failed to cleanup");
    }

    // Test 1: list_prefix on empty prefix
    let objects = storage
        .list_prefix(test_prefix)
        .await
        .expect("Failed to list empty prefix");
    assert_eq!(
        objects.len(),
        0,
        "Expected empty list for non-existent prefix"
    );

    // Test 2: put and get
    let test_keys = vec![
        format!("{}file1.txt", test_prefix),
        format!("{}file2.txt", test_prefix),
        format!("{}subdir/file3.txt", test_prefix),
    ];

    for key in &test_keys {
        storage
            .put(key, key.as_bytes().to_vec().into(), PutOptions::default())
            .await
            .unwrap_or_else(|e| panic!("Failed to put {}: {}", key, e));

        let (data, _etag) = storage
            .get(key, GetOptions::new(Default::default()))
            .await
            .unwrap_or_else(|e| panic!("Failed to get {}: {}", key, e));
        assert!(data.is_unique());
        assert_eq!(data, key.as_bytes(), "Content mismatch for {}", key);
    }

    // Test 3: list_prefix with results
    let objects = storage
        .list_prefix(test_prefix)
        .await
        .expect("Failed to list prefix");
    assert_eq!(
        objects.len(),
        test_keys.len(),
        "Expected {} objects",
        test_keys.len()
    );
    for key in &test_keys {
        assert!(objects.contains(key), "Missing key: {}", key);
    }

    // Test 4: copy
    let src = &test_keys[0];
    let dst = format!("{}-copy", src);
    storage.copy(src, &dst).await.expect("Failed to copy");

    let (data, _) = storage
        .get(&dst, GetOptions::new(Default::default()))
        .await
        .expect("Failed to get copied file");
    assert!(data.is_unique());
    assert_eq!(data, src.as_bytes(), "Copied file content mismatch");

    // Verify original still exists
    storage
        .get(src, GetOptions::new(Default::default()))
        .await
        .expect("Original should still exist after copy");

    // Test 5: rename
    let src = &test_keys[1];
    let dst = format!("{}-renamed", src);
    storage.rename(src, &dst).await.expect("Failed to rename");

    let (data, _) = storage
        .get(&dst, GetOptions::new(Default::default()))
        .await
        .expect("Failed to get renamed file");
    assert!(data.is_unique());
    assert_eq!(data, src.as_bytes(), "Renamed file content mismatch");

    // Verify original no longer exists
    let result = storage.get(src, GetOptions::new(Default::default())).await;
    assert!(result.is_err(), "Original should not exist after rename");

    // Test 6: delete single
    storage.delete(&dst).await.expect("Failed to delete");
    let result = storage.get(&dst, GetOptions::new(Default::default())).await;
    assert!(result.is_err(), "Deleted object should not exist");

    // Test 7: delete_many
    let remaining = storage
        .list_prefix(test_prefix)
        .await
        .expect("Failed to list");
    let result = storage
        .delete_many(remaining.clone())
        .await
        .expect("Failed to delete_many");
    assert_eq!(
        result.deleted.len(),
        remaining.len(),
        "Should delete all objects"
    );

    let objects = storage
        .list_prefix(test_prefix)
        .await
        .expect("Failed to list after cleanup");
    assert_eq!(objects.len(), 0, "All objects should be deleted");
}

/// Test Group 2: Multipart upload/download
///
/// Tests: large file uploads, multipart downloads with parallelism
pub async fn test_multipart_operations(storage: &ObjectStorage, test_prefix: &str) {
    // Cleanup
    let existing = storage.list_prefix(test_prefix).await.unwrap_or_default();
    if !existing.is_empty() {
        storage.delete_many(existing).await.ok();
    }

    // Test 1: Small file (oneshot)
    let small_key = format!("{}small.txt", test_prefix);
    let small_content = small_key.repeat(1000); // ~25 KB

    storage
        .put(
            &small_key,
            small_content.as_bytes().to_vec().into(),
            PutOptions::default(),
        )
        .await
        .expect("Failed to put small file");

    let (data, _) = storage
        .get(&small_key, GetOptions::new(Default::default()))
        .await
        .expect("Failed to get small file");
    assert!(data.is_unique());
    assert_eq!(
        data,
        small_content.as_bytes(),
        "Small file content mismatch"
    );

    // Test 2: Large file (multipart upload)
    let large_key = format!("{}large.txt", test_prefix);
    let large_content = large_key.repeat(400_000); // ~10 MB - well over 5MB threshold

    storage
        .put(
            &large_key,
            large_content.as_bytes().to_vec().into(),
            PutOptions::default(),
        )
        .await
        .expect("Failed to put large file");

    // Test 3: Download with oneshot
    let (data, _) = storage
        .get(&large_key, GetOptions::new(Default::default()))
        .await
        .expect("Failed to get large file with oneshot");
    assert!(data.is_unique());
    assert_eq!(
        data,
        large_content.as_bytes(),
        "Large file content mismatch (oneshot)"
    );

    // Test 4: Download with parallelism
    let (data, _) = storage
        .get(
            &large_key,
            GetOptions::new(Default::default()).with_parallelism(),
        )
        .await
        .expect("Failed to get large file with parallelism");
    assert!(data.is_unique());
    assert_eq!(
        data,
        large_content.as_bytes(),
        "Large file content mismatch (multipart)"
    );

    // Cleanup
    storage
        .delete_many(vec![small_key, large_key])
        .await
        .expect("Failed to cleanup");
}

/// Test Group 3: Conditional operations (concurrency-aware APIs)
///
/// Tests: if_not_exists, if_match, confirm_same, put_file with conditions
///
/// Note: Conditional operations always use oneshot uploads (not multipart),
/// even for large files, because multipart uploads don't support ETags.
/// The race condition test uses ~800KB content to verify this behavior.
pub async fn test_conditional_operations(storage: &ObjectStorage, test_prefix: &str) {
    // Cleanup
    let existing = storage.list_prefix(test_prefix).await.unwrap_or_default();
    if !existing.is_empty() {
        storage.delete_many(existing).await.ok();
    }

    // Test 1: if_not_exists
    let key1 = format!("{}create-once.txt", test_prefix);
    let content1 = format!("{}-v1", key1);

    let _etag1 = storage
        .put(
            &key1,
            content1.as_bytes().to_vec().into(),
            PutOptions::if_not_exists(Default::default()),
        )
        .await
        .expect("First if_not_exists should succeed");

    let (data, _) = storage
        .get(&key1, GetOptions::new(Default::default()))
        .await
        .expect("Failed to get");
    assert!(data.is_unique());
    assert_eq!(data, content1.as_bytes());

    // Second create should fail
    let result = storage
        .put(
            &key1,
            "different".as_bytes().to_vec().into(),
            PutOptions::if_not_exists(Default::default()),
        )
        .await;
    assert!(result.is_err(), "Second if_not_exists should fail");

    // Test 2: if_match
    let key2 = format!("{}conditional-update.txt", test_prefix);
    let content_v1 = format!("{}-v1", key2);
    let content_v2 = format!("{}-v2", key2);

    let etag_v1 = storage
        .put(
            &key2,
            content_v1.as_bytes().to_vec().into(),
            PutOptions::default(),
        )
        .await
        .expect("Initial put failed");

    // Update with correct ETag should succeed
    let _etag_v2 = storage
        .put(
            &key2,
            content_v2.as_bytes().to_vec().into(),
            PutOptions::if_matches(&etag_v1, Default::default()),
        )
        .await
        .expect("Update with correct ETag should succeed");

    let (data, _) = storage
        .get(&key2, GetOptions::new(Default::default()))
        .await
        .expect("Failed to get");
    assert!(data.is_unique());
    assert_eq!(data, content_v2.as_bytes());

    // Update with stale ETag should fail
    let result = storage
        .put(
            &key2,
            "v3".as_bytes().to_vec().into(),
            PutOptions::if_matches(&etag_v1, Default::default()),
        )
        .await;
    assert!(result.is_err(), "Update with stale ETag should fail");

    // Test 3: confirm_same
    let key3 = format!("{}etag-check.txt", test_prefix);
    let content3_v1 = format!("{}-v1", key3);
    let content3_v2 = format!("{}-v2", key3);

    let etag3_v1 = storage
        .put(
            &key3,
            content3_v1.as_bytes().to_vec().into(),
            PutOptions::default(),
        )
        .await
        .expect("Put failed");

    let same = storage
        .confirm_same(&key3, &etag3_v1)
        .await
        .expect("confirm_same failed");
    assert!(same, "confirm_same with correct ETag should return true");

    // Update object
    let _etag3_v2 = storage
        .put(
            &key3,
            content3_v2.as_bytes().to_vec().into(),
            PutOptions::default(),
        )
        .await
        .expect("Update failed");

    let same = storage
        .confirm_same(&key3, &etag3_v1)
        .await
        .expect("confirm_same failed");
    assert!(!same, "confirm_same with stale ETag should return false");

    // Test 4: put_file with conditions
    let key4 = format!("{}file-upload.txt", test_prefix);
    let content4 = format!("{}-content", key4);

    let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    std::fs::write(temp_file.path(), &content4).expect("Failed to write temp file");

    let _etag4 = storage
        .put_file(
            &key4,
            temp_file.path().to_str().unwrap(),
            PutOptions::if_not_exists(Default::default()),
        )
        .await
        .expect("put_file should succeed");

    let (data, _) = storage
        .get(&key4, GetOptions::new(Default::default()))
        .await
        .expect("Failed to get");
    assert!(data.is_unique());
    assert_eq!(data, content4.as_bytes());

    // Second put_file should fail
    let result = storage
        .put_file(
            &key4,
            temp_file.path().to_str().unwrap(),
            PutOptions::if_not_exists(Default::default()),
        )
        .await;
    assert!(result.is_err(), "Second put_file should fail");

    // Test 5: Race conditions with large files
    let key5 = format!("{}race-test.txt", test_prefix);
    // Use files > 5MB to:
    // 1. Make uploads take longer (more realistic race condition)
    // 2. Test that conditional operations use oneshot (not multipart) even for large files
    //    that would normally trigger multipart upload, because multipart doesn't support ETags
    let initial = "initial-data-".repeat(500_000); // ~6.5 MB - over 5MB threshold

    let race_etag = storage
        .put(
            &key5,
            initial.as_bytes().to_vec().into(),
            PutOptions::default(),
        )
        .await
        .expect("Initial put failed");

    // Spawn concurrent updates with same stale ETag - use large payloads > 5MB
    let storage_a = storage.clone();
    let storage_b = storage.clone();
    let key5_a = key5.clone();
    let key5_b = key5.clone();
    let etag_a = race_etag.clone();
    let etag_b = race_etag.clone();
    let writer_a_content = "writer-a-data-".repeat(500_000); // ~7 MB - over 5MB threshold
    let writer_b_content = "writer-b-data-".repeat(500_000); // ~7 MB - over 5MB threshold

    // Keep copies for verification after the tasks complete
    let writer_a_content_copy = writer_a_content.clone();
    let writer_b_content_copy = writer_b_content.clone();

    let task_a = tokio::spawn(async move {
        storage_a
            .put(
                &key5_a,
                writer_a_content.as_bytes().to_vec().into(),
                PutOptions::if_matches(&etag_a, Default::default()),
            )
            .await
    });

    let task_b = tokio::spawn(async move {
        storage_b
            .put(
                &key5_b,
                writer_b_content.as_bytes().to_vec().into(),
                PutOptions::if_matches(&etag_b, Default::default()),
            )
            .await
    });

    let (result_a, result_b) = tokio::join!(task_a, task_b);
    let result_a = result_a.expect("Task A panicked");
    let result_b = result_b.expect("Task B panicked");

    // Exactly one should succeed
    let success_count = result_a.is_ok() as u32 + result_b.is_ok() as u32;
    assert_eq!(success_count, 1, "Exactly one writer should succeed");

    // Verify the winner's content was actually written - COMPLETE byte-by-byte verification
    let (final_data, _) = storage
        .get(&key5, GetOptions::new(Default::default()))
        .await
        .expect("Failed to get final content");
    assert!(final_data.is_unique());

    // Compare against both possible winning contents
    let is_writer_a = final_data == writer_a_content_copy.as_bytes();
    let is_writer_b = final_data == writer_b_content_copy.as_bytes();

    assert!(
        is_writer_a || is_writer_b,
        "Final content must exactly match one of the writers' content (not initial or corrupted)"
    );

    // Cleanup
    storage
        .delete_many(vec![key1, key2, key3, key4, key5])
        .await
        .expect("Failed to cleanup");
}
