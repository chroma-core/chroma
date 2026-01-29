//! Version file management utilities for handling collection version files.

//! This module provides a centralized way to download, upload, and manage
//! collection version files stored in S3/storage. Version files track the
//! evolution of segments within a collection.

use chroma_storage::{GetOptions, PutOptions, Storage, StorageError};
use chroma_types::chroma_proto::CollectionVersionFile;
use chroma_types::{Collection, CollectionUuid};
use prost::Message;
use thiserror::Error;
use uuid;

/// Types of version file operations that determine the file naming convention.
#[derive(Debug, Clone, PartialEq)]
pub enum VersionFileType {
    /// Compaction operation - file name ends with _flush
    Compaction,
    /// Garbage collection operation - file name ends with _gc_mark
    GarbageCollection,
}

impl VersionFileType {
    /// Get the file suffix for the version file type
    pub fn suffix(&self) -> &'static str {
        match self {
            VersionFileType::Compaction => "flush",
            VersionFileType::GarbageCollection => "gc_mark",
        }
    }
}

/// Manager for version file operations including download, upload, and validation.
#[derive(Clone, Debug)]
pub struct VersionFileManager {
    storage: Storage,
}

/// Errors that can occur during version file operations.
#[derive(Error, Debug)]
pub enum VersionFileError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("Invalid collection ID: {0}")]
    InvalidUuid(#[from] uuid::Error),
    #[error("Missing collection info in version file")]
    MissingCollectionInfo,
    #[error("Invalid version file path: {0}")]
    InvalidPath(String),
    #[error("Version file validation failed: {0}")]
    ValidationFailed(String),
}

impl VersionFileManager {
    /// Create a new VersionFileManager with the given storage backend.
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }

    /// Download and decode a version file from storage.
    ///
    /// # Arguments
    /// * `collection` - The collection containing the version file path
    ///
    /// # Returns
    /// A decoded `CollectionVersionFile`
    pub async fn fetch(
        &self,
        collection: &Collection,
    ) -> Result<CollectionVersionFile, VersionFileError> {
        let version_file_path = collection.version_file_path.as_ref().ok_or_else(|| {
            VersionFileError::InvalidPath("Collection has no version file path".to_string())
        })?;

        if version_file_path.is_empty() {
            return Err(VersionFileError::InvalidPath(
                "Version file path cannot be empty".to_string(),
            ));
        }

        let content = self
            .storage
            .get(version_file_path, GetOptions::default())
            .await
            .map_err(|e| {
                tracing::error!(
                    error = ?e,
                    path = %version_file_path,
                    "Failed to fetch version file"
                );
                e
            })?;

        tracing::info!(
            path = %version_file_path,
            size = content.len(),
            "Successfully fetched version file"
        );

        let version_file = CollectionVersionFile::decode(content.as_slice())?;

        // Extract collection ID from the collection for validation
        let collection_id_str = &collection.collection_id.to_string();
        let version = collection.version;

        // Validate the version file
        self.validate(&version_file, collection_id_str, version.into())?;

        Ok(version_file)
    }

    /// Encode and upload a version file to storage.
    ///
    /// # Arguments
    /// * `version_file` - The version file to upload
    /// * `collection` - The collection metadata for generating the path
    /// * `file_type` - The type of version file operation (COMPACTION or GARBAGE_COLLECTION)
    /// * `new_version_id` - The new version identifier
    ///
    /// # Returns
    /// The path where the version file was stored
    pub async fn upload(
        &self,
        version_file: &CollectionVersionFile,
        collection: &chroma_types::Collection,
        file_type: VersionFileType,
        new_version_id: i64,
    ) -> Result<String, VersionFileError> {
        // Validate the version file before uploading
        let collection_id_str = &collection.collection_id.to_string();
        // For upload, we don't know the expected version yet, so we'll validate structure
        // and collection ID match only. The version validation will happen during fetch.
        if let Some(ref collection_info) = version_file.collection_info_immutable {
            if collection_info.collection_id != *collection_id_str {
                return Err(VersionFileError::ValidationFailed(
                    "Version file collection ID does not match collection".to_string(),
                ));
            }
        } else {
            return Err(VersionFileError::MissingCollectionInfo);
        }

        // Generate the version file path from collection metadata
        let version_file_path = self.generate_file_path(
            &collection.tenant,
            &collection.database,
            &collection.collection_id,
            new_version_id,
            file_type,
        );

        // Validate the version file before uploading
        let collection_id_str = &collection.collection_id.to_string();
        // The given version file's latest version is expected to be new_version_id
        self.validate(version_file, collection_id_str, new_version_id)?;

        // Encode the version file
        let content = version_file.encode_to_vec();
        let content_size = content.len();

        // Upload to storage
        self.storage
            .put_bytes(&version_file_path, content, PutOptions::default())
            .await
            .map_err(|e| {
                tracing::error!(
                    error = ?e,
                    path = %version_file_path,
                    size = content_size,
                    "Failed to upload version file"
                );
                e
            })?;

        tracing::info!(
            path = %version_file_path,
            size = content_size,
            "Successfully uploaded version file"
        );

        Ok(version_file_path)
    }

    /// Generate a standard version file path based on collection metadata.
    ///
    /// # Arguments
    /// * `tenant_id` - The tenant ID
    /// * `database_id` - The database ID
    /// * `collection_id` - The collection UUID
    /// * `version_id` - The version identifier (typically a UUID or timestamp)
    /// * `file_type` - The type of version file operation (COMPACTION or GARBAGE_COLLECTION)
    ///
    /// # Returns
    /// A formatted path matching Go implementation with appropriate suffix:
    /// - For COMPACTION: "tenant/{tenant_id}/database/{database_id}/collection/{collection_id}/versionfiles/{version_id}_flush"
    /// - For GARBAGE_COLLECTION: "tenant/{tenant_id}/database/{database_id}/collection/{collection_id}/versionfiles/{version_id}_gc_mark"
    fn generate_file_path(
        &self,
        tenant_id: &str,
        database_id: &str,
        collection_id: &CollectionUuid,
        version_id: i64,
        file_type: VersionFileType,
    ) -> String {
        // Generate a UUID for the version file name (matching Go logic)
        let version_uuid = uuid::Uuid::new_v4().to_string();

        let version_file_name =
            format!("{:06}_{}_{}", version_id, version_uuid, file_type.suffix());

        format!(
            "tenant/{}/database/{}/collection/{}/versionfiles/{}",
            tenant_id, database_id, collection_id, version_file_name
        )
    }

    /// Validate that a version file contains the required fields and is well-formed.
    ///
    /// # Arguments
    /// * `version_file` - The version file to validate
    /// * `expected_collection_id` - The expected collection ID to match against
    /// * `expected_version` - The expected version to match against
    pub fn validate(
        &self,
        version_file: &CollectionVersionFile,
        expected_collection_id: &str,
        expected_version: i64,
    ) -> Result<(), VersionFileError> {
        let collection_info = match version_file.collection_info_immutable.as_ref() {
            Some(info) => info,
            None => return Err(VersionFileError::MissingCollectionInfo),
        };

        // Validate collection ID matches expected collection ID
        if collection_info.collection_id != expected_collection_id {
            tracing::error!(
                expected_collection_id = %expected_collection_id,
                version_file_collection_id = %collection_info.collection_id,
                "collection id mismatch"
            );
            return Err(VersionFileError::ValidationFailed(
                "collection id mismatch".to_string(),
            ));
        }

        // Validate version history is not empty
        let version_history = match version_file.version_history.as_ref() {
            Some(history) => history,
            None => {
                tracing::error!("version history is empty");
                return Err(VersionFileError::ValidationFailed(
                    "version history is empty".to_string(),
                ));
            }
        };

        if version_history.versions.is_empty() {
            tracing::error!("version history is empty");
            return Err(VersionFileError::ValidationFailed(
                "version history is empty".to_string(),
            ));
        }

        let versions = &version_history.versions;
        let mut seen_paths = false;

        // Validate segments for versions beyond the first
        if versions.len() > 1 {
            for (idx, version_info) in versions.iter().enumerate() {
                if idx == 0 {
                    continue; // Skip first version
                }

                let segment_info = match version_info.segment_info.as_ref() {
                    Some(info) => info,
                    None => {
                        tracing::error!(
                            collection_id = %expected_collection_id,
                            version = %version_info.version,
                            "version has no segment info"
                        );
                        return Err(VersionFileError::ValidationFailed(
                            "version has no segment info".to_string(),
                        ));
                    }
                };

                let segments = &segment_info.segment_compaction_info;
                if segments.is_empty() {
                    tracing::error!(
                        collection_id = %expected_collection_id,
                        version = %version_info.version,
                        "version has no segments"
                    );
                    return Err(VersionFileError::ValidationFailed(
                        "version has no segments".to_string(),
                    ));
                }

                for segment in segments {
                    let file_paths = &segment.file_paths;
                    if seen_paths && file_paths.is_empty() {
                        tracing::error!(
                            collection_id = %expected_collection_id,
                            version = %version_info.version,
                            segment_id = %segment.segment_id,
                            "version has no file paths"
                        );
                        return Err(VersionFileError::ValidationFailed(
                            "version has no file paths".to_string(),
                        ));
                    } else if !file_paths.is_empty() {
                        seen_paths = true;
                    }
                }
            }
        }

        // Validate the last version matches expected version
        let last_version = versions[versions.len() - 1].version;
        if last_version != expected_version {
            let version_numbers: Vec<i64> = versions.iter().map(|v| v.version).collect();
            tracing::error!(
                expected_version = %expected_version,
                last_version = %last_version,
                version_history = ?version_numbers,
                "version mismatch"
            );
            return Err(VersionFileError::ValidationFailed(
                "version mismatch".to_string(),
            ));
        }

        Ok(())
    }

    /// Get the collection ID from a version file.
    ///
    /// # Arguments
    /// * `version_file` - The version file to extract collection ID from
    ///
    /// # Returns
    /// The collection UUID
    pub fn extract_collection_id(
        &self,
        version_file: &CollectionVersionFile,
    ) -> Result<CollectionUuid, VersionFileError> {
        let collection_id_str = &version_file
            .collection_info_immutable
            .as_ref()
            .ok_or(VersionFileError::MissingCollectionInfo)?
            .collection_id;

        let collection_id = collection_id_str
            .parse()
            .map_err(VersionFileError::InvalidUuid)?;

        Ok(CollectionUuid(collection_id))
    }

    /// Get the tenant ID from a version file.
    ///
    /// # Arguments
    /// * `version_file` - The version file to extract tenant ID from
    ///
    /// # Returns
    /// The tenant ID string
    pub fn extract_tenant_id(
        &self,
        version_file: &CollectionVersionFile,
    ) -> Result<String, VersionFileError> {
        version_file
            .collection_info_immutable
            .as_ref()
            .ok_or(VersionFileError::MissingCollectionInfo)
            .map(|info| info.tenant_id.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_config::Configurable;
    use chroma_storage::config::{LocalStorageConfig, StorageConfig};
    use chroma_types::chroma_proto::{
        CollectionInfoImmutable, CollectionSegmentInfo, CollectionVersionHistory,
        CollectionVersionInfo, FlushSegmentCompactionInfo,
    };
    use tempfile::TempDir;
    use uuid::Uuid;

    async fn create_test_storage() -> (Storage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig::Local(LocalStorageConfig {
            root: temp_dir.path().to_string_lossy().to_string(),
        });
        let storage = Storage::try_from_config(&config, &chroma_config::registry::Registry::new())
            .await
            .unwrap();
        (storage, temp_dir)
    }

    fn create_test_version_file() -> CollectionVersionFile {
        let collection_id = Uuid::new_v4();
        let tenant_id = Uuid::new_v4().to_string();
        let database_id = Uuid::new_v4().to_string();

        CollectionVersionFile {
            collection_info_immutable: Some(CollectionInfoImmutable {
                tenant_id,
                database_id,
                database_name: "test_db".to_string(),
                is_deleted: false,
                dimension: 128,
                collection_id: collection_id.to_string(),
                collection_name: "test_collection".to_string(),
                collection_creation_secs: 1640995200, // 2022-01-01
            }),
            version_history: Some(CollectionVersionHistory { versions: vec![] }),
        }
    }

    fn create_test_collection() -> chroma_types::Collection {
        let collection_id = CollectionUuid(Uuid::new_v4());
        let tenant_id = Uuid::new_v4().to_string();
        let database_id = Uuid::new_v4().to_string();
        let database_uuid = chroma_types::DatabaseUuid(Uuid::new_v4());

        chroma_types::Collection {
            collection_id,
            name: "test_collection".to_string(),
            config: chroma_types::InternalCollectionConfiguration::default_hnsw(),
            schema: None,
            metadata: Some(std::collections::HashMap::new()),
            dimension: Some(128),
            tenant: tenant_id,
            database: database_id,
            log_position: 0,
            version: 1,
            total_records_post_compaction: 0,
            size_bytes_post_compaction: 0,
            last_compaction_time_secs: 0,
            version_file_path: None,
            root_collection_id: None,
            lineage_file_path: None,
            updated_at: std::time::SystemTime::now(),
            database_id: database_uuid,
            compaction_failure_count: 0,
        }
    }

    #[tokio::test]
    async fn test_upload_and_fetch_version_file() {
        let (storage, _temp_dir) = create_test_storage().await;
        let manager = VersionFileManager::new(storage);
        let collection = create_test_collection();

        // Create a version file with collection ID matching the test collection
        let mut version_file = create_test_version_file();
        version_file
            .collection_info_immutable
            .as_mut()
            .unwrap()
            .collection_id = collection.collection_id.to_string();

        // Add a version history entry to satisfy validation
        let new_version = collection.version as i64; // Use collection's current version as i64
        version_file.version_history = Some(CollectionVersionHistory {
            versions: vec![CollectionVersionInfo {
                version: new_version,
                segment_info: Some(CollectionSegmentInfo {
                    segment_compaction_info: vec![],
                }),
                collection_info_mutable: None,
                created_at_secs: 1640995200,
                version_change_reason: 0,
                version_file_name: "test_file.binpb".to_string(),
                marked_for_deletion: false,
            }],
        });

        // Upload the version file
        let uploaded_path = manager
            .upload(
                &version_file,
                &collection,
                VersionFileType::Compaction,
                new_version,
            )
            .await
            .unwrap();

        // Expected path should follow the new Go-style format: {version_id:06d}_{uuid}_flush
        // We'll check that the path contains the expected components
        assert!(uploaded_path.contains(&format!(
            "tenant/{}/database/{}/collection/{}/versionfiles/",
            collection.tenant, collection.database, collection.collection_id
        )));
        assert!(uploaded_path.contains("_flush"));

        // Check that the version ID part is present in the path (formatted as 6 digits)
        assert!(uploaded_path.contains(&format!("{:06}", new_version)));

        // Test fetching the uploaded file
        let mut collection_with_path = collection.clone();
        collection_with_path.version_file_path = Some(uploaded_path.clone());
        let fetched_file = manager.fetch(&collection_with_path).await.unwrap();

        // Verify the content
        assert_eq!(
            fetched_file
                .collection_info_immutable
                .as_ref()
                .unwrap()
                .collection_id,
            version_file
                .collection_info_immutable
                .as_ref()
                .unwrap()
                .collection_id
        );
    }

    #[tokio::test]
    async fn test_fetch_nonexistent_version_file() {
        let (storage, _temp_dir) = create_test_storage().await;
        let manager = VersionFileManager::new(storage);
        let mut collection = create_test_collection();
        collection.version_file_path = Some("nonexistent/file.binpb".to_string());
        let result = manager.fetch(&collection).await;
        assert!(matches!(result, Err(VersionFileError::Storage(_))));
    }

    #[tokio::test]
    async fn test_validate_version_file() {
        let (storage, _temp_dir) = create_test_storage().await;
        let manager = VersionFileManager::new(storage);

        // Create a test file with proper version history for full validation
        let mut valid_file = create_test_version_file();
        let collection_id = valid_file
            .collection_info_immutable
            .as_ref()
            .unwrap()
            .collection_id
            .clone();
        let version = 1;

        // Add version history with one version
        valid_file.version_history = Some(CollectionVersionHistory {
            versions: vec![CollectionVersionInfo {
                version,
                segment_info: Some(CollectionSegmentInfo {
                    segment_compaction_info: vec![],
                }),
                collection_info_mutable: None,
                created_at_secs: 1640995200,
                version_change_reason: 0,
                version_file_name: "test_file.binpb".to_string(),
                marked_for_deletion: false,
            }],
        });

        // Valid version file should pass validation
        assert!(manager
            .validate(&valid_file, &collection_id, version)
            .is_ok());

        // Invalid version file (missing collection info) should fail
        let mut invalid_file = create_test_version_file();
        invalid_file.collection_info_immutable = None;
        assert!(matches!(
            manager.validate(&invalid_file, "test-id", 1),
            Err(VersionFileError::MissingCollectionInfo)
        ));
    }

    #[tokio::test]
    async fn test_validate_version_file_full() {
        let (storage, _temp_dir) = create_test_storage().await;
        let manager = VersionFileManager::new(storage);

        let collection_id = "test-collection-id";
        let version = 1;

        // Create a test file with proper version history
        let mut test_file = create_test_version_file();
        test_file
            .collection_info_immutable
            .as_mut()
            .unwrap()
            .collection_id = collection_id.to_string();

        // Add version history with one version
        test_file.version_history = Some(CollectionVersionHistory {
            versions: vec![CollectionVersionInfo {
                version,
                segment_info: Some(CollectionSegmentInfo {
                    segment_compaction_info: vec![],
                }),
                collection_info_mutable: None,
                created_at_secs: 1640995200,
                version_change_reason: 0,
                version_file_name: "test_file.binpb".to_string(),
                marked_for_deletion: false,
            }],
        });

        // Test with matching collection_id and version should pass
        assert!(manager.validate(&test_file, collection_id, version).is_ok());

        // Test with mismatching collection_id should fail
        let mut mismatching_file = test_file.clone();
        mismatching_file
            .collection_info_immutable
            .as_mut()
            .unwrap()
            .collection_id = "different-id".to_string();

        assert!(matches!(
            manager.validate(&mismatching_file, collection_id, version),
            Err(VersionFileError::ValidationFailed(msg)) if msg.contains("collection id mismatch")
        ));

        // Test with mismatching version should fail
        let mut version_mismatch_file = test_file.clone();
        version_mismatch_file
            .collection_info_immutable
            .as_mut()
            .unwrap()
            .collection_id = collection_id.to_string();
        version_mismatch_file
            .version_history
            .as_mut()
            .unwrap()
            .versions
            .last_mut()
            .unwrap()
            .version = 999;

        assert!(matches!(
            manager.validate(&version_mismatch_file, collection_id, version),
            Err(VersionFileError::ValidationFailed(msg)) if msg.contains("version mismatch")
        ));
    }

    #[tokio::test]
    async fn test_extract_collection_id() {
        let (storage, _temp_dir) = create_test_storage().await;
        let manager = VersionFileManager::new(storage);
        let version_file = create_test_version_file();

        let expected_id = version_file
            .collection_info_immutable
            .as_ref()
            .unwrap()
            .collection_id
            .parse::<Uuid>()
            .unwrap();

        let extracted_id = manager.extract_collection_id(&version_file).unwrap();
        assert_eq!(extracted_id, CollectionUuid(expected_id));
    }

    #[tokio::test]
    async fn test_empty_path_validation() {
        let (storage, _temp_dir) = create_test_storage().await;
        let manager = VersionFileManager::new(storage);
        let mut collection = create_test_collection();
        collection.version_file_path = Some("".to_string());

        let fetch_result = manager.fetch(&collection).await;
        assert!(matches!(
            fetch_result,
            Err(VersionFileError::InvalidPath(_))
        ));
    }

    #[tokio::test]
    async fn test_upload_modify_and_reupload() {
        let (storage, _temp_dir) = create_test_storage().await;
        let manager = VersionFileManager::new(storage);
        let collection = create_test_collection();

        // Create initial version file with matching collection ID
        let mut version_file = create_test_version_file();
        version_file
            .collection_info_immutable
            .as_mut()
            .unwrap()
            .collection_id = collection.collection_id.to_string();

        // Add a version history entry to satisfy validation
        let new_version = collection.version as i64; // Use collection's current version
        version_file.version_history = Some(CollectionVersionHistory {
            versions: vec![CollectionVersionInfo {
                version: new_version,
                segment_info: Some(CollectionSegmentInfo {
                    segment_compaction_info: vec![],
                }),
                collection_info_mutable: None,
                created_at_secs: 1640995200,
                version_change_reason: 0,
                version_file_name: "test_file.binpb".to_string(),
                marked_for_deletion: false,
            }],
        });

        // Upload initial version
        let upload_result = manager
            .upload(
                &version_file,
                &collection,
                VersionFileType::Compaction,
                new_version as i64,
            )
            .await;
        assert!(upload_result.is_ok());
        let initial_path = upload_result.unwrap();

        // Download and validate
        let mut collection_with_initial_path = collection.clone();
        collection_with_initial_path.version_file_path = Some(initial_path.clone());
        let downloaded = manager.fetch(&collection_with_initial_path).await.unwrap();
        assert_eq!(
            downloaded
                .collection_info_immutable
                .as_ref()
                .unwrap()
                .collection_id,
            version_file
                .collection_info_immutable
                .as_ref()
                .unwrap()
                .collection_id
        );
        assert_eq!(
            downloaded.version_history.as_ref().unwrap().versions.len(),
            version_file
                .version_history
                .as_ref()
                .unwrap()
                .versions
                .len()
        );

        // Modify the version file
        let original_version_count = version_file
            .version_history
            .as_ref()
            .unwrap()
            .versions
            .len();
        let new_version = CollectionVersionInfo {
            version: 999,
            created_at_secs: 1234567890,
            marked_for_deletion: false,
            segment_info: Some(CollectionSegmentInfo {
                segment_compaction_info: vec![FlushSegmentCompactionInfo {
                    segment_id: "test_segment_999".to_string(),
                    file_paths: std::collections::HashMap::from([(
                        "test_key".to_string(),
                        chroma_types::chroma_proto::FilePaths {
                            paths: vec!["test_file_999.bin".to_string()],
                        },
                    )]),
                }],
            }),
            collection_info_mutable: None,
            version_change_reason: 0,
            version_file_name: "modified_file.binpb".to_string(),
        };
        version_file
            .version_history
            .as_mut()
            .unwrap()
            .versions
            .push(new_version);

        // Upload modified version with version ID matching the new version
        let modified_version_id = 999;

        // Update collection version to match the new version
        let mut modified_collection = collection.clone();
        modified_collection.version = 999;

        let reupload_result = manager
            .upload(
                &version_file,
                &modified_collection,
                VersionFileType::Compaction,
                modified_version_id,
            )
            .await;
        let modified_path = reupload_result.unwrap();

        // Validate that the path follows the new Go-style format: {version_id:06d}_{uuid}_flush
        // We'll check that the path contains the expected components
        assert!(modified_path.contains(&format!(
            "tenant/{}/database/{}/collection/{}/versionfiles/",
            modified_collection.tenant,
            modified_collection.database,
            modified_collection.collection_id
        )));
        assert!(modified_path.contains("_flush"));

        // Check that the version ID part is present in the path (formatted as 6 digits)
        assert!(modified_path.contains(&format!("{:06}", modified_version_id)));

        // Download modified version and validate changes
        let mut collection_with_modified_path = modified_collection.clone();
        collection_with_modified_path.version_file_path = Some(modified_path.clone());
        let modified_downloaded = manager.fetch(&collection_with_modified_path).await.unwrap();
        assert_eq!(
            modified_downloaded
                .collection_info_immutable
                .as_ref()
                .unwrap()
                .collection_id,
            version_file
                .collection_info_immutable
                .as_ref()
                .unwrap()
                .collection_id
        );
        assert_eq!(
            modified_downloaded
                .version_history
                .as_ref()
                .unwrap()
                .versions
                .len(),
            original_version_count + 1
        );
        assert_eq!(
            modified_downloaded
                .version_history
                .as_ref()
                .unwrap()
                .versions
                .last()
                .unwrap()
                .version,
            999
        );

        // Verify original file is unchanged
        let original_downloaded = manager.fetch(&collection_with_initial_path).await.unwrap();
        assert_eq!(
            original_downloaded
                .version_history
                .as_ref()
                .unwrap()
                .versions
                .len(),
            original_version_count
        );
    }
}
