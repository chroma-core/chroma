use std::sync::Arc;

use chroma_storage::ETag;
use chroma_storage::Storage;

use crate::interfaces::s3::ManifestManager;
use crate::interfaces::{ManifestConsumer, ManifestWitness};
use crate::Cursor;
use crate::CursorStore;
use crate::CursorStoreOptions;
use crate::CursorWitness;
use crate::Error;
use crate::FragmentSeqNo;
use crate::LogPosition;
use crate::LogReaderOptions;
use crate::Manifest;
use crate::Snapshot;
use crate::SnapshotCache;
use crate::SnapshotPointer;
use crate::ThrottleOptions;
use crate::INTRINSIC_CURSOR;

pub struct ManifestReader {
    options: LogReaderOptions,
    storage: Arc<Storage>,
    prefix: String,
    snapshot_cache: Arc<dyn SnapshotCache>,
}

impl ManifestReader {
    pub fn new(
        options: LogReaderOptions,
        storage: Arc<Storage>,
        prefix: String,
        snapshot_cache: Arc<dyn SnapshotCache>,
    ) -> Self {
        Self {
            options,
            storage,
            prefix,
            snapshot_cache,
        }
    }

    /// Load the latest manifest from object storage.
    pub async fn load(
        options: &ThrottleOptions,
        storage: &Storage,
        prefix: &str,
    ) -> Result<Option<(Manifest, ETag)>, Error> {
        super::manifest_load(options, storage, prefix).await
    }
}

#[async_trait::async_trait]
impl ManifestConsumer<(FragmentSeqNo, LogPosition)> for ManifestReader {
    async fn snapshot_load(&self, pointer: &SnapshotPointer) -> Result<Option<Snapshot>, Error> {
        super::snapshot_load(
            self.options.throttle,
            &self.storage,
            &self.prefix,
            &self.snapshot_cache,
            pointer,
        )
        .await
    }

    async fn manifest_head(&self, witness: &ManifestWitness) -> Result<bool, Error> {
        let ManifestWitness::ETag(e_tag) = witness else {
            return Err(Error::internal(file!(), line!()));
        };
        ManifestManager::head(&self.options.throttle, &self.storage, &self.prefix, e_tag).await
    }

    async fn manifest_load(&self) -> Result<Option<(Manifest, ManifestWitness)>, Error> {
        match ManifestManager::load(&self.options.throttle, &self.storage, &self.prefix).await {
            Ok(Some((manifest, e_tag))) => Ok(Some((manifest, ManifestWitness::ETag(e_tag)))),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn update_intrinsic_cursor(
        &self,
        position: LogPosition,
        epoch_us: u64,
        writer: &str,
        allow_rollback: bool,
    ) -> Result<Option<CursorWitness>, Error> {
        let cursor_store = CursorStore::new(
            CursorStoreOptions::default(),
            Arc::clone(&self.storage),
            self.prefix.clone(),
            writer.to_string(),
        );
        let witness = cursor_store.load(&INTRINSIC_CURSOR).await?;
        let default = Cursor::default();
        let current = witness.as_ref().map(|w| w.cursor()).unwrap_or(&default);
        if !allow_rollback && current.position.offset() > position.offset() {
            return Ok(None);
        }
        let cursor = Cursor {
            position,
            epoch_us,
            writer: writer.to_string(),
        };
        let new_witness = if let Some(witness) = witness.as_ref() {
            cursor_store
                .save(&INTRINSIC_CURSOR, &cursor, witness)
                .await?
        } else {
            cursor_store.init(&INTRINSIC_CURSOR, cursor).await?
        };
        Ok(Some(new_witness))
    }

    async fn load_intrinsic_cursor(&self) -> Result<Option<LogPosition>, Error> {
        let cursor_store = CursorStore::new(
            CursorStoreOptions::default(),
            Arc::clone(&self.storage),
            self.prefix.clone(),
            "load_intrinsic_cursor".to_string(),
        );
        let witness = cursor_store.load(&INTRINSIC_CURSOR).await?;
        Ok(witness.map(|w| w.cursor.position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::interfaces::ManifestConsumer;

    fn make_reader(storage: Arc<Storage>, prefix: &str) -> ManifestReader {
        ManifestReader::new(
            LogReaderOptions::default(),
            storage,
            prefix.to_string(),
            Arc::new(()),
        )
    }

    #[tokio::test]
    async fn test_k8s_integration_update_intrinsic_cursor_initializes_when_absent() {
        let storage = Arc::new(chroma_storage::s3_client_for_test_with_new_bucket().await);
        let reader = make_reader(Arc::clone(&storage), "init_absent");

        let result = reader
            .update_intrinsic_cursor(LogPosition::from_offset(42), 1000, "writer-a", false)
            .await
            .unwrap();

        let witness = result.expect("should return Some on first init");
        assert_eq!(witness.cursor().position, LogPosition::from_offset(42));
        println!("update_intrinsic_cursor_initializes_when_absent: passed");
    }

    #[tokio::test]
    async fn test_k8s_integration_update_intrinsic_cursor_advances_forward() {
        let storage = Arc::new(chroma_storage::s3_client_for_test_with_new_bucket().await);
        let reader = make_reader(Arc::clone(&storage), "advance_forward");

        reader
            .update_intrinsic_cursor(LogPosition::from_offset(10), 1000, "writer-a", false)
            .await
            .unwrap()
            .expect("first init should succeed");

        let witness = reader
            .update_intrinsic_cursor(LogPosition::from_offset(50), 2000, "writer-a", false)
            .await
            .unwrap()
            .expect("forward update should succeed");
        assert_eq!(witness.cursor().position, LogPosition::from_offset(50));
        println!("update_intrinsic_cursor_advances_forward: passed");
    }

    #[tokio::test]
    async fn test_k8s_integration_update_intrinsic_cursor_rollback_guard_blocks() {
        let storage = Arc::new(chroma_storage::s3_client_for_test_with_new_bucket().await);
        let reader = make_reader(Arc::clone(&storage), "rollback_guard");

        reader
            .update_intrinsic_cursor(LogPosition::from_offset(100), 1000, "writer-a", false)
            .await
            .unwrap()
            .expect("init should succeed");

        let result = reader
            .update_intrinsic_cursor(LogPosition::from_offset(50), 2000, "writer-a", false)
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "rollback should be blocked when allow_rollback is false"
        );

        // Verify cursor was not changed.
        let cursor_store = CursorStore::new(
            CursorStoreOptions::default(),
            Arc::clone(&storage),
            "rollback_guard".to_string(),
            "writer-a".to_string(),
        );
        let loaded = cursor_store.load(&INTRINSIC_CURSOR).await.unwrap().unwrap();
        assert_eq!(loaded.cursor().position, LogPosition::from_offset(100));
        println!("update_intrinsic_cursor_rollback_guard_blocks: passed");
    }

    #[tokio::test]
    async fn test_k8s_integration_update_intrinsic_cursor_allow_rollback() {
        let storage = Arc::new(chroma_storage::s3_client_for_test_with_new_bucket().await);
        let reader = make_reader(Arc::clone(&storage), "allow_rollback");

        reader
            .update_intrinsic_cursor(LogPosition::from_offset(100), 1000, "writer-a", false)
            .await
            .unwrap()
            .expect("init should succeed");

        let witness = reader
            .update_intrinsic_cursor(LogPosition::from_offset(50), 2000, "writer-a", true)
            .await
            .unwrap()
            .expect("rollback should succeed when allow_rollback is true");
        assert_eq!(witness.cursor().position, LogPosition::from_offset(50));
        println!("update_intrinsic_cursor_allow_rollback: passed");
    }
}
