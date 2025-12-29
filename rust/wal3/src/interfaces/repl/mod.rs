use std::sync::Arc;

use google_cloud_spanner::client::Client;
use uuid::Uuid;

mod fragment_manager;
mod manifest_manager;

use crate::{Error, FragmentUuid, LogWriterOptions, Manifest};

use super::batch_manager::BatchManager;
use super::{FragmentManagerFactory, ManifestManagerFactory};
use fragment_manager::{
    FragmentReader, ReplicatedFragmentOptions, ReplicatedFragmentUploader, StorageWrapper,
};
use manifest_manager::ManifestManager;

pub struct ReplicatedFragmentManagerFactory {
    write: LogWriterOptions,
    repl: ReplicatedFragmentOptions,
    storages: Arc<Vec<StorageWrapper>>,
}

#[async_trait::async_trait]
impl FragmentManagerFactory for ReplicatedFragmentManagerFactory {
    type FragmentPointer = FragmentUuid;
    type Publisher = BatchManager<FragmentUuid, fragment_manager::ReplicatedFragmentUploader>;
    type Consumer = fragment_manager::FragmentReader;

    async fn make_publisher(&self) -> Result<Self::Publisher, Error> {
        let fragment_uploader = ReplicatedFragmentUploader::new(
            self.repl.clone(),
            self.write.clone(),
            Arc::clone(&self.storages),
        );
        BatchManager::new(self.write.clone(), fragment_uploader)
            .ok_or_else(|| Error::internal(file!(), line!()))
    }

    async fn make_consumer(&self) -> Result<Self::Consumer, Error> {
        Ok(FragmentReader)
    }
}

pub struct ReplicatedManifestManagerFactory {
    spanner: Arc<Client>,
    log_id: Uuid,
}

#[async_trait::async_trait]
impl ManifestManagerFactory for ReplicatedManifestManagerFactory {
    type FragmentPointer = FragmentUuid;
    type Publisher = ManifestManager;
    type Consumer = ManifestManager;

    async fn init_manifest(&self, manifest: &Manifest) -> Result<(), Error> {
        ManifestManager::init(&self.spanner, self.log_id, manifest).await
    }

    async fn open_publisher(&self) -> Result<Self::Publisher, Error> {
        Ok(ManifestManager::new(Arc::clone(&self.spanner), self.log_id))
    }

    async fn make_consumer(&self) -> Result<Self::Consumer, Error> {
        Ok(ManifestManager::new(Arc::clone(&self.spanner), self.log_id))
    }
}
