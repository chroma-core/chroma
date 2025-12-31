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

impl ReplicatedManifestManagerFactory {
    /// Creates a new ReplicatedManifestManagerFactory.
    pub fn new(spanner: Arc<Client>, log_id: Uuid) -> Self {
        Self { spanner, log_id }
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chroma_config::spanner::SpannerEmulatorConfig;
    use google_cloud_gax::conn::Environment;
    use google_cloud_spanner::client::{Client, ClientConfig};
    use setsum::Setsum;
    use uuid::Uuid;

    use super::{
        ReplicatedFragmentManagerFactory, ReplicatedFragmentOptions,
        ReplicatedManifestManagerFactory, StorageWrapper,
    };
    use crate::interfaces::{FragmentManagerFactory, ManifestManagerFactory};
    use crate::{LogPosition, LogWriterOptions, Manifest};

    fn emulator_config() -> SpannerEmulatorConfig {
        SpannerEmulatorConfig {
            host: "localhost".to_string(),
            grpc_port: 9010,
            rest_port: 9020,
            project: "local-project".to_string(),
            instance: "test-instance".to_string(),
            database: "local-database".to_string(),
        }
    }

    async fn setup_spanner_client() -> Option<Client> {
        let emulator = emulator_config();
        let client_config = ClientConfig {
            environment: Environment::Emulator(emulator.grpc_endpoint()),
            ..Default::default()
        };
        match Client::new(&emulator.database_path(), client_config).await {
            Ok(client) => Some(client),
            Err(e) => {
                eprintln!(
                    "Failed to connect to Spanner emulator: {:?}. Is Tilt running?",
                    e
                );
                None
            }
        }
    }

    fn make_empty_manifest() -> Manifest {
        Manifest {
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: Some(LogPosition::from_offset(0)),
            initial_seq_no: None,
            writer: "test-writer".to_string(),
        }
    }

    // ==================== ReplicatedFragmentManagerFactory tests ====================

    // Test make_publisher returns a BatchManager.
    #[tokio::test]
    async fn replicated_fragment_manager_factory_make_publisher() {
        use chroma_storage::s3_client_for_test_with_new_bucket;
        use std::time::Duration;

        let storage = s3_client_for_test_with_new_bucket().await;
        let wrapper = StorageWrapper::new("test-region".to_string(), storage, "prefix".to_string());
        let storages = Arc::new(vec![wrapper]);
        let options = ReplicatedFragmentOptions {
            minimum_allowed_replication_factor: 1,
            minimum_failures_to_exclude_replica: 100,
            decimation_interval: Duration::from_secs(3600),
            slow_writer_tolerance: Duration::from_secs(30),
        };
        let factory = ReplicatedFragmentManagerFactory {
            write: LogWriterOptions::default(),
            repl: options,
            storages,
        };

        let result = factory.make_publisher().await;
        assert!(
            result.is_ok(),
            "make_publisher should succeed: {:?}",
            result.err()
        );

        println!("replicated_fragment_manager_factory_make_publisher: passed");
    }

    // Test make_consumer returns a FragmentReader.
    #[tokio::test]
    async fn replicated_fragment_manager_factory_make_consumer() {
        use chroma_storage::s3_client_for_test_with_new_bucket;
        use std::time::Duration;

        let storage = s3_client_for_test_with_new_bucket().await;
        let wrapper = StorageWrapper::new("test-region".to_string(), storage, "prefix".to_string());
        let storages = Arc::new(vec![wrapper]);
        let options = ReplicatedFragmentOptions {
            minimum_allowed_replication_factor: 1,
            minimum_failures_to_exclude_replica: 100,
            decimation_interval: Duration::from_secs(3600),
            slow_writer_tolerance: Duration::from_secs(30),
        };
        let factory = ReplicatedFragmentManagerFactory {
            write: LogWriterOptions::default(),
            repl: options,
            storages,
        };

        let result = factory.make_consumer().await;
        assert!(
            result.is_ok(),
            "make_consumer should succeed: {:?}",
            result.err()
        );

        println!("replicated_fragment_manager_factory_make_consumer: passed");
    }

    // Test make_publisher with empty storages still succeeds (failure happens at upload time).
    #[tokio::test]
    async fn replicated_fragment_manager_factory_make_publisher_empty_storages() {
        use std::time::Duration;

        let storages: Arc<Vec<StorageWrapper>> = Arc::new(vec![]);
        let options = ReplicatedFragmentOptions {
            minimum_allowed_replication_factor: 1,
            minimum_failures_to_exclude_replica: 100,
            decimation_interval: Duration::from_secs(3600),
            slow_writer_tolerance: Duration::from_secs(30),
        };
        let factory = ReplicatedFragmentManagerFactory {
            write: LogWriterOptions::default(),
            repl: options,
            storages,
        };

        let result = factory.make_publisher().await;
        // BatchManager::new always succeeds; failure happens at upload time with no replicas.
        assert!(
            result.is_ok(),
            "make_publisher should succeed even with empty storages: {:?}",
            result.err()
        );

        println!("replicated_fragment_manager_factory_make_publisher_empty_storages: passed");
    }

    // ==================== ReplicatedManifestManagerFactory tests ====================

    // Test init_manifest delegates to ManifestManager::init.
    #[tokio::test]
    async fn replicated_manifest_manager_factory_init_manifest() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let factory = ReplicatedManifestManagerFactory::new(Arc::new(client), log_id);
        let manifest = make_empty_manifest();

        let result = factory.init_manifest(&manifest).await;
        assert!(
            result.is_ok(),
            "init_manifest should succeed: {:?}",
            result.err()
        );

        println!("replicated_manifest_manager_factory_init_manifest: passed");
    }

    // Test init_manifest fails on duplicate log_id.
    #[tokio::test]
    async fn replicated_manifest_manager_factory_init_manifest_duplicate() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let factory = ReplicatedManifestManagerFactory::new(Arc::new(client), log_id);
        let manifest = make_empty_manifest();

        let result1 = factory.init_manifest(&manifest).await;
        assert!(result1.is_ok(), "first init should succeed");

        let result2 = factory.init_manifest(&manifest).await;
        assert!(
            result2.is_err(),
            "second init should fail for duplicate log_id"
        );

        println!(
            "replicated_manifest_manager_factory_init_manifest_duplicate: error={:?}",
            result2.err()
        );
    }

    // Test open_publisher returns a ManifestManager.
    #[tokio::test]
    async fn replicated_manifest_manager_factory_open_publisher() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let factory = ReplicatedManifestManagerFactory::new(Arc::new(client), log_id);

        let result = factory.open_publisher().await;
        assert!(
            result.is_ok(),
            "open_publisher should succeed: {:?}",
            result.err()
        );

        println!("replicated_manifest_manager_factory_open_publisher: passed");
    }

    // Test make_consumer returns a ManifestManager.
    #[tokio::test]
    async fn replicated_manifest_manager_factory_make_consumer() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let factory = ReplicatedManifestManagerFactory::new(Arc::new(client), log_id);

        let result = factory.make_consumer().await;
        assert!(
            result.is_ok(),
            "make_consumer should succeed: {:?}",
            result.err()
        );

        println!("replicated_manifest_manager_factory_make_consumer: passed");
    }

    // Test that publisher from factory can be used to publish fragments.
    #[tokio::test]
    async fn replicated_manifest_manager_factory_publisher_works() {
        use crate::interfaces::ManifestPublisher;
        use crate::FragmentUuid;
        use setsum::Setsum;

        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let factory = ReplicatedManifestManagerFactory::new(Arc::new(client), log_id);
        let manifest = make_empty_manifest();

        factory.init_manifest(&manifest).await.expect("init failed");

        let publisher = factory
            .open_publisher()
            .await
            .expect("open_publisher failed");
        let pointer = FragmentUuid::generate();
        let result = publisher
            .publish_fragment(&pointer, "test/path.parquet", 10, 100, Setsum::default())
            .await;

        assert!(
            result.is_ok(),
            "publish_fragment should succeed: {:?}",
            result.err()
        );

        println!(
            "replicated_manifest_manager_factory_publisher_works: position={}",
            result.unwrap().offset()
        );
    }

    // Test that consumer from factory can load manifest.
    #[tokio::test]
    async fn replicated_manifest_manager_factory_consumer_works() {
        use crate::interfaces::ManifestConsumer;

        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let factory = ReplicatedManifestManagerFactory::new(Arc::new(client), log_id);
        let manifest = make_empty_manifest();

        factory.init_manifest(&manifest).await.expect("init failed");

        let consumer = factory.make_consumer().await.expect("make_consumer failed");
        let result = consumer.manifest_load().await;

        assert!(
            result.is_ok(),
            "manifest_load should succeed: {:?}",
            result.err()
        );
        assert!(
            result.unwrap().is_some(),
            "manifest should exist after init"
        );

        println!("replicated_manifest_manager_factory_consumer_works: passed");
    }
}
