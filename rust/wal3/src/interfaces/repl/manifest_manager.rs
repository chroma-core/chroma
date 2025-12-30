use std::convert::TryFrom;
use std::sync::Arc;

use google_cloud_spanner::client::Client;
use google_cloud_spanner::key::Key;
use google_cloud_spanner::mutation::{insert, update};
use google_cloud_spanner::statement::Statement;
use setsum::Setsum;
use uuid::Uuid;

use crate::interfaces::{ManifestConsumer, ManifestPublisher};
use crate::{
    Error, Fragment, FragmentIdentifier, FragmentUuid, Garbage, GarbageCollectionOptions,
    LogPosition, Manifest, ManifestAndWitness, ManifestWitness, Snapshot, SnapshotPointer,
};

pub struct ManifestManager {
    spanner: Arc<Client>,
    log_id: Uuid,
}

impl ManifestManager {
    pub fn new(spanner: Arc<Client>, log_id: Uuid) -> Self {
        Self { spanner, log_id }
    }

    pub async fn init(spanner: &Client, log_id: Uuid, manifest: &Manifest) -> Result<(), Error> {
        let mutations = vec![insert(
            "manifests",
            &[
                "log_id",
                "setsum",
                "collected",
                "acc_bytes",
                "writer",
                "enumeration_offset",
            ],
            &[
                &log_id.to_string(),
                &manifest.setsum.hexdigest(),
                &manifest.collected.hexdigest(),
                &(manifest.acc_bytes as i64),
                &"spanner init",
                &(manifest
                    .initial_offset
                    .unwrap_or(LogPosition::from_offset(1))
                    .offset() as i64),
            ],
        )];
        spanner.apply(mutations).await?;
        Ok(())
    }

    /// Manifest storers and accessors
    async fn head(
        spanner: &Client,
        log_id: Uuid,
        witness: &ManifestWitness,
    ) -> Result<bool, Error> {
        let ManifestWitness::Position(position) = witness else {
            return Err(Error::internal(file!(), line!()));
        };
        let mut stmt = Statement::new(
            "SELECT enumeration_offset FROM manifests WHERE log_id = @log_id LIMIT 1",
        );
        stmt.add_param("log_id", &log_id.to_string());
        let mut tx = spanner.read_only_transaction().await?;
        let mut reader = tx.query(stmt).await?;
        while let Some(row) = reader.next().await? {
            let enumeration_offset = row.column_by_name::<i64>("enumeration_offset")?;
            if enumeration_offset as u64 == position.offset() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn load(
        spanner: &Client,
        log_id: Uuid,
    ) -> Result<Option<(Manifest, ManifestWitness)>, Error> {
        let mut stmt1 = Statement::new(
            "SELECT setsum, collected, acc_bytes, writer, enumeration_offset FROM manifests WHERE log_id = @log_id"
        );
        stmt1.add_param("log_id", &log_id.to_string());
        let mut stmt2 = Statement::new(
            "SELECT ident, path, position_start, position_limit, num_bytes, setsum FROM fragments WHERE log_id = @log_id"
        );
        stmt2.add_param("log_id", &log_id.to_string());
        let mut tx = spanner.read_only_transaction().await?;
        // Load the manifest table
        let mut manifest = tx.query(stmt1).await?;
        let Some(manifest_row) = manifest.next().await? else {
            return Err(Error::ManifestMissing { log_id });
        };
        let setsum = manifest_row.column_by_name::<String>("setsum")?;
        let collected = manifest_row.column_by_name::<String>("collected")?;
        let acc_bytes = manifest_row.column_by_name::<i64>("acc_bytes")?;
        let writer = manifest_row.column_by_name::<String>("writer")?;
        let enumeration_offset = manifest_row.column_by_name::<i64>("enumeration_offset")?;
        let Some(setsum) = Setsum::from_hexdigest(&setsum) else {
            return Err(Error::CorruptManifest(format!(
                "invalid setsum {setsum} for manifest {log_id}"
            )));
        };
        let Some(collected) = Setsum::from_hexdigest(&collected) else {
            return Err(Error::CorruptManifest(format!(
                "invalid collected setsum {collected} for manifest {log_id}"
            )));
        };
        if acc_bytes < 0 {
            return Err(Error::CorruptManifest(format!(
                "negative acc_bytes {acc_bytes} for manifest {log_id}"
            )));
        }
        let acc_bytes = acc_bytes as u64;
        if enumeration_offset < 0 {
            return Err(Error::CorruptManifest(format!(
                "negative enumeration_offset {enumeration_offset} for manifest {log_id}"
            )));
        }
        let enumeration_offset = enumeration_offset as u64;
        // Load the fragments.
        let mut fragments = vec![];
        let mut fragments_quer = tx.query(stmt2).await?;
        while let Some(row) = fragments_quer.next().await? {
            let ident = row.column_by_name::<String>("ident")?;
            let path = row.column_by_name::<String>("path")?;
            let position_start = row.column_by_name::<i64>("position_start")?;
            let position_limit = row.column_by_name::<i64>("position_limit")?;
            let num_bytes = row.column_by_name::<i64>("num_bytes")?;
            let setsum = row.column_by_name::<String>("setsum")?;
            let Ok(ident) = Uuid::parse_str(&ident) else {
                return Err(Error::CorruptFragment(format!(
                    "invalid fragment ident {ident} for manifest {log_id}"
                )));
            };
            if position_start < 0 {
                return Err(Error::CorruptFragment(format!(
                    "negative position_start {position_start} for fragment {ident}"
                )));
            }
            if position_limit < 0 {
                return Err(Error::CorruptFragment(format!(
                    "negative position_limit {position_limit} for fragment {ident}"
                )));
            }
            if num_bytes < 0 {
                return Err(Error::CorruptFragment(format!(
                    "negative num_bytes {num_bytes} for fragment {ident}"
                )));
            }
            let start = LogPosition::from_offset(position_start as u64);
            let limit = LogPosition::from_offset(position_limit as u64);
            let num_bytes = num_bytes as u64;
            let Some(setsum) = Setsum::from_hexdigest(&setsum) else {
                return Err(Error::CorruptFragment(format!(
                    "invalid setsum {setsum} for fragment {ident}"
                )));
            };
            let seq_no = FragmentIdentifier::Uuid(FragmentUuid(ident));
            let fragment = Fragment {
                seq_no,
                path,
                num_bytes,
                setsum,
                start,
                limit,
            };
            fragments.push(fragment);
        }
        // Construct the manifest and witness.
        let manifest = Manifest {
            setsum,
            collected,
            acc_bytes,
            snapshots: vec![],
            fragments,
            initial_offset: None,
            initial_seq_no: None,
            writer,
        };
        let manifest_witness =
            ManifestWitness::Position(LogPosition::from_offset(enumeration_offset));
        Ok(Some((manifest, manifest_witness)))
    }
}

#[async_trait::async_trait]
impl ManifestPublisher<FragmentUuid> for ManifestManager {
    /// Recover the manifest so that it can do work.
    async fn recover(&mut self) -> Result<(), Error> {
        // NOTE(rescrv):  In the repl case we assume the person doing the replication will
        // eventually install the manifest in Spanner.  Fragments not in the manifest are not to be
        // linked into the manifest; they should be garbage-collected instead.
        Ok(())
    }

    /// Return a possibly-stale version of the manifest.
    async fn manifest_and_witness(&self) -> Result<ManifestAndWitness, Error> {
        let Some((manifest, witness)) =
            <Self as ManifestPublisher<FragmentUuid>>::manifest_load(self).await?
        else {
            return Err(Error::UninitializedLog);
        };
        Ok(ManifestAndWitness { manifest, witness })
    }

    /// Assign a timestamp for the next fragment that's going to be published on this manifest.
    fn assign_timestamp(&self, _: usize) -> Option<FragmentUuid> {
        Some(FragmentUuid::generate())
    }

    /// Publish a fragment previously assigned a timestamp using assign_timestamp.
    async fn publish_fragment(
        &self,
        pointer: &FragmentUuid,
        path: &str,
        messages_len: u64,
        num_bytes: u64,
        setsum: Setsum,
    ) -> Result<LogPosition, Error> {
        if messages_len > i64::MAX as u64 {
            return Err(Error::ReplicationBatchTooLarge {
                messages_len,
                limit: i64::MAX as u64,
            });
        }
        let messages_len_i64 = messages_len as i64;
        let _log_position: Option<LogPosition> = None;
        let pointer = pointer.to_string();
        let path = path.to_string();
        let (_, log_position) = self
            .spanner
            .read_write_transaction(|tx| {
                let log_id = self.log_id.to_string();
                let pointer = pointer.clone();
                let path = path.clone();
                Box::pin(async move {
                    let row = tx
                        .read_row(
                            "manifests",
                            &["setsum", "enumeration_offset"],
                            Key::new(&log_id),
                        )
                        .await?;
                    let manifest_setsum = row
                        .as_ref()
                        .map(|x| x.column_by_name::<String>("setsum"))
                        .transpose()?
                        .ok_or_else(|| {
                            google_cloud_spanner::client::Error::InvalidConfig(format!(
                                "manifest not found for log_id: {}",
                                log_id
                            ))
                        })?
                        .clone();
                    let Some(manifest_setsum) = Setsum::from_hexdigest(manifest_setsum.as_ref())
                    else {
                        return Err(google_cloud_spanner::client::Error::InvalidConfig(format!(
                            "setsum not parseable for log_id: {}",
                            log_id
                        )));
                    };
                    let enumeration_offset = row
                        .map(|x| x.column_by_name::<i64>("enumeration_offset"))
                        .transpose()?
                        .ok_or_else(|| {
                            google_cloud_spanner::client::Error::InvalidConfig(format!(
                                "manifest not found for log_id: {}",
                                log_id
                            ))
                        })?;
                    if enumeration_offset < 0 {
                        return Ok(Err(Error::CorruptManifest(format!(
                            "negative enumeration_offset {enumeration_offset} for manifest {log_id}"
                        ))));
                    }
                    let enumeration_limit = match enumeration_offset.checked_add(messages_len_i64) {
                        Some(limit) => limit,
                        None => return Ok(Err(Error::LogFull)),
                    };
                    let num_bytes = match i64::try_from(num_bytes) {
                        Ok(num_bytes) => num_bytes,
                        Err(_) => {
                            return Ok(Err(Error::ReplicationConsistencyError(format!(
                                "num_bytes overflowed spanner range for manifest {log_id}"
                            ))))
                        }
                    };
                    let updated_setsum = (manifest_setsum + setsum).hexdigest();
                    let mutations = vec![
                        insert(
                            "fragments",
                            &[
                                "log_id",
                                "ident",
                                "path",
                                "position_start",
                                "position_limit",
                                "num_bytes",
                                "setsum",
                            ],
                            &[
                                &log_id,
                                &pointer,
                                &path,
                                &enumeration_offset,
                                &enumeration_limit,
                                &num_bytes,
                                &setsum.hexdigest(),
                            ],
                        ),
                        update(
                            "manifests",
                            &["log_id", "setsum", "enumeration_offset"],
                            // NOTE(rescrv):  Pass in enumeration_limit so that we advance the enumeration offset.
                            &[&log_id, &updated_setsum, &enumeration_limit],
                        ),
                    ];
                    tx.buffer_write(mutations);
                    // TODO(rescrv): buffer writes to manifest enum offset, too
                    Ok::<Result<LogPosition, Error>, google_cloud_spanner::client::Error>(Ok(
                        LogPosition::from_offset(enumeration_offset as u64),
                    ))
                })
            })
            .await?;
        let log_position = log_position?;
        Ok(log_position)
    }

    /// Check if the garbge will apply "cleanly", that is without violating invariants.
    async fn garbage_applies_cleanly(&self, garbage: &Garbage) -> Result<bool, Error> {
        // TODO(rescrv, mcmr.gc):  Check that it applies cleanly.
        Ok(true)
    }

    /// Apply a garbage file to the manifest.
    async fn apply_garbage(&self, garbage: Garbage) -> Result<(), Error> {
        // TODO(rescrv, mcmr.gc):  Apply the garbage.
        Ok(())
    }

    /// Compute the garbage assuming at least log position will be kept.
    async fn compute_garbage(
        &self,
        options: &GarbageCollectionOptions,
        first_to_keep: LogPosition,
    ) -> Result<Option<Garbage>, Error> {
        // TODO(rescrv, mcmr.gc):  Compute the garbage.
        Ok(None)
    }

    /// Snapshot storers and accessors
    async fn snapshot_load(&self, _: &SnapshotPointer) -> Result<Option<Snapshot>, Error> {
        Err(Error::internal(file!(), line!()))
    }

    async fn snapshot_install(&self, _: &Snapshot) -> Result<SnapshotPointer, Error> {
        Err(Error::internal(file!(), line!()))
    }

    /// Manifest storers and accessors
    async fn manifest_head(&self, witness: &ManifestWitness) -> Result<bool, Error> {
        Self::head(&self.spanner, self.log_id, witness).await
    }

    async fn manifest_load(&self) -> Result<Option<(Manifest, ManifestWitness)>, Error> {
        Self::load(&self.spanner, self.log_id).await
    }

    /// Shutdown the manifest manager.  Must be called between prepare and finish of
    /// FragmentPublisher shutdown.
    fn shutdown(&self) {}
}

#[async_trait::async_trait]
impl ManifestConsumer<FragmentUuid> for ManifestManager {
    /// Snapshot storers and accessors
    async fn snapshot_load(&self, pointer: &SnapshotPointer) -> Result<Option<Snapshot>, Error> {
        _ = pointer;
        Err(Error::internal(file!(), line!()))
    }

    /// Manifest storers and accessors
    async fn manifest_head(&self, witness: &ManifestWitness) -> Result<bool, Error> {
        Self::head(&self.spanner, self.log_id, witness).await
    }

    async fn manifest_load(&self) -> Result<Option<(Manifest, ManifestWitness)>, Error> {
        Self::load(&self.spanner, self.log_id).await
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

    use super::ManifestManager;
    use crate::interfaces::ManifestPublisher;
    use crate::{Error, FragmentUuid, LogPosition, Manifest, ManifestWitness};

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

    fn make_setsum(seed: u8) -> Setsum {
        match seed {
            1 => Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
            2 => Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
            _ => Setsum::from_hexdigest(
                "307d93deb6b3e91525dc277027bc34958d8f1e74965e4c027820c3596e0f2847",
            )
            .unwrap(),
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

    // Initialize a manifest and verify it can be loaded.
    #[tokio::test]
    async fn test_k8s_integration_init_and_load() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();

        let result = ManifestManager::init(&client, log_id, &manifest).await;
        assert!(result.is_ok(), "init failed: {:?}", result.err());

        let loaded = ManifestManager::load(&client, log_id).await;
        assert!(loaded.is_ok(), "load failed: {:?}", loaded.err());

        let (loaded_manifest, witness) = loaded.unwrap().expect("manifest should exist");
        assert_eq!(loaded_manifest.setsum, manifest.setsum);
        assert_eq!(loaded_manifest.collected, manifest.collected);
        assert_eq!(loaded_manifest.acc_bytes, manifest.acc_bytes);

        let ManifestWitness::Position(pos) = witness else {
            panic!("expected Position witness");
        };
        assert_eq!(pos.offset(), 0, "initial enumeration_offset should be 0");

        println!(
            "test_k8s_integration_init_and_load: log_id={}, witness={:?}",
            log_id, pos
        );
    }

    // Load a non-existent manifest returns ManifestMissing error.
    #[tokio::test]
    async fn test_k8s_integration_load_nonexistent() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let result = ManifestManager::load(&client, log_id).await;

        match result {
            Err(Error::ManifestMissing { log_id: missing_id }) => {
                assert_eq!(missing_id, log_id);
                println!(
                    "test_k8s_integration_load_nonexistent: correctly returned ManifestMissing"
                );
            }
            other => panic!("expected ManifestMissing, got {:?}", other),
        }
    }

    // Test manifest_head returns true when witness matches current state.
    #[tokio::test]
    async fn test_k8s_integration_manifest_head_matches() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();
        ManifestManager::init(&client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), log_id);
        let witness = ManifestWitness::Position(LogPosition::from_offset(0));

        let result = manager.manifest_head(&witness).await;
        assert!(result.is_ok(), "manifest_head failed: {:?}", result.err());
        assert!(
            result.unwrap(),
            "manifest_head should return true for matching witness"
        );

        println!("test_k8s_integration_manifest_head_matches: passed");
    }

    // Test manifest_head returns false when witness does not match.
    #[tokio::test]
    async fn test_k8s_integration_manifest_head_mismatch() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();
        ManifestManager::init(&client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), log_id);
        let witness = ManifestWitness::Position(LogPosition::from_offset(999));

        let result = manager.manifest_head(&witness).await;
        assert!(result.is_ok(), "manifest_head failed: {:?}", result.err());
        assert!(
            !result.unwrap(),
            "manifest_head should return false for mismatched witness"
        );

        println!("test_k8s_integration_manifest_head_mismatch: passed");
    }

    // Test manifest_and_witness via ManifestPublisher trait.
    #[tokio::test]
    async fn test_k8s_integration_manifest_and_witness() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();
        ManifestManager::init(&client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), log_id);
        let result = manager.manifest_and_witness().await;
        assert!(
            result.is_ok(),
            "manifest_and_witness failed: {:?}",
            result.err()
        );

        let maw = result.unwrap();
        assert_eq!(maw.manifest.setsum, manifest.setsum);
        println!(
            "test_k8s_integration_manifest_and_witness: witness={:?}",
            maw.witness
        );
    }

    // Test manifest_and_witness returns error for missing manifest.
    #[tokio::test]
    async fn test_k8s_integration_manifest_and_witness_uninitialized() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manager = ManifestManager::new(Arc::new(client), log_id);
        let result = manager.manifest_and_witness().await;

        match result {
            Err(Error::ManifestMissing { .. }) => {
                println!(
                    "test_k8s_integration_manifest_and_witness_uninitialized: returned ManifestMissing"
                );
            }
            other => panic!("expected ManifestMissing, got {:?}", other),
        }
    }

    // Test assign_timestamp generates unique UUIDs.
    #[tokio::test]
    async fn test_k8s_integration_assign_timestamp() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manager = ManifestManager::new(Arc::new(client), log_id);

        let ts1 = manager.assign_timestamp(0);
        let ts2 = manager.assign_timestamp(0);

        assert!(ts1.is_some(), "assign_timestamp should return Some");
        assert!(ts2.is_some(), "assign_timestamp should return Some");
        assert_ne!(ts1.unwrap(), ts2.unwrap(), "timestamps should be unique");

        println!("test_k8s_integration_assign_timestamp: passed");
    }

    // Test publish_fragment inserts a fragment and updates enumeration_offset.
    #[tokio::test]
    async fn test_k8s_integration_publish_fragment() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();
        ManifestManager::init(&client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), log_id);
        let pointer = FragmentUuid::generate();
        let path = "test/path/fragment.parquet";
        let messages_len = 10u64;
        let num_bytes = 1024u64;
        let setsum = make_setsum(1);

        let result = manager
            .publish_fragment(&pointer, path, messages_len, num_bytes, setsum)
            .await;

        assert!(
            result.is_ok(),
            "publish_fragment failed: {:?}",
            result.err()
        );
        let log_position = result.unwrap();
        assert_eq!(
            log_position.offset(),
            0,
            "first fragment should start at offset 0"
        );

        // Verify enumeration_offset was updated by checking manifest_head.
        let witness = ManifestWitness::Position(LogPosition::from_offset(messages_len));
        let head_result = manager.manifest_head(&witness).await;
        assert!(head_result.is_ok(), "manifest_head failed");
        assert!(
            head_result.unwrap(),
            "enumeration_offset should have advanced to messages_len"
        );

        println!(
            "test_k8s_integration_publish_fragment: log_position={}",
            log_position.offset()
        );
    }

    // Test publishing multiple fragments in sequence.
    #[tokio::test]
    async fn test_k8s_integration_publish_multiple_fragments() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();
        ManifestManager::init(&client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), log_id);

        // Publish first fragment.
        let pointer1 = FragmentUuid::generate();
        let pos1 = manager
            .publish_fragment(&pointer1, "path1", 10, 100, make_setsum(1))
            .await
            .expect("first publish failed");
        assert_eq!(pos1.offset(), 0);

        // Publish second fragment.
        let pointer2 = FragmentUuid::generate();
        let pos2 = manager
            .publish_fragment(&pointer2, "path2", 20, 200, make_setsum(2))
            .await
            .expect("second publish failed");
        assert_eq!(pos2.offset(), 10);

        // Publish third fragment.
        let pointer3 = FragmentUuid::generate();
        let pos3 = manager
            .publish_fragment(&pointer3, "path3", 30, 300, make_setsum(3))
            .await
            .expect("third publish failed");
        assert_eq!(pos3.offset(), 30);

        // Verify final enumeration_offset.
        let witness = ManifestWitness::Position(LogPosition::from_offset(60));
        let head_result = manager.manifest_head(&witness).await;
        assert!(head_result.is_ok() && head_result.unwrap());

        println!(
            "test_k8s_integration_publish_multiple_fragments: positions={}, {}, {}",
            pos1.offset(),
            pos2.offset(),
            pos3.offset()
        );
    }

    // Test publish_fragment with messages_len exceeding i64::MAX.
    #[tokio::test]
    async fn test_k8s_integration_publish_fragment_too_large() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();
        ManifestManager::init(&client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), log_id);
        let pointer = FragmentUuid::generate();

        let messages_len = (i64::MAX as u64) + 1;
        let result = manager
            .publish_fragment(&pointer, "path", messages_len, 100, Setsum::default())
            .await;

        match result {
            Err(Error::ReplicationBatchTooLarge { .. }) => {
                println!(
                    "test_k8s_integration_publish_fragment_too_large: correctly returned ReplicationBatchTooLarge"
                );
            }
            other => panic!("expected ReplicationBatchTooLarge, got {:?}", other),
        }
    }

    // Test that load returns fragments after publish_fragment.
    #[tokio::test]
    async fn test_k8s_integration_load_with_fragments() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();
        ManifestManager::init(&client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client.clone()), log_id);

        // Publish two fragments.
        let pointer1 = FragmentUuid::generate();
        let setsum1 = make_setsum(1);
        manager
            .publish_fragment(&pointer1, "path/frag1.parquet", 5, 500, setsum1)
            .await
            .expect("publish failed");

        let pointer2 = FragmentUuid::generate();
        let setsum2 = make_setsum(2);
        manager
            .publish_fragment(&pointer2, "path/frag2.parquet", 10, 1000, setsum2)
            .await
            .expect("publish failed");

        // Load and verify fragments.
        let (loaded, witness) = ManifestManager::load(&client, log_id)
            .await
            .expect("load failed")
            .expect("manifest should exist");

        assert_eq!(loaded.fragments.len(), 2, "should have 2 fragments");

        let ManifestWitness::Position(pos) = witness else {
            panic!("expected Position witness");
        };
        assert_eq!(pos.offset(), 15, "enumeration_offset should be 5 + 10");

        println!(
            "test_k8s_integration_load_with_fragments: loaded {} fragments",
            loaded.fragments.len()
        );
    }

    // Test recover is a no-op that succeeds.
    #[tokio::test]
    async fn test_k8s_integration_recover() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let mut manager = ManifestManager::new(Arc::new(client), log_id);
        let result = manager.recover().await;
        assert!(result.is_ok(), "recover should succeed");

        println!("test_k8s_integration_recover: passed");
    }

    // Test shutdown is a no-op that completes.
    #[tokio::test]
    async fn test_k8s_integration_shutdown() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manager = ManifestManager::new(Arc::new(client), log_id);
        manager.shutdown();

        println!("test_k8s_integration_shutdown: passed");
    }

    // Test init with non-default manifest values.
    #[tokio::test]
    async fn test_k8s_integration_init_with_nondefault_manifest() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let setsum = make_setsum(1);
        let collected = make_setsum(2);
        let manifest = Manifest {
            setsum,
            collected,
            acc_bytes: 12345,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: Some(LogPosition::from_offset(100)),
            initial_seq_no: None,
            writer: "custom-writer".to_string(),
        };

        ManifestManager::init(&client, log_id, &manifest)
            .await
            .expect("init failed");

        let (loaded, _) = ManifestManager::load(&client, log_id)
            .await
            .expect("load failed")
            .expect("manifest should exist");

        assert_eq!(loaded.setsum, setsum);
        assert_eq!(loaded.collected, collected);
        assert_eq!(loaded.acc_bytes, 12345);

        println!("test_k8s_integration_init_with_nondefault_manifest: passed");
    }
}
