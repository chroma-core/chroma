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
                "initial_offset",
                "initial_seq_no",
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
                &0,
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
                            "records",
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
