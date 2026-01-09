use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;

use google_cloud_spanner::client::Client;
use google_cloud_spanner::key::Key;
use google_cloud_spanner::mutation::{delete, insert, update};
use google_cloud_spanner::statement::Statement;
use setsum::Setsum;
use tonic::Code;
use uuid::Uuid;

use crate::interfaces::{ManifestConsumer, ManifestPublisher};
use crate::{
    Error, Fragment, FragmentIdentifier, FragmentSeqNo, FragmentUuid, Garbage,
    GarbageCollectionOptions, LogPosition, Manifest, ManifestAndWitness, ManifestWitness, Snapshot,
    SnapshotPointer,
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
        let enum_offset = manifest.fragments.iter().map(|f| f.limit).max().unwrap_or(
            manifest
                .initial_offset
                .unwrap_or(LogPosition::from_offset(1)),
        );
        let mut mutations = vec![insert(
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
                &(enum_offset.offset() as i64),
            ],
        )];
        // Also insert any fragments from the manifest.
        for fragment in &manifest.fragments {
            let FragmentIdentifier::Uuid(uuid) = fragment.seq_no else {
                return Err(Error::internal(file!(), line!()));
            };
            mutations.push(insert(
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
                    &log_id.to_string(),
                    &uuid.to_string(),
                    &fragment.path,
                    &(fragment.start.offset() as i64),
                    &(fragment.limit.offset() as i64),
                    &(fragment.num_bytes as i64),
                    &fragment.setsum.hexdigest(),
                ],
            ));
        }
        spanner
            .read_write_transaction(|tx| {
                let mutations = mutations.clone();
                Box::pin(async move {
                    tx.buffer_write(mutations);
                    Ok::<_, Error>(())
                })
            })
            .await?;
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
            return Ok(None);
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
        let mut fragments_query = tx.query(stmt2).await?;
        while let Some(row) = fragments_query.next().await? {
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
        // Sort fragments by position_start for sequential ordering expected by scrub.
        fragments.sort_by_key(|f| f.start);
        let initial_offset = fragments
            .iter()
            .map(|f| f.start)
            .min()
            .unwrap_or(LogPosition::from_offset(enumeration_offset));
        // Construct the manifest and witness.
        let manifest = Manifest {
            setsum,
            collected,
            acc_bytes,
            snapshots: vec![],
            fragments,
            initial_offset: Some(initial_offset),
            initial_seq_no: Some(FragmentUuid::generate().into()),
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
        //
        // Verify the log exists before returning success.
        let _ = self.manifest_and_witness().await?;
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
        regions: &[&str],
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
        let regions: Vec<String> = regions.iter().map(|s| s.to_string()).collect();
        for _ in 0..3 {
            let res = self
                .spanner
                .read_write_transaction(|tx| {
                    let log_id = self.log_id.to_string();
                    let pointer = pointer.clone();
                    let path = path.clone();
                    let regions = regions.clone();
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
                        let Some(manifest_setsum) =
                            Setsum::from_hexdigest(manifest_setsum.as_ref())
                        else {
                            return Err(google_cloud_spanner::client::Error::InvalidConfig(
                                format!("setsum not parseable for log_id: {}", log_id),
                            ));
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
                        let enumeration_limit =
                            match enumeration_offset.checked_add(messages_len_i64) {
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
                        // Read current acc_bytes to update it.
                        let acc_bytes_row = tx
                            .read_row("manifests", &["acc_bytes"], Key::new(&log_id))
                            .await?;
                        let current_acc_bytes = acc_bytes_row
                            .as_ref()
                            .map(|x| x.column_by_name::<i64>("acc_bytes"))
                            .transpose()?
                            .unwrap_or(0);
                        let updated_acc_bytes = current_acc_bytes.saturating_add(num_bytes);
                        let mut mutations = vec![
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
                                &["log_id", "setsum", "enumeration_offset", "acc_bytes"],
                                // NOTE(rescrv):  Pass in enumeration_limit so that we advance the enumeration offset.
                                &[
                                    &log_id,
                                    &updated_setsum,
                                    &enumeration_limit,
                                    &updated_acc_bytes,
                                ],
                            ),
                        ];
                        for region in regions.iter() {
                            mutations.push(insert(
                                "fragment_regions",
                                &["log_id", "ident", "region"],
                                &[&log_id, &pointer, region],
                            ));
                        }
                        tx.buffer_write(mutations);
                        Ok::<Result<LogPosition, Error>, google_cloud_spanner::client::Error>(Ok(
                            LogPosition::from_offset(enumeration_offset as u64),
                        ))
                    })
                })
                .await;
            let (_, log_position) = match res {
                Ok(x) => x,
                Err(err) => {
                    if let google_cloud_spanner::client::Error::GRPC(grpc) = &err {
                        if grpc.code() == Code::Aborted {
                            continue;
                        }
                    }
                    return Err(err.into());
                }
            };
            let log_position = log_position?;
            return Ok(log_position);
        }
        Err(Error::Backoff)
    }

    /// Check if the garbge will apply "cleanly", that is without violating invariants.
    async fn garbage_applies_cleanly(&self, garbage: &Garbage) -> Result<bool, Error> {
        if garbage.first_to_keep.offset() > i64::MAX as u64 {
            return Err(Error::Overflow(format!(
                "first_to_keep offset {} exceeds i64::MAX",
                garbage.first_to_keep.offset()
            )));
        }
        let mut stmt1 = Statement::new(
            "
            SELECT setsum
            FROM fragments
                INNER JOIN fragment_regions
                ON fragments.log_id = fragment_regions.log_id
                    AND fragments.ident = fragment_regions.ident
            WHERE fragments.log_id = @log_id
                AND fragments.position_limit <= @threshold
            ",
        );
        stmt1.add_param("log_id", &self.log_id.to_string());
        stmt1.add_param("threshold", &(garbage.first_to_keep.offset() as i64));
        let mut tx = self.spanner.read_only_transaction().await?;
        let mut iter = tx.query(stmt1).await?;
        let mut acc = Setsum::default();
        while let Some(row) = iter.next().await? {
            let cur = row.column_by_name::<String>("setsum")?;
            let cur = Setsum::from_hexdigest(&cur)
                .ok_or_else(|| Error::CorruptGarbage(format!("invalid setsum hexdigest: {cur}")))?;
            acc += cur;
        }
        Ok(acc == garbage.setsum_to_discard)
    }

    /// Apply a garbage file to the manifest.
    async fn apply_garbage(&self, garbage: Garbage) -> Result<(), Error> {
        garbage.check_invariants_for_repl()?;
        let mut acc = Setsum::default();
        let _cr = self
            .spanner
            .read_write_transaction(|tx| {
                let mut stmt1 = Statement::new(
                    "
                    SELECT fragments.ident, setsum, position_limit
                    FROM fragments
                        INNER JOIN fragment_regions
                        ON fragments.log_id = fragment_regions.log_id
                            AND fragments.ident = fragment_regions.ident
                    WHERE fragments.log_id = @log_id
                        AND fragments.position_limit <= @threshold
                        AND fragment_regions.region = @local_region
                ",
                );
                let log_id = self.log_id.to_string();
                stmt1.add_param("log_id", &log_id);
                stmt1.add_param("threshold", &(garbage.first_to_keep.offset() as i64));
                // TODO(rescrv, mcmr):  dummy region.
                stmt1.add_param("local_region", &"dummy");
                let mut stmt2 = Statement::new(
                    "
                    SELECT collected
                    FROM manifests
                    WHERE manifests.log_id = @log_id
                    LIMIT 1
                ",
                );
                stmt2.add_param("log_id", &log_id);
                let mut stmt3 = Statement::new(
                    "
                    SELECT fragments.ident, fragments.path, count(fragment_regions.region) as c
                    FROM fragments
                        INNER JOIN fragment_regions
                        ON fragments.log_id = fragment_regions.log_id
                            AND fragments.ident = fragment_regions.ident
                    WHERE fragments.log_id = @log_id
                        AND fragments.position_limit <= @threshold
                    GROUP BY fragments.ident, fragments.path
                    HAVING c <= 1
                ",
                );
                stmt3.add_param("log_id", &log_id);
                stmt3.add_param("threshold", &(garbage.first_to_keep.offset() as i64));
                // TODO(rescrv, mcmr):  dummy region.
                stmt3.add_param("local_region", &"dummy");
                Box::pin(async move {
                    let mut query = tx.query(stmt2).await?;
                    let Some(row) = query.next().await? else {
                        return Err(Error::UninitializedLog);
                    };
                    let collected = row.column_by_name::<String>("collected")?;
                    let collected = Setsum::from_hexdigest(&collected).ok_or_else(|| {
                        Error::CorruptGarbage(format!("invalid setsum hexdigest: {collected}"))
                    })?;
                    let mut iter = tx.query(stmt1).await?;
                    let mut mutations = vec![];
                    let mut selected: HashSet<String> = HashSet::default();
                    while let Some(row) = iter.next().await? {
                        let cur = row.column_by_name::<String>("setsum")?;
                        let cur = Setsum::from_hexdigest(&cur).ok_or_else(|| {
                            Error::CorruptGarbage(format!("invalid setsum hexdigest: {cur}"))
                        })?;
                        acc += cur;
                        let ident = row.column_by_name::<String>("ident")?;
                        // TODO(rescrv, mcmr):  Configure region.
                        mutations.push(delete(
                            "fragment_regions",
                            Key::composite(&[&log_id, &ident, &"dummy"]),
                        ));
                        selected.insert(ident);
                    }
                    let collected = collected + acc;
                    mutations.push(update(
                        "manifests",
                        &["log_id", "collected", "writer"],
                        &[
                            &log_id,
                            &collected.hexdigest(),
                            &"replicated manifest manager",
                        ],
                    ));
                    let mut query = tx.query(stmt3).await?;
                    while let Some(row) = query.next().await? {
                        let ident = row.column_by_name::<String>("ident")?;
                        if selected.contains(&ident) {
                            mutations.push(delete("fragments", Key::composite(&[&log_id, &ident])));
                        }
                    }
                    if acc == garbage.setsum_to_discard {
                        tx.buffer_write(mutations);
                        Ok::<_, Error>(())
                    } else {
                        Err(Error::GarbageCollection(
                            "setsum to discard does not match available fragments".to_string(),
                        ))
                    }
                })
            })
            .await?;
        Ok(())
    }

    /// Compute the garbage assuming at least log position will be kept.
    async fn compute_garbage(
        &self,
        _: &GarbageCollectionOptions,
        first_to_keep: LogPosition,
    ) -> Result<Option<Garbage>, Error> {
        if first_to_keep.offset() > i64::MAX as u64 {
            return Err(Error::Overflow(format!(
                "first_to_keep offset {} exceeds i64::MAX",
                first_to_keep.offset()
            )));
        }
        let mut stmt1 = Statement::new(
            "
            SELECT setsum, position_limit
            FROM fragments
                INNER JOIN fragment_regions
                ON fragments.log_id = fragment_regions.log_id
                    AND fragments.ident = fragment_regions.ident
            WHERE fragments.log_id = @log_id
                AND fragments.position_limit <= @threshold
                AND fragment_regions.region = @local_region
            ",
        );
        stmt1.add_param("log_id", &self.log_id.to_string());
        stmt1.add_param("threshold", &(first_to_keep.offset() as i64));
        // TODO(rescrv, mcmr):  dummy region.
        stmt1.add_param("local_region", &"dummy");
        let mut tx = self.spanner.read_only_transaction().await?;
        let mut iter = tx.query(stmt1).await?;
        let mut acc = Setsum::default();
        let mut max_log_position = first_to_keep.offset() as i64;
        while let Some(row) = iter.next().await? {
            let cur = row.column_by_name::<String>("setsum")?;
            let cur = Setsum::from_hexdigest(&cur)
                .ok_or_else(|| Error::CorruptGarbage(format!("invalid setsum hexdigest: {cur}")))?;
            acc += cur;
            let position_limit = row.column_by_name::<i64>("position_limit")?;
            max_log_position = std::cmp::max(max_log_position, position_limit);
        }
        if max_log_position as u64 > first_to_keep.offset() {
            return Err(Error::GarbageCollection(format!(
                "max_log_position {} exceeds first_to_keep offset {}",
                max_log_position,
                first_to_keep.offset()
            )));
        }
        let mut stmt2 = Statement::new(
            "
            SELECT COALESCE(MIN(position_start), @threshold), log_id
            FROM fragments
            WHERE log_id = @log_id
                AND position_limit > @threshold
            GROUP BY log_id
            LIMIT 1
",
        );
        stmt2.add_param("log_id", &self.log_id.to_string());
        stmt2.add_param("threshold", &(first_to_keep.offset() as i64));
        let mut iter = tx.query(stmt2).await?;
        let first_to_keep = if let Some(row) = iter.next().await? {
            let mut min_position_start = row.column::<i64>(0)?;
            if min_position_start < 0 {
                min_position_start = 0;
            }
            LogPosition::from_offset(first_to_keep.offset().min(min_position_start as u64))
        } else {
            if max_log_position < 0 {
                max_log_position = 0;
            }
            LogPosition::from_offset(first_to_keep.offset().min(max_log_position as u64))
        };
        if acc != Setsum::default() {
            Ok(Some(Garbage {
                snapshots_to_drop: vec![],
                snapshots_to_make: vec![],
                snapshot_for_root: None,
                fragments_to_drop_start: FragmentSeqNo::ZERO,
                fragments_to_drop_limit: FragmentSeqNo::ZERO,
                setsum_to_discard: acc,
                fragments_are_uuids: true,
                first_to_keep,
            }))
        } else {
            Ok(None)
        }
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
    async fn test_k8s_mcmr_integration_init_and_load() {
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
            "test_k8s_mcmr_integration_init_and_load: log_id={}, witness={:?}",
            log_id, pos
        );
    }

    // Load a non-existent manifest returns None.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_load_nonexistent() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let result = ManifestManager::load(&client, log_id).await;

        match result {
            Ok(None) => {
                println!("test_k8s_mcmr_integration_load_nonexistent: correctly returned Ok(None)");
            }
            other => panic!("expected Ok(None), got {:?}", other),
        }
    }

    // Test manifest_head returns true when witness matches current state.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_manifest_head_matches() {
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

        println!("test_k8s_mcmr_integration_manifest_head_matches: passed");
    }

    // Test manifest_head returns false when witness does not match.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_manifest_head_mismatch() {
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

        println!("test_k8s_mcmr_integration_manifest_head_mismatch: passed");
    }

    // Test manifest_and_witness via ManifestPublisher trait.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_manifest_and_witness() {
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
            "test_k8s_mcmr_integration_manifest_and_witness: witness={:?}",
            maw.witness
        );
    }

    // Test manifest_and_witness returns UninitializedLog for missing manifest.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_manifest_and_witness_uninitialized() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manager = ManifestManager::new(Arc::new(client), log_id);
        let result = manager.manifest_and_witness().await;

        match result {
            Err(Error::UninitializedLog) => {
                println!(
                    "test_k8s_mcmr_integration_manifest_and_witness_uninitialized: returned UninitializedLog"
                );
            }
            other => panic!("expected UninitializedLog, got {:?}", other),
        }
    }

    // Test assign_timestamp generates unique UUIDs.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_assign_timestamp() {
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

        println!("test_k8s_mcmr_integration_assign_timestamp: passed");
    }

    // Test publish_fragment inserts a fragment and updates enumeration_offset.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_publish_fragment() {
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
            .publish_fragment(&pointer, &[], path, messages_len, num_bytes, setsum)
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
            "test_k8s_mcmr_integration_publish_fragment: log_position={}",
            log_position.offset()
        );
    }

    // Test publishing multiple fragments in sequence.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_publish_multiple_fragments() {
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
            .publish_fragment(&pointer1, &[], "path1", 10, 100, make_setsum(1))
            .await
            .expect("first publish failed");
        assert_eq!(pos1.offset(), 0);

        // Publish second fragment.
        let pointer2 = FragmentUuid::generate();
        let pos2 = manager
            .publish_fragment(&pointer2, &[], "path2", 20, 200, make_setsum(2))
            .await
            .expect("second publish failed");
        assert_eq!(pos2.offset(), 10);

        // Publish third fragment.
        let pointer3 = FragmentUuid::generate();
        let pos3 = manager
            .publish_fragment(&pointer3, &[], "path3", 30, 300, make_setsum(3))
            .await
            .expect("third publish failed");
        assert_eq!(pos3.offset(), 30);

        // Verify final enumeration_offset.
        let witness = ManifestWitness::Position(LogPosition::from_offset(60));
        let head_result = manager.manifest_head(&witness).await;
        assert!(head_result.is_ok() && head_result.unwrap());

        println!(
            "test_k8s_mcmr_integration_publish_multiple_fragments: positions={}, {}, {}",
            pos1.offset(),
            pos2.offset(),
            pos3.offset()
        );
    }

    // Test publish_fragment with messages_len exceeding i64::MAX.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_publish_fragment_too_large() {
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
            .publish_fragment(&pointer, &[], "path", messages_len, 100, Setsum::default())
            .await;

        match result {
            Err(Error::ReplicationBatchTooLarge { .. }) => {
                println!(
                    "test_k8s_mcmr_integration_publish_fragment_too_large: correctly returned ReplicationBatchTooLarge"
                );
            }
            other => panic!("expected ReplicationBatchTooLarge, got {:?}", other),
        }
    }

    // Test that load returns fragments after publish_fragment.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_load_with_fragments() {
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
            .publish_fragment(&pointer1, &[], "path/frag1.parquet", 5, 500, setsum1)
            .await
            .expect("publish failed");

        let pointer2 = FragmentUuid::generate();
        let setsum2 = make_setsum(2);
        manager
            .publish_fragment(&pointer2, &[], "path/frag2.parquet", 10, 1000, setsum2)
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
            "test_k8s_mcmr_integration_load_with_fragments: loaded {} fragments",
            loaded.fragments.len()
        );
    }

    // Test recover succeeds on an initialized log.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_recover() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();
        ManifestManager::init(&client, log_id, &manifest)
            .await
            .expect("init failed");

        let mut manager = ManifestManager::new(Arc::new(client), log_id);
        let result = manager.recover().await;
        assert!(result.is_ok(), "recover should succeed");

        println!("test_k8s_mcmr_integration_recover: passed");
    }

    // Test shutdown is a no-op that completes.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_shutdown() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manager = ManifestManager::new(Arc::new(client), log_id);
        manager.shutdown();

        println!("test_k8s_mcmr_integration_shutdown: passed");
    }

    // Test init with non-default manifest values.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_init_with_nondefault_manifest() {
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

        println!("test_k8s_mcmr_integration_init_with_nondefault_manifest: passed");
    }

    // ==================== Concurrent operations tests ====================

    // Test concurrent publish_fragment calls from multiple tasks.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_publish_fragment_concurrent() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();
        ManifestManager::init(&client, log_id, &manifest)
            .await
            .expect("init failed");

        let client = Arc::new(client);
        let mut handles = vec![];

        for i in 0..5 {
            let client = Arc::clone(&client);
            let handle = tokio::spawn(async move {
                let manager = ManifestManager::new(client, log_id);
                let pointer = FragmentUuid::generate();
                let path = format!("path/fragment_{}.parquet", i);
                manager
                    .publish_fragment(&pointer, &[], &path, 10, 100, make_setsum((i + 1) as u8))
                    .await
            });
            handles.push(handle);
        }

        let mut positions = vec![];
        for handle in handles {
            let result = handle.await.expect("task panicked");
            match result {
                Ok(pos) => positions.push(pos.offset()),
                Err(e) => println!("publish_fragment failed: {:?}", e),
            }
        }

        // All positions should be unique (serialized by Spanner transactions).
        positions.sort();
        let unique_positions: std::collections::HashSet<_> = positions.iter().collect();
        assert_eq!(
            positions.len(),
            unique_positions.len(),
            "all positions should be unique: {:?}",
            positions
        );

        println!(
            "test_k8s_mcmr_integration_publish_fragment_concurrent: positions={:?}",
            positions
        );
    }

    // ==================== Error path tests ====================

    // Test that init fails when called twice with the same log_id.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_init_duplicate() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();

        let result1 = ManifestManager::init(&client, log_id, &manifest).await;
        assert!(result1.is_ok(), "first init should succeed");

        let result2 = ManifestManager::init(&client, log_id, &manifest).await;
        assert!(
            result2.is_err(),
            "second init should fail for duplicate log_id"
        );

        println!(
            "test_k8s_mcmr_integration_init_duplicate: second init error={:?}",
            result2.err()
        );
    }

    // Test publish_fragment with enumeration_offset near i64::MAX causing overflow.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_publish_with_overflow() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        // Create manifest with enumeration_offset near i64::MAX.
        let manifest = Manifest {
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: Some(LogPosition::from_offset((i64::MAX - 10) as u64)),
            initial_seq_no: None,
            writer: "test-writer".to_string(),
        };

        ManifestManager::init(&client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), log_id);
        let pointer = FragmentUuid::generate();

        // Try to publish a fragment that would overflow enumeration_offset.
        let result = manager
            .publish_fragment(&pointer, &[], "path", 100, 100, Setsum::default())
            .await;

        match result {
            Err(Error::LogFull) => {
                println!("test_k8s_mcmr_integration_publish_with_overflow: correctly returned LogFull");
            }
            other => panic!("expected LogFull, got {:?}", other),
        }
    }

    // ==================== Snapshot operation tests ====================

    // Test snapshot_load returns internal error (not implemented).
    #[tokio::test]
    async fn test_k8s_mcmr_integration_snapshot_load_returns_internal() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manager = ManifestManager::new(Arc::new(client), log_id);
        let pointer = crate::SnapshotPointer {
            setsum: Setsum::default(),
            path_to_snapshot: "test/path".to_string(),
            depth: 0,
            start: LogPosition::from_offset(0),
            limit: LogPosition::from_offset(100),
            num_bytes: 1000,
        };

        let result =
            <ManifestManager as ManifestPublisher<FragmentUuid>>::snapshot_load(&manager, &pointer)
                .await;

        match result {
            Err(Error::Internal { .. }) => {
                println!(
                    "test_k8s_mcmr_integration_snapshot_load_returns_internal: correctly returned Internal"
                );
            }
            other => panic!("expected Internal error, got {:?}", other),
        }
    }

    // Test snapshot_install returns internal error (not implemented).
    #[tokio::test]
    async fn test_k8s_mcmr_integration_snapshot_install_returns_internal() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manager = ManifestManager::new(Arc::new(client), log_id);
        let snapshot = crate::Snapshot {
            path: "test/snapshot".to_string(),
            depth: 0,
            setsum: Setsum::default(),
            writer: "test-writer".to_string(),
            snapshots: vec![],
            fragments: vec![],
        };

        let result = manager.snapshot_install(&snapshot).await;

        match result {
            Err(Error::Internal { .. }) => {
                println!(
                    "test_k8s_mcmr_integration_snapshot_install_returns_internal: correctly returned Internal"
                );
            }
            other => panic!("expected Internal error, got {:?}", other),
        }
    }

    // Test snapshot_load via ManifestConsumer trait.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_snapshot_load_consumer_returns_internal() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manager = ManifestManager::new(Arc::new(client), log_id);
        let pointer = crate::SnapshotPointer {
            setsum: Setsum::default(),
            path_to_snapshot: "test/path".to_string(),
            depth: 0,
            start: LogPosition::from_offset(0),
            limit: LogPosition::from_offset(100),
            num_bytes: 1000,
        };

        use crate::interfaces::ManifestConsumer;
        let result =
            <ManifestManager as ManifestConsumer<FragmentUuid>>::snapshot_load(&manager, &pointer)
                .await;

        match result {
            Err(Error::Internal { .. }) => {
                println!("test_k8s_mcmr_integration_snapshot_load_consumer_returns_internal: correctly returned Internal");
            }
            other => panic!("expected Internal error, got {:?}", other),
        }
    }

    // ==================== Garbage collection stub tests ====================

    // Test garbage_applies_cleanly returns Ok(true).
    #[tokio::test]
    async fn test_k8s_mcmr_integration_garbage_applies_cleanly() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manager = ManifestManager::new(Arc::new(client), log_id);
        let garbage = crate::Garbage::empty();

        let result = manager.garbage_applies_cleanly(&garbage).await;
        assert!(
            result.is_ok(),
            "garbage_applies_cleanly failed: {:?}",
            result
        );
        assert!(
            result.unwrap(),
            "garbage_applies_cleanly should return true"
        );

        println!("test_k8s_mcmr_integration_garbage_applies_cleanly: passed");
    }

    // Test apply_garbage returns Ok(()).
    #[tokio::test]
    async fn test_k8s_mcmr_integration_apply_garbage() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        ManifestManager::init(&client, log_id, &Manifest::new_empty("test writer"))
            .await
            .unwrap();
        let manager = ManifestManager::new(Arc::new(client), log_id);
        let garbage = crate::Garbage::empty();

        let result = manager.apply_garbage(garbage).await;
        assert!(result.is_ok(), "apply_garbage failed: {:?}", result);

        println!("test_k8s_mcmr_integration_apply_garbage: passed");
    }

    // Test compute_garbage returns Ok(None).
    #[tokio::test]
    async fn test_k8s_mcmr_integration_compute_garbage() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manager = ManifestManager::new(Arc::new(client), log_id);
        let options = crate::GarbageCollectionOptions::default();
        let first_to_keep = LogPosition::from_offset(0);

        let result = manager.compute_garbage(&options, first_to_keep).await;
        assert!(result.is_ok(), "compute_garbage failed: {:?}", result);
        assert!(
            result.unwrap().is_none(),
            "compute_garbage should return None"
        );

        println!("test_k8s_mcmr_integration_compute_garbage: passed");
    }

    // ==================== Setsum consistency tests ====================

    // Test that publish_fragment correctly accumulates setsum in manifest.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_publish_updates_manifest_setsum() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();
        ManifestManager::init(&client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client.clone()), log_id);

        // Publish first fragment with known setsum.
        let setsum1 = make_setsum(1);
        let pointer1 = FragmentUuid::generate();
        manager
            .publish_fragment(&pointer1, &[], "path1", 5, 100, setsum1)
            .await
            .expect("first publish failed");

        // Load and verify setsum equals setsum1.
        let (loaded1, _) = ManifestManager::load(&client, log_id)
            .await
            .expect("load failed")
            .expect("manifest should exist");
        assert_eq!(
            loaded1.setsum, setsum1,
            "manifest setsum should equal first fragment setsum"
        );

        // Publish second fragment with another setsum.
        let setsum2 = make_setsum(2);
        let pointer2 = FragmentUuid::generate();
        manager
            .publish_fragment(&pointer2, &[], "path2", 5, 100, setsum2)
            .await
            .expect("second publish failed");

        // Load and verify setsum equals setsum1 + setsum2.
        let (loaded2, _) = ManifestManager::load(&client, log_id)
            .await
            .expect("load failed")
            .expect("manifest should exist");
        let expected_setsum = setsum1 + setsum2;
        assert_eq!(
            loaded2.setsum, expected_setsum,
            "manifest setsum should equal sum of fragment setsums"
        );

        println!(
            "test_k8s_mcmr_integration_publish_updates_manifest_setsum: final setsum={}",
            loaded2.setsum.hexdigest()
        );
    }

    // Test manifest_head with ETag witness returns error (only Position is supported).
    #[tokio::test]
    async fn test_k8s_mcmr_integration_manifest_head_etag_witness() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();
        ManifestManager::init(&client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), log_id);
        let witness = ManifestWitness::ETag(crate::interfaces::ETag("test-etag".to_string()));

        let result = manager.manifest_head(&witness).await;
        match result {
            Err(Error::Internal { .. }) => {
                println!(
                    "test_k8s_mcmr_integration_manifest_head_etag_witness: correctly returned Internal"
                );
            }
            other => panic!("expected Internal error, got {:?}", other),
        }
    }
}
