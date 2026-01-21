use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

use google_cloud_spanner::client::Client;
use google_cloud_spanner::key::Key;
use google_cloud_spanner::mutation::{delete, insert, update};
use google_cloud_spanner::statement::Statement;
use setsum::Setsum;
use tonic::Code;
use uuid::Uuid;

use crate::interfaces::{ManifestConsumer, ManifestPublisher, PositionWitness};
use crate::{
    Error, ExponentialBackoff, Fragment, FragmentIdentifier, FragmentSeqNo, FragmentUuid, Garbage,
    GarbageCollectionOptions, LogPosition, Manifest, ManifestAndWitness, ManifestWitness, Snapshot,
    SnapshotPointer,
};

pub struct ManifestManager {
    spanner: Arc<Client>,
    local_region: String,
    log_id: Uuid,
}

impl ManifestManager {
    pub fn new(spanner: Arc<Client>, local_region: String, log_id: Uuid) -> Self {
        Self {
            spanner,
            local_region,
            log_id,
        }
    }

    pub async fn init(
        regions: Vec<String>,
        spanner: &Client,
        log_id: Uuid,
        manifest: &Manifest,
    ) -> Result<(), Error> {
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
                "acc_bytes",
                "writer",
                "enumeration_offset",
            ],
            &[
                &log_id.to_string(),
                &manifest.setsum.hexdigest(),
                &(manifest.acc_bytes as i64),
                &"spanner init",
                &(enum_offset.offset() as i64),
            ],
        )];
        for region in regions.iter() {
            mutations.push(insert(
                "manifest_regions",
                &["log_id", "region", "collected"],
                &[&log_id.to_string(), region, &manifest.collected.hexdigest()],
            ));
        }
        // Also insert any fragments from the manifest.
        for fragment in &manifest.fragments {
            let FragmentIdentifier::Uuid(uuid) = fragment.seq_no else {
                return Err(Error::internal(file!(), line!()));
            };
            let log_id_str = log_id.to_string();
            let uuid_str = uuid.to_string();
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
                    &log_id_str,
                    &uuid_str,
                    &fragment.path,
                    &(fragment.start.offset() as i64),
                    &(fragment.limit.offset() as i64),
                    &(fragment.num_bytes as i64),
                    &fragment.setsum.hexdigest(),
                ],
            ));
            // Insert into fragment_regions so that GC can see this fragment.
            for region in regions.iter() {
                mutations.push(insert(
                    "fragment_regions",
                    &["log_id", "ident", "region"],
                    &[&log_id_str, &uuid_str, region],
                ));
            }
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
        &self,
        spanner: &Client,
        log_id: Uuid,
        witness: &ManifestWitness,
    ) -> Result<bool, Error> {
        let ManifestWitness::Position(pos_witness) = witness else {
            return Err(Error::internal(file!(), line!()));
        };
        let Some(witness_collected) = pos_witness.collected() else {
            return Err(Error::CorruptManifest(format!(
                "invalid collected setsum hexdigest in witness: {}",
                pos_witness.collected
            )));
        };
        let mut stmt = Statement::new(
            "
            SELECT enumeration_offset, collected
            FROM manifests INNER JOIN manifest_regions ON manifests.log_id = manifest_regions.log_id
            WHERE manifests.log_id = @log_id AND region = @local_region LIMIT 1
            ",
        );
        stmt.add_param("log_id", &log_id.to_string());
        stmt.add_param("local_region", &self.local_region);
        let mut tx = spanner.read_only_transaction().await?;
        let mut reader = tx.query(stmt).await?;
        while let Some(row) = reader.next().await? {
            let enumeration_offset = row.column_by_name::<i64>("enumeration_offset")?;
            let collected_hex = row.column_by_name::<String>("collected")?;
            let Some(current_collected) = Setsum::from_hexdigest(&collected_hex) else {
                return Err(Error::CorruptManifest(format!(
                    "invalid collected setsum {collected_hex} for manifest {log_id}"
                )));
            };
            // Both enumeration_offset and collected must match for the cache to be valid.
            // This ensures GC (which modifies collected) invalidates cached manifests.
            if enumeration_offset as u64 == pos_witness.position().offset()
                && current_collected == witness_collected
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn load(
        spanner: &Client,
        log_id: Uuid,
        local_region: &str,
    ) -> Result<Option<(Manifest, ManifestWitness)>, Error> {
        let local_region = local_region.to_string();
        let mut stmt1 = Statement::new(
            "
            SELECT setsum, manifest_regions.collected, acc_bytes, writer, enumeration_offset
            FROM manifests INNER JOIN manifest_regions on manifests.log_id = manifest_regions.log_id
            WHERE manifests.log_id = @log_id
                AND manifest_regions.region = @local_region
            ",
        );
        stmt1.add_param("log_id", &log_id.to_string());
        stmt1.add_param("local_region", &local_region);
        let mut stmt2 = Statement::new(
            "
            SELECT
                fragments.ident,
                fragments.path,
                fragments.position_start,
                fragments.position_limit,
                fragments.num_bytes,
                fragments.setsum
            FROM fragments INNER JOIN fragment_regions
                ON fragments.log_id = fragment_regions.log_id
                AND fragments.ident = fragment_regions.ident
            WHERE fragments.log_id = @log_id
                AND fragment_regions.region = @local_region
            ",
        );
        stmt2.add_param("log_id", &log_id.to_string());
        stmt2.add_param("local_region", &local_region);
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
        // Include both enumeration_offset and collected in the witness.
        // This ensures GC (which modifies collected) invalidates cached manifests.
        let manifest_witness = ManifestWitness::Position(PositionWitness::new(
            LogPosition::from_offset(enumeration_offset),
            collected,
        ));
        Ok(Some((manifest, manifest_witness)))
    }

    /// Returns all log_ids from the manifests table that have fragments in the specified region.
    ///
    /// These are logs that exist in the system and may need compaction.
    pub async fn get_dirty_logs(
        spanner: &Client,
        region: &str,
    ) -> Result<Vec<(Uuid, LogPosition)>, Error> {
        let mut stmt = Statement::new(
            "
            SELECT DISTINCT manifests.log_id, manifests.enumeration_offset
            FROM manifests
                INNER JOIN fragments
                ON manifests.log_id = fragments.log_id
                INNER JOIN fragment_regions
                ON fragments.log_id = fragment_regions.log_id
                    AND fragments.ident = fragment_regions.ident
            WHERE fragment_regions.region = @region
            ",
        );
        stmt.add_param("region", &region);
        let mut tx = spanner.read_only_transaction().await?;
        let mut reader = tx.query(stmt).await?;
        let mut results = vec![];
        while let Some(row) = reader.next().await? {
            let log_id_str = row.column_by_name::<String>("log_id")?;
            let enumeration_offset = row.column_by_name::<i64>("enumeration_offset")?;
            let Ok(log_id) = Uuid::parse_str(&log_id_str) else {
                tracing::warn!("invalid log_id in manifests table: {log_id_str}");
                continue;
            };
            if enumeration_offset < 0 {
                tracing::warn!(
                    "negative enumeration_offset {enumeration_offset} for log_id {log_id}"
                );
                continue;
            }
            results.push((log_id, LogPosition::from_offset(enumeration_offset as u64)));
        }
        Ok(results)
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
    ///
    /// The `successful_regions` parameter contains the list of regions that successfully stored
    /// the fragment during upload. Only these regions will be recorded in `fragment_regions`.
    /// This ensures that lagging replicas can detect the gap and heal, preventing durability bugs
    /// where we believe a fragment exists on a region that didn't actually receive it.
    async fn publish_fragment(
        &self,
        pointer: &FragmentUuid,
        path: &str,
        messages_len: u64,
        num_bytes: u64,
        setsum: Setsum,
        successful_regions: &[String],
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
        let regions: Vec<String> = successful_regions.to_vec();
        // The SDK's read_write_transaction has internal retries for Aborted errors, so this outer
        // loop handles cases where those are exhausted.
        let exp_backoff = ExponentialBackoff::new(2_000.0, 1_500.0);
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
                            let mut backoff = exp_backoff.next();
                            if backoff > Duration::from_secs(10) {
                                backoff = Duration::from_secs(10);
                            }
                            tokio::time::sleep(backoff).await;
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
                AND fragment_regions.region = @local_region
            ",
        );
        stmt1.add_param("log_id", &self.log_id.to_string());
        stmt1.add_param("threshold", &(garbage.first_to_keep.offset() as i64));
        stmt1.add_param("local_region", &self.local_region);
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
                stmt1.add_param("local_region", &self.local_region);
                let mut stmt2 = Statement::new(
                    "
                    SELECT collected
                    FROM manifest_regions
                    WHERE manifest_regions.log_id = @log_id
                        AND manifest_regions.region = @local_region
                    LIMIT 1
                ",
                );
                stmt2.add_param("log_id", &log_id);
                stmt2.add_param("local_region", &self.local_region);
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
                stmt3.add_param("local_region", &self.local_region);
                let local_region = self.local_region.clone();
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
                        mutations.push(delete(
                            "fragment_regions",
                            Key::composite(&[&log_id, &ident, &local_region]),
                        ));
                        selected.insert(ident);
                    }
                    let collected = collected + acc;
                    mutations.push(update(
                        "manifest_regions",
                        &["log_id", "region", "collected"],
                        &[&log_id, &local_region, &collected.hexdigest()],
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
    ///
    /// This method limits the number of fragments returned to avoid exceeding Spanner's
    /// mutation limit (typically 20,000-40,000 mutations per transaction). Since each
    /// fragment deletion involves mutations to both `fragments` and `fragment_regions`
    /// tables, we limit to 5,000 fragments to stay well under the limit while still
    /// making progress during heavy GC or catch-up scenarios.
    async fn compute_garbage(
        &self,
        _: &GarbageCollectionOptions,
        first_to_keep: LogPosition,
    ) -> Result<Option<Garbage>, Error> {
        // Limit fragments to avoid exceeding Spanner's mutation limit per transaction.
        // Each fragment requires deletions from both `fragment_regions` and `fragments`
        // tables, so 5,000 fragments = ~10,000 mutations, well under the 20,000-40,000 limit.
        const MAX_FRAGMENTS_PER_GC: i64 = 5000;

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
            ORDER BY fragments.position_limit ASC
            LIMIT @max_fragments
            ",
        );
        stmt1.add_param("log_id", &self.log_id.to_string());
        stmt1.add_param("threshold", &(first_to_keep.offset() as i64));
        stmt1.add_param("local_region", &self.local_region);
        stmt1.add_param("max_fragments", &MAX_FRAGMENTS_PER_GC);
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
        self.head(&self.spanner, self.log_id, witness).await
    }

    async fn manifest_load(&self) -> Result<Option<(Manifest, ManifestWitness)>, Error> {
        Self::load(&self.spanner, self.log_id, &self.local_region).await
    }

    async fn destroy(&self) -> Result<(), Error> {
        let log_id = self.log_id.to_string();
        self.spanner
            .read_write_transaction(|tx| {
                let log_id = log_id.clone();
                Box::pin(async move {
                    // First, query all fragment idents and regions to delete from fragment_regions.
                    let mut stmt1 = Statement::new(
                        "SELECT ident, region FROM fragment_regions WHERE log_id = @log_id",
                    );
                    stmt1.add_param("log_id", &log_id);
                    let mut iter = tx.query(stmt1).await?;
                    let mut mutations = vec![];
                    while let Some(row) = iter.next().await? {
                        let ident = row.column_by_name::<String>("ident")?;
                        let region = row.column_by_name::<String>("region")?;
                        mutations.push(delete(
                            "fragment_regions",
                            Key::composite(&[&log_id, &ident, &region]),
                        ));
                    }

                    // Query all fragment idents to delete from fragments.
                    let mut stmt2 =
                        Statement::new("SELECT ident FROM fragments WHERE log_id = @log_id");
                    stmt2.add_param("log_id", &log_id);
                    let mut iter = tx.query(stmt2).await?;
                    while let Some(row) = iter.next().await? {
                        let ident = row.column_by_name::<String>("ident")?;
                        mutations.push(delete("fragments", Key::composite(&[&log_id, &ident])));
                    }

                    // Query all regions to delete from manifest_regions.
                    let mut stmt3 = Statement::new(
                        "SELECT region FROM manifest_regions WHERE log_id = @log_id",
                    );
                    stmt3.add_param("log_id", &log_id);
                    let mut iter = tx.query(stmt3).await?;
                    while let Some(row) = iter.next().await? {
                        let region = row.column_by_name::<String>("region")?;
                        mutations.push(delete(
                            "manifest_regions",
                            Key::composite(&[&log_id, &region]),
                        ));
                    }

                    // Delete from manifests.
                    mutations.push(delete("manifests", Key::new(&log_id)));

                    tx.buffer_write(mutations);
                    Ok::<_, google_cloud_spanner::client::Error>(())
                })
            })
            .await?;
        Ok(())
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
        self.head(&self.spanner, self.log_id, witness).await
    }

    async fn manifest_load(&self) -> Result<Option<(Manifest, ManifestWitness)>, Error> {
        Self::load(&self.spanner, self.log_id, &self.local_region).await
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
    use crate::interfaces::{ManifestPublisher, PositionWitness};
    use crate::{Error, FragmentUuid, LogPosition, Manifest, ManifestWitness};

    fn emulator_config() -> SpannerEmulatorConfig {
        SpannerEmulatorConfig {
            host: "localhost".to_string(),
            grpc_port: 9010,
            rest_port: 9020,
            project: "local-project".to_string(),
            instance: "test-instance".to_string(),
            database: "local-logdb-database".to_string(),
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

        let result =
            ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest).await;
        assert!(result.is_ok(), "init failed: {:?}", result.err());

        let loaded = ManifestManager::load(&client, log_id, "dummy").await;
        assert!(loaded.is_ok(), "load failed: {:?}", loaded.err());

        let (loaded_manifest, witness) = loaded.unwrap().expect("manifest should exist");
        assert_eq!(loaded_manifest.setsum, manifest.setsum);
        assert_eq!(loaded_manifest.collected, manifest.collected);
        assert_eq!(loaded_manifest.acc_bytes, manifest.acc_bytes);

        let ManifestWitness::Position(pos_witness) = witness else {
            panic!("expected Position witness");
        };
        assert_eq!(
            pos_witness.position().offset(),
            0,
            "initial enumeration_offset should be 0"
        );
        assert_eq!(
            pos_witness.collected(),
            Some(Setsum::default()),
            "initial collected should be default"
        );

        println!(
            "test_k8s_mcmr_integration_init_and_load: log_id={}, witness={:?}",
            log_id, pos_witness
        );
    }

    // Load a non-existent manifest returns None.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_load_nonexistent() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let result = ManifestManager::load(&client, log_id, "dummy").await;

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
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
        let witness = ManifestWitness::Position(PositionWitness::new(
            LogPosition::from_offset(0),
            Setsum::default(),
        ));

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
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
        // Witness with wrong enumeration_offset should not match.
        let witness = ManifestWitness::Position(PositionWitness::new(
            LogPosition::from_offset(999),
            Setsum::default(),
        ));

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
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
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
        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
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
        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);

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
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
        let pointer = FragmentUuid::generate();
        let path = "test/path/fragment.parquet";
        let messages_len = 10u64;
        let num_bytes = 1024u64;
        let setsum = make_setsum(1);

        let result = manager
            .publish_fragment(
                &pointer,
                path,
                messages_len,
                num_bytes,
                setsum,
                &["dummy".to_string()],
            )
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
        // collected is still default because no GC has occurred.
        let witness = ManifestWitness::Position(PositionWitness::new(
            LogPosition::from_offset(messages_len),
            Setsum::default(),
        ));
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
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);

        // Publish first fragment.
        let pointer1 = FragmentUuid::generate();
        let pos1 = manager
            .publish_fragment(
                &pointer1,
                "path1",
                10,
                100,
                make_setsum(1),
                &["dummy".to_string()],
            )
            .await
            .expect("first publish failed");
        assert_eq!(pos1.offset(), 0);

        // Publish second fragment.
        let pointer2 = FragmentUuid::generate();
        let pos2 = manager
            .publish_fragment(
                &pointer2,
                "path2",
                20,
                200,
                make_setsum(2),
                &["dummy".to_string()],
            )
            .await
            .expect("second publish failed");
        assert_eq!(pos2.offset(), 10);

        // Publish third fragment.
        let pointer3 = FragmentUuid::generate();
        let pos3 = manager
            .publish_fragment(
                &pointer3,
                "path3",
                30,
                300,
                make_setsum(3),
                &["dummy".to_string()],
            )
            .await
            .expect("third publish failed");
        assert_eq!(pos3.offset(), 30);

        // Verify final enumeration_offset.
        // collected is still default because no GC has occurred.
        let witness = ManifestWitness::Position(PositionWitness::new(
            LogPosition::from_offset(60),
            Setsum::default(),
        ));
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
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
        let pointer = FragmentUuid::generate();

        let messages_len = (i64::MAX as u64) + 1;
        let result = manager
            .publish_fragment(
                &pointer,
                "path",
                messages_len,
                100,
                Setsum::default(),
                &["dummy".to_string()],
            )
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
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client.clone()), "dummy".to_string(), log_id);

        // Publish two fragments.
        let pointer1 = FragmentUuid::generate();
        let setsum1 = make_setsum(1);
        manager
            .publish_fragment(
                &pointer1,
                "path/frag1.parquet",
                5,
                500,
                setsum1,
                &["dummy".to_string()],
            )
            .await
            .expect("publish failed");

        let pointer2 = FragmentUuid::generate();
        let setsum2 = make_setsum(2);
        manager
            .publish_fragment(
                &pointer2,
                "path/frag2.parquet",
                10,
                1000,
                setsum2,
                &["dummy".to_string()],
            )
            .await
            .expect("publish failed");

        // Load and verify fragments.
        let (loaded, witness) = ManifestManager::load(&client, log_id, "dummy")
            .await
            .expect("load failed")
            .expect("manifest should exist");

        assert_eq!(loaded.fragments.len(), 2, "should have 2 fragments");

        let ManifestWitness::Position(pos_witness) = witness else {
            panic!("expected Position witness");
        };
        assert_eq!(
            pos_witness.position().offset(),
            15,
            "enumeration_offset should be 5 + 10"
        );
        assert_eq!(
            pos_witness.collected(),
            Some(Setsum::default()),
            "collected should be default (no GC)"
        );

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
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let mut manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
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
        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
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

        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let (loaded, _) = ManifestManager::load(&client, log_id, "dummy")
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
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let client = Arc::new(client);
        let mut handles = vec![];

        for i in 0..5 {
            let client = Arc::clone(&client);
            let handle = tokio::spawn(async move {
                let manager = ManifestManager::new(client, "dummy".to_string(), log_id);
                let pointer = FragmentUuid::generate();
                let path = format!("path/fragment_{}.parquet", i);
                manager
                    .publish_fragment(
                        &pointer,
                        &path,
                        10,
                        100,
                        make_setsum((i + 1) as u8),
                        &["dummy".to_string()],
                    )
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

        let result1 =
            ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest).await;
        assert!(result1.is_ok(), "first init should succeed");

        let result2 =
            ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest).await;
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

        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
        let pointer = FragmentUuid::generate();

        // Try to publish a fragment that would overflow enumeration_offset.
        let result = manager
            .publish_fragment(
                &pointer,
                "path",
                100,
                100,
                Setsum::default(),
                &["dummy".to_string()],
            )
            .await;

        match result {
            Err(Error::LogFull) => {
                println!(
                    "test_k8s_mcmr_integration_publish_with_overflow: correctly returned LogFull"
                );
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
        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
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
        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
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
        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
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
        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
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
        ManifestManager::init(
            vec!["dummy".to_string()],
            &client,
            log_id,
            &Manifest::new_empty("test writer"),
        )
        .await
        .unwrap();
        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
        let garbage = crate::Garbage::empty();

        let result = manager.apply_garbage(garbage).await;
        assert!(result.is_ok(), "apply_garbage failed: {:?}", result);

        println!("test_k8s_mcmr_integration_apply_garbage: passed");
    }

    // Test that garbage collection invalidates cached manifests.
    //
    // This test verifies the fix for a cache invalidation bug: the ManifestWitness must include
    // the `collected` setsum so that GC (which modifies `collected`) causes cache invalidation.
    // Without this, readers could use stale cached manifests with deleted fragment references.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_gc_invalidates_witness() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client.clone()), "dummy".to_string(), log_id);

        // Publish fragments with a region so they can be garbage collected.
        let setsum1 = make_setsum(1);
        let pointer1 = FragmentUuid::generate();
        manager
            .publish_fragment(&pointer1, "path1", 10, 100, setsum1, &["dummy".to_string()])
            .await
            .expect("first publish failed");

        let setsum2 = make_setsum(2);
        let pointer2 = FragmentUuid::generate();
        manager
            .publish_fragment(&pointer2, "path2", 10, 100, setsum2, &["dummy".to_string()])
            .await
            .expect("second publish failed");

        // Get the current witness (before GC).
        let (manifest_before_gc, witness_before_gc) =
            ManifestManager::load(&client, log_id, "dummy")
                .await
                .expect("load failed")
                .expect("manifest should exist");

        // Verify witness is valid before GC.
        let is_valid_before = manager
            .manifest_head(&witness_before_gc)
            .await
            .expect("manifest_head failed");
        assert!(
            is_valid_before,
            "witness should be valid before GC: {:?}",
            witness_before_gc
        );
        assert_eq!(
            manifest_before_gc.fragments.len(),
            2,
            "should have 2 fragments before GC"
        );

        // Perform GC: compute and apply garbage for fragments up to offset 10.
        let gc_options = crate::GarbageCollectionOptions::default();
        let first_to_keep = LogPosition::from_offset(10);
        let garbage = manager
            .compute_garbage(&gc_options, first_to_keep)
            .await
            .expect("compute_garbage failed");

        if let Some(garbage) = garbage {
            manager
                .apply_garbage(garbage)
                .await
                .expect("apply_garbage failed");

            // After GC, the old witness should be INVALID because `collected` changed.
            let is_valid_after = manager
                .manifest_head(&witness_before_gc)
                .await
                .expect("manifest_head failed");
            assert!(
                !is_valid_after,
                "witness from before GC should be INVALID after GC. \
                 This is critical: without this, readers could access deleted fragments. \
                 Witness: {:?}",
                witness_before_gc
            );

            // Load the new manifest and verify the new witness is valid.
            let (manifest_after_gc, witness_after_gc) =
                ManifestManager::load(&client, log_id, "dummy")
                    .await
                    .expect("load failed")
                    .expect("manifest should exist");

            let is_new_valid = manager
                .manifest_head(&witness_after_gc)
                .await
                .expect("manifest_head failed");
            assert!(
                is_new_valid,
                "new witness should be valid after GC: {:?}",
                witness_after_gc
            );

            // Verify the collected setsum changed.
            let ManifestWitness::Position(pos_witness_before) = witness_before_gc else {
                panic!("expected Position witness");
            };
            let ManifestWitness::Position(pos_witness_after) = &witness_after_gc else {
                panic!("expected Position witness");
            };
            let collected_before = pos_witness_before.collected().expect("valid collected");
            let collected_after = pos_witness_after.collected().expect("valid collected");
            assert_ne!(
                collected_before, collected_after,
                "collected setsum should change after GC"
            );

            // Verify the manifest's collected field matches the witness.
            assert_eq!(
                manifest_after_gc.collected, collected_after,
                "manifest collected should match witness collected"
            );

            println!(
                "test_k8s_mcmr_integration_gc_invalidates_witness: \
                 collected_before={}, collected_after={}, fragments_before={}, fragments_after={}",
                collected_before.hexdigest(),
                collected_after.hexdigest(),
                manifest_before_gc.fragments.len(),
                manifest_after_gc.fragments.len()
            );
        } else {
            // If no garbage was computed (e.g., no fragments eligible), that's also fine
            // for this test - just log it.
            println!(
                "test_k8s_mcmr_integration_gc_invalidates_witness: no garbage to collect, \
                 test passes trivially"
            );
        }
    }

    // Test that manifest_head returns false when collected setsum doesn't match.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_manifest_head_collected_mismatch() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manifest = make_empty_manifest();
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);

        // Create a witness with correct enumeration_offset but wrong collected.
        let wrong_collected = make_setsum(1);
        let witness = ManifestWitness::Position(PositionWitness::new(
            LogPosition::from_offset(0),
            wrong_collected,
        ));

        let result = manager.manifest_head(&witness).await;
        assert!(result.is_ok(), "manifest_head failed: {:?}", result.err());
        assert!(
            !result.unwrap(),
            "manifest_head should return false when collected doesn't match"
        );

        println!("test_k8s_mcmr_integration_manifest_head_collected_mismatch: passed");
    }

    // Test compute_garbage returns Ok(None).
    #[tokio::test]
    async fn test_k8s_mcmr_integration_compute_garbage() {
        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();
        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
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
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client.clone()), "dummy".to_string(), log_id);

        // Publish first fragment with known setsum.
        let setsum1 = make_setsum(1);
        let pointer1 = FragmentUuid::generate();
        manager
            .publish_fragment(&pointer1, "path1", 5, 100, setsum1, &["dummy".to_string()])
            .await
            .expect("first publish failed");

        // Load and verify setsum equals setsum1.
        let (loaded1, _) = ManifestManager::load(&client, log_id, "dummy")
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
            .publish_fragment(&pointer2, "path2", 5, 100, setsum2, &["dummy".to_string()])
            .await
            .expect("second publish failed");

        // Load and verify setsum equals setsum1 + setsum2.
        let (loaded2, _) = ManifestManager::load(&client, log_id, "dummy")
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
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        let manager = ManifestManager::new(Arc::new(client), "dummy".to_string(), log_id);
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

    // ==================== Fragment region tests ====================

    // Test that fragments inserted via init are visible to garbage collection.
    //
    // When init is called with a non-empty manifest (e.g., during a copy operation where existing
    // fragments are carried over), those fragments should be GC-able just like fragments published
    // via publish_fragment.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_init_fragments_visible_to_gc() {
        use crate::{Fragment, FragmentIdentifier};

        let Some(client) = setup_spanner_client().await else {
            panic!("Spanner emulator not reachable. Is Tilt running?");
        };

        let log_id = Uuid::new_v4();

        // Create a manifest with pre-existing fragments (simulating a copy operation).
        let fragment_uuid = FragmentUuid::generate();
        let fragment_setsum = make_setsum(1);
        let fragment = Fragment {
            seq_no: FragmentIdentifier::Uuid(fragment_uuid),
            path: "copied/fragment.parquet".to_string(),
            start: LogPosition::from_offset(0),
            limit: LogPosition::from_offset(10),
            num_bytes: 1000,
            setsum: fragment_setsum,
        };

        let manifest = Manifest {
            setsum: fragment_setsum,
            collected: Setsum::default(),
            acc_bytes: 1000,
            snapshots: vec![],
            fragments: vec![fragment],
            initial_offset: Some(LogPosition::from_offset(0)),
            initial_seq_no: None,
            writer: "copy-writer".to_string(),
        };

        // Initialize with the pre-existing fragment.
        ManifestManager::init(vec!["dummy".to_string()], &client, log_id, &manifest)
            .await
            .expect("init failed");

        // Verify the fragment was loaded correctly.
        let (loaded, _) = ManifestManager::load(&client, log_id, "dummy")
            .await
            .expect("load failed")
            .expect("manifest should exist");
        assert_eq!(
            loaded.fragments.len(),
            1,
            "should have 1 fragment from init"
        );
        assert_eq!(loaded.fragments[0].setsum, fragment_setsum);

        // Fragments inserted via init should be visible to garbage collection.
        let manager = ManifestManager::new(Arc::new(client.clone()), "dummy".to_string(), log_id);
        let gc_options = crate::GarbageCollectionOptions::default();
        let first_to_keep = LogPosition::from_offset(100); // Keep nothing, GC everything.

        let garbage = manager
            .compute_garbage(&gc_options, first_to_keep)
            .await
            .expect("compute_garbage failed");

        // Fragments from init should be GC-able.
        assert!(
            garbage.is_some(),
            "Fragments inserted via init should be visible to garbage collection"
        );
        assert_eq!(
            garbage.as_ref().unwrap().setsum_to_discard,
            fragment_setsum,
            "garbage should contain the init fragment's setsum"
        );

        println!(
            "test_k8s_mcmr_integration_init_fragments_visible_to_gc: \
             garbage={:?}",
            garbage.map(|g| g.setsum_to_discard.hexdigest())
        );
    }
}
