//! Manifests, and their immutable cousin, snapshots, are the metadata that describe a log.
//!
//! A manifest transitively names every file in object storage that is part of the log.
//!
//! Snapshots are content-addressable and immutable, while manifests get overwritten.  For that
//! reason, manifests embed the ETag for conditional writes while snapshots do not.

use std::sync::Arc;
use std::time::Duration;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, ETag, GetOptions, PutOptions, Storage,
    StorageError,
};
use setsum::Setsum;

use crate::{
    Error, Fragment, FragmentSeqNo, LogPosition, LogWriterOptions, ScrubError, ScrubSuccess,
    SnapshotOptions, ThrottleOptions,
};

/////////////////////////////////////////////// paths //////////////////////////////////////////////

pub fn manifest_path(prefix: &str) -> String {
    format!("{prefix}/manifest/MANIFEST")
}

pub fn unprefixed_snapshot_path(setsum: Setsum) -> String {
    format!("snapshot/SNAPSHOT.{}", setsum.hexdigest())
}

pub fn snapshot_setsum(path: &str) -> Result<Setsum, Error> {
    let setsum = path
        .strip_prefix("snapshot/SNAPSHOT.")
        .ok_or_else(|| Error::CorruptManifest(format!("unparseable snapshot path: {}", path,)))?;
    let setsum = Setsum::from_hexdigest(setsum).ok_or_else(|| {
        Error::CorruptManifest(format!("unparseable snapshot setsum in {}", path,))
    })?;
    Ok(setsum)
}

////////////////////////////////////////// SnapshotPointer /////////////////////////////////////////

/// A SnapshotPointer is a pointer to a snapshot.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SnapshotPointer {
    #[serde(
        deserialize_with = "super::deserialize_setsum",
        serialize_with = "super::serialize_setsum"
    )]
    pub setsum: setsum::Setsum,
    pub path_to_snapshot: String,
    pub depth: u8,
    pub start: LogPosition,
    pub limit: LogPosition,
    pub num_bytes: u64,
}

impl SnapshotPointer {
    /// An estimate on the number of bytes required to serialize this object as JSON.
    pub const JSON_SIZE_ESTIMATE: usize = 142;
}

///////////////////////////////////////////// Snapshot /////////////////////////////////////////////

/// A snapshot is, transitively, a set of contiguous fragments.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Snapshot {
    pub path: String,
    pub depth: u8,
    #[serde(
        deserialize_with = "super::deserialize_setsum",
        serialize_with = "super::serialize_setsum"
    )]
    pub setsum: Setsum,
    pub writer: String,
    pub snapshots: Vec<SnapshotPointer>,
    pub fragments: Vec<Fragment>,
}

impl Snapshot {
    /// Sums up the number of bytes referred to by this snapshot.
    pub fn num_bytes(&self) -> u64 {
        self.snapshots.iter().map(|s| s.num_bytes).sum::<u64>()
            + self.fragments.iter().map(|f| f.num_bytes).sum::<u64>()
    }

    /// Scrub the setsums of this snapshot and compare to the fragments.
    pub fn scrub(&self) -> Result<ScrubSuccess, Box<ScrubError>> {
        if !self.fragments.is_empty() && !self.snapshots.is_empty() {
            return Err(ScrubError::CorruptManifest {
                manifest: self.path.to_string(),
                what: format!(
                "snapshot contains both fragments and snapshots in {}: fragments:{} snapshots:{}",
                self.path,
                self.fragments.len(),
                self.snapshots.len(),
            ),
            }
            .into());
        }
        let mut calculated_setsum = Setsum::default();
        let mut bytes_read = 0u64;
        for snapshot in self.snapshots.iter() {
            calculated_setsum += snapshot.setsum;
            bytes_read += snapshot.num_bytes;
        }
        let depth = self.snapshots.iter().map(|s| s.depth).max().unwrap_or(0);
        if depth + 1 != self.depth {
            return Err(ScrubError::CorruptManifest{
                manifest: self.path.to_string(),
                what: format!(
                "expected snapshot depth does not match observed contents in {}: expected:{} != observed:{}",
                self.path,
                self.depth,
                depth + 1,
            )}.into());
        }
        for frag in self.fragments.iter() {
            calculated_setsum += frag.setsum;
        }
        if calculated_setsum != self.setsum {
            return Err(ScrubError::CorruptManifest{
                manifest: self.path.to_string(),
                what: format!(
                "expected snapshot setsum does not match observed contents in {}: expected:{} != observed:{}",
                self.path,
                self.setsum.hexdigest(),
                calculated_setsum.hexdigest()
            )}.into());
        }
        let path_setsum = snapshot_setsum(&self.path).map_err(|_| ScrubError::CorruptManifest {
            manifest: self.path.to_string(),
            what: format!(
                "expected snapshot setsum does not match observed path in {}: expected:{}",
                self.path,
                self.setsum.hexdigest(),
            ),
        })?;
        if path_setsum != self.setsum {
            return Err(ScrubError::CorruptManifest{
                manifest: self.path.to_string(),
                what: format!(
                "expected snapshot setsum does not match observed path in {}: expected:{} != observed:{}",
                self.path,
                self.setsum.hexdigest(),
                path_setsum.hexdigest(),
            )}.into());
        }
        Ok(ScrubSuccess {
            calculated_setsum,
            bytes_read,
        })
    }

    #[tracing::instrument(skip(storage), err(Display))]
    pub async fn load(
        options: &ThrottleOptions,
        storage: &Storage,
        prefix: &str,
        pointer: &SnapshotPointer,
    ) -> Result<Option<Snapshot>, Error> {
        let exp_backoff = crate::backoff::ExponentialBackoff::new(
            options.throughput as f64,
            options.headroom as f64,
        );
        let mut retries = 0;
        let path = format!("{}/{}", prefix, pointer.path_to_snapshot);
        loop {
            match storage
                .get_with_e_tag(&path, GetOptions::new(StorageRequestPriority::P0))
                .await
                .map_err(Arc::new)
            {
                Ok((ref snapshot, _)) => {
                    let snapshot: Snapshot = serde_json::from_slice(snapshot).map_err(|e| {
                        Error::CorruptManifest(format!("could not decode JSON snapshot: {e:?}"))
                    })?;
                    return Ok(Some(snapshot));
                }
                Err(err) => match &*err {
                    StorageError::NotFound { path: _, source: _ } => return Ok(None),
                    err => {
                        let backoff = exp_backoff.next();
                        tokio::time::sleep(backoff).await;
                        if retries >= 3 {
                            return Err(Error::StorageError(Arc::new(err.clone())));
                        }
                        retries += 1;
                    }
                },
            }
        }
    }

    #[tracing::instrument(skip(self, storage), err(Display))]
    pub async fn install(
        &self,
        options: &ThrottleOptions,
        storage: &Storage,
        prefix: &str,
    ) -> Result<SnapshotPointer, Error> {
        let exp_backoff = crate::backoff::ExponentialBackoff::new(
            options.throughput as f64,
            options.headroom as f64,
        );
        loop {
            let path = format!("{}/{}", prefix, self.path);
            let payload = serde_json::to_string(&self)
                .map_err(|e| {
                    Error::CorruptManifest(format!("could not encode JSON manifest: {e:?}"))
                })?
                .into_bytes();
            let options = PutOptions::if_not_exists(StorageRequestPriority::P0);
            match storage.put_bytes(&path, payload, options).await {
                Ok(_) => {
                    return Ok(self.to_pointer());
                }
                Err(StorageError::Precondition { path: _, source: _ }) => {
                    // NOTE(rescrv):  This is something of a lie.  We know that someone put the
                    // file before us, and we know the setsum of the file is embedded in the path.
                    // Because the setsum is only calculable if you have the file and we assume
                    // non-malicious code, anyone who puts the same setsum as us has, in all
                    // likelihood, put something referencing the same content as us.
                    return Ok(self.to_pointer());
                }
                Err(e) => {
                    tracing::error!("error uploading manifest: {e:?}");
                    let mut backoff = exp_backoff.next();
                    if backoff > Duration::from_secs(3_600) {
                        backoff = Duration::from_secs(3_600);
                    }
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    /// Return the lowest addressable offset in the log.
    pub fn maximum_log_position(&self) -> LogPosition {
        let frags = self
            .fragments
            .iter()
            .map(|f| f.limit)
            .max_by_key(|p| p.offset());
        let snaps = self
            .snapshots
            .iter()
            .map(|s| s.limit)
            .max_by_key(|p| p.offset());
        match (frags, snaps) {
            (Some(f), Some(s)) => LogPosition {
                offset: std::cmp::max(f.offset, s.offset),
            },
            (Some(f), None) => f,
            (None, Some(s)) => s,
            (None, None) => LogPosition::from_offset(1),
        }
    }

    /// Return the lowest addressable offset in the log.
    pub fn minimum_log_position(&self) -> LogPosition {
        let frags = self
            .fragments
            .iter()
            .map(|f| f.start)
            .min_by_key(|p| p.offset());
        let snaps = self
            .snapshots
            .iter()
            .map(|s| s.start)
            .min_by_key(|p| p.offset());
        match (frags, snaps) {
            (Some(f), Some(s)) => LogPosition {
                offset: std::cmp::min(f.offset, s.offset),
            },
            (Some(f), None) => f,
            (None, Some(s)) => s,
            (None, None) => LogPosition::from_offset(1),
        }
    }

    pub fn to_pointer(&self) -> SnapshotPointer {
        SnapshotPointer {
            setsum: self.setsum,
            path_to_snapshot: self.path.clone(),
            depth: self.depth,
            start: self.minimum_log_position(),
            limit: self.maximum_log_position(),
            num_bytes: self.num_bytes(),
        }
    }
}

///////////////////////////////////////////// Manifest /////////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Manifest {
    #[serde(
        deserialize_with = "super::deserialize_setsum",
        serialize_with = "super::serialize_setsum"
    )]
    pub setsum: Setsum,
    pub acc_bytes: u64,
    pub writer: String,
    pub snapshots: Vec<SnapshotPointer>,
    pub fragments: Vec<Fragment>,
}

impl Manifest {
    /// Generate a new manifest that's empty and suitable for initialization.
    pub fn new_empty(writer: &str) -> Self {
        Self {
            setsum: Setsum::default(),
            acc_bytes: 0,
            writer: writer.to_string(),
            snapshots: vec![],
            fragments: vec![],
        }
    }

    /// Possibly generate a new snapshot from self if the conditions are right.
    ///
    /// This just creates a snapshot.  Install it to object store and then call apply_snapshot when
    /// it is durable to modify the manifest.
    pub fn generate_snapshot(
        &self,
        snapshot_options: SnapshotOptions,
        writer: &str,
    ) -> Option<Snapshot> {
        let writer = writer.to_string();
        let mut snapshot_depth = self.snapshots.iter().map(|s| s.depth).max().unwrap_or(0);
        while snapshot_depth > 0 {
            // NOTE(rescrv):  We _either_ compact a snapshot of snapshots or a snapshot of log
            // fragments.  We don't do both as interior snapshot nodes only refer to objects of the
            // same type.  Manifests are the only objects to refer to both fragments and snapshots.
            let mut snapshots = vec![];
            let mut setsum = Setsum::default();
            for snapshot in self.snapshots.iter().filter(|s| s.depth == snapshot_depth) {
                if snapshots.len() < snapshot_options.snapshot_rollover_threshold {
                    setsum += snapshot.setsum;
                    snapshots.push(snapshot.clone());
                }
            }
            if snapshots.len() >= snapshot_options.snapshot_rollover_threshold {
                let path = unprefixed_snapshot_path(setsum);
                tracing::info!("generating snapshot {path}");
                return Some(Snapshot {
                    path,
                    depth: snapshot_depth + 1,
                    setsum,
                    writer,
                    snapshots,
                    fragments: vec![],
                });
            }
            snapshot_depth -= 1;
        }
        if self.fragments.len() >= snapshot_options.fragment_rollover_threshold {
            let mut setsum = Setsum::default();
            let mut fragments = vec![];
            for fragment in self.fragments.iter() {
                // NOTE(rescrv):  When taking a snapshot, it's important that we keep around
                // one fragment so that the max seq no is always calculable.
                //
                // Otherwise, a low-traffic log could be compacted into a state where all of
                // its fragments have been compacted and therefore the implicit fragment seq no
                // for each fragment is zero.  This wedges the manifest manager.
                //
                // The fix is to keep around the last fragment.
                if fragments.len() < snapshot_options.fragment_rollover_threshold
                    && self
                        .fragments
                        .iter()
                        .map(|f| f.seq_no)
                        .max()
                        .unwrap_or(FragmentSeqNo(0))
                        != fragment.seq_no
                {
                    setsum += fragment.setsum;
                    fragments.push(fragment.clone());
                }
            }
            if fragments.len() >= snapshot_options.fragment_rollover_threshold {
                let path = unprefixed_snapshot_path(setsum);
                tracing::info!("generating snapshot {path}");
                return Some(Snapshot {
                    path,
                    depth: 1,
                    setsum,
                    writer,
                    snapshots: vec![],
                    fragments,
                });
            }
        }
        None
    }

    /// Given a snapshot, apply it to the manifest.  This modifies the manifest to refer to the
    /// snapshot and removes from the manifest all data that is now part of the snapshot.
    pub fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Error> {
        if snapshot.fragments.len() > self.fragments.len() {
            return Err(Error::CorruptManifest(format!(
                "snapshot has more fragments than manifest: {} > {}",
                snapshot.fragments.len(),
                self.fragments.len()
            )));
        }
        for (idx, (lhs, rhs)) in
            std::iter::zip(self.fragments.iter(), snapshot.fragments.iter()).enumerate()
        {
            if lhs != rhs {
                return Err(Error::CorruptManifest(format!(
                    "fragment {} does not match: {:?} != {:?}",
                    idx, lhs, rhs
                )));
            }
        }
        // Remove all snapshots now part of the current snapshot.
        self.snapshots
            .retain(|s| !snapshot.snapshots.iter().any(|t| t.setsum == s.setsum));
        // Remove all fragments referenced by the snapshot.
        // This assumes the fragments are in offset order and that the snapshot is a contiguous set
        // of fragments.
        // The setsum is intended to catch cases where this doesn't hold.
        self.fragments = self.fragments.split_off(snapshot.fragments.len());
        self.snapshots.push(SnapshotPointer {
            setsum: snapshot.setsum,
            path_to_snapshot: snapshot.path.clone(),
            depth: snapshot.depth,
            start: snapshot.minimum_log_position(),
            limit: snapshot.maximum_log_position(),
            num_bytes: snapshot.num_bytes(),
        });
        Ok(())
    }

    /// Can the fragment be applied to the manifest.  True iff sequentiality.
    ///
    /// Once upon a time there was more parallelism in wal3 and this was a more interesting.  Now
    /// it mostly returns true unless internal invariants are violated.
    pub fn can_apply_fragment(&self, fragment: &Fragment) -> bool {
        let max_seq_no = self
            .fragments
            .iter()
            .map(|f| f.seq_no)
            .max()
            .unwrap_or(FragmentSeqNo(0));
        max_seq_no < max_seq_no + 1
            && max_seq_no + 1 == fragment.seq_no
            && fragment.start.offset() < fragment.limit.offset()
    }

    /// Modify the manifest to apply the fragment to it.
    pub fn apply_fragment(&mut self, fragment: Fragment) {
        self.setsum += fragment.setsum;
        self.acc_bytes = self.acc_bytes.saturating_add(fragment.num_bytes);
        self.fragments.push(fragment);
    }

    /// Estimate the size of the manifest in bytes.
    pub fn estimate_size(&self) -> usize {
        let mut acc = 0;
        for fragment in self.fragments.iter() {
            acc += std::mem::size_of::<Fragment>();
            acc += fragment.path.len();
        }
        acc += std::mem::size_of::<Manifest>();
        acc
    }

    /// True iff some fragment overlaps with the given position, by offset.
    pub fn contains_position(&self, position: LogPosition) -> bool {
        self.fragments
            .iter()
            .any(|f| f.possibly_contains_position(position))
    }

    /// The oldest LogPosition in the manifest.
    pub fn oldest_timestamp(&self) -> Option<LogPosition> {
        self.fragments
            .iter()
            .map(|f| f.start)
            .min_by_key(|p| p.offset())
    }

    /// The LogPosition of the next record to be written.
    pub fn newest_timestamp(&self) -> Option<LogPosition> {
        self.fragments
            .iter()
            .map(|f| f.limit)
            .max_by_key(|p| p.offset())
    }

    /// Given a position, get the fragment to be written.
    pub fn fragment_for_position(&self, position: LogPosition) -> Option<&Fragment> {
        self.fragments
            .iter()
            .find(|f| f.limit.offset() >= position.offset())
    }

    /// Scrub the manifest.
    pub fn scrub(&self) -> Result<ScrubSuccess, Box<ScrubError>> {
        let mut calculated_setsum = Setsum::default();
        let mut bytes_read = 0u64;
        for snapshot in self.snapshots.iter() {
            calculated_setsum += snapshot.setsum;
            bytes_read += snapshot.num_bytes;
        }
        for frag in self.fragments.iter() {
            calculated_setsum += frag.setsum;
            bytes_read += frag.num_bytes;
        }
        if self.setsum != calculated_setsum {
            return Err(ScrubError::CorruptManifest{
                manifest: format!("{:?}", self),
                what: format!(
                "expected manifest setsum does not match observed contents: expected:{} != observed:{}",
                self.setsum.hexdigest(),
                calculated_setsum.hexdigest()
            )}.into());
        }
        // TODO(rescrv):  Check the sequence numbers for sequentiality.
        Ok(ScrubSuccess {
            calculated_setsum,
            bytes_read,
        })
    }

    /// The next sequence number to generate, or None if the log has exhausted them.
    pub fn next_fragment_seq_no(&self) -> Option<FragmentSeqNo> {
        let max_seq_no = self
            .fragments
            .iter()
            .map(|f| f.seq_no)
            .max()
            .unwrap_or(FragmentSeqNo(0));
        if max_seq_no + 1 > max_seq_no {
            Some(max_seq_no + 1)
        } else {
            None
        }
    }

    /// Initialize the log with an empty manifest.
    #[tracing::instrument(skip(storage), err(Display))]
    pub async fn initialize(
        options: &LogWriterOptions,
        storage: &Storage,
        prefix: &str,
        writer: &str,
    ) -> Result<(), Error> {
        let writer = writer.to_string();
        let initial = Manifest {
            writer,
            setsum: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
        };
        Self::initialize_from_manifest(options, storage, prefix, initial).await
    }

    /// Initialize the log with an empty manifest.
    #[tracing::instrument(skip(storage), err(Display))]
    pub async fn initialize_from_manifest(
        _: &LogWriterOptions,
        storage: &Storage,
        prefix: &str,
        initial: Manifest,
    ) -> Result<(), Error> {
        let payload = serde_json::to_string(&initial)
            .map_err(|e| Error::CorruptManifest(format!("could not encode JSON manifest: {e:?}")))?
            .into_bytes();
        storage
            .put_bytes(
                &manifest_path(prefix),
                payload,
                PutOptions::if_not_exists(StorageRequestPriority::P0),
            )
            .await
            .map_err(Arc::new)?;
        Ok(())
    }

    /// Load the latest manifest from object storage.
    #[tracing::instrument(skip(storage), err(Display))]
    pub async fn load(
        options: &ThrottleOptions,
        storage: &Storage,
        prefix: &str,
    ) -> Result<Option<(Manifest, ETag)>, Error> {
        let exp_backoff = crate::backoff::ExponentialBackoff::new(
            options.throughput as f64,
            options.headroom as f64,
        );
        let mut retries = 0;
        let path = manifest_path(prefix);
        loop {
            match storage
                .get_with_e_tag(
                    &path,
                    GetOptions::new(StorageRequestPriority::P0).with_strong_consistency(),
                )
                .await
                .map_err(Arc::new)
            {
                Ok((ref manifest, e_tag)) => {
                    let Some(e_tag) = e_tag else {
                        return Err(Error::CorruptManifest(format!(
                            "no ETag for manifest at {}",
                            path
                        )));
                    };
                    let manifest: Manifest = serde_json::from_slice(manifest).map_err(|e| {
                        Error::CorruptManifest(format!("could not decode JSON manifest: {e:?}"))
                    })?;
                    return Ok(Some((manifest, e_tag)));
                }
                Err(err) => match &*err {
                    StorageError::NotFound { path: _, source: _ } => return Ok(None),
                    err => {
                        let backoff = exp_backoff.next();
                        tokio::time::sleep(backoff).await;
                        if retries >= 3 {
                            return Err(Error::StorageError(Arc::new(err.clone())));
                        }
                        retries += 1;
                    }
                },
            }
        }
    }

    /// Install a manifest to object storage.
    #[tracing::instrument(skip(self, storage, new), err(Display))]
    pub async fn install(
        &self,
        options: &ThrottleOptions,
        storage: &Storage,
        prefix: &str,
        current: Option<&ETag>,
        new: &Manifest,
    ) -> Result<ETag, Error> {
        let exp_backoff = crate::backoff::ExponentialBackoff::new(
            options.throughput as f64,
            options.headroom as f64,
        );
        tracing::info!(
            "installing manifest at {} {:?} {:?}",
            prefix,
            new.maximum_log_position(),
            current,
        );
        loop {
            let payload = serde_json::to_string(&new)
                .map_err(|e| {
                    Error::CorruptManifest(format!("could not encode JSON manifest: {e:?}"))
                })?
                .into_bytes();
            let options = if let Some(e_tag) = current {
                PutOptions::if_matches(e_tag, StorageRequestPriority::P0)
            } else {
                PutOptions::if_not_exists(StorageRequestPriority::P0)
            };
            match storage
                .put_bytes(&manifest_path(prefix), payload, options)
                .await
            {
                Ok(Some(e_tag)) => {
                    return Ok(e_tag);
                }
                Ok(None) => {
                    // NOTE(rescrv):  This is something of a lie.  We know that we put the log, but
                    // without an e_tag we cannot do anything.  The log contention backoff protocol
                    // cares for this case, rather than having to error-handle it separately
                    // because it "crashes" the log and reinitializes.
                    return Err(Error::LogContention);
                }
                Err(StorageError::Precondition { path: _, source: _ }) => {
                    return Err(Error::LogContention);
                }
                Err(e) => {
                    tracing::error!("error uploading manifest: {e:?}");
                    let mut backoff = exp_backoff.next();
                    if backoff > Duration::from_secs(3_600) {
                        backoff = Duration::from_secs(3_600);
                    }
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    /// Return the lowest addressable offset in the log.
    pub fn maximum_log_position(&self) -> LogPosition {
        let frags = self
            .fragments
            .iter()
            .map(|f| f.limit)
            .max_by_key(|p| p.offset());
        let snaps = self
            .snapshots
            .iter()
            .map(|s| s.limit)
            .max_by_key(|p| p.offset());
        match (frags, snaps) {
            (Some(f), Some(s)) => LogPosition {
                offset: std::cmp::max(f.offset, s.offset),
            },
            (Some(f), None) => f,
            (None, Some(s)) => s,
            (None, None) => LogPosition::from_offset(1),
        }
    }

    /// Return the lowest addressable offset in the log.
    pub fn minimum_log_position(&self) -> LogPosition {
        let frags = self
            .fragments
            .iter()
            .map(|f| f.start)
            .min_by_key(|p| p.offset());
        let snaps = self
            .snapshots
            .iter()
            .map(|s| s.start)
            .min_by_key(|p| p.offset());
        match (frags, snaps) {
            (Some(f), Some(s)) => LogPosition {
                offset: std::cmp::min(f.offset, s.offset),
            },
            (Some(f), None) => f,
            (None, Some(s)) => s,
            (None, None) => LogPosition::default(),
        }
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths() {
        assert_eq!("myprefix/manifest/MANIFEST", manifest_path("myprefix"));
        assert_eq!(
            "snapshot/SNAPSHOT.0000000000000000000000000000000000000000000000000000000000000000",
            unprefixed_snapshot_path(Setsum::default())
        );
    }

    #[test]
    fn fragment_contains_position() {
        let fragment = Fragment {
            path: "path".to_string(),
            seq_no: FragmentSeqNo(1),
            start: LogPosition::from_offset(1),
            limit: LogPosition::from_offset(42),
            num_bytes: 4100,
            setsum: Setsum::default(),
        };
        assert!(!fragment.possibly_contains_position(LogPosition::from_offset(0)));
        assert!(fragment.possibly_contains_position(LogPosition::from_offset(1)));
        assert!(fragment.possibly_contains_position(LogPosition::from_offset(41)));
        assert!(!fragment.possibly_contains_position(LogPosition::from_offset(42)));
        assert!(!fragment.possibly_contains_position(LogPosition::from_offset(u64::MAX)));
    }

    #[test]
    fn manifest_contains_position() {
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentSeqNo(1),
            start: LogPosition::from_offset(1),
            limit: LogPosition::from_offset(22),
            num_bytes: 4100,
            setsum: Setsum::default(),
        };
        let fragment2 = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentSeqNo(2),
            start: LogPosition::from_offset(22),
            limit: LogPosition::from_offset(42),
            num_bytes: 4100,
            setsum: Setsum::default(),
        };
        let manifest = Manifest {
            writer: "manifest writer 1".to_string(),
            setsum: Setsum::default(),
            acc_bytes: 8200,
            snapshots: vec![],
            fragments: vec![fragment1, fragment2],
        };
        assert!(!manifest.contains_position(LogPosition::from_offset(0)));
        assert!(manifest.contains_position(LogPosition::from_offset(1)));
        assert!(manifest.contains_position(LogPosition::from_offset(41)));
        assert!(manifest.contains_position(LogPosition::from_offset(41)));
        assert!(!manifest.contains_position(LogPosition::from_offset(42)));
        assert!(!manifest.contains_position(LogPosition::from_offset(u64::MAX)));
    }

    #[test]
    fn manifest_scrub_setsum() {
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentSeqNo(1),
            start: LogPosition::from_offset(1),
            limit: LogPosition::from_offset(22),
            num_bytes: 4100,
            setsum: Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
        };
        let fragment2 = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentSeqNo(2),
            start: LogPosition::from_offset(22),
            limit: LogPosition::from_offset(42),
            num_bytes: 4100,
            setsum: Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let manifest = Manifest {
            writer: "manifest writer 1".to_string(),
            setsum: Setsum::from_hexdigest(
                "307d93deb6b3e91525dc277027bc34958d8f1e74965e4c027820c3596e0f2847",
            )
            .unwrap(),
            acc_bytes: 8200,
            snapshots: vec![],
            fragments: vec![fragment1.clone(), fragment2.clone()],
        };
        assert!(manifest.scrub().is_ok());
        let manifest = Manifest {
            writer: "manifest writer 1".to_string(),
            setsum: Setsum::from_hexdigest(
                "6c5b5ee2c5e741a8d190d215d6cb2802a57ce0d3bb5a1a0223964e97acfa8083",
            )
            .unwrap(),
            acc_bytes: 8200,
            snapshots: vec![],
            fragments: vec![fragment1, fragment2],
        };
        assert!(manifest.scrub().is_err());
    }

    #[test]
    fn apply_fragment() {
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentSeqNo(1),
            start: LogPosition::from_offset(1),
            limit: LogPosition::from_offset(22),
            num_bytes: 41,
            setsum: Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
        };
        let fragment2 = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentSeqNo(2),
            start: LogPosition::from_offset(22),
            limit: LogPosition::from_offset(42),
            num_bytes: 42,
            setsum: Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let mut manifest = Manifest {
            writer: "manifest writer 1".to_string(),
            setsum: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
        };
        assert!(!manifest.can_apply_fragment(&fragment2));
        assert!(manifest.can_apply_fragment(&fragment1));
        manifest.apply_fragment(fragment1);
        assert!(manifest.can_apply_fragment(&fragment2));
        manifest.apply_fragment(fragment2);
        assert_eq!(
            Manifest {
                writer: "manifest writer 1".to_string(),
                setsum: Setsum::from_hexdigest(
                    "307d93deb6b3e91525dc277027bc34958d8f1e74965e4c027820c3596e0f2847",
                )
                .unwrap(),
                acc_bytes: 83,
                snapshots: vec![],
                fragments: vec![
                    Fragment {
                        path: "path1".to_string(),
                        seq_no: FragmentSeqNo(1),
                        start: LogPosition::from_offset(1),
                        limit: LogPosition::from_offset(22),
                        num_bytes: 41,
                        setsum: Setsum::from_hexdigest(
                            "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465"
                        )
                        .unwrap()
                    },
                    Fragment {
                        path: "path2".to_string(),
                        seq_no: FragmentSeqNo(2),
                        start: LogPosition::from_offset(22),
                        limit: LogPosition::from_offset(42),
                        num_bytes: 42,
                        setsum: Setsum::from_hexdigest(
                            "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1"
                        )
                        .unwrap()
                    }
                ],
            },
            manifest
        );
    }

    #[test]
    fn apply_fragment_with_snapshots() {
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentSeqNo(1),
            start: LogPosition::from_offset(1),
            limit: LogPosition::from_offset(22),
            num_bytes: 41,
            setsum: Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
        };
        let fragment2 = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentSeqNo(2),
            start: LogPosition::from_offset(22),
            limit: LogPosition::from_offset(42),
            num_bytes: 42,
            setsum: Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let fragment3 = Fragment {
            path: "path3".to_string(),
            seq_no: FragmentSeqNo(3),
            start: LogPosition::from_offset(42),
            limit: LogPosition::from_offset(84),
            num_bytes: 100,
            setsum: Setsum::from_hexdigest(
                "3b82c2baba815ec0f7ead22dc91939cc31bf338bb599ff0435251380fd0722ad",
            )
            .unwrap(),
        };
        let mut manifest = Manifest {
            writer: "manifest writer 1".to_string(),
            setsum: fragment1.setsum + fragment2.setsum,
            acc_bytes: 83,
            snapshots: vec![SnapshotPointer {
                path_to_snapshot: "snap.1".to_string(),
                setsum: fragment1.setsum,
                start: fragment1.start,
                limit: fragment1.limit,
                depth: 1,
                num_bytes: fragment1.num_bytes,
            }],
            fragments: vec![fragment2.clone()],
        };
        assert!(manifest.can_apply_fragment(&fragment3));
        manifest.apply_fragment(fragment3.clone());
        assert_eq!(
            Manifest {
                writer: "manifest writer 1".to_string(),
                setsum: Setsum::from_hexdigest(
                    "70ff5599703548d61cc7fa9d53d66d61be4e52ff4bf84b07ad45d6d96b174af4"
                )
                .unwrap(),
                acc_bytes: 183,
                snapshots: vec![SnapshotPointer {
                    path_to_snapshot: "snap.1".to_string(),
                    setsum: fragment1.setsum,
                    start: fragment1.start,
                    limit: fragment1.limit,
                    depth: 1,
                    num_bytes: fragment1.num_bytes,
                }],
                fragments: vec![fragment2.clone(), fragment3.clone()],
            },
            manifest
        );
    }
}
