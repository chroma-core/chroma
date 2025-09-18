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
    Error, Fragment, FragmentSeqNo, Garbage, LogPosition, LogWriterOptions, ScrubError,
    ScrubSuccess, SnapshotOptions, SnapshotPointerOrFragmentSeqNo, ThrottleOptions,
};

/////////////////////////////////////////////// paths //////////////////////////////////////////////

pub fn manifest_path(prefix: &str) -> String {
    format!("{prefix}/{}", unprefixed_manifest_path())
}

pub fn unprefixed_manifest_path() -> String {
    "manifest/MANIFEST".to_string()
}

pub fn snapshot_prefix() -> String {
    "snapshot/".to_string()
}

pub fn unprefixed_snapshot_path(setsum: Setsum) -> String {
    format!("{}SNAPSHOT.{}", snapshot_prefix(), setsum.hexdigest())
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

impl From<&Snapshot> for SnapshotPointer {
    fn from(snap: &Snapshot) -> Self {
        snap.to_pointer()
    }
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
        if depth >= self.depth {
            return Err(Box::new(ScrubError::CorruptManifest {
                manifest: self.path.to_string(),
                what: format!(
                    "expected snapshot depth is not monotonoic for {}",
                    self.path
                ),
            }));
        }
        for frag in self.fragments.iter() {
            calculated_setsum += frag.setsum;
            bytes_read += frag.num_bytes;
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
            short_read: false,
        })
    }

    #[tracing::instrument(skip(storage))]
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

    #[tracing::instrument(skip(self, storage))]
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
        let mut retry_count = 0;
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
                    let backoff = exp_backoff.next();
                    if backoff > Duration::from_secs(60) || retry_count >= 3 {
                        return Err(Arc::new(e).into());
                    }
                    tokio::time::sleep(backoff).await;
                }
            }
            retry_count += 1;
        }
    }

    /// Return the the next address to insert into the log.
    pub fn limiting_log_position(&self) -> LogPosition {
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
            limit: self.limiting_log_position(),
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
    #[serde(
        default,
        deserialize_with = "super::deserialize_setsum",
        serialize_with = "super::serialize_setsum"
    )]
    pub collected: Setsum,
    pub acc_bytes: u64,
    pub writer: String,
    pub snapshots: Vec<SnapshotPointer>,
    pub fragments: Vec<Fragment>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_offset: Option<LogPosition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_seq_no: Option<FragmentSeqNo>,
}

impl Manifest {
    /// Generate a new manifest that's empty and suitable for initialization.
    pub fn new_empty(writer: &str) -> Self {
        Self {
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 0,
            writer: writer.to_string(),
            snapshots: vec![],
            fragments: vec![],
            initial_offset: None,
            initial_seq_no: None,
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
            let mut snapshots = vec![];
            let mut setsum = Setsum::default();
            for snapshot in self.snapshots.iter().rev() {
                if snapshot.depth < snapshot_depth {
                    continue;
                } else if snapshot.depth == snapshot_depth
                    && snapshots.len() < snapshot_options.snapshot_rollover_threshold
                {
                    snapshots.push(snapshot.clone());
                    setsum += snapshot.setsum;
                } else {
                    break;
                }
            }
            snapshots.reverse();
            if snapshots.len() >= snapshot_options.snapshot_rollover_threshold {
                if let Some(snap) = snapshots.iter().min_by_key(|s| s.start) {
                    if !self.snapshots.is_empty()
                        && self.snapshots[0].limit == snap.start
                        && self.snapshots[0].depth < snapshot_depth
                    {
                        let to_insert = &self.snapshots[0];
                        setsum += to_insert.setsum;
                        snapshots.insert(0, to_insert.clone());
                    }
                }
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
        self.scrub()?;
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
            limit: snapshot.limiting_log_position(),
            num_bytes: snapshot.num_bytes(),
        });
        self.scrub()?;
        Ok(())
    }

    /// Can the fragment be applied to the manifest.  True iff sequentiality.
    ///
    /// Once upon a time there was more parallelism in wal3 and this was a more interesting.  Now
    /// it mostly returns true unless internal invariants are violated.
    pub fn can_apply_fragment(&self, fragment: &Fragment) -> bool {
        Some(fragment.seq_no) == self.next_fragment_seq_no()
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
    pub fn oldest_timestamp(&self) -> LogPosition {
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
            (None, None) => self.initial_offset.unwrap_or(LogPosition::from_offset(1)),
        }
    }

    /// The LogPosition of the next record to be written.
    pub fn next_write_timestamp(&self) -> LogPosition {
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
            (None, None) => self.initial_offset.unwrap_or(LogPosition::from_offset(1)),
        }
    }

    /// Given a position, get the fragment to be written.
    pub fn fragment_for_position(&self, position: LogPosition) -> Option<&Fragment> {
        self.fragments
            .iter()
            .find(|f| f.limit.offset() >= position.offset())
    }

    /// Round down to the nearest boundary so that the given log position will not bisect a
    /// snapshot or fragment in the manifest.  If the given log position does not overlap any
    /// snapshot or fragment, None will be returned.
    pub fn round_to_boundary(&self, position: LogPosition) -> Option<LogPosition> {
        for snapshot in self.snapshots.iter() {
            if LogPosition::contains_offset(snapshot.start, snapshot.limit, position.offset) {
                return Some(snapshot.start);
            }
        }
        for fragment in self.fragments.iter() {
            if LogPosition::contains_offset(fragment.start, fragment.limit, position.offset) {
                return Some(fragment.start);
            }
        }
        if position == self.next_write_timestamp() {
            Some(position)
        } else {
            None
        }
    }

    /// Scrub the manifest without doing I/O.
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
        if self.setsum != calculated_setsum + self.collected {
            return Err(ScrubError::CorruptManifest{
                manifest: format!("{:?}", self),
                what: format!(
                "expected manifest setsum does not match observed contents: expected:{} != observed:{}+{}",
                self.setsum.hexdigest(),
                calculated_setsum.hexdigest(), self.collected.hexdigest(),
            )}.into());
        }
        for (lhs, rhs) in std::iter::zip(self.snapshots.iter(), self.snapshots.iter().skip(1)) {
            if lhs.limit != rhs.start {
                return Err(ScrubError::CorruptManifest {
                    manifest: format!("{:?}", self),
                    what: format!(
                        "expected snapshots to be sequential within the manifest: gap {:?} -> {:?}",
                        lhs.limit, rhs.start,
                    ),
                }
                .into());
            }
        }
        for (lhs, rhs) in std::iter::zip(self.fragments.iter(), self.fragments.iter().skip(1)) {
            if lhs.limit != rhs.start {
                return Err(ScrubError::CorruptManifest {
                    manifest: format!("{:?}", self),
                    what: format!(
                        "expected fragments to be sequential within the manifest: gap {:?} -> {:?}",
                        lhs.limit, rhs.start,
                    ),
                }
                .into());
            }
        }
        if let (Some(snap), Some(frag)) = (self.snapshots.last(), self.fragments.first()) {
            if snap.limit != frag.start {
                return Err(ScrubError::CorruptManifest {
                    manifest: format!("{:?}", self),
                    what: format!(
                        "expected snapshots-fragments to be sequential within the manifest: gap {:?} -> {:?}",
                        snap.limit, frag.start,
                    ),
                }
                .into());
            }
        }
        if let Some(initial_offset) = self.initial_offset {
            let oldest_timestamp = self.oldest_timestamp();
            if initial_offset != oldest_timestamp {
                return Err(ScrubError::CorruptManifest {
                    manifest: format!("{:?}", self),
                    what: format!(
                        "expected initial offset to be equal to oldest timestamp when present: gap {:?} != {:?}",
                        initial_offset, oldest_timestamp,
                    ),
                }
                .into());
            }
        }
        // TODO(rescrv):  Check the sequence numbers for sequentiality.
        Ok(ScrubSuccess {
            calculated_setsum,
            bytes_read,
            short_read: false,
        })
    }

    /// The next sequence number to generate, or None if the log has exhausted them.
    pub fn next_fragment_seq_no(&self) -> Option<FragmentSeqNo> {
        if let Some(max_seq_no) = self.fragments.iter().map(|f| f.seq_no).max() {
            if max_seq_no + 1 > max_seq_no {
                Some(max_seq_no + 1)
            } else {
                None
            }
        } else {
            Some(self.initial_seq_no.unwrap_or(FragmentSeqNo::BEGIN))
        }
    }

    /// Initialize the log with an empty manifest.
    #[tracing::instrument(skip(storage))]
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
            collected: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: None,
            initial_seq_no: None,
        };
        Self::initialize_from_manifest(options, storage, prefix, initial).await
    }

    /// Initialize the log with an empty manifest.
    #[tracing::instrument(skip(storage))]
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

    /// Validate the e_tag against the manifest on object storage.
    pub async fn head(
        _: &ThrottleOptions,
        storage: &Storage,
        prefix: &str,
        e_tag: &ETag,
    ) -> Result<bool, Error> {
        let path = manifest_path(prefix);
        Ok(storage.confirm_same(&path, e_tag).await.map_err(Arc::new)?)
    }

    /// Load the latest manifest from object storage.
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
    #[tracing::instrument(skip(self, storage, new))]
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
            new.next_write_timestamp(),
            current,
        );
        let mut retry_count = 0;
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
                    return Err(Error::LogContentionFailure);
                }
                Err(StorageError::Precondition { path: _, source: _ }) => {
                    // NOTE(rescrv):  This is "durable" because it's a manifest failure.  See the
                    // comment in the Error enum for why this makes sense.
                    return Err(Error::LogContentionDurable);
                }
                Err(e) => {
                    tracing::error!("error uploading manifest: {e:?}");
                    let backoff = exp_backoff.next();
                    if backoff > Duration::from_secs(60) || retry_count >= 3 {
                        // NOTE(rescrv):  This is "durable" because it's a manifest failure.  See the
                        // comment in the Error enum for why this makes sense.  By returning
                        // "durable" rather than the underlying error we force an end-to-end
                        // recovery.
                        return Err(Error::LogContentionDurable);
                    }
                    tokio::time::sleep(backoff).await;
                }
            }
            retry_count += 1;
        }
    }

    /// Apply the destructive operation specified by the Garbage struct.
    #[allow(clippy::result_large_err)]
    pub fn apply_garbage(&self, garbage: Garbage) -> Result<Option<Self>, Error> {
        if garbage.is_empty() {
            return Err(Error::GarbageCollectionPrecondition(
                SnapshotPointerOrFragmentSeqNo::Stringy("cannot apply empty garbage".to_string()),
            ));
        }
        if garbage.fragments_to_drop_limit <= self.initial_seq_no.unwrap_or(FragmentSeqNo::BEGIN)
            && !garbage
                .snapshots_to_drop
                .iter()
                .any(|snap| self.snapshots.contains(snap))
            && garbage.snapshots_to_make.is_empty()
        {
            return Ok(None);
        }
        let mut setsum_to_discard = Setsum::default();
        if garbage.fragments_to_drop_start > garbage.fragments_to_drop_limit {
            return Err(Error::GarbageCollection(format!(
                "Garbage has start > limit: {:?} > {:?}",
                garbage.fragments_to_drop_start, garbage.fragments_to_drop_limit
            )));
        }
        if garbage.fragments_to_drop_limit == FragmentSeqNo(0) {
            return Ok(None);
        }
        let mut new = self.clone();
        for to_drop in garbage.snapshots_to_drop.iter() {
            if let Some(index) = new.snapshots.iter().position(|s| s == to_drop) {
                if Some(to_drop) != garbage.snapshot_for_root.as_ref() {
                    setsum_to_discard += to_drop.setsum;
                }
                new.snapshots.remove(index);
            }
        }
        // TODO(rescrv):  When Step stabilizes, revisit this ugliness.
        for seq_no in garbage.fragments_to_drop_start.0..garbage.fragments_to_drop_limit.0 {
            if let Some(index) = new
                .fragments
                .iter()
                .position(|f| f.seq_no == FragmentSeqNo(seq_no))
            {
                setsum_to_discard += new.fragments[index].setsum;
                new.fragments.remove(index);
            }
        }
        let mut root_setsum = Setsum::default();
        if let Some(snap) = garbage.snapshot_for_root.as_ref() {
            if !new.snapshots.contains(snap) {
                root_setsum = snap.setsum;
                new.snapshots.insert(0, snap.clone());
            }
        }
        if setsum_to_discard - root_setsum != garbage.setsum_to_discard {
            return Err(Error::GarbageCollectionPrecondition(
                SnapshotPointerOrFragmentSeqNo::Stringy(format!(
                    "Setsum mismatch: {} != {}",
                    setsum_to_discard.hexdigest(),
                    garbage.setsum_to_discard.hexdigest()
                )),
            ));
        }
        new.collected += garbage.setsum_to_discard;
        new.initial_offset = Some(garbage.first_to_keep);
        new.initial_seq_no = Some(garbage.fragments_to_drop_limit);
        new.scrub()?;

        // Sanity check that new manifest contains valid range of logs
        // From the scrub above we know the manifest is continuous, so we only need to check endpoints
        if new.oldest_timestamp() > garbage.first_to_keep {
            tracing::error!("Manifest after garbage collection does not contain the first log to keep: needs logs since position {:?} to be present, but the smallest log postion available is {:?}", garbage.first_to_keep, new.oldest_timestamp());
            return Err(Error::CorruptManifest(
                "Manifest corruption detected after GC: missing first log to keep".to_string(),
            ));
        }
        if new.next_write_timestamp() != self.next_write_timestamp() {
            tracing::error!("Manifest after garbage collection has a different max log position: expected next log position to be {:?}, but got {:?}", self.next_write_timestamp(), new.next_write_timestamp());
            return Err(Error::CorruptManifest(
                "Manifest corruption detected after GC: next log position mismatch".to_string(),
            ));
        }
        Ok(Some(new))
    }
}

////////////////////////////////////////// ManifestAndETag /////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ManifestAndETag {
    pub manifest: Manifest,
    pub e_tag: ETag,
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
        assert_eq!("snapshot/", snapshot_prefix(),);
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
            collected: Setsum::default(),
            acc_bytes: 8200,
            snapshots: vec![],
            fragments: vec![fragment1, fragment2],
            initial_offset: None,
            initial_seq_no: None,
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
            collected: Setsum::default(),
            acc_bytes: 8200,
            snapshots: vec![],
            fragments: vec![fragment1.clone(), fragment2.clone()],
            initial_offset: None,
            initial_seq_no: None,
        };
        assert!(manifest.scrub().is_ok());
        let manifest = Manifest {
            writer: "manifest writer 1".to_string(),
            setsum: Setsum::from_hexdigest(
                "6c5b5ee2c5e741a8d190d215d6cb2802a57ce0d3bb5a1a0223964e97acfa8083",
            )
            .unwrap(),
            collected: Setsum::default(),
            acc_bytes: 8200,
            snapshots: vec![],
            fragments: vec![fragment1, fragment2],
            initial_offset: None,
            initial_seq_no: None,
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
            collected: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: None,
            initial_seq_no: None,
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
                collected: Setsum::default(),
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
                initial_offset: None,
                initial_seq_no: None,
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
            collected: Setsum::default(),
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
            initial_offset: None,
            initial_seq_no: None,
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
                collected: Setsum::default(),
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
                initial_offset: None,
                initial_seq_no: None,
            },
            manifest
        );
    }

    #[test]
    fn manifest_limiting_log_position_with_initial_offset() {
        let manifest = Manifest {
            writer: "bootstrap".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: Some(LogPosition::from_offset(100)),
            initial_seq_no: Some(FragmentSeqNo(10)),
        };
        assert_eq!(
            manifest.next_write_timestamp(),
            LogPosition::from_offset(100)
        );
        assert_eq!(manifest.oldest_timestamp(), LogPosition::from_offset(100));
        assert_eq!(manifest.next_fragment_seq_no(), Some(FragmentSeqNo(10)));
        assert_eq!(manifest.next_fragment_seq_no(), Some(FragmentSeqNo(10)));

        let fragment = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentSeqNo(10),
            start: LogPosition::from_offset(100),
            limit: LogPosition::from_offset(200),
            num_bytes: 100,
            setsum: Setsum::default(),
        };
        let manifest_with_fragment = Manifest {
            writer: "bootstrap".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 100,
            snapshots: vec![],
            fragments: vec![fragment],
            initial_offset: Some(LogPosition::from_offset(100)),
            initial_seq_no: Some(FragmentSeqNo(10)),
        };
        assert_eq!(
            manifest_with_fragment.next_write_timestamp(),
            LogPosition::from_offset(200)
        );
        assert_eq!(
            manifest_with_fragment.oldest_timestamp(),
            LogPosition::from_offset(100)
        );
        assert_eq!(
            manifest_with_fragment.next_fragment_seq_no(),
            Some(FragmentSeqNo(11)),
        );
    }

    #[test]
    fn manifest_minimum_log_position_with_initial_offset() {
        let manifest = Manifest {
            writer: "bootstrap".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: Some(LogPosition::from_offset(100)),
            initial_seq_no: Some(FragmentSeqNo(10)),
        };
        assert_eq!(manifest.oldest_timestamp(), LogPosition::from_offset(100));
        assert_eq!(
            manifest.next_write_timestamp(),
            LogPosition::from_offset(100)
        );
        assert_eq!(manifest.next_fragment_seq_no(), Some(FragmentSeqNo(10)));

        let fragment = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentSeqNo(10),
            start: LogPosition::from_offset(100),
            limit: LogPosition::from_offset(200),
            num_bytes: 100,
            setsum: Setsum::default(),
        };
        let manifest_with_fragment = Manifest {
            writer: "bootstrap".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 100,
            snapshots: vec![],
            fragments: vec![fragment],
            initial_offset: Some(LogPosition::from_offset(100)),
            initial_seq_no: Some(FragmentSeqNo(10)),
        };
        assert_eq!(
            manifest_with_fragment.oldest_timestamp(),
            LogPosition::from_offset(100)
        );
        assert_eq!(
            manifest_with_fragment.next_fragment_seq_no(),
            Some(FragmentSeqNo(11))
        );
    }

    #[test]
    fn manifest_position_bounds_with_initial_offset_and_snapshots() {
        let snapshot = SnapshotPointer {
            path_to_snapshot: "snap.1".to_string(),
            setsum: Setsum::default(),
            start: LogPosition::from_offset(50),
            limit: LogPosition::from_offset(75),
            depth: 1,
            num_bytes: 25,
        };
        let fragment = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentSeqNo(1),
            start: LogPosition::from_offset(100),
            limit: LogPosition::from_offset(200),
            num_bytes: 100,
            setsum: Setsum::default(),
        };

        let manifest = Manifest {
            writer: "bootstrap".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 125,
            snapshots: vec![snapshot],
            fragments: vec![fragment],
            initial_offset: Some(LogPosition::from_offset(25)),
            initial_seq_no: Some(FragmentSeqNo(1)),
        };

        assert_eq!(manifest.oldest_timestamp(), LogPosition::from_offset(50));
        assert_eq!(
            manifest.next_write_timestamp(),
            LogPosition::from_offset(200)
        );
    }

    #[test]
    fn manifest_serialization_with_initial_offset() {
        let manifest = Manifest {
            writer: "bootstrap".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: Some(LogPosition::from_offset(1000)),
            initial_seq_no: Some(FragmentSeqNo(100)),
        };

        let serialized = serde_json::to_string(&manifest).unwrap();
        let deserialized: Manifest = serde_json::from_str(&serialized).unwrap();
        assert_eq!(manifest, deserialized);
        assert_eq!(
            deserialized.initial_offset,
            Some(LogPosition::from_offset(1000))
        );
        assert_eq!(deserialized.initial_seq_no, Some(FragmentSeqNo(100)));

        let manifest_none = Manifest {
            writer: "bootstrap".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: None,
            initial_seq_no: None,
        };

        let serialized_none = serde_json::to_string(&manifest_none).unwrap();
        let deserialized_none: Manifest = serde_json::from_str(&serialized_none).unwrap();
        assert_eq!(manifest_none, deserialized_none);
        assert_eq!(deserialized_none.initial_offset, None);
    }

    #[test]
    fn manifest_fragment_operations_with_initial_offset() {
        let mut manifest = Manifest {
            writer: "bootstrap".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: Some(LogPosition::from_offset(500)),
            initial_seq_no: Some(FragmentSeqNo(50)),
        };

        let fragment = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentSeqNo(50),
            start: LogPosition::from_offset(500),
            limit: LogPosition::from_offset(600),
            num_bytes: 100,
            setsum: Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
        };

        assert!(manifest.can_apply_fragment(&fragment));
        manifest.apply_fragment(fragment.clone());

        let expected_manifest = Manifest {
            writer: "bootstrap".to_string(),
            setsum: fragment.setsum,
            collected: Setsum::default(),
            acc_bytes: 100,
            snapshots: vec![],
            fragments: vec![fragment],
            initial_offset: Some(LogPosition::from_offset(500)),
            initial_seq_no: Some(FragmentSeqNo(50)),
        };

        assert_eq!(manifest, expected_manifest);
        assert_eq!(manifest.next_fragment_seq_no(), Some(FragmentSeqNo(51)));
    }

    #[test]
    fn manifest_fragment_for_position_with_initial_offset() {
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentSeqNo(1),
            start: LogPosition::from_offset(100),
            limit: LogPosition::from_offset(150),
            num_bytes: 50,
            setsum: Setsum::default(),
        };
        let fragment2 = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentSeqNo(2),
            start: LogPosition::from_offset(150),
            limit: LogPosition::from_offset(200),
            num_bytes: 50,
            setsum: Setsum::default(),
        };

        let manifest = Manifest {
            writer: "bootstrap".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 100,
            snapshots: vec![],
            fragments: vec![fragment1.clone(), fragment2.clone()],
            initial_offset: Some(LogPosition::from_offset(100)),
            initial_seq_no: Some(FragmentSeqNo(1)),
        };

        assert_eq!(
            manifest.fragment_for_position(LogPosition::from_offset(120)),
            Some(&fragment1)
        );
        assert_eq!(
            manifest.fragment_for_position(LogPosition::from_offset(175)),
            Some(&fragment2)
        );
        assert_eq!(
            manifest.fragment_for_position(LogPosition::from_offset(50)),
            Some(&fragment1)
        );
        assert_eq!(
            manifest.fragment_for_position(LogPosition::from_offset(250)),
            None
        );
    }

    #[test]
    fn manifest_contains_position_with_initial_offset() {
        let fragment = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentSeqNo(1),
            start: LogPosition::from_offset(100),
            limit: LogPosition::from_offset(200),
            num_bytes: 100,
            setsum: Setsum::default(),
        };

        let manifest = Manifest {
            writer: "bootstrap".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 100,
            snapshots: vec![],
            fragments: vec![fragment],
            initial_offset: Some(LogPosition::from_offset(100)),
            initial_seq_no: Some(FragmentSeqNo(1)),
        };

        assert!(!manifest.contains_position(LogPosition::from_offset(50)));
        assert!(manifest.contains_position(LogPosition::from_offset(150)));
        assert!(!manifest.contains_position(LogPosition::from_offset(250)));
    }

    #[test]
    fn manifest_timestamps_with_initial_offset() {
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentSeqNo(1),
            start: LogPosition::from_offset(100),
            limit: LogPosition::from_offset(150),
            num_bytes: 50,
            setsum: Setsum::default(),
        };
        let fragment2 = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentSeqNo(2),
            start: LogPosition::from_offset(150),
            limit: LogPosition::from_offset(200),
            num_bytes: 50,
            setsum: Setsum::default(),
        };

        let manifest = Manifest {
            writer: "bootstrap".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 100,
            snapshots: vec![],
            fragments: vec![fragment1, fragment2],
            initial_offset: Some(LogPosition::from_offset(100)),
            initial_seq_no: Some(FragmentSeqNo(42)),
        };

        assert_eq!(manifest.oldest_timestamp(), LogPosition::from_offset(100));
        assert_eq!(
            manifest.next_write_timestamp(),
            LogPosition::from_offset(200)
        );

        let empty_manifest = Manifest {
            writer: "bootstrap".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: Some(LogPosition::from_offset(500)),
            initial_seq_no: Some(FragmentSeqNo(50)),
        };

        assert_eq!(
            empty_manifest.oldest_timestamp(),
            LogPosition::from_offset(500)
        );
        assert_eq!(
            empty_manifest.next_write_timestamp(),
            LogPosition::from_offset(500)
        );
    }

    #[test]
    fn apply_garbage_equal_nonzero_fragment_seq_nos() {
        let manifest = Manifest {
            writer: "test_writer".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: Some(LogPosition::from_offset(100)),
            initial_seq_no: None,
        };

        let garbage = Garbage {
            snapshots_to_drop: vec![],
            snapshots_to_make: vec![],
            fragments_to_drop_start: FragmentSeqNo(5),
            fragments_to_drop_limit: FragmentSeqNo(5),
            setsum_to_discard: Setsum::default(),
            first_to_keep: LogPosition::from_offset(100),
            snapshot_for_root: None,
        };

        let result = manifest.apply_garbage(garbage).unwrap().unwrap();

        // When fragments_to_drop_start == fragments_to_drop_limit and both are non-zero,
        // initial_seq_no should be set to the limit value
        assert_eq!(result.initial_seq_no, Some(FragmentSeqNo(5)));
        assert_eq!(result.initial_offset, Some(LogPosition::from_offset(100)));
    }

    #[test]
    fn apply_garbage_validates_fragment_drop_range() {
        use crate::gc::Garbage;

        let manifest = Manifest::new_empty("test");

        // Test case: fragments_to_drop_start > fragments_to_drop_limit should fail
        let invalid_garbage = Garbage {
            snapshots_to_drop: vec![],
            snapshots_to_make: vec![],
            snapshot_for_root: None,
            fragments_to_drop_start: FragmentSeqNo(10),
            fragments_to_drop_limit: FragmentSeqNo(5),
            setsum_to_discard: Setsum::default(),
            first_to_keep: LogPosition::from_offset(1),
        };

        let result = manifest.apply_garbage(invalid_garbage);
        assert!(result.is_err());

        if let Err(crate::Error::GarbageCollection(msg)) = result {
            assert!(msg.contains("Garbage has start > limit"));
            assert!(msg.contains("FragmentSeqNo(10) > FragmentSeqNo(5)"));
        } else {
            panic!("Expected GarbageCollection error, got {:?}", result);
        }

        // Test case: fragments_to_drop_start == fragments_to_drop_limit should succeed
        let valid_garbage_equal = Garbage {
            snapshots_to_drop: vec![],
            snapshots_to_make: vec![],
            snapshot_for_root: None,
            fragments_to_drop_start: FragmentSeqNo(5),
            fragments_to_drop_limit: FragmentSeqNo(5),
            setsum_to_discard: Setsum::default(),
            first_to_keep: LogPosition::from_offset(1),
        };

        let result = manifest.apply_garbage(valid_garbage_equal);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Test case: fragments_to_drop_start < fragments_to_drop_limit should succeed
        let valid_garbage_less = Garbage {
            snapshots_to_drop: vec![],
            snapshots_to_make: vec![],
            snapshot_for_root: None,
            fragments_to_drop_start: FragmentSeqNo(1),
            fragments_to_drop_limit: FragmentSeqNo(5),
            setsum_to_discard: Setsum::default(),
            first_to_keep: LogPosition::from_offset(1),
        };

        let result = manifest.apply_garbage(valid_garbage_less);
        assert!(result.is_ok());
    }

    #[test]
    fn apply_garbage_early_return_when_no_work_to_do() {
        // Test the new early return condition: when fragments_to_drop_limit <= initial_seq_no
        // and there are no snapshots to drop/make, return Ok(None)
        let manifest = Manifest {
            writer: "test_writer".to_string(),
            setsum: Setsum::from_hexdigest(
                "9eabcf03849e73854ebd6c80795d0fe4fbbbe4320151a011db9daf7419624dca",
            )
            .unwrap(),
            collected: Setsum::from_hexdigest(
                "8ab5679e202200027046c6b42d7ca4605004c8de3e3988ce3240212c9fa269cd",
            )
            .unwrap(),
            acc_bytes: 6606733560,
            snapshots: vec![],
            fragments: vec![Fragment {
                path: "log/Bucket=00000000002f2000/FragmentSeqNo=00000000002f2372.parquet"
                    .to_string(),
                seq_no: FragmentSeqNo(3089266),
                start: LogPosition { offset: 5566918 },
                limit: LogPosition { offset: 5566919 },
                num_bytes: 2116,
                setsum: Setsum::from_hexdigest(
                    "0ff66765647c73839d76a6cb4ce16a8340b71c543c171843a95d8e48c1bee3fc",
                )
                .unwrap(),
            }],
            initial_offset: Some(LogPosition { offset: 5566918 }),
            initial_seq_no: Some(FragmentSeqNo(3089266)),
        };

        // Case 1: fragments_to_drop_limit <= initial_seq_no, no snapshots to drop/make
        let garbage = Garbage {
            snapshots_to_drop: vec![],
            snapshots_to_make: vec![],
            snapshot_for_root: None,
            fragments_to_drop_start: FragmentSeqNo(3089257),
            fragments_to_drop_limit: FragmentSeqNo(3089266), // Equal to initial_seq_no
            setsum_to_discard: Setsum::from_hexdigest(
                "7287d2d717e35117811f1afb7c5e8dd6517417dcbc5ad195dabbafaca6df9ef3",
            )
            .unwrap(),
            first_to_keep: LogPosition { offset: 5566918 },
        };

        let result = manifest.apply_garbage(garbage.clone());
        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "Expected None when no work to do"
        );

        // Case 2: fragments_to_drop_limit < initial_seq_no, no snapshots to drop/make
        let garbage_below_initial = Garbage {
            fragments_to_drop_limit: FragmentSeqNo(3089265), // Less than initial_seq_no
            ..garbage.clone()
        };

        let result = manifest.apply_garbage(garbage_below_initial);
        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "Expected None when fragments_to_drop_limit < initial_seq_no"
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_head_returns_true_for_matching_etag() {
        use chroma_storage::s3::s3_client_for_test_with_new_bucket;

        let storage = s3_client_for_test_with_new_bucket().await;
        let prefix = "test-head-matching";
        let throttle_options = crate::ThrottleOptions::default();

        let manifest = Manifest::new_empty("test-writer");

        Manifest::initialize_from_manifest(
            &crate::LogWriterOptions::default(),
            &storage,
            prefix,
            manifest,
        )
        .await
        .unwrap();

        let (_loaded_manifest, etag) = Manifest::load(&throttle_options, &storage, prefix)
            .await
            .unwrap()
            .unwrap();

        let result = Manifest::head(&throttle_options, &storage, prefix, &etag)
            .await
            .unwrap();
        assert!(result, "head should return true for matching etag");
    }

    #[tokio::test]
    async fn test_k8s_integration_head_returns_false_for_non_matching_etag() {
        use chroma_storage::s3::s3_client_for_test_with_new_bucket;

        let storage = s3_client_for_test_with_new_bucket().await;
        let prefix = "test-head-non-matching";
        let throttle_options = crate::ThrottleOptions::default();

        let manifest = Manifest::new_empty("test-writer");

        Manifest::initialize_from_manifest(
            &crate::LogWriterOptions::default(),
            &storage,
            prefix,
            manifest,
        )
        .await
        .unwrap();

        let fake_etag = chroma_storage::ETag("fake-etag-wont-match".to_string());

        let result = Manifest::head(&throttle_options, &storage, prefix, &fake_etag)
            .await
            .unwrap();
        assert!(!result, "head should return false for non-matching etag");
    }

    #[tokio::test]
    async fn test_k8s_integration_head_returns_error_for_nonexistent_manifest() {
        use chroma_storage::s3::s3_client_for_test_with_new_bucket;

        let storage = s3_client_for_test_with_new_bucket().await;
        let prefix = "test-head-nonexistent";
        let throttle_options = crate::ThrottleOptions::default();

        let fake_etag = chroma_storage::ETag("fake-etag".to_string());

        let result = Manifest::head(&throttle_options, &storage, prefix, &fake_etag).await;
        assert!(
            result.is_err(),
            "head should return error for nonexistent manifest"
        );
    }
}
