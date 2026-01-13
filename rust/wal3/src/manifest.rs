//! Manifests, and their immutable cousin, snapshots, are the metadata that describe a log.
//!
//! A manifest transitively names every file in object storage that is part of the log.
//!
//! Snapshots are content-addressable and immutable, while manifests get overwritten.  For that
//! reason, manifests embed the ETag for conditional writes while snapshots do not.

use setsum::Setsum;

use crate::{
    Error, Fragment, FragmentIdentifier, FragmentSeqNo, Garbage, LogPosition, ManifestWitness,
    ScrubError, ScrubSuccess, SnapshotOptions, SnapshotPointerOrFragmentIdentifier,
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

///////////////////////////////////////////// scrubbing /////////////////////////////////////////////

/// Verify that all fragments have the same FragmentIdentifier variant (all SeqNo or all Uuid).
fn scrub_fragment_identifier_uniformity(
    manifest_name: &str,
    fragments: &[Fragment],
) -> Result<(), Box<ScrubError>> {
    let Some(first_is_seq_no) = fragments.first().map(|s| s.seq_no.as_seq_no().is_some()) else {
        return Ok(());
    };
    for frag in fragments.iter().skip(1) {
        let is_seq_no = frag.seq_no.as_seq_no().is_some();
        if is_seq_no != first_is_seq_no {
            return Err(ScrubError::CorruptManifest {
                manifest: manifest_name.to_string(),
                what: format!(
                    "contains mixed FragmentIdentifier variants: first is {} but found {}",
                    if first_is_seq_no { "SeqNo" } else { "Uuid" },
                    if is_seq_no { "SeqNo" } else { "Uuid" },
                ),
            }
            .into());
        }
    }
    Ok(())
}

////////////////////////////////////////// SnapshotPointer /////////////////////////////////////////

/// A SnapshotPointer is a pointer to a snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Deserialize, serde::Serialize)]
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
        scrub_fragment_identifier_uniformity(&self.path, &self.fragments)?;
        Ok(ScrubSuccess {
            calculated_setsum,
            bytes_read,
            short_read: false,
        })
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
    pub initial_seq_no: Option<FragmentIdentifier>,
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
                        .unwrap_or(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(0)))
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
        (Some(fragment.seq_no) == self.next_fragment_seq_no()
            || matches!(fragment.seq_no, FragmentIdentifier::Uuid(_)))
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
        scrub_fragment_identifier_uniformity(&format!("{:?}", self), &self.fragments)?;
        // TODO(rescrv):  Check the sequence numbers for sequentiality.
        Ok(ScrubSuccess {
            calculated_setsum,
            bytes_read,
            short_read: false,
        })
    }

    /// The next sequence number to generate, or None if the log has exhausted them.
    pub fn next_fragment_seq_no(&self) -> Option<FragmentIdentifier> {
        if let Some(max_seq_no) = self.fragments.iter().map(|f| f.seq_no).max() {
            max_seq_no.successor()
        } else {
            Some(self.initial_seq_no.unwrap_or(FragmentIdentifier::BEGIN))
        }
    }

    /// Apply the destructive operation specified by the Garbage struct.
    #[allow(clippy::result_large_err)]
    pub fn apply_garbage(&self, garbage: Garbage) -> Result<Option<Self>, Error> {
        if garbage.is_empty() {
            return Err(Error::GarbageCollectionPrecondition(
                SnapshotPointerOrFragmentIdentifier::Stringy(
                    "cannot apply empty garbage".to_string(),
                ),
            ));
        }
        if !garbage.fragments_are_uuids
            && FragmentIdentifier::from(garbage.fragments_to_drop_limit)
                <= self.initial_seq_no.unwrap_or(FragmentIdentifier::BEGIN)
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
        if garbage.fragments_to_drop_limit == FragmentSeqNo::ZERO && !garbage.fragments_are_uuids {
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
        let start = garbage.fragments_to_drop_start.as_u64();
        let limit = garbage.fragments_to_drop_limit.as_u64();
        for seq_no in start..limit {
            if let Some(index) = new.fragments.iter().position(|f| {
                f.seq_no == FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(seq_no))
            }) {
                setsum_to_discard += new.fragments[index].setsum;
                new.fragments.remove(index);
            }
        }
        if garbage.fragments_are_uuids {
            let mut acc = Setsum::default();
            new.fragments.retain(|frag| {
                if frag.limit <= garbage.first_to_keep {
                    acc += frag.setsum;
                    false
                } else {
                    true
                }
            });
            setsum_to_discard += acc;
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
                SnapshotPointerOrFragmentIdentifier::Stringy(format!(
                    "Setsum mismatch: {} != {}",
                    setsum_to_discard.hexdigest(),
                    garbage.setsum_to_discard.hexdigest()
                )),
            ));
        }
        new.collected += garbage.setsum_to_discard;
        new.initial_offset = Some(garbage.first_to_keep);
        // Only update initial_seq_no for SeqNo-based logs. UUID-based logs don't use sequential
        // numbering, so setting initial_seq_no to SeqNo(0) would mix identifier types.
        if !garbage.fragments_are_uuids {
            new.initial_seq_no = Some(FragmentIdentifier::from(garbage.fragments_to_drop_limit));
        }
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

////////////////////////////////////////// ManifestAndWitness /////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ManifestAndWitness {
    pub manifest: Manifest,
    pub witness: ManifestWitness,
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use chroma_storage::ETag;

    use super::*;
    use crate::FragmentUuid;

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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
            start: LogPosition::from_offset(1),
            limit: LogPosition::from_offset(22),
            num_bytes: 4100,
            setsum: Setsum::default(),
        };
        let fragment2 = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
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
                        seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
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
                        seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(3)),
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
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(10))),
        };
        assert_eq!(
            manifest.next_write_timestamp(),
            LogPosition::from_offset(100)
        );
        assert_eq!(manifest.oldest_timestamp(), LogPosition::from_offset(100));
        assert_eq!(
            manifest.next_fragment_seq_no(),
            Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(10)))
        );
        assert_eq!(
            manifest.next_fragment_seq_no(),
            Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(10)))
        );

        let fragment = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(10)),
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
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(10))),
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
            Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(11))),
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
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(10))),
        };
        assert_eq!(manifest.oldest_timestamp(), LogPosition::from_offset(100));
        assert_eq!(
            manifest.next_write_timestamp(),
            LogPosition::from_offset(100)
        );
        assert_eq!(
            manifest.next_fragment_seq_no(),
            Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(10)))
        );

        let fragment = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(10)),
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
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(10))),
        };
        assert_eq!(
            manifest_with_fragment.oldest_timestamp(),
            LogPosition::from_offset(100)
        );
        assert_eq!(
            manifest_with_fragment.next_fragment_seq_no(),
            Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(11)))
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
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
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1))),
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
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(100))),
        };

        let serialized = serde_json::to_string(&manifest).unwrap();
        let deserialized: Manifest = serde_json::from_str(&serialized).unwrap();
        assert_eq!(manifest, deserialized);
        assert_eq!(
            deserialized.initial_offset,
            Some(LogPosition::from_offset(1000))
        );
        assert_eq!(
            deserialized.initial_seq_no,
            Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(100)))
        );

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
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(50))),
        };

        let fragment = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(50)),
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
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(50))),
        };

        assert_eq!(manifest, expected_manifest);
        assert_eq!(
            manifest.next_fragment_seq_no(),
            Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(51)))
        );
    }

    #[test]
    fn manifest_fragment_for_position_with_initial_offset() {
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
            start: LogPosition::from_offset(100),
            limit: LogPosition::from_offset(150),
            num_bytes: 50,
            setsum: Setsum::default(),
        };
        let fragment2 = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
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
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1))),
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
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
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1))),
        };

        assert!(!manifest.contains_position(LogPosition::from_offset(50)));
        assert!(manifest.contains_position(LogPosition::from_offset(150)));
        assert!(!manifest.contains_position(LogPosition::from_offset(250)));
    }

    #[test]
    fn manifest_timestamps_with_initial_offset() {
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
            start: LogPosition::from_offset(100),
            limit: LogPosition::from_offset(150),
            num_bytes: 50,
            setsum: Setsum::default(),
        };
        let fragment2 = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
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
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(42))),
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
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(50))),
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
    fn apply_garbage_equal_nonzero_fragment_identifiers() {
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
            fragments_to_drop_start: FragmentSeqNo::from_u64(5),
            fragments_to_drop_limit: FragmentSeqNo::from_u64(5),
            fragments_are_uuids: false,
            setsum_to_discard: Setsum::default(),
            first_to_keep: LogPosition::from_offset(100),
            snapshot_for_root: None,
        };

        let result = manifest.apply_garbage(garbage).unwrap().unwrap();

        // When fragments_to_drop_start == fragments_to_drop_limit and both are non-zero,
        // initial_seq_no should be set to the limit value
        assert_eq!(
            result.initial_seq_no,
            Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(5)))
        );
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
            fragments_to_drop_start: FragmentSeqNo::from_u64(10),
            fragments_to_drop_limit: FragmentSeqNo::from_u64(5),
            fragments_are_uuids: false,
            setsum_to_discard: Setsum::default(),
            first_to_keep: LogPosition::from_offset(1),
        };

        let result = manifest.apply_garbage(invalid_garbage);
        assert!(result.is_err());

        if let Err(crate::Error::GarbageCollection(msg)) = result {
            println!("GarbageCollection error message: {msg}");
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
            fragments_to_drop_start: FragmentSeqNo::from_u64(5),
            fragments_to_drop_limit: FragmentSeqNo::from_u64(5),
            fragments_are_uuids: false,
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
            fragments_to_drop_start: FragmentSeqNo::from_u64(1),
            fragments_to_drop_limit: FragmentSeqNo::from_u64(5),
            fragments_are_uuids: false,
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
                seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(3089266)),
                start: LogPosition { offset: 5566918 },
                limit: LogPosition { offset: 5566919 },
                num_bytes: 2116,
                setsum: Setsum::from_hexdigest(
                    "0ff66765647c73839d76a6cb4ce16a8340b71c543c171843a95d8e48c1bee3fc",
                )
                .unwrap(),
            }],
            initial_offset: Some(LogPosition { offset: 5566918 }),
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(3089266))),
        };

        // Case 1: fragments_to_drop_limit <= initial_seq_no, no snapshots to drop/make
        let garbage = Garbage {
            snapshots_to_drop: vec![],
            snapshots_to_make: vec![],
            snapshot_for_root: None,
            fragments_to_drop_start: FragmentSeqNo::from_u64(3089257),
            fragments_to_drop_limit: FragmentSeqNo::from_u64(3089266), // Equal to initial_seq_no
            fragments_are_uuids: false,
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
            fragments_to_drop_limit: FragmentSeqNo::from_u64(3089265), // Less than initial_seq_no
            ..garbage.clone()
        };

        let result = manifest.apply_garbage(garbage_below_initial);
        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "Expected None when fragments_to_drop_limit < initial_seq_no"
        );
    }

    #[test]
    fn manifest_scrub_rejects_mixed_fragment_identifiers() {
        use uuid::Uuid;

        let fragment_seq_no = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
            start: LogPosition::from_offset(1),
            limit: LogPosition::from_offset(22),
            num_bytes: 4100,
            setsum: Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
        };
        let fragment_uuid = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentIdentifier::Uuid(FragmentUuid::from_uuid(Uuid::nil())),
            start: LogPosition::from_offset(22),
            limit: LogPosition::from_offset(42),
            num_bytes: 4100,
            setsum: Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let manifest = Manifest {
            writer: "test_writer".to_string(),
            setsum: fragment_seq_no.setsum + fragment_uuid.setsum,
            collected: Setsum::default(),
            acc_bytes: 8200,
            snapshots: vec![],
            fragments: vec![fragment_seq_no, fragment_uuid],
            initial_offset: None,
            initial_seq_no: None,
        };
        let result = manifest.scrub();
        assert!(
            result.is_err(),
            "scrub should reject mixed FragmentIdentifier variants"
        );
        let err = result.unwrap_err();
        let err_str = format!("{err}");
        println!("manifest_scrub_rejects_mixed_fragment_identifiers error: {err_str}");
        assert!(
            err_str.contains("mixed FragmentIdentifier variants"),
            "error message should mention mixed variants: {err_str}"
        );
    }

    #[test]
    fn manifest_scrub_accepts_uniform_seq_no_identifiers() {
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
            start: LogPosition::from_offset(22),
            limit: LogPosition::from_offset(42),
            num_bytes: 4100,
            setsum: Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let manifest = Manifest {
            writer: "test_writer".to_string(),
            setsum: Setsum::from_hexdigest(
                "307d93deb6b3e91525dc277027bc34958d8f1e74965e4c027820c3596e0f2847",
            )
            .unwrap(),
            collected: Setsum::default(),
            acc_bytes: 8200,
            snapshots: vec![],
            fragments: vec![fragment1, fragment2],
            initial_offset: None,
            initial_seq_no: None,
        };
        assert!(
            manifest.scrub().is_ok(),
            "scrub should accept uniform SeqNo identifiers"
        );
    }

    #[test]
    fn manifest_scrub_accepts_uniform_uuid_identifiers() {
        use uuid::Uuid;

        let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();
        let uuid2 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440002").unwrap();
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid1)),
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
            seq_no: FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid2)),
            start: LogPosition::from_offset(22),
            limit: LogPosition::from_offset(42),
            num_bytes: 4100,
            setsum: Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let manifest = Manifest {
            writer: "test_writer".to_string(),
            setsum: Setsum::from_hexdigest(
                "307d93deb6b3e91525dc277027bc34958d8f1e74965e4c027820c3596e0f2847",
            )
            .unwrap(),
            collected: Setsum::default(),
            acc_bytes: 8200,
            snapshots: vec![],
            fragments: vec![fragment1, fragment2],
            initial_offset: None,
            initial_seq_no: None,
        };
        assert!(
            manifest.scrub().is_ok(),
            "scrub should accept uniform Uuid identifiers"
        );
    }

    #[test]
    fn snapshot_scrub_rejects_mixed_fragment_identifiers() {
        use uuid::Uuid;

        let fragment_seq_no = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
            start: LogPosition::from_offset(1),
            limit: LogPosition::from_offset(22),
            num_bytes: 4100,
            setsum: Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
        };
        let fragment_uuid = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentIdentifier::Uuid(FragmentUuid::from_uuid(Uuid::nil())),
            start: LogPosition::from_offset(22),
            limit: LogPosition::from_offset(42),
            num_bytes: 4100,
            setsum: Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let combined_setsum = fragment_seq_no.setsum + fragment_uuid.setsum;
        let snapshot = Snapshot {
            path: unprefixed_snapshot_path(combined_setsum),
            depth: 1,
            setsum: combined_setsum,
            writer: "test_writer".to_string(),
            snapshots: vec![],
            fragments: vec![fragment_seq_no, fragment_uuid],
        };
        let result = snapshot.scrub();
        assert!(
            result.is_err(),
            "scrub should reject mixed FragmentIdentifier variants"
        );
        let err = result.unwrap_err();
        let err_str = format!("{err}");
        println!("snapshot_scrub_rejects_mixed_fragment_identifiers error: {err_str}");
        assert!(
            err_str.contains("mixed FragmentIdentifier variants"),
            "error message should mention mixed variants: {err_str}"
        );
    }

    #[test]
    fn snapshot_scrub_accepts_uniform_seq_no_identifiers() {
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
            start: LogPosition::from_offset(22),
            limit: LogPosition::from_offset(42),
            num_bytes: 4100,
            setsum: Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let combined_setsum = fragment1.setsum + fragment2.setsum;
        let snapshot = Snapshot {
            path: unprefixed_snapshot_path(combined_setsum),
            depth: 1,
            setsum: combined_setsum,
            writer: "test_writer".to_string(),
            snapshots: vec![],
            fragments: vec![fragment1, fragment2],
        };
        assert!(
            snapshot.scrub().is_ok(),
            "scrub should accept uniform SeqNo identifiers"
        );
    }

    #[test]
    fn snapshot_scrub_accepts_uniform_uuid_identifiers() {
        use uuid::Uuid;

        let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();
        let uuid2 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440002").unwrap();
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid1)),
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
            seq_no: FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid2)),
            start: LogPosition::from_offset(22),
            limit: LogPosition::from_offset(42),
            num_bytes: 4100,
            setsum: Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let combined_setsum = fragment1.setsum + fragment2.setsum;
        let snapshot = Snapshot {
            path: unprefixed_snapshot_path(combined_setsum),
            depth: 1,
            setsum: combined_setsum,
            writer: "test_writer".to_string(),
            snapshots: vec![],
            fragments: vec![fragment1, fragment2],
        };
        assert!(
            snapshot.scrub().is_ok(),
            "scrub should accept uniform Uuid identifiers"
        );
    }

    #[test]
    fn manifest_and_witness_serde_round_trip_empty_manifest() {
        let manifest = Manifest::new_empty("test-writer");
        let witness = ManifestWitness::ETag(ETag("test-etag-12345".to_string()));
        let original = ManifestAndWitness { manifest, witness };
        let serialized = serde_json::to_string(&original).expect("serialization should succeed");
        println!("serialized ManifestAndWitness (empty): {serialized}");
        let deserialized: ManifestAndWitness =
            serde_json::from_str(&serialized).expect("deserialization should succeed");
        assert_eq!(original, deserialized);
    }

    #[test]
    fn manifest_and_witness_serde_round_trip_with_fragments() {
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(1)),
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
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(2)),
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
            fragments: vec![fragment1, fragment2],
            initial_offset: None,
            initial_seq_no: None,
        };
        let witness = ManifestWitness::ETag(ETag("etag-with-fragments".to_string()));
        let original = ManifestAndWitness { manifest, witness };
        let serialized = serde_json::to_string(&original).expect("serialization should succeed");
        println!("serialized ManifestAndWitness (with fragments): {serialized}");
        let deserialized: ManifestAndWitness =
            serde_json::from_str(&serialized).expect("deserialization should succeed");
        assert_eq!(original, deserialized);
    }

    #[test]
    fn manifest_and_witness_serde_round_trip_with_snapshots() {
        let snapshot_setsum = Setsum::from_hexdigest(
            "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
        )
        .unwrap();
        let snapshot_pointer = SnapshotPointer {
            path_to_snapshot: unprefixed_snapshot_path(snapshot_setsum),
            setsum: snapshot_setsum,
            start: LogPosition::from_offset(0),
            limit: LogPosition::from_offset(100),
            depth: 1,
            num_bytes: 5000,
        };
        let fragment = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(10)),
            start: LogPosition::from_offset(100),
            limit: LogPosition::from_offset(200),
            num_bytes: 3000,
            setsum: Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let manifest = Manifest {
            writer: "snapshot-writer".to_string(),
            setsum: snapshot_setsum + fragment.setsum,
            collected: Setsum::default(),
            acc_bytes: 8000,
            snapshots: vec![snapshot_pointer],
            fragments: vec![fragment],
            initial_offset: None,
            initial_seq_no: None,
        };
        let witness = ManifestWitness::ETag(ETag("snapshot-etag-xyz".to_string()));
        let original = ManifestAndWitness { manifest, witness };
        let serialized = serde_json::to_string(&original).expect("serialization should succeed");
        println!("serialized ManifestAndWitness (with snapshots): {serialized}");
        let deserialized: ManifestAndWitness =
            serde_json::from_str(&serialized).expect("deserialization should succeed");
        assert_eq!(original, deserialized);
    }

    #[test]
    fn manifest_and_witness_serde_round_trip_with_initial_offset() {
        let manifest = Manifest {
            writer: "bootstrap".to_string(),
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: Some(LogPosition::from_offset(1000)),
            initial_seq_no: Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(100))),
        };
        let witness = ManifestWitness::ETag(ETag("initial-offset-etag".to_string()));
        let original = ManifestAndWitness { manifest, witness };
        let serialized = serde_json::to_string(&original).expect("serialization should succeed");
        println!("serialized ManifestAndWitness (with initial_offset): {serialized}");
        let deserialized: ManifestAndWitness =
            serde_json::from_str(&serialized).expect("deserialization should succeed");
        assert_eq!(original, deserialized);
        assert_eq!(
            deserialized.manifest.initial_offset,
            Some(LogPosition::from_offset(1000))
        );
        assert_eq!(
            deserialized.manifest.initial_seq_no,
            Some(FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(100)))
        );
    }

    #[test]
    fn manifest_and_witness_serde_round_trip_with_uuid_fragments() {
        let uuid1 = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let uuid2 = uuid::Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid1)),
            start: LogPosition::from_offset(0),
            limit: LogPosition::from_offset(50),
            num_bytes: 1000,
            setsum: Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
        };
        let fragment2 = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentIdentifier::Uuid(FragmentUuid::from_uuid(uuid2)),
            start: LogPosition::from_offset(50),
            limit: LogPosition::from_offset(100),
            num_bytes: 2000,
            setsum: Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let manifest = Manifest {
            writer: "uuid-writer".to_string(),
            setsum: fragment1.setsum + fragment2.setsum,
            collected: Setsum::default(),
            acc_bytes: 3000,
            snapshots: vec![],
            fragments: vec![fragment1, fragment2],
            initial_offset: None,
            initial_seq_no: None,
        };
        let witness = ManifestWitness::ETag(ETag("uuid-etag".to_string()));
        let original = ManifestAndWitness { manifest, witness };
        let serialized = serde_json::to_string(&original).expect("serialization should succeed");
        println!("serialized ManifestAndWitness (with uuid fragments): {serialized}");
        let deserialized: ManifestAndWitness =
            serde_json::from_str(&serialized).expect("deserialization should succeed");
        assert_eq!(original, deserialized);
    }

    #[test]
    fn manifest_and_witness_serde_round_trip_with_collected() {
        let collected_setsum = Setsum::from_hexdigest(
            "1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();
        let manifest = Manifest {
            writer: "gc-writer".to_string(),
            setsum: Setsum::default(),
            collected: collected_setsum,
            acc_bytes: 0,
            snapshots: vec![],
            fragments: vec![],
            initial_offset: None,
            initial_seq_no: None,
        };
        let witness = ManifestWitness::ETag(ETag("collected-etag".to_string()));
        let original = ManifestAndWitness { manifest, witness };
        let serialized = serde_json::to_string(&original).expect("serialization should succeed");
        println!("serialized ManifestAndWitness (with collected): {serialized}");
        let deserialized: ManifestAndWitness =
            serde_json::from_str(&serialized).expect("deserialization should succeed");
        assert_eq!(original, deserialized);
        assert_eq!(deserialized.manifest.collected, collected_setsum);
    }
}
