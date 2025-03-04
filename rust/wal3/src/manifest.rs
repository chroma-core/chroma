use setsum::Setsum;

use crate::{
    Error, Fragment, FragmentSeqNo, LogPosition, LogWriterOptions, ScrubError, SnapshotOptions,
    ThrottleOptions,
};

/////////////////////////////////////////////// paths //////////////////////////////////////////////

fn manifest_path() -> String {
    "manifest/MANIFEST".to_string()
}

fn snapshot_path(setsum: Setsum) -> String {
    format!("snapshot/SNAPSHOT.{}", setsum.hexdigest())
}

fn snapshot_setsum(path: &str) -> Result<Setsum, Error> {
    let setsum = path
        .strip_prefix("snapshot/SNAPSHOT.")
        .ok_or_else(|| Error::CorruptManifest(format!("unparseable snapshot path: {}", path,)))?;
    let setsum = Setsum::from_hexdigest(setsum).ok_or_else(|| {
        Error::CorruptManifest(format!("unparseable snapshot setsum in {}", path,))
    })?;
    Ok(setsum)
}

//////////////////////////////////////////// SnapPointer ///////////////////////////////////////////

/// A SnapPointer is a pointer to a snapshot.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SnapPointer {
    #[serde(
        deserialize_with = "super::deserialize_setsum",
        serialize_with = "super::serialize_setsum"
    )]
    pub setsum: setsum::Setsum,
    pub path_to_snapshot: String,
    pub depth: u8,
    pub start: LogPosition,
    pub limit: LogPosition,
}

impl SnapPointer {
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
    pub snapshots: Vec<SnapPointer>,
    pub fragments: Vec<Fragment>,
}

impl Snapshot {
    /// Scrub the setsums of this snapshot and compare to the fragments.
    pub fn scrub(&self) -> Result<Setsum, ScrubError> {
        if !self.fragments.is_empty() && !self.snapshots.is_empty() {
            return Err(ScrubError::CorruptManifest {
                manifest: self.path.to_string(),
                what: format!(
                "snapshot contains both fragments and snapshots in {}: fragments:{} snapshots:{}",
                self.path,
                self.fragments.len(),
                self.snapshots.len(),
            ),
            });
        }
        let mut acc = Setsum::default();
        for snapshot in self.snapshots.iter() {
            acc += snapshot.setsum;
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
            )});
        }
        for frag in self.fragments.iter() {
            acc += frag.setsum;
        }
        if acc != self.setsum {
            return Err(ScrubError::CorruptManifest{
                manifest: self.path.to_string(),
                what: format!(
                "expected snapshot setsum does not match observed contents in {}: expected:{} != observed:{}",
                self.path,
                self.setsum.hexdigest(),
                acc.hexdigest()
            )});
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
            )});
        }
        Ok(acc)
    }

    pub async fn install(&self, options: &ThrottleOptions) -> Result<(), Error> {
        todo!("robert will implement");
    }
}
///////////////////////////////////////////// Manifest /////////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Manifest {
    pub path: String,
    #[serde(
        deserialize_with = "super::deserialize_setsum",
        serialize_with = "super::serialize_setsum"
    )]
    pub setsum: Setsum,
    pub writer: String,
    pub snapshots: Vec<SnapPointer>,
    pub fragments: Vec<Fragment>,
}

impl Manifest {
    // Possibly generate a new snapshot from self if the conditions are right.
    //
    // This just creates a snapshot.  Install it to object store and then call apply_snapshot when
    // it is durable to modify the manifest.
    pub fn generate_snapshot(&self, snapshot_options: SnapshotOptions) -> Option<Snapshot> {
        // TODO(rescrv):  A real, random string.
        let writer = "TODO".to_string();
        let can_snapshot_snapshots = self.snapshots.iter().filter(|s| s.depth < 2).count()
            >= snapshot_options.snapshot_rollover_threshold;
        let can_snapshot_fragments =
            self.fragments.len() >= snapshot_options.fragment_rollover_threshold;
        if can_snapshot_snapshots || can_snapshot_fragments {
            // NOTE(rescrv):  We _either_ compact a snapshot of snapshots or a snapshot of log
            // fragments.  We don't do both as interior snapshot nodes only refer to objects of the
            // same type.  Manifests are the only objects to refer to both fragments and snapshots.
            let mut snapshots = vec![];
            let mut fragments = vec![];
            let mut setsum = Setsum::default();
            let depth = if can_snapshot_snapshots {
                for snapshot in self.snapshots.iter() {
                    if snapshot.depth < 2
                        && snapshots.len() < snapshot_options.snapshot_rollover_threshold
                    {
                        setsum += snapshot.setsum;
                        snapshots.push(snapshot.clone());
                    }
                }
                2
            } else if can_snapshot_fragments {
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
                1
            } else {
                unreachable!();
            };
            let path = snapshot_path(setsum);
            Some(Snapshot {
                path,
                depth,
                setsum,
                writer,
                snapshots,
                fragments,
            })
        } else {
            None
        }
    }

    /// Given a snapshot, apply it to the manifest.  This modifies the manifest to refer to the
    /// snapshot and removes from the snapshot all data that is now part of the snapshot.
    pub fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Error> {
        if snapshot.fragments.is_empty() {
            return Ok(());
        }
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
        self.snapshots
            .retain(|s| !snapshot.snapshots.iter().any(|t| t.setsum == s.setsum));
        self.fragments = self.fragments.split_off(snapshot.fragments.len());
        self.snapshots.push(SnapPointer {
            setsum: snapshot.setsum,
            path_to_snapshot: snapshot.path.clone(),
            depth: snapshot.depth,
            start: snapshot
                .fragments
                .iter()
                .map(|f| f.start)
                .min_by_key(|p| p.offset())
                .unwrap_or(LogPosition::default()),
            limit: snapshot
                .fragments
                .iter()
                .map(|f| f.limit)
                .max_by_key(|p| p.offset())
                .unwrap_or(LogPosition::default()),
        });
        Ok(())
    }

    /// Can the fragment be applied to the manifest.  True iff sequentiality.
    ///
    /// Once upon a time there was more parallelism in wal3 and this was a more interesting.  Now
    /// it mostly returns true unless internal invariants are violated.
    pub fn can_apply_fragment(&self, fragment: &Fragment) -> bool {
        let Fragment {
            path: _,
            seq_no,
            start,
            limit,
            setsum: _,
        } = fragment;
        let max_seq_no = self
            .fragments
            .iter()
            .map(|f| f.seq_no)
            .max()
            .unwrap_or(FragmentSeqNo(0));
        max_seq_no < max_seq_no + 1 && max_seq_no + 1 == *seq_no && start.offset() < limit.offset()
    }

    /// Modify the manifest to apply the fragment to it.
    pub fn apply_fragment(&mut self, fragment: Fragment) {
        self.setsum += fragment.setsum;
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
    pub fn scrub(&self) -> Result<Setsum, ScrubError> {
        let mut acc = Setsum::default();
        for snapshot in self.snapshots.iter() {
            acc += snapshot.setsum;
        }
        for frag in self.fragments.iter() {
            acc += frag.setsum;
        }
        if self.setsum != acc {
            return Err(ScrubError::CorruptManifest{
                manifest: self.path.to_string(),
                what: format!(
                "expected manifest setsum does not match observed contents: expected:{} != observed:{}",
                self.setsum.hexdigest(),
                acc.hexdigest()
            )});
        }
        // TODO(rescrv):  Check the sequence numbers for sequentiality.
        Ok(acc)
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
    pub async fn initialize(_: &LogWriterOptions) -> Result<(), Error> {
        todo!("robert will implement once storage supports If-Match");
    }

    /// Load the latest manifest from object storage.
    pub async fn load() -> Result<Option<Manifest>, Error> {
        todo!("robert will implement");
    }

    /// Install a manifest to object storage.
    pub async fn install(&self, options: &ThrottleOptions, new: &Manifest) -> Result<(), Error> {
        todo!("robert will implement");
    }
}
/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths() {
        assert_eq!("manifest/MANIFEST", manifest_path());
    }

    #[test]
    fn fragment_contains_position() {
        let fragment = Fragment {
            path: "path".to_string(),
            seq_no: FragmentSeqNo(1),
            start: LogPosition::uni(1),
            limit: LogPosition::uni(42),
            setsum: Setsum::default(),
        };
        assert!(!fragment.possibly_contains_position(LogPosition::uni(0)));
        assert!(fragment.possibly_contains_position(LogPosition::uni(1)));
        assert!(fragment.possibly_contains_position(LogPosition::uni(41)));
        assert!(!fragment.possibly_contains_position(LogPosition::uni(42)));
        assert!(!fragment.possibly_contains_position(LogPosition::uni(u64::MAX)));
    }

    #[test]
    fn manifest_contains_position() {
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentSeqNo(1),
            start: LogPosition::uni(1),
            limit: LogPosition::uni(22),
            setsum: Setsum::default(),
        };
        let fragment2 = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentSeqNo(2),
            start: LogPosition::uni(22),
            limit: LogPosition::uni(42),
            setsum: Setsum::default(),
        };
        let manifest = Manifest {
            path: String::from("manifest/MANIFEST.ffffffffffffffff"),
            writer: "manifest writer 1".to_string(),
            setsum: Setsum::default(),
            snapshots: vec![],
            fragments: vec![fragment1, fragment2],
        };
        assert!(!manifest.contains_position(LogPosition::uni(0)));
        assert!(manifest.contains_position(LogPosition::uni(1)));
        assert!(manifest.contains_position(LogPosition::uni(41)));
        assert!(manifest.contains_position(LogPosition::uni(41)));
        assert!(!manifest.contains_position(LogPosition::uni(42)));
        assert!(!manifest.contains_position(LogPosition::uni(u64::MAX)));
    }

    #[test]
    fn manifest_scrub_setsum() {
        let fragment1 = Fragment {
            path: "path1".to_string(),
            seq_no: FragmentSeqNo(1),
            start: LogPosition::uni(1),
            limit: LogPosition::uni(22),
            setsum: Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
        };
        let fragment2 = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentSeqNo(2),
            start: LogPosition::uni(22),
            limit: LogPosition::uni(42),
            setsum: Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let manifest = Manifest {
            path: String::from("manifest/MANIFEST.ffffffffffffffff"),
            writer: "manifest writer 1".to_string(),
            setsum: Setsum::from_hexdigest(
                "307d93deb6b3e91525dc277027bc34958d8f1e74965e4c027820c3596e0f2847",
            )
            .unwrap(),
            snapshots: vec![],
            fragments: vec![fragment1.clone(), fragment2.clone()],
        };
        assert!(manifest.scrub().is_ok());
        let manifest = Manifest {
            path: String::from("manifest/MANIFEST.ffffffffffffffff"),
            writer: "manifest writer 1".to_string(),
            setsum: Setsum::from_hexdigest(
                "6c5b5ee2c5e741a8d190d215d6cb2802a57ce0d3bb5a1a0223964e97acfa8083",
            )
            .unwrap(),
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
            start: LogPosition::uni(1),
            limit: LogPosition::uni(22),
            setsum: Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
        };
        let fragment2 = Fragment {
            path: "path2".to_string(),
            seq_no: FragmentSeqNo(2),
            start: LogPosition::uni(22),
            limit: LogPosition::uni(42),
            setsum: Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let mut manifest = Manifest {
            path: String::from("manifest/MANIFEST.ffffffffffffffff"),
            writer: "manifest writer 1".to_string(),
            setsum: Setsum::default(),
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
                path: String::from("manifest/MANIFEST.ffffffffffffffff"),
                writer: "manifest writer 1".to_string(),
                setsum: Setsum::from_hexdigest(
                    "307d93deb6b3e91525dc277027bc34958d8f1e74965e4c027820c3596e0f2847",
                )
                .unwrap(),
                snapshots: vec![],
                fragments: vec![
                    Fragment {
                        path: "path1".to_string(),
                        seq_no: FragmentSeqNo(1),
                        start: LogPosition::uni(1),
                        limit: LogPosition::uni(22),
                        setsum: Setsum::from_hexdigest(
                            "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465"
                        )
                        .unwrap()
                    },
                    Fragment {
                        path: "path2".to_string(),
                        seq_no: FragmentSeqNo(2),
                        start: LogPosition::uni(22),
                        limit: LogPosition::uni(42),
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
}
