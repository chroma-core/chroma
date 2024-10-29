use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, Result};

use crate::{Error, LogPosition, LogWriterOptions, ScrubError, ThrottleOptions, MANIFEST_UPLOADED};

///////////////////////////////////////////// constants ////////////////////////////////////////////

pub fn manifest_path(timestamp: u128) -> String {
    format!("manifest/MANIFEST.{}", timestamp)
}

pub fn manifest_timestamp(path: &str) -> Result<u128, Error> {
    let timestamp = path
        .strip_prefix("manifest/MANIFEST.")
        .ok_or_else(|| Error::CorruptManifest(format!("unparseable manifest path: {}", path,)))?;
    let timestamp = timestamp.parse::<u128>().map_err(|e| {
        Error::CorruptManifest(format!("unparseable manifest timestamp in {}: {e}", path,))
    })?;
    Ok(timestamp)
}

////////////////////////////////////////////// ShardID /////////////////////////////////////////////

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct ShardID(pub usize);

//////////////////////////////////////////// ShardSeqNo ////////////////////////////////////////////

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct ShardSeqNo(pub usize);

impl std::ops::Add<ShardSeqNo> for usize {
    type Output = ShardSeqNo;

    fn add(self, rhs: ShardSeqNo) -> Self::Output {
        ShardSeqNo(self.wrapping_add(rhs.0))
    }
}

impl std::ops::Add<usize> for ShardSeqNo {
    type Output = ShardSeqNo;

    fn add(self, rhs: usize) -> Self::Output {
        ShardSeqNo(self.0.wrapping_add(rhs))
    }
}

impl std::ops::AddAssign<usize> for ShardSeqNo {
    fn add_assign(&mut self, rhs: usize) {
        self.0 = self.0.wrapping_add(rhs);
    }
}

/////////////////////////////////////////// ShardFragment //////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ShardFragment {
    pub path: String,
    pub shard_id: ShardID,
    pub seq_no: ShardSeqNo,
    pub start: LogPosition,
    pub limit: LogPosition,
    #[serde(
        deserialize_with = "super::deserialize_setsum",
        serialize_with = "super::serialize_setsum"
    )]
    pub setsum: sst::Setsum,
}

impl ShardFragment {
    pub fn contains_position(&self, position: LogPosition) -> bool {
        self.start <= position && position < self.limit
    }

    pub fn scrub(&self) -> Result<sst::Setsum, ScrubError> {
        Ok(self.setsum)
    }
}

//////////////////////////////////////////// PrevPointer ///////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct PrevPointer {
    #[serde(
        deserialize_with = "super::deserialize_setsum",
        serialize_with = "super::serialize_setsum"
    )]
    pub setsum: sst::Setsum,
    pub path_to_manifest: String,
}

//////////////////////////////////////////// NextPointer ///////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct NextPointer {
    pub path_to_manifest: String,
}

impl NextPointer {
    pub fn generate(current: u128) -> Self {
        let mut now_micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("the system will never go back to before the UNIX epoch")
            .as_micros();
        if now_micros <= current {
            now_micros = current + 1;
        }
        NextPointer {
            path_to_manifest: manifest_path(now_micros),
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
    pub setsum: sst::Setsum,
    pub writer: String,
    pub fragments: Vec<ShardFragment>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev: Option<PrevPointer>,
    pub next: NextPointer,
}

impl Manifest {
    pub fn can_apply_fragment(&self, fragment: &ShardFragment) -> bool {
        let ShardFragment {
            path: _,
            shard_id,
            seq_no,
            start,
            limit,
            setsum: _,
        } = fragment;
        let max_seq_no = self
            .fragments
            .iter()
            .filter(|f| f.shard_id == *shard_id)
            .map(|f| f.seq_no)
            .max()
            .unwrap_or(ShardSeqNo(0));
        max_seq_no < max_seq_no + 1 && max_seq_no + 1 == *seq_no && *start < *limit
    }

    pub fn apply_fragment(&mut self, fragment: ShardFragment) {
        let ShardFragment {
            path,
            shard_id,
            seq_no,
            start,
            limit,
            setsum,
        } = fragment;
        self.fragments.push(ShardFragment {
            path: path.clone(),
            shard_id,
            seq_no,
            start,
            limit,
            setsum,
        });
        self.setsum += setsum;
    }

    pub fn generate_next_pointer(&mut self) -> Result<(), Error> {
        let timestamp = manifest_timestamp(&self.next.path_to_manifest)?;
        self.next = NextPointer::generate(timestamp);
        Ok(())
    }

    pub fn contains_position(&self, position: LogPosition) -> bool {
        self.fragments.iter().any(|f| f.contains_position(position))
    }

    pub fn scrub(&self) -> Result<sst::Setsum, ScrubError> {
        let mut acc = self.prev.as_ref().map(|p| p.setsum).unwrap_or_default();
        for fragment in self.fragments.iter() {
            let fragment_acc = fragment.scrub()?;
            acc += fragment_acc;
        }
        if self.setsum != acc {
            return Err(ScrubError::CorruptManifest(format!(
                "expected manifest setsum does not match observed contents: expected:{} != observed:{}",
                self.setsum.hexdigest(),
                acc.hexdigest()
            )));
        }
        let mut acc = self.prev.as_ref().map(|p| p.setsum).unwrap_or_default();
        for frag in self.fragments.iter() {
            acc += frag.setsum;
        }
        if self.setsum != acc {
            return Err(ScrubError::CorruptManifest(format!(
                "expected manifest setsum does not match observed contents: expected:{} != observed:{}",
                self.setsum.hexdigest(),
                acc.hexdigest()
            )));
        }
        // TODO(rescrv):  Check the sequence numbers for sequentiality.
        Ok(acc)
    }

    pub fn next_seq_no_for_shard(&self, shard_id: ShardID) -> Option<ShardSeqNo> {
        let max_seq_no = self
            .fragments
            .iter()
            .filter(|f| f.shard_id == shard_id)
            .map(|f| f.seq_no)
            .max()
            .unwrap_or(ShardSeqNo(0));
        if max_seq_no + 1 > max_seq_no {
            Some(max_seq_no + 1)
        } else {
            None
        }
    }

    pub async fn initialize(
        _: &LogWriterOptions,
        object_store: &impl ObjectStore,
    ) -> Result<(), Error> {
        let opts: PutOptions = PutMode::Create.into();
        let this = Path::from(manifest_path(0));
        let next = NextPointer::generate(0);
        let manifest = Manifest {
            setsum: sst::Setsum::default(),
            fragments: vec![],
            prev: None,
            next,
            // TODO(rescrv):  A real, random string.
            writer: "log initializer".to_string(),
        };
        let payload = serde_json::to_string(&manifest).map_err(|e| {
            Error::CorruptManifest(format!("could not encode JSON manifest: {e:?}"))
        })?;
        object_store
            .put_opts(&this, payload.into(), opts)
            .await
            .map_err(|err| match err {
                object_store::Error::AlreadyExists { .. } => Error::AlreadyInitialized,
                _ => Error::ObjectStore(err.into()),
            })?;
        Ok(())
    }

    /// Load the latest manifest from object storage.
    pub async fn load(
        object_store: &impl ObjectStore,
        alpha: usize,
    ) -> Result<Option<Manifest>, Error> {
        // First, list all manifests and make sure we find a root manifest.
        let mut saw_root = false;
        let mut listings = object_store.list(Some(&Path::from("manifest/")));
        let mut candidate_paths = Vec::with_capacity(alpha);
        while let Some(meta) = listings.next().await.transpose()? {
            let timestamp = manifest_timestamp(meta.location.as_ref())?;
            if timestamp == 0 {
                saw_root = true;
            }
            candidate_paths.push((timestamp, meta.location));
        }
        if !saw_root {
            return Err(Error::CorruptManifest("no root manifest found".to_string()));
        }
        candidate_paths.sort();
        candidate_paths.reverse();
        let mut seen: HashMap<String, usize> = HashMap::default();
        let mut fetched: HashMap<Path, Manifest> = HashMap::default();
        let mut blessed_writer = None;
        let mut blessed_timestamp = 0u128;
        // Fetch the manifests in reverse order until we hit a stopping condition.
        for (timestamp, path) in candidate_paths.iter() {
            let object = match object_store.get(path).await {
                Ok(object) => object,
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(Error::Internal);
                }
                Err(e) => return Err(e.into()),
            };
            let body = object.bytes().await?;
            let manifest = String::from_utf8(body.to_vec()).map_err(|err| {
                Error::CorruptManifest(format!("could not decode UTF8 manifest: {err:?}"))
            })?;
            let manifest: Manifest = serde_json::from_str(&manifest).map_err(|err| {
                Error::CorruptManifest(format!("could not decode JSON manifest: {err:?}"))
            })?;
            let writer = manifest.writer.clone();
            fetched.insert(path.clone(), manifest);
            let count = match seen.entry(writer.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(1usize);
                    1usize
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    *entry.get_mut() += 1;
                    *entry.get()
                }
            };
            if (*timestamp == 0 || count >= alpha) && blessed_writer.is_none() {
                blessed_writer = Some(writer.clone());
                blessed_timestamp = *timestamp;
                break;
            }
        }
        // Fetch the highest-timestamped manifest from the blessed writer that also forms a
        // consistent chain of manifests going back to the blessed timestamp.
        if let Some(writer) = blessed_writer {
            for (timestamp, path) in candidate_paths.iter() {
                if let Some(manifest) = fetched.remove(path) {
                    fn forms_a_chain(
                        blessed_timestamp: u128,
                        timestamp: u128,
                        manifest: &Manifest,
                        fetched: &HashMap<Path, Manifest>,
                        alpha: usize,
                    ) -> bool {
                        if alpha == 0 {
                            return true;
                        }
                        let Some(prev) = manifest.prev.as_ref() else {
                            return timestamp == blessed_timestamp;
                        };
                        let prev_timestamp =
                            match manifest_timestamp(prev.path_to_manifest.as_str()) {
                                Ok(timestamp) => timestamp,
                                Err(_) => return false,
                            };
                        if let Some(prev) = fetched.get(&Path::from(prev.path_to_manifest.as_str()))
                        {
                            forms_a_chain(
                                blessed_timestamp,
                                prev_timestamp,
                                prev,
                                fetched,
                                alpha - 1,
                            )
                        } else {
                            false
                        }
                    }
                    if manifest.writer == writer && (*timestamp == blessed_timestamp)
                        || forms_a_chain(blessed_timestamp, *timestamp, &manifest, &fetched, alpha)
                    {
                        return Ok(Some(manifest));
                    }
                } else {
                    return Err(Error::Internal);
                }
            }
        }
        Err(Error::Internal)
    }

    pub async fn install(
        &self,
        options: &ThrottleOptions,
        object_store: &impl ObjectStore,
        new: &Manifest,
    ) -> Result<(), Error> {
        let exp_backoff = crate::backoff::ExponentialBackoff::new(
            options.throughput as f64,
            options.headroom as f64,
        );
        loop {
            let payload = serde_json::to_string(&new).map_err(|e| {
                Error::CorruptManifest(format!("could not encode JSON manifest: {e:?}"))
            })?;
            let payload = PutPayload::from_bytes(payload.into());
            let opts: PutOptions = PutMode::Create.into();
            match object_store
                .put_opts(
                    &Path::from(self.next.path_to_manifest.as_str()),
                    payload,
                    opts.clone(),
                )
                .await
            {
                Ok(_) => {
                    MANIFEST_UPLOADED.click();
                    println!("uploaded manifest to {}", self.next.path_to_manifest);
                    return Ok(());
                }
                Err(object_store::Error::Precondition { .. }) => {
                    return Err(Error::LogContention);
                }
                Err(object_store::Error::AlreadyExists { .. }) => {
                    return Err(Error::LogContention);
                }
                Err(e) => {
                    println!("error uploading manifest: {e:?}");
                    let mut backoff = exp_backoff.next();
                    if backoff > Duration::from_secs(3_600) {
                        backoff = Duration::from_secs(3_600);
                    }
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths() {
        assert_eq!("manifest/MANIFEST.0", manifest_path(0));
        assert_eq!(0, manifest_timestamp("manifest/MANIFEST.0").unwrap());
        assert_eq!(
            "manifest/MANIFEST.340282366920938463463374607431768211455",
            manifest_path(u128::MAX)
        );
        assert_eq!(
            u128::MAX,
            manifest_timestamp("manifest/MANIFEST.340282366920938463463374607431768211455")
                .unwrap()
        );
    }

    #[test]
    fn shard_fragment_contains_position() {
        let shard_fragment = ShardFragment {
            path: "path".to_string(),
            shard_id: ShardID(1),
            seq_no: ShardSeqNo(1),
            start: LogPosition(1),
            limit: LogPosition(42),
            setsum: sst::Setsum::default(),
        };
        assert!(!shard_fragment.contains_position(LogPosition(0)));
        assert!(shard_fragment.contains_position(LogPosition(1)));
        assert!(shard_fragment.contains_position(LogPosition(41)));
        assert!(!shard_fragment.contains_position(LogPosition(42)));
        assert!(!shard_fragment.contains_position(LogPosition(u64::MAX)));
    }

    #[test]
    fn manifest_contains_position() {
        let shard_fragment1 = ShardFragment {
            path: "path1".to_string(),
            shard_id: ShardID(1),
            seq_no: ShardSeqNo(1),
            start: LogPosition(1),
            limit: LogPosition(22),
            setsum: sst::Setsum::default(),
        };
        let shard_fragment2 = ShardFragment {
            path: "path2".to_string(),
            shard_id: ShardID(1),
            seq_no: ShardSeqNo(2),
            start: LogPosition(22),
            limit: LogPosition(42),
            setsum: sst::Setsum::default(),
        };
        let manifest = Manifest {
            writer: "manifest writer 1".to_string(),
            setsum: sst::Setsum::default(),
            fragments: vec![shard_fragment1, shard_fragment2],
            prev: None,
            next: NextPointer {
                path_to_manifest: "manifest/MANIFEST.1".to_string(),
            },
        };
        assert!(!manifest.contains_position(LogPosition(0)));
        assert!(manifest.contains_position(LogPosition(1)));
        assert!(manifest.contains_position(LogPosition(41)));
        assert!(manifest.contains_position(LogPosition(41)));
        assert!(!manifest.contains_position(LogPosition(42)));
        assert!(!manifest.contains_position(LogPosition(u64::MAX)));
    }

    #[test]
    fn manifest_scrub_setsum() {
        let shard_fragment1 = ShardFragment {
            path: "path1".to_string(),
            shard_id: ShardID(1000),
            seq_no: ShardSeqNo(1),
            start: LogPosition(1),
            limit: LogPosition(22),
            setsum: sst::Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
        };
        let shard_fragment2 = ShardFragment {
            path: "path2".to_string(),
            shard_id: ShardID(1000),
            seq_no: ShardSeqNo(2),
            start: LogPosition(22),
            limit: LogPosition(42),
            setsum: sst::Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let manifest = Manifest {
            writer: "manifest writer 1".to_string(),
            setsum: sst::Setsum::from_hexdigest(
                "307d93deb6b3e91525dc277027bc34958d8f1e74965e4c027820c3596e0f2847",
            )
            .unwrap(),
            fragments: vec![shard_fragment1.clone(), shard_fragment2.clone()],
            prev: None,
            next: NextPointer {
                path_to_manifest: "manifest/MANIFEST.1".to_string(),
            },
        };
        assert!(manifest.scrub().is_ok());
        let manifest = Manifest {
            writer: "manifest writer 1".to_string(),
            setsum: sst::Setsum::from_hexdigest(
                "6c5b5ee2c5e741a8d190d215d6cb2802a57ce0d3bb5a1a0223964e97acfa8083",
            )
            .unwrap(),
            fragments: vec![shard_fragment1, shard_fragment2],
            prev: None,
            next: NextPointer {
                path_to_manifest: "manifest/MANIFEST.1".to_string(),
            },
        };
        assert!(manifest.scrub().is_err());
    }

    #[test]
    fn apply_fragment() {
        let fragment1 = ShardFragment {
            path: "path1".to_string(),
            shard_id: ShardID(1000),
            seq_no: ShardSeqNo(1),
            start: LogPosition(1),
            limit: LogPosition(22),
            setsum: sst::Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
        };
        let fragment2 = ShardFragment {
            path: "path2".to_string(),
            shard_id: ShardID(1000),
            seq_no: ShardSeqNo(2),
            start: LogPosition(22),
            limit: LogPosition(42),
            setsum: sst::Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        let mut manifest = Manifest {
            writer: "manifest writer 1".to_string(),
            setsum: sst::Setsum::default(),
            fragments: vec![],
            prev: None,
            next: NextPointer {
                path_to_manifest: "manifest/MANIFEST.1".to_string(),
            },
        };
        assert!(!manifest.can_apply_fragment(&fragment2));
        assert!(manifest.can_apply_fragment(&fragment1));
        manifest.apply_fragment(fragment1);
        assert!(manifest.can_apply_fragment(&fragment2));
        manifest.apply_fragment(fragment2);
        assert_eq!(
            Manifest {
                writer: "manifest writer 1".to_string(),
                setsum: sst::Setsum::from_hexdigest(
                    "307d93deb6b3e91525dc277027bc34958d8f1e74965e4c027820c3596e0f2847",
                )
                .unwrap(),
                fragments: vec![
                    ShardFragment {
                        path: "path1".to_string(),
                        shard_id: ShardID(1000),
                        seq_no: ShardSeqNo(1),
                        start: LogPosition(1),
                        limit: LogPosition(22),
                        setsum: sst::Setsum::from_hexdigest(
                            "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465"
                        )
                        .unwrap()
                    },
                    ShardFragment {
                        path: "path2".to_string(),
                        shard_id: ShardID(1000),
                        seq_no: ShardSeqNo(2),
                        start: LogPosition(22),
                        limit: LogPosition(42),
                        setsum: sst::Setsum::from_hexdigest(
                            "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1"
                        )
                        .unwrap()
                    }
                ],
                prev: None,
                next: NextPointer {
                    path_to_manifest: "manifest/MANIFEST.1".to_string(),
                },
            },
            manifest
        );
    }

    #[tokio::test]
    async fn manifest_initialize() {
        let object_store = object_store::memory::InMemory::new();
        let options = LogWriterOptions::default();
        Manifest::initialize(&options, &object_store).await.unwrap();
        let mut manifest = Manifest::load(&object_store, 1).await.unwrap().unwrap();
        manifest.next = NextPointer {
            path_to_manifest: "manifest/MANIFEST.1".to_string(),
        };
        assert_eq!(
            Manifest {
                writer: "log initializer".to_string(),
                setsum: sst::Setsum::default(),
                fragments: vec![],
                next: NextPointer {
                    path_to_manifest: "manifest/MANIFEST.1".to_string(),
                },
                prev: None,
            },
            manifest
        );
    }

    #[tokio::test]
    async fn manifest_install_many_load_latest() {
        let object_store = object_store::memory::InMemory::new();
        let options = LogWriterOptions::default();
        // First manifest.
        Manifest::initialize(&options, &object_store).await.unwrap();
        let mut manifest0 = Manifest::load(&object_store, 1).await.unwrap().unwrap();
        manifest0.generate_next_pointer().unwrap();
        // Second manifest.
        let mut manifest1 = manifest0.clone();
        let ptr1 = manifest1.next.clone();
        manifest1.generate_next_pointer().unwrap();
        let ptr2 = manifest1.next.clone();
        let options = ThrottleOptions::default();
        Manifest::install(&manifest0, &options, &object_store, &manifest1)
            .await
            .unwrap();
        // Third manifest.
        let mut manifest2 = manifest1.clone();
        let fragment1 = ShardFragment {
            path: "path1".to_string(),
            shard_id: ShardID(1),
            seq_no: ShardSeqNo(1),
            start: LogPosition(1),
            limit: LogPosition(22),
            setsum: sst::Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
        };
        manifest2.apply_fragment(fragment1);
        manifest2.generate_next_pointer().unwrap();
        if Manifest::install(&manifest1, &options, &object_store, &manifest2)
            .await
            .is_err()
        {
            panic!("{ptr1:?} {ptr2:?}");
        }
        // Fourth manifest.
        let mut manifest3 = manifest2.clone();
        let fragment2 = ShardFragment {
            path: "path2".to_string(),
            shard_id: ShardID(1),
            seq_no: ShardSeqNo(2),
            start: LogPosition(22),
            limit: LogPosition(42),
            setsum: sst::Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
        };
        manifest3.apply_fragment(fragment2);
        manifest3.generate_next_pointer().unwrap();
        Manifest::install(&manifest2, &options, &object_store, &manifest3)
            .await
            .unwrap();
        // Load the manifest.  It should match manifest3.
        let manifest = Manifest::load(&object_store, 1).await.unwrap().unwrap();
        assert_eq!(manifest3, manifest);
    }
}
