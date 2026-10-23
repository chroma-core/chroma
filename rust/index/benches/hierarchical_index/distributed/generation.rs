//! Durable publication for the bounded fixture. This adapter uses one filesystem
//! namespace shared by the coordinator and readers. Owners upload immutable bytes
//! into that namespace before acknowledging their manifests. It does not rely on
//! `Storage::Local` conditional writes, and it does not implement crash recovery.
use super::generation_backend::{ConditionalObjects, DurableStoreConfig};
#[path = "generation_parallel.rs"]
mod parallel;
pub(super) use parallel::save_blocks_parallel;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

pub type Result<T> = std::result::Result<T, GenerationError>;
#[derive(Debug)]
pub enum GenerationError {
    Io(io::Error),
    Codec(serde_json::Error),
    Invalid(String),
    Conflict,
    Busy,
}
impl std::fmt::Display for GenerationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "generation publication: {self:?}")
    }
}
impl std::error::Error for GenerationError {}
impl From<io::Error> for GenerationError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<serde_json::Error> for GenerationError {
    fn from(e: serde_json::Error) -> Self {
        Self::Codec(e)
    }
}
fn invalid(s: &str) -> GenerationError {
    GenerationError::Invalid(s.into())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FormatContract {
    pub version: u32,
    pub dimensions: usize,
    pub metric: String,
    pub centroid_bits: u8,
    pub posting_bits: u8,
    pub vector_bits: u8,
}
impl FormatContract {
    fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.dimensions == 0
            || !matches!(self.metric.as_str(), "cosine" | "l2" | "ip")
            || self.centroid_bits != 4
            || self.posting_bits != 1
            || self.vector_bits != 32
        {
            return Err(invalid("unsupported format, metric or precision"));
        }
        Ok(())
    }
}

/// The checksum detects accidental corruption; it is not an authentication hash.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectRef {
    pub path: String,
    pub bytes: u64,
    pub checksum: u64,
    /// Absent only in legacy POSIX references; durable readers require it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnerManifest {
    pub run: String,
    pub save_attempt: String,
    pub generation: String,
    pub owner: u64,
    pub roots: Vec<u128>,
    pub vector_count: u64,
    pub format: FormatContract,
    pub nodes: Vec<ObjectRef>,
    pub postings: Vec<ObjectRef>,
    pub full_vectors: Vec<ObjectRef>,
    /// Saved format v2: binary sibling arrays, read when a reader opens.
    /// Empty and omitted for v1, so v1 manifests keep their exact bytes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub navigation: Vec<ObjectRef>,
    /// Saved format v2: exact internal centroids, read only by explicit code
    /// validation. Metadata opening checks names, as it does for postings.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub validation: Vec<ObjectRef>,
}
impl OwnerManifest {
    /// Every immutable object this owner's save produced, besides its manifest.
    pub fn objects(&self) -> impl Iterator<Item = &ObjectRef> {
        self.nodes
            .iter()
            .chain(&self.navigation)
            .chain(&self.validation)
            .chain(&self.postings)
            .chain(&self.full_vectors)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RootCoverage {
    pub root: u128,
    pub owner: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenerationManifest {
    pub run: String,
    pub save_attempt: String,
    pub generation: String,
    pub format: FormatContract,
    pub expected_roots: Vec<RootCoverage>,
    pub owners: Vec<ObjectRef>,
    pub shared_routing: ObjectRef,
    pub full_vectors: Vec<ObjectRef>,
    pub total_vectors: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenerationDescriptor {
    pub generation: String,
    pub save_attempt: String,
    pub manifest: ObjectRef,
}

/// Counts belong to one store handle and its clones, not a global process.
/// Sync calls count attempts; times include failed calls. Byte counts describe
/// complete temporary writes and supplied immutable payloads, not device I/O.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StoreIoProfile {
    pub immutable_calls: u64,
    pub immutable_payload_bytes: u64,
    pub temporary_bytes_written: u64,
    pub new_names: u64,
    pub replayed_names: u64,
    pub file_sync_calls: u64,
    pub file_sync_nanos: u64,
    pub directory_sync_calls: u64,
    pub directory_sync_nanos: u64,
}
struct StoreWork {
    profile: Arc<Mutex<StoreIoProfile>>,
    counts: StoreIoProfile,
}
impl Drop for StoreWork {
    fn drop(&mut self) {
        let mut total = self.profile.lock().unwrap_or_else(|e| e.into_inner());
        total.immutable_calls = total
            .immutable_calls
            .saturating_add(self.counts.immutable_calls);
        total.immutable_payload_bytes = total
            .immutable_payload_bytes
            .saturating_add(self.counts.immutable_payload_bytes);
        total.temporary_bytes_written = total
            .temporary_bytes_written
            .saturating_add(self.counts.temporary_bytes_written);
        total.new_names = total.new_names.saturating_add(self.counts.new_names);
        total.replayed_names = total
            .replayed_names
            .saturating_add(self.counts.replayed_names);
        total.file_sync_calls = total
            .file_sync_calls
            .saturating_add(self.counts.file_sync_calls);
        total.file_sync_nanos = total
            .file_sync_nanos
            .saturating_add(self.counts.file_sync_nanos);
        total.directory_sync_calls = total
            .directory_sync_calls
            .saturating_add(self.counts.directory_sync_calls);
        total.directory_sync_nanos = total
            .directory_sync_nanos
            .saturating_add(self.counts.directory_sync_nanos);
    }
}
#[derive(Clone)]
pub struct GenerationStore {
    root: PathBuf,
    backend: Option<Arc<dyn ConditionalObjects>>,
    io: Arc<Mutex<StoreIoProfile>>,
}
impl std::fmt::Debug for GenerationStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenerationStore")
            .field("root", &self.root)
            .field("durable", &self.backend.is_some())
            .finish()
    }
}
struct PublicationLock {
    path: PathBuf,
}
impl Drop for PublicationLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
fn checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |a, b| {
        (a ^ *b as u64).wrapping_mul(0x100000001b3)
    })
}
fn identifier(s: &str) -> Result<()> {
    if s.is_empty()
        || s.len() > 128
        || !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(invalid("invalid identity"));
    }
    Ok(())
}
fn relative(s: &str) -> Result<()> {
    if s.is_empty()
        || !Path::new(s)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(invalid("object path must be relative without traversal"));
    }
    Ok(())
}
fn sync_dir(p: &Path, work: &mut StoreIoProfile) -> Result<()> {
    let start = Instant::now();
    work.directory_sync_calls += 1;
    let result = File::open(p).and_then(|file| file.sync_all());
    work.directory_sync_nanos += start.elapsed().as_nanos() as u64;
    result.map_err(Into::into)
}
fn sync_file(file: &File, work: &mut StoreIoProfile) -> Result<()> {
    let start = Instant::now();
    work.file_sync_calls += 1;
    let result = file.sync_all();
    work.file_sync_nanos += start.elapsed().as_nanos() as u64;
    result.map_err(Into::into)
}
impl GenerationStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        let root = fs::canonicalize(root)?;
        for d in ["objects", "owners", "generations", "attempts"] {
            fs::create_dir_all(root.join(d))?;
        }
        let mut io = StoreIoProfile::default();
        sync_dir(&root, &mut io)?;
        Ok(Self {
            root,
            backend: None,
            io: Arc::new(Mutex::new(io)),
        })
    }
    pub fn open_durable(config: &DurableStoreConfig) -> Result<Self> {
        Self::with_backend(config.connect()?)
    }
    pub(super) fn with_backend(backend: Arc<dyn ConditionalObjects>) -> Result<Self> {
        Ok(Self {
            root: PathBuf::new(),
            backend: Some(backend),
            io: Arc::new(Mutex::new(StoreIoProfile::default())),
        })
    }
    pub fn is_durable(&self) -> bool {
        self.backend.is_some()
    }
    pub fn io_profile(&self) -> StoreIoProfile {
        self.io.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    fn work(&self) -> StoreWork {
        StoreWork {
            profile: Arc::clone(&self.io),
            counts: StoreIoProfile::default(),
        }
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    fn path(&self, p: &str) -> Result<PathBuf> {
        relative(p)?;
        Ok(self.root.join(p))
    }
    /// Install immutable bytes without overwriting an existing name. Hard-linking
    /// a synced temporary file makes an incomplete write unobservable by name.
    fn immutable(&self, path: &str, bytes: &[u8]) -> Result<ObjectRef> {
        let mut work = self.work();
        work.counts.immutable_calls = 1;
        work.counts.immutable_payload_bytes = bytes.len() as u64;
        relative(path)?;
        if let Some(backend) = &self.backend {
            match backend.put(path, bytes, None) {
                Ok(_) => work.counts.new_names += 1,
                Err(error) => {
                    // Multipart completion can report a generic storage error for
                    // a failed condition, or lose a successful write response.
                    // Resolve either only by comparing the complete immutable bytes.
                    match backend.get(path) {
                        Ok(Some(existing)) if existing.bytes == bytes => {
                            work.counts.replayed_names += 1;
                        }
                        _ => return Err(error),
                    }
                }
            }
            return Ok(ObjectRef {
                path: path.into(),
                bytes: bytes.len() as u64,
                checksum: checksum(bytes),
                sha256: Some(format!("{:x}", Sha256::digest(bytes))),
            });
        }
        let final_path = self.path(path)?;
        let parent = final_path
            .parent()
            .ok_or_else(|| invalid("missing parent"))?;
        if !parent.is_dir() {
            return Err(invalid("object parent must already exist"));
        }
        let tmp = parent.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
            f.write_all(bytes)?;
            work.counts.temporary_bytes_written += bytes.len() as u64;
            sync_file(&f, &mut work.counts)?;
            match fs::hard_link(&tmp, &final_path) {
                Ok(()) => {
                    work.counts.new_names += 1;
                    sync_dir(parent, &mut work.counts)?;
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    if fs::read(&final_path)? != bytes {
                        return Err(GenerationError::Conflict);
                    }
                    work.counts.replayed_names += 1;
                }
                Err(e) => return Err(e.into()),
            }
            Ok(ObjectRef {
                path: path.into(),
                bytes: bytes.len() as u64,
                checksum: checksum(bytes),
                sha256: Some(format!("{:x}", Sha256::digest(bytes))),
            })
        })();
        let _ = fs::remove_file(&tmp);
        result
    }
    pub fn put_object(&self, name: &str, bytes: &[u8]) -> Result<ObjectRef> {
        identifier(name)?;
        self.immutable(&format!("objects/{name}"), bytes)
    }
    pub fn read_object(&self, r: &ObjectRef) -> Result<Vec<u8>> {
        relative(&r.path)?;
        if self.is_durable() && r.sha256.is_none() {
            return Err(invalid("durable object reference lacks SHA256"));
        }
        let bytes = if let Some(backend) = &self.backend {
            backend
                .get(&r.path)?
                .ok_or_else(|| invalid("durable object not found"))?
                .bytes
        } else {
            fs::read(self.path(&r.path)?)?
        };
        if bytes.len() as u64 != r.bytes
            || checksum(&bytes) != r.checksum
            || r.sha256
                .as_ref()
                .is_some_and(|s| *s != format!("{:x}", Sha256::digest(&bytes)))
        {
            return Err(invalid("object integrity mismatch"));
        }
        Ok(bytes)
    }
    fn read_json<T: serde::de::DeserializeOwned>(&self, r: &ObjectRef) -> Result<T> {
        Ok(serde_json::from_slice(&self.read_object(r)?)?)
    }
    fn check_refs(&self, refs: &[ObjectRef]) -> Result<()> {
        self.check_ref_names(refs)?;
        for r in refs {
            self.read_object(r)?;
        }
        Ok(())
    }
    fn check_ref_names(&self, refs: &[ObjectRef]) -> Result<()> {
        let mut seen = BTreeSet::new();
        for r in refs {
            relative(&r.path)?;
            if self.is_durable()
                && r.sha256.as_ref().is_none_or(|s| {
                    s.len() != 64
                        || !s
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                })
            {
                return Err(invalid("durable reference lacks supported SHA256"));
            }
            if !seen.insert(&r.path) {
                return Err(invalid("duplicate durable object reference"));
            }
        }
        Ok(())
    }
    fn validate_owner(&self, m: &OwnerManifest) -> Result<()> {
        self.validate_owner_with_postings(m, true)
    }
    fn validate_owner_with_postings(&self, m: &OwnerManifest, verify_postings: bool) -> Result<()> {
        for id in [&m.run, &m.save_attempt, &m.generation] {
            identifier(id)?;
        }
        m.format.validate()?;
        if m.roots.is_empty()
            || m.nodes.is_empty()
            || (m.vector_count > 0 && (m.postings.is_empty() || m.full_vectors.is_empty()))
        {
            return Err(invalid("owner manifest lacks roots or durable payloads"));
        }
        if m.roots.iter().collect::<BTreeSet<_>>().len() != m.roots.len() {
            return Err(invalid("duplicate owned root"));
        }
        self.check_refs(&m.nodes)?;
        self.check_refs(&m.navigation)?;
        if verify_postings {
            self.check_refs(&m.postings)?;
            self.check_refs(&m.validation)?;
        } else {
            self.check_ref_names(&m.postings)?;
            self.check_ref_names(&m.validation)?;
        }
        self.check_refs(&m.full_vectors)?;
        Ok(())
    }
    /// Reuse an owner's already durable immutable result only after verifying
    /// its frozen filename, manifest checksum and every referenced payload.
    /// This performs the validation in `save_owner` without rewriting bytes.
    pub fn verify_saved_owner(&self, reference: &ObjectRef) -> Result<OwnerManifest> {
        let manifest: OwnerManifest = self.read_json(reference)?;
        if reference.path
            != format!(
                "owners/{}-{}-{}-{}.json",
                manifest.run.len(),
                manifest.run,
                manifest.save_attempt,
                manifest.owner
            )
        {
            return Err(invalid("owner reference is not its frozen save result"));
        }
        self.validate_owner(&manifest)?;
        Ok(manifest)
    }
    /// Publication checks routing and strong immutable references without
    /// downloading postings. The owner verifies all payloads before its ack.
    pub fn verify_saved_owner_metadata(&self, reference: &ObjectRef) -> Result<OwnerManifest> {
        let manifest: OwnerManifest = self.read_json(reference)?;
        if reference.path
            != format!(
                "owners/{}-{}-{}-{}.json",
                manifest.run.len(),
                manifest.run,
                manifest.save_attempt,
                manifest.owner
            )
        {
            return Err(invalid("owner reference is not its frozen save result"));
        }
        self.validate_owner_with_postings(&manifest, false)?;
        Ok(manifest)
    }
    pub fn save_owner(&self, m: &OwnerManifest) -> Result<ObjectRef> {
        self.save_owner_reusing(m, &[])
    }
    pub(super) fn save_owner_reusing(
        &self,
        m: &OwnerManifest,
        verified: &[ObjectRef],
    ) -> Result<ObjectRef> {
        self.validate_owner_with_postings(m, false)?;
        // Reused objects have already passed owner-side verification in this
        // namespace. All newly materialized postings still verify before ack.
        let unverified: Vec<_> = m.postings.iter().chain(&m.validation).cloned().collect();
        save_blocks_parallel(&unverified, |object| {
            if !verified.contains(object) {
                self.read_object(object)?;
            }
            Ok(())
        })?;
        self.immutable(
            &format!(
                "owners/{}-{}-{}-{}.json",
                m.run.len(),
                m.run,
                m.save_attempt,
                m.owner
            ),
            &serde_json::to_vec(m)?,
        )
    }
    fn validate_generation(&self, m: &GenerationManifest) -> Result<()> {
        self.validate_generation_with_postings(m, true)
    }
    fn validate_generation_with_postings(
        &self,
        m: &GenerationManifest,
        verify_postings: bool,
    ) -> Result<()> {
        for id in [&m.run, &m.save_attempt, &m.generation] {
            identifier(id)?;
        }
        m.format.validate()?;
        if m.owners.is_empty() || m.expected_roots.is_empty() {
            return Err(invalid("generation has no owner coverage"));
        }
        self.read_object(&m.shared_routing)?;
        self.check_refs(&m.full_vectors)?;
        let mut expected = BTreeMap::new();
        for c in &m.expected_roots {
            if expected.insert(c.root, c.owner).is_some() {
                return Err(invalid("duplicate expected root"));
            }
        }
        let mut actual = BTreeMap::new();
        let mut owners = BTreeSet::new();
        let mut count = 0u64;
        let mut vectors: BTreeMap<String, ObjectRef> = BTreeMap::new();
        for r in &m.owners {
            let o: OwnerManifest = self.read_json(r)?;
            if o.run != m.run
                || o.save_attempt != m.save_attempt
                || o.generation != m.generation
                || o.format != m.format
                || !owners.insert(o.owner)
            {
                return Err(invalid("owner identity or format mismatch"));
            }
            // Local publication verifies all payloads. Durable publication trusts
            // owner-verified posting references and reads routing metadata only.
            self.validate_owner_with_postings(&o, verify_postings)?;
            if r.path
                != format!(
                    "owners/{}-{}-{}-{}.json",
                    o.run.len(),
                    o.run,
                    o.save_attempt,
                    o.owner
                )
            {
                return Err(invalid("owner reference is not its frozen save result"));
            }
            for root in o.roots {
                if actual.insert(root, o.owner).is_some() {
                    return Err(invalid("root covered more than once"));
                }
            }
            count = count
                .checked_add(o.vector_count)
                .ok_or_else(|| invalid("vector count overflow"))?;
            for v in o.full_vectors {
                if let Some(old) = vectors.insert(v.path.clone(), v.clone()) {
                    if old != v {
                        return Err(invalid("incompatible full-vector reference"));
                    }
                }
            }
        }
        let given: BTreeMap<_, _> = m
            .full_vectors
            .iter()
            .map(|v| (v.path.clone(), v.clone()))
            .collect();
        if actual != expected || count != m.total_vectors || vectors != given {
            return Err(invalid(
                "root coverage, vector count or full-vector view mismatch",
            ));
        }
        Ok(())
    }
    /// Caller closes admission and drains mutation/transfer/publication before
    /// freezing this body. No live writer state is serialized here.
    pub fn save_generation(&self, m: &GenerationManifest) -> Result<GenerationDescriptor> {
        self.validate_generation_with_postings(m, !self.is_durable())?;
        // Bind the save identity before the public generation name. Reusing an
        // attempt with changed bytes fails even if a new generation name is used.
        let bytes = serde_json::to_vec(m)?;
        self.immutable(
            &format!("attempts/{}-{}-{}.json", m.run.len(), m.run, m.save_attempt),
            &bytes,
        )?;
        let manifest = self.immutable(&format!("generations/{}.json", m.generation), &bytes)?;
        Ok(GenerationDescriptor {
            generation: m.generation.clone(),
            save_attempt: m.save_attempt.clone(),
            manifest,
        })
    }
    pub fn reopen(&self, d: &GenerationDescriptor) -> Result<GenerationManifest> {
        let m: GenerationManifest = self.read_json(&d.manifest)?;
        if m.generation != d.generation || m.save_attempt != d.save_attempt {
            return Err(invalid("descriptor identity mismatch"));
        }
        self.validate_generation(&m)?;
        Ok(m)
    }
    /// Open routing and reference metadata without fetching posting payloads.
    /// Each later posting read verifies its immutable bytes; explicit full
    /// validation uses the strict `reopen` path. Durable publication relies on
    /// owner-verified immutable posting references to avoid a central download.
    pub fn reopen_metadata(&self, d: &GenerationDescriptor) -> Result<GenerationManifest> {
        let m: GenerationManifest = self.read_json(&d.manifest)?;
        if m.generation != d.generation || m.save_attempt != d.save_attempt {
            return Err(invalid("descriptor identity mismatch"));
        }
        self.validate_generation_with_postings(&m, false)?;
        Ok(m)
    }
    pub fn latest(&self) -> Result<Option<GenerationDescriptor>> {
        if let Some(backend) = &self.backend {
            return backend
                .get("latest.json")?
                .map(|o| serde_json::from_slice(&o.bytes).map_err(Into::into))
                .transpose();
        }
        match fs::read(self.root.join("latest.json")) {
            Ok(b) => Ok(Some(serde_json::from_slice(&b)?)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    fn lock(&self) -> Result<PublicationLock> {
        let path = self.root.join("publication.lock");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(_) => return Ok(PublicationLock { path }),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    if Instant::now() >= deadline {
                        return Err(GenerationError::Busy);
                    }
                    thread::sleep(Duration::from_millis(2));
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    /// Replace the expected generation through an ETag condition in durable storage
    /// or a POSIX lock locally. The local lock is never stolen: a crashed
    /// publisher requires stopping this fixture.
    /// A repeated successful descriptor resolves a lost response idempotently.
    pub fn publish_latest(&self, d: &GenerationDescriptor, expected: Option<&str>) -> Result<()> {
        if let Some(backend) = &self.backend {
            self.reopen_metadata(d)?;
            let current = backend.get("latest.json")?;
            let existing: Option<GenerationDescriptor> = current
                .as_ref()
                .map(|o| serde_json::from_slice(&o.bytes))
                .transpose()?;
            if existing.as_ref() == Some(d) {
                return Ok(());
            }
            if existing.as_ref().map(|d| d.generation.as_str()) != expected {
                return Err(GenerationError::Conflict);
            }
            let bytes = serde_json::to_vec(d)?;
            match backend.put(
                "latest.json",
                &bytes,
                current.as_ref().map(|o| o.etag.as_str()),
            ) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    // Resolve the observed state, without retrying the write.
                    // S3 can report concurrent conditional writes as a generic
                    // 409 error rather than the adapter's typed Conflict.
                    match backend.get("latest.json") {
                        Ok(Some(observed)) if observed.bytes == bytes => return Ok(()),
                        Ok(Some(observed))
                            if serde_json::from_slice::<GenerationDescriptor>(&observed.bytes)
                                .is_ok_and(|descriptor| existing.as_ref() != Some(&descriptor)) =>
                        {
                            return Err(GenerationError::Conflict);
                        }
                        Err(read_error) => return Err(invalid(&format!(
                            "conditional latest write failed: {e}; reconciliation read failed: {read_error}"
                        ))),
                        _ => return Err(e),
                    }
                }
            }
        }
        self.reopen(d)?;
        let _lock = self.lock()?;
        let current = self.latest()?;
        if current.as_ref() == Some(d) {
            return Ok(());
        }
        if current.as_ref().map(|d| d.generation.as_str()) != expected {
            return Err(GenerationError::Conflict);
        }
        let mut work = self.work();
        let tmp = self.root.join(format!(".latest-{}", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
            f.write_all(&serde_json::to_vec(d)?)?;
            sync_file(&f, &mut work.counts)?;
            fs::rename(&tmp, self.root.join("latest.json"))?;
            sync_dir(&self.root, &mut work.counts)
        })();
        let _ = fs::remove_file(tmp);
        result
    }
}
#[cfg(test)]
#[path = "generation_tests.rs"]
mod tests;
