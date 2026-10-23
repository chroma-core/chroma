//! Durable publication for the bounded fixture. This adapter uses one filesystem
//! namespace shared by the coordinator and readers. Owners upload immutable bytes
//! into that namespace before acknowledging their manifests. It does not rely on
//! `Storage::Local` conditional writes, and it does not implement crash recovery.
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Component, Path, PathBuf},
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

#[derive(Clone, Debug)]
pub struct GenerationStore {
    root: PathBuf,
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
fn sync_dir(p: &Path) -> Result<()> {
    File::open(p)?.sync_all()?;
    Ok(())
}
impl GenerationStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        let root = fs::canonicalize(root)?;
        for d in ["objects", "owners", "generations", "attempts"] {
            fs::create_dir_all(root.join(d))?;
        }
        sync_dir(&root)?;
        Ok(Self { root })
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
            f.sync_all()?;
            match fs::hard_link(&tmp, &final_path) {
                Ok(()) => sync_dir(parent)?,
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    if fs::read(&final_path)? != bytes {
                        return Err(GenerationError::Conflict);
                    }
                }
                Err(e) => return Err(e.into()),
            }
            Ok(ObjectRef {
                path: path.into(),
                bytes: bytes.len() as u64,
                checksum: checksum(bytes),
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
        let bytes = fs::read(self.path(&r.path)?)?;
        if bytes.len() as u64 != r.bytes || checksum(&bytes) != r.checksum {
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
        if verify_postings {
            self.check_refs(&m.postings)?;
        } else {
            self.check_ref_names(&m.postings)?;
        }
        self.check_refs(&m.full_vectors)?;
        Ok(())
    }
    pub fn save_owner(&self, m: &OwnerManifest) -> Result<ObjectRef> {
        self.validate_owner(m)?;
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
            // This also verifies every payload from disk rather than trusting its acknowledgement.
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
        self.validate_generation(m)?;
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
    /// validation and publication continue to use the strict `reopen` path.
    pub fn reopen_metadata(&self, d: &GenerationDescriptor) -> Result<GenerationManifest> {
        let m: GenerationManifest = self.read_json(&d.manifest)?;
        if m.generation != d.generation || m.save_attempt != d.save_attempt {
            return Err(invalid("descriptor identity mismatch"));
        }
        self.validate_generation_with_postings(&m, false)?;
        Ok(m)
    }
    pub fn latest(&self) -> Result<Option<GenerationDescriptor>> {
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
    /// Atomic expected-generation replacement on a local POSIX filesystem. The
    /// lock is never stolen: a crashed publisher requires stopping this fixture.
    /// A repeated successful descriptor resolves a lost response idempotently.
    pub fn publish_latest(&self, d: &GenerationDescriptor, expected: Option<&str>) -> Result<()> {
        self.reopen(d)?;
        let _lock = self.lock()?;
        let current = self.latest()?;
        if current.as_ref() == Some(d) {
            return Ok(());
        }
        if current.as_ref().map(|d| d.generation.as_str()) != expected {
            return Err(GenerationError::Conflict);
        }
        let tmp = self.root.join(format!(".latest-{}", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
            f.write_all(&serde_json::to_vec(d)?)?;
            f.sync_all()?;
            fs::rename(&tmp, self.root.join("latest.json"))?;
            sync_dir(&self.root)
        })();
        let _ = fs::remove_file(tmp);
        result
    }
}
#[cfg(test)]
#[path = "generation_tests.rs"]
mod tests;
