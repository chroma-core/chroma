//! Memory-mapped full-vector corpus for the saved reader. Opening costs one
//! metadata check when a hash receipt is current; rows are then faulted in on
//! demand by the kernel instead of being copied into per-row allocations.
use super::engine::VectorSource;
use super::saved_reader::VectorFileDescriptor;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

type Result<T> = std::result::Result<T, String>;

/// Identity of one hashed file state. Any rewrite changes mtime or ctime, and a
/// replacement changes the inode, so a matching receipt proves the bytes are
/// the ones already hashed without reading 30 GB again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct HashReceipt {
    device: u64,
    inode: u64,
    size: u64,
    mtime_ns: i64,
    ctime_ns: i64,
    sha256: String,
}
impl HashReceipt {
    fn of(meta: &std::fs::Metadata, sha256: &str) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let ns = |s: i64, n: i64| {
            s.checked_mul(1_000_000_000)
                .and_then(|v| v.checked_add(n))
                .ok_or_else(|| "file timestamp overflow".to_string())
        };
        Ok(Self {
            device: meta.dev(),
            inode: meta.ino(),
            size: meta.len(),
            mtime_ns: ns(meta.mtime(), meta.mtime_nsec())?,
            ctime_ns: ns(meta.ctime(), meta.ctime_nsec())?,
            sha256: sha256.to_ascii_lowercase(),
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct OpenReport {
    /// False when a current receipt let open skip the full-file hash.
    pub hashed: bool,
    pub hash_seconds: f64,
    pub receipt_path: PathBuf,
    /// Why the receipt could not be saved, if it could not.
    pub receipt_error: Option<String>,
}

pub struct MappedVectorSource {
    descriptor: VectorFileDescriptor,
    // Held so the mapping's file stays the one whose identity was verified.
    _file: File,
    ptr: *mut u8,
    len: usize,
}
// SAFETY: the mapping is PROT_READ, never remapped, and unmapped only in Drop,
// so shared references from any thread read immutable memory.
unsafe impl Send for MappedVectorSource {}
unsafe impl Sync for MappedVectorSource {}

impl MappedVectorSource {
    pub fn open(
        descriptor: VectorFileDescriptor,
        receipt_dir: Option<&Path>,
    ) -> Result<(Self, OpenReport)> {
        if cfg!(not(target_endian = "little")) {
            return Err("mapped vectors borrow little-endian f32 rows in place".into());
        }
        validate(&descriptor)?;
        let path = Path::new(&descriptor.path);
        let receipt_path = receipt_path(path, receipt_dir)?;
        let mut file = File::open(path).map_err(|e| e.to_string())?;
        let meta = file.metadata().map_err(|e| e.to_string())?;
        if meta.len() != descriptor.file_bytes {
            return Err("vector file length mismatch".into());
        }
        let expected = HashReceipt::of(&meta, &descriptor.sha256)?;
        // An unreadable or stale receipt only costs a rehash.
        let mut receipt_error = None;
        let current = std::fs::read(&receipt_path)
            .ok()
            .and_then(|b| serde_json::from_slice::<HashReceipt>(&b).ok())
            .is_some_and(|r| r == expected);
        let started = Instant::now();
        if !current {
            let digest = hash_file(&mut file)?;
            if !digest.eq_ignore_ascii_case(&descriptor.sha256) {
                return Err("vector corpus SHA-256 mismatch".into());
            }
            // A write during hashing would make the digest describe neither
            // state; refuse rather than bless it with a receipt.
            let after = file.metadata().map_err(|e| e.to_string())?;
            if HashReceipt::of(&after, &descriptor.sha256)? != expected {
                return Err("vector file changed while hashing".into());
            }
            // The hash already matched; a receipt that cannot be written only
            // means the next open hashes again.
            receipt_error = write_receipt(&receipt_path, &expected).err();
        }
        let report = OpenReport {
            hashed: !current,
            hash_seconds: if current {
                0.0
            } else {
                started.elapsed().as_secs_f64()
            },
            receipt_path,
            receipt_error,
        };
        let source = Self::map(descriptor, file)?;
        Ok((source, report))
    }

    fn map(descriptor: VectorFileDescriptor, file: File) -> Result<Self> {
        use std::os::unix::io::AsRawFd;
        let len = usize::try_from(descriptor.file_bytes)
            .map_err(|_| "vector file exceeds address space")?;
        // SAFETY: a fresh read-only shared mapping of an open descriptor with a
        // nonzero length; the kernel chooses the address.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(format!("mmap: {}", std::io::Error::last_os_error()));
        }
        // Construct first so Drop unmaps on the madvise error path.
        let source = Self {
            descriptor,
            _file: file,
            ptr: ptr.cast(),
            len,
        };
        // Queries touch scattered ~3 KB rows; default readahead would pull in
        // neighbouring pages that no query asked for.
        // SAFETY: the range is exactly the live mapping.
        if unsafe { libc::madvise(ptr, len, libc::MADV_RANDOM) } != 0 {
            return Err(format!("madvise: {}", std::io::Error::last_os_error()));
        }
        Ok(source)
    }

    /// Borrow one row in place. There is no per-row finiteness check: the
    /// full-file SHA-256 pins content that was finite when the corpus was made.
    pub fn row(&self, id: u64) -> Result<&[f32]> {
        let d = &self.descriptor;
        let row = id
            .checked_sub(d.first_id)
            .filter(|n| *n < d.rows)
            .ok_or("vector id outside saved corpus")?;
        // open() proved the last row ends within the file, so smaller rows
        // cannot overflow or run past the mapping.
        let offset = (row * d.stride_bytes + d.offset_bytes) as usize;
        // SAFETY: offset + dims * 4 <= len (validated extent); the mapping is
        // page-aligned and offset is a multiple of 4, so f32 alignment holds.
        Ok(unsafe { std::slice::from_raw_parts(self.ptr.add(offset).cast::<f32>(), d.dimensions) })
    }

    pub fn descriptor(&self) -> &VectorFileDescriptor {
        &self.descriptor
    }

    /// Pages of the corpus currently in the page cache, for warm/cold reports.
    pub fn resident_pages(&self) -> Result<u64> {
        // SAFETY: sysconf has no preconditions.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page <= 0 {
            return Err("unknown page size".into());
        }
        let mut vec = vec![0u8; self.len.div_ceil(page as usize)];
        // SAFETY: vec holds one byte per page of the live mapping. The element
        // type is c_char on macOS and c_uchar on Linux; both are one byte.
        let rc = unsafe { libc::mincore(self.ptr.cast(), self.len, vec.as_mut_ptr().cast()) };
        if rc != 0 {
            return Err(format!("mincore: {}", std::io::Error::last_os_error()));
        }
        Ok(vec.iter().filter(|b| **b & 1 != 0).count() as u64)
    }
}

impl Drop for MappedVectorSource {
    fn drop(&mut self) {
        // SAFETY: ptr/len are the mapping created in map(); no borrows outlive self.
        unsafe { libc::munmap(self.ptr.cast(), self.len) };
    }
}

impl VectorSource for MappedVectorSource {
    /// Existing callers own their vectors, so this copies the borrowed row.
    fn get(&self, id: u64) -> Result<Arc<[f32]>> {
        self.row(id).map(Arc::from)
    }
}

/// Same checks as CanonicalVectorSource::open, plus 4-byte alignment of every
/// row so it can be borrowed as &[f32] straight from the page-aligned map.
fn validate(d: &VectorFileDescriptor) -> Result<()> {
    if d.rows == 0
        || d.dimensions == 0
        || d.dimensions as u64 > u64::MAX / 4
        || d.stride_bytes < d.dimensions as u64 * 4
        || d.sha256.len() != 64
        || !d.sha256.bytes().all(|b| b.is_ascii_hexdigit())
        || d.immutable_locators.is_empty()
    {
        return Err("invalid canonical vector descriptor".into());
    }
    if d.offset_bytes % 4 != 0 || d.stride_bytes % 4 != 0 {
        return Err("vector rows are not 4-byte aligned".into());
    }
    let end = (d.rows - 1)
        .checked_mul(d.stride_bytes)
        .and_then(|n| n.checked_add(d.offset_bytes))
        .and_then(|n| n.checked_add(d.dimensions as u64 * 4))
        .ok_or("vector file extent overflow")?;
    d.first_id
        .checked_add(d.rows - 1)
        .ok_or("vector id extent overflow")?;
    if end > d.file_bytes {
        return Err("vector file length mismatch".into());
    }
    Ok(())
}

/// Check `file` against `sha256`, skipping the full hash when a current
/// receipt in `receipt_dir` already records this file's identity with that
/// digest, and writing one after a successful hash. Returns whether it hashed.
pub(super) fn verify_with_receipt(
    path: &Path,
    file: &mut File,
    sha256: &str,
    receipt_dir: Option<&Path>,
) -> Result<bool> {
    let receipt_path = receipt_path(path, receipt_dir)?;
    let meta = file.metadata().map_err(|e| e.to_string())?;
    let expected = HashReceipt::of(&meta, sha256)?;
    let current = std::fs::read(&receipt_path)
        .ok()
        .and_then(|b| serde_json::from_slice::<HashReceipt>(&b).ok())
        .is_some_and(|r| r == expected);
    if current {
        return Ok(false);
    }
    let digest = hash_file(file)?;
    if !digest.eq_ignore_ascii_case(sha256) {
        return Err("vector corpus SHA-256 mismatch".into());
    }
    let after = file.metadata().map_err(|e| e.to_string())?;
    if HashReceipt::of(&after, sha256)? != expected {
        return Err("vector file changed while hashing".into());
    }
    let _ = write_receipt(&receipt_path, &expected);
    Ok(true)
}

fn receipt_path(corpus: &Path, dir: Option<&Path>) -> Result<PathBuf> {
    let mut name = corpus
        .file_name()
        .ok_or("vector corpus path has no file name")?
        .to_os_string();
    name.push(".sha256-receipt.json");
    Ok(match dir {
        Some(dir) => dir.join(name),
        None => corpus.with_file_name(name),
    })
}

fn hash_file(file: &mut File) -> Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::{Read, Seek};
    file.rewind().map_err(|e| e.to_string())?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buffer).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

/// Temp file plus rename, so a crash never leaves a truncated receipt that a
/// later open could misparse.
fn write_receipt(path: &Path, receipt: &HashReceipt) -> Result<()> {
    use std::io::Write;
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(format!(".tmp.{}", std::process::id()));
    let tmp = PathBuf::from(tmp);
    let bytes = serde_json::to_vec_pretty(receipt).map_err(|e| e.to_string())?;
    let mut f = File::create(&tmp).map_err(|e| e.to_string())?;
    f.write_all(&bytes)
        .and_then(|_| f.sync_all())
        .map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, path).map_err(|e| e.to_string())
}

#[cfg(test)]
#[path = "mapped_vectors_tests.rs"]
mod tests;
