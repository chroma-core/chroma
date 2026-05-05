//! Per-file SHA256 leaves + per-container Merkle root via `rs_merkle`.
//!
//! Why a Merkle tree at all when a per-file SHA256 map already gives us
//! exact change detection?
//!
//! 1. The container-level *root* is a single 32-byte fingerprint that flips
//!    iff any file in the container changed. Persisted alongside the leaf
//!    map, it's a cheap "does this container actually differ from the prior
//!    snapshot?" check we can use to suppress empty changelog deltas.
//!
//! 2. Roots compose: hashing the sorted list of `(container_id, root)` pairs
//!    gives a workspace-level root that downstream consumers can use as a
//!    manifest version (`"the dump is at 0xab43..."`) without trusting any
//!    single file.
//!
//! Leaf bytes are the contents of the file directly (SHA256 of the bytes is
//! the leaf hash). Path is **not** included in the leaf -- moves of identical
//! content produce the same leaf hash, and the `(rel_path, leaf_hash)` map
//! is what we diff to enumerate add/modify/remove. This matches how
//! content-addressed stores (git, restic) do it.

use anyhow::{Context, Result};
use rs_merkle::algorithms::Sha256 as MerkleSha256;
use rs_merkle::{Hasher, MerkleTree};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// One row of the per-container hash map: a single file's content hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileLeaf {
    /// Hex-encoded SHA256 of the file contents (no path mixed in).
    pub sha256: String,
    pub size_bytes: u64,
}

/// `rel_path` (relative to the container's `unzipped/` dir) -> leaf.
pub type ContainerHashMap = BTreeMap<String, FileLeaf>;

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedHashMap {
    /// Merkle root over the leaves, hex-encoded. Empty container -> `"0x"`.
    pub merkle_root_hex: String,
    /// Number of files (= leaves).
    pub file_count: u64,
    /// Sum of all file sizes in bytes.
    pub total_bytes: u64,
    /// Walltime epoch ms when this map was computed.
    pub computed_at_ms: i64,
    /// `rel_path -> {sha256, size_bytes}`. BTreeMap preserves the lexical
    /// order, which is also the order we feed to the Merkle builder so the
    /// root is reproducible.
    pub leaves: ContainerHashMap,
}

/// Walk `dir` recursively, hash every regular file, and return the leaf map.
/// Hidden dotfiles at any level are skipped.
pub fn hash_directory(dir: &Path) -> Result<ContainerHashMap> {
    let mut out = ContainerHashMap::new();
    if !dir.is_dir() {
        return Ok(out);
    }
    for entry in walkdir::WalkDir::new(dir)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| {
            e.file_name()
                .to_str()
                .map(|n| !n.starts_with('.'))
                .unwrap_or(true)
        })
    {
        let entry = entry.context("walking dir")?;
        if !entry.file_type().is_file() {
            continue;
        }
        let abs = entry.path();
        let rel = match abs.strip_prefix(dir) {
            Ok(r) => r,
            Err(_) => continue,
        };
        let bytes = std::fs::read(abs)
            .with_context(|| format!("reading {}", abs.display()))?;
        let mut h = Sha256::new();
        h.update(&bytes);
        let digest = h.finalize();
        let key = path_to_rel_key(rel);
        out.insert(
            key,
            FileLeaf {
                sha256: hex::encode(digest),
                size_bytes: bytes.len() as u64,
            },
        );
    }
    Ok(out)
}

/// Use `/`-separated paths as map keys regardless of OS, so the same dump
/// produces identical state on macOS, Linux, and Windows.
fn path_to_rel_key(rel: &Path) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for c in rel.components() {
        if let std::path::Component::Normal(s) = c {
            if let Some(s) = s.to_str() {
                parts.push(s);
            }
        }
    }
    parts.join("/")
}

#[derive(Debug, Default)]
pub struct HashDiff {
    pub added: Vec<String>,
    pub modified: Vec<String>,
    pub removed: Vec<String>,
}

impl HashDiff {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.modified.is_empty() && self.removed.is_empty()
    }
}

pub fn diff_hash_maps(prev: &ContainerHashMap, curr: &ContainerHashMap) -> HashDiff {
    let mut out = HashDiff::default();
    for (path, curr_leaf) in curr {
        match prev.get(path) {
            None => out.added.push(path.clone()),
            Some(prev_leaf) if prev_leaf.sha256 != curr_leaf.sha256 => {
                out.modified.push(path.clone())
            }
            Some(_) => {}
        }
    }
    for path in prev.keys() {
        if !curr.contains_key(path) {
            out.removed.push(path.clone());
        }
    }
    out.added.sort();
    out.modified.sort();
    out.removed.sort();
    out
}

/// Compute the Merkle root over the (lexically sorted) leaf hashes.
/// Returns `"0x"` for an empty container -- that signals "nothing here".
pub fn container_merkle_root_hex(map: &ContainerHashMap) -> String {
    let leaves: Vec<[u8; 32]> = map
        .values()
        .map(|leaf| {
            let mut buf = [0u8; 32];
            // We trust our own writers, but be defensive against truncated/bad hex.
            if let Ok(bytes) = hex::decode(&leaf.sha256) {
                if bytes.len() == 32 {
                    buf.copy_from_slice(&bytes);
                }
            }
            buf
        })
        .collect();
    if leaves.is_empty() {
        return "0x".into();
    }
    let tree = MerkleTree::<MerkleSha256>::from_leaves(&leaves);
    match tree.root() {
        Some(r) => format!("0x{}", hex::encode(r)),
        None => "0x".into(),
    }
}

/// Workspace-level root: hash of `(container_id || \0 || container_root_bytes)`
/// concatenated in container_id order, then SHA256.
pub fn workspace_merkle_root_hex(per_container: &BTreeMap<String, String>) -> String {
    if per_container.is_empty() {
        return "0x".into();
    }
    // Use rs_merkle here too for consistency: each container root (after
    // stripping the "0x" prefix and decoding) becomes a leaf.
    let leaves: Vec<[u8; 32]> = per_container
        .iter()
        .map(|(cid, root_hex)| {
            let stripped = root_hex.trim_start_matches("0x");
            let mut combined = Vec::with_capacity(cid.len() + 1 + 32);
            combined.extend_from_slice(cid.as_bytes());
            combined.push(0u8);
            if let Ok(bytes) = hex::decode(stripped) {
                combined.extend_from_slice(&bytes);
            }
            MerkleSha256::hash(&combined)
        })
        .collect();
    let tree = MerkleTree::<MerkleSha256>::from_leaves(&leaves);
    match tree.root() {
        Some(r) => format!("0x{}", hex::encode(r)),
        None => "0x".into(),
    }
}

pub fn save_hash_map(state_dir: &Path, container_id: &str, map: &ContainerHashMap) -> Result<()> {
    let dir = state_dir.join("file-hashes");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{container_id}.json"));
    let total_bytes: u64 = map.values().map(|l| l.size_bytes).sum();
    let persisted = PersistedHashMap {
        merkle_root_hex: container_merkle_root_hex(map),
        file_count: map.len() as u64,
        total_bytes,
        computed_at_ms: crate::util::now_ms(),
        leaves: map.clone(),
    };
    let bytes = serde_json::to_vec(&persisted)?;
    crate::util::write_atomic(path, &bytes)?;
    Ok(())
}

pub fn load_hash_map(state_dir: &Path, container_id: &str) -> Result<Option<PersistedHashMap>> {
    let path = state_dir.join("file-hashes").join(format!("{container_id}.json"));
    if !path.exists() {
        return Ok(None);
    }
    let bytes =
        std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    let v: PersistedHashMap = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing {}", path.display()))?;
    Ok(Some(v))
}

/// Helper for tombstoning: list the `rel_path -> leaf` of a previous hash map
/// so the caller can emit `removed` changelog entries. Returns an empty map
/// if there's no prior hash file.
pub fn previous_leaves_or_empty(
    state_dir: &Path,
    container_id: &str,
) -> ContainerHashMap {
    load_hash_map(state_dir, container_id)
        .ok()
        .flatten()
        .map(|p| p.leaves)
        .unwrap_or_default()
}

/// Wipe the persisted hash map for a container. Used after a tombstone.
pub fn delete_hash_map(state_dir: &Path, container_id: &str) -> Result<()> {
    let path = state_dir.join("file-hashes").join(format!("{container_id}.json"));
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("removing {}", path.display()))?;
    }
    Ok(())
}

/// Resolve the absolute path of a file in a container's `unzipped/` tree.
#[allow(dead_code)]
pub fn resolve_in_container(unzipped_dir: &Path, rel: &str) -> PathBuf {
    let mut p = unzipped_dir.to_path_buf();
    for part in rel.split('/').filter(|s| !s.is_empty()) {
        p.push(part);
    }
    p
}
