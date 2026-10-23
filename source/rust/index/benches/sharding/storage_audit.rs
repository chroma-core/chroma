//! Read-only accounting for immutable roots and their owned checkpoint files.
use anyhow::{ensure, Context, Result};
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, RootManager};
use chroma_cache::nop::NopCache;
use chroma_storage::{local::LocalStorage, Storage};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read,
    path::{Path, PathBuf},
    time::{Instant, UNIX_EPOCH},
};
use uuid::Uuid;

const ROOT_FIELDS: [&str; 5] = [
    "posting_list_id",
    "scalar_metadata_id",
    "vector_data_id",
    "leaf_node_id",
    "internal_node_id",
];
#[derive(Clone, Debug)]
pub struct Options {
    pub checkpoint_dir: PathBuf,
    pub retained_manifests: Vec<PathBuf>,
    pub expected_current_sha256: Option<String>,
    pub max_inventory_entries: usize,
    pub max_manifests: usize,
    pub max_root_bytes: u64,
    pub max_root_references: usize,
}
impl Options {
    pub fn new(checkpoint_dir: PathBuf) -> Self {
        Self {
            checkpoint_dir,
            retained_manifests: Vec::new(),
            expected_current_sha256: None,
            max_inventory_entries: 1_000_000,
            max_manifests: 10_000,
            max_root_bytes: 16 * 1024 * 1024,
            max_root_references: 1_000_000,
        }
    }
}
#[derive(Clone, Deserialize)]
struct Mapping {
    path: String,
    sha256: String,
    bytes: u64,
    count: u64,
}
#[derive(Clone, Deserialize)]
struct Manifest {
    generation: u64,
    shard_id: String,
    source_id: String,
    dimension: usize,
    metric: String,
    vector_count: u64,
    block_ids: BTreeMap<String, Uuid>,
    id_mapping: Option<Mapping>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct FileRecord {
    kind: String,
    bytes: u64,
    modified_ns: Option<u128>,
    device: Option<u64>,
    inode: Option<u64>,
    allocated_bytes: Option<u64>,
}
fn record(metadata: &fs::Metadata) -> FileRecord {
    let kind = if metadata.is_file() {
        "regular"
    } else if metadata.file_type().is_symlink() {
        "symlink_not_followed"
    } else {
        "other_not_read"
    };
    #[cfg(unix)]
    let (device, inode, allocated_bytes) = {
        use std::os::unix::fs::MetadataExt;
        (
            Some(metadata.dev()),
            Some(metadata.ino()),
            metadata.blocks().checked_mul(512),
        )
    };
    #[cfg(not(unix))]
    let (device, inode, allocated_bytes) = (None, None, None);
    FileRecord {
        kind: kind.into(),
        bytes: metadata.len(),
        modified_ns: metadata
            .modified()
            .ok()
            .and_then(|v| v.duration_since(UNIX_EPOCH).ok())
            .map(|v| v.as_nanos()),
        device,
        inode,
        allocated_bytes,
    }
}
fn inventory(directory: &Path, maximum: usize) -> Result<BTreeMap<String, FileRecord>> {
    fn visit(
        base: &Path,
        directory: &Path,
        depth: usize,
        count: &mut usize,
        maximum: usize,
        out: &mut BTreeMap<String, FileRecord>,
    ) -> Result<()> {
        ensure!(depth <= 32, "inventory directory-depth limit exceeded");
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            *count += 1;
            ensure!(*count <= maximum, "inventory entry limit exceeded");
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_dir() {
                visit(base, &path, depth + 1, count, maximum, out)?;
            } else {
                let name = path
                    .strip_prefix(base)?
                    .to_str()
                    .context("non-UTF8 checkpoint path")?
                    .to_string();
                out.insert(name, record(&metadata));
            }
        }
        Ok(())
    }
    let mut out = BTreeMap::new();
    visit(directory, directory, 0, &mut 0, maximum, &mut out)?;
    Ok(out)
}
fn hash_file(path: &Path) -> Result<String> {
    ensure!(
        fs::symlink_metadata(path)?.file_type().is_file(),
        "referenced file is not a regular file: {}",
        path.display()
    );
    let mut file = fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
fn object_kind(path: &str) -> Option<&'static str> {
    for (prefix, kind) in [("data/sparse_index/", "root"), ("data/block/", "block")] {
        if let Some(id) = path.strip_prefix(prefix) {
            if Uuid::parse_str(id).is_ok_and(|uuid| uuid.to_string() == id) {
                return Some(kind);
            }
        }
    }
    None
}
fn checked_sum(mut values: impl Iterator<Item = u64>) -> Result<u64> {
    values.try_fold(0u64, |sum, value| {
        sum.checked_add(value).context("file byte sum overflow")
    })
}
fn summary(
    paths: &BTreeSet<String>,
    files: &BTreeMap<String, FileRecord>,
) -> Result<serde_json::Value> {
    let mut unique = BTreeMap::new();
    let mut rows = Vec::new();
    for path in paths {
        let file = files
            .get(path)
            .with_context(|| format!("referenced object is missing: {path}"))?;
        ensure!(
            file.kind == "regular",
            "referenced object is not a regular file: {path}"
        );
        let key = match (file.device, file.inode) {
            (Some(d), Some(i)) => format!("{d}:{i}"),
            _ => path.clone(),
        };
        unique.entry(key).or_insert(file);
        rows.push(file);
    }
    let allocated = if unique.values().all(|f| f.allocated_bytes.is_some()) {
        Some(checked_sum(
            unique.values().map(|f| f.allocated_bytes.unwrap()),
        )?)
    } else {
        None
    };
    Ok(
        json!({"files":paths.len(),"logical_file_bytes":checked_sum(rows.iter().map(|f|f.bytes))?,
        "unique_inode_file_bytes":checked_sum(unique.values().map(|f|f.bytes))?,"allocated_bytes_unix":allocated,
        "inode_deduplication_available":unique.values().all(|f|f.inode.is_some())}),
    )
}
#[derive(Serialize)]
struct RootObject {
    path: String,
    sha256: String,
    bytes: u64,
    block_ids: Vec<Uuid>,
}

pub async fn audit(options: &Options) -> Result<serde_json::Value> {
    let started = Instant::now();
    let directory = options.checkpoint_dir.canonicalize()?;
    ensure!(
        !directory.join("build.lock").exists(),
        "active or stale build.lock prevents a stable storage audit"
    );
    ensure!(
        fs::symlink_metadata(directory.join("data"))?
            .file_type()
            .is_dir(),
        "data must be an owned directory, not a symlink"
    );
    let before = inventory(&directory, options.max_inventory_entries)?;
    let head = directory.join("manifest.json");
    let head_hash = hash_file(&head)?;
    ensure!(
        options
            .expected_current_sha256
            .as_ref()
            .is_none_or(|expected| expected == &head_hash),
        "current manifest checksum differs from the requested snapshot"
    );
    let current: Manifest = serde_json::from_reader(fs::File::open(&head)?)?;
    let immutable = directory.join(format!("manifest-{}.json", current.generation));
    ensure!(
        hash_file(&immutable)? == head_hash,
        "current head differs from its immutable generation manifest"
    );
    let mut manifest_paths = BTreeSet::new();
    for path in before.keys() {
        if let Some(generation) = path
            .strip_prefix("manifest-")
            .and_then(|p| p.strip_suffix(".json"))
        {
            if !generation.is_empty() && generation.bytes().all(|b| b.is_ascii_digit()) {
                manifest_paths.insert(directory.join(path));
            }
        }
    }
    for path in &options.retained_manifests {
        manifest_paths.insert(path.canonicalize()?);
    }
    ensure!(
        manifest_paths.len() <= options.max_manifests,
        "retained manifest limit exceeded"
    );
    let provider = ArrowBlockfileProvider::new(
        Storage::Local(LocalStorage::new(
            directory
                .join("data")
                .to_str()
                .context("non-UTF8 data path")?,
        )),
        3 * 1024 * 1024,
        Box::new(NopCache),
        Box::new(NopCache),
        16,
    );
    let mut generations: BTreeMap<u64, serde_json::Value> = BTreeMap::new();
    let mut roots: BTreeMap<String, RootObject> = BTreeMap::new();
    let mut retained_paths = BTreeSet::new();
    let mut current_paths = BTreeSet::new();
    let mut metadata_paths = BTreeSet::from(["manifest.json".into()]);
    let mut manifest_hashes = BTreeMap::new();
    let mut references = 0usize;
    let mut map_bytes_read = 0u64;
    for path in manifest_paths {
        let digest = hash_file(&path)?;
        manifest_hashes.insert(path.clone(), digest.clone());
        let manifest: Manifest = serde_json::from_reader(fs::File::open(&path)?)?;
        ensure!(
            manifest.shard_id == current.shard_id
                && manifest.source_id == current.source_id
                && manifest.dimension == current.dimension
                && manifest.metric == current.metric
                && manifest.id_mapping.is_some() == current.id_mapping.is_some(),
            "retained manifest belongs to another index identity"
        );
        ensure!(
            manifest.block_ids.len() == ROOT_FIELDS.len()
                && ROOT_FIELDS
                    .iter()
                    .all(|field| manifest.block_ids.contains_key(*field)),
            "manifest must name the five known blockfile roots"
        );
        if path.parent() == Some(directory.as_path()) {
            let name = path
                .file_name()
                .and_then(|v| v.to_str())
                .context("invalid manifest filename")?;
            if let Some(generation) = name
                .strip_prefix("manifest-")
                .and_then(|v| v.strip_suffix(".json"))
            {
                ensure!(
                    generation.parse::<u64>()? == manifest.generation,
                    "immutable manifest filename and generation differ"
                );
            }
        }
        if let Ok(relative) = path.strip_prefix(&directory) {
            metadata_paths.insert(relative.to_str().context("non-UTF8 manifest path")?.into());
        }
        if let Some(existing) = generations.get(&manifest.generation) {
            ensure!(
                existing["manifest_sha256"] == digest,
                "conflicting retained manifests for the same generation"
            );
            continue;
        }
        let mut mapping_binding = serde_json::Value::Null;
        if let Some(map) = &manifest.id_mapping {
            ensure!(
                Path::new(&map.path).components().count() == 1
                    && !Path::new(&map.path).is_absolute(),
                "retained map must be one owned filename"
            );
            ensure!(
                map.count == manifest.vector_count && Some(map.bytes) == map.count.checked_mul(8),
                "retained map count/byte binding differs"
            );
            let file = before.get(&map.path).context("retained map is missing")?;
            ensure!(
                file.kind == "regular" && file.bytes == map.bytes,
                "retained map file size differs"
            );
            ensure!(
                hash_file(&directory.join(&map.path))? == map.sha256,
                "retained map checksum differs"
            );
            map_bytes_read = map_bytes_read
                .checked_add(map.bytes)
                .context("map read-byte sum overflow")?;
            metadata_paths.insert(map.path.clone());
            mapping_binding =
                json!({"path":map.path,"sha256":map.sha256,"bytes":map.bytes,"count":map.count});
        }
        let mut generation_paths = BTreeSet::new();
        for root in manifest.block_ids.values() {
            let id = root.to_string();
            let root_path = format!("data/{}", RootManager::get_storage_key("", root));
            if !roots.contains_key(&id) {
                let file = before
                    .get(&root_path)
                    .with_context(|| format!("referenced root is missing: {root}"))?;
                ensure!(
                    file.kind == "regular" && file.bytes <= options.max_root_bytes,
                    "root file is nonregular or exceeds metadata-byte limit"
                );
                let sha = hash_file(&directory.join(&root_path))?;
                let blocks = provider
                    .get_all_block_ids(root, "")
                    .await
                    .map_err(|e| anyhow::anyhow!("read root {root}: {e}"))?;
                references = references
                    .checked_add(blocks.len())
                    .context("root-reference count overflow")?;
                ensure!(
                    references <= options.max_root_references,
                    "root-reference limit exceeded"
                );
                ensure!(
                    hash_file(&directory.join(&root_path))? == sha,
                    "root metadata changes during enumeration"
                );
                roots.insert(
                    id.clone(),
                    RootObject {
                        path: root_path.clone(),
                        sha256: sha,
                        bytes: file.bytes,
                        block_ids: blocks,
                    },
                );
            }
            generation_paths.insert(root_path);
            for block in &roots[&id].block_ids {
                generation_paths.insert(format!("data/block/{block}"));
            }
        }
        let bytes = summary(&generation_paths, &before)?;
        if manifest.generation == current.generation {
            ensure!(
                digest == head_hash,
                "current generation conflicts with head"
            );
            current_paths = generation_paths.clone();
        }
        retained_paths.extend(generation_paths);
        generations.insert(manifest.generation, json!({"generation":manifest.generation,"manifest_path":path,"manifest_sha256":digest,"vector_count":manifest.vector_count,"root_ids":manifest.block_ids,"id_mapping":mapping_binding,"live_objects":bytes}));
    }
    ensure!(
        generations.contains_key(&current.generation),
        "current generation absent from retained roots"
    );
    let regular: BTreeSet<_> = before
        .iter()
        .filter(|(_, f)| f.kind == "regular")
        .map(|(p, _)| p.clone())
        .collect();
    let store: BTreeSet<_> = regular
        .iter()
        .filter(|p| p.starts_with("data/"))
        .cloned()
        .collect();
    let recognized: BTreeSet<_> = regular
        .iter()
        .filter(|p| object_kind(p).is_some())
        .cloned()
        .collect();
    let not_referenced: BTreeSet<_> = recognized.difference(&retained_paths).cloned().collect();
    let unknown: BTreeMap<_, _> = before
        .iter()
        .filter(|(p, f)| {
            f.kind != "regular" || (object_kind(p).is_none() && !metadata_paths.contains(*p))
        })
        .map(|(p, f)| (p.clone(), f.clone()))
        .collect();
    ensure!(
        !directory.join("build.lock").exists(),
        "writer lock appears during storage audit"
    );
    ensure!(
        inventory(&directory, options.max_inventory_entries)? == before,
        "checkpoint files change during storage audit"
    );
    for (path, digest) in &manifest_hashes {
        ensure!(
            &hash_file(path)? == digest,
            "retained manifest changes during audit"
        );
    }
    ensure!(hash_file(&head)? == head_hash, "head changes during audit");
    Ok(
        json!({"diagnostic_version":1,"checkpoint_dir":directory,"current_generation":current.generation,"current_manifest_sha256":head_hash,
        "identity":{"shard_id":current.shard_id,"source_id":current.source_id,"dimension":current.dimension,"metric":current.metric},
        "retention_policy":"all immutable manifest-N.json files found plus explicit retained manifests; no deletion safety claim",
        "current_live_objects":summary(&current_paths,&before)?,"retained_union_live_objects":summary(&retained_paths,&before)?,
        "physical_object_store":summary(&store,&before)?,"total_owned_checkpoint_regular_files":summary(&regular,&before)?,
        "not_referenced_by_enumerated_retained_manifests":{"summary":summary(&not_referenced,&before)?,"paths":not_referenced,"classification":"not proof of safe deletion"},
        "unknown_paths":unknown,"retained_manifest_files":manifest_hashes,"generations":generations.values().collect::<Vec<_>>(),"root_objects":roots,"inventory":before,
        "limits":{"max_inventory_entries":options.max_inventory_entries,"max_manifests":options.max_manifests,"max_root_bytes":options.max_root_bytes,"max_root_references":options.max_root_references},
        "read_work":{"raw_block_payloads_read":false,"map_bytes_hashed":map_bytes_read,"unique_root_references":references},
        "no_cleanup":true,"elapsed_ms":started.elapsed().as_secs_f64()*1000.,
        "limitations":["A stable file inventory and absent writer lock are checked; this is not a transactional filesystem snapshot.","Raw block contents are not read or independently validated.","Allocated-byte sums do not deduplicate shared reflink extents; logical and unique-inode byte counts are separate.","Symlink targets and other nonregular files are not followed; unknown paths remain conservatively classified.","Staging outside this owned directory and peak build scratch are not measured."]}),
    )
}
