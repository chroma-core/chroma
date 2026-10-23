//! A bounded, read-only saved-index GT census. No query search or corpus vectors.
use crate::hierarchical_index::{
    config::HierarchicalSpannConfig,
    reader::{
        quality::{
            checked_gt, quality_oracles, GroundTruthQuery, OwnerGroundTruth, QualityLimits,
            QueryQuality,
        },
        HierarchicalSpannReader,
    },
    writer::HierarchicalSpannIds,
};
use crate::{id_map::IdMapping, provider_cache::ProviderCachePolicy};
use anyhow::{ensure, Context, Result};
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_distance::DistanceFunction;
use chroma_storage::{local::LocalStorage, Storage};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Owner {
    pub owner_id: String,
    pub generation: u64,
    pub snapshot: Artifact,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QuerySelection {
    pub query_id: String,
    pub expected: usize,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub format_version: u32,
    pub source_id: String,
    pub dimension: usize,
    pub metric: String,
    pub global_rows: u64,
    pub max_map_rows: u64,
    pub ground_truth: Artifact,
    pub query_vectors: Artifact,
    pub selected_queries: Vec<QuerySelection>,
    pub owners: Vec<Owner>,
}
#[derive(Deserialize)]
struct RootIds {
    posting_list_id: uuid::Uuid,
    scalar_metadata_id: uuid::Uuid,
    vector_data_id: uuid::Uuid,
    leaf_node_id: uuid::Uuid,
    internal_node_id: uuid::Uuid,
}
impl RootIds {
    fn ids(&self) -> HierarchicalSpannIds {
        HierarchicalSpannIds {
            posting_list_id: self.posting_list_id,
            scalar_metadata_id: self.scalar_metadata_id,
            vector_data_id: self.vector_data_id,
            leaf_node_id: self.leaf_node_id,
            internal_node_id: self.internal_node_id,
        }
    }
}
#[derive(Deserialize)]
struct Snapshot {
    generation: u64,
    source_id: String,
    shard_id: String,
    metric: String,
    dimension: usize,
    vector_count: u64,
    id_mapping: Option<IdMapping>,
    root_ref: serde_json::Value,
    block_ids: RootIds,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReferenceRow {
    query_id: String,
    ids: Vec<u64>,
    distances: Vec<f64>,
}
#[derive(Deserialize)]
struct QueryRow {
    query_id: String,
    vector: Vec<f32>,
}
#[derive(Debug, Serialize)]
pub struct OwnerProof {
    pub owner_id: String,
    pub generation: u64,
    pub snapshot_sha256: String,
    pub five_roots: serde_json::Value,
    pub map_sha256: String,
    pub map_rows: u64,
    pub map_bytes_read: u64,
    pub census: OwnerGroundTruth,
}
#[derive(Debug, Serialize)]
pub struct Report {
    pub policy: &'static str,
    pub spec_sha256: String,
    pub source_id: String,
    pub dimension: usize,
    pub metric: String,
    pub declared_global_rows: u64,
    pub ground_truth: Artifact,
    pub query_vectors: Artifact,
    pub gt_union_count: usize,
    pub queries: Vec<QueryQuality>,
    pub owners: Vec<OwnerProof>,
    pub scope: &'static str,
    pub raw_embedding_reads: u64,
}
fn sha(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}
fn valid_hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn consumed(path: &Path, cap: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?.take(cap + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= cap, "diagnostic input cap");
    Ok(bytes)
}
fn checked_artifact(a: &Artifact, cap: u64) -> Result<Vec<u8>> {
    ensure!(
        a.path.is_absolute() && a.bytes <= cap && valid_hash(&a.sha256),
        "artifact admission"
    );
    let bytes = consumed(&a.path, cap)?;
    ensure!(
        bytes.len() as u64 == a.bytes && sha(&bytes) == a.sha256,
        "consumed artifact length/hash differs"
    );
    Ok(bytes)
}
fn lines(a: &Artifact, mut visit: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
    ensure!(
        a.path.is_absolute() && a.bytes <= 16 * 1024 * 1024 && valid_hash(&a.sha256),
        "JSONL artifact cap"
    );
    let mut reader = BufReader::with_capacity(64 * 1024, File::open(&a.path)?);
    let mut hash = Sha256::new();
    let mut bytes = 0u64;
    loop {
        let mut line = Vec::new();
        let n = (&mut reader)
            .take(256 * 1024 + 1)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        ensure!(n <= 256 * 1024, "JSONL line cap");
        bytes = bytes.checked_add(n as u64).context("line count overflow")?;
        ensure!(bytes <= a.bytes, "JSONL grew");
        hash.update(&line);
        visit(&line)?;
    }
    ensure!(
        bytes == a.bytes && format!("{:x}", hash.finalize()) == a.sha256,
        "consumed JSONL identity differs"
    );
    Ok(())
}
fn inputs(spec: &Spec) -> Result<Vec<GroundTruthQuery>> {
    ensure!(
        spec.format_version == 1
            && !spec.source_id.is_empty()
            && spec.source_id.len() <= 256
            && spec.dimension > 0
            && spec.dimension <= 4096
            && ["l2", "ip", "cosine"].contains(&spec.metric.as_str())
            && spec.global_rows > 0
            && spec.global_rows <= spec.max_map_rows,
        "diagnostic shape/corpus cap"
    );
    ensure!(
        !spec.owners.is_empty()
            && spec.owners.len() <= 128
            && !spec.selected_queries.is_empty()
            && spec.selected_queries.len() <= 16,
        "owner/query cap"
    );
    let mut selected = BTreeMap::new();
    for q in &spec.selected_queries {
        ensure!(
            !q.query_id.is_empty()
                && q.query_id.len() <= 256
                && q.expected > 0
                && q.expected <= 100
                && selected.insert(q.query_id.clone(), q.expected).is_none(),
            "selected query identity/cardinality"
        );
    }
    let mut query_seen = BTreeSet::new();
    lines(&spec.query_vectors, |b| {
        let q: QueryRow = serde_json::from_slice(b)?;
        ensure!(
            q.vector.len() == spec.dimension && q.vector.iter().all(|v| v.is_finite()),
            "query dimensions/finite values"
        );
        if selected.contains_key(&q.query_id) {
            ensure!(
                query_seen.insert(q.query_id),
                "duplicate selected query vector"
            );
        }
        Ok(())
    })?;
    ensure!(
        query_seen.len() == selected.len(),
        "missing selected query vector"
    );
    let mut queries = BTreeMap::new();
    lines(&spec.ground_truth, |b| {
        let row: ReferenceRow = serde_json::from_slice(b)?;
        ensure!(
            row.ids.len() <= 100
                && row.ids.len() == row.distances.len()
                && row.distances.iter().all(|v| v.is_finite()),
            "GT record shape"
        );
        ensure!(
            row.distances.windows(2).all(|d| d[0] <= d[1])
                && row
                    .ids
                    .iter()
                    .zip(&row.distances)
                    .collect::<Vec<_>>()
                    .windows(2)
                    .all(|v| v[0].1 != v[1].1 || v[0].0 < v[1].0),
            "GT reference order/ties differ"
        );
        if let Some(&expected) = selected.get(&row.query_id) {
            let name = row.query_id.clone();
            ensure!(
                queries
                    .insert(
                        name,
                        GroundTruthQuery {
                            query_id: row.query_id,
                            expected,
                            ids: row.ids
                        }
                    )
                    .is_none(),
                "duplicate selected GT record"
            );
        }
        Ok(())
    })?;
    ensure!(
        queries.len() == selected.len(),
        "missing selected GT record"
    );
    let queries: Vec<_> = spec
        .selected_queries
        .iter()
        .map(|q| queries.remove(&q.query_id).unwrap())
        .collect();
    checked_gt(&queries, spec.global_rows).map_err(anyhow::Error::msg)?;
    Ok(queries)
}
struct PreparedOwner {
    owner: Owner,
    snapshot: Snapshot,
    map_hash: String,
    map_bytes: u64,
    targets: BTreeMap<u32, u64>,
}
fn prepare_maps(spec: &Spec, gt: &BTreeSet<u64>) -> Result<Vec<PreparedOwner>> {
    let mut names = BTreeSet::new();
    let mut mapped_gt = BTreeSet::new();
    let mut result = Vec::new();
    let mut total = 0u64;
    for owner in &spec.owners {
        ensure!(
            !owner.owner_id.is_empty()
                && owner.owner_id.len() <= 256
                && names.insert(owner.owner_id.clone()),
            "duplicate/invalid owner identity"
        );
        let snapshot: Snapshot =
            serde_json::from_slice(&checked_artifact(&owner.snapshot, 1024 * 1024)?)?;
        ensure!(
            snapshot.generation == owner.generation
                && snapshot.shard_id == owner.owner_id
                && snapshot.source_id == spec.source_id
                && snapshot.dimension == spec.dimension
                && snapshot.metric == spec.metric
                && snapshot.vector_count > 0
                && snapshot.vector_count <= u64::from(u32::MAX) + 1,
            "snapshot scope differs"
        );
        total = total
            .checked_add(snapshot.vector_count)
            .context("map total overflow")?;
        ensure!(total <= spec.global_rows, "owner count exceeds corpus");
        let map = snapshot
            .id_mapping
            .as_ref()
            .context("explicit local/global map required")?;
        ensure!(
            map.count == snapshot.vector_count
                && map.bytes == map.count.checked_mul(8).context("map length overflow")?
                && valid_hash(&map.sha256),
            "saved map descriptor differs"
        );
        let name = Path::new(&map.path);
        ensure!(
            !name.is_absolute()
                && name.components().count() == 1
                && !matches!(
                    name.components().next(),
                    Some(std::path::Component::ParentDir | std::path::Component::CurDir)
                ),
            "map must be one generation-local filename"
        );
        let mut file = BufReader::with_capacity(
            64 * 1024,
            File::open(
                owner
                    .snapshot
                    .path
                    .parent()
                    .context("snapshot parent")?
                    .join(name),
            )?,
        );
        let mut hash = Sha256::new();
        let mut targets = BTreeMap::new();
        for local in 0..map.count {
            let mut bytes = [0; 8];
            file.read_exact(&mut bytes)?;
            hash.update(bytes);
            let global = u64::from_le_bytes(bytes);
            ensure!(global < spec.global_rows, "map global ID outside corpus");
            if gt.contains(&global) {
                ensure!(
                    mapped_gt.insert(global),
                    "duplicate GT global ID in owner maps"
                );
                targets.insert(u32::try_from(local)?, global);
            }
        }
        let mut extra = [0];
        ensure!(file.read(&mut extra)? == 0, "map grew/extra rows");
        let map_hash = format!("{:x}", hash.finalize());
        ensure!(map_hash == map.sha256, "consumed map hash differs");
        let map_bytes = map.bytes;
        result.push(PreparedOwner {
            owner: owner.clone(),
            snapshot,
            map_hash,
            map_bytes,
            targets,
        });
    }
    ensure!(
        total == spec.global_rows && &mapped_gt == gt,
        "complete declared owner count/GT map coverage differs"
    );
    Ok(result)
}
pub async fn evaluate(path: &Path) -> Result<Report> {
    let spec_bytes = consumed(path, 1024 * 1024)?;
    let spec: Spec = serde_json::from_slice(&spec_bytes)?;
    let queries = inputs(&spec)?;
    let gt = checked_gt(&queries, spec.global_rows).map_err(anyhow::Error::msg)?;
    let prepared = prepare_maps(&spec, &gt)?;
    let distance = match spec.metric.as_str() {
        "l2" => DistanceFunction::Euclidean,
        "ip" => DistanceFunction::InnerProduct,
        _ => DistanceFunction::Cosine,
    };
    let mut proofs = Vec::new();
    for p in prepared {
        let data = p
            .owner
            .snapshot
            .path
            .parent()
            .context("snapshot parent")?
            .join("data");
        ensure!(data.is_dir(), "saved data store absent");
        let policy = ProviderCachePolicy::default();
        let provider = BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
            Storage::Local(LocalStorage::new(data.to_str().context("path encoding")?)),
            3 * 1024 * 1024,
            policy
                .blocks()
                .build_memory_persistent()
                .await
                .map_err(|e| anyhow::anyhow!("cache {e}"))?,
            policy
                .roots()
                .build_memory_persistent()
                .await
                .map_err(|e| anyhow::anyhow!("cache {e}"))?,
            2,
        ));
        let mut reader = HierarchicalSpannReader::open(
            &provider,
            p.snapshot.block_ids.ids(),
            distance.clone(),
            HierarchicalSpannConfig::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("saved open: {e}"))?;
        ensure!(
            reader.memory_usage().dim == spec.dimension,
            "actual saved dimension differs"
        );
        let root = reader
            .describe_root()
            .map_err(|e| anyhow::anyhow!("root {e}"))?;
        ensure!(
            serde_json::json!({"kind":root.kind,"id":root.id}) == p.snapshot.root_ref,
            "actual saved root differs"
        );
        let census = reader
            .ground_truth_memberships(
                &p.owner.owner_id,
                p.snapshot.vector_count,
                &p.targets,
                QualityLimits::default(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("saved GT census: {e}"))?;
        ensure!(
            reader.memory_usage().embedding_count == 0 && reader.loaded_postings_stats().0 == 0,
            "diagnostic unexpectedly retained corpus payload"
        );
        let ids = p.snapshot.block_ids;
        proofs.push(OwnerProof{owner_id:p.owner.owner_id,generation:p.owner.generation,snapshot_sha256:p.owner.snapshot.sha256,five_roots:serde_json::json!({"posting_list_id":ids.posting_list_id,"scalar_metadata_id":ids.scalar_metadata_id,"vector_data_id":ids.vector_data_id,"leaf_node_id":ids.leaf_node_id,"internal_node_id":ids.internal_node_id}),map_sha256:p.map_hash,map_rows:p.snapshot.vector_count,map_bytes_read:p.map_bytes,census});
    }
    let censuses: Vec<_> = proofs.iter().map(|p| p.census.clone()).collect();
    let oracles =
        quality_oracles(&queries, &censuses, spec.global_rows).map_err(anyhow::Error::msg)?;
    Ok(Report{policy:"saved-gt-quality-v1",spec_sha256:sha(&spec_bytes),source_id:spec.source_id,dimension:spec.dimension,metric:spec.metric,declared_global_rows:spec.global_rows,ground_truth:spec.ground_truth,query_vectors:spec.query_vectors,gt_union_count:gt.len(),queries:oracles,owners:proofs,scope:"Complete unique expected-GT membership only; non-GT global-map uniqueness/full-corpus coverage is not proved. Row weights count current physical copies, not block I/O, latency or executable navigation. Supplied GT/reference correctness and corpus pairing are upstream prerequisites; hashes bind their bytes. Inputs are fixed immutable POC inputs; no storage immutability enforcement.",raw_embedding_reads:0})
}
pub async fn run(path: &Path) -> Result<()> {
    let report = evaluate(path).await?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
