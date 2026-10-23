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
    let mut report_leaves = 0usize;
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
        report_leaves = report_leaves
            .checked_add(census.leaves.len())
            .context("leaf count overflow")?;
        ensure!(report_leaves <= 16384, "total numeric leaf report cap");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hierarchical_index::reader::quality::{
        checked_gt, quality_oracles, GroundTruthLeaf,
    };
    use chroma_cache::new_cache_for_test;
    use std::fs;
    fn hash(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }
    fn artifact(path: &Path) -> Artifact {
        let b = fs::read(path).unwrap();
        Artifact {
            path: path.to_path_buf(),
            bytes: b.len() as u64,
            sha256: hash(&b),
        }
    }
    fn output(name: &str) -> PathBuf {
        let root = PathBuf::from(
            std::env::var("SPANN_QUALITY_TEST_OUTPUT")
                .expect("explicit fresh evidence root required"),
        );
        let p = root.join(name);
        fs::create_dir(&p).unwrap();
        p
    }
    fn fixture() -> (serde_json::Value, Vec<Vec<f32>>) {
        let path = PathBuf::from(
            std::env::var("SPANN_QUALITY_BUNDLE").expect("retained98 bundle required"),
        );
        let bytes = fs::read(path).unwrap();
        assert_eq!(
            hash(&bytes),
            std::env::var("SPANN_QUALITY_BUNDLE_SHA256").unwrap()
        );
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let a = &v["deployment"]["canonical_vectors"];
        let bytes = fs::read(a["path"].as_str().unwrap()).unwrap();
        assert_eq!(hash(&bytes), a["sha256"].as_str().unwrap());
        assert_eq!(bytes.len(), 98 * 32 * 4);
        let vectors = bytes
            .chunks_exact(32 * 4)
            .map(|row| {
                row.chunks_exact(4)
                    .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
                    .collect()
            })
            .collect();
        (v, vectors)
    }
    fn provider(path: &Path) -> BlockfileProvider {
        BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
            Storage::Local(LocalStorage::new(path.to_str().unwrap())),
            3 * 1024 * 1024,
            new_cache_for_test(),
            new_cache_for_test(),
            2,
        ))
    }
    fn gt(vectors: &[Vec<f32>], query: &[f32], k: usize) -> (Vec<u64>, Vec<f64>) {
        let mut rows: Vec<_> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| {
                (
                    i as u64,
                    v.iter()
                        .zip(query)
                        .map(|(&x, &y)| (f64::from(x) - f64::from(y)).powi(2))
                        .sum::<f64>(),
                )
            })
            .collect();
        rows.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        rows.truncate(k);
        rows.into_iter().unzip()
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quality_saved98_maps_memberships_and_exact_set_oracles() {
        let dir = output("saved98");
        let (bundle, vectors) = fixture();
        let tree = &bundle["deployment"]["tree"];
        let mut reference = Vec::new();
        let mut query_rows = Vec::new();
        let mut queries = Vec::new();
        for (i, x) in [0.25f32, 47.25, 73.5].into_iter().enumerate() {
            let mut q = vec![0.; 32];
            q[0] = x;
            q[1] = 1.;
            q[2] = -0.;
            let (ids, distances) = gt(&vectors, &q, 20);
            let name = format!("q{i}");
            reference.push(serde_json::json!({"query_id":name,"ids":ids,"distances":distances}));
            query_rows.push(serde_json::json!({"query_id":name,"vector":q}));
            queries.push(GroundTruthQuery {
                query_id: name,
                expected: 20,
                ids,
            });
        }
        let (ids, distances) = gt(&vectors, &vectors[0], 98);
        reference.push(serde_json::json!({"query_id":"all98","ids":ids,"distances":distances}));
        query_rows.push(serde_json::json!({"query_id":"all98","vector":vectors[0]}));
        queries.push(GroundTruthQuery {
            query_id: "all98".into(),
            expected: 98,
            ids,
        });
        let gt_path = dir.join("gt.jsonl");
        let q_path = dir.join("queries.jsonl");
        fs::write(
            &gt_path,
            reference
                .iter()
                .map(|v| v.to_string() + "\n")
                .collect::<String>(),
        )
        .unwrap();
        fs::write(
            &q_path,
            query_rows
                .iter()
                .map(|v| v.to_string() + "\n")
                .collect::<String>(),
        )
        .unwrap();
        let owners: Vec<_> = tree["subtrees"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| Owner {
                owner_id: s["subtree_id"].as_str().unwrap().into(),
                generation: s["generation"].as_u64().unwrap(),
                snapshot: serde_json::from_value(s["snapshot"].clone()).unwrap(),
            })
            .collect();
        let spec = Spec {
            format_version: 1,
            source_id: tree["source"]["source_id"].as_str().unwrap().into(),
            dimension: 32,
            metric: "l2".into(),
            global_rows: 98,
            max_map_rows: 98,
            ground_truth: artifact(&gt_path),
            query_vectors: artifact(&q_path),
            selected_queries: queries
                .iter()
                .map(|q| QuerySelection {
                    query_id: q.query_id.clone(),
                    expected: q.expected,
                })
                .collect(),
            owners,
        };
        let spec_path = dir.join("spec.json");
        fs::write(&spec_path, serde_json::to_vec_pretty(&spec).unwrap()).unwrap();
        let report = evaluate(&spec_path).await.unwrap();
        assert_eq!(report.gt_union_count, 98);
        assert_eq!(report.raw_embedding_reads, 0);
        assert_eq!(report.owners.len(), 2);
        let mut independent = Vec::<(String, u32, BTreeSet<u64>, u64, u64)>::new();
        // A serial saved-reader set oracle is independent of the diagnostic's
        // batched collection and weighted dynamic program. It is only98 rows.
        for owner in &report.owners {
            let slot = tree["subtrees"]
                .as_array()
                .unwrap()
                .iter()
                .find(|s| s["subtree_id"] == owner.owner_id)
                .unwrap();
            let snapshot: serde_json::Value = serde_json::from_slice(
                &fs::read(slot["snapshot"]["path"].as_str().unwrap()).unwrap(),
            )
            .unwrap();
            let root_path = Path::new(slot["snapshot"]["path"].as_str().unwrap())
                .parent()
                .unwrap();
            let saved_map: IdMapping =
                serde_json::from_value(snapshot["id_mapping"].clone()).unwrap();
            let map_bytes = fs::read(root_path.join(saved_map.path)).unwrap();
            let mapping: Vec<_> = map_bytes
                .chunks_exact(8)
                .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                .collect();
            let b = &snapshot["block_ids"];
            let ids = crate::hierarchical_index::writer::HierarchicalSpannIds {
                posting_list_id: b["posting_list_id"].as_str().unwrap().parse().unwrap(),
                scalar_metadata_id: b["scalar_metadata_id"].as_str().unwrap().parse().unwrap(),
                vector_data_id: b["vector_data_id"].as_str().unwrap().parse().unwrap(),
                leaf_node_id: b["leaf_node_id"].as_str().unwrap().parse().unwrap(),
                internal_node_id: b["internal_node_id"].as_str().unwrap().parse().unwrap(),
            };
            let p = provider(&root_path.join("data"));
            let mut reader = HierarchicalSpannReader::open(
                &p,
                ids,
                DistanceFunction::Euclidean,
                HierarchicalSpannConfig::default(),
            )
            .await
            .unwrap();
            let (rows, error) = reader.quality_serial_fixture_oracle(&mapping).await;
            for (leaf, valid, live, physical) in rows {
                independent.push((owner.owner_id.clone(), leaf, valid, live, physical));
            }
            fs::write(
                dir.join(format!("{}-unreachable-error.txt", owner.owner_id)),
                error,
            )
            .unwrap();
        }
        for owner in &report.owners {
            for leaf in &owner.census.leaves {
                let oracle = independent
                    .iter()
                    .find(|(name, id, _, _, _)| name == &owner.owner_id && *id == leaf.leaf_id)
                    .unwrap();
                assert_eq!(
                    leaf.gt_ids.iter().copied().collect::<BTreeSet<_>>(),
                    oracle.2
                );
                assert_eq!((leaf.live_rows, leaf.physical_rows), (oracle.3, oracle.4));
            }
        }
        for (query, actual) in queries.iter().zip(&report.queries) {
            let expected: BTreeSet<_> = query.ids.iter().copied().collect();
            let leaves: Vec<_> = independent
                .iter()
                .filter(|(_, _, s, _, _)| !s.is_disjoint(&expected))
                .collect();
            assert_eq!(actual.covered, query.expected);
            assert_eq!(actual.gt_leaf_count, leaves.len());
            if query.expected == 98 {
                assert_eq!(actual.oracles[2].minimum_leaves, leaves.len());
                assert_eq!(
                    actual.oracles[2].minimum_live_rows,
                    leaves.iter().map(|l| l.3).sum::<u64>()
                );
                continue;
            }
            assert!(leaves.len() <= 20);
            for oracle in &actual.oracles {
                let mut best_count = usize::MAX;
                let mut best_rows = u64::MAX;
                for mask in 0usize..(1 << leaves.len()) {
                    let mut hits = BTreeSet::new();
                    let mut rows = 0;
                    let mut count = 0;
                    for (i, leaf) in leaves.iter().enumerate() {
                        if mask & (1 << i) != 0 {
                            hits.extend(leaf.2.intersection(&expected).copied());
                            rows += leaf.3;
                            count += 1;
                        }
                    }
                    if hits.len() >= oracle.target_hits {
                        best_count = best_count.min(count);
                        best_rows = best_rows.min(rows);
                    }
                }
                assert_eq!(
                    (oracle.minimum_leaves, oracle.minimum_live_rows),
                    (best_count, best_rows)
                );
            }
        }
        let mut duplicate: Vec<_> = report.owners.iter().map(|o| o.census.clone()).collect();
        let first = duplicate[0].leaves[0].gt_ids[0];
        duplicate[1].leaves[0].gt_ids[0] = first;
        assert!(quality_oracles(&queries, &duplicate, 98)
            .unwrap_err()
            .contains("duplicate"));
        let mut missing: Vec<_> = report.owners.iter().map(|o| o.census.clone()).collect();
        missing[0].expected_gt -= 1;
        missing[0].leaves[0].gt_ids.remove(0);
        missing[0].leaves.retain(|l| !l.gt_ids.is_empty());
        assert!(quality_oracles(&queries, &missing, 98)
            .unwrap_err()
            .contains("incomplete"));
        let mut bad = serde_json::to_value(&spec).unwrap();
        bad["global_rows"] = true.into();
        assert!(serde_json::from_value::<Spec>(bad).is_err());
        for scenario in ["duplicate-map", "wrong-map-hash"] {
            let case = dir.join(scenario);
            fs::create_dir(&case).unwrap();
            let mut changed = spec.clone();
            let original = &spec.owners[0];
            let mut snapshot: serde_json::Value =
                serde_json::from_slice(&fs::read(&original.snapshot.path).unwrap()).unwrap();
            let map: IdMapping = serde_json::from_value(snapshot["id_mapping"].clone()).unwrap();
            let mut bytes =
                fs::read(original.snapshot.path.parent().unwrap().join(&map.path)).unwrap();
            if scenario == "duplicate-map" {
                let first = bytes[..8].to_vec();
                bytes[8..16].copy_from_slice(&first);
            }
            fs::write(case.join(&map.path), &bytes).unwrap();
            snapshot["id_mapping"]["sha256"] = if scenario == "wrong-map-hash" {
                "0".repeat(64)
            } else {
                hash(&bytes)
            }
            .into();
            let path = case.join("manifest.json");
            fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
            changed.owners[0].snapshot = artifact(&path);
            let path = case.join("spec.json");
            fs::write(&path, serde_json::to_vec(&changed).unwrap()).unwrap();
            let error = evaluate(&path).await.unwrap_err().to_string();
            assert!(
                error.contains(if scenario == "duplicate-map" {
                    "duplicate GT global ID"
                } else {
                    "consumed map hash"
                }),
                "{error}"
            );
            fs::write(case.join("expected-error.txt"), error).unwrap();
        }
        fs::write(
            dir.join("report.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("independent-leaf-sets.json"),
            serde_json::to_vec_pretty(&independent).unwrap(),
        )
        .unwrap();
    }
}
