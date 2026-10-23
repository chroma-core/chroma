//! Independent, checkpointed hierarchical indexes for shard experiments.
#![recursion_limit = "256"]
#![allow(dead_code)]
mod hierarchical_index;
#[path = "sharding/dynamic_construct.rs"]
mod dynamic_construct;
#[path = "sharding/build_speed_diagnostic.rs"]
mod build_speed_diagnostic;
#[path="sharding/reference_owner_diagnostic.rs"]
mod reference_owner_diagnostic;
#[path = "sharding/quality_diagnostic.rs"]
mod quality_diagnostic;
#[path = "sharding/id_map.rs"]
mod id_map;
#[path = "sharding/write_plan.rs"]
mod write_plan;
#[path = "sharding/selected_write.rs"]
mod selected_write;
#[path = "sharding/selected_write_protocol.rs"]
mod selected_write_protocol;
#[path = "sharding/provider_cache.rs"]
mod provider_cache;
#[path = "sharding/canonical_view.rs"]
mod canonical_view;
#[path = "sharding/poc_audit.rs"]
mod poc_audit;
#[path = "sharding/poc_lazy_write.rs"]
mod poc_lazy_write;
#[path = "sharding/poc_copy.rs"]
mod poc_copy;
#[path = "sharding/save_layout.rs"]
mod save_layout;
use anyhow::{bail, ensure, Context, Result};
use chroma_blockstore::provider::BlockfileProvider;
use chroma_distance::DistanceFunction;
use clap::{Parser, Subcommand};
use hierarchical_index::{
    config::HierarchicalSpannConfig,
    reader::HierarchicalSpannReader,
    writer::{HierarchicalSpannIds, HierarchicalSpannWriter},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Bounded reference-kernel parallel routing/balance with owner-authoritative data.
    ReferenceOwnerDiagnostic { #[arg(long)] spec:PathBuf },
    /// Bounded construction attribution; source prefix and explicit stage timings.
    BuildSpeedDiagnostic { #[arg(long)] spec: PathBuf },
    /// Build an explicit FP reference or owner-authoritative global tree.
    ConstructGlobal { #[arg(long)] spec: PathBuf },
    /// Read saved GT memberships and exact disjoint row-cost oracles.
    DiagnoseGroundTruth { #[arg(long)] spec:PathBuf },
    /// Fixed-input POC append; canonical spans and only selected-owner copies.
    PocLazyGlobalWrites { #[arg(long)] spec: PathBuf },
    /// Read-only semantic validation under the explicit fixed-input POC assumption.
    PocAuditSnapshot { #[arg(long)] spec: PathBuf },
    /// Commit selected global writes only into admitted private checkpoint clones.
    CompactGlobalWrites {
        #[arg(long)]
        spec: PathBuf,
    },
    /// Serve one bounded, exclusive selected-leaf writer over JSONL.
    SelectedWriteSession {
        #[arg(long)]
        spec: PathBuf,
    },
    Build {
        #[arg(long)]
        vectors: PathBuf,
        #[arg(long)]
        generation: u64,
        #[arg(long)]
        source_id: String,
        #[arg(long, default_value_t = 100)]
        branching_factor: usize,
        #[arg(long, default_value_t = 2048)]
        split_threshold: usize,
        #[arg(long, default_value_t = 512)]
        merge_threshold: usize,
        #[arg(long, default_value_t = 1)]
        threads: usize,
        #[arg(long)]
        dimension: usize,
        #[arg(long, default_value_t = 0)]
        global_id_start: u64,
        #[arg(long)]
        id_map: Option<PathBuf>,
        #[arg(long, default_value = "")]
        boundary_hash: String,
        #[arg(long, default_value_t = 1)]
        id_stride: u64,
        /// Number of rows in this input file; resume appends this file.
        #[arg(long)]
        row_count: u64,
        /// Benchmark fixture split target; defaults preserve the normal builder.
        #[arg(long)]
        fixture_block_target_bytes: Option<usize>,
        #[arg(long)]
        checkpoint_dir: PathBuf,
        #[arg(long)]
        shard_id: String,
        #[arg(long, default_value = "l2")]
        metric: String,
        #[arg(long)]
        resume: bool,
        #[arg(long, default_value_t = 10000)]
        checkpoint_rows: u64,
        #[arg(long, default_value_t = 268_435_456)]
        audit_max_bitmap_bytes: u64,
        #[arg(long, default_value_t = 1_000_000)]
        audit_max_nodes: usize,
        #[arg(long, default_value_t = 262_144)]
        audit_max_leaf_entries: usize,
    },
    Read {
        #[arg(long)]
        checkpoint_dir: PathBuf,
        #[arg(long)]
        generation: Option<u64>,
        /// Return local IDs for a coordinator with a pinned ownership mapper.
        #[arg(long)]
        local_ids_only: bool,
        /// Release owned posting arrays after each operation; provider cache is unchanged.
        #[arg(long)]
        evict_postings_after_operation: bool,
        /// JSON provider-wide soft cache targets and optional operation-boundary clearing.
        #[arg(long)]
        provider_cache_policy: Option<String>,
        /// Observe reader-held vector-block references; excludes provider/RSS.
        #[arg(long)]
        profile_rerank_pins: bool,
        /// Report actual ordinary-query scoring and exact-rerank cardinalities.
        #[arg(long)]
        profile_query_work: bool,
        /// Bounded actual leaf and candidate IDs for outside-query GT attribution.
        #[arg(long)]
        trace_query_stages: bool,
        /// Immutable diagnostic residual Code4 sidecar; original saved tree remains unchanged.
        #[arg(long)]
        posting_code4_overlay_dir:Option<PathBuf>,
        /// Explicit diagnostic centroid scorer; omission preserves one-bit navigation.
        #[arg(long,value_parser=["one-bit","fp","four-bit"])]
        query_navigation:Option<String>,
        /// Opt-in serial exact reranking with release after every owned score.
        #[arg(long)]
        release_rerank_vectors: bool,
        /// Report selected-leaf scoring/selection stages without changing candidates.
        #[arg(long)]
        profile_posting_selection: bool,
        /// Retain inclusive cutoff ties using partition before sorting.
        #[arg(long)]
        partition_posting_candidates: bool,
        /// Explicit paired scorer policy; runtime requires hardware detection.
        #[arg(long, value_parser=["portable","runtime"])]
        posting_scorer_policy: Option<String>,
        /// Bounded request-local posting preparation diagnostics; requires selection profile.
        #[arg(long)]
        profile_posting_preparation: bool,
    },
}
#[derive(Serialize, Deserialize)]
struct Manifest {
    generation: u64,
    source_id: String,
    shard_id: String,
    metric: String,
    dimension: usize,
    vector_count: u64,
    global_id_start: u64,
    global_id_end: u64,
    id_offset: u64,
    id_stride: u64,
    #[serde(default)]
    id_mapping: Option<id_map::IdMapping>,
    #[serde(default)]
    routing_boundary_hash: String,
    #[serde(default)]
    root_ref: Option<serde_json::Value>,
    #[serde(default)]
    integrity_audit: Option<serde_json::Value>,
    construction: serde_json::Value,
    block_ids: BlockIds,
}
#[derive(Serialize, Deserialize)]
struct BlockIds {
    posting_list_id: String,
    scalar_metadata_id: String,
    vector_data_id: String,
    leaf_node_id: String,
    internal_node_id: String,
}
impl BlockIds {
    fn from_ids(i: &HierarchicalSpannIds) -> Self {
        Self {
            posting_list_id: i.posting_list_id.to_string(),
            scalar_metadata_id: i.scalar_metadata_id.to_string(),
            vector_data_id: i.vector_data_id.to_string(),
            leaf_node_id: i.leaf_node_id.to_string(),
            internal_node_id: i.internal_node_id.to_string(),
        }
    }
    fn ids(&self) -> Result<HierarchicalSpannIds> {
        Ok(HierarchicalSpannIds {
            posting_list_id: self.posting_list_id.parse()?,
            scalar_metadata_id: self.scalar_metadata_id.parse()?,
            vector_data_id: self.vector_data_id.parse()?,
            leaf_node_id: self.leaf_node_id.parse()?,
            internal_node_id: self.internal_node_id.parse()?,
        })
    }
}
fn construction() -> serde_json::Value {
    serde_json::json!({"version":1,"branching_factor":100,"split_threshold":2048,"merge_threshold":512,"write_beam_tau":1.5,"write_beam_min":10,"write_beam_max":16,"write_level_taus":[],"write_level_min_pcts":[],"max_replicas":1,"write_rng_epsilon":0.0,"write_rng_factor":1.0,"reassign_neighbor_count":32,"write_navigation":"fp","fp_npa":true})
}
fn metric(s: &str) -> Result<DistanceFunction> {
    match s {
        "l2" => Ok(DistanceFunction::Euclidean),
        "cosine" => Ok(DistanceFunction::Cosine),
        "ip" => Ok(DistanceFunction::InnerProduct),
        _ => bail!("metric must be l2, cosine, or ip"),
    }
}
fn load(dir: &Path) -> Result<Manifest> {
    Ok(serde_json::from_reader(File::open(
        dir.join("manifest.json"),
    )?)?)
}
fn publish(dir: &Path, m: &Manifest) -> Result<()> {
    let tmp = dir.join("manifest.json.tmp");
    let mut file = File::create(&tmp)?;
    serde_json::to_writer_pretty(&mut file, m)?;
    file.sync_all()?;
    let generation_path = dir.join(format!("manifest-{}.json", m.generation));
    fs::hard_link(&tmp, generation_path)?;
    fs::rename(tmp, dir.join("manifest.json"))?;
    File::open(dir)?.sync_all()?;
    Ok(())
}
async fn provider(dir: &Path) -> Result<BlockfileProvider> {
    provider_cache::provider(dir, provider_cache::ProviderCachePolicy::default()).await
}
// One writer owns publication; a crash leaves a lock requiring explicit operator cleanup.
struct BuildLock(PathBuf);
impl Drop for BuildLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    match Args::parse().command {
        Command::ReferenceOwnerDiagnostic { spec } => reference_owner_diagnostic::run(&spec).await?,
        Command::BuildSpeedDiagnostic { spec } => build_speed_diagnostic::run(&spec).await?,
        Command::ConstructGlobal { spec } => dynamic_construct::run(&spec).await?,
        Command::PocLazyGlobalWrites { spec } => poc_lazy_write::run(&spec).await?,
        Command::DiagnoseGroundTruth { spec } => quality_diagnostic::run(&spec).await?,
        Command::PocAuditSnapshot { spec } => poc_audit::run(&spec).await?,
        Command::CompactGlobalWrites { spec } => write_plan::run(&spec).await?,
        Command::SelectedWriteSession { spec } => selected_write::run(&spec).await?,
        Command::Build {
            vectors,
            generation,
            source_id,
            branching_factor,
            split_threshold,
            merge_threshold,
            threads,
            dimension,
            global_id_start,
            id_map,
            boundary_hash,
            row_count,
            fixture_block_target_bytes,
            checkpoint_dir: dir,
            shard_id,
            metric: metric_name,
            resume,
            checkpoint_rows,
            audit_max_bitmap_bytes,
            audit_max_nodes,
            audit_max_leaf_entries,
            id_stride,
        } => {
            ensure!(
                dimension > 0 && checkpoint_rows > 0 && row_count > 0 && id_stride > 0,
                "dimension, row-count and checkpoint-rows must be positive"
            );
            ensure!(
                threads > 0
                    && branching_factor >= 2
                    && split_threshold >= 2
                    && merge_threshold < split_threshold,
                "invalid construction settings"
            );
            let config = HierarchicalSpannConfig {
                branching_factor,
                split_threshold,
                merge_threshold,
                ..Default::default()
            };
            let mut construction_config = construction();
            construction_config["branching_factor"] = branching_factor.into();
            construction_config["split_threshold"] = split_threshold.into();
            construction_config["merge_threshold"] = merge_threshold.into();
            let distance = metric(&metric_name)?;
            let bytes = row_count
                .checked_mul(dimension as u64)
                .and_then(|v| v.checked_mul(4))
                .context("input size overflow")?;
            let file = File::open(vectors)?;
            ensure!(
                file.metadata()?.len() == bytes,
                "input length differs from row-count * dimension * 4"
            );
            fs::create_dir_all(&dir)?;
            let lock_path = dir.join("build.lock");
            let _lock_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
                .context("another builder or stale build.lock exists")?;
            let _lock = BuildLock(lock_path);
            let old = if resume {
                Some(load(&dir)?)
            } else {
                ensure!(
                    !dir.join("manifest.json").exists(),
                    "checkpoint exists; use --resume"
                );
                None
            };
            let start_count = old.as_ref().map_or(0, |m| m.vector_count);
            let total = start_count
                .checked_add(row_count)
                .context("count overflow")?;
            ensure!(total <= u32::MAX as u64, "local IDs exceed u32 capacity");
            ensure!(
                audit_max_nodes > 0
                    && audit_max_leaf_entries > 0
                    && (total + 7) / 8 <= audit_max_bitmap_bytes,
                "audit limits cannot cover the requested snapshot"
            );
            global_id_start
                .checked_add(
                    row_count
                        .checked_mul(id_stride)
                        .context("global ID overflow")?,
                )
                .context("global ID overflow")?;
            if let Some(m) = &old {
                ensure!(
                    m.shard_id == shard_id
                        && m.dimension == dimension
                        && m.metric == metric_name
                        && m.construction == construction_config
                        && m.source_id == source_id
                        && m.routing_boundary_hash == boundary_hash,
                    "incompatible resume configuration"
                );
                ensure!(
                    (m.id_mapping.is_some() || m.global_id_end == global_id_start)
                        && m.id_stride == id_stride,
                    "resume IDs must append exactly at global_id_end"
                );
            }
            let base = old.as_ref().map_or(global_id_start, |m| m.global_id_start);
            ensure!(
                old.as_ref().is_none_or(|m| generation > m.generation),
                "generation must advance"
            );
            ensure!(
                !dir.join(format!("manifest-{generation}.json")).exists(),
                "generation already exists"
            );
            let mut prepared_map = id_map::prepare(
                id_map.as_deref(),
                old.as_ref().and_then(|m| m.id_mapping.as_ref()),
                start_count,
                total,
                &dir,
                generation,
            )?;
            ensure!(fixture_block_target_bytes.is_none() || !resume, "fixture block target requires a fresh build");
            let p = match fixture_block_target_bytes {
                Some(target) => provider_cache::provider_with_block_target(&dir, provider_cache::ProviderCachePolicy::default(), target).await?,
                None => provider(&dir).await?,
            };
            let mut ids = old.as_ref().map(|m| m.block_ids.ids()).transpose()?;
            let mut writer = if let Some(i) = &ids {
                HierarchicalSpannWriter::open(&p, i.clone(), distance.clone(), config.clone())
                    .await
                    .map_err(|e| anyhow::anyhow!("open: {e}"))?
            } else {
                HierarchicalSpannWriter::new(dimension, distance.clone(), config.clone())
            };
            let mut input = BufReader::new(file);
            let mut raw = vec![0u8; dimension * 4];
            for row in 0..row_count {
                input.read_exact(&mut raw)?;
                let mut vector: Vec<f32> = raw
                    .chunks_exact(4)
                    .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
                    .collect();
                ensure!(
                    vector.iter().all(|x| x.is_finite()),
                    "non-finite input at row {row}"
                );
                if metric_name == "cosine" {
                    normalize(&mut vector)?;
                }
                writer.add((start_count + row) as u32, &vector);
                if (row + 1) % checkpoint_rows == 0 || row + 1 == row_count {
                    writer.balance_index_parallel(threads);
                    let flushed = writer
                        .commit(&p, ids.as_ref())
                        .await
                        .map_err(|e| anyhow::anyhow!("commit: {e}"))?
                        .flush()
                        .await
                        .map_err(|e| anyhow::anyhow!("flush: {e}"))?;
                    let count = start_count + row + 1;
                    drop(writer);
                    writer = HierarchicalSpannWriter::open(
                        &p,
                        flushed.clone(),
                        distance.clone(),
                        config.clone(),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("reopen: {e}"))?;
                    if row + 1 == row_count {
                        ensure!(
                            writer.num_nodes() <= audit_max_nodes,
                            "audit metadata node limit exceeded before opening validation reader"
                        );
                        let root_reader = HierarchicalSpannReader::open(
                            &p,
                            flushed.clone(),
                            distance.clone(),
                            config.clone(),
                        )
                        .await
                        .map_err(|e| anyhow::anyhow!("verify root: {e}"))?;
                        let root = root_reader
                            .describe_root()
                            .map_err(|e| anyhow::anyhow!("root descriptor: {e}"))?;
                        let audit_started = Instant::now();
                        let audit = root_reader
                            .audit_reachable_ids(
                                count,
                                hierarchical_index::reader::AuditLimits {
                                    max_bitmap_bytes: audit_max_bitmap_bytes,
                                    max_nodes: audit_max_nodes,
                                    max_leaf_entries: audit_max_leaf_entries,
                                },
                            )
                            .await
                            .map_err(|e| anyhow::anyhow!("full integrity audit: {e}"))?;
                        let audit_elapsed_ms = audit_started.elapsed().as_secs_f64() * 1000.0;
                        let audit_binding = serde_json::json!({"generation":generation,"block_ids":BlockIds::from_ids(&flushed),"id_map_sha256":prepared_map.as_ref().map(|map|map.descriptor.sha256.clone()),"elapsed_ms":audit_elapsed_ms,"coverage":audit});
                        eprintln!("full integrity audit generation {generation}: {count} rows in {audit_elapsed_ms:.3} ms");
                        let mapping = prepared_map.take().map(|map| map.publish()).transpose()?;
                        publish(
                            &dir,
                            &Manifest {
                                generation,
                                source_id: source_id.clone(),
                                shard_id: shard_id.clone(),
                                metric: metric_name.clone(),
                                dimension,
                                vector_count: count,
                                global_id_start: base,
                                global_id_end: base + count * id_stride,
                                id_offset: base,
                                id_stride,
                                id_mapping: mapping,
                                routing_boundary_hash: boundary_hash.clone(),
                                root_ref: Some(serde_json::json!({"kind":root.kind,"id":root.id})),
                                integrity_audit: Some(audit_binding),
                                construction: construction_config.clone(),
                                block_ids: BlockIds::from_ids(&flushed),
                            },
                        )?;
                    }
                    eprintln!("committed build batch for generation {generation}: {count} vectors");
                    ids = Some(flushed);
                }
            }
        }
        Command::Read {
            checkpoint_dir: dir,
            generation,
            local_ids_only,
            evict_postings_after_operation,
            provider_cache_policy,
            profile_rerank_pins,
            profile_query_work,
            trace_query_stages,
            posting_code4_overlay_dir,
            query_navigation,
            release_rerank_vectors,
            profile_posting_selection,
            partition_posting_candidates,
            posting_scorer_policy,
            profile_posting_preparation,
        } => {
            ensure!(query_navigation.is_none() || profile_query_work,"explicit query navigation requires work profile");
            ensure!(!trace_query_stages || profile_query_work,"query stage trace requires query work profile");
            ensure!(!profile_posting_preparation || profile_posting_selection,"preparation observation requires selection profile");
            ensure!(!partition_posting_candidates || profile_posting_selection,"partition policy requires explicit profile");
            ensure!(posting_scorer_policy.is_none() || profile_posting_selection,"scorer policy requires explicit selected-leaf profile");
            let posting_scorer=posting_scorer_policy.as_deref().map(|name|chroma_index::quantization::OneBitScorer::new(if name=="runtime"{chroma_index::quantization::OneBitScorerPolicy::Runtime}else{chroma_index::quantization::OneBitScorerPolicy::Portable}));
            let cache_policy=provider_cache_policy.as_deref().map(serde_json::from_str::<provider_cache::ProviderCachePolicy>).transpose()?;
            if let Some(policy)=cache_policy {policy.validate()?;}
            let m: Manifest = if let Some(g) = generation {
                serde_json::from_reader(File::open(dir.join(format!("manifest-{g}.json")))?)?
            } else {
                load(&dir)?
            };
            ensure!(!matches!(query_navigation.as_deref(),Some("fp"|"four-bit")) || m.vector_count<=16384,"FP navigation snapshot cap");
            ensure!(!trace_query_stages || m.vector_count<=16384,"query stage trace snapshot cap");
            ensure!(!local_ids_only || m.vector_count <= 100_000,"local-ID-only mode retains the 100k correctness cap");
            ensure!(posting_code4_overlay_dir.is_none() || (query_navigation.as_deref()==Some("fp") && profile_query_work && !evict_postings_after_operation && m.vector_count<=16384),"posting overlay requires FP diagnostic immutable16K reader");
            let config = read_config(&m.construction)?;
            let p = provider_cache::provider(&dir,cache_policy.unwrap_or_default()).await?;
            let preparation_reader_open_started=profile_posting_preparation.then(Instant::now);
            let query_reader_open_started=query_navigation.as_ref().map(|_|Instant::now());
            let mut reader =
                HierarchicalSpannReader::open(&p, m.block_ids.ids()?, metric(&m.metric)?, config)
                    .await
                    .map_err(|e| anyhow::anyhow!("open: {e}"))?;
            let preparation_reader_open_ns=preparation_reader_open_started.map(|t|t.elapsed().as_nanos().min(u64::MAX as u128) as u64);
            let query_reader_open_seconds=query_reader_open_started.map(|t|t.elapsed().as_secs_f64());
            let fp_navigation=match query_navigation.as_deref(){Some("fp")=>Some(reader.prepare_fp_navigation().await.map_err(|e|anyhow::anyhow!("FP navigation preparation: {e}"))?),Some("four-bit")=>Some(reader.prepare_four_bit_navigation().await.map_err(|e|anyhow::anyhow!("four-bit navigation preparation: {e}"))?),_=>None};
            if let Some(cache)=fp_navigation.as_ref(){ensure!(cache.report()["dimension"]==m.dimension,"FP centroid/manifest dimension mismatch");}
            ensure!(
                m.id_mapping
                    .as_ref()
                    .is_none_or(|map| map.count == m.vector_count),
                "persisted ID map count differs from vector count"
            );
            let posting_overlay_report=if let Some(path)=posting_code4_overlay_dir.as_ref(){
                use sha2::Digest;
                let manifest_path=if let Some(g)=generation{dir.join(format!("manifest-{g}.json"))}else{dir.join("manifest.json")};
                let binding=format!("{:x}",sha2::Sha256::digest(std::fs::read(manifest_path)?));
                Some(reader.prepare_posting_overlay(path,&binding).await?)
            }else{None};
            let mut lookup = if local_ids_only { None } else { m
                .id_mapping
                .as_ref()
                .map(|map| id_map::IdLookup::open(&dir, map))
                .transpose()? };
            let stdin = std::io::stdin();
            let mut out = std::io::stdout().lock();
            for line in stdin.lock().lines() {
                let line = line?;
                let value = serde_json::from_str::<serde_json::Value>(&line);
                let request_id = value
                    .as_ref()
                    .ok()
                    .and_then(|v| v.get("request_id"))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let started = Instant::now();
                if profile_rerank_pins {reader.start_vector_pin_observation();}
                let mut profile_memory = false;
                let mut posting_selection_profile=None;
                let mut posting_preparation_profile=None;
                let mut rerank_operation_ns=None;
                let mut response=async {
                    let q:Query=serde_json::from_value(value.context("malformed JSON")?)?;
                    profile_memory = q.profile_memory;
                    ensure!(!profile_posting_selection || q.streaming_limits.is_none(),"posting selection profile does not combine with streaming");
                    ensure!(q.candidate_limits.is_none() || q.streaming_limits.is_some(),"candidate limits require streaming");
                    ensure!(q.streaming_limits.is_none() || q.op=="score_selected_leaves","streaming limits require selected-leaf scoring");
                    ensure!(q.generation.is_none_or(|g|g==m.generation),"stale generation");
                    if q.op=="describe_root" {
                        let root=reader.describe_root().map_err(|e|anyhow::anyhow!("root: {e}"))?;
                        return Ok::<_,anyhow::Error>(serde_json::json!({"root":root,"subtree_height":reader.depth(),"dimension":m.dimension,"metric":m.metric,"vector_count":m.vector_count,"id_mapping":m.id_mapping,"routing_boundary_hash":m.routing_boundary_hash}));
                    }
                    if q.op=="describe_fp_nodes" {
                        ensure!((1..=16).contains(&m.dimension),"diagnostic dimension exceeds bounds");
                        let nodes=reader.describe_fp_nodes(&q.nodes).await.map_err(|e|anyhow::anyhow!("diagnostic nodes: {e}"))?;
                        return Ok(serde_json::json!({"nodes":nodes,"dimension":m.dimension,"diagnostic_only":true}));
                    }
                    ensure!(q.query.len()==m.dimension && q.query.iter().all(|v|v.is_finite()),"invalid query vector");
                    let mut vector=q.query; if m.metric=="cosine" { normalize(&mut vector)?; }
                    let response=match q.op.as_str() {
                        "query" => {
                            ensure!(!local_ids_only,"local-ID-only readers require coordinator-selected operations");
                            ensure!(q.k>0 && q.k<=10000 && q.beam_min>0 && q.beam_max>=q.beam_min && q.beam_max<=100000 && q.rerank_factor>=2 && q.rerank_factor<=10000,"invalid search parameters; rerank_factor must be >=2 for exact distances");
                            let policy=hierarchical_index::common::ReadBeamPolicy::uniform(Some(2.0),q.beam_min,q.beam_max);
                            let (hits,scanned,leaves,timings)=if let Some(cache)=fp_navigation.as_ref(){reader.search_with_policy_lazy_fp(&vector,q.k,q.rerank_factor,&policy,cache,trace_query_stages).await}else if trace_query_stages {reader.search_with_policy_lazy_traced(&vector,q.k,q.rerank_factor,&policy).await}else{reader.search_with_policy_lazy(&vector,q.k,q.rerank_factor,&policy).await}.map_err(|e|anyhow::anyhow!("search: {e}"))?;
                            let results=mapped_hits(&hits,&m,&mut lookup)?;
                            let mut value=serde_json::json!({"ids":results.iter().map(|h|h["id"].clone()).collect::<Vec<_>>(),"distances":hits.iter().map(|(_,d)|d).collect::<Vec<_>>(),"results":results,"rerank_factor":q.rerank_factor,"candidate_budget":q.k*q.rerank_factor,"actual_candidate_count":scanned.min(q.k*q.rerank_factor)});
                            if profile_query_work {value["query_work_policy"]="ordinary-query-work-v1".into();value["query_work"]=serde_json::json!({"counts":timings.work,"returned_results":hits.len(),"returned_distance_bits":hits.iter().map(|(_,d)|d.to_bits()).collect::<Vec<_>>(),"selected_leaves":leaves,"navigate_ns":timings.navigate_nanos,"quantize_ns":timings.quantize_nanos,"distance_ns":timings.distance_nanos,"sort_dedup_ns":timings.sort_dedup_nanos,"rerank_ns":timings.rerank_nanos,"scope":"Posting rows are current loaded copies actually scored, not physical disk rows. Distinct IDs before trim are unbounded by candidate budget. Exact rerank count precedes final k truncation. Timers omit posting preparation and response mapping; no physicalIO/actorfanout claim."});}
                            if trace_query_stages {value["query_stage_trace_policy"]="bounded-query-stages-v1".into();value["query_stage_trace"]=serde_json::to_value(&timings.query_stage_trace)?;}
                            value
                        },
                        "expand" => {
                            ensure!(!q.nodes.is_empty() && q.nodes.len()<=100000,"invalid expansion node count");
                            let children=reader.expand_nodes(&vector,&q.nodes).map_err(|e|anyhow::anyhow!("expand: {e}"))?;
                            serde_json::json!({"expanded_nodes":q.nodes.len(),"children_scored":children.len(),"children":children})
                        },
                        "score_selected_leaves" => {
                            ensure!(!q.nodes.is_empty() && q.nodes.len()<=100000 && q.candidate_budget>0 && q.candidate_budget<=1000000,"invalid selected-leaf parameters");
                            let (hits,unique,streaming_metrics)=if let Some(limits)=q.streaming_limits {
                                let (hits,unique,metrics)=reader.score_selected_leaves_streaming_with_candidates(&vector,&q.nodes,q.candidate_budget,m.vector_count as usize,limits,q.candidate_limits).await.map_err(|e|anyhow::anyhow!("stream selected leaves: {e}"))?;
                                (hits,unique,Some(metrics))
                            } else if profile_posting_selection {
                                let scored=if profile_posting_preparation {let (scored,prep)=reader.score_selected_leaves_preparation(&vector,&q.nodes,q.candidate_budget,partition_posting_candidates,posting_scorer).await;posting_preparation_profile=Some(prep);scored}else{reader.score_selected_leaves_partitioned_with_scorer(&vector,&q.nodes,q.candidate_budget,partition_posting_candidates,posting_scorer).await};let (hits,unique,profile)=scored.map_err(|e|anyhow::anyhow!("score selected leaves: {e}"))?;posting_selection_profile=Some(profile);(hits,unique,None)
                            } else { let (hits,unique)=reader.score_selected_leaves(&vector,&q.nodes,q.candidate_budget).await.map_err(|e|anyhow::anyhow!("score selected leaves: {e}"))?;(hits,unique,None) };
                            // The reader retains every boundary tie. A local-ID
                            // coordinator must map ALL ties before applying the
                            // external-ID tiebreak and the original M cutoff.
                            let mut candidates=if local_ids_only { local_hits(&hits) } else { mapped_hits(&hits,&m,&mut lookup)? };
                            if !local_ids_only {
                                candidates.sort_unstable_by(|a,b|a["distance"].as_f64().unwrap().total_cmp(&b["distance"].as_f64().unwrap()).then_with(||a["id"].as_u64().unwrap().cmp(&b["id"].as_u64().unwrap())));
                                candidates.truncate(q.candidate_budget);
                            }
                            let mut result=serde_json::json!({"actual_candidate_count":candidates.len(),"candidates":candidates,"leaves_scanned":q.nodes.len(),"unique_candidates_scanned":unique,"candidate_budget":q.candidate_budget});
                            if let Some(metrics)=streaming_metrics { result["streaming_metrics"]=serde_json::to_value(metrics)?;result["streaming_limits"]=serde_json::to_value(q.streaming_limits)?;if q.candidate_limits.is_some() { result["candidate_limits"]=serde_json::to_value(q.candidate_limits)?; } }
                            result
                        },
                        "rerank_selected" => {
                            ensure!(!q.local_ids.is_empty() && q.local_ids.len()<=1000000 && q.local_ids.iter().all(|id|u64::from(*id)<m.vector_count),"invalid selected rerank IDs");
                            let rerank_started=Instant::now();
                            let hits_result=if release_rerank_vectors {
                                reader.rerank_selected_releasing_vectors(&vector,&q.local_ids).await
                            } else {reader.rerank_selected(&vector,&q.local_ids).await};
                            if profile_rerank_pins {rerank_operation_ns=Some(rerank_started.elapsed().as_nanos().min(u64::MAX as u128) as u64);}
                            let hits=hits_result.map_err(|e|anyhow::anyhow!("rerank selected: {e}"))?;
                            serde_json::json!({"results":if local_ids_only { local_hits(&hits) } else { mapped_hits(&hits,&m,&mut lookup)? },"exact_vectors_scored":hits.len()})
                        },
                        _=>bail!("unsupported operation"),
                    };
                    Ok::<_,anyhow::Error>(response)
                }.await;
                // Lazy reranking copies embeddings into a per-query map. Once
                // search completes, raw block pins can be released on every path.
                let evicted_postings = if evict_postings_after_operation { reader.clear_loaded_postings() } else { (0, 0) };
                reader.clear_loaded_blocks();
                if cache_policy.is_some_and(|policy|policy.clear_after_operation) {
                    if let Err(e)=p.clear().await {response=Err(anyhow::anyhow!("provider cache clear failed: {e}"));}
                }
                let mut response = match response {
                    Ok(v) => v,
                    Err(e) => serde_json::json!({"error":e.to_string()}),
                };
                let memory = reader.memory_usage();
                response["memory_bytes"] = memory.total_bytes().into();
                if profile_memory {
                    let (leaves, leaf_owned_bytes) = reader.loaded_postings_stats();
                    let (posting_pins, vector_pins) = reader.loaded_blocks_stats();
                    response["retained_postings"] = serde_json::json!({
                        "eviction_policy": if evict_postings_after_operation { "after-operation" } else { "retain" },
                        "evicted_leaves": evicted_postings.0,
                        "evicted_array_payload_bytes": evicted_postings.1,
                        "materialized_leaves": leaves,
                        "entries": memory.posting_entries,
                        "payload_bytes": memory.posting_bytes,
                        "leaf_owned_bytes_including_centroids": leaf_owned_bytes,
                        "posting_block_pins": posting_pins.0,
                        "vector_block_pins": vector_pins.0,
                    });
                }
                if let Some(policy)=cache_policy {response["provider_cache_policy"]=serde_json::to_value(policy)?;}
                if let Some(scorer)=posting_scorer {
                    response["posting_scorer_policy"]=scorer.policy().name().into();
                    response["posting_scorer_implementation"]=scorer.implementation_name().into();
                }
                if profile_posting_preparation {response["posting_preparation_reader_open_ns"]=serde_json::to_value(preparation_reader_open_ns)?;response["posting_preparation_provider"]=serde_json::json!({"kind":"plain-memory-foyer","effective_policy":cache_policy.unwrap_or_default(),"open_cost":"one-time reader open repeated as metadata; charge once; provider/config creation excluded"});response["posting_preparation_policy"]="posting-preparation-v1".into();if let Some(profile)=posting_preparation_profile {response["posting_preparation_profile"]=profile;}}
                if profile_posting_selection {
                    response["posting_selection_policy"]=if partition_posting_candidates {"partition-inclusive"}else{"full-sort-inclusive"}.into();
                    if let Some(profile)=posting_selection_profile {response["posting_selection_profile"]=profile;}
                }
                if profile_rerank_pins {
                    response["rerank_operation_ns"]=serde_json::to_value(rerank_operation_ns)?;
                    response["vector_pin_observation"]=serde_json::to_value(reader.vector_pin_observation())?;
                    response["exact_rerank_policy"]=if release_rerank_vectors {"serial-release"} else {"default-concurrent-retain"}.into();
                }
                if let Some(policy)=query_navigation.as_ref(){response["query_navigation_policy"]=policy.clone().into();response["query_reader_open_seconds"]=serde_json::to_value(query_reader_open_seconds)?;response["fp_navigation_cache"]=if policy=="fp"{fp_navigation.as_ref().unwrap().report().clone()}else{serde_json::Value::Null};response["compressed_navigation_cache"]=if policy=="four-bit"{fp_navigation.as_ref().unwrap().report().clone()}else{serde_json::Value::Null};response["query_navigation_snapshot"]=serde_json::json!({"generation":m.generation,"source_id":m.source_id,"dimension":m.dimension,"metric":m.metric,"root_ref":m.root_ref});}
                if let Some(report)=posting_overlay_report.as_ref(){response["posting_score_policy"]="code4-residual-postings-v1".into();response["posting_overlay_report"]=report.clone();}
                response["elapsed_ms"] = (started.elapsed().as_secs_f64() * 1000.0).into();
                response["request_id"] = request_id;
                response["generation"] = m.generation.into();
                response["shard_id"] = m.shard_id.clone().into();
                if local_ids_only { response["id_mode"]="local-only".into(); }
                serde_json::to_writer(&mut out, &response)?;
                writeln!(out)?;
                out.flush()?;
            }
        }
    }
    Ok(())
}
fn normalize(v: &mut [f32]) -> Result<()> {
    let norm = v.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
    ensure!(norm > 0.0, "cosine vector must have nonzero norm");
    for x in v {
        *x = (f64::from(*x) / norm) as f32;
    }
    Ok(())
}
#[derive(Deserialize)]
struct Query {
    #[serde(default)]
    profile_memory: bool,
    #[serde(default)]
    streaming_limits: Option<hierarchical_index::reader::StreamingLimits>,
    #[serde(default)]
    candidate_limits: Option<hierarchical_index::reader::WorkingSetLimits>,
    #[serde(default = "query_op")]
    op: String,
    #[serde(default, alias = "vector")]
    query: Vec<f32>,
    #[serde(default)]
    k: usize,
    #[serde(default)]
    nodes: Vec<u32>,
    #[serde(default)]
    local_ids: Vec<u32>,
    #[serde(default)]
    candidate_budget: usize,
    #[serde(default)]
    generation: Option<u64>,
    #[serde(default = "beam_min")]
    beam_min: usize,
    #[serde(default = "beam_max")]
    beam_max: usize,
    #[serde(default = "rerank", alias = "rerank")]
    rerank_factor: usize,
}
fn query_op() -> String {
    "query".into()
}
fn beam_min() -> usize {
    10
}
fn beam_max() -> usize {
    256
}
fn rerank() -> usize {
    10
}

fn read_config(value: &serde_json::Value) -> Result<HierarchicalSpannConfig> {
    if value["version"] == 2 { return dynamic_construct::read_config(value); }
    let mut expected = construction();
    let branching_factor = value["branching_factor"]
        .as_u64()
        .context("missing branching_factor")? as usize;
    let split_threshold = value["split_threshold"]
        .as_u64()
        .context("missing split_threshold")? as usize;
    let merge_threshold = value["merge_threshold"]
        .as_u64()
        .context("missing merge_threshold")? as usize;
    expected["branching_factor"] = branching_factor.into();
    expected["split_threshold"] = split_threshold.into();
    expected["merge_threshold"] = merge_threshold.into();
    ensure!(*value == expected, "unsupported construction configuration");
    Ok(HierarchicalSpannConfig {
        branching_factor,
        split_threshold,
        merge_threshold,
        ..Default::default()
    })
}

fn mapped_hits(
    hits: &[(u32, f32)],
    manifest: &Manifest,
    lookup: &mut Option<id_map::IdLookup>,
) -> Result<Vec<serde_json::Value>> {
    hits.iter()
        .map(|&(local_id, distance)| {
            let id = if let Some(lookup) = lookup {
                lookup.get(local_id)?
            } else {
                manifest.id_offset + u64::from(local_id) * manifest.id_stride
            };
            Ok(serde_json::json!({"local_id":local_id,"id":id,"distance":distance}))
        })
        .collect()
}

fn local_hits(hits: &[(u32, f32)]) -> Vec<serde_json::Value> {
    hits.iter().map(|&(local_id,distance)|serde_json::json!({"local_id":local_id,"distance":distance})).collect()
}
