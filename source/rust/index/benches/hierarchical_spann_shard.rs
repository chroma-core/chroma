//! Independent, checkpointed hierarchical indexes for shard experiments.
#![recursion_limit = "256"]
#![allow(dead_code)]
mod hierarchical_index;
#[path = "sharding/id_map.rs"]
mod id_map;
use anyhow::{bail, ensure, Context, Result};
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::FoyerCacheConfig;
use chroma_distance::DistanceFunction;
use chroma_storage::{local::LocalStorage, Storage};
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
    },
    Read {
        #[arg(long)]
        checkpoint_dir: PathBuf,
        #[arg(long)]
        generation: Option<u64>,
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
    let data = dir.join("data");
    fs::create_dir_all(&data)?;
    Ok(BlockfileProvider::ArrowBlockfileProvider(
        ArrowBlockfileProvider::new(
            Storage::Local(LocalStorage::new(data.to_str().context("non-UTF8 path")?)),
            3 * 1024 * 1024,
            FoyerCacheConfig {
                name: "shard_blocks".into(),
                capacity: 256,
                ..Default::default()
            }
            .build_memory_persistent()
            .await
            .map_err(|e| anyhow::anyhow!("block cache: {e}"))?,
            FoyerCacheConfig {
                name: "shard_roots".into(),
                capacity: 4096,
                ..Default::default()
            }
            .build_memory_persistent()
            .await
            .map_err(|e| anyhow::anyhow!("root cache: {e}"))?,
            16,
        ),
    ))
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
            checkpoint_dir: dir,
            shard_id,
            metric: metric_name,
            resume,
            checkpoint_rows,
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
            let p = provider(&dir).await?;
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
                        // Bound validation memory; benchmark-sized shards rely on smoke coverage.
                        if count <= 100_000 {
                            writer
                                .load_all_postings()
                                .await
                                .map_err(|e| anyhow::anyhow!("validate postings: {e}"))?;
                            let reachable = writer
                                .root_reachable_valid_ids()
                                .map_err(|e| anyhow::anyhow!("validate tree: {e}"))?;
                            ensure!(
                                reachable.len() as u64 == count
                                    && (0..count).all(|id| reachable.contains(&(id as u32))),
                                "reopened index does not contain expected local IDs"
                            );
                        }
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
        } => {
            let m: Manifest = if let Some(g) = generation {
                serde_json::from_reader(File::open(dir.join(format!("manifest-{g}.json")))?)?
            } else {
                load(&dir)?
            };
            let config = read_config(&m.construction)?;
            let p = provider(&dir).await?;
            let reader =
                HierarchicalSpannReader::open(&p, m.block_ids.ids()?, metric(&m.metric)?, config)
                    .await
                    .map_err(|e| anyhow::anyhow!("open: {e}"))?;
            ensure!(
                m.id_mapping
                    .as_ref()
                    .is_none_or(|map| map.count == m.vector_count),
                "persisted ID map count differs from vector count"
            );
            let mut lookup = m
                .id_mapping
                .as_ref()
                .map(|map| id_map::IdLookup::open(&dir, map))
                .transpose()?;
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
                let response=async {
                    let q:Query=serde_json::from_value(value.context("malformed JSON")?)?;
                    ensure!(q.generation.is_none_or(|g|g==m.generation),"stale generation");
                    if q.op=="describe_root" {
                        let root=reader.describe_root().map_err(|e|anyhow::anyhow!("root: {e}"))?;
                        return Ok::<_,anyhow::Error>(serde_json::json!({"root":root,"subtree_height":reader.depth(),"dimension":m.dimension,"metric":m.metric,"vector_count":m.vector_count,"id_mapping":m.id_mapping,"routing_boundary_hash":m.routing_boundary_hash}));
                    }
                    ensure!(q.query.len()==m.dimension && q.query.iter().all(|v|v.is_finite()),"invalid query vector");
                    let mut vector=q.query; if m.metric=="cosine" { normalize(&mut vector)?; }
                    let response=match q.op.as_str() {
                        "query" => {
                            ensure!(q.k>0 && q.k<=10000 && q.beam_min>0 && q.beam_max>=q.beam_min && q.beam_max<=100000 && q.rerank_factor>=2 && q.rerank_factor<=10000,"invalid search parameters; rerank_factor must be >=2 for exact distances");
                            let (hits,scanned,_,_)=reader.search_with_policy_lazy(&vector,q.k,q.rerank_factor,&hierarchical_index::common::ReadBeamPolicy::uniform(Some(2.0),q.beam_min,q.beam_max)).await.map_err(|e|anyhow::anyhow!("search: {e}"))?;
                            let results=mapped_hits(&hits,&m,&mut lookup)?;
                            serde_json::json!({"ids":results.iter().map(|h|h["id"].clone()).collect::<Vec<_>>(),"distances":hits.iter().map(|(_,d)|d).collect::<Vec<_>>(),"results":results,"rerank_factor":q.rerank_factor,"candidate_budget":q.k*q.rerank_factor,"actual_candidate_count":scanned.min(q.k*q.rerank_factor)})
                        },
                        "expand" => {
                            ensure!(!q.nodes.is_empty() && q.nodes.len()<=100000,"invalid expansion node count");
                            let children=reader.expand_nodes(&vector,&q.nodes).map_err(|e|anyhow::anyhow!("expand: {e}"))?;
                            serde_json::json!({"expanded_nodes":q.nodes.len(),"children_scored":children.len(),"children":children})
                        },
                        "score_selected_leaves" => {
                            ensure!(!q.nodes.is_empty() && q.nodes.len()<=100000 && q.candidate_budget>0 && q.candidate_budget<=1000000,"invalid selected-leaf parameters");
                            let (hits,unique)=reader.score_selected_leaves(&vector,&q.nodes,q.candidate_budget).await.map_err(|e|anyhow::anyhow!("score selected leaves: {e}"))?;
                            let mut candidates=mapped_hits(&hits,&m,&mut lookup)?;
                            candidates.sort_unstable_by(|a,b|a["distance"].as_f64().unwrap().total_cmp(&b["distance"].as_f64().unwrap()).then_with(||a["id"].as_u64().unwrap().cmp(&b["id"].as_u64().unwrap())));
                            candidates.truncate(q.candidate_budget);
                            serde_json::json!({"actual_candidate_count":candidates.len(),"candidates":candidates,"leaves_scanned":q.nodes.len(),"unique_candidates_scanned":unique,"candidate_budget":q.candidate_budget})
                        },
                        "rerank_selected" => {
                            ensure!(!q.local_ids.is_empty() && q.local_ids.len()<=1000000 && q.local_ids.iter().all(|id|u64::from(*id)<m.vector_count),"invalid selected rerank IDs");
                            let hits=reader.rerank_selected(&vector,&q.local_ids).await.map_err(|e|anyhow::anyhow!("rerank selected: {e}"))?;
                            serde_json::json!({"results":mapped_hits(&hits,&m,&mut lookup)?,"exact_vectors_scored":hits.len()})
                        },
                        _=>bail!("unsupported operation"),
                    };
                    Ok::<_,anyhow::Error>(response)
                }.await;
                // Lazy reranking copies embeddings into a per-query map. Once
                // search completes, raw block pins can be released on every path.
                reader.clear_loaded_blocks();
                let mut response = match response {
                    Ok(v) => v,
                    Err(e) => serde_json::json!({"error":e.to_string()}),
                };
                response["memory_bytes"] = reader.memory_usage().total_bytes().into();
                response["elapsed_ms"] = (started.elapsed().as_secs_f64() * 1000.0).into();
                response["request_id"] = request_id;
                response["generation"] = m.generation.into();
                response["shard_id"] = m.shard_id.clone().into();
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
