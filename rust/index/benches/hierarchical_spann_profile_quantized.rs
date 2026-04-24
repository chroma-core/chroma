//! Benchmark for 1-bit quantized HierarchicalSpannWriter: incremental index
//! build with recall evaluation at each checkpoint. Uses 1-bit RaBitQ codes
//! for both data vectors and centroid navigation.

#![recursion_limit = "256"]

// Use jemalloc on non-Windows so we can tune fragmentation behavior at
// runtime via the `_RJEM_MALLOC_CONF` env var. With the default glibc
// allocator there is no good way to force the long-lived `add()` /
// balance churn over `Vec<u8>` posting buffers and `Vec<f32>` centroids
// to release dirty pages back to the OS, which shows up as RSS that
// climbs steadily checkpoint-over-checkpoint even after every cache and
// retained-vector pool has been bounded.
//
// To experiment with reclaim behavior, set e.g.:
//   _RJEM_MALLOC_CONF=background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000
// Note the `_RJEM_` prefix is required by tikv-jemallocator (it is the
// renamed-symbol form of the standard `MALLOC_CONF`).
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod datasets;
mod hierarchical_index;
mod optimal_gt;

use std::collections::{BTreeMap, HashSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn flush_stdout() {
    let _ = std::io::stdout().flush();
}

use rayon::prelude::*;

use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::{new_cache_for_test, FoyerCacheConfig};
use chroma_distance::DistanceFunction;
use chroma_storage::{local::LocalStorage, Storage};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};

use datasets::{format_count, recall_at_k, Dataset, DatasetType, MetricType, Query};
use hierarchical_index::common::ReadBeamPolicy;
use hierarchical_index::config::{HierarchicalSpannConfig, NavigationMode};
use hierarchical_index::instrumentation::SearchTimings;
use hierarchical_index::mem_probe::{self, RssSampler};
use hierarchical_index::reader::HierarchicalSpannReader;
use hierarchical_index::writer::{
    format_data_loaded_table, format_task_tables, HierarchicalSpannIds, HierarchicalSpannWriter,
    LeafMissDiagnostic, LeafTraits, WriterStatsSnapshot,
};

// =============================================================================
// CLI
// =============================================================================

#[derive(Parser, Debug)]
#[command(name = "hierarchical_spann_profile_quantized")]
#[command(about = "Benchmark for 1-bit quantized HierarchicalSpannWriter")]
#[command(trailing_var_arg = true)]
struct Args {
    #[arg(long, default_value = "wikipedia-en")]
    dataset: DatasetType,

    #[arg(long, default_value = "l2")]
    metric: MetricType,

    /// Number of checkpoints to run (default: all)
    #[arg(long)]
    checkpoint: Option<usize>,

    /// Vectors per checkpoint
    #[arg(long, default_value = "1000000")]
    checkpoint_size: usize,

    /// Min beam width for read/search dynamic beam
    #[arg(long = "read-beam-min", default_value = "10")]
    read_beam_min: usize,

    /// Max beam width for read/search dynamic beam
    #[arg(long = "read-beam-max", default_value = "128")]
    read_beam_max: usize,

    #[arg(long, default_value = "100")]
    branching_factor: usize,

    #[arg(long, default_value = "2048")]
    split_threshold: usize,

    #[arg(long, default_value = "512")]
    merge_threshold: usize,

    /// Dynamic beam tau for write path (add/reassign/merge navigate)
    #[arg(long, default_value = "1.5")]
    write_beam_tau: f64,

    /// Per-level write taus overriding the global write tau, comma-separated.
    /// Use `_` to fall back to the global write tau for a level.
    #[arg(long)]
    write_level_taus: Option<String>,

    /// Per-level write beam *floors* as percentages of the full level
    /// width, comma-separated. Acts as a `max(default_beam_min,
    /// ceil(level_width * pct/100))` floor for the tau-filtered beam at
    /// each level. The hard cap is always `--write-beam-max` -- a high
    /// `min_pct` cannot push the beam past the cap.
    #[arg(long)]
    write_level_min_pcts: Option<String>,

    /// Min beam width for write path
    #[arg(long, default_value = "10")]
    write_beam_min: usize,

    /// Max beam width for write path
    #[arg(long, default_value = "16")]
    write_beam_max: usize,

    /// Max replicas per vector (RNG select)
    #[arg(long, default_value = "1")]
    max_replicas: usize,

    /// RNG epsilon filter
    #[arg(long, default_value = "0")]
    write_rng_epsilon: f32,

    /// RNG distance factor
    #[arg(long, default_value = "4.0")]
    write_rng_factor: f32,

    /// Force brute-force ground truth computation (slow at scale, and
    /// requires retaining every input vector in RAM — at dim=1024 that is
    /// ~4 GB per million vectors, so leave this off for runs > a few M
    /// unless you have headroom).
    #[arg(
        long,
        default_value = "false",
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    brute_force_gt: bool,

    /// Compute a flat k-means GT baseline using the same number of clusters as leaf nodes
    #[arg(long)]
    compute_optimal_gt: bool,

    /// Compute per-query GT cluster coverage stats (Index Quality section).
    /// Walks every leaf for every query, which is slow at large index sizes.
    #[arg(
        long,
        default_value = "true",
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    compute_gt_clusters: bool,

    /// Unified recall execution mode. When `true` (default), the recall step:
    ///   1. Skips `load_all_postings()` and `load_all_embeddings()` at setup.
    ///   2. Uses `search_with_policy_lazy` for each query: posting `load_node`
    ///      and embedding `get` calls run with bounded async concurrency
    ///      (~32 in flight per query) on top of rayon-parallel queries.
    ///   3. Runs each `(tau, rerank)` row twice: a `cold` pass with all
    ///      `loaded_blocks` pins and per-leaf posting data cleared first,
    ///      then a `warm` pass that reuses what the cold pass populated.
    ///   4. After every warm pass and at end of round, clears the
    ///      `loaded_blocks` block pins on both internal blockfile readers
    ///      and (if postings were lazy-loaded) clears posting data from
    ///      `self.nodes`, so the next row's cold pass starts truly cold.
    /// When `false`, the recall step uses the legacy eager path:
    /// `load_all_postings` + (when any rerank > 1) `load_all_embeddings`,
    /// `search_with_policy_sync`, single pass per row, no clearing. The
    /// eager path will OOM at large index sizes (e.g. 113M x dim=1024 needs
    /// ~454 GB just for embeddings).
    #[arg(
        long,
        default_value = "true",
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    lazy_recall: bool,

    /// Max number of queries to use for recall evaluation
    #[arg(long, default_value = "100")]
    num_queries: usize,

    /// Vector dimension (only for --dataset synthetic)
    #[arg(long, default_value = "1024")]
    dim: usize,

    /// Number of vectors for synthetic dataset
    #[arg(long, default_value = "1000000")]
    synthetic_size: usize,

    /// Default beam tau for search
    #[arg(long, default_value = "2.0")]
    beam_tau: f64,

    /// Per-level read taus overriding the global recall tau, comma-separated.
    /// Use `_` to fall back to the per-row tau for a level.
    #[arg(long)]
    read_level_taus: Option<String>,

    /// Per-level read beam *floors* as percentages of the full level
    /// width, comma-separated. Acts as a `max(default_beam_min,
    /// ceil(level_width * pct/100))` floor for the tau-filtered beam at
    /// each level. The hard cap is always `--read-beam-max` -- a high
    /// `min_pct` cannot push the beam past the cap.
    #[arg(long)]
    read_level_min_pcts: Option<String>,

    /// Number of threads for parallel add
    #[arg(long, default_value = "32")]
    threads: usize,

    /// Write-path navigation mode: fp (f32), 4bit (QuantizedQuery)
    #[arg(long, default_value = "fp")]
    write_navigation: String,

    /// Use full precision f32 distances for NPA instead of quantized
    #[arg(long)]
    fp_npa: bool,

    /// Tau values for recall sweep, comma-separated
    #[arg(long, default_value = "1.5,2.0")]
    recall_tau_values: String,

    /// Vector rerank factors to sweep during recall
    #[arg(long, default_value = "1,8", value_delimiter = ',')]
    recall_rerank_vectors: Vec<usize>,

    /// Run deferred balancing in parallel across subtrees
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    parallel_balancing: bool,

    /// Print leaf-miss diagnostic: rank distribution of missed GT-containing leaves
    #[arg(long)]
    leaf_miss_diagnostic: bool,

    /// Print search geometry: cluster radius, search radius, GT radius distributions
    #[arg(long)]
    geometry_diagnostic: bool,

    /// Print legend explaining all table columns
    #[arg(long)]
    print_legend: bool,

    /// Save checkpoint to disk after each checkpoint.
    #[arg(long)]
    save: bool,

    /// Directory for checkpoint blockfiles and metadata.
    /// Defaults to target/hierarchical_cache/save/<dataset>/<config_slug>.
    #[arg(long)]
    save_dir: Option<String>,

    /// Resume from the last committed checkpoint in --save-dir
    #[arg(long)]
    resume: bool,

    /// Maximum bytes the in-memory blockfile cache may hold. Above this
    /// the cache evicts LRU. Default 32 GiB. Set to 0 for an unbounded
    /// cache (legacy `new_cache_for_test` behavior; will OOM on long
    /// runs because every committed block stays resident).
    #[arg(long, default_value_t = 32u64 * 1024 * 1024 * 1024)]
    max_cache_bytes: u64,

    /// Garbage-collect orphaned block + sparse-index files after each
    /// successful commit. Without this, fork-on-commit blockfiles
    /// accumulate every checkpoint's blocks indefinitely (~25-35 GB per
    /// CP at 1M vectors / dim=1024) and quickly fill the save volume.
    /// Only takes effect when `--save` is set; ignored otherwise.
    #[arg(
        long,
        default_value = "true",
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    gc_blockfiles: bool,

    /// Skip commit/flush/save/reopen/GC at each checkpoint boundary.
    /// Intended for profiling iterations: keeps the writer alive across
    /// the loop so the per-CP `--- Task Counts ---` and `--- Data Loaded
    /// ---` tables reflect pure add+balance work without any commit
    /// overhead. With `--resume`, leaves the source `--save-dir` intact
    /// (no new blockfiles, no checkpoint.json rewrite). The recall
    /// section after the loop will run against the originally resumed
    /// committed_ids (i.e. the on-disk state at start of the bench).
    #[arg(long)]
    no_commit: bool,

    /// Drop the writer's per-reader `loaded_blocks` HashMap between
    /// checkpoint phases (after `add` + `balance`, before `commit`).
    ///
    /// Each `BlockfileReader` keeps an unbounded, never-evicting
    /// `HashMap<Uuid, Box<Block>>` of every block it has ever fetched
    /// (the unsafe `transmute` in `ArrowBlockfileReader::get_block`
    /// requires the box to never move/free). At dim=1024, ID-distributed
    /// embedding lookups touch ~every block in the vector_data
    /// blockfile, so this map ends up pinning hundreds of GB by the end
    /// of a CP -- this is the bulk of the unaccounted RSS visible as
    /// `(jemalloc.allocated - writer.memory_usage)` in the per-CP
    /// `Process mem` line. See `docs/README.md` -> "Reader-side block
    /// pinning" for the full diagnosis and the upstream fix.
    ///
    /// Default true. Disable for A/B comparisons or to repro the leak.
    #[arg(
        long,
        default_value = "true",
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    clear_reader_block_pins: bool,

    #[arg(hide = true, allow_hyphen_values = true)]
    _extra: Vec<String>,
}

// =============================================================================
// Helpers
// =============================================================================

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.2}s", secs)
    } else {
        format!("{:.1}m", secs / 60.0)
    }
}

fn format_latency(nanos: u64) -> String {
    let us = nanos as f64 / 1000.0;
    if us < 1000.0 {
        format!("{:.1}us", us)
    } else if us < 1_000_000.0 {
        format!("{:.1}ms", us / 1000.0)
    } else {
        format!("{:.2}s", us / 1_000_000.0)
    }
}

fn format_mb(mb: f64) -> String {
    if mb < 1.0 {
        format!("{:.1}KB", mb * 1024.0)
    } else if mb < 1024.0 {
        format!("{:.1}MB", mb)
    } else {
        format!("{:.2}GB", mb / 1024.0)
    }
}

fn parse_level_taus(
    input: Option<&str>,
) -> Result<Vec<Option<f64>>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(input) = input else {
        return Ok(Vec::new());
    };
    input
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| {
            let token = s.trim();
            if token == "_" || token.eq_ignore_ascii_case("default") {
                Ok(None)
            } else {
                Ok(Some(token.parse::<f64>()?))
            }
        })
        .collect()
}

fn parse_level_f64s(
    input: Option<&str>,
) -> Result<Vec<f64>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(input) = input else {
        return Ok(Vec::new());
    };
    input
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| Ok(s.trim().parse::<f64>()?))
        .collect()
}

fn format_level_taus(taus: &[Option<f64>]) -> String {
    taus.iter()
        .map(|tau| {
            tau.map(|t| format!("{:.2}", t))
                .unwrap_or_else(|| "_".to_string())
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn compute_ground_truth(
    query_vectors: &[Vec<f32>],
    data_vectors: &[(u32, Arc<[f32]>)],
    distance_fn: &DistanceFunction,
    k: usize,
) -> Vec<Query> {
    query_vectors
        .iter()
        .map(|qv| {
            let mut dists: Vec<(u32, f32)> = data_vectors
                .iter()
                .map(|(id, emb)| (*id, distance_fn.distance(qv, emb)))
                .collect();
            dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            let neighbors: Vec<u32> = dists.iter().take(k).map(|(id, _)| *id).collect();
            Query {
                vector: qv.clone(),
                neighbors,
                max_vector_id: data_vectors.len() as u64,
            }
        })
        .collect()
}

fn gt_cache_path(
    dataset_name: &str,
    metric: &str,
    num_vectors: usize,
    num_queries: usize,
) -> std::path::PathBuf {
    std::path::PathBuf::from(format!(
        "target/hierarchical_cache/gt_{}_{}_{}_q{}.bin",
        dataset_name, metric, num_vectors, num_queries,
    ))
}

fn print_cluster_stats(label: &str, values: &[usize]) {
    if values.is_empty() {
        println!("  {}: avg=0.0, min=0, p50=0, p90=0, p99=0, max=0", label);
        return;
    }

    let pct = |v: &[usize], p: f64| v[(p * v.len() as f64 - 1.0).max(0.0) as usize];
    let n = values.len() as f64;
    println!(
        "  {}: avg={:.1}, min={}, p50={}, p90={}, p99={}, max={}",
        label,
        values.iter().sum::<usize>() as f64 / n,
        values.first().unwrap_or(&0),
        pct(values, 0.50),
        pct(values, 0.90),
        pct(values, 0.99),
        values.last().unwrap_or(&0),
    );
}

fn save_ground_truth(path: &std::path::Path, queries: &[Query]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let mut f = match std::fs::File::create(path) {
        Ok(f) => std::io::BufWriter::new(f),
        Err(_) => return,
    };
    let n = queries.len() as u32;
    f.write_all(&n.to_le_bytes()).ok();
    for q in queries {
        let dim = q.vector.len() as u32;
        f.write_all(&dim.to_le_bytes()).ok();
        for &v in &q.vector {
            f.write_all(&v.to_le_bytes()).ok();
        }
        let nn = q.neighbors.len() as u32;
        f.write_all(&nn.to_le_bytes()).ok();
        for &id in &q.neighbors {
            f.write_all(&id.to_le_bytes()).ok();
        }
        f.write_all(&q.max_vector_id.to_le_bytes()).ok();
    }
}

fn load_ground_truth(path: &std::path::Path) -> Option<Vec<Query>> {
    let data = std::fs::read(path).ok()?;
    let mut pos = 0usize;
    let r32 = |p: &mut usize| -> Option<u32> {
        if *p + 4 > data.len() {
            return None;
        }
        let v = u32::from_le_bytes(data[*p..*p + 4].try_into().ok()?);
        *p += 4;
        Some(v)
    };
    let r64 = |p: &mut usize| -> Option<u64> {
        if *p + 8 > data.len() {
            return None;
        }
        let v = u64::from_le_bytes(data[*p..*p + 8].try_into().ok()?);
        *p += 8;
        Some(v)
    };
    let rf32 = |p: &mut usize| -> Option<f32> {
        if *p + 4 > data.len() {
            return None;
        }
        let v = f32::from_le_bytes(data[*p..*p + 4].try_into().ok()?);
        *p += 4;
        Some(v)
    };
    let n = r32(&mut pos)? as usize;
    let mut queries = Vec::with_capacity(n);
    for _ in 0..n {
        let dim = r32(&mut pos)? as usize;
        let mut vector = Vec::with_capacity(dim);
        for _ in 0..dim {
            vector.push(rf32(&mut pos)?);
        }
        let nn = r32(&mut pos)? as usize;
        let mut neighbors = Vec::with_capacity(nn);
        for _ in 0..nn {
            neighbors.push(r32(&mut pos)?);
        }
        let max_vector_id = r64(&mut pos)?;
        queries.push(Query {
            vector,
            neighbors,
            max_vector_id,
        });
    }
    Some(queries)
}

fn group_queries_by_checkpoint(queries: Vec<Query>) -> BTreeMap<u64, Vec<Query>> {
    let mut map: BTreeMap<u64, Vec<Query>> = BTreeMap::new();
    for q in queries {
        map.entry(q.max_vector_id).or_default().push(q);
    }
    map
}

// =============================================================================
// Checkpoint Persistence
// =============================================================================

const BLOCK_SIZE_BYTES: usize = 3 * 1024 * 1024;

#[derive(serde::Serialize, serde::Deserialize)]
struct CheckpointMeta {
    checkpoint_idx: usize,
    total_vectors: usize,
    posting_list_id: String,
    scalar_metadata_id: String,
    vector_data_id: String,
    leaf_node_id: String,
    internal_node_id: String,
}

impl CheckpointMeta {
    fn from_ids(checkpoint_idx: usize, total_vectors: usize, ids: &HierarchicalSpannIds) -> Self {
        Self {
            checkpoint_idx,
            total_vectors,
            posting_list_id: ids.posting_list_id.to_string(),
            scalar_metadata_id: ids.scalar_metadata_id.to_string(),
            vector_data_id: ids.vector_data_id.to_string(),
            leaf_node_id: ids.leaf_node_id.to_string(),
            internal_node_id: ids.internal_node_id.to_string(),
        }
    }

    fn to_ids(&self) -> HierarchicalSpannIds {
        HierarchicalSpannIds {
            posting_list_id: self
                .posting_list_id
                .parse()
                .expect("invalid posting_list_id UUID"),
            scalar_metadata_id: self
                .scalar_metadata_id
                .parse()
                .expect("invalid scalar_metadata_id UUID"),
            vector_data_id: self
                .vector_data_id
                .parse()
                .expect("invalid vector_data_id UUID"),
            leaf_node_id: self
                .leaf_node_id
                .parse()
                .expect("invalid leaf_node_id UUID"),
            internal_node_id: self
                .internal_node_id
                .parse()
                .expect("invalid internal_node_id UUID"),
        }
    }
}

fn save_checkpoint_meta(
    save_dir: &Path,
    checkpoint_idx: usize,
    total_vectors: usize,
    ids: &HierarchicalSpannIds,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let meta = CheckpointMeta::from_ids(checkpoint_idx, total_vectors, ids);
    let json = serde_json::to_string_pretty(&meta)?;
    let path = save_dir.join("checkpoint.json");
    // Atomic write: write to a temp file in the same directory and rename
    // over the destination. This guarantees readers never observe a torn
    // file on crash, and -- crucially for the iteration-experiment workflow
    // (see docs/README.md) -- it breaks any hardlink that may exist on the
    // destination so the source `checkpoint.json` of a `cp -al`'d directory
    // is never truncated.
    let tmp_path = save_dir.join("checkpoint.json.tmp");
    std::fs::write(&tmp_path, json)?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

fn load_checkpoint_meta(
    save_dir: &Path,
) -> Result<CheckpointMeta, Box<dyn std::error::Error + Send + Sync>> {
    let path = save_dir.join("checkpoint.json");
    let json = std::fs::read_to_string(&path)?;
    let meta: CheckpointMeta = serde_json::from_str(&json)?;
    Ok(meta)
}

async fn make_blockfile_provider(storage_path: &Path, max_cache_bytes: u64) -> BlockfileProvider {
    let storage = Storage::Local(LocalStorage::new(storage_path.to_str().unwrap()));
    let (block_cache, sparse_index_cache) = if max_cache_bytes == 0 {
        // Legacy unbounded cache. Useful when correctness depends on
        // never evicting (e.g. small/medium runs), but grows monotonically
        // and will eventually OOM the bench on long runs.
        (new_cache_for_test(), new_cache_for_test())
    } else {
        // Foyer's `capacity` is the total weight budget; the cache
        // weighter returns each value's `Weighted::weight()`. Crucially:
        //   * `Block::weight()` returns MiB (rounded up, min 1), NOT
        //     bytes (see rust/blockstore/src/arrow/block/types.rs). So
        //     the block cache capacity must be in MiB units. Passing
        //     bytes here makes the cap ~1M times too large and the cache
        //     effectively unbounded.
        //   * `RootReader::weight()` returns 1 (a count), so the
        //     sparse-index cache capacity is in #-of-readers, not bytes.
        const MIB_BYTES: u64 = 1024 * 1024;
        let block_cap_mib = (max_cache_bytes / MIB_BYTES).max(64) as usize;
        // Each cached RootReader is small (KBs); 4096 entries is plenty
        // of headroom for any benchmark workload but keeps it bounded.
        let root_cap_entries = 4096usize;
        let block_cfg = FoyerCacheConfig {
            name: "bench_block_cache".to_string(),
            capacity: block_cap_mib,
            ..FoyerCacheConfig::default()
        };
        let root_cfg = FoyerCacheConfig {
            name: "bench_sparse_index_cache".to_string(),
            capacity: root_cap_entries,
            ..FoyerCacheConfig::default()
        };
        let block_cache = block_cfg
            .build_memory_persistent()
            .await
            .expect("failed to build foyer block cache");
        let sparse_index_cache = root_cfg
            .build_memory_persistent()
            .await
            .expect("failed to build foyer sparse-index cache");
        (block_cache, sparse_index_cache)
    };
    let arrow_blockfile_provider = ArrowBlockfileProvider::new(
        storage,
        BLOCK_SIZE_BYTES,
        block_cache,
        sparse_index_cache,
        16,
    );
    BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider)
}

#[derive(Debug, Default, Clone, Copy)]
struct GcStats {
    kept_blocks: u64,
    kept_block_bytes: u64,
    swept_blocks: u64,
    swept_block_bytes: u64,
    kept_roots: u64,
    kept_root_bytes: u64,
    swept_roots: u64,
    swept_root_bytes: u64,
    skipped: u64,
    elapsed_ms: u64,
}

/// Remove block + sparse-index files in `data_dir` that are no longer
/// referenced by `live_ids`.
///
/// The blockfile provider is fork-on-commit: every commit produces fresh
/// root files (one per `HierarchicalSpannIds` field) which reference a
/// set of block UUIDs, most of which are inherited from the parent root.
/// Old roots and any blocks reachable only from old roots are dead the
/// instant `committed_ids` advances and the writer is reopened against
/// the new ids. Without GC those dead files stay on disk forever — at
/// dim=1024 / 1M vectors per CP that's ~25-35 GB of orphans per
/// checkpoint and quickly fills any save volume.
///
/// Only safe to call AFTER:
///   1. `commit()` and `flush()` have both succeeded (so all blocks
///      referenced by `live_ids` are durable on disk),
///   2. the writer has been reopened against `live_ids` (so the cache
///      reflects only the new roots), and
///   3. no concurrent reader/writer is active for any prior id.
///
/// Walks the on-disk layout directly (`<data_dir>/block/<uuid>` and
/// `<data_dir>/sparse_index/<uuid>`) rather than going through the
/// storage abstraction; this only works for `Storage::Local` (which the
/// bench always uses) and is much cheaper than enumerating via storage
/// (`fs::read_dir` + `unlink`, no async or RPC overhead).
async fn gc_blockfile_storage(
    provider: &BlockfileProvider,
    data_dir: &Path,
    live_ids: &HierarchicalSpannIds,
) -> Result<GcStats, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let arrow = match provider {
        BlockfileProvider::ArrowBlockfileProvider(p) => p,
        BlockfileProvider::HashMapBlockfileProvider(_) => {
            // In-memory blockfiles never touch disk; nothing to GC.
            return Ok(GcStats::default());
        }
    };

    // ---- Mark phase ----
    // Each `HierarchicalSpannIds` field is a sparse-index root. The bench
    // uses an empty prefix path (see `BlockfileWriterOptions::new("".to_string())`
    // in writer/persistence.rs), so on-disk layout is `block/<uuid>` and
    // `sparse_index/<uuid>` directly under `data_dir`.
    let root_ids = [
        live_ids.posting_list_id,
        live_ids.scalar_metadata_id,
        live_ids.vector_data_id,
        live_ids.leaf_node_id,
        live_ids.internal_node_id,
    ];
    let mut live_blocks: std::collections::HashSet<uuid::Uuid> = std::collections::HashSet::new();
    for root_id in &root_ids {
        let block_ids = arrow
            .get_all_block_ids(root_id, "")
            .await
            .map_err(|e| format!("get_all_block_ids({}) failed: {e}", root_id))?;
        live_blocks.extend(block_ids);
    }
    let live_roots: std::collections::HashSet<uuid::Uuid> = root_ids.iter().copied().collect();

    // ---- Sweep phase ----
    let mut stats = GcStats::default();
    sweep_dir(
        &data_dir.join("block"),
        &live_blocks,
        &mut stats.kept_blocks,
        &mut stats.kept_block_bytes,
        &mut stats.swept_blocks,
        &mut stats.swept_block_bytes,
        &mut stats.skipped,
    )?;
    sweep_dir(
        &data_dir.join("sparse_index"),
        &live_roots,
        &mut stats.kept_roots,
        &mut stats.kept_root_bytes,
        &mut stats.swept_roots,
        &mut stats.swept_root_bytes,
        &mut stats.skipped,
    )?;
    stats.elapsed_ms = start.elapsed().as_millis() as u64;
    Ok(stats)
}

/// Helper for `gc_blockfile_storage`: walk a directory, classify each file
/// by UUID against `live`, delete the dead ones, accumulate counts/bytes.
/// Non-UUID-named entries (and unreadable entries) are warn-and-skipped.
fn sweep_dir(
    dir: &Path,
    live: &std::collections::HashSet<uuid::Uuid>,
    kept: &mut u64,
    kept_bytes: &mut u64,
    swept: &mut u64,
    swept_bytes: &mut u64,
    skipped: &mut u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("read_dir({}): {e}", dir.display()).into()),
    };
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                eprintln!("  GC warn: read_dir entry in {}: {}", dir.display(), e);
                *skipped += 1;
                continue;
            }
        };
        let path = entry.path();
        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => {
                eprintln!("  GC warn: non-utf8 filename {}", path.display());
                *skipped += 1;
                continue;
            }
        };
        let uuid = match uuid::Uuid::parse_str(&file_name) {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "  GC warn: non-uuid filename in {}: {}",
                    dir.display(),
                    file_name
                );
                *skipped += 1;
                continue;
            }
        };
        let size = match entry.metadata() {
            Ok(m) => m.len(),
            Err(e) => {
                eprintln!("  GC warn: metadata({}): {}", path.display(), e);
                *skipped += 1;
                continue;
            }
        };
        if live.contains(&uuid) {
            *kept += 1;
            *kept_bytes = kept_bytes.saturating_add(size);
        } else {
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    *swept += 1;
                    *swept_bytes = swept_bytes.saturating_add(size);
                }
                Err(e) => {
                    eprintln!("  GC warn: remove_file({}): {}", path.display(), e);
                    *skipped += 1;
                }
            }
        }
    }
    Ok(())
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse_from(std::env::args().filter(|a| a != "--bench"));

    let write_level_taus = parse_level_taus(args.write_level_taus.as_deref())?;
    let write_level_min_pcts = parse_level_f64s(args.write_level_min_pcts.as_deref())?;
    let read_level_taus = parse_level_taus(args.read_level_taus.as_deref())?;
    let read_level_min_pcts = parse_level_f64s(args.read_level_min_pcts.as_deref())?;

    let distance_fn = args.metric.to_distance_function();

    let dataset: Box<dyn Dataset> = match args.dataset {
        DatasetType::DbPedia => Box::new(datasets::dbpedia::DbPedia::load().await?),
        DatasetType::Arxiv => Box::new(datasets::arxiv::Arxiv::load().await?),
        DatasetType::Sec => Box::new(datasets::sec::Sec::load().await?),
        DatasetType::MsMarco => Box::new(datasets::msmarco::MsMarco::load().await?),
        DatasetType::MsMarcoEn => Box::new(datasets::msmarco_en::MsMarcoEn::load().await?),
        DatasetType::WikipediaEn => Box::new(datasets::wikipedia::Wikipedia::load().await?),
        DatasetType::Sift => Box::new(datasets::sift::Sift::load().await?),
        DatasetType::BigAnnSift1b => Box::new(datasets::bigann_sift1b::BigAnnSift1b::load().await?),
        DatasetType::Deep10m => Box::new(datasets::deep::Deep10M::load().await?),
        DatasetType::Synthetic => Box::new(datasets::synthetic::Synthetic::load(
            args.dim,
            args.synthetic_size,
        )?),
    };

    let data_len = dataset.data_len();
    let dimension = dataset.dimension();
    let k = dataset.k();
    let batch_size = args.checkpoint_size;

    let max_checkpoints = (data_len + batch_size - 1) / batch_size;
    let num_checkpoints = args
        .checkpoint
        .unwrap_or(max_checkpoints)
        .min(max_checkpoints);

    let tau_values: Vec<f64> = args
        .recall_tau_values
        .split(',')
        .map(|s| s.trim().parse().expect("invalid tau value"))
        .collect();

    let write_nav = match args.write_navigation.as_str() {
        "fp" => NavigationMode::Fp,
        "4bit" => NavigationMode::FourBit,
        other => panic!("invalid --write-navigation value '{other}': must be fp or 4bit"),
    };

    let config = HierarchicalSpannConfig {
        branching_factor: args.branching_factor,
        split_threshold: args.split_threshold,
        merge_threshold: args.merge_threshold,
        write_beam_tau: args.write_beam_tau,
        write_beam_min: args.write_beam_min,
        write_beam_max: args.write_beam_max,
        write_level_taus: write_level_taus.clone(),
        write_level_min_pcts: write_level_min_pcts.clone(),
        // beam_tau: args.beam_tau,
        // beam_min: args.read_beam_min,
        // beam_max: args.read_beam_max,
        max_replicas: args.max_replicas,
        write_rng_epsilon: args.write_rng_epsilon,
        write_rng_factor: args.write_rng_factor,
        reassign_neighbor_count: 32,
        write_navigation: write_nav,
        fp_npa: args.fp_npa,
    };

    println!("=== 1-Bit Quantized Hierarchical SPANN Writer Benchmark ===");
    println!();
    println!("--- Dataset ---");
    println!(
        "  Source: {} ({} vectors, {} dims)",
        dataset.name(),
        format_count(data_len),
        dimension
    );
    println!(
        "  Metric: {:?} | Checkpoints: {} ({}/CP)",
        distance_fn,
        num_checkpoints,
        format_count(batch_size),
    );
    println!();
    println!("--- Indexing ---");
    println!(
        "  Tree: bf={} split={} merge={} replicas={} eps={} rng_f={}",
        config.branching_factor,
        config.split_threshold,
        config.merge_threshold,
        config.max_replicas,
        config.write_rng_epsilon,
        config.write_rng_factor,
    );
    println!(
        "  Write beam: tau={} min={} max={}",
        config.write_beam_tau, config.write_beam_min, config.write_beam_max,
    );
    if !write_level_taus.is_empty() || !write_level_min_pcts.is_empty() {
        println!(
            "  Write beam schedule: taus=[{}] min_pcts=[{}] leaf_min={} leaf_max={}",
            format_level_taus(&write_level_taus),
            write_level_min_pcts
                .iter()
                .map(|v| format!("{:.1}", v))
                .collect::<Vec<_>>()
                .join(","),
            config.write_beam_min,
            config.write_beam_max,
        );
    }
    println!(
        "  Quantization: 1-bit | Write nav: {:?} | Read nav: 4bit | NPA: {}",
        write_nav,
        if args.fp_npa { "f32" } else { "1x4" },
    );
    println!("  Threads: {}", args.threads);
    {
        let m = mem_probe::read_self();
        let sys_total = mem_probe::read_sys_total().unwrap_or(0);
        let sys_avail = mem_probe::read_sys_available().unwrap_or(0);
        println!(
            "  Process mem: rss={} peak={} vmsize={} | sys total={} avail={} | block cache cap={}",
            mem_probe::format_bytes(m.rss),
            mem_probe::format_bytes(m.rss_peak),
            mem_probe::format_bytes(m.vmsize),
            mem_probe::format_bytes(sys_total),
            mem_probe::format_bytes(sys_avail),
            if args.max_cache_bytes == 0 {
                "unbounded".to_string()
            } else {
                mem_probe::format_bytes(args.max_cache_bytes)
            },
        );
        if m.rss == 0 && cfg!(not(target_os = "linux")) {
            println!("  (process memory probes are Linux-only; values shown as `-` on this OS)");
        }
        let j = mem_probe::read_jemalloc();
        if j.allocated > 0 {
            println!(
                "  Jemalloc baseline (alloc/active/res/retained): {}/{}/{}/{}",
                mem_probe::format_bytes(j.allocated),
                mem_probe::format_bytes(j.active),
                mem_probe::format_bytes(j.resident),
                mem_probe::format_bytes(j.retained),
            );
        }
    }
    println!();

    let all_queries = dataset.queries(distance_fn.clone())?;
    let query_vectors: Vec<Vec<f32>> = all_queries
        .iter()
        .take(100)
        .map(|q| q.vector.clone())
        .collect();
    let queries_by_checkpoint = group_queries_by_checkpoint(all_queries);

    let sample_queries_as_gt = query_vectors.is_empty() && args.brute_force_gt;
    if sample_queries_as_gt {
        println!(
            "  No precomputed queries; will sample 100 data vectors as queries for brute-force GT."
        );
    }
    flush_stdout();

    let do_save = args.save || args.resume;
    let save_dir: Option<PathBuf> = if do_save {
        let dir: PathBuf = args
            .save_dir
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let wlt = args.write_level_taus.as_deref().unwrap_or("_");
                let wlp = args.write_level_min_pcts.as_deref().unwrap_or("_");
                let config_slug = format!(
                    "cs{}_bf{}_st{}_mt{}_wt{}_wmin{}_wmax{}_wlt{}_wlp{}_mr{}_eps{}_rng{}_wnav{}_npa{}",
                    args.checkpoint_size,
                    args.branching_factor,
                    args.split_threshold,
                    args.merge_threshold,
                    args.write_beam_tau,
                    args.write_beam_min,
                    args.write_beam_max,
                    wlt,
                    wlp,
                    args.max_replicas,
                    args.write_rng_epsilon,
                    args.write_rng_factor,
                    args.write_navigation,
                    args.fp_npa,
                );
                PathBuf::from(format!(
                    "target/hierarchical_cache/save/{}/{}",
                    dataset.name(),
                    config_slug,
                ))
            });
        std::fs::create_dir_all(&dir)?;
        Some(dir)
    } else {
        None
    };

    let mut start_checkpoint = 0usize;
    let mut total_vectors = 0usize;
    // We only need to retain every input vector for paths that brute-force
    // distances over the full corpus: brute-force GT generation, the
    // sampled-query GT path (gated by --brute-force-gt), and the optimal
    // GT baseline. Without these, retaining all vectors costs ~4 GB per
    // 1M vectors at dim=1024 and is the second-largest memory hog after
    // the unbounded blockfile cache (now bounded via --max-cache-bytes).
    let retain_indexed_vectors = args.brute_force_gt || args.compute_optimal_gt;
    let mut all_indexed_vectors: Vec<(u32, Arc<[f32]>)> = Vec::new();

    // Persistent provider + data directory used across all checkpoints and for
    // the final recall reader. With fork-based commits, new blockfiles share
    // blocks with their parents in the same storage directory, so we can't
    // swap directories between commits.
    let effective_dir: PathBuf = save_dir.as_ref().cloned().unwrap_or_else(|| {
        let d = PathBuf::from("target/hierarchical_cache/_tmp_recall");
        std::fs::create_dir_all(&d).expect("failed to create temp save dir");
        d
    });
    let data_dir = effective_dir.join("data");
    std::fs::create_dir_all(&data_dir)?;
    let provider = make_blockfile_provider(&data_dir, args.max_cache_bytes).await;

    let mut committed_ids: Option<HierarchicalSpannIds> = None;

    let mut writer = if args.resume {
        let dir = save_dir
            .as_ref()
            .expect("--resume requires --save or --save-dir");
        let meta = load_checkpoint_meta(dir)?;
        start_checkpoint = meta.checkpoint_idx + 1;
        total_vectors = meta.total_vectors;
        let ids = meta.to_ids();
        committed_ids = Some(ids.clone());

        println!(
            "  Resuming from checkpoint {} ({} vectors)",
            start_checkpoint,
            format_count(total_vectors)
        );
        flush_stdout();

        print!("  Opening saved index...");
        flush_stdout();
        let open_start = Instant::now();
        let w = HierarchicalSpannWriter::open(&provider, ids, distance_fn.clone(), config.clone())
            .await
            .map_err(|e| format!("failed to open index for resume: {e}"))?;
        println!(" {}", format_duration(open_start.elapsed()));
        flush_stdout();

        println!("  Lazy resume: postings and embeddings will load on demand.");
        flush_stdout();

        w
    } else {
        HierarchicalSpannWriter::new(dimension, distance_fn.clone(), config.clone())
    };

    let total_start = Instant::now();
    let mut prev_snapshot = WriterStatsSnapshot::default();
    let mut all_snapshots: Vec<WriterStatsSnapshot> = Vec::new();
    let temp_recall_dir: Option<PathBuf> = if do_save {
        None
    } else {
        Some(effective_dir.clone())
    };

    // Background sampler so per-checkpoint peak RSS isn't missed between
    // phase-boundary point samples. Polls every 250ms.
    let rss_sampler = RssSampler::spawn(Duration::from_millis(250));
    let _ = rss_sampler.take_interval_peak();

    for checkpoint_idx in start_checkpoint..num_checkpoints {
        let offset = checkpoint_idx * batch_size;
        let limit = batch_size.min(data_len.saturating_sub(offset));

        if limit == 0 {
            println!("Checkpoint {}: No more data", checkpoint_idx + 1);
            break;
        }

        let mem_at_cp_start = mem_probe::read_self();
        let jem_at_cp_start = mem_probe::read_jemalloc();

        let load_start = Instant::now();
        let batch_vectors = dataset.load_range(offset, limit)?;
        let load_time = load_start.elapsed();
        let actual_count = batch_vectors.len();

        if actual_count == 0 {
            println!("Checkpoint {}: No vectors loaded", checkpoint_idx + 1);
            break;
        }

        // Index vectors
        let index_start = Instant::now();
        let progress = ProgressBar::new(actual_count as u64);
        progress.set_style(
            ProgressStyle::default_bar()
                .template(&format!(
                    "[CP {}/{} Add] {{wide_bar}} {{pos}}/{{len}} [{{elapsed_precise}}<{{eta_precise}}]",
                    checkpoint_idx + 1,
                    num_checkpoints
                ))
                .unwrap(),
        );

        let num_threads = args.threads;
        let early_balance_size = 100_000usize;
        let needs_early_balance = batch_size > early_balance_size && total_vectors < 1_000_000;

        let batches: Vec<&[(u32, Arc<[f32]>)]> = if needs_early_balance {
            let mut subs = Vec::new();
            let mut remaining = &batch_vectors[..];
            let mut running_total = total_vectors;
            while !remaining.is_empty() && running_total < 1_000_000 {
                let take = early_balance_size
                    .min(remaining.len())
                    .min(1_000_000 - running_total);
                subs.push(&remaining[..take]);
                remaining = &remaining[take..];
                running_total += take;
            }
            if !remaining.is_empty() {
                subs.push(remaining);
            }
            subs
        } else {
            vec![&batch_vectors[..]]
        };

        let mut balance_time = Duration::ZERO;

        for batch in &batches {
            if num_threads <= 1 {
                for (id, embedding) in *batch {
                    writer.add(*id, embedding);
                    progress.inc(1);
                }
            } else {
                let chunk_size = (batch.len() + num_threads - 1) / num_threads;
                let writer_ref = &writer;
                let progress_ref = &progress;
                std::thread::scope(|s| {
                    for chunk in batch.chunks(chunk_size) {
                        s.spawn(move || {
                            for (id, embedding) in chunk {
                                writer_ref.add(*id, embedding);
                                progress_ref.inc(1);
                            }
                        });
                    }
                });
            }

            let balance_start = Instant::now();
            progress.suspend(|| {
                if args.parallel_balancing {
                    writer.balance_index_parallel(args.threads);
                } else {
                    writer.balance_index();
                }
            });
            balance_time += balance_start.elapsed();
        }
        progress.finish_and_clear();
        let index_time = index_start.elapsed() - balance_time;
        let mem_after_balance = mem_probe::read_self();
        let jem_after_balance = mem_probe::read_jemalloc();
        let writer_mem_after_balance = writer.memory_usage();
        // Per-reader pinned-block stats *before* we (optionally) clear
        // them, so the printed numbers reflect the actual peak we paid
        // for during add+balance. Both readers' loaded_blocks HashMaps
        // are independent of the foyer block cache and grow until the
        // reader is dropped (see docs/README.md -> "Reader-side block
        // pinning"). The bytes column undercounts the heap footprint
        // by the Box header + RecordBatch metadata + validity bitmaps;
        // expect the true footprint to be ~1.3x larger.
        let (reader_pl_pins_at_balance, reader_vd_pins_at_balance) =
            writer.reader_block_pin_stats();
        // Drop the pins now (before commit), capping the post-clear
        // RSS at roughly (writer.memory_usage + foyer block cache cap +
        // jemalloc slack). Safe because all `add`/`balance` calls have
        // returned by this point and no value borrowed from either
        // reader is still live on the stack (the writer's `load`/
        // `load_raw` paths copy via `to_vec()` and drop the returned
        // value before returning).
        if args.clear_reader_block_pins {
            writer.clear_reader_block_pins();
        }
        let mem_after_clear_pins = if args.clear_reader_block_pins {
            Some(mem_probe::read_self())
        } else {
            None
        };

        total_vectors += actual_count;
        if retain_indexed_vectors {
            all_indexed_vectors.extend(batch_vectors.iter().cloned());
        }

        let mut delta = writer.stats.snapshot_delta(&prev_snapshot);
        delta.wall_nanos = (index_time + balance_time).as_nanos() as u64;
        all_snapshots.push(delta.clone());

        // --- Commit to disk + reopen index ---
        //
        // Every checkpoint commits (forking from the previously committed
        // blockfiles, if any), then drops the writer and re-opens it from
        // the freshly committed ids. This:
        //   (1) keeps memory usage stable across checkpoints (embeddings /
        //       versions / materialized postings all reset at the boundary),
        //   (2) exercises open() and reports its timing per checkpoint, and
        //   (3) validates correctness of the commit/open round-trip.
        // Split commit into its two phases for memory attribution:
        //   - fork:  writer.commit() - builds the in-memory delta tree,
        //            forks blockfiles (copy-on-write of root readers),
        //            returns a Flusher holding all the dirty blocks.
        //   - flush: flusher.flush() - serializes dirty blocks to Arrow
        //            record batches and writes them to local storage.
        // Historically the +192 GB CP47 spike (balanced -> committed)
        // was opaque; with this split we can see whether it lands in
        // the fork (in-memory delta construction) or the flush (Arrow
        // serialization buffers). sys_avail is sampled at each boundary
        // so we can distinguish hard allocation from MADV_FREE'd RSS.
        let sys_avail_before_commit = mem_probe::read_sys_available().unwrap_or(0);

        // `--no-commit` short-circuits the whole commit/flush/save/reopen/GC
        // block. We still populate every variable consumed by the per-CP
        // print statements below so output remains shape-compatible (just
        // with zero durations and no-op deltas). The writer is kept alive
        // across iterations: subsequent `add()`s see the previous CP's
        // working set, and the post-loop recall section runs against the
        // originally resumed `committed_ids`.
        let (
            fork_time,
            flush_time,
            commit_time,
            reopen_time,
            mem_after_fork,
            mem_after_flush,
            mem_after_reopen,
            jem_after_fork,
            jem_after_flush,
            jem_after_reopen,
            sys_avail_after_fork,
            sys_avail_after_flush,
            writer_mem_after_reopen,
            gc_stats,
        ) = if args.no_commit {
            let same_mem = mem_after_balance.clone();
            let same_jem = jem_after_balance;
            (
                Duration::ZERO,
                Duration::ZERO,
                Duration::ZERO,
                Duration::ZERO,
                same_mem.clone(),
                same_mem.clone(),
                same_mem,
                same_jem,
                same_jem,
                same_jem,
                sys_avail_before_commit,
                sys_avail_before_commit,
                writer_mem_after_balance.clone(),
                None,
            )
        } else {
            let fork_start = Instant::now();
            let flusher = writer
                .commit(&provider, committed_ids.as_ref())
                .await
                .map_err(|e| format!("commit failed: {e}"))?;
            let fork_time = fork_start.elapsed();
            let mem_after_fork = mem_probe::read_self();
            let jem_after_fork = mem_probe::read_jemalloc();
            let sys_avail_after_fork = mem_probe::read_sys_available().unwrap_or(0);

            let flush_start = Instant::now();
            let ids = flusher
                .flush()
                .await
                .map_err(|e| format!("flush failed: {e}"))?;
            let flush_time = flush_start.elapsed();
            let mem_after_flush = mem_probe::read_self();
            let jem_after_flush = mem_probe::read_jemalloc();
            let sys_avail_after_flush = mem_probe::read_sys_available().unwrap_or(0);

            if do_save {
                let dir = save_dir.as_ref().unwrap();
                save_checkpoint_meta(dir, checkpoint_idx, total_vectors, &ids)?;
            }
            committed_ids = Some(ids.clone());
            let commit_time = fork_time + flush_time;

            // Drop the old writer and re-open clean from the just-committed ids.
            // This bounds per-checkpoint memory at the cost of one open() per
            // checkpoint (primarily a scan of the node-level metadata rows).
            drop(writer);
            let reopen_start = Instant::now();
            writer =
                HierarchicalSpannWriter::open(&provider, ids, distance_fn.clone(), config.clone())
                    .await
                    .map_err(|e| format!("failed to reopen index after commit: {e}"))?;
            let reopen_time = reopen_start.elapsed();
            let mem_after_reopen = mem_probe::read_self();
            let jem_after_reopen = mem_probe::read_jemalloc();
            let writer_mem_after_reopen = writer.memory_usage();

            // GC orphaned blockfile data left behind by previous commits.
            // Safe here: commit/flush succeeded, the writer has been
            // reopened against the new ids, and `committed_ids` was updated
            // above — so no in-flight reader/writer references the prior
            // root ids. Only useful when --save is set (otherwise the
            // temp/_tmp_recall directory is fully replaced each CP).
            let gc_stats = if args.gc_blockfiles && do_save {
                let live_ids = committed_ids
                    .as_ref()
                    .expect("committed_ids must be set after a commit");
                match gc_blockfile_storage(&provider, &data_dir, live_ids).await {
                    Ok(s) => Some(s),
                    Err(e) => {
                        eprintln!("  GC warn: aborted: {e}");
                        None
                    }
                }
            } else {
                None
            };

            (
                fork_time,
                flush_time,
                commit_time,
                reopen_time,
                mem_after_fork,
                mem_after_flush,
                mem_after_reopen,
                jem_after_fork,
                jem_after_flush,
                jem_after_reopen,
                sys_avail_after_fork,
                sys_avail_after_flush,
                writer_mem_after_reopen,
                gc_stats,
            )
        };
        // Capture (and reset) the interval-peak RSS and interval-min
        // sys_avail observed by the background sampler since the
        // previous checkpoint boundary. The sys_avail trough is the
        // "true" peak hard-allocation: when sys_avail drops, the kernel
        // had to commit RAM to satisfy the bench's allocations; when
        // RSS rises but sys_avail stays flat, the rise is just
        // MADV_FREE'd pages still counting against the process.
        let interval_peak_rss = rss_sampler.take_interval_peak();
        let interval_min_sys_avail = rss_sampler.take_interval_min_sys_avail().unwrap_or(0);
        let sys_avail = mem_probe::read_sys_available().unwrap_or(0);
        // Stats counters reset on reopen; reset the rolling snapshot so next
        // iteration's delta is computed against the fresh writer.
        prev_snapshot = WriterStatsSnapshot::default();

        let throughput = actual_count as f64 / index_time.as_secs_f64();
        let checkpoint_total = index_time + balance_time + load_time + commit_time + reopen_time;

        println!(
            "--- Checkpoint {} ({} total) ---",
            checkpoint_idx + 1,
            format_count(total_vectors),
        );
        println!(
            "  Indexed {} vec in {} ({:.0} vec/s) | balance {} ({} iterations) | load {} | commit {} | reopen {} | total {}",
            format_count(actual_count),
            format_duration(index_time),
            throughput,
            format_duration(balance_time),
            delta.balance_rounds,
            format_duration(load_time),
            format_duration(commit_time),
            format_duration(reopen_time),
            format_duration(checkpoint_total),
        );

        // Per-checkpoint lazy-IO summary. The writer was reopened above so
        // these counters reflect work done in this checkpoint only.
        // Posting list bytes ≈ entries * (4 [id u32] + code_size [u8 codes]
        // + 1 [version u8]). Embedding bytes = count * dim * 4.
        {
            let posting_loads = delta.posting_loads;
            let posting_entries = delta.posting_load_entries;
            let posting_bytes =
                posting_entries.saturating_mul((4 + (dimension as u64 / 8) + 1) as u64);
            let embedding_loads = delta.embedding_loads;
            let embedding_bytes = embedding_loads.saturating_mul((dimension as u64) * 4);
            let added = delta.embeddings_added;
            let added_bytes = added.saturating_mul((dimension as u64) * 4);
            let total_io_bytes = posting_bytes + embedding_bytes + added_bytes;
            println!(
                "  Lazy IO: postings {} nodes / {} entries ({}) | embeddings loaded {} ({}) | added {} ({}) | total {}",
                format_count(posting_loads as usize),
                format_count(posting_entries as usize),
                format_mb(posting_bytes as f64 / (1024.0 * 1024.0)),
                format_count(embedding_loads as usize),
                format_mb(embedding_bytes as f64 / (1024.0 * 1024.0)),
                format_count(added as usize),
                format_mb(added_bytes as f64 / (1024.0 * 1024.0)),
                format_mb(total_io_bytes as f64 / (1024.0 * 1024.0)),
            );
        }

        // Per-checkpoint process RSS trace. SIGKILL with no panic almost
        // always means the OOM killer fired on Linux; this line lets us
        // see the trajectory leading up to the kill (the *previous*
        // checkpoint's line is the last thing we'll see).
        //
        //  - start:    RSS at start of CP (after previous reopen released)
        //  - balanced: RSS after add+balance (peak of writer working set)
        //  - committed: RSS after flush (blockfile cache may grow here)
        //  - reopened: RSS after dropping writer and reopening
        //  - peak:     max RSS observed by background sampler this CP
        //  - lifetime peak: VmHWM (process-lifetime high water mark)
        //  - sys avail: MemAvailable from /proc/meminfo
        // Estimate the byte cost of the retained-vector buffer so we can
        // attribute RSS: rss ~= retained_vectors + cache + writer working
        // set + allocator slack. `retained_vectors` is exact; the rest is
        // bounded above by `--max-cache-bytes` plus the lazy IO total
        // printed above.
        let retained_vectors_bytes = if retain_indexed_vectors {
            (all_indexed_vectors.len() as u64)
                .saturating_mul((4 + dimension as u64 * 4 + 16) as u64)
        } else {
            0
        };
        let rss_minus_vectors = mem_after_reopen.rss.saturating_sub(retained_vectors_bytes);
        println!(
            "  Process mem: start {} -> balanced {} -> forked {} -> flushed {} -> reopened {} | cp peak {} | lifetime peak {} | sys avail {} (min {}) | retained vectors {} ({}) | rss - vectors {}",
            mem_probe::format_bytes(mem_at_cp_start.rss),
            mem_probe::format_bytes(mem_after_balance.rss),
            mem_probe::format_bytes(mem_after_fork.rss),
            mem_probe::format_bytes(mem_after_flush.rss),
            mem_probe::format_bytes(mem_after_reopen.rss),
            mem_probe::format_bytes(interval_peak_rss),
            mem_probe::format_bytes(mem_after_reopen.rss_peak),
            mem_probe::format_bytes(sys_avail),
            mem_probe::format_bytes(interval_min_sys_avail),
            format_count(all_indexed_vectors.len()),
            mem_probe::format_bytes(retained_vectors_bytes),
            mem_probe::format_bytes(rss_minus_vectors),
        );

        // RssAnon vs RssFile breakdown of the same RSS values.
        // RssFile should be small (~MB, libraries only) since we don't
        // mmap parquet/blockfile data. If it grows, something started
        // mmaping. RssAnon is "heap RSS" -- this is what jemalloc owns
        // and what we want to attribute to allocator vs application.
        println!(
            "  RSS split (anon|file): start {}|{} -> balanced {}|{} -> forked {}|{} -> flushed {}|{} -> reopened {}|{}",
            mem_probe::format_bytes(mem_at_cp_start.rss_anon),
            mem_probe::format_bytes(mem_at_cp_start.rss_file),
            mem_probe::format_bytes(mem_after_balance.rss_anon),
            mem_probe::format_bytes(mem_after_balance.rss_file),
            mem_probe::format_bytes(mem_after_fork.rss_anon),
            mem_probe::format_bytes(mem_after_fork.rss_file),
            mem_probe::format_bytes(mem_after_flush.rss_anon),
            mem_probe::format_bytes(mem_after_flush.rss_file),
            mem_probe::format_bytes(mem_after_reopen.rss_anon),
            mem_probe::format_bytes(mem_after_reopen.rss_file),
        );

        // Jemalloc internal accounting at the same five probe points.
        // The key signal is `allocated`: that's what malloc-callers think
        // they're holding right now (before frees). If `allocated` at
        // "balanced" matches our writer.memory_usage() output, the gap
        // between `allocated` and `resident` is allocator slack and the
        // writer accounting is honest. If `allocated` is much larger
        // than writer.memory_usage(), there's live heap we're not tracking
        // (almost certainly inside chroma_blockstore -- e.g. cached
        // Block bytes held by Arc<Block> references that the foyer cache
        // can't evict).
        //
        // Format: each phase shows `alloc / active / resident / retained`.
        // `mapped` is omitted to keep the line readable; it's almost
        // always `resident + retained + small overhead`.
        let fmt_jem = |j: &mem_probe::JemallocSnapshot| {
            format!(
                "{}/{}/{}/{}",
                mem_probe::format_bytes(j.allocated),
                mem_probe::format_bytes(j.active),
                mem_probe::format_bytes(j.resident),
                mem_probe::format_bytes(j.retained),
            )
        };
        println!(
            "  Jemalloc (alloc/active/res/retained): start {} -> balanced {} -> forked {} -> flushed {} -> reopened {}",
            fmt_jem(&jem_at_cp_start),
            fmt_jem(&jem_after_balance),
            fmt_jem(&jem_after_fork),
            fmt_jem(&jem_after_flush),
            fmt_jem(&jem_after_reopen),
        );

        // Commit phase attribution. Two complementary views:
        //
        //   RSS delta  = how much resident memory the phase added.
        //                Includes MADV_FREE'd pages, so it can over-
        //                estimate the true cost.
        //   avail drop = how much MemAvailable fell during the phase.
        //                This is the *kernel's* view of hard-allocated
        //                memory the bench took from the system. Smaller
        //                than the RSS delta when jemalloc is decaying
        //                pages back to the OS in real time.
        //
        // If avail-drop in flush is much smaller than the RSS delta in
        // flush, the spike is mostly soft (MADV_FREE) and OOM risk is
        // overstated. If avail-drop matches the RSS delta, the spike
        // is real and we need to bound serialization buffers.
        let fork_rss_delta = mem_after_fork.rss as i64 - mem_after_balance.rss as i64;
        let flush_rss_delta = mem_after_flush.rss as i64 - mem_after_fork.rss as i64;
        let fork_avail_drop = sys_avail_before_commit as i64 - sys_avail_after_fork as i64;
        let flush_avail_drop = sys_avail_after_fork as i64 - sys_avail_after_flush as i64;
        println!(
            "  Commit phases: fork {} (rss {} {}, avail {} {}) | flush {} (rss {} {}, avail {} {})",
            format_duration(fork_time),
            if fork_rss_delta >= 0 { "+" } else { "-" },
            mem_probe::format_bytes(fork_rss_delta.unsigned_abs()),
            if fork_avail_drop >= 0 { "-" } else { "+" },
            mem_probe::format_bytes(fork_avail_drop.unsigned_abs()),
            format_duration(flush_time),
            if flush_rss_delta >= 0 { "+" } else { "-" },
            mem_probe::format_bytes(flush_rss_delta.unsigned_abs()),
            if flush_avail_drop >= 0 { "-" } else { "+" },
            mem_probe::format_bytes(flush_avail_drop.unsigned_abs()),
        );

        // Writer-owned heap accounting at two interesting moments:
        //   - balanced: post add+balance, before commit/drop. This is the
        //     writer's working-set peak.
        //   - reopened: after dropping the writer and re-opening from
        //     committed ids. Should be tiny (just the persisted tree
        //     metadata + tombstones=0 + balancing=0). If this number
        //     drifts up CP-over-CP, reopen is leaking state.
        // The gap between (rss - vectors - cache_cap - writer_total) is
        // unaccounted RSS — almost certainly allocator-retained dirty
        // pages or live blockfile cache pages above the cap.
        // Reader-side block pin accounting for the "balanced" probe:
        // payload bytes pinned in each reader's loaded_blocks HashMap.
        // This is the dominant chunk of unaccounted RSS during add+
        // balance (see docs/README.md -> "Reader-side block pinning").
        let (pl_count, pl_bytes) = reader_pl_pins_at_balance;
        let (vd_count, vd_bytes) = reader_vd_pins_at_balance;
        let pinned_total_bytes = pl_bytes.saturating_add(vd_bytes);
        let cleared_str = match mem_after_clear_pins.as_ref() {
            Some(snap) => {
                let dropped = mem_after_balance.rss.saturating_sub(snap.rss);
                format!(
                    " | post-clear rss {} (-{}, freed pl+vd={})",
                    mem_probe::format_bytes(snap.rss),
                    mem_probe::format_bytes(dropped),
                    mem_probe::format_bytes(pinned_total_bytes),
                )
            }
            None => String::new(),
        };
        println!(
            "  Reader pins (balanced): postings {} blocks/{} | vector_data {} blocks/{} | total {}{}",
            format_count(pl_count),
            mem_probe::format_bytes(pl_bytes),
            format_count(vd_count),
            mem_probe::format_bytes(vd_bytes),
            mem_probe::format_bytes(pinned_total_bytes),
            cleared_str,
        );

        let wb = &writer_mem_after_balance;
        let wr = &writer_mem_after_reopen;
        println!(
            "  Writer mem: balanced total={} (tree={} centroids={} postings={} embeddings={}x{} versions={}x{} sets={}+{} dirty={}n+{}v+{}e) | reopened total={} (tree={} centroids={} postings={} embeddings={}x{} versions={}x{} sets={}+{} dirty={}n+{}v+{}e)",
            mem_probe::format_bytes(wb.total_bytes()),
            mem_probe::format_bytes(wb.tree_bytes),
            mem_probe::format_bytes(wb.centroid_bytes),
            mem_probe::format_bytes(wb.posting_bytes),
            format_count(wb.embedding_count as usize),
            mem_probe::format_bytes(wb.embedding_bytes),
            format_count(wb.versions_count as usize),
            mem_probe::format_bytes(wb.versions_bytes),
            format_count(wb.tombstones_count as usize),
            format_count(wb.balancing_count as usize),
            format_count(wb.dirty_nodes_count as usize),
            format_count(wb.dirty_versions_count as usize),
            format_count(wb.dirty_embeddings_count as usize),
            mem_probe::format_bytes(wr.total_bytes()),
            mem_probe::format_bytes(wr.tree_bytes),
            mem_probe::format_bytes(wr.centroid_bytes),
            mem_probe::format_bytes(wr.posting_bytes),
            format_count(wr.embedding_count as usize),
            mem_probe::format_bytes(wr.embedding_bytes),
            format_count(wr.versions_count as usize),
            mem_probe::format_bytes(wr.versions_bytes),
            format_count(wr.tombstones_count as usize),
            format_count(wr.balancing_count as usize),
            format_count(wr.dirty_nodes_count as usize),
            format_count(wr.dirty_versions_count as usize),
            format_count(wr.dirty_embeddings_count as usize),
        );

        if let Some(gc) = gc_stats {
            let kept_total_bytes = gc.kept_block_bytes.saturating_add(gc.kept_root_bytes);
            println!(
                "  GC: kept {} blocks ({}) + {} roots ({}) | swept {} blocks ({}) + {} roots ({}) | skipped {} | took {}ms | live disk {}",
                format_count(gc.kept_blocks as usize),
                mem_probe::format_bytes(gc.kept_block_bytes),
                format_count(gc.kept_roots as usize),
                mem_probe::format_bytes(gc.kept_root_bytes),
                format_count(gc.swept_blocks as usize),
                mem_probe::format_bytes(gc.swept_block_bytes),
                format_count(gc.swept_roots as usize),
                mem_probe::format_bytes(gc.swept_root_bytes),
                gc.skipped,
                gc.elapsed_ms,
                mem_probe::format_bytes(kept_total_bytes),
            );
        }

        flush_stdout();
    }

    println!("\n=== Build Summary ===");
    // Materialize lazy leaf shells so per-leaf diagnostics (counts, GT cluster
    // coverage, etc.) reflect actual on-disk contents and not just whatever
    // happened to be touched during the last checkpoint.
    if let Err(e) = writer.load_all_postings().await {
        eprintln!("warning: failed to load postings for build summary: {e}");
    }
    writer.print_tree_stats(format_count, Some(total_vectors));
    let total_time = total_start.elapsed();
    let overall_throughput = total_vectors as f64 / total_time.as_secs_f64();

    println!("--- Summary ---");
    println!(
        "Total vectors: {} | Total time: {} | Overall: {:.0} vec/s\n",
        format_count(total_vectors),
        format_duration(total_time),
        overall_throughput,
    );
    println!("{}", format_task_tables(&all_snapshots));
    println!("{}", format_data_loaded_table(&all_snapshots, dimension));
    flush_stdout();

    // Queries are grouped by `max_vector_id`. Datasets like msmarco-en tag every query with the
    // full corpus length (`data_len`), but runs often stop early (e.g. 100M). Try exact indexed
    // count first; only fall back to the full-corpus bucket when brute-force GT is disabled
    // (the full-corpus GT references vectors outside the indexed prefix and recall against it
    // is bounded by |GT ∩ indexed| / |GT|, which is ~0 for small prefixes of large datasets).
    let exact_precomputed: Vec<&Query> = queries_by_checkpoint
        .get(&(total_vectors as u64))
        .map(|qs| qs.iter().collect())
        .unwrap_or_default();
    let precomputed: Vec<&Query> = if !exact_precomputed.is_empty() {
        exact_precomputed
    } else if !args.brute_force_gt && total_vectors <= data_len {
        let qs = queries_by_checkpoint
            .get(&(data_len as u64))
            .map(|qs| qs.iter().collect::<Vec<_>>())
            .unwrap_or_default();
        if !qs.is_empty() {
            println!(
                "  Precomputed GT: using queries keyed at full corpus ({} vectors); indexed {} vectors. Recall will be bounded by GT ∩ indexed.",
                format_count(data_len),
                format_count(total_vectors),
            );
            flush_stdout();
        }
        qs
    } else {
        Vec::new()
    };
    if !precomputed.is_empty() && args.brute_force_gt {
        println!(
            "  Precomputed GT keyed exactly at indexed total ({}).",
            format_count(total_vectors),
        );
        flush_stdout();
    }

    let sampled_query_vectors: Vec<Vec<f32>>;
    let effective_query_vectors = if sample_queries_as_gt {
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        let sample_count = 100.min(all_indexed_vectors.len());
        let mut indices: Vec<usize> = (0..all_indexed_vectors.len()).collect();
        indices.shuffle(&mut rng);
        sampled_query_vectors = indices[..sample_count]
            .iter()
            .map(|&i| all_indexed_vectors[i].1.to_vec())
            .collect();
        &sampled_query_vectors
    } else {
        &query_vectors
    };

    let computed_gt;
    let cached_gt;
    let cache_path = gt_cache_path(
        dataset.name(),
        &format!("{:?}", distance_fn),
        total_vectors,
        effective_query_vectors.len(),
    );

    let mut checkpoint_queries: Vec<&Query> = if !precomputed.is_empty() {
        precomputed
    } else if let Some(loaded) = load_ground_truth(&cache_path) {
        println!("  Loaded cached ground truth from {}", cache_path.display());
        cached_gt = loaded;
        cached_gt.iter().collect()
    } else if args.brute_force_gt {
        if all_indexed_vectors.is_empty() {
            println!("  WARNING: --brute-force-gt after --resume: all_indexed_vectors is empty, skipping GT computation.");
            Vec::new()
        } else {
            println!(
                "  Computing ground truth ({} queries x {} vectors)...",
                effective_query_vectors.len(),
                all_indexed_vectors.len()
            );
            let gt_start = Instant::now();
            computed_gt = compute_ground_truth(
                effective_query_vectors,
                &all_indexed_vectors,
                &distance_fn,
                k,
            );
            println!("  Ground truth: {}", format_duration(gt_start.elapsed()));
            save_ground_truth(&cache_path, &computed_gt);
            println!("  Cached ground truth to {}", cache_path.display());
            computed_gt.iter().collect()
        }
    } else {
        Vec::new()
    };
    checkpoint_queries.truncate(args.num_queries);

    if checkpoint_queries.is_empty() {
        println!(
            "  (no ground truth for {}M boundary, use --brute-force-gt)",
            total_vectors / 1_000_000
        );
    } else {
        let tree_depth = writer.depth();
        let num_levels = tree_depth.saturating_sub(1).max(1);
        let num_queries = checkpoint_queries.len();

        let level_counts = writer.level_node_counts();
        let level_widths: Vec<usize> = level_counts
            .iter()
            .skip(1)
            .take(num_levels)
            .copied()
            .collect();

        let mut beam_col_headers: Vec<String> = Vec::new();
        for lvl in 1..=num_levels {
            let total_at_level = level_counts.get(lvl).copied().unwrap_or(0);
            beam_col_headers.push(format!("L{} beam ({})", lvl, format_count(total_at_level)));
        }
        let beam_col_width = beam_col_headers
            .iter()
            .map(|h| h.len())
            .max()
            .unwrap_or(12)
            .max(12);

        // The unified `--lazy-recall` flag controls everything: lazy posting
        // + embedding loads, the cold/warm two-pass mode, and the
        // between-row pin/posting clears. When off, the recall step runs
        // the legacy eager single-pass path.
        let lazy_recall = args.lazy_recall;

        let mut header = format!("  | {:>6} | {:>6} |", "tau", "rr_v",);
        if lazy_recall {
            header.push_str(&format!(" {:>5} |", "phase"));
        }
        for lvl in 0..num_levels {
            header.push_str(&format!(
                " {:>width$} | {:>8} | {:>7} |",
                beam_col_headers[lvl],
                format!("L{} R@100", lvl + 1),
                format!("L{} MB", lvl + 1),
                width = beam_col_width,
            ));
        }
        header.push_str(&format!(
            " {:>10} | {:>15} | {:>7} | {:>7} | {:>7} | {:>8} | {:>10} | {:>15} | {:>15} | {:>15} | {:>15} | {:>15} | {:>14} |",
            "opt R@100", "scanned_vectors", "scan MB", "tot MB", "MB/s", "R@100", "avg lat",
            "lat_nav", "lat_quant", "lat_dist", "lat_sort", "lat_rerank", "lat_dist / vec",
        ));

        let mut separator = format!("  |{:-^8}|{:-^8}|", "", "",);
        if lazy_recall {
            separator.push_str(&format!("{:-^7}|", ""));
        }
        for _ in 1..=num_levels {
            separator.push_str(&format!(
                "{:-^w$}|{:-^10}|{:-^9}|",
                "",
                "",
                "",
                w = beam_col_width + 2
            ));
        }
        separator.push_str(&format!(
            "{:-^12}|{:-^17}|{:-^9}|{:-^9}|{:-^9}|{:-^10}|{:-^12}|{:-^17}|{:-^17}|{:-^17}|{:-^17}|{:-^17}|{:-^16}|",
            "", "", "", "", "", "", "", "", "", "", "", "", "",
        ));

        println!("\n=== Index Quality ===");
        println!("  Index: {} vectors", format_count(writer.total_vectors()));
        if args.compute_gt_clusters {
            let mut all_p100 = Vec::with_capacity(num_queries);
            let mut all_p95 = Vec::with_capacity(num_queries);
            let mut all_p90 = Vec::with_capacity(num_queries);
            for gt in &checkpoint_queries {
                let gt_100: HashSet<u32> = gt.neighbors.iter().take(100).copied().collect();
                let (p100, p95, p90) = writer.gt_cluster_counts(&gt_100);
                all_p100.push(p100);
                all_p95.push(p95);
                all_p90.push(p90);
            }
            all_p100.sort_unstable();
            all_p95.sort_unstable();
            all_p90.sort_unstable();
            print_cluster_stats("GT clusters (p100)", &all_p100);
            print_cluster_stats("GT clusters (p95) ", &all_p95);
            print_cluster_stats("GT clusters (p90) ", &all_p90);
        } else {
            println!("  GT cluster stats: skipped (pass --compute-gt-clusters to enable)");
        }

        if args.compute_optimal_gt {
            let flat_start = Instant::now();
            let leaf_count = writer.leaf_count();
            println!(
                "  Computing optimal GT baseline with flat k-means (k={} leaves)...",
                format_count(leaf_count),
            );
            let flat_index = optimal_gt::FlatKmeansGtIndex::build(
                &all_indexed_vectors,
                &checkpoint_queries,
                dimension,
                leaf_count,
                distance_fn.clone(),
            )?;
            println!(
                "  Flat k-means: {} | trained on {} sampled vectors | non-empty clusters: {}/{}",
                format_duration(flat_start.elapsed()),
                format_count(flat_index.training_sample_size()),
                format_count(flat_index.num_non_empty_clusters()),
                format_count(flat_index.num_clusters()),
            );

            let mut optimal_p100 = Vec::with_capacity(num_queries);
            let mut optimal_p95 = Vec::with_capacity(num_queries);
            let mut optimal_p90 = Vec::with_capacity(num_queries);
            for gt in &checkpoint_queries {
                let gt_100: HashSet<u32> = gt.neighbors.iter().take(100).copied().collect();
                let (p100, p95, p90) = flat_index.gt_cluster_counts(&gt_100);
                optimal_p100.push(p100);
                optimal_p95.push(p95);
                optimal_p90.push(p90);
            }
            optimal_p100.sort_unstable();
            optimal_p95.sort_unstable();
            optimal_p90.sort_unstable();
            print_cluster_stats("Optimal GT clusters (p100)", &optimal_p100);
            print_cluster_stats("Optimal GT clusters (p95) ", &optimal_p95);
            print_cluster_stats("Optimal GT clusters (p90) ", &optimal_p90);
        }

        // GT is computed; the per-vector buffer is no longer needed for
        // the recall path. Release it so peak RSS is taken during indexing
        // (when we need it) rather than during recall (when we also open
        // a reader and load all postings/embeddings into memory). At
        // dim=1024, this returns ~4 GB per million retained vectors.
        if !all_indexed_vectors.is_empty() {
            let n = all_indexed_vectors.len();
            let bytes = (n as u64).saturating_mul((4 + dimension as u64 * 4 + 16) as u64);
            drop(std::mem::take(&mut all_indexed_vectors));
            println!(
                "  Released retained vector buffer: {} vectors ({})",
                format_count(n),
                mem_probe::format_bytes(bytes),
            );
        }

        // --- Open reader from committed blockfiles ---
        let reader_dir = save_dir
            .as_ref()
            .or(temp_recall_dir.as_ref())
            .expect("no committed blockfiles available for reader");
        let reader = {
            let reader_total_start = Instant::now();

            let open_start = Instant::now();
            let data_dir = reader_dir.join("data");
            let provider = make_blockfile_provider(&data_dir, args.max_cache_bytes).await;
            let ids = committed_ids.clone().unwrap_or_else(|| {
                let meta =
                    load_checkpoint_meta(reader_dir).expect("failed to load checkpoint meta");
                meta.to_ids()
            });
            let r =
                HierarchicalSpannReader::open(&provider, ids, distance_fn.clone(), config.clone())
                    .await
                    .expect("failed to open reader");
            let open_time = open_start.elapsed();

            // Snapshot memory right after open() but before load_all_*: this
            // is what just walking the tree (root + internal nodes + their
            // centroids) brought into memory. Postings and embeddings are
            // not touched yet.
            let ((pl_open_n, pl_open_b), (vd_open_n, vd_open_b)) = r.loaded_blocks_stats();
            let mu_open = r.memory_usage();

            let load_start = Instant::now();
            if !args.lazy_recall {
                r.load_all_postings()
                    .await
                    .expect("failed to load reader postings");
                if args.recall_rerank_vectors.iter().any(|&v| v > 1) {
                    r.load_all_embeddings()
                        .await
                        .expect("failed to load reader embeddings");
                }
            }
            let load_time = load_start.elapsed();

            // Both `load_all_postings` and `load_all_embeddings` COPY data
            // into the reader's owned structures (`nodes`, `embeddings`), but
            // the blockfile readers' `loaded_blocks` cache still pins every
            // block we read -- unbounded and never-evicting. Clear those pins
            // now so RSS during recall reflects reader-owned data, not
            // duplicated block bytes. See docs/README.md ("Reader-side block
            // pinning").
            let ((pl_before_n, pl_before_b), (vd_before_n, vd_before_b)) = r.loaded_blocks_stats();
            let pins_before_total = pl_before_b + vd_before_b;
            r.clear_loaded_blocks();

            println!(
                "  Reader: {} nodes | open {} | load {} | total {}",
                format_count(r.node_count()),
                format_duration(open_time),
                format_duration(load_time),
                format_duration(reader_total_start.elapsed()),
            );
            println!(
                "  Reader after open: tree {} nodes ({} leaf, {} internal, {}) | centroids ({}) \
                 | block pins postings {} blocks/{} | vector_data {} blocks/{} | total pins {}",
                format_count((mu_open.leaf_count + mu_open.internal_count) as usize),
                format_count(mu_open.leaf_count as usize),
                format_count(mu_open.internal_count as usize),
                mem_probe::format_bytes(mu_open.tree_bytes),
                mem_probe::format_bytes(mu_open.centroid_bytes),
                format_count(pl_open_n),
                mem_probe::format_bytes(pl_open_b),
                format_count(vd_open_n),
                mem_probe::format_bytes(vd_open_b),
                mem_probe::format_bytes(pl_open_b + vd_open_b),
            );
            println!(
                "  Reader pins after load: postings {} blocks/{} | vector_data {} blocks/{} | total {} (cleared)",
                format_count(pl_before_n),
                mem_probe::format_bytes(pl_before_b),
                format_count(vd_before_n),
                mem_probe::format_bytes(vd_before_b),
                mem_probe::format_bytes(pins_before_total),
            );
            if args.lazy_recall {
                println!(
                    "  Lazy recall: ON (skipped load_all_postings + load_all_embeddings; \
                     per-query lazy fetches with cold/warm two-pass and between-row clears)"
                );
            } else {
                println!(
                    "  Lazy recall: OFF (eager load_all_postings + (rr>1 ? load_all_embeddings); \
                     single pass per row)"
                );
            }
            r
        };

        println!("\n=== Recall ===");
        {
            let mu = reader.memory_usage();
            println!(
                "  Reader memory: tree {} nodes ({} leaf, {} internal, {}) | postings {} entries ({}) | centroids ({}) | embeddings {} ({}) | total {}",
                format_count((mu.leaf_count + mu.internal_count) as usize),
                format_count(mu.leaf_count as usize),
                format_count(mu.internal_count as usize),
                format_mb(mu.tree_bytes as f64 / (1024.0 * 1024.0)),
                format_count(mu.posting_entries as usize),
                format_mb(mu.posting_bytes as f64 / (1024.0 * 1024.0)),
                format_mb(mu.centroid_bytes as f64 / (1024.0 * 1024.0)),
                format_count(mu.embedding_count as usize),
                format_mb(mu.embedding_bytes as f64 / (1024.0 * 1024.0)),
                format_mb(mu.total_bytes() as f64 / (1024.0 * 1024.0)),
            );
        }
        println!(
            "  Search beam: tau={} min={} max={} | Tau sweep: {:?}",
            args.beam_tau, args.read_beam_min, args.read_beam_max, tau_values,
        );
        println!("  Rerank vectors: {:?}", args.recall_rerank_vectors,);
        println!("  Brute-force GT: {}", args.brute_force_gt,);
        if !read_level_taus.is_empty() || !read_level_min_pcts.is_empty() {
            println!(
                "  Read beam schedule: taus=[{}] min_pcts=[{}] leaf_min={} leaf_max={}",
                format_level_taus(&read_level_taus),
                read_level_min_pcts
                    .iter()
                    .map(|v| format!("{:.1}", v))
                    .collect::<Vec<_>>()
                    .join(","),
                format_count(args.read_beam_min),
                format_count(args.read_beam_max),
            );
        }

        let beam_min = args.read_beam_min;
        let beam_max = args.read_beam_max;

        let recall_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(32)
            .build()
            .expect("failed to build rayon pool");

        println!(
            "  Recall ({} queries, k={}, depth={}):",
            num_queries, k, tree_depth,
        );
        println!("{}", header);
        println!("{}", separator);

        let recall_start = Instant::now();

        // Capture a tokio runtime handle here so rayon workers (which run on
        // non-tokio threads) can `block_on` the lazy async search path.
        let rt_handle = tokio::runtime::Handle::current();

        // Passes per (tau, rr_v) row. In lazy mode we run the same query
        // set twice -- a `cold` pass with caches cleared up front, and a
        // `warm` pass that reuses what cold populated. The pair isolates
        // the lazy fetch cost. Eager mode runs a single unlabelled pass.
        let passes: &[&str] = if lazy_recall {
            &["cold", "warm"]
        } else {
            &[""]
        };

        for &tau in &tau_values {
            for &rr_v in &args.recall_rerank_vectors {
                let read_policy = ReadBeamPolicy::with_level_overrides(
                    Some(tau),
                    beam_min,
                    beam_max,
                    read_level_taus.clone(),
                    read_level_min_pcts.clone(),
                    level_widths.clone(),
                );
                struct QueryResult {
                    r100: f64,
                    nanos: u64,
                    scanned: usize,
                    level_r100: Vec<f64>,
                    level_beam: Vec<u64>,
                    level_candidates: Vec<u64>,
                    timings: SearchTimings,
                    optimal_r100: f64,
                }

                for &phase in passes {
                    // Make the cold pass actually cold by dropping all reader
                    // caches (block pins on both readers, plus posting data
                    // in `self.nodes`) right before it. Skipped for the warm
                    // pass and for the eager (single-pass) mode.
                    if lazy_recall && phase == "cold" {
                        reader.clear_loaded_blocks();
                        reader.clear_loaded_postings();
                    }

                    let results: Vec<QueryResult> = recall_pool.install(|| {
                        checkpoint_queries
                            .par_iter()
                            .map(|gt| {
                                let t0 = Instant::now();
                                let (results, scanned, _leaves_scanned, timings) = if lazy_recall {
                                    rt_handle
                                        .block_on(reader.search_with_policy_lazy(
                                            &gt.vector,
                                            k,
                                            rr_v,
                                            &read_policy,
                                        ))
                                        .expect("lazy recall search failed")
                                } else {
                                    reader.search_with_policy_sync(
                                        &gt.vector,
                                        k,
                                        rr_v,
                                        &read_policy,
                                    )
                                };
                                let nanos = t0.elapsed().as_nanos() as u64;

                                let result_ids: Vec<u32> =
                                    results.iter().map(|(id, _)| *id).collect();
                                let r100 = recall_at_k(&result_ids, &gt.neighbors, 100);

                                let gt_100: HashSet<u32> =
                                    gt.neighbors.iter().take(100).copied().collect();
                                let level_recall = writer.diagnose_level_recall_with_policy(
                                    &gt.vector,
                                    &gt_100,
                                    &read_policy,
                                );

                                let mut level_r100 = vec![0.0f64; num_levels];
                                let mut level_beam = vec![0u64; num_levels];
                                let mut level_candidates = vec![0u64; num_levels];
                                for lr in &level_recall {
                                    if lr.level <= num_levels {
                                        level_r100[lr.level - 1] = lr.reachable_100;
                                        level_beam[lr.level - 1] = lr.beam_size as u64;
                                        level_candidates[lr.level - 1] = lr.total_candidates as u64;
                                    }
                                }

                                let last_beam = level_beam.last().copied().unwrap_or(0) as usize;
                                let optimal_r100 = writer.optimal_leaf_recall(&gt_100, last_beam);

                                QueryResult {
                                    r100,
                                    nanos,
                                    scanned,
                                    level_r100,
                                    level_beam,
                                    level_candidates,
                                    timings,
                                    optimal_r100,
                                }
                            })
                            .collect()
                    });

                    let mut total_r100 = 0.0f64;
                    let mut total_nanos = 0u64;
                    let mut total_scanned = 0usize;
                    let mut level_r100_sums = vec![0.0f64; num_levels];
                    let mut level_beam_sums = vec![0u64; num_levels];
                    let mut level_candidates_sums = vec![0u64; num_levels];
                    let mut total_nav_nanos = 0u64;
                    let mut total_qq_nanos = 0u64;
                    let mut total_dq_nanos = 0u64;
                    let mut total_sort_nanos = 0u64;
                    let mut total_rr_nanos = 0u64;
                    let mut total_optimal_r100 = 0.0f64;
                    for qr in &results {
                        total_r100 += qr.r100;
                        total_nanos += qr.nanos;
                        total_scanned += qr.scanned;
                        total_nav_nanos += qr.timings.navigate_nanos;
                        total_qq_nanos += qr.timings.quantize_nanos;
                        total_dq_nanos += qr.timings.distance_nanos;
                        total_sort_nanos += qr.timings.sort_dedup_nanos;
                        total_rr_nanos += qr.timings.rerank_nanos;
                        total_optimal_r100 += qr.optimal_r100;
                        for i in 0..num_levels {
                            level_r100_sums[i] += qr.level_r100[i];
                            level_beam_sums[i] += qr.level_beam[i];
                            level_candidates_sums[i] += qr.level_candidates[i];
                        }
                    }

                    let n = num_queries as f64;
                    let avg_r100 = total_r100 / n;
                    let avg_lat = total_nanos / num_queries as u64;
                    let avg_scanned = total_scanned / num_queries;

                    let mut row = format!("  | {:>6.2} | {:>5}x |", tau, rr_v,);
                    if lazy_recall {
                        row.push_str(&format!(" {:>5} |", phase));
                    }
                    let dim = dimension;
                    let mut total_mb = 0.0f64;
                    let is_last = |lvl: usize| lvl == num_levels - 1;

                    for lvl in 0..num_levels {
                        let avg_beam = level_beam_sums[lvl] / num_queries as u64;
                        let avg_candidates = level_candidates_sums[lvl] / num_queries as u64;
                        let avg_lr = level_r100_sums[lvl] / n * 100.0;
                        let level_bytes = {
                            let mut level_sum = (avg_candidates as f64 * dim as f64) / 8.0;
                            if is_last(lvl) {
                                level_sum += avg_beam as f64 * dim as f64 * 4.0;
                            }
                            level_sum
                        };
                        let level_mb = level_bytes / (1024.0 * 1024.0);
                        total_mb += level_mb;
                        row.push_str(&format!(
                            " {:>width$} | {:>7.2}% | {:>7} |",
                            format!(
                                "{}/{}",
                                format_count(avg_beam as usize),
                                format_count(avg_candidates as usize)
                            ),
                            avg_lr,
                            format_mb(level_mb),
                            width = beam_col_width,
                        ));
                    }

                    let scan_bytes = avg_scanned as f64 * dim as f64 / 8.0
                        + (k * rr_v) as f64 * dim as f64 * 4.0;
                    let scan_mb = scan_bytes / (1024.0 * 1024.0);
                    total_mb += scan_mb;

                    let avg_lat_secs = avg_lat as f64 / 1_000_000_000.0;
                    let mb_per_sec = if avg_lat_secs > 0.0 {
                        total_mb / avg_lat_secs
                    } else {
                        0.0
                    };

                    let nq = num_queries as u64;
                    let avg_nav = total_nav_nanos / nq;
                    let avg_qq = total_qq_nanos / nq;
                    let avg_dq = total_dq_nanos / nq;
                    let avg_sort = total_sort_nanos / nq;
                    let avg_rr = total_rr_nanos / nq;
                    let pct = |v: u64| {
                        if avg_lat > 0 {
                            v as f64 / avg_lat as f64 * 100.0
                        } else {
                            0.0
                        }
                    };

                    let dist_per_vec_ns = if avg_scanned > 0 {
                        avg_dq as f64 / avg_scanned as f64
                    } else {
                        0.0
                    };

                    let avg_optimal = total_optimal_r100 / n;

                    let scanned_pct = avg_scanned as f64 / total_vectors as f64 * 100.0;
                    let scanned_label =
                        format!("{} ({:.2}%)", format_count(avg_scanned), scanned_pct);
                    row.push_str(&format!(
                            " {:>9.2}% | {:>15} | {:>7} | {:>7} | {:>7} | {:>7.2}% | {:>10} | {:>15} | {:>15} | {:>15} | {:>15} | {:>15} | {:>14} |",
                            avg_optimal * 100.0,
                            scanned_label,
                            format_mb(scan_mb),
                            format_mb(total_mb),
                            format_mb(mb_per_sec),
                            avg_r100 * 100.0,
                            format_latency(avg_lat),
                            format!("{} ({:.0}%)", format_latency(avg_nav), pct(avg_nav)),
                            format!("{} ({:.0}%)", format_latency(avg_qq), pct(avg_qq)),
                            format!("{} ({:.0}%)", format_latency(avg_dq), pct(avg_dq)),
                            format!("{} ({:.0}%)", format_latency(avg_sort), pct(avg_sort)),
                            format!("{} ({:.0}%)", format_latency(avg_rr), pct(avg_rr)),
                            format!("{:.1}ns", dist_per_vec_ns),
                        ));
                    println!("{}", row);
                    flush_stdout();
                } // end of `for &phase in passes`

                // After the row's warm pass: report what the warm pass left
                // populated, then clear so the next row's cold pass starts
                // from the same baseline as this row's did. Cold pass at the
                // top of the next iteration also clears, so this is mostly
                // for visibility + freeing RSS for the recall_time line.
                if lazy_recall {
                    let ((pl_n, pl_b), (vd_n, vd_b)) = reader.loaded_blocks_stats();
                    let (post_leaves, post_bytes) = reader.loaded_postings_stats();
                    reader.clear_loaded_blocks();
                    reader.clear_loaded_postings();
                    println!(
                        "    Reader cache (end of row tau={:.2}, rr={}): postings {} blocks/{} | vector_data {} blocks/{} | total {} | leaves data {} ({}) (cleared)",
                        tau,
                        rr_v,
                        format_count(pl_n),
                        mem_probe::format_bytes(pl_b),
                        format_count(vd_n),
                        mem_probe::format_bytes(vd_b),
                        mem_probe::format_bytes(pl_b + vd_b),
                        format_count(post_leaves),
                        mem_probe::format_bytes(post_bytes),
                    );
                    flush_stdout();
                }
            }
        }
        let recall_time = recall_start.elapsed();
        println!("  Recall duration: {}", format_duration(recall_time));
        flush_stdout();

        if args.leaf_miss_diagnostic || args.geometry_diagnostic {
            let diag_tau = tau_values[0];

            let policy = ReadBeamPolicy::with_level_overrides(
                Some(diag_tau),
                beam_min,
                beam_max,
                read_level_taus.clone(),
                read_level_min_pcts.clone(),
                level_widths.clone(),
            );
            let diags: Vec<LeafMissDiagnostic> = recall_pool.install(|| {
                checkpoint_queries
                    .par_iter()
                    .map(|gt| {
                        let gt_100: HashSet<u32> = gt.neighbors.iter().take(100).copied().collect();
                        writer.diagnose_leaf_miss_ranks(&gt.vector, &gt_100, &policy)
                    })
                    .collect()
            });

            if args.leaf_miss_diagnostic {
                println!("\n--- Leaf Miss Diagnostic (tau={:.2}) ---", diag_tau);

                let n = diags.len() as f64;
                let avg_beam: f64 = diags.iter().map(|d| d.beam_size as f64).sum::<f64>() / n;
                let avg_total: f64 = diags.iter().map(|d| d.total_leaves as f64).sum::<f64>() / n;
                let avg_missed: f64 = diags
                    .iter()
                    .map(|d| d.missed_gt_ranks.len() as f64)
                    .sum::<f64>()
                    / n;
                let total_missed: usize = diags.iter().map(|d| d.missed_gt_ranks.len()).sum();

                println!("  Avg beam: {:.1} / {:.0} leaves", avg_beam, avg_total);
                println!(
                    "  Avg missed GT vectors: {:.1} / 100 ({} total across {} queries)",
                    avg_missed,
                    total_missed,
                    diags.len()
                );

                if total_missed > 0 {
                    let mut all_ranks: Vec<usize> = diags
                        .iter()
                        .flat_map(|d| d.missed_gt_ranks.iter().map(|&(_, rank)| rank))
                        .collect();
                    all_ranks.sort_unstable();

                    let pct = |idx: f64| all_ranks[(idx * (all_ranks.len() - 1) as f64) as usize];
                    println!("  Missed leaf rank distribution (1-indexed):");
                    println!(
                        "    min={}, p10={}, p25={}, p50={}, p75={}, p90={}, max={}",
                        all_ranks[0],
                        pct(0.10),
                        pct(0.25),
                        pct(0.50),
                        pct(0.75),
                        pct(0.90),
                        all_ranks[all_ranks.len() - 1],
                    );

                    let expansions = [5, 10, 20, 50, 100];
                    println!("  Recovery by beam expansion:");
                    for &extra in &expansions {
                        let mut recovered = 0usize;
                        for d in &diags {
                            for &(_, rank) in &d.missed_gt_ranks {
                                if rank <= d.beam_size + extra {
                                    recovered += 1;
                                }
                            }
                        }
                        let pct_recovered = if total_missed > 0 {
                            recovered as f64 / total_missed as f64 * 100.0
                        } else {
                            0.0
                        };
                        println!(
                            "    +{:>3} leaves: {:>4} / {} missed recovered ({:.1}%)",
                            extra, recovered, total_missed, pct_recovered
                        );
                    }

                    let mut per_query: Vec<(usize, usize, Vec<usize>)> = diags
                        .iter()
                        .enumerate()
                        .filter(|(_, d)| !d.missed_gt_ranks.is_empty())
                        .map(|(qi, d)| {
                            let ranks: Vec<usize> =
                                d.missed_gt_ranks.iter().map(|&(_, r)| r).collect();
                            (qi, d.beam_size, ranks)
                        })
                        .collect();
                    per_query.sort_by(|a, b| b.2.len().cmp(&a.2.len()));

                    let show = per_query.len().min(10);
                    println!("  Top {} queries by missed GT count:", show);
                    for entry in per_query.iter().take(show) {
                        let qi = entry.0;
                        let beam = entry.1;
                        let ranks = &entry.2;
                        let near = ranks.iter().filter(|r| **r <= beam + 20).count();
                        let far = ranks.iter().filter(|r| **r > beam * 2).count();
                        println!("    q{:>3}: {} missed, beam={}, near-miss(+20)={}, far-miss(>2x beam)={}, ranks={:?}",
                        qi, ranks.len(), beam, near, far,
                        if ranks.len() <= 20 { ranks.clone() } else {
                            let mut trunc = ranks[..10].to_vec();
                            trunc.push(0);
                            trunc.extend_from_slice(&ranks[ranks.len()-5..]);
                            trunc
                        });
                    }
                }

                // Leaf traits comparison: selected-with-GT vs selected-no-GT vs missed-with-GT.
                let all_sel_gt: Vec<&LeafTraits> = diags
                    .iter()
                    .flat_map(|d| d.selected_with_gt.iter())
                    .collect();
                let all_sel_no: Vec<&LeafTraits> =
                    diags.iter().flat_map(|d| d.selected_no_gt.iter()).collect();
                let all_miss: Vec<&LeafTraits> =
                    diags.iter().flat_map(|d| d.missed_with_gt.iter()).collect();

                struct TraitSummary {
                    label: &'static str,
                    n: usize,
                    score: [f64; 7],
                    rank: [f64; 7],
                    leaf_size: [f64; 7],
                    gt_count: [f64; 4],
                    min_gt_d: [f64; 7],
                    score_gt: [f64; 5],
                }

                fn compute_trait_summary(
                    label: &'static str,
                    traits: &[&LeafTraits],
                ) -> Option<TraitSummary> {
                    if traits.is_empty() {
                        return None;
                    }
                    let fp = |v: &[f32], p: f64| v[(p * (v.len() - 1) as f64) as usize] as f64;
                    let up = |v: &[usize], p: f64| v[(p * (v.len() - 1) as f64) as usize] as f64;
                    let favg =
                        |v: &[f32]| v.iter().map(|x| *x as f64).sum::<f64>() / v.len() as f64;
                    let uavg = |v: &[usize]| v.iter().sum::<usize>() as f64 / v.len() as f64;

                    let mut scores: Vec<f32> = traits.iter().map(|t| t.score).collect();
                    scores.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    let mut sizes: Vec<usize> = traits.iter().map(|t| t.leaf_size).collect();
                    sizes.sort_unstable();
                    let mut ranks: Vec<usize> = traits.iter().map(|t| t.rank).collect();
                    ranks.sort_unstable();

                    let score = [
                        scores[0] as f64,
                        fp(&scores, 0.25),
                        fp(&scores, 0.5),
                        favg(&scores),
                        fp(&scores, 0.75),
                        fp(&scores, 0.9),
                        *scores.last().unwrap() as f64,
                    ];
                    let rank = [
                        ranks[0] as f64,
                        up(&ranks, 0.25),
                        up(&ranks, 0.5),
                        uavg(&ranks),
                        up(&ranks, 0.75),
                        up(&ranks, 0.9),
                        *ranks.last().unwrap() as f64,
                    ];
                    let leaf_size = [
                        sizes[0] as f64,
                        up(&sizes, 0.25),
                        up(&sizes, 0.5),
                        uavg(&sizes),
                        up(&sizes, 0.75),
                        up(&sizes, 0.9),
                        *sizes.last().unwrap() as f64,
                    ];

                    let gt_only: Vec<&LeafTraits> =
                        traits.iter().filter(|t| t.gt_count > 0).copied().collect();
                    let mut gt_counts: Vec<usize> = traits.iter().map(|t| t.gt_count).collect();
                    gt_counts.sort_unstable();
                    let gt_count = [
                        gt_counts[0] as f64,
                        up(&gt_counts, 0.5),
                        uavg(&gt_counts),
                        *gt_counts.last().unwrap() as f64,
                    ];

                    let mut min_gt_d = [0.0f64; 7];
                    let mut score_gt = [0.0f64; 5];
                    if !gt_only.is_empty() {
                        let mut gt_dists: Vec<f32> =
                            gt_only.iter().map(|t| t.min_gt_dist).collect();
                        gt_dists.sort_by(|a, b| a.partial_cmp(b).unwrap());
                        min_gt_d = [
                            gt_dists[0] as f64,
                            fp(&gt_dists, 0.25),
                            fp(&gt_dists, 0.5),
                            favg(&gt_dists),
                            fp(&gt_dists, 0.75),
                            fp(&gt_dists, 0.9),
                            *gt_dists.last().unwrap() as f64,
                        ];

                        let mut ratios: Vec<f32> = gt_only
                            .iter()
                            .filter(|t| t.min_gt_dist > 1e-10)
                            .map(|t| t.score / t.min_gt_dist)
                            .collect();
                        if !ratios.is_empty() {
                            ratios.sort_by(|a, b| a.partial_cmp(b).unwrap());
                            score_gt = [
                                ratios[0] as f64,
                                fp(&ratios, 0.25),
                                fp(&ratios, 0.5),
                                favg(&ratios),
                                *ratios.last().unwrap() as f64,
                            ];
                        }
                    }

                    Some(TraitSummary {
                        label,
                        n: traits.len(),
                        score,
                        rank,
                        leaf_size,
                        gt_count,
                        min_gt_d,
                        score_gt,
                    })
                }

                let summaries: Vec<TraitSummary> = [
                    ("Sel+GT (TP)", &all_sel_gt),
                    ("Sel+noGT (FP)", &all_sel_no),
                    ("Miss+GT (FN)", &all_miss),
                ]
                .iter()
                .filter_map(|(label, data)| compute_trait_summary(label, data))
                .collect();

                if !summaries.is_empty() {
                    println!("\n  --- Leaf Traits Comparison ---");
                    let w = 16;
                    print!("  {:14}", "metric");
                    for s in &summaries {
                        print!("  {:>w$}", format!("{} ({})", s.label, s.n), w = w);
                    }
                    println!();
                    print!("  {:14}", "--------------");
                    for _ in &summaries {
                        print!("  {:>w$}", "----------------", w = w);
                    }
                    println!();

                    let row_f4 =
                        |label: &str,
                         idx: usize,
                         summaries: &[TraitSummary],
                         getter: fn(&TraitSummary) -> &[f64]| {
                            print!("  {:14}", label);
                            for s in summaries {
                                let v = getter(s);
                                if idx < v.len() {
                                    print!("  {:>w$.4}", v[idx], w = w);
                                } else {
                                    print!("  {:>w$}", "", w = w);
                                }
                            }
                            println!();
                        };

                    let row_f0 =
                        |label: &str,
                         idx: usize,
                         summaries: &[TraitSummary],
                         getter: fn(&TraitSummary) -> &[f64]| {
                            print!("  {:14}", label);
                            for s in summaries {
                                let v = getter(s);
                                if idx < v.len() {
                                    print!("  {:>w$.0}", v[idx], w = w);
                                } else {
                                    print!("  {:>w$}", "", w = w);
                                }
                            }
                            println!();
                        };

                    let stats7 = ["min", "p25", "p50", "avg", "p75", "p90", "max"];

                    println!("  -- score (centroid dist) --");
                    for (i, &lbl) in stats7.iter().enumerate() {
                        row_f4(lbl, i, &summaries, |s| &s.score);
                    }

                    println!("  -- rank --");
                    for (i, &lbl) in stats7.iter().enumerate() {
                        row_f0(lbl, i, &summaries, |s| &s.rank);
                    }

                    println!("  -- leaf_size --");
                    for (i, &lbl) in stats7.iter().enumerate() {
                        row_f0(lbl, i, &summaries, |s| &s.leaf_size);
                    }

                    let stats4 = ["min", "p50", "avg", "max"];
                    println!("  -- gt_count --");
                    for (i, &lbl) in stats4.iter().enumerate() {
                        row_f0(lbl, i, &summaries, |s| &s.gt_count);
                    }

                    println!("  -- min_gt_dist --");
                    for (i, &lbl) in stats7.iter().enumerate() {
                        row_f4(lbl, i, &summaries, |s| &s.min_gt_d);
                    }

                    let stats5b = ["min", "p25", "p50", "avg", "max"];
                    println!("  -- score(centroid dist)/gt_dist -- (< 1 means centroid is closer to the query than the GT vector");
                    for (i, &lbl) in stats5b.iter().enumerate() {
                        row_f4(lbl, i, &summaries, |s| &s.score_gt);
                    }
                }
            } // end if args.leaf_miss_diagnostic

            if args.geometry_diagnostic {
                println!("\n  --- Search Geometry (tau={:.2}) ---", diag_tau);

                let pf = |v: &[f32], p: f64| -> f32 {
                    if v.is_empty() {
                        return 0.0;
                    }
                    v[(p * (v.len() - 1) as f64) as usize]
                };

                let mut search_radii: Vec<f32> = diags.iter().map(|d| d.search_radius).collect();
                search_radii.sort_by(|a, b| a.partial_cmp(b).unwrap());

                let mut beam_radii: Vec<f32> = diags.iter().map(|d| d.beam_radius).collect();
                beam_radii.sort_by(|a, b| a.partial_cmp(b).unwrap());

                let mut gt_radii: Vec<f32> = diags
                    .iter()
                    .map(|d| d.gt_distances.iter().cloned().fold(0.0f32, f32::max))
                    .collect();
                gt_radii.sort_by(|a, b| a.partial_cmp(b).unwrap());

                println!(
                    "  {:32}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}",
                    "metric", "min", "p25", "p50", "p75", "p90", "max"
                );
                println!(
                    "  {:32}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}",
                    "--------------------------------",
                    "-------",
                    "-------",
                    "-------",
                    "-------",
                    "-------",
                    "-------"
                );

                for (label, vals) in [
                    ("search radius (d1*(1+tau))", &search_radii),
                    ("beam radius (farthest sel.)", &beam_radii),
                    ("GT radius (max gt dist)", &gt_radii),
                ] {
                    if !vals.is_empty() {
                        println!(
                            "  {:32}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}",
                            label,
                            pf(vals, 0.0),
                            pf(vals, 0.25),
                            pf(vals, 0.50),
                            pf(vals, 0.75),
                            pf(vals, 0.90),
                            pf(vals, 1.0),
                        );
                    }
                }
            } // end if args.geometry_diagnostic
        }

        drop(reader);
        if let Some(ref dir) = temp_recall_dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    if args.print_legend {
        println!();
        println!("=== Legend ===");
        println!();
        println!("--- Task Counts / Total Time / Avg Time ---");
        println!("add        - full add() pipeline (navigate + rng_select + register + balance)");
        println!("navigate   - beam search the tree to find nearest leaf nodes");
        println!("split      - 2-means split of an oversized leaf (SPANN utils::split)");
        println!("merge      - merge a small leaf into its nearest neighbor");
        println!(
            "reassign   - re-route a vector after split/merge (navigate + register + balance)"
        );
        println!("scrub      - remove stale version entries from a leaf");
        println!("scrub_rm   - number of stale entries removed by scrub");
        println!("wall       - wall-clock time for the checkpoint");
        println!();
        println!("--- Task Breakdowns (concurrency diagnostics) ---");
        println!(
            "navigate.missing_node  - navigate saw a child_id in a parent's children list but the"
        );
        println!(
            "                     node was missing from the DashMap (removed by concurrent split)"
        );
        println!(
            "add.missing_nodes     - add() failed to register in any navigated cluster (all gone)"
        );
        println!("                     and fell back to inserting in the root node");
        println!(
            "register.missing_node - register_in_leaf target was gone (split by balance cascade),"
        );
        println!("                     fell back to reassign");
        println!();
        println!("--- Recall Table ---");
        println!("tau             - dynamic beam tau threshold (controls beam width)");
        println!("rr_v            - vector rerank factor (1x = no rerank)");
        println!("Lk beam (N)     - effective_beam / candidates_considered at level k (N = total nodes at level)");
        println!("Lk R@100        - fraction of true top-100 reachable after level k");
        println!("Lk MB           - avg data loaded per query at level k");
        println!("scanned_vectors - avg unique vectors scored per query across all leaves");
        println!("scan MB         - data loaded for vector scoring (quantized codes + rerank f32 vectors)");
        println!("tot MB          - total data loaded per query (all levels + scan)");
        println!("MB/s            - data throughput (tot MB / avg lat)");
        println!("R@100           - fraction of true top-100 neighbors in final results");
        println!("avg lat         - average end-to-end search latency per query");
        println!("lat_nav         - time in navigate() (tree traversal to find leaves)");
        println!("lat_quant       - time building QuantizedQuery per leaf (residual, norms)");
        println!("lat_dist        - time scoring vectors against quantized query (includes version checks)");
        println!("lat_sort        - time deduplicating + sorting all scored vectors");
        println!(
            "lat_rerank      - time reranking top candidates with f32 embeddings (0 when rr_v=1)"
        );
    }
    flush_stdout();

    Ok(())
}
