//! Benchmark for 1-bit quantized HierarchicalSpannWriter: incremental index
//! build with recall evaluation at each checkpoint. Uses 1-bit RaBitQ codes
//! for both data vectors and centroid navigation.

#![recursion_limit = "256"]

mod datasets;
mod hierarchical_index;
mod optimal_gt;

use std::collections::{BTreeMap, HashSet};
use std::io::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rayon::prelude::*;

use chroma_distance::DistanceFunction;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};

use datasets::{format_count, recall_at_k, Dataset, DatasetType, MetricType, Query};
use hierarchical_index::writer_quantized::{
    format_task_tables, HierarchicalSpannConfig, HierarchicalSpannWriter, LeafMissDiagnostic,
    LeafTraits, NavigationMode, ReadBeamPolicy, SearchTimings, WriterStatsSnapshot,
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
    #[arg(long, default_value = "1.0")]
    write_beam_tau: f64,

    /// Per-level write taus overriding the global write tau, comma-separated.
    /// Use `_` to fall back to the global write tau for a level.
    #[arg(long)]
    write_level_taus: Option<String>,

    /// Per-level minimum write beam widths as percentages of the full level width, comma-separated
    #[arg(long)]
    write_level_min_pcts: Option<String>,

    /// Min beam width for write path
    #[arg(long, default_value = "10")]
    write_beam_min: usize,

    /// Max beam width for write path
    #[arg(long, default_value = "512")]
    write_beam_max: usize,

    /// Max replicas per vector (RNG select)
    #[arg(long, default_value = "2")]
    max_replicas: usize,

    /// RNG epsilon filter
    #[arg(long, default_value = "8.0")]
    write_rng_epsilon: f32,

    /// RNG distance factor
    #[arg(long, default_value = "4.0")]
    write_rng_factor: f32,

    /// Force brute-force ground truth computation (slow at scale)
    #[arg(
        long,
        default_value = "true",
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    brute_force_gt: bool,

    /// Compute a flat k-means GT baseline using the same number of clusters as leaf nodes
    #[arg(long)]
    compute_optimal_gt: bool,

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
    #[arg(long, default_value = "1.0")]
    beam_tau: f64,

    /// Per-level read taus overriding the global recall tau, comma-separated.
    /// Use `_` to fall back to the per-row tau for a level.
    #[arg(long)]
    read_level_taus: Option<String>,

    /// Per-level minimum beam widths as percentages of the full level width, comma-separated
    #[arg(long)]
    read_level_min_pcts: Option<String>,

    /// Number of threads for parallel add
    #[arg(long, default_value = "1")]
    threads: usize,

    /// Write-path navigation mode: fp (f32), 1bit (code-to-code), 4bit (QuantizedQuery)
    #[arg(long, default_value = "fp")]
    write_navigation: String,

    /// Read-path navigation modes to sweep during recall: fp, 1bit, 4bit
    #[arg(long, default_value = "4bit", value_delimiter = ',')]
    read_navigation: Vec<String>,

    /// Use full precision f32 distances for NPA instead of quantized
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    fp_npa: bool,

    /// Tau values for recall sweep, comma-separated
    #[arg(long, default_value = "0.1,0.5,1")]
    recall_tau_values: String,

    /// Centroid rerank factors to sweep during recall
    #[arg(long, default_value = "1,4,16", value_delimiter = ',')]
    recall_rerank_centroids: Vec<usize>,

    /// Vector rerank factors to sweep during recall
    #[arg(long, default_value = "1,4,16", value_delimiter = ',')]
    recall_rerank_vectors: Vec<usize>,

    /// Defer splits/merges until an explicit balance_index() call after each checkpoint
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    deferred_balance: bool,

    /// Run deferred balancing in parallel across subtrees
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    parallel_balancing: bool,

    /// Use p90-radius-corrected leaf scoring during search navigation
    #[arg(long)]
    radius_corrected_nav: bool,

    /// Number of representative codes per leaf for leaf reranking (0 = off)
    #[arg(long, default_value = "0")]
    leaf_rerank_reps: usize,

    /// Print leaf-miss diagnostic: rank distribution of missed GT-containing leaves
    #[arg(long)]
    leaf_miss_diagnostic: bool,

    /// Print search geometry: cluster radius, search radius, GT radius distributions
    #[arg(long)]
    geometry_diagnostic: bool,

    /// Print legend explaining all table columns
    #[arg(long)]
    print_legend: bool,

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

fn parse_level_taus(input: Option<&str>) -> Result<Vec<Option<f64>>, Box<dyn std::error::Error + Send + Sync>> {
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

fn parse_level_f64s(input: Option<&str>) -> Result<Vec<f64>, Box<dyn std::error::Error + Send + Sync>> {
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
        .map(|tau| tau.map(|t| format!("{:.2}", t)).unwrap_or_else(|| "_".to_string()))
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

fn gt_cache_path(dataset_name: &str, metric: &str, num_vectors: usize, num_queries: usize) -> std::path::PathBuf {
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
        if *p + 4 > data.len() { return None; }
        let v = u32::from_le_bytes(data[*p..*p + 4].try_into().ok()?);
        *p += 4;
        Some(v)
    };
    let r64 = |p: &mut usize| -> Option<u64> {
        if *p + 8 > data.len() { return None; }
        let v = u64::from_le_bytes(data[*p..*p + 8].try_into().ok()?);
        *p += 8;
        Some(v)
    };
    let rf32 = |p: &mut usize| -> Option<f32> {
        if *p + 4 > data.len() { return None; }
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
        queries.push(Query { vector, neighbors, max_vector_id });
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
    let args = Args::parse_from(
        std::env::args().filter(|a| a != "--bench"),
    );

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
        DatasetType::WikipediaEn => Box::new(datasets::wikipedia::Wikipedia::load().await?),
        DatasetType::Sift => Box::new(datasets::sift::Sift::load().await?),
        DatasetType::Deep10m => Box::new(datasets::deep::Deep10M::load().await?),
        DatasetType::Synthetic => {
            Box::new(datasets::synthetic::Synthetic::load(args.dim, args.synthetic_size)?)
        }
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

    let parse_nav = |s: &str, flag: &str| match s {
        "fp" => NavigationMode::Fp,
        "1bit" => NavigationMode::OneBit,
        "4bit" => NavigationMode::FourBit,
        other => panic!("invalid {} value '{}': must be fp, 1bit, or 4bit", flag, other),
    };
    let write_nav = parse_nav(&args.write_navigation, "--write-navigation");
    let read_navs: Vec<NavigationMode> = args
        .read_navigation
        .iter()
        .map(|s| parse_nav(s, "--read-navigation"))
        .collect();
    let read_nav = read_navs[0];

    let config = HierarchicalSpannConfig {
        branching_factor: args.branching_factor,
        split_threshold: args.split_threshold,
        merge_threshold: args.merge_threshold,
        write_beam_tau: args.write_beam_tau,
        write_beam_min: args.write_beam_min,
        write_beam_max: args.write_beam_max,
        write_level_taus: write_level_taus.clone(),
        write_level_min_pcts: write_level_min_pcts.clone(),
        beam_tau: args.beam_tau,
        beam_min: args.read_beam_min,
        beam_max: args.read_beam_max,
        max_replicas: args.max_replicas,
        write_rng_epsilon: args.write_rng_epsilon,
        write_rng_factor: args.write_rng_factor,
        reassign_neighbor_count: 32,
        write_navigation: write_nav,
        read_navigation: read_nav,
        fp_npa: args.fp_npa,
        deferred_balance: args.deferred_balance,
        representative_count: args.leaf_rerank_reps,
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
        "  Quantization: 1-bit | Write nav: {:?} | Read nav: {} | NPA: {} | Radius nav: {} | Leaf reps: {}",
        write_nav,
        args.read_navigation.join(","),
        if args.fp_npa { "f32" } else { "1x4" },
        if args.radius_corrected_nav { "p90" } else { "off" },
        if args.leaf_rerank_reps > 0 { format!("{}", args.leaf_rerank_reps) } else { "off".to_string() },
    );
    println!("  Threads: {}", args.threads);
    println!();

    let all_queries = dataset.queries(distance_fn.clone())?;
    let query_vectors: Vec<Vec<f32>> =
        all_queries.iter().take(100).map(|q| q.vector.clone()).collect();
    let queries_by_checkpoint = group_queries_by_checkpoint(all_queries);

    let sample_queries_as_gt = query_vectors.is_empty() && args.brute_force_gt;
    if sample_queries_as_gt {
        println!("  No precomputed queries; will sample 100 data vectors as queries for brute-force GT.");
    }

    let writer = HierarchicalSpannWriter::new(dimension, distance_fn.clone(), config);

    let mut total_vectors = 0usize;
    let mut all_indexed_vectors: Vec<(u32, Arc<[f32]>)> = Vec::new();
    let total_start = Instant::now();
    let mut prev_snapshot = WriterStatsSnapshot::default();
    let mut all_snapshots: Vec<WriterStatsSnapshot> = Vec::new();

    for checkpoint_idx in 0..num_checkpoints {
        let offset = checkpoint_idx * batch_size;
        let limit = batch_size.min(data_len.saturating_sub(offset));

        if limit == 0 {
            println!("Checkpoint {}: No more data", checkpoint_idx + 1);
            break;
        }

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
        let needs_early_balance = args.deferred_balance
            && batch_size > early_balance_size
            && total_vectors < 1_000_000;

        let sub_batches: Vec<&[(u32, Arc<[f32]>)]> = if needs_early_balance {
            let mut subs = Vec::new();
            let mut remaining = &batch_vectors[..];
            let mut running_total = total_vectors;
            while !remaining.is_empty() && running_total < 1_000_000 {
                let take = early_balance_size.min(remaining.len()).min(1_000_000 - running_total);
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

        for sub_batch in &sub_batches {
            if num_threads <= 1 {
                for (id, embedding) in *sub_batch {
                    writer.add(*id, embedding);
                    progress.inc(1);
                }
            } else {
                let chunk_size = (sub_batch.len() + num_threads - 1) / num_threads;
                let writer_ref = &writer;
                let progress_ref = &progress;
                std::thread::scope(|s| {
                    for chunk in sub_batch.chunks(chunk_size) {
                        s.spawn(move || {
                            for (id, embedding) in chunk {
                                writer_ref.add(*id, embedding);
                                progress_ref.inc(1);
                            }
                        });
                    }
                });
            }

            if args.deferred_balance {
                let balance_start = Instant::now();
                if args.parallel_balancing {
                    writer.balance_index_parallel(args.threads);
                } else {
                    writer.balance_index();
                }
                balance_time += balance_start.elapsed();
            }
        }
        progress.finish_and_clear();
        let index_time = index_start.elapsed() - balance_time;

        total_vectors += actual_count;
        all_indexed_vectors.extend(batch_vectors.iter().cloned());

        let throughput = actual_count as f64 / index_time.as_secs_f64();

        println!(
            "--- Checkpoint {} ({} total) ---",
            checkpoint_idx + 1,
            format_count(total_vectors),
        );
        if args.deferred_balance {
            println!(
                "  Indexed {} vec in {} ({:.0} vec/s) | balance {} | load {}",
                format_count(actual_count),
                format_duration(index_time),
                throughput,
                format_duration(balance_time),
                format_duration(load_time),
            );
        } else {
            println!(
                "  Indexed {} vec in {} ({:.0} vec/s) | load {}",
                format_count(actual_count),
                format_duration(index_time),
                throughput,
                format_duration(load_time),
            );
        }


        let mut delta = writer.stats.snapshot_delta(&prev_snapshot);
        delta.wall_nanos = (index_time + balance_time).as_nanos() as u64;
        prev_snapshot = writer.stats.snapshot();
        all_snapshots.push(delta);
    }

    println!("\n=== Build Summary ===");
    writer.print_tree_stats(format_count);
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

    let precomputed: Vec<&Query> = queries_by_checkpoint
        .get(&(total_vectors as u64))
        .map(|qs| qs.iter().collect())
        .unwrap_or_default();

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
        let level_widths: Vec<usize> = level_counts.iter().skip(1).take(num_levels).copied().collect();

        let mut beam_col_headers: Vec<String> = Vec::new();
        for lvl in 1..=num_levels {
            let total_at_level = level_counts.get(lvl).copied().unwrap_or(0);
            beam_col_headers.push(format!("L{} beam ({})", lvl, format_count(total_at_level)));
        }
        let beam_col_width = beam_col_headers.iter().map(|h| h.len()).max().unwrap_or(12).max(12);

        let mut header = format!(
            "  | {:>4} | {:>6} | {:>6} | {:>6} |",
            "nav", "tau", "rr_c", "rr_v",
        );
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

        let mut separator = format!(
            "  |{:-^6}|{:-^8}|{:-^8}|{:-^8}|",
            "", "", "", "",
        );
        for _ in 1..=num_levels {
            separator.push_str(&format!("{:-^w$}|{:-^10}|{:-^9}|", "", "", "", w = beam_col_width + 2));
        }
        separator.push_str(&format!(
            "{:-^12}|{:-^17}|{:-^9}|{:-^9}|{:-^9}|{:-^10}|{:-^12}|{:-^17}|{:-^17}|{:-^17}|{:-^17}|{:-^17}|{:-^16}|",
            "", "", "", "", "", "", "", "", "", "", "", "", "",
        ));

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
        println!("\n=== Index Quality ===");
        println!("  Index: {} vectors", format_count(writer.total_vectors()));
        print_cluster_stats("GT clusters (p100)", &all_p100);
        print_cluster_stats("GT clusters (p95) ", &all_p95);
        print_cluster_stats("GT clusters (p90) ", &all_p90);

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

        println!("\n=== Recall ===");
        println!(
            "  Search beam: tau={} min={} max={} | Tau sweep: {:?}",
            args.beam_tau, args.read_beam_min, args.read_beam_max, tau_values,
        );
        println!(
            "  Rerank centroids: {:?} | Rerank vectors: {:?}",
            args.recall_rerank_centroids, args.recall_rerank_vectors,
        );
        println!(
            "  Read nav: {} | Brute-force GT: {}",
            args.read_navigation.join(","),
            args.brute_force_gt,
        );
        if !read_level_taus.is_empty()
            || !read_level_min_pcts.is_empty()
        {
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

        for &recall_nav in &read_navs {
            let nav_label = match recall_nav {
                NavigationMode::Fp => "fp",
                NavigationMode::OneBit => "1bit",
                NavigationMode::FourBit => "4bit",
            };

            for &tau in &tau_values {
                for &rr_c in &args.recall_rerank_centroids {
                    for &rr_v in &args.recall_rerank_vectors {
                        let mut read_policy = ReadBeamPolicy::with_level_overrides(
                            Some(tau),
                            beam_min,
                            beam_max,
                            read_level_taus.clone(),
                            read_level_min_pcts.clone(),
                            level_widths.clone(),
                        );
                        read_policy.radius_corrected = args.radius_corrected_nav;
                        read_policy.leaf_rerank_reps = args.leaf_rerank_reps;

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

                        let results: Vec<QueryResult> = recall_pool.install(|| {
                            checkpoint_queries.par_iter().map(|gt| {
                                let t0 = Instant::now();
                                let (results, scanned, _leaves_scanned, timings) = writer
                                    .search_with_policy(&gt.vector, k, rr_c, rr_v, recall_nav, &read_policy);
                                let nanos = t0.elapsed().as_nanos() as u64;

                                let result_ids: Vec<u32> = results.iter().map(|(id, _)| *id).collect();
                                let r100 = recall_at_k(&result_ids, &gt.neighbors, 100);

                                let gt_100: HashSet<u32> =
                                    gt.neighbors.iter().take(100).copied().collect();
                                let level_recall = writer.diagnose_level_recall_with_policy(
                                    &gt.vector,
                                    &gt_100,
                                    rr_c,
                                    recall_nav,
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

                                QueryResult { r100, nanos, scanned, level_r100, level_beam, level_candidates, timings, optimal_r100 }
                            }).collect()
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

                        let mut row = format!(
                            "  | {:>4} | {:>6.2} | {:>5}x | {:>5}x |",
                            nav_label,
                            tau,
                            rr_c,
                            rr_v,
                        );
                        let dim = dimension;
                        let mut total_mb = 0.0f64;

                        for lvl in 0..num_levels {
                            let avg_beam = level_beam_sums[lvl] / num_queries as u64;
                            let avg_candidates = level_candidates_sums[lvl] / num_queries as u64;
                            let avg_lr = level_r100_sums[lvl] / n * 100.0;
                            let level_bytes = if recall_nav == NavigationMode::Fp {
                                (avg_candidates as f64 * dim as f64) * 4.0
                            } else {
                                let mut level_sum = (avg_candidates as f64 * dim as f64) / 8.0;
                                if rr_c > 1 {
                                    level_sum += ((avg_candidates.min(avg_beam * rr_c as u64) as f64) * dim as f64) * 4.0;
                                }
                                level_sum
                            };
                            let level_mb = level_bytes / (1024.0 * 1024.0);
                            total_mb += level_mb;
                            row.push_str(&format!(
                                " {:>width$} | {:>7.2}% | {:>7} |",
                                format!("{}/{}", format_count(avg_beam as usize), format_count(avg_candidates as usize)),
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
                        let mb_per_sec = if avg_lat_secs > 0.0 { total_mb / avg_lat_secs } else { 0.0 };

                        let nq = num_queries as u64;
                        let avg_nav = total_nav_nanos / nq;
                        let avg_qq = total_qq_nanos / nq;
                        let avg_dq = total_dq_nanos / nq;
                        let avg_sort = total_sort_nanos / nq;
                        let avg_rr = total_rr_nanos / nq;
                        let pct = |v: u64| if avg_lat > 0 { v as f64 / avg_lat as f64 * 100.0 } else { 0.0 };

                        let dist_per_vec_ns = if avg_scanned > 0 {
                            avg_dq as f64 / avg_scanned as f64
                        } else {
                            0.0
                        };

                        let avg_optimal = total_optimal_r100 / n;

                        row.push_str(&format!(
                            " {:>9.2}% | {:>15} | {:>7} | {:>7} | {:>7} | {:>7.2}% | {:>10} | {:>15} | {:>15} | {:>15} | {:>15} | {:>15} | {:>14} |",
                            avg_optimal * 100.0,
                            format_count(avg_scanned),
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
                    }
                }
            }
        }
        let recall_time = recall_start.elapsed();
        println!("  Recall duration: {}", format_duration(recall_time));

        if args.radius_corrected_nav {
            let diag_tau = tau_values[0];
            let diag_rr_c = args.recall_rerank_centroids[0];
            let diag_nav = read_navs[0];

            let mut policy_rc = ReadBeamPolicy::with_level_overrides(
                Some(diag_tau),
                beam_min,
                beam_max,
                read_level_taus.clone(),
                read_level_min_pcts.clone(),
                level_widths.clone(),
            );
            policy_rc.radius_corrected = true;
            policy_rc.leaf_rerank_reps = args.leaf_rerank_reps;

            let mut policy_no_rc = ReadBeamPolicy::with_level_overrides(
                Some(diag_tau),
                beam_min,
                beam_max,
                read_level_taus.clone(),
                read_level_min_pcts.clone(),
                level_widths.clone(),
            );
            policy_no_rc.radius_corrected = false;
            policy_no_rc.leaf_rerank_reps = args.leaf_rerank_reps;

            println!("\n--- Radius Correction Diagnostic (tau={:.2}, rr_c={}x, nav={:?}) ---",
                diag_tau, diag_rr_c, diag_nav);

            struct RcDiag {
                leaf_r100_rc: f64,
                leaf_r100_no_rc: f64,
                leaf_beam_rc: usize,
                leaf_beam_no_rc: usize,
            }

            let diags: Vec<RcDiag> = recall_pool.install(|| {
                checkpoint_queries.par_iter().map(|gt| {
                    let gt_100: HashSet<u32> =
                        gt.neighbors.iter().take(100).copied().collect();

                    let lr_rc = writer.diagnose_level_recall_with_policy(
                        &gt.vector, &gt_100, diag_rr_c, diag_nav, &policy_rc,
                    );
                    let lr_no = writer.diagnose_level_recall_with_policy(
                        &gt.vector, &gt_100, diag_rr_c, diag_nav, &policy_no_rc,
                    );

                    let last_rc = lr_rc.last().map(|l| (l.reachable_100, l.beam_size)).unwrap_or((0.0, 0));
                    let last_no = lr_no.last().map(|l| (l.reachable_100, l.beam_size)).unwrap_or((0.0, 0));

                    RcDiag {
                        leaf_r100_rc: last_rc.0,
                        leaf_r100_no_rc: last_no.0,
                        leaf_beam_rc: last_rc.1,
                        leaf_beam_no_rc: last_no.1,
                    }
                }).collect()
            });

            let n = diags.len() as f64;
            let avg_rc = diags.iter().map(|d| d.leaf_r100_rc).sum::<f64>() / n;
            let avg_no = diags.iter().map(|d| d.leaf_r100_no_rc).sum::<f64>() / n;
            let avg_beam_rc = diags.iter().map(|d| d.leaf_beam_rc).sum::<usize>() as f64 / n;
            let avg_beam_no = diags.iter().map(|d| d.leaf_beam_no_rc).sum::<usize>() as f64 / n;

            let mut improved = 0usize;
            let mut degraded = 0usize;
            let mut unchanged = 0usize;
            for d in &diags {
                let delta = d.leaf_r100_rc - d.leaf_r100_no_rc;
                if delta > 0.005 {
                    improved += 1;
                } else if delta < -0.005 {
                    degraded += 1;
                } else {
                    unchanged += 1;
                }
            }

            println!("  Leaf R@100 with rc:    {:.2}%  (avg beam: {:.1})", avg_rc * 100.0, avg_beam_rc);
            println!("  Leaf R@100 without rc: {:.2}%  (avg beam: {:.1})", avg_no * 100.0, avg_beam_no);
            println!("  Delta:                 {:+.2}%", (avg_rc - avg_no) * 100.0);
            println!("  Per-query: {} improved, {} degraded, {} unchanged (threshold: 0.5%)", improved, degraded, unchanged);

            let mut deltas: Vec<f64> = diags.iter().map(|d| d.leaf_r100_rc - d.leaf_r100_no_rc).collect();
            deltas.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let p10 = deltas[(0.1 * (deltas.len() - 1) as f64) as usize];
            let p50 = deltas[(0.5 * (deltas.len() - 1) as f64) as usize];
            let p90 = deltas[(0.9 * (deltas.len() - 1) as f64) as usize];
            println!("  Delta distribution: p10={:+.2}%, p50={:+.2}%, p90={:+.2}%",
                p10 * 100.0, p50 * 100.0, p90 * 100.0);
        }

        if args.leaf_miss_diagnostic || args.geometry_diagnostic {
            let diag_tau = tau_values[0];
            let diag_rr_c = args.recall_rerank_centroids[0];
            let diag_nav = read_navs[0];

            let mut policy = ReadBeamPolicy::with_level_overrides(
                Some(diag_tau),
                beam_min,
                beam_max,
                read_level_taus.clone(),
                read_level_min_pcts.clone(),
                level_widths.clone(),
            );
            policy.radius_corrected = args.radius_corrected_nav;
            policy.leaf_rerank_reps = args.leaf_rerank_reps;

            let diags: Vec<LeafMissDiagnostic> = recall_pool.install(|| {
                checkpoint_queries.par_iter().map(|gt| {
                    let gt_100: HashSet<u32> = gt.neighbors.iter().take(100).copied().collect();
                    writer.diagnose_leaf_miss_ranks(&gt.vector, &gt_100, diag_rr_c, diag_nav, &policy)
                }).collect()
            });

          if args.leaf_miss_diagnostic {
            println!("\n--- Leaf Miss Diagnostic (tau={:.2}, rr_c={}x, nav={:?}) ---",
                diag_tau, diag_rr_c, diag_nav);

            let n = diags.len() as f64;
            let avg_beam: f64 = diags.iter().map(|d| d.beam_size as f64).sum::<f64>() / n;
            let avg_total: f64 = diags.iter().map(|d| d.total_leaves as f64).sum::<f64>() / n;
            let avg_missed: f64 = diags.iter().map(|d| d.missed_gt_ranks.len() as f64).sum::<f64>() / n;
            let total_missed: usize = diags.iter().map(|d| d.missed_gt_ranks.len()).sum();

            println!("  Avg beam: {:.1} / {:.0} leaves", avg_beam, avg_total);
            println!("  Avg missed GT vectors: {:.1} / 100 ({} total across {} queries)",
                avg_missed, total_missed, diags.len());

            if total_missed > 0 {
                let mut all_ranks: Vec<usize> = diags.iter()
                    .flat_map(|d| d.missed_gt_ranks.iter().map(|&(_, rank)| rank))
                    .collect();
                all_ranks.sort_unstable();

                let pct = |idx: f64| all_ranks[(idx * (all_ranks.len() - 1) as f64) as usize];
                println!("  Missed leaf rank distribution (1-indexed):");
                println!("    min={}, p10={}, p25={}, p50={}, p75={}, p90={}, max={}",
                    all_ranks[0],
                    pct(0.10), pct(0.25), pct(0.50), pct(0.75), pct(0.90),
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
                    let pct_recovered = if total_missed > 0 { recovered as f64 / total_missed as f64 * 100.0 } else { 0.0 };
                    println!("    +{:>3} leaves: {:>4} / {} missed recovered ({:.1}%)",
                        extra, recovered, total_missed, pct_recovered);
                }

                let mut per_query: Vec<(usize, usize, Vec<usize>)> = diags.iter().enumerate()
                    .filter(|(_, d)| !d.missed_gt_ranks.is_empty())
                    .map(|(qi, d)| {
                        let ranks: Vec<usize> = d.missed_gt_ranks.iter().map(|&(_, r)| r).collect();
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
            let all_sel_gt: Vec<&LeafTraits> = diags.iter().flat_map(|d| d.selected_with_gt.iter()).collect();
            let all_sel_no: Vec<&LeafTraits> = diags.iter().flat_map(|d| d.selected_no_gt.iter()).collect();
            let all_miss: Vec<&LeafTraits> = diags.iter().flat_map(|d| d.missed_with_gt.iter()).collect();

            struct TraitSummary {
                label: &'static str,
                n: usize,
                score: [f64; 7],
                rank: [f64; 7],
                leaf_size: [f64; 7],
                p90_rad: [f64; 5],
                gt_count: [f64; 4],
                min_gt_d: [f64; 7],
                score_gt: [f64; 5],
            }

            fn compute_trait_summary(label: &'static str, traits: &[&LeafTraits]) -> Option<TraitSummary> {
                if traits.is_empty() {
                    return None;
                }
                let fp = |v: &[f32], p: f64| v[(p * (v.len() - 1) as f64) as usize] as f64;
                let up = |v: &[usize], p: f64| v[(p * (v.len() - 1) as f64) as usize] as f64;
                let favg = |v: &[f32]| v.iter().map(|x| *x as f64).sum::<f64>() / v.len() as f64;
                let uavg = |v: &[usize]| v.iter().sum::<usize>() as f64 / v.len() as f64;

                let mut scores: Vec<f32> = traits.iter().map(|t| t.score).collect();
                scores.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let mut sizes: Vec<usize> = traits.iter().map(|t| t.leaf_size).collect();
                sizes.sort_unstable();
                let mut radii: Vec<f32> = traits.iter().map(|t| t.p90_radius).collect();
                radii.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let mut ranks: Vec<usize> = traits.iter().map(|t| t.rank).collect();
                ranks.sort_unstable();

                let score = [scores[0] as f64, fp(&scores, 0.25), fp(&scores, 0.5), favg(&scores),
                    fp(&scores, 0.75), fp(&scores, 0.9), *scores.last().unwrap() as f64];
                let rank = [ranks[0] as f64, up(&ranks, 0.25), up(&ranks, 0.5), uavg(&ranks),
                    up(&ranks, 0.75), up(&ranks, 0.9), *ranks.last().unwrap() as f64];
                let leaf_size = [sizes[0] as f64, up(&sizes, 0.25), up(&sizes, 0.5), uavg(&sizes),
                    up(&sizes, 0.75), up(&sizes, 0.9), *sizes.last().unwrap() as f64];
                let p90_rad = [radii[0] as f64, fp(&radii, 0.25), fp(&radii, 0.5), favg(&radii),
                    *radii.last().unwrap() as f64];

                let gt_only: Vec<&LeafTraits> = traits.iter().filter(|t| t.gt_count > 0).copied().collect();
                let mut gt_counts: Vec<usize> = traits.iter().map(|t| t.gt_count).collect();
                gt_counts.sort_unstable();
                let gt_count = [gt_counts[0] as f64, up(&gt_counts, 0.5), uavg(&gt_counts),
                    *gt_counts.last().unwrap() as f64];

                let mut min_gt_d = [0.0f64; 7];
                let mut score_gt = [0.0f64; 5];
                if !gt_only.is_empty() {
                    let mut gt_dists: Vec<f32> = gt_only.iter().map(|t| t.min_gt_dist).collect();
                    gt_dists.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    min_gt_d = [gt_dists[0] as f64, fp(&gt_dists, 0.25), fp(&gt_dists, 0.5),
                        favg(&gt_dists), fp(&gt_dists, 0.75), fp(&gt_dists, 0.9), *gt_dists.last().unwrap() as f64];

                    let mut ratios: Vec<f32> = gt_only.iter()
                        .filter(|t| t.min_gt_dist > 1e-10)
                        .map(|t| t.score / t.min_gt_dist)
                        .collect();
                    if !ratios.is_empty() {
                        ratios.sort_by(|a, b| a.partial_cmp(b).unwrap());
                        score_gt = [ratios[0] as f64, fp(&ratios, 0.25), fp(&ratios, 0.5),
                            favg(&ratios), *ratios.last().unwrap() as f64];
                    }
                }

                Some(TraitSummary { label, n: traits.len(), score, rank, leaf_size, p90_rad, gt_count, min_gt_d, score_gt })
            }

            let summaries: Vec<TraitSummary> = [
                ("Sel+GT (TP)", &all_sel_gt),
                ("Sel+noGT (FP)", &all_sel_no),
                ("Miss+GT (FN)", &all_miss),
            ].iter().filter_map(|(label, data)| compute_trait_summary(label, data)).collect();

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

                let row_f4 = |label: &str, idx: usize, summaries: &[TraitSummary], getter: fn(&TraitSummary) -> &[f64]| {
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

                let row_f0 = |label: &str, idx: usize, summaries: &[TraitSummary], getter: fn(&TraitSummary) -> &[f64]| {
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

                let stats5a = ["min", "p25", "p50", "avg", "max"];
                println!("  -- p90_radius --");
                for (i, &lbl) in stats5a.iter().enumerate() {
                    row_f4(lbl, i, &summaries, |s| &s.p90_rad);
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
            println!("\n  --- Search Geometry (tau={:.2}, rr_c={}x, nav={:?}) ---",
                diag_tau, diag_rr_c, diag_nav);

            let pf = |v: &[f32], p: f64| -> f32 {
                if v.is_empty() { return 0.0; }
                v[(p * (v.len() - 1) as f64) as usize]
            };

            let mut cluster_radii: Vec<f32> = diags.iter().map(|d| {
                let mut r = d.beam_cluster_radii.clone();
                r.sort_by(|a, b| a.partial_cmp(b).unwrap());
                if r.is_empty() { 0.0 } else { r[r.len() / 2] }
            }).collect();
            cluster_radii.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let mut search_radii: Vec<f32> = diags.iter().map(|d| d.search_radius).collect();
            search_radii.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let mut beam_radii: Vec<f32> = diags.iter().map(|d| d.beam_radius).collect();
            beam_radii.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let mut gt_radii: Vec<f32> = diags.iter().map(|d| {
                d.gt_distances.iter().cloned().fold(0.0f32, f32::max)
            }).collect();
            gt_radii.sort_by(|a, b| a.partial_cmp(b).unwrap());

            println!("  {:32}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}",
                "metric", "min", "p25", "p50", "p75", "p90", "max");
            println!("  {:32}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}",
                "--------------------------------", "-------", "-------", "-------",
                "-------", "-------", "-------");

            for (label, vals) in [
                ("cluster radius (beam med p90)", &cluster_radii),
                ("search radius (d1*(1+tau))", &search_radii),
                ("beam radius (farthest sel.)", &beam_radii),
                ("GT radius (max gt dist)", &gt_radii),
            ] {
                if !vals.is_empty() {
                    println!("  {:32}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}  {:>7.4}",
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
        println!("reassign   - re-route a vector after split/merge (navigate + register + balance)");
        println!("scrub      - remove stale version entries from a leaf");
        println!("scrub_rm   - number of stale entries removed by scrub");
        println!("wall       - wall-clock time for the checkpoint");
        println!();
        println!("--- Task Breakdowns (concurrency diagnostics) ---");
        println!("navigate.missing_node  - navigate saw a child_id in a parent's children list but the");
        println!("                     node was missing from the DashMap (removed by concurrent split)");
        println!("add.missing_nodes     - add() failed to register in any navigated cluster (all gone)");
        println!("                     and fell back to inserting in the root node");
        println!("register.missing_node - register_in_leaf target was gone (split by balance cascade),");
        println!("                     fell back to reassign");
        println!();
        println!("--- Recall Table ---");
        println!("tau             - dynamic beam tau threshold (controls beam width)");
        println!("rr_c            - centroid rerank factor (1x = no rerank)");
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
        println!("lat_rerank      - time reranking top candidates with f32 embeddings (0 when rr_v=1)");
    }

    Ok(())
}
