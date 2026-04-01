//! Benchmark for 1-bit quantized HierarchicalSpannWriter: incremental index
//! build with recall evaluation at each checkpoint. Uses 1-bit RaBitQ codes
//! for both data vectors and centroid navigation.

#![recursion_limit = "256"]

mod datasets;
mod hierarchical_index;

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
    format_task_tables, HierarchicalSpannConfig, HierarchicalSpannWriter, NavigationMode,
    SearchTimings, WriterStatsSnapshot,
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

    /// Min beam width for dynamic beam
    #[arg(long, default_value = "10")]
    beam_min: usize,

    /// Max beam width for dynamic beam
    #[arg(long, default_value = "50000")]
    beam_max: usize,

    #[arg(long, default_value = "100")]
    branching_factor: usize,

    #[arg(long, default_value = "2048")]
    split_threshold: usize,

    #[arg(long, default_value = "512")]
    merge_threshold: usize,

    /// Dynamic beam tau for write path (add/reassign/merge navigate)
    #[arg(long, default_value = "1.0")]
    write_beam_tau: f64,

    /// Min beam width for write path
    #[arg(long, default_value = "10")]
    write_beam_min: usize,

    /// Max beam width for write path
    #[arg(long, default_value = "50000")]
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
    #[arg(long)]
    brute_force_gt: bool,

    /// Vector dimension (only for --dataset synthetic)
    #[arg(long, default_value = "1024")]
    dim: usize,

    /// Number of vectors for synthetic dataset
    #[arg(long, default_value = "1000000")]
    synthetic_size: usize,

    /// Default beam tau for search
    #[arg(long, default_value = "1.0")]
    beam_tau: f64,

    /// Number of threads for parallel add
    #[arg(long, default_value = "1")]
    threads: usize,

    /// Write-path navigation mode: fp (f32), 1bit (code-to-code), 4bit (QuantizedQuery)
    #[arg(long, default_value = "fp")]
    write_navigation: String,

    /// Read-path navigation mode: fp (f32), 1bit (code-to-code), 4bit (QuantizedQuery)
    #[arg(long, default_value = "1bit")]
    read_navigation: String,

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
    #[arg(long)]
    deferred_balance: bool,

    /// Run deferred balancing in parallel across subtrees
    #[arg(long)]
    parallel_balancing: bool,

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

    let distance_fn = args.metric.to_distance_function();

    let dataset: Box<dyn Dataset> = match args.dataset {
        DatasetType::DbPedia => Box::new(datasets::dbpedia::DbPedia::load().await?),
        DatasetType::Arxiv => Box::new(datasets::arxiv::Arxiv::load().await?),
        DatasetType::Sec => Box::new(datasets::sec::Sec::load().await?),
        DatasetType::MsMarco => Box::new(datasets::msmarco::MsMarco::load().await?),
        DatasetType::WikipediaEn => Box::new(datasets::wikipedia::Wikipedia::load().await?),
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
    let read_nav = parse_nav(&args.read_navigation, "--read-navigation");

    let config = HierarchicalSpannConfig {
        branching_factor: args.branching_factor,
        split_threshold: args.split_threshold,
        merge_threshold: args.merge_threshold,
        write_beam_tau: args.write_beam_tau,
        write_beam_min: args.write_beam_min,
        write_beam_max: args.write_beam_max,
        beam_tau: args.beam_tau,
        beam_min: args.beam_min,
        beam_max: args.beam_max,
        max_replicas: args.max_replicas,
        write_rng_epsilon: args.write_rng_epsilon,
        write_rng_factor: args.write_rng_factor,
        reassign_neighbor_count: 32,
        write_navigation: write_nav,
        read_navigation: read_nav,
        fp_npa: args.fp_npa,
        deferred_balance: args.deferred_balance,
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
    println!(
        "  Quantization: 1-bit | Write nav: {:?} | Read nav: {:?} | NPA: {}",
        write_nav,
        read_nav,
        if args.fp_npa { "f32" } else { "1-bit" },
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
    println!("\n{}", format_task_tables(&all_snapshots));

    let total_time = total_start.elapsed();
    let overall_throughput = total_vectors as f64 / total_time.as_secs_f64();

    println!("--- Summary ---");
    println!(
        "Total vectors: {} | Total time: {} | Overall: {:.0} vec/s\n",
        format_count(total_vectors),
        format_duration(total_time),
        overall_throughput,
    );

    println!("=== Recall ===");
    println!(
        "  Search beam: tau={} min={} max={} | Tau sweep: {:?}",
        args.beam_tau, args.beam_min, args.beam_max, tau_values,
    );
    println!(
        "  Rerank centroids: {:?} | Rerank vectors: {:?}",
        args.recall_rerank_centroids, args.recall_rerank_vectors,
    );
    println!(
        "  Read nav: {:?} | Brute-force GT: {}",
        read_nav,
        args.brute_force_gt,
    );

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

    let checkpoint_queries: Vec<&Query> = if !precomputed.is_empty() {
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

        let mut beam_col_headers: Vec<String> = Vec::new();
        for lvl in 1..=num_levels {
            let total_at_level = level_counts.get(lvl).copied().unwrap_or(0);
            beam_col_headers.push(format!("L{} beam ({})", lvl, format_count(total_at_level)));
        }
        let beam_col_width = beam_col_headers.iter().map(|h| h.len()).max().unwrap_or(12).max(12);

        let mut header = format!(
            "  | {:>6} | {:>6} | {:>6} |",
            "tau", "rr_c", "rr_v",
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
            " {:>15} | {:>7} | {:>7} | {:>7} | {:>8} | {:>10} | {:>15} | {:>15} | {:>15} | {:>15} | {:>15} | {:>14} |",
            "scanned_vectors", "scan MB", "tot MB", "MB/s", "R@100", "avg lat",
            "lat_nav", "lat_quant", "lat_dist", "lat_sort", "lat_rerank", "lat_dist / vec",
        ));

        let mut separator = format!(
            "  |{:-^8}|{:-^8}|{:-^8}|",
            "", "", "",
        );
        for _ in 1..=num_levels {
            separator.push_str(&format!("{:-^w$}|{:-^10}|{:-^9}|", "", "", "", w = beam_col_width + 2));
        }
        separator.push_str(&format!(
            "{:-^17}|{:-^9}|{:-^9}|{:-^9}|{:-^10}|{:-^12}|{:-^17}|{:-^17}|{:-^17}|{:-^17}|{:-^17}|{:-^16}|",
            "", "", "", "", "", "", "", "", "", "", "", "",
        ));

        println!(
            "  Recall ({} queries, k={}, depth={}):",
            num_queries, k, tree_depth,
        );
        println!("{}", header);
        println!("{}", separator);

        let beam_min = args.beam_min;
        let beam_max = args.beam_max;

        let recall_start = Instant::now();

        let recall_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(32)
            .build()
            .expect("failed to build rayon pool");

        for &tau in &tau_values {
            for &rr_c in &args.recall_rerank_centroids {
                for &rr_v in &args.recall_rerank_vectors {
                    struct QueryResult {
                        r100: f64,
                        nanos: u64,
                        scanned: usize,
                        level_r100: Vec<f64>,
                        level_beam: Vec<u64>,
                        level_candidates: Vec<u64>,
                        timings: SearchTimings,
                    }

                    let results: Vec<QueryResult> = recall_pool.install(|| {
                        checkpoint_queries.par_iter().map(|gt| {
                            let t0 = Instant::now();
                            let (results, scanned, _leaves_scanned, timings) =
                                writer.search(&gt.vector, k, tau, beam_min, beam_max, rr_c, rr_v);
                            let nanos = t0.elapsed().as_nanos() as u64;

                            let result_ids: Vec<u32> = results.iter().map(|(id, _)| *id).collect();
                            let r100 = recall_at_k(&result_ids, &gt.neighbors, 100);

                            let gt_100: HashSet<u32> =
                                gt.neighbors.iter().take(100).copied().collect();
                            let level_recall =
                                writer.diagnose_level_recall(&gt.vector, &gt_100, tau, beam_min, beam_max, rr_c);

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

                            QueryResult { r100, nanos, scanned, level_r100, level_beam, level_candidates, timings }
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
                    for qr in &results {
                        total_r100 += qr.r100;
                        total_nanos += qr.nanos;
                        total_scanned += qr.scanned;
                        total_nav_nanos += qr.timings.navigate_nanos;
                        total_qq_nanos += qr.timings.quantize_nanos;
                        total_dq_nanos += qr.timings.distance_nanos;
                        total_sort_nanos += qr.timings.sort_dedup_nanos;
                        total_rr_nanos += qr.timings.rerank_nanos;
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
                        "  | {:>6.2} | {:>5}x | {:>5}x |",
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
                        let level_bytes = if read_nav == NavigationMode::Fp {
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

                    row.push_str(&format!(
                        " {:>15} | {:>7} | {:>7} | {:>7} | {:>7.2}% | {:>10} | {:>15} | {:>15} | {:>15} | {:>15} | {:>15} | {:>14} |",
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
        let recall_time = recall_start.elapsed();
        println!("  Recall duration: {}", format_duration(recall_time));
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
