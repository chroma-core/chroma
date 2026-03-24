//! Benchmark for HierarchicalSpannWriter: incremental index build with
//! recall evaluation at each checkpoint. Analogous to quantized_spann.rs
//! but using the in-memory hierarchical tree instead of USearch + blockfiles.

#![recursion_limit = "256"]

mod datasets;
mod hierarchical_index;

use std::collections::{BTreeMap, HashSet};
use std::io::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chroma_distance::DistanceFunction;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};

use datasets::{format_count, recall_at_k, Dataset, DatasetType, MetricType, Query};
use hierarchical_index::writer::{
    format_task_tables, HierarchicalSpannConfig, HierarchicalSpannWriter, WriterStatsSnapshot,
};

// =============================================================================
// CLI
// =============================================================================

#[derive(Parser, Debug)]
#[command(name = "hierarchical_spann_profile")]
#[command(about = "Benchmark for HierarchicalSpannWriter (full-precision hierarchical SPANN)")]
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

    /// Tau values for recall sweep, comma-separated
    #[arg(long, default_value = "0.1,0.5,1")]
    tau_values: String,

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

    /// nprobe used during add() for navigate
    #[arg(long, default_value = "64")]
    write_nprobe: usize,

    /// Max replicas per vector (RNG select)
    #[arg(long, default_value = "2")]
    nreplica_count: usize,

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
    let args = Args::parse();

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
        .tau_values
        .split(',')
        .map(|s| s.trim().parse().expect("invalid tau value"))
        .collect();

    let config = HierarchicalSpannConfig {
        branching_factor: args.branching_factor,
        split_threshold: args.split_threshold,
        merge_threshold: args.merge_threshold,
        write_nprobe: args.write_nprobe,
        beam_tau: args.beam_tau,
        beam_min: args.beam_min,
        beam_max: args.beam_max,
        nreplica_count: args.nreplica_count,
        write_rng_epsilon: args.write_rng_epsilon,
        write_rng_factor: args.write_rng_factor,
        reassign_neighbor_count: 32,
    };

    println!("=== Hierarchical SPANN Writer Benchmark ===");
    println!(
        "Dataset: {} ({} vectors, {} dims)",
        dataset.name(),
        format_count(data_len),
        dimension
    );
    println!(
        "Metric: {:?} | Checkpoints: {} ({}/CP)",
        distance_fn,
        num_checkpoints,
        format_count(batch_size),
    );
    println!(
        "Config: bf={} split={} merge={} write_nprobe={} replicas={} eps={} rng_f={}",
        config.branching_factor,
        config.split_threshold,
        config.merge_threshold,
        config.write_nprobe,
        config.nreplica_count,
        config.write_rng_epsilon,
        config.write_rng_factor,
    );
    println!(
        "Search beam: tau={} min={} max={} | Tau sweep: {:?} | Brute-force GT: {}",
        config.beam_tau, config.beam_min, config.beam_max, tau_values, args.brute_force_gt
    );
    println!("Threads: {}", args.threads);
    println!();

    let all_queries = dataset.queries(distance_fn.clone())?;
    let query_vectors: Vec<Vec<f32>> =
        all_queries.iter().take(100).map(|q| q.vector.clone()).collect();
    let queries_by_checkpoint = group_queries_by_checkpoint(all_queries);

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
        if num_threads <= 1 {
            for (id, embedding) in &batch_vectors {
                writer.add(*id, embedding);
                progress.inc(1);
            }
        } else {
            let chunk_size = (batch_vectors.len() + num_threads - 1) / num_threads;
            let writer_ref = &writer;
            let progress_ref = &progress;
            std::thread::scope(|s| {
                for chunk in batch_vectors.chunks(chunk_size) {
                    s.spawn(move || {
                        for (id, embedding) in chunk {
                            writer_ref.add(*id, embedding);
                            progress_ref.inc(1);
                        }
                    });
                }
            });
        }
        progress.finish_and_clear();
        let index_time = index_start.elapsed();

        total_vectors += actual_count;
        all_indexed_vectors.extend(batch_vectors.iter().cloned());

        let throughput = actual_count as f64 / index_time.as_secs_f64();

        println!(
            "--- Checkpoint {} ({} total) ---",
            checkpoint_idx + 1,
            format_count(total_vectors),
        );
        println!(
            "  Indexed {} vec in {} ({:.0} vec/s) | load {}",
            format_count(actual_count),
            format_duration(index_time),
            throughput,
            format_duration(load_time),
        );


        let mut delta = writer.stats.snapshot_delta(&prev_snapshot);
        delta.wall_nanos = index_time.as_nanos() as u64;
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
    let precomputed: Vec<&Query> = queries_by_checkpoint
        .get(&(total_vectors as u64))
        .map(|qs| qs.iter().collect())
        .unwrap_or_default();

    let computed_gt;
    let cached_gt;
    let cache_path = gt_cache_path(
        dataset.name(),
        &format!("{:?}", distance_fn),
        total_vectors,
        query_vectors.len(),
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
            query_vectors.len(),
            all_indexed_vectors.len()
        );
        let gt_start = Instant::now();
        computed_gt = compute_ground_truth(
            &query_vectors,
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

        // Build header with per-level beam + R@100 columns
        let mut header = format!(
            "  | {:>6} | {:>8} | {:>8} | {:>10} | {:>14} | {:>15} |",
            "tau", "R@10", "R@100", "avg lat", "scanned_leaves", "scanned_vectors"
        );
        for lvl in 1..=num_levels {
            header.push_str(&format!(
                " {:>6} | {:>10} |",
                format!("L{} beam", lvl),
                format!("L{} R@100", lvl),
            ));
        }

        let mut separator = format!(
            "  |{:-^8}|{:-^10}|{:-^10}|{:-^12}|{:-^8}|{:-^10}|",
            "", "", "", "", "", ""
        );
        for _ in 1..=num_levels {
            separator.push_str(&format!("{:-^8}|{:-^10}|", "", ""));
        }

        println!(
            "  Recall ({} queries, k={}, depth={}):",
            num_queries, k, tree_depth,
        );
        println!("{}", header);
        println!("{}", separator);

        let beam_min = args.beam_min;
        let beam_max = args.beam_max;

        let recall_start = Instant::now();

        for &tau in &tau_values {
            let mut total_r10 = 0.0;
            let mut total_r100 = 0.0;
            let mut total_nanos = 0u64;
            let mut total_scanned = 0usize;
            let mut total_leaves = 0usize;
            let mut level_r100_sums: Vec<f64> = vec![0.0; num_levels];
            let mut level_beam_sums: Vec<u64> = vec![0; num_levels];

            for gt in &checkpoint_queries {
                let t0 = Instant::now();
                let (results, scanned, leaves_scanned) =
                    writer.search_with_tau(&gt.vector, k, tau, beam_min, beam_max);
                total_nanos += t0.elapsed().as_nanos() as u64;
                total_scanned += scanned;
                total_leaves += leaves_scanned;

                let result_ids: Vec<u32> = results.iter().map(|(id, _)| *id).collect();
                total_r10 += recall_at_k(&result_ids, &gt.neighbors, 10);
                total_r100 += recall_at_k(&result_ids, &gt.neighbors, 100);

                let gt_100: HashSet<u32> =
                    gt.neighbors.iter().take(100).copied().collect();
                let level_recall =
                    writer.diagnose_level_recall(&gt.vector, &gt_100, tau, beam_min, beam_max);
                for lr in &level_recall {
                    if lr.level <= num_levels {
                        level_r100_sums[lr.level - 1] += lr.reachable_100;
                        level_beam_sums[lr.level - 1] += lr.beam_size as u64;
                    }
                }
            }

            let n = num_queries as f64;
            let avg_r10 = total_r10 / n;
            let avg_r100 = total_r100 / n;
            let avg_lat = total_nanos / num_queries as u64;
            let avg_scanned = total_scanned / num_queries;
            let avg_leaves = total_leaves / num_queries;

            let mut row = format!(
                "  | {:>6.2} | {:>7.2}% | {:>7.2}% | {:>10} | {:>6} | {:>8} |",
                tau,
                avg_r10 * 100.0,
                avg_r100 * 100.0,
                format_latency(avg_lat),
                format_count(avg_leaves),
                format_count(avg_scanned),
            );
            for lvl in 0..num_levels {
                let avg_beam = level_beam_sums[lvl] / num_queries as u64;
                let avg_lr = level_r100_sums[lvl] / n * 100.0;
                row.push_str(&format!(
                    " {:>6} | {:>7.2}% |",
                    format_count(avg_beam as usize),
                    avg_lr,
                ));
            }
            println!("{}", row);
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
        println!();
        println!("--- Recall ---");
        println!("R@100      - fraction of true top-100 neighbors found");
        println!("leaves     - avg number of leaf nodes visited per query");
        println!("scanned    - avg unique vectors whose f32 distances were computed per query");
        println!("Lk bm      - avg effective beam width (nodes kept) at tree level k");
        println!("Lk R@100   - fraction of true top-100 reachable at tree level k");
    }

    Ok(())
}
