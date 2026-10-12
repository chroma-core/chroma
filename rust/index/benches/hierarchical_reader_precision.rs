//! Read-only precision and beam experiments against one saved tree and query set.
#![recursion_limit = "256"]

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(dead_code)]
mod datasets;
#[allow(dead_code)]
mod hierarchical_index;

use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{anyhow, ensure, Result};
use chroma_blockstore::arrow::provider::ArrowBlockfileProvider;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_cache::FoyerCacheConfig;
use chroma_distance::DistanceFunction;
use chroma_storage::{local::LocalStorage, Storage};
use clap::Parser;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use datasets::ground_truth;
use hierarchical_index::common::ReadBeamPolicy;
use hierarchical_index::config::HierarchicalSpannConfig;
use hierarchical_index::reader::{HierarchicalSpannReader, NavigationPrecision};
use hierarchical_index::writer::HierarchicalSpannIds;

#[derive(Parser)]
struct Args {
    /// Saved index directory; this experiment opens it for reads only.
    #[arg(long)]
    index: PathBuf,
    /// Exact query vectors and answers for the saved vector prefix.
    #[arg(long)]
    ground_truth: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "1bit-4bit,4bit-4bit,4bit-fp"
    )]
    navigation: Vec<NavigationPrecision>,
    /// Keep all root children; vary the next internal level independently.
    #[arg(long, value_delimiter = ',', default_value = "64,256")]
    internal_beams: Vec<usize>,
    #[arg(long, value_delimiter = ',', default_value = "8,16,32,64,128,256")]
    leaf_beams: Vec<usize>,
    #[arg(long, value_delimiter = ',', default_value = "1,2,4,8,16")]
    rerank: Vec<usize>,
    #[arg(long, default_value = "100")]
    queries: usize,
    #[arg(long, default_value = "32")]
    threads: usize,
    #[arg(long, default_value = "1073741824")]
    max_cache_bytes: u64,
    /// Repeat each cold/warm pair to expose variation at a fixed setting.
    #[arg(long, default_value = "1")]
    repeats: usize,
}

#[derive(Deserialize)]
struct Checkpoint {
    total_vectors: usize,
    posting_list_id: uuid::Uuid,
    scalar_metadata_id: uuid::Uuid,
    vector_data_id: uuid::Uuid,
    leaf_node_id: uuid::Uuid,
    internal_node_id: uuid::Uuid,
}

#[derive(Serialize)]
struct Row {
    navigation: String,
    internal_beam: usize,
    leaf_beam: usize,
    rerank_factor: usize,
    repeat: usize,
    phase: String,
    queries: usize,
    reachable_recall: Vec<f64>,
    selected_per_level: Vec<f64>,
    candidates_per_level: Vec<f64>,
    recall_at_100: f64,
    leaves_scanned: f64,
    codes_scanned: f64,
    vectors_reranked: f64,
    logical_centroid_bytes: usize,
    estimated_query_bytes: f64,
    latency_ms: f64,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    navigation_ms: f64,
    query_quantization_ms: f64,
    posting_distance_ms: f64,
    selection_ms: f64,
    rerank_ms: f64,
    wall_s: f64,
}

async fn provider(args: &Args) -> BlockfileProvider {
    let blocks = FoyerCacheConfig {
        name: "reader_precision_blocks".into(),
        capacity: (args.max_cache_bytes / (1024 * 1024)).max(64) as usize,
        ..Default::default()
    }
    .build_memory_persistent()
    .await
    .unwrap();
    let roots = FoyerCacheConfig {
        name: "reader_precision_roots".into(),
        capacity: 4096,
        ..Default::default()
    }
    .build_memory_persistent()
    .await
    .unwrap();
    BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
        Storage::Local(LocalStorage::new(args.index.join("data").to_str().unwrap())),
        3 * 1024 * 1024,
        blocks,
        roots,
        16,
        0,
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.queries > 0 && args.threads > 0 && args.repeats > 0);
    ensure!(args
        .leaf_beams
        .iter()
        .chain(&args.internal_beams)
        .chain(&args.rerank)
        .all(|&x| x > 0));
    let checkpoint: Checkpoint =
        serde_json::from_slice(&std::fs::read(args.index.join("checkpoint.json"))?)?;
    let queries: Vec<_> = ground_truth::load(&args.ground_truth, DistanceFunction::Euclidean)?
        .into_iter()
        .filter(|q| q.max_vector_id == checkpoint.total_vectors as u64)
        .take(args.queries)
        .collect();
    ensure!(
        queries.len() == args.queries,
        "not enough exact queries for this checkpoint"
    );
    ensure!(queries.iter().all(|q| q.neighbors.len() >= 100));
    let ids = HierarchicalSpannIds {
        posting_list_id: checkpoint.posting_list_id,
        scalar_metadata_id: checkpoint.scalar_metadata_id,
        vector_data_id: checkpoint.vector_data_id,
        leaf_node_id: checkpoint.leaf_node_id,
        internal_node_id: checkpoint.internal_node_id,
    };
    let provider = provider(&args).await;
    let mut reader = HierarchicalSpannReader::open(
        &provider,
        ids,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig {
            max_replicas: 1,
            ..Default::default()
        },
    )
    .await
    .map_err(|e| anyhow!("{e}"))?;
    ensure!(
        reader.depth() == 4,
        "this beam sweep expects the baseline's four-level tree"
    );
    let prepare = Instant::now();
    reader
        .load_all_postings()
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let neighbors: Vec<_> = queries
        .iter()
        .map(|q| q.neighbors.iter().take(100).copied().collect())
        .collect();
    let reachable = reader.ground_truth_reachability(&neighbors);
    // Verify every exact neighbor is present before attributing loss to search.
    for (query, paths) in reachable.iter().enumerate() {
        let covered: HashSet<_> = paths.values().flat_map(|ids| ids.iter().copied()).collect();
        ensure!(
            neighbors[query].iter().all(|id| covered.contains(id)),
            "ground truth missing from saved postings"
        );
    }
    reader.clear_loaded_blocks();
    reader.clear_loaded_postings();
    println!("Read-only precision sweep: {} vectors, {} queries, {} threads; ground-truth map prepared in {:.2}s",
        checkpoint.total_vectors, queries.len(), args.threads, prepare.elapsed().as_secs_f64());
    println!("Cold clears application postings and block pins; OS pages remain cached. Estimated bytes are logical payload, not network traffic.");
    let mut output = std::fs::File::create(&args.output)?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.threads)
        .build()?;
    let runtime = tokio::runtime::Handle::current();
    for &mode in &args.navigation {
        let setup = Instant::now();
        reader
            .set_navigation_precision(mode)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        println!(
            "Navigation {} ready in {:.3}s; code payload {} bytes",
            mode.label(),
            setup.elapsed().as_secs_f64(),
            reader.navigation_code_bytes()
        );
        for &internal in &args.internal_beams {
            for &beam in &args.leaf_beams {
                let policy = ReadBeamPolicy::fixed_widths(vec![usize::MAX, internal, beam]);
                // Replay the actual reader navigation outside measured query
                // time. Reachability therefore follows this mode, not the
                // writer's original one-bit diagnostics.
                let traces: Vec<_> = pool.install(|| {
                    queries
                        .par_iter()
                        .map(|q| reader.navigation_trace(&q.vector, &policy))
                        .collect()
                });
                let mut reach = vec![0.0; 3];
                let mut selected = vec![0.0; 3];
                let mut candidates = vec![0.0; 3];
                for (query, trace) in traces.iter().enumerate() {
                    ensure!(trace.levels.len() == 3);
                    for level in 0..3 {
                        let found: HashSet<_> = trace.levels[level]
                            .iter()
                            .filter_map(|id| reachable[query].get(id))
                            .flat_map(|ids| ids.iter().copied())
                            .collect();
                        reach[level] += found.len() as f64 / 100.0 / queries.len() as f64;
                        selected[level] += trace.levels[level].len() as f64 / queries.len() as f64;
                        candidates[level] += trace.candidates[level] as f64 / queries.len() as f64;
                    }
                }
                for &rerank in &args.rerank {
                    for repeat in 0..args.repeats {
                        for phase in ["cold", "warm"] {
                            if phase == "cold" {
                                reader.clear_loaded_blocks();
                                reader.clear_loaded_postings();
                            }
                            let wall = Instant::now();
                            let results: Vec<_> = pool.install(|| {
                                queries
                                    .par_iter()
                                    .map(|q| {
                                        let start = Instant::now();
                                        let (found, scanned, leaves, timing) = runtime
                                            .block_on(reader.search_with_policy_lazy(
                                                &q.vector, 100, rerank, &policy,
                                            ))
                                            .unwrap_or_else(|e| panic!("search failed: {e}"));
                                        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                                        let ids: Vec<_> = found.iter().map(|(id, _)| *id).collect();
                                        (
                                            datasets::recall_at_k(&ids, &q.neighbors, 100),
                                            scanned,
                                            leaves,
                                            timing,
                                            elapsed,
                                        )
                                    })
                                    .collect()
                            });
                            let n = queries.len() as f64;
                            let mut latencies: Vec<_> = results.iter().map(|r| r.4).collect();
                            latencies.sort_by(f64::total_cmp);
                            let scanned = results.iter().map(|r| r.1 as f64).sum::<f64>() / n;
                            let reranked = if rerank > 1 {
                                results
                                    .iter()
                                    .map(|r| r.1.min(100 * rerank) as f64)
                                    .sum::<f64>()
                                    / n
                            } else {
                                0.0
                            };
                            let dim = queries[0].vector.len();
                            let centroid_size = if mode == NavigationPrecision::OneBitFourBit {
                                chroma_index::quantization::Code::<1>::size(dim)
                            } else {
                                chroma_index::quantization::Code::<4>::size(dim)
                            };
                            let centroid_bytes =
                                candidates.iter().sum::<f64>() * centroid_size as f64;
                            let row = Row {
                                navigation: mode.label().into(),
                                internal_beam: internal,
                                leaf_beam: beam,
                                rerank_factor: rerank,
                                repeat,
                                phase: phase.into(),
                                queries: queries.len(),
                                reachable_recall: reach.clone(),
                                selected_per_level: selected.clone(),
                                candidates_per_level: candidates.clone(),
                                recall_at_100: results.iter().map(|r| r.0).sum::<f64>() / n,
                                leaves_scanned: results.iter().map(|r| r.2 as f64).sum::<f64>() / n,
                                codes_scanned: scanned,
                                vectors_reranked: reranked,
                                logical_centroid_bytes: reader.navigation_code_bytes(),
                                estimated_query_bytes: centroid_bytes
                                    + scanned
                                        * chroma_index::quantization::Code::<1>::size(dim) as f64
                                    + (selected.last().unwrap() + reranked) * (dim * 4) as f64,
                                latency_ms: latencies.iter().sum::<f64>() / n,
                                latency_p50_ms: latencies[latencies.len() / 2],
                                latency_p95_ms: latencies
                                    [(latencies.len() * 95 / 100).min(latencies.len() - 1)],
                                navigation_ms: results
                                    .iter()
                                    .map(|r| r.3.navigate_nanos as f64)
                                    .sum::<f64>()
                                    / n
                                    / 1e6,
                                query_quantization_ms: results
                                    .iter()
                                    .map(|r| r.3.quantize_nanos as f64)
                                    .sum::<f64>()
                                    / n
                                    / 1e6,
                                posting_distance_ms: results
                                    .iter()
                                    .map(|r| r.3.distance_nanos as f64)
                                    .sum::<f64>()
                                    / n
                                    / 1e6,
                                selection_ms: results
                                    .iter()
                                    .map(|r| r.3.sort_dedup_nanos as f64)
                                    .sum::<f64>()
                                    / n
                                    / 1e6,
                                rerank_ms: results
                                    .iter()
                                    .map(|r| r.3.rerank_nanos as f64)
                                    .sum::<f64>()
                                    / n
                                    / 1e6,
                                wall_s: wall.elapsed().as_secs_f64(),
                            };
                            serde_json::to_writer(&mut output, &row)?;
                            writeln!(output)?;
                            output.flush()?;
                            println!("{} internal={} leaves={} rr={} {} reach={:.2}% recall={:.2}% scanned={:.0} reranked={:.0} mean={:.2}ms nav={:.2}ms",
                                row.navigation,internal,beam,rerank,phase,reach[2]*100.0,row.recall_at_100*100.0,scanned,reranked,row.latency_ms,row.navigation_ms);
                            std::io::stdout().flush()?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
