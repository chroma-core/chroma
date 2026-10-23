//! Fresh-process validation consumes only saved files and an independently
//! verified immutable vector corpus; it starts no writer service. Bench mode
//! runs frozen queries against exact references and records recall, latency,
//! CPU, bytes and heap per query for reader development.
#![recursion_limit = "256"]
#![allow(unexpected_cfgs, dead_code)]
mod hierarchical_index;
use clap::Parser;
use hierarchical_index::{
    common::ReadBeamPolicy,
    distributed::{
        engine::VectorSource,
        generation::{GenerationDescriptor, GenerationStore},
        generation_backend::DurableStoreConfig,
        reader_bench::{self as rb, BenchConfig, BenchQuery},
        saved_reader::{CanonicalVectorSource, SavedReader},
    },
    mem_probe,
};
use rayon::prelude::*;
use serde_json::{json, Value};
use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    time::Instant,
};
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: rb::CountingJemalloc = rb::CountingJemalloc;
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Mode {
    Validate,
    Bench,
}
#[derive(Parser)]
struct Args {
    #[arg(long, value_enum, default_value_t = Mode::Validate)]
    mode: Mode,
    #[arg(long)]
    store: PathBuf,
    /// Shared durable backend configuration; --store is only a local export path.
    #[arg(long)]
    storage_config: Option<PathBuf>,
    #[arg(long)]
    descriptor: Option<PathBuf>,
    /// Relocate the same corpus; all persisted identity checks still apply.
    #[arg(long)]
    vector_path: Option<PathBuf>,
    /// Conservative resident corpus-cache allowance, including row allocations.
    #[arg(long, default_value_t = 12 * 1024 * 1024 * 1024u64)]
    vector_cache_bytes: u64,
    #[arg(long)]
    expected_rows: u64,
    /// Validation report file, or the bench results directory.
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 16)]
    query_count: usize,
    #[arg(long, default_value_t = 32)]
    beam: usize,
    #[arg(long, default_value_t = 10)]
    k: usize,
    #[arg(long, default_value_t = 100)]
    rerank: usize,
    /// Bench: frozen query vectors, one JSON object per line.
    #[arg(long)]
    queries: Option<PathBuf>,
    /// Bench: exact top-100 references for the same queries.
    #[arg(long)]
    groundtruth: Option<PathBuf>,
    /// Bench: named configurations to sweep.
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    query_limit: Option<usize>,
    /// Bench: comma-separated passes per configuration, from `cold` and `warm`.
    #[arg(long, default_value = "cold,warm")]
    passes: String,
    /// Bench: threads for the cold pass, which fills caches in parallel.
    #[arg(long, default_value_t = 32)]
    cold_threads: usize,
    /// Bench: skip the corpus; only rerank 0 can run, and recall cannot
    /// resolve ties that continue past the listed references.
    #[arg(long)]
    no_vectors: bool,
}
fn open_reader(
    args: &Args,
) -> Result<(GenerationDescriptor, SavedReader), Box<dyn std::error::Error>> {
    let store = if let Some(path) = &args.storage_config {
        GenerationStore::open_durable(&serde_json::from_slice::<DurableStoreConfig>(&fs::read(
            path,
        )?)?)?
    } else {
        GenerationStore::open(&args.store)?
    };
    let descriptor: GenerationDescriptor = if let Some(path) = &args.descriptor {
        serde_json::from_slice(&fs::read(path)?)?
    } else {
        store.latest()?.ok_or("saved generation is absent")?
    };
    let reader = SavedReader::open(store, &descriptor)?;
    if reader.manifest.total_vectors != args.expected_rows {
        return Err("saved generation count differs from expected workload".into());
    }
    Ok((descriptor, reader))
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.output.exists() {
        return Err("validation output already exists; preserve prior attempt".into());
    }
    if args.mode == Mode::Bench {
        return bench(&args);
    }
    if args.expected_rows == 0 || args.query_count == 0 || args.beam == 0 || args.k == 0 {
        return Err("positive validation bounds are required".into());
    }
    let begin = Instant::now();
    let (descriptor, reader) = open_reader(&args)?;
    let t = Instant::now();
    let vectors = reader
        .open_vector_source_at(args.vector_path.as_deref())?
        .preload_with_limit(args.vector_cache_bytes)?;
    let vector_validation = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let membership = reader.validate_membership()?;
    let first = vectors.descriptor().first_id;
    let end = first
        .checked_add(args.expected_rows)
        .ok_or("expected ID interval overflow")?;
    if membership.len() as u64 != args.expected_rows || membership.iter().copied().ne(first..end) {
        return Err("saved exact membership differs from input IDs".into());
    }
    let (nodes, validated_vectors) = reader.validate_codes(&vectors)?;
    let membership_and_codes = t.elapsed().as_secs_f64();
    let mut queries = vec![];
    for slot in 0..args.query_count.min(args.expected_rows as usize) {
        let id = first + (slot as u64 * args.expected_rows / args.query_count as u64);
        let query = vectors.get(id)?;
        let t = Instant::now();
        let (results, work) = reader.query(
            &query,
            args.k,
            &ReadBeamPolicy::uniform(None, args.beam, args.beam),
            args.rerank,
            if args.rerank > 0 {
                Some(&vectors)
            } else {
                None
            },
        )?;
        if results.is_empty()
            || results
                .iter()
                .any(|(id, d)| !membership.contains(id) || !d.is_finite())
        {
            return Err("saved query returned invalid IDs or scores".into());
        }
        queries.push(json!({"input_id":id,"seconds":t.elapsed().as_secs_f64(),"results":results,"work":work}));
    }
    let report = json!({"status":"durable_reader_validated","generation":descriptor,"rows":args.expected_rows,"nodes":nodes,"validated_posting_codes":validated_vectors,"vector_sha256_verified":true,"exact_membership":true,"all_centroid_codes_recomputed":true,"all_posting_codes_recomputed":true,"vector_validation_seconds":vector_validation,"membership_and_code_validation_seconds":membership_and_codes,"elapsed_seconds":begin.elapsed().as_secs_f64(),"queries":queries,"query_cohort":"training-vector smoke queries, not held-out recall evidence","peak_rss":mem_probe::read_self().rss_peak,"writer_objects_required":false});
    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(&args.output)?;
    file.write_all(&serde_json::to_vec_pretty(&report)?)?;
    file.sync_all()?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

/// Shared inputs for every query of one configuration.
struct BenchContext<'a> {
    reader: &'a SavedReader,
    vectors: Option<&'a CanonicalVectorSource>,
    config: &'a BenchConfig,
    policy: ReadBeamPolicy,
    centroid_code_bytes: u64,
}
/// Run one query and describe it. `alone` marks a query with nothing else in
/// flight, so process-wide CPU and heap counters belong to it.
fn run_query(
    cx: &BenchContext,
    index: usize,
    q: &BenchQuery,
    pass: &str,
    alone: bool,
) -> Result<Value, String> {
    rb::take_touched_vectors();
    let recording = cx.vectors.map(|v| rb::RecordingVectors(v));
    if alone {
        rb::start_heap_peak();
    }
    let cpu = rb::process_cpu_seconds();
    let t = Instant::now();
    let (results, work) = cx.reader.query(
        &q.vector,
        cx.config.k,
        &cx.policy,
        cx.config.rerank,
        recording.as_ref().map(|r| r as &dyn VectorSource),
    )?;
    let seconds = t.elapsed().as_secs_f64();
    let cpu = rb::process_cpu_seconds() - cpu;
    let heap_peak = alone.then(rb::finish_heap_peak);
    let touched = rb::take_touched_vectors();
    let ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
    let recall = rb::tie_aware_recall(&ids, q, cx.config.k, &mut |id| {
        let v = cx.vectors?.get(id).ok()?;
        Some(rb::exact_squared_l2(&q.vector, &v))
    })?;
    let (row_bytes, pages) = match cx.vectors {
        Some(v) => {
            let d = v.descriptor();
            let row = d.dimensions as u64 * 4;
            let pages = rb::distinct_pages(&touched, d.first_id, d.offset_bytes, d.stride_bytes, row);
            (row, pages)
        }
        None => (0, 0),
    };
    let page_bytes = pages * rb::PAGE_BYTES;
    Ok(json!({
        "config": cx.config.name, "pass": pass, "query_index": index, "query_id": q.id,
        "seconds": seconds,
        "cpu_seconds": if alone { json!(cpu) } else { Value::Null },
        "recall": recall, "recall_fraction": recall.fraction(),
        "ids": ids, "distances": results.iter().map(|(_, d)| *d).collect::<Vec<_>>(),
        "work": work,
        "bytes": {
            "posting_bytes_fetched": work.posting_object_bytes,
            "posting_fetches": work.leaves,
            "navigation_codes_read": work.navigation.distances * cx.centroid_code_bytes,
            "postings_scanned": work.posting_code_bytes + work.vectors as u64 * 8,
            "full_vector_rows_read": touched.len() as u64 * row_bytes,
            "full_vector_pages": page_bytes,
            "cold_equivalent": work.posting_object_bytes + page_bytes,
        },
        "heap_peak_bytes": heap_peak,
    }))
}
const BYTE_FIELDS: [&str; 7] = [
    "posting_bytes_fetched",
    "posting_fetches",
    "navigation_codes_read",
    "postings_scanned",
    "full_vector_rows_read",
    "full_vector_pages",
    "cold_equivalent",
];
fn summarize(records: &[Value], pass_seconds: f64, network: Option<u64>) -> Value {
    let field = |path: &[&str]| -> Vec<f64> {
        records
            .iter()
            .filter_map(|r| path.iter().try_fold(r, |v, key| v.get(key))?.as_f64())
            .filter(|x| x.is_finite())
            .collect()
    };
    let ms = |path: &[&str]| json!(rb::stats(&field(path).iter().map(|x| x * 1e3).collect::<Vec<_>>()));
    let stat = |path: &[&str]| json!(rb::stats(&field(path)));
    let bytes: serde_json::Map<_, _> = BYTE_FIELDS
        .iter()
        .map(|key| (key.to_string(), stat(&["bytes", key])))
        .collect();
    let mem = mem_probe::read_self();
    json!({
        "queries": records.len(),
        "pass_seconds": pass_seconds,
        // Includes recall and record building; the reader-only figure divides by
        // summed query latency instead.
        "harness_queries_per_second": records.len() as f64 / pass_seconds,
        "reader_queries_per_second": records.len() as f64 / field(&["seconds"]).iter().sum::<f64>(),
        "latency_ms": ms(&["seconds"]),
        "cpu_ms": ms(&["cpu_seconds"]),
        "recall": rb::stats(&field(&["recall_fraction"])).map(|s| s.mean),
        "recall_unresolved": field(&["recall", "unresolved"]).iter().sum::<f64>(),
        "bytes": bytes,
        "heap_peak_bytes": stat(&["heap_peak_bytes"]),
        "leaves": stat(&["work", "leaves"]),
        "vectors_scanned": stat(&["work", "vectors"]),
        "vectors_reranked": stat(&["work", "reranked"]),
        "navigation_distances": stat(&["work", "navigation", "distances"]),
        "owner_requests": stat(&["work", "navigation", "owner_requests"]),
        "network_received_bytes_all_traffic": network,
        "rss_bytes_after": mem.rss,
        "rss_peak_bytes": mem.rss_peak,
    })
}
fn bench(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let (Some(queries_path), Some(truth_path), Some(config_path)) =
        (&args.queries, &args.groundtruth, &args.config)
    else {
        return Err("bench mode needs --queries, --groundtruth and --config".into());
    };
    let passes: Vec<&str> = args.passes.split(',').map(str::trim).collect();
    if passes.iter().any(|p| !matches!(*p, "cold" | "warm")) || args.cold_threads == 0 {
        return Err("passes must be cold and/or warm, with at least one cold thread".into());
    }
    let configs = rb::load_configs(config_path)?;
    if args.no_vectors && configs.iter().any(|c| c.rerank > 0) {
        return Err("rerank needs the corpus; drop --no-vectors".into());
    }
    let begin = Instant::now();
    let (descriptor, reader) = open_reader(args)?;
    let routing_open_seconds = begin.elapsed().as_secs_f64();
    let t = Instant::now();
    let vectors = if args.no_vectors {
        None
    } else {
        Some(
            reader
                .open_vector_source_at(args.vector_path.as_deref())?
                .preload_with_limit(args.vector_cache_bytes)?,
        )
    };
    let vector_open_seconds = t.elapsed().as_secs_f64();
    let open_rss = mem_probe::read_self().rss;
    let queries = rb::load_queries(
        queries_path,
        truth_path,
        reader.manifest.format.dimensions,
        args.query_limit,
    )?;
    fs::create_dir_all(&args.output)?;
    let mut records = BufWriter::new(File::create(args.output.join("queries.jsonl"))?);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.cold_threads)
        .thread_name(|i| format!("reader-bench-{i}"))
        .build()?;
    let mut summaries = vec![];
    for config in &configs {
        let cx = BenchContext {
            reader: &reader,
            vectors: vectors.as_ref(),
            config,
            policy: ReadBeamPolicy::uniform(config.tau, config.beam_min, config.beam_max),
            centroid_code_bytes: reader.centroid_code_bytes() as u64,
        };
        for &pass in &passes {
            let network = rb::network_received_bytes();
            let t = Instant::now();
            let results: Vec<Value> = if pass == "cold" {
                let alone = args.cold_threads == 1;
                pool.install(|| {
                    queries
                        .par_iter()
                        .enumerate()
                        .map(|(i, q)| run_query(&cx, i, q, pass, alone))
                        .collect::<Result<_, _>>()
                })?
            } else {
                queries
                    .iter()
                    .enumerate()
                    .map(|(i, q)| run_query(&cx, i, q, pass, true))
                    .collect::<Result<_, _>>()?
            };
            let seconds = t.elapsed().as_secs_f64();
            let network = rb::network_received_bytes()
                .zip(network)
                .map(|(after, before)| after.saturating_sub(before));
            for r in &results {
                serde_json::to_writer(&mut records, r)?;
                records.write_all(b"\n")?;
            }
            let summary = summarize(&results, seconds, network);
            let num = |v: &Value| v.as_f64().unwrap_or(f64::NAN);
            println!(
                "{} {}: recall@{} {:.4}, p50 {:.2} ms, p99 {:.2} ms, {:.1} qps",
                config.name,
                pass,
                config.k,
                num(&summary["recall"]),
                num(&summary["latency_ms"]["p50"]),
                num(&summary["latency_ms"]["p99"]),
                num(&summary["reader_queries_per_second"]),
            );
            summaries.push(json!({"config": config, "pass": pass, "summary": summary}));
        }
    }
    records.flush()?;
    let report = json!({
        "status": "reader_bench_complete",
        "generation": descriptor,
        "rows": args.expected_rows,
        "queries": queries.len(),
        "passes": passes,
        "cold_threads": args.cold_threads,
        // Today's reader fetches every visited posting object on every query,
        // from S3 for a durable store or from local files otherwise.
        "posting_store": if args.storage_config.is_some() { "durable" } else { "local" },
        "corpus": vectors.as_ref().map(|v| v.descriptor().path.clone()),
        "routing_open_seconds": routing_open_seconds,
        "vector_open_seconds": vector_open_seconds,
        "open_rss_bytes": open_rss,
        "elapsed_seconds": begin.elapsed().as_secs_f64(),
        "results": summaries,
    });
    let mut file = File::create(args.output.join("summary.json"))?;
    file.write_all(&serde_json::to_vec_pretty(&report)?)?;
    file.sync_all()?;
    Ok(())
}
