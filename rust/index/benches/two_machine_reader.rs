//! Fresh-process validation consumes only saved files and an independently
//! verified immutable vector corpus; it starts no writer service. Bench mode
//! runs frozen queries against exact references and records recall, latency,
//! CPU, bytes and heap per query for reader development. Serve mode answers
//! leaf scans for one owner, so a bench process on another machine can read a
//! replicated routing tree and spread the scans across owners.
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
        mapped_vectors::MappedVectorSource,
        owner_scan::{self, OwnerQuery, RemoteOwner, ScanEngine, ScanRequest},
        reader_bench::{self as rb, BenchConfig, BenchQuery},
        saved_reader::{CanonicalVectorSource, SavedReader},
        transport::{RpcClient, RpcServer, TransportConfig},
    },
    mem_probe,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: rb::CountingJemalloc = rb::CountingJemalloc;
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Mode {
    Validate,
    Bench,
    Serve,
}
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum ReaderKind {
    /// Today's single-process reader: no cache, serial fetches.
    Single,
    /// Local navigation, cached per-owner scans, remote owners over RPC.
    Owners,
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
    /// Bench: which reader answers queries.
    #[arg(long, value_enum, default_value_t = ReaderKind::Single)]
    reader: ReaderKind,
    /// Owners whose leaves this process scans: the served owner in serve mode,
    /// or the query machine's own owners in bench mode. Empty in bench mode
    /// with no --remote means every owner is scanned locally.
    #[arg(long, value_delimiter = ',')]
    local_owners: Vec<u64>,
    /// Bench: a remote owner as OWNER@DATA_ADDR/CONTROL_ADDR; repeatable.
    #[arg(long)]
    remote: Vec<String>,
    /// Serve: data and control listen addresses.
    #[arg(long)]
    scan_data_bind: Option<std::net::SocketAddr>,
    #[arg(long)]
    scan_control_bind: Option<std::net::SocketAddr>,
    /// Shared secret for scan requests; the file's contents are never logged.
    #[arg(long)]
    token_file: Option<PathBuf>,
    /// Threads that fetch, verify and decode missing posting lists.
    #[arg(long, default_value_t = 64)]
    fetch_threads: usize,
    #[arg(long, default_value_t = 8 * 1024 * 1024 * 1024u64)]
    posting_cache_bytes: u64,
    /// Where corpus hash receipts live; defaults to beside the corpus.
    #[arg(long)]
    receipt_dir: Option<PathBuf>,
    /// Longest a scan request may take, on both the client and the server.
    #[arg(long, default_value_t = 120)]
    scan_timeout_seconds: u64,
}
fn transport_config(args: &Args) -> TransportConfig {
    TransportConfig {
        max_operation_time: Duration::from_secs(args.scan_timeout_seconds),
        ..TransportConfig::default()
    }
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
    match args.mode {
        Mode::Bench => return bench(&args),
        Mode::Serve => return serve(&args),
        Mode::Validate => {}
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

/// The reader that answers one configuration's queries.
enum Runner<'a> {
    Single {
        reader: &'a SavedReader,
        vectors: Option<&'a CanonicalVectorSource>,
    },
    Owners {
        query: &'a OwnerQuery,
        vectors: Option<&'a MappedVectorSource>,
    },
}
impl Runner<'_> {
    /// Exact float64 distance for recall ties, read outside the timed region.
    fn exact(&self, id: u64, q: &[f32]) -> Option<f64> {
        match self {
            Runner::Single { vectors, .. } => {
                let v = vectors.as_ref()?.get(id).ok()?;
                Some(rb::exact_squared_l2(q, &v))
            }
            Runner::Owners { vectors, .. } => {
                Some(rb::exact_squared_l2(q, vectors.as_ref()?.row(id).ok()?))
            }
        }
    }
}
/// Shared inputs for every query of one configuration.
struct BenchContext<'a> {
    runner: Runner<'a>,
    config: &'a BenchConfig,
    policy: ReadBeamPolicy,
    centroid_code_bytes: u64,
}
struct Answer {
    results: Vec<(u64, f32)>,
    seconds: f64,
    work: Value,
    bytes: Value,
    owners: Value,
}
/// `alone` marks a query with nothing else in flight; only then do counters
/// shared by every query, such as transport bytes and remote CPU, belong to it.
fn answer(cx: &BenchContext, q: &BenchQuery, alone: bool) -> Result<Answer, String> {
    let k = cx.config.k;
    let rerank = cx.config.rerank;
    match &cx.runner {
        Runner::Single { reader, vectors } => {
            rb::take_touched_vectors();
            let recording = vectors.map(|v| rb::RecordingVectors(v));
            let t = Instant::now();
            let (results, work) = reader.query(
                &q.vector,
                k,
                &cx.policy,
                rerank,
                recording.as_ref().map(|r| r as &dyn VectorSource),
            )?;
            let seconds = t.elapsed().as_secs_f64();
            let touched = rb::take_touched_vectors();
            let (row_bytes, pages) = match vectors {
                Some(v) => {
                    let d = v.descriptor();
                    let row = d.dimensions as u64 * 4;
                    let pages =
                        rb::distinct_pages(&touched, d.first_id, d.offset_bytes, d.stride_bytes, row);
                    (row, pages)
                }
                None => (0, 0),
            };
            let page_bytes = pages * rb::PAGE_BYTES;
            Ok(Answer {
                results,
                seconds,
                bytes: json!({
                    "posting_bytes_fetched": work.posting_object_bytes,
                    "posting_fetches": work.leaves,
                    "navigation_codes_read": work.navigation.distances * cx.centroid_code_bytes,
                    "postings_scanned": work.posting_code_bytes + work.vectors as u64 * 8,
                    "full_vector_rows_read": touched.len() as u64 * row_bytes,
                    "full_vector_pages": page_bytes,
                    "cold_equivalent": work.posting_object_bytes + page_bytes,
                }),
                work: json!(work),
                owners: Value::Null,
            })
        }
        Runner::Owners { query, .. } => {
            let t = Instant::now();
            let (results, record) = query.query(&q.vector, k, &cx.policy, rerank)?;
            let seconds = t.elapsed().as_secs_f64();
            let total = &record.total;
            let remote: Vec<_> = record.parts.iter().filter(|p| p.remote).collect();
            let machine_bytes: Option<u64> = alone.then(|| {
                remote
                    .iter()
                    .map(|p| p.transport_bytes_sent + p.transport_bytes_received)
                    .sum()
            });
            let remote_request = remote.iter().map(|p| p.request_ns).max();
            let remote_owner = remote.iter().map(|p| p.timing.total).max();
            Ok(Answer {
                results,
                seconds,
                bytes: json!({
                    "posting_bytes_fetched": total.posting_bytes_fetched,
                    "posting_fetches": total.posting_fetches,
                    "navigation_codes_read": record.navigation.distances * cx.centroid_code_bytes,
                    "postings_scanned": total.posting_bytes_scanned,
                    "full_vector_rows_read": total.rerank_row_bytes,
                    "full_vector_pages": total.rerank_page_bytes,
                    "cold_equivalent": total.posting_object_bytes + total.rerank_page_bytes,
                    "machine_to_machine": machine_bytes,
                }),
                work: json!({
                    "leaves": total.leaves,
                    "vectors": total.vectors,
                    "reranked": total.reranked,
                    "cache_hits": total.cache_hits,
                    "navigation": record.navigation,
                }),
                owners: json!({
                    "navigation_seconds": record.navigation_ns as f64 / 1e9,
                    "merge_seconds": record.merge_ns as f64 / 1e9,
                    "owners_contacted": record.parts.len(),
                    "remote_request_seconds": remote_request.map(|n| n as f64 / 1e9),
                    "remote_owner_seconds": remote_owner.map(|n| n as f64 / 1e9),
                    "remote_cpu_seconds": alone.then(|| remote.iter().map(|p| p.remote_cpu_seconds).sum::<f64>()),
                    "remote_heap_peak_bytes": alone.then(|| remote.iter().map(|p| p.remote_heap_peak_bytes).max()).flatten(),
                    "parts": record.parts,
                }),
            })
        }
    }
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
    if alone {
        rb::start_heap_peak();
    }
    let cpu = rb::process_cpu_seconds();
    let a = answer(cx, q, alone);
    let cpu = rb::process_cpu_seconds() - cpu;
    let heap_peak = alone.then(rb::finish_heap_peak);
    let a = a?;
    let ids: Vec<u64> = a.results.iter().map(|(id, _)| *id).collect();
    let recall = rb::tie_aware_recall(&ids, q, cx.config.k, &mut |id| cx.runner.exact(id, &q.vector))?;
    Ok(json!({
        "config": cx.config.name, "pass": pass, "query_index": index, "query_id": q.id,
        "seconds": a.seconds,
        "cpu_seconds": if alone { json!(cpu) } else { Value::Null },
        "recall": recall, "recall_fraction": recall.fraction(),
        "ids": ids, "distances": a.results.iter().map(|(_, d)| *d).collect::<Vec<_>>(),
        "work": a.work,
        "bytes": a.bytes,
        "owners": a.owners,
        "heap_peak_bytes": heap_peak,
    }))
}
const BYTE_FIELDS: [&str; 8] = [
    "posting_bytes_fetched",
    "posting_fetches",
    "navigation_codes_read",
    "postings_scanned",
    "full_vector_rows_read",
    "full_vector_pages",
    "cold_equivalent",
    "machine_to_machine",
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
        "cache_hits": stat(&["work", "cache_hits"]),
        "navigation_distances": stat(&["work", "navigation", "distances"]),
        "owner_requests": stat(&["work", "navigation", "owner_requests"]),
        "navigation_ms": ms(&["owners", "navigation_seconds"]),
        "merge_ms": ms(&["owners", "merge_seconds"]),
        "owners_contacted": stat(&["owners", "owners_contacted"]),
        "remote_request_ms": ms(&["owners", "remote_request_seconds"]),
        "remote_owner_ms": ms(&["owners", "remote_owner_seconds"]),
        "remote_cpu_ms": ms(&["owners", "remote_cpu_seconds"]),
        "remote_heap_peak_bytes": stat(&["owners", "remote_heap_peak_bytes"]),
        "network_received_bytes_all_traffic": network,
        "rss_bytes_after": mem.rss,
        "rss_peak_bytes": mem.rss_peak,
    })
}
fn read_token(args: &Args) -> Result<String, Box<dyn std::error::Error>> {
    let path = args.token_file.as_ref().ok_or("--token-file is required")?;
    let token = fs::read_to_string(path)?.trim().to_string();
    if token.is_empty() {
        return Err("empty token file".into());
    }
    Ok(token)
}
/// Parse OWNER@DATA_ADDR/CONTROL_ADDR.
fn parse_remote(spec: &str) -> Result<(u64, std::net::SocketAddr, std::net::SocketAddr), String> {
    let (owner, addrs) = spec.split_once('@').ok_or("remote needs OWNER@DATA/CONTROL")?;
    let (data, control) = addrs.split_once('/').ok_or("remote needs OWNER@DATA/CONTROL")?;
    Ok((
        owner.parse().map_err(|_| "bad remote owner")?,
        data.parse().map_err(|_| "bad remote data address")?,
        control.parse().map_err(|_| "bad remote control address")?,
    ))
}
fn open_mapped(
    args: &Args,
    reader: &SavedReader,
) -> Result<(Option<Arc<MappedVectorSource>>, Value), Box<dyn std::error::Error>> {
    if args.no_vectors {
        return Ok((None, Value::Null));
    }
    let descriptor = reader.vector_descriptor_at(args.vector_path.as_deref())?;
    let (source, report) = MappedVectorSource::open(descriptor, args.receipt_dir.as_deref())?;
    Ok((Some(Arc::new(source)), json!(report)))
}
/// Run every query once with `threads` plain threads pulling from a shared
/// index. Plain threads matter: a rayon worker that blocks on another pool
/// runs other queued queries meanwhile, so in-flight requests would exceed the
/// thread count and the scan server's limit.
fn cold_pass(cx: &BenchContext, queries: &[BenchQuery], threads: usize) -> Result<Vec<Value>, String> {
    let next = std::sync::atomic::AtomicUsize::new(0);
    let alone = threads == 1;
    let mut out: Vec<Option<Value>> = vec![None; queries.len()];
    let results = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads.min(queries.len()))
            .map(|_| {
                scope.spawn(|| -> Result<Vec<(usize, Value)>, String> {
                    let mut done = vec![];
                    loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some(q) = queries.get(i) else { return Ok(done) };
                        done.push((i, run_query(cx, i, q, "cold", alone)?));
                    }
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|w| w.join().map_err(|_| "cold worker panicked".to_string())?)
            .collect::<Result<Vec<_>, String>>()
    })?;
    for (i, v) in results.into_iter().flatten() {
        out[i] = Some(v);
    }
    out.into_iter().map(|v| v.ok_or_else(|| "cold pass skipped a query".to_string())).collect()
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
    let reader = Arc::new(reader);
    let routing_open_seconds = begin.elapsed().as_secs_f64();
    let t = Instant::now();
    let mut single_vectors = None;
    let mut mapped = None;
    let mut vector_open = Value::Null;
    let mut owner_query = None;
    match args.reader {
        ReaderKind::Single if !args.no_vectors => {
            single_vectors = Some(
                reader
                    .open_vector_source_at(args.vector_path.as_deref())?
                    .preload_with_limit(args.vector_cache_bytes)?,
            );
        }
        ReaderKind::Single => {}
        ReaderKind::Owners => {
            let (vectors, report) = open_mapped(args, &reader)?;
            vector_open = report;
            let mut remotes = vec![];
            for spec in &args.remote {
                let (owner, data, control) = parse_remote(spec)?;
                let client = RpcClient::new(data, control, read_token(args)?, transport_config(args))
                    .map_err(|e| format!("{e:?}"))?;
                remotes.push(RemoteOwner { owner, client });
            }
            let all: BTreeSet<u64> = owner_scan::leaves_by_owner(&reader).into_keys().collect();
            let local_owners: BTreeSet<u64> = if args.local_owners.is_empty() && remotes.is_empty() {
                all
            } else {
                args.local_owners.iter().copied().collect()
            };
            let engine = ScanEngine::new(
                reader.clone(),
                &local_owners,
                vectors.clone(),
                args.fetch_threads,
                args.posting_cache_bytes,
            )?;
            owner_query = Some(OwnerQuery {
                reader: reader.clone(),
                local: Arc::new(engine),
                local_owners,
                remotes,
                deadline: Duration::from_secs(args.scan_timeout_seconds),
            });
            owner_query.as_ref().unwrap().check_owners()?;
            if !owner_query.as_ref().unwrap().remotes.is_empty() && args.cold_threads > 64 {
                return Err("the scan server accepts at most 64 requests at once; use --cold-threads 64 or fewer".into());
            }
            mapped = vectors;
        }
    }
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
    let mut summaries = vec![];
    for config in &configs {
        let runner = match &owner_query {
            Some(query) => Runner::Owners {
                query,
                vectors: mapped.as_deref(),
            },
            None => Runner::Single {
                reader: &reader,
                vectors: single_vectors.as_ref(),
            },
        };
        let cx = BenchContext {
            runner,
            config,
            policy: ReadBeamPolicy::uniform(config.tau, config.beam_min, config.beam_max),
            centroid_code_bytes: reader.centroid_code_bytes() as u64,
        };
        for &pass in &passes {
            let network = rb::network_received_bytes();
            let t = Instant::now();
            let results: Vec<Value> = if pass == "cold" {
                cold_pass(&cx, &queries, args.cold_threads)?
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
        "reader": match args.reader { ReaderKind::Single => "single", ReaderKind::Owners => "owners" },
        "generation": descriptor,
        "rows": args.expected_rows,
        "queries": queries.len(),
        "passes": passes,
        "cold_threads": args.cold_threads,
        "local_owners": owner_query.as_ref().map(|q| q.local_owners.clone()),
        "remote_owners": owner_query.as_ref().map(|q| q.remotes.iter().map(|r| r.owner).collect::<Vec<_>>()),
        "leaves_by_owner": owner_scan::leaves_by_owner(&reader),
        "posting_cache_bytes_after": owner_query.as_ref().map(|q| q.local.cached_bytes()),
        "corpus_resident_pages_after": mapped.as_ref().and_then(|m| m.resident_pages().ok()),
        // Today's single reader fetches every visited posting object on every
        // query, from S3 for a durable store or from local files otherwise.
        "posting_store": if args.storage_config.is_some() { "durable" } else { "local" },
        "routing_open_seconds": routing_open_seconds,
        "vector_open_seconds": vector_open_seconds,
        "vector_open": vector_open,
        "open_rss_bytes": open_rss,
        "elapsed_seconds": begin.elapsed().as_secs_f64(),
        "results": summaries,
    });
    let mut file = File::create(args.output.join("summary.json"))?;
    file.write_all(&serde_json::to_vec_pretty(&report)?)?;
    file.sync_all()?;
    Ok(())
}
/// Answer scan requests for one owner until the process is stopped.
fn serve(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let [owner] = args.local_owners.as_slice() else {
        return Err("serve needs exactly one --local-owners".into());
    };
    let (Some(data), Some(control)) = (args.scan_data_bind, args.scan_control_bind) else {
        return Err("serve needs --scan-data-bind and --scan-control-bind".into());
    };
    let token = read_token(args)?;
    let begin = Instant::now();
    let (descriptor, reader) = open_reader(args)?;
    let reader = Arc::new(reader);
    let (vectors, vector_open) = open_mapped(args, &reader)?;
    let engine = Arc::new(ScanEngine::new(
        reader.clone(),
        &[*owner].into(),
        vectors,
        args.fetch_threads,
        args.posting_cache_bytes,
    )?);
    let handler = engine.clone();
    let server = RpcServer::bind(
        data,
        control,
        token,
        transport_config(args),
        Arc::new(move |request: ScanRequest| handler.handle(request)),
    )
    .map_err(|e| format!("{e:?}"))?;
    fs::create_dir_all(&args.output)?;
    let ready = json!({
        "status": "serving",
        "owner": owner,
        "generation": descriptor,
        "data_addr": server.data_addr().to_string(),
        "control_addr": server.control_addr().to_string(),
        "leaves_by_owner": owner_scan::leaves_by_owner(&reader),
        "open_seconds": begin.elapsed().as_secs_f64(),
        "vector_open": vector_open,
        "pid": std::process::id(),
    });
    fs::write(args.output.join("ready.json"), serde_json::to_vec_pretty(&ready)?)?;
    println!("{}", serde_json::to_string(&ready)?);
    loop {
        std::thread::sleep(Duration::from_secs(60));
        let status = json!({
            "posting_cache_bytes": engine.cached_bytes(),
            "transport": server.counters(),
            "rss_bytes": mem_probe::read_self().rss,
        });
        if let Err(e) = fs::write(args.output.join("status.json"), serde_json::to_vec_pretty(&status)?) {
            eprintln!("status write failed: {e}");
        }
    }
}
