//! Fresh-process validation consumes only saved files and an independently
//! verified immutable vector corpus; it starts no writer service. Bench mode
//! runs frozen queries against exact references and records recall, latency,
//! CPU, bytes and heap per query for reader development. Serve mode answers
//! leaf scans for one owner, so a bench process on another machine can read a
//! replicated routing tree and spread the scans across owners.
#![recursion_limit = "256"]
#![allow(unexpected_cfgs, dead_code, unused_imports, clippy::all)]
mod hierarchical_index;
use clap::Parser;
use hierarchical_index::{
    common::ReadBeamPolicy,
    distributed::{
        engine::VectorSource,
        generation::{GenerationDescriptor, GenerationStore},
        generation_backend::DurableStoreConfig,
        mapped_vectors::MappedVectorSource,
        owner_scan::{
            self, FullVectors, OwnerQuery, Placement, RemoteOwner, ScanEngine, ScanRequest,
        },
        reader_bench::{self as rb, BenchConfig, BenchQuery},
        saved_reader::{CanonicalVectorSource, SavedReader},
        transport::{RpcClient, RpcServer, TransportConfig},
        vector_store::{VectorStoreLocation, VectorStoreReader},
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
enum FullVectorSource {
    Corpus,
    Store,
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
    /// Bench: comma-separated passes per configuration, from `cold`, `warm`
    /// and `load`. A load pass is a cold pass over the queries repeated
    /// --load-repeats times, for throughput over a longer window.
    #[arg(long, default_value = "cold,warm")]
    passes: String,
    #[arg(long, default_value_t = 1)]
    load_repeats: usize,
    /// Bench: threads for the cold pass, which fills caches in parallel.
    #[arg(long, default_value_t = 32)]
    cold_threads: usize,
    /// Bench: before each pass, write `ready-<step>` in this directory and
    /// wait for `go-<step>`, which holds the Unix time at which to start, so
    /// benches on several machines start each pass together. Steps count
    /// passes across configurations from 0.
    #[arg(long)]
    pass_barrier: Option<PathBuf>,
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
    /// Split the index into this many parts when it opens, by subtree, and
    /// route each leaf to its part instead of its saved owner. --local-owners
    /// and --remote then name parts 1 to N. 0 keeps the saved owners. Every
    /// process serving one index must use the same value.
    #[arg(long, default_value_t = 0)]
    parts: u64,
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
    /// Threads that score one scan request's leaves in parallel.
    #[arg(long, default_value_t = 16)]
    scan_threads: usize,
    #[arg(long, default_value_t = 8 * 1024 * 1024 * 1024u64)]
    posting_cache_bytes: u64,
    /// Where corpus hash receipts live; defaults to beside the corpus.
    #[arg(long)]
    receipt_dir: Option<PathBuf>,
    /// Bench: where the true-neighbor-to-leaf map lives. Loaded when present,
    /// otherwise built once by scanning every posting list, then saved.
    #[arg(long)]
    gt_leaf_map: Option<PathBuf>,
    /// Bench: skip the per-level and optimal recall diagnostics.
    #[arg(long)]
    no_diagnostics: bool,
    /// Bench: add each query's chosen leaf IDs to its record, from the
    /// diagnostic walk, for offline fan-out analysis.
    #[arg(long)]
    record_leaves: bool,
    /// Bench: write the routing tree shape to this JSONL file after open.
    #[arg(long)]
    dump_tree: Option<PathBuf>,
    /// Bench: also rank every leaf by exact distance to its float32 centroid
    /// and report how many leaves cover each query's true neighbors. Needs a
    /// v1 generation, whose routing nodes carry exact centroids.
    #[arg(long)]
    exact_leaf_ranking: bool,
    /// Where owner scans read full vectors for rerank: a local corpus copy, or
    /// each owner's vector blockfile saved with the generation.
    #[arg(long, value_enum, default_value_t = FullVectorSource::Corpus)]
    full_vectors: FullVectorSource,
    /// Block-cache budget for each owner's vector store reader.
    #[arg(long, default_value_t = 32 * 1024 * 1024 * 1024u64)]
    vector_store_cache_bytes: u64,
    /// Longest a scan request may take, on both the client and the server.
    #[arg(long, default_value_t = 120)]
    scan_timeout_seconds: u64,
    /// Persistent request threads per remote owner on the query machine; at
    /// least --cold-threads, or remote requests queue for a worker.
    #[arg(long, default_value_t = 64)]
    remote_workers: usize,
    /// Scan requests a server runs at once. A server answering several query
    /// machines needs their combined --cold-threads.
    #[arg(long, default_value_t = 64)]
    scan_handlers: usize,
}
fn transport_config(args: &Args) -> TransportConfig {
    TransportConfig {
        max_operation_time: Duration::from_secs(args.scan_timeout_seconds),
        max_data_handlers: args.scan_handlers,
        ..TransportConfig::default()
    }
}
fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(f64::NAN)
}
/// Announce that this bench is ready for `step` and sleep until the start
/// time the driver writes for it.
fn pass_barrier(dir: &std::path::Path, step: usize) -> Result<(), String> {
    fs::write(dir.join(format!("ready-{step}")), b"").map_err(|e| format!("pass barrier: {e}"))?;
    let go = dir.join(format!("go-{step}"));
    let start: f64 = loop {
        match fs::read_to_string(&go) {
            Ok(text) if !text.trim().is_empty() => {
                break text
                    .trim()
                    .parse()
                    .map_err(|e| format!("pass barrier start time: {e}"))?
            }
            _ => std::thread::sleep(Duration::from_millis(5)),
        }
    };
    let wait = start - unix_now();
    if wait > 0.0 {
        std::thread::sleep(Duration::from_secs_f64(wait));
    }
    Ok(())
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
/// Compare every vector in each owner's saved vector store with the
/// independently hashed corpus, bit for bit, in posting order. Threads take
/// batches of 64 leaves in turn (`SPANN_VECTOR_VERIFY_THREADS`, default 32).
fn verify_vector_stores(
    args: &Args,
    reader: &SavedReader,
    corpus: &CanonicalVectorSource,
) -> Result<Value, Box<dyn std::error::Error>> {
    let refs = reader.owner_vector_stores();
    if refs.is_empty() {
        return Ok(Value::Null);
    }
    let threads = std::env::var("SPANN_VECTOR_VERIFY_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| (1..=256).contains(n))
        .unwrap_or(32);
    let location = vector_store_location(args)?;
    let mut verified = serde_json::Map::new();
    for (owner, r) in &refs {
        let store = VectorStoreReader::open(&location, r, 1024 * 1024 * 1024)?;
        let mut leaves: Vec<u128> = reader
            .leaves()
            .filter(|l| reader.node_owner(*l) == Some(*owner))
            .collect();
        leaves.sort_unstable();
        let batches: Vec<&[u128]> = leaves.chunks(64).collect();
        let next = std::sync::atomic::AtomicUsize::new(0);
        let verify_batch = |batch: &[u128]| -> Result<u64, String> {
            let mut keys = vec![];
            let mut ids = vec![];
            for leaf in batch {
                let p = reader.posting_list(*leaf).map_err(|e| e.to_string())?;
                for (ordinal, id) in p.ids.iter().enumerate() {
                    keys.push((*leaf, ordinal as u32));
                    ids.push(*id);
                }
            }
            for (id, stored) in ids.iter().zip(store.get_many(&keys)?) {
                let expected = corpus.get(*id).map_err(|e| e.to_string())?;
                if stored.len() != expected.len()
                    || stored
                        .iter()
                        .zip(expected.iter())
                        .any(|(a, b)| a.to_bits() != b.to_bits())
                {
                    return Err(format!(
                        "owner {owner} vector store differs from the corpus at vector {id}"
                    ));
                }
            }
            Ok(ids.len() as u64)
        };
        let counts: Vec<Result<u64, String>> = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..threads.min(batches.len()).max(1))
                .map(|_| {
                    scope.spawn(|| {
                        let mut rows = 0u64;
                        loop {
                            let b = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let Some(batch) = batches.get(b) else {
                                return Ok(rows);
                            };
                            rows += verify_batch(batch)?;
                        }
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|w| {
                    w.join()
                        .unwrap_or_else(|_| Err("vector store verifier panicked".into()))
                })
                .collect()
        });
        let mut rows = 0u64;
        for count in counts {
            rows += count?;
        }
        if rows != r.rows {
            return Err(format!(
                "owner {owner} vector store holds {} rows, postings hold {rows}",
                r.rows
            )
            .into());
        }
        verified.insert(owner.to_string(), json!({"rows": rows, "store": r}));
    }
    Ok(Value::Object(verified))
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
    // A zero allowance validates from the corpus file on demand instead of a
    // resident copy, for corpora larger than the machine's spare memory.
    let source = reader.open_vector_source_at(args.vector_path.as_deref())?;
    let vectors = if args.vector_cache_bytes == 0 {
        source
    } else {
        source.preload_with_limit(args.vector_cache_bytes)?
    };
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
    let t = Instant::now();
    let vector_stores = verify_vector_stores(&args, &reader, &vectors)?;
    let vector_store_seconds = t.elapsed().as_secs_f64();
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
    let report = json!({"status":"durable_reader_validated","generation":descriptor,"rows":args.expected_rows,"nodes":nodes,"validated_posting_codes":validated_vectors,"vector_sha256_verified":true,"exact_membership":true,"all_centroid_codes_recomputed":true,"all_posting_codes_recomputed":true,"vector_validation_seconds":vector_validation,"membership_and_code_validation_seconds":membership_and_codes,"vector_stores_verified":vector_stores,"vector_store_validation_seconds":vector_store_seconds,"elapsed_seconds":begin.elapsed().as_secs_f64(),"queries":queries,"query_cohort":"training-vector smoke queries, not held-out recall evidence","peak_rss":mem_probe::read_self().rss_peak,"writer_objects_required":false});
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
    /// Reader and true-neighbor leaves for walk diagnostics, when enabled.
    walk: Option<(&'a SavedReader, &'a std::collections::HashMap<u64, u128>)>,
    config: &'a BenchConfig,
    policy: ReadBeamPolicy,
    centroid_code_bytes: u64,
    record_leaves: bool,
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
                    let pages = rb::distinct_pages(
                        &touched,
                        d.first_id,
                        d.offset_bytes,
                        d.stride_bytes,
                        row,
                    );
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
            // Where the slowest remote request spent its time, in seconds.
            let remote_breakdown = remote.iter().max_by_key(|p| p.request_ns).map(|p| {
                let s = |n: u64| n as f64 / 1e9;
                let server_wall = p.server.before_handler_ns + p.server_handler_ns;
                json!({
                    "overhead": s(p.request_ns.saturating_sub(p.timing.total)),
                    "client_spawn": s(p.client_spawn_ns),
                    "client_build": s(p.client_build_ns),
                    "call_connect": s(p.call.connect_ns),
                    "call_encode": s(p.call.encode_ns),
                    "call_write": s(p.call.write_ns),
                    "call_first_byte_wait": s(p.call.first_byte_wait_ns),
                    "call_read": s(p.call.read_ns),
                    "call_decode": s(p.call.decode_ns),
                    "client_results": s(p.client_results_ns),
                    "server_read": s(p.server.read_ns),
                    "server_decode_queue": s(p.server.decode_queue_ns),
                    "server_decode": s(p.server.decode_ns),
                    "server_dispatch": s(p.server.dispatch_ns),
                    "server_worker_wake": s(p.server.worker_wake_ns),
                    "server_handler": s(p.server_handler_ns),
                    "server_handler_outside_scan": s(p.server_handler_ns.saturating_sub(p.timing.total)),
                    // First reply byte wait not spent between the owner's first
                    // request byte and its handler exit: the network both ways,
                    // reply encoding and sending, and client wakeup.
                    "unattributed": s(p.call.first_byte_wait_ns.saturating_sub(server_wall.saturating_sub(p.server.read_ns))),
                })
            });
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
                    "vector_store_bytes_loaded": total.rerank_bytes_loaded,
                    "vector_store_blocks_touched": total.rerank_blocks_touched,
                    "cold_equivalent": total.posting_object_bytes + total.rerank_page_bytes + total.rerank_cold_bytes_estimate,
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
                    "queries_in_flight": record.queries_in_flight,
                    "navigation_parallel": record.navigation_parallel,
                    "merge_seconds": record.merge_ns as f64 / 1e9,
                    "owners_contacted": record.parts.len(),
                    "remote_request_seconds": remote_request.map(|n| n as f64 / 1e9),
                    "remote_owner_seconds": remote_owner.map(|n| n as f64 / 1e9),
                    "remote_breakdown_seconds": remote_breakdown,
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
    // Diagnostics re-walk the tree outside the timed region; the traced walk
    // uses the same scoring and selection as the measured query.
    let mut leaf_ids = Value::Null;
    let walk = match cx.walk {
        Some((reader, gt_leaves)) => {
            let (leaves, frontiers) = reader.navigate_traced(&q.vector, &cx.policy)?;
            let leaves: Vec<u128> = leaves.iter().map(|(n, _)| n.id).collect();
            if cx.record_leaves {
                leaf_ids = json!(leaves
                    .iter()
                    .map(|l| format!("{l:032x}"))
                    .collect::<Vec<_>>());
            }
            let truth: Vec<(u64, u128)> = q
                .truth_ids
                .iter()
                .take(cx.config.k)
                .filter_map(|id| gt_leaves.get(id).map(|leaf| (*id, *leaf)))
                .collect();
            json!(rb::walk_diagnostics(reader, &frontiers, &leaves, &truth))
        }
        None => Value::Null,
    };
    let recall = rb::tie_aware_recall(&ids, q, cx.config.k, &mut |id| {
        cx.runner.exact(id, &q.vector)
    })?;
    Ok(json!({
        "config": cx.config.name, "pass": pass, "query_index": index, "query_id": q.id,
        "seconds": a.seconds,
        "finished_unix": unix_now(),
        "cpu_seconds": if alone { json!(cpu) } else { Value::Null },
        "recall": recall, "recall_fraction": recall.fraction(),
        "ids": ids, "distances": a.results.iter().map(|(_, d)| *d).collect::<Vec<_>>(),
        "work": a.work,
        "bytes": a.bytes,
        "owners": a.owners,
        "heap_peak_bytes": heap_peak,
        "walk": walk,
        "leaf_ids": leaf_ids,
    }))
}
const BYTE_FIELDS: [&str; 10] = [
    "vector_store_bytes_loaded",
    "vector_store_blocks_touched",
    "posting_bytes_fetched",
    "posting_fetches",
    "navigation_codes_read",
    "postings_scanned",
    "full_vector_rows_read",
    "full_vector_pages",
    "cold_equivalent",
    "machine_to_machine",
];
/// Means of the per-query walk diagnostics, level by level.
fn walk_summary(records: &[Value]) -> Value {
    let walks: Vec<&Value> = records
        .iter()
        .map(|r| &r["walk"])
        .filter(|w| !w.is_null())
        .collect();
    if walks.is_empty() {
        return Value::Null;
    }
    let mean = |f: &dyn Fn(&Value) -> Option<f64>| {
        let v: Vec<f64> = walks.iter().filter_map(|w| f(w)).collect();
        (!v.is_empty()).then(|| v.iter().sum::<f64>() / v.len() as f64)
    };
    let depth = walks
        .iter()
        .map(|w| w["levels"].as_array().map_or(0, |l| l.len()))
        .max()
        .unwrap_or(0);
    let levels: Vec<Value> = (0..depth)
        .map(|i| {
            let at = |key: &'static str| mean(&|w: &Value| w["levels"].get(i)?.get(key)?.as_f64());
            json!({"level": i + 1, "scored": at("scored"), "kept": at("kept"), "reach": at("reach")})
        })
        .collect();
    json!({
        "levels": levels,
        "leaf_reach": mean(&|w: &Value| w["leaf_reach"].as_f64()),
        "optimal_recall": mean(&|w: &Value| w["optimal_recall"].as_f64()),
    })
}
/// Rank every leaf by exact centroid distance for each query and summarize,
/// for k = 10 and 100: the mean share of true neighbors whose leaf ranks
/// inside each beam, and per query the number of leaves that covers 95% and
/// 100% of its true neighbors.
fn exact_ranking_report(
    reader: &SavedReader,
    queries: &[BenchQuery],
    gt_leaves: &std::collections::HashMap<u64, u128>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let leaves: Vec<u128> = reader.leaves().collect();
    let centroids: Vec<(u128, &[f32])> = leaves
        .iter()
        .map(|l| reader.node_centroid(*l).map(|c| (*l, c)))
        .collect::<Option<_>>()
        .ok_or("exact leaf ranking needs a v1 generation with exact leaf centroids")?;
    const BEAMS: [usize; 10] = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192];
    let t = Instant::now();
    let ranks: Vec<Vec<usize>> = queries
        .iter()
        .map(|q| {
            let truth: Vec<(u64, u128)> = q
                .truth_ids
                .iter()
                .take(100)
                .filter_map(|id| gt_leaves.get(id).map(|leaf| (*id, *leaf)))
                .collect();
            rb::exact_leaf_ranks(&centroids, &q.vector, &truth)
        })
        .collect();
    let mut by_k = serde_json::Map::new();
    for k in [10usize, 100] {
        let coverage: Vec<Value> = BEAMS
            .iter()
            .map(|beam| {
                let share = ranks
                    .iter()
                    .map(|r| {
                        r.iter().take(k).filter(|rank| **rank < *beam).count() as f64 / k as f64
                    })
                    .sum::<f64>()
                    / ranks.len().max(1) as f64;
                json!({"beam": beam, "leaf_reach": share})
            })
            .collect();
        let needed = |fraction: f64| -> Value {
            let mut v: Vec<f64> = ranks
                .iter()
                .map(|r| {
                    let mut r: Vec<usize> = r.iter().take(k).copied().collect();
                    r.sort_unstable();
                    let i = ((fraction * k as f64).ceil() as usize).clamp(1, r.len().max(1)) - 1;
                    r.get(i).map_or(f64::NAN, |x| *x as f64 + 1.0)
                })
                .collect();
            v.sort_by(|a, b| a.total_cmp(b));
            let at = |q: f64| v[((q * v.len() as f64) as usize).min(v.len() - 1)];
            json!({"mean": v.iter().sum::<f64>() / v.len() as f64, "p50": at(0.5), "p90": at(0.9), "p99": at(0.99)})
        };
        by_k.insert(
            format!("k{k}"),
            json!({"coverage": coverage, "leaves_for_95pct": needed(0.95), "leaves_for_all": needed(1.0)}),
        );
    }
    Ok(
        json!({"leaves": leaves.len(), "queries": queries.len(), "seconds": t.elapsed().as_secs_f64(), "by_k": by_k}),
    )
}
/// Map every listed true neighbor of the loaded queries to its leaf, by
/// scanning each posting list once. Loaded from `path` when it exists.
fn gt_leaf_map(
    reader: &SavedReader,
    queries: &[BenchQuery],
    path: Option<&std::path::Path>,
) -> Result<std::collections::HashMap<u64, u128>, Box<dyn std::error::Error>> {
    let wanted: BTreeSet<u64> = queries
        .iter()
        .flat_map(|q| q.truth_ids.iter().copied())
        .collect();
    if let Some(path) = path.filter(|p| p.exists()) {
        let pairs: Vec<(u64, u128)> = serde_json::from_slice(&fs::read(path)?)?;
        let map: std::collections::HashMap<u64, u128> = pairs.into_iter().collect();
        if wanted.iter().all(|id| map.contains_key(id)) {
            return Ok(map);
        }
    }
    let leaves: Vec<u128> = reader.leaves().collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let found = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..32)
            .map(|_| {
                scope.spawn(|| -> Result<Vec<(u64, u128)>, String> {
                    let mut out = vec![];
                    loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some(leaf) = leaves.get(i) else {
                            return Ok(out);
                        };
                        for id in reader.posting_list(*leaf)?.ids {
                            if wanted.contains(&id) {
                                out.push((id, *leaf));
                            }
                        }
                    }
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|w| {
                w.join()
                    .map_err(|_| "leaf map worker panicked".to_string())?
            })
            .collect::<Result<Vec<_>, String>>()
    })?;
    let pairs: Vec<(u64, u128)> = found.into_iter().flatten().collect();
    if let Some(path) = path {
        fs::write(path, serde_json::to_vec(&pairs)?)?;
    }
    Ok(pairs.into_iter().collect())
}
fn summarize(records: &[Value], pass_seconds: f64, network: Option<u64>) -> Value {
    let field = |path: &[&str]| -> Vec<f64> {
        records
            .iter()
            .filter_map(|r| path.iter().try_fold(r, |v, key| v.get(key))?.as_f64())
            .filter(|x| x.is_finite())
            .collect()
    };
    let ms = |path: &[&str]| {
        json!(rb::stats(
            &field(path).iter().map(|x| x * 1e3).collect::<Vec<_>>()
        ))
    };
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
        "remote_breakdown_ms": remote_breakdown_summary(records),
        "remote_cpu_ms": ms(&["owners", "remote_cpu_seconds"]),
        "remote_heap_peak_bytes": stat(&["owners", "remote_heap_peak_bytes"]),
        "network_received_bytes_all_traffic": network,
        "rss_bytes_after": mem.rss,
        "rss_peak_bytes": mem.rss_peak,
        "walk": walk_summary(records),
    })
}
/// Latency statistics for each phase of the slowest remote request per query.
fn remote_breakdown_summary(records: &[Value]) -> Value {
    let Some(keys) = records
        .iter()
        .find_map(|r| {
            r.get("owners")?
                .get("remote_breakdown_seconds")?
                .as_object()
        })
        .map(|o| o.keys().cloned().collect::<Vec<_>>())
    else {
        return Value::Null;
    };
    let phases: serde_json::Map<_, _> = keys
        .into_iter()
        .map(|key| {
            let values: Vec<f64> = records
                .iter()
                .filter_map(|r| {
                    r.get("owners")?
                        .get("remote_breakdown_seconds")?
                        .get(&key)?
                        .as_f64()
                })
                .map(|x| x * 1e3)
                .collect();
            (key, json!(rb::stats(&values)))
        })
        .collect();
    Value::Object(phases)
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
    let (owner, addrs) = spec
        .split_once('@')
        .ok_or("remote needs OWNER@DATA/CONTROL")?;
    let (data, control) = addrs
        .split_once('/')
        .ok_or("remote needs OWNER@DATA/CONTROL")?;
    Ok((
        owner.parse().map_err(|_| "bad remote owner")?,
        data.parse().map_err(|_| "bad remote data address")?,
        control.parse().map_err(|_| "bad remote control address")?,
    ))
}
/// The vector-store location matching the generation store: S3 for a durable
/// store, otherwise the local store directory.
fn vector_store_location(args: &Args) -> Result<VectorStoreLocation, Box<dyn std::error::Error>> {
    Ok(match &args.storage_config {
        Some(path) => VectorStoreLocation::Durable(serde_json::from_slice::<DurableStoreConfig>(
            &fs::read(path)?,
        )?),
        None => VectorStoreLocation::Local(args.store.clone()),
    })
}
/// The leaf placement every process of this run uses: saved owners, or
/// --parts subtree parts.
fn placement(
    args: &Args,
    reader: &SavedReader,
) -> Result<Arc<Placement>, Box<dyn std::error::Error>> {
    Ok(Arc::new(if args.parts == 0 {
        Placement::saved(reader)
    } else {
        Placement::subtrees(reader, args.parts)?
    }))
}
/// Vector stores of the saved owners whose leaves this process scans. Under a
/// read-time placement, each store keeps only the blocks of those leaves.
fn open_vector_stores(
    args: &Args,
    reader: &SavedReader,
    placement: &Placement,
    parts: &BTreeSet<u64>,
) -> Result<std::collections::BTreeMap<u64, Arc<VectorStoreReader>>, Box<dyn std::error::Error>> {
    let location = vector_store_location(args)?;
    let refs = reader.owner_vector_stores();
    let leaves = placement.leaves_of(parts);
    let owners: BTreeSet<u64> = leaves
        .iter()
        .filter_map(|l| reader.node_owner(*l))
        .collect();
    let mut stores = std::collections::BTreeMap::new();
    for owner in owners {
        let r = refs
            .get(&owner)
            .ok_or_else(|| format!("generation has no vector store for owner {owner}"))?;
        let mut store = VectorStoreReader::open(&location, r, args.vector_store_cache_bytes)?;
        if placement.kind != "saved" {
            store.restrict_to_leaves(&leaves);
        }
        stores.insert(owner, Arc::new(store));
    }
    Ok(stores)
}
/// The rerank source for the parts this process scans.
fn full_vectors(
    args: &Args,
    reader: &SavedReader,
    placement: &Placement,
    parts: &BTreeSet<u64>,
    mapped: Option<Arc<MappedVectorSource>>,
) -> Result<Option<FullVectors>, Box<dyn std::error::Error>> {
    Ok(match args.full_vectors {
        FullVectorSource::Corpus => mapped.map(FullVectors::Mapped),
        FullVectorSource::Store => Some(FullVectors::Store(open_vector_stores(
            args, reader, placement, parts,
        )?)),
    })
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
/// `repeats` runs the query list that many times in a row.
fn cold_pass(
    cx: &BenchContext,
    queries: &[BenchQuery],
    threads: usize,
    pass: &str,
    repeats: usize,
) -> Result<Vec<Value>, String> {
    let next = std::sync::atomic::AtomicUsize::new(0);
    let alone = threads == 1;
    let total = queries.len() * repeats;
    let mut out: Vec<Option<Value>> = vec![None; total];
    let results = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads.min(total))
            .map(|_| {
                scope.spawn(|| -> Result<Vec<(usize, Value)>, String> {
                    let mut done = vec![];
                    loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if i >= total {
                            return Ok(done);
                        }
                        let q = &queries[i % queries.len()];
                        done.push((i, run_query(cx, i % queries.len(), q, pass, alone)?));
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
    out.into_iter()
        .map(|v| v.ok_or_else(|| "cold pass skipped a query".to_string()))
        .collect()
}
fn bench(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let (Some(queries_path), Some(truth_path), Some(config_path)) =
        (&args.queries, &args.groundtruth, &args.config)
    else {
        return Err("bench mode needs --queries, --groundtruth and --config".into());
    };
    let passes: Vec<&str> = args.passes.split(',').map(str::trim).collect();
    if passes
        .iter()
        .any(|p| !matches!(*p, "cold" | "warm" | "load"))
        || args.cold_threads == 0
        || args.load_repeats == 0
    {
        return Err(
            "passes must be cold, warm or load, with at least one cold thread and one load repeat"
                .into(),
        );
    }
    let configs = rb::load_configs(config_path)?;
    if args.no_vectors
        && args.full_vectors == FullVectorSource::Corpus
        && configs.iter().any(|c| c.rerank > 0)
    {
        return Err("rerank needs the corpus or --full-vectors store; drop --no-vectors".into());
    }
    let begin = Instant::now();
    let rss_before_open = mem_probe::read_self().rss;
    rb::start_heap_peak();
    let (descriptor, reader) = open_reader(args)?;
    let routing_open_heap =
        json!({"live_bytes": rb::heap_live(), "peak_bytes": rb::finish_heap_peak()});
    let routing_open_rss = mem_probe::read_self().rss;
    let reader = Arc::new(reader);
    let routing_open_seconds = begin.elapsed().as_secs_f64();
    let mut tree_counts = json!({});
    {
        let (mut leaves, mut internal) = (0usize, 0usize);
        let mut dump = match &args.dump_tree {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                Some(BufWriter::new(File::create(path)?))
            }
            None => None,
        };
        for (id, parent, leaf, owner) in reader.tree_shape() {
            if leaf {
                leaves += 1
            } else {
                internal += 1
            }
            if let Some(out) = dump.as_mut() {
                let posting_bytes = if leaf {
                    reader.posting_object_bytes(id)
                } else {
                    None
                };
                serde_json::to_writer(
                    &mut *out,
                    &json!({
                        "id": format!("{id:032x}"), "parent": parent.map(|p| format!("{p:032x}")),
                        "leaf": leaf, "owner": owner, "posting_bytes": posting_bytes,
                    }),
                )?;
                out.write_all(b"\n")?;
            }
        }
        if let Some(mut out) = dump {
            out.flush()?;
        }
        tree_counts = json!({"leaves": leaves, "internal": internal});
    }
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
                let client =
                    RpcClient::new(data, control, read_token(args)?, transport_config(args))
                        .map_err(|e| format!("{e:?}"))?;
                remotes.push(RemoteOwner::new(owner, client, args.remote_workers));
            }
            let placement = placement(args, &reader)?;
            let all: BTreeSet<u64> = placement.leaves_by_part().into_keys().collect();
            let local_owners: BTreeSet<u64> = if args.local_owners.is_empty() && remotes.is_empty()
            {
                all
            } else {
                args.local_owners.iter().copied().collect()
            };
            let rerank_source =
                full_vectors(args, &reader, &placement, &local_owners, vectors.clone())?;
            let engine = ScanEngine::with_placement(
                reader.clone(),
                placement.clone(),
                &local_owners,
                rerank_source,
                args.fetch_threads,
                args.scan_threads,
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
            if !owner_query.as_ref().unwrap().remotes.is_empty()
                && args.remote_workers < args.cold_threads
            {
                return Err("--remote-workers must be at least --cold-threads, or remote requests queue for a worker".into());
            }
            if !owner_query.as_ref().unwrap().remotes.is_empty()
                && args.cold_threads > args.scan_handlers
            {
                return Err("each scan server must accept --cold-threads requests at once; raise --scan-handlers on every server".into());
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
    let t = Instant::now();
    let gt_leaves = if args.no_diagnostics {
        None
    } else {
        Some(gt_leaf_map(&reader, &queries, args.gt_leaf_map.as_deref())?)
    };
    let gt_leaf_map_seconds = t.elapsed().as_secs_f64();
    let exact_ranking = match (&gt_leaves, args.exact_leaf_ranking) {
        (Some(map), true) => exact_ranking_report(&reader, &queries, map)?,
        (None, true) => return Err("--exact-leaf-ranking needs the diagnostics' leaf map".into()),
        _ => Value::Null,
    };
    let mut records = BufWriter::new(File::create(args.output.join("queries.jsonl"))?);
    let mut summaries = vec![];
    let mut step = 0;
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
            walk: gt_leaves.as_ref().map(|m| (reader.as_ref(), m)),
            config,
            policy: ReadBeamPolicy::with_level_overrides(
                config.tau,
                config.beam_min,
                config.beam_max,
                config.level_tau.clone(),
                vec![],
                vec![],
            )
            .with_level_beam_maxes(config.level_beam_max.clone())
            .with_level_beam_mins(config.level_beam_min.clone()),
            centroid_code_bytes: reader.centroid_code_bytes() as u64,
            record_leaves: args.record_leaves,
        };
        for &pass in &passes {
            if let Some(dir) = &args.pass_barrier {
                pass_barrier(dir, step)?;
            }
            step += 1;
            let network = rb::network_received_bytes();
            let usage = rb::ProcessUsage::now();
            let started_unix = unix_now();
            let t = Instant::now();
            let results: Vec<Value> = if pass == "cold" {
                cold_pass(&cx, &queries, args.cold_threads, pass, 1)?
            } else if pass == "load" {
                cold_pass(&cx, &queries, args.cold_threads, pass, args.load_repeats)?
            } else {
                queries
                    .iter()
                    .enumerate()
                    .map(|(i, q)| run_query(&cx, i, q, pass, true))
                    .collect::<Result<_, _>>()?
            };
            let seconds = t.elapsed().as_secs_f64();
            let usage = rb::ProcessUsage::now()
                .zip(usage)
                .map(|(after, before)| after.since(&before, seconds));
            let network = rb::network_received_bytes()
                .zip(network)
                .map(|(after, before)| after.saturating_sub(before));
            for r in &results {
                serde_json::to_writer(&mut records, r)?;
                records.write_all(b"\n")?;
            }
            let mut summary = summarize(&results, seconds, network);
            // The whole process over the pass, so 32-thread passes, whose
            // per-query CPU is not recorded, still report the cores they used.
            summary["process_cpu"] = usage.unwrap_or(Value::Null);
            summary["started_unix"] = json!(started_unix);
            summary["finished_unix"] = json!(started_unix + seconds);
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
        "placement": owner_query.as_ref().map(|q| { let p = q.local.placement(); json!({"kind": p.kind, "parts": p.parts(), "leaves_by_part": p.leaves_by_part()}) }),
        "posting_cache_bytes_after": owner_query.as_ref().map(|q| q.local.cached_bytes()),
        "corpus_resident_pages_after": mapped.as_ref().and_then(|m| m.resident_pages().ok()),
        // Today's single reader fetches every visited posting object on every
        // query, from S3 for a durable store or from local files otherwise.
        "posting_store": if args.storage_config.is_some() { "durable" } else { "local" },
        "routing_open_seconds": routing_open_seconds,
        "rss_before_open_bytes": rss_before_open,
        "routing_open_rss_bytes": routing_open_rss,
        "routing_open_heap": routing_open_heap,
        "tree_nodes": tree_counts,
        "gt_leaf_map_seconds": gt_leaf_map_seconds,
        "exact_leaf_ranking": exact_ranking,
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
    if args.local_owners.is_empty() {
        return Err("serve needs --local-owners".into());
    }
    let owners: BTreeSet<u64> = args.local_owners.iter().copied().collect();
    let (Some(data), Some(control)) = (args.scan_data_bind, args.scan_control_bind) else {
        return Err("serve needs --scan-data-bind and --scan-control-bind".into());
    };
    let token = read_token(args)?;
    let begin = Instant::now();
    let (descriptor, reader) = open_reader(args)?;
    let reader = Arc::new(reader);
    let (vectors, vector_open) = if args.full_vectors == FullVectorSource::Store {
        (None, Value::Null)
    } else {
        open_mapped(args, &reader)?
    };
    let placement = placement(args, &reader)?;
    let rerank_source = full_vectors(args, &reader, &placement, &owners, vectors)?;
    let engine = Arc::new(ScanEngine::with_placement(
        reader.clone(),
        placement.clone(),
        &owners,
        rerank_source,
        args.fetch_threads,
        args.scan_threads,
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
        "owners": owners,
        "placement": {"kind": placement.kind, "parts": placement.parts(), "leaves_by_part": placement.leaves_by_part()},
        "generation": descriptor,
        "data_addr": server.data_addr().to_string(),
        "control_addr": server.control_addr().to_string(),
        "leaves_by_owner": owner_scan::leaves_by_owner(&reader),
        "open_seconds": begin.elapsed().as_secs_f64(),
        "vector_open": vector_open,
        "pid": std::process::id(),
    });
    fs::write(
        args.output.join("ready.json"),
        serde_json::to_vec_pretty(&ready)?,
    )?;
    println!("{}", serde_json::to_string(&ready)?);
    loop {
        std::thread::sleep(Duration::from_secs(60));
        let status = json!({
            "posting_cache_bytes": engine.cached_bytes(),
            "transport": server.counters(),
            "rss_bytes": mem_probe::read_self().rss,
        });
        if let Err(e) = fs::write(
            args.output.join("status.json"),
            serde_json::to_vec_pretty(&status)?,
        ) {
            eprintln!("status write failed: {e}");
        }
    }
}
