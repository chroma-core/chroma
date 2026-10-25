//! The same owner service runs on each physical machine. This executable keeps
//! remote construction separate from the retained unsharded reference harness.
#![recursion_limit = "256"]
#![allow(unexpected_cfgs, dead_code)]
mod hierarchical_index;
use chroma_distance::DistanceFunction;
use clap::Parser;
use hierarchical_index::{
    config::HierarchicalSpannConfig,
    distributed::{
        engine::*,
        placement_runtime::{PlacementRequest, PlacementResponse, PlacementStatistics},
        profile_gate::{phase_timestamps, ProfileGate},
        reporting::{placement_plan_json, unsigned_count_json},
        routing::*,
        saved_reader::{CanonicalVectorSource, VectorFileDescriptor},
        transport::*,
    },
    mem_probe,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    mode: String,
    /// Restore the corresponding owner file from an immutable writer checkpoint.
    #[arg(long)]
    restore_checkpoint: Option<PathBuf>,
    /// Stop after drained redistribution at this input position and checkpoint both owners.
    #[arg(long)]
    checkpoint_at: Option<u64>,
    /// Checkpoint immediately after insertion, before balance discovers queued work.
    #[arg(long, requires = "checkpoint_directory", conflicts_with_all = ["checkpoint_at", "balance_only", "replay_queries", "save_unchanged_generation", "storage_config"])]
    checkpoint_after_add_at: Option<u64>,
    /// Balance an exact restored post-insertion state without inserting more rows.
    #[arg(long, requires = "restore_checkpoint", conflicts_with_all = ["checkpoint_at", "checkpoint_directory", "checkpoint_after_add_at", "replay_queries", "save_unchanged_generation", "storage_config"])]
    balance_only: bool,
    /// Keep preparation on its initial owner until this redistribution boundary.
    #[arg(long, default_value_t = 0)]
    placement_start: u64,
    #[arg(long)]
    checkpoint_directory: Option<PathBuf>,
    /// Save before balancing and after placement at each multiple of this row count.
    #[arg(long, requires = "checkpoint_directory", conflicts_with_all = ["checkpoint_at", "checkpoint_after_add_at", "balance_only", "replay_queries"])]
    checkpoint_every: Option<u64>,
    /// Save before balancing and after placement at each listed row count,
    /// as explicit batch boundaries; combines with `--checkpoint-every`.
    #[arg(long, value_delimiter = ',', requires = "checkpoint_directory", conflicts_with_all = ["checkpoint_at", "checkpoint_after_add_at", "balance_only", "replay_queries"])]
    checkpoint_rows: Vec<u64>,
    /// Longest allowed time from start to `--shutdown-at-unix`. Long builds,
    /// such as a 100M single-owner ladder, raise it from the 90-minute default.
    #[arg(long, default_value_t = 5400)]
    max_run_seconds: u64,
    /// Write periodic checkpoints only after balancing and placement, not
    /// also before balancing; each is a complete recovery point on its own.
    #[arg(long)]
    checkpoint_after_balance_only: bool,
    /// After restoring a balanced checkpoint at exactly `--rows`, redistribute
    /// it between the two owners with `--owner-budget` each before saving.
    /// Ladder builds keep every row on owner 1 until the end, so each smaller
    /// ladder size is placed only when its generation is saved.
    #[arg(long, requires = "restore_checkpoint")]
    place_after_restore: bool,
    /// Validate live writer membership and all posting codes after timing; omit reader publication.
    #[arg(long)]
    construction_only: bool,
    /// Run fixed, read-only navigation queries against a restored routing view.
    #[arg(long, default_value_t = 0)]
    replay_queries: u64,
    #[arg(long)]
    owner: u64,
    #[arg(long)]
    run: u128,
    #[arg(long)]
    input_descriptor: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    data_bind: SocketAddr,
    #[arg(long)]
    control_bind: SocketAddr,
    #[arg(long)]
    peer_data: SocketAddr,
    #[arg(long)]
    peer_control: SocketAddr,
    #[arg(long)]
    token_file: PathBuf,
    #[arg(long)]
    shutdown_at_unix: u64,
    /// Prebuilt standalone saved-reader executable; never launches Cargo.
    #[arg(long)]
    reader_binary: Option<PathBuf>,
    /// Shared write-once durable store. Owners write directly; no saved payload relay.
    #[arg(long)]
    storage_config: Option<PathBuf>,
    /// Measure a second unchanged durable generation separately from first save.
    #[arg(long)]
    save_unchanged_generation: bool,
    /// Save each owner's full vectors as a blockfile with the generation.
    /// Remote owners write directly to the durable store, so this needs
    /// --storage-config; a repeated save would reuse blocks the store needs.
    #[arg(long, requires = "storage_config", conflicts_with = "save_unchanged_generation")]
    save_vector_store: bool,
    #[arg(long, default_value_t = 16)]
    validation_queries: usize,
    /// Conservative resident corpus-cache allowance per machine, including row
    /// allocations; 0 reads vectors from the input file on demand.
    #[arg(long, default_value_t = 12 * 1024 * 1024 * 1024u64)]
    vector_cache_bytes: u64,
    /// Maximum request payload and reply chunk in bytes; configure both owners equally.
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    max_frame_bytes: usize,
    #[arg(long, default_value_t = 32)]
    workers: usize,
    #[arg(long, default_value_t = 100_000)]
    rows: u64,
    #[arg(long, default_value_t = 100_000)]
    initial_batch: u64,
    #[arg(long, default_value_t = 1_000_000)]
    transition: u64,
    #[arg(long, default_value_t = 1_000_000)]
    later_batch: u64,
    #[arg(long)]
    owner_budget: u64,
    /// A diagnostic-only first-interval budget. Zero uses the fixed budget.
    #[arg(long, default_value_t = 0)]
    first_owner_budget: u64,
    #[arg(long, default_value_t = 2048)]
    split_threshold: usize,
    #[arg(long, default_value_t = 512)]
    merge_threshold: usize,
    #[arg(long, default_value_t = 100)]
    branching_factor: usize,
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
fn event(file: &mut File, mut value: Value) -> Result<(), Box<dyn std::error::Error>> {
    let timestamps = phase_timestamps();
    value["realtime_ns"] = timestamps["realtime_ns"].clone();
    value["monotonic_ns"] = timestamps["monotonic_ns"].clone();
    serde_json::to_writer(&mut *file, &value)?;
    writeln!(file)?;
    file.flush()?;
    Ok(())
}
fn counts(cluster: &Cluster) -> Result<Vec<OwnerCounts>, String> {
    (1..=2)
        .map(
            |owner| match cluster.request(owner, EngineRequest::Counts)? {
                EngineResponse::Counts(c) => Ok(c),
                _ => Err("invalid owner counts response".into()),
            },
        )
        .collect()
}
fn placement_statistics(
    cluster: &Cluster,
    run: u128,
) -> Result<BTreeMap<u64, PlacementStatistics>, String> {
    (1..=2)
        .map(|owner| {
            match cluster.request(
                owner,
                EngineRequest::Placement(PlacementRequest::Statistics { run }),
            )? {
                EngineResponse::Placement(PlacementResponse::Statistics(statistics)) => {
                    Ok((owner, statistics))
                }
                _ => Err("invalid placement statistics response".into()),
            }
        })
        .collect()
}
/// Inspect routing records after timed construction without reading postings.
fn routing_shape(cluster: &Cluster) -> Result<Value, String> {
    let root = match cluster.request(1, EngineRequest::CurrentRoot)? {
        EngineResponse::Routing(root) => root,
        _ => return Err("invalid routing-shape root response".into()),
    };
    let mut queue = vec![(root.target, 0usize)];
    let mut seen = BTreeSet::new();
    let mut nodes_by_depth = BTreeMap::<usize, usize>::new();
    let mut leaves_by_depth = BTreeMap::<usize, usize>::new();
    let mut internal_child_counts = BTreeMap::<usize, usize>::new();
    let mut nodes_by_authority = BTreeMap::<u64, usize>::new();
    while let Some((target, depth)) = queue.pop() {
        if seen.len() >= 1_000_000 || !seen.insert(target.id) {
            return Err(
                "routing-shape traversal exceeded its bound or found a repeated node".into(),
            );
        }
        let node = cluster.node(target)?;
        *nodes_by_depth.entry(depth).or_default() += 1;
        *nodes_by_authority.entry(target.authority).or_default() += 1;
        if node.leaf {
            *leaves_by_depth.entry(depth).or_default() += 1;
        } else {
            *internal_child_counts
                .entry(node.children.len())
                .or_default() += 1;
            for child in node.children {
                queue.push((child.target, depth + 1));
            }
        }
    }
    Ok(
        json!({"nodes":seen.len(),"nodes_by_depth":nodes_by_depth,"leaves_by_depth":leaves_by_depth,"internal_node_count_by_child_count":internal_child_counts,"nodes_by_routing_authority":nodes_by_authority,"maximum_depth":nodes_by_depth.keys().next_back(),"scope":"routing metadata only; root depth is zero; no posting payloads fetched; measured outside construction timer"}),
    )
}
/// Count each cross-owner connection at its coordinator endpoint, whether the
/// coordinator initiated it or answered a peer-initiated request.
fn rpc_accounting(client: &RpcClient, server: &RpcServer) -> Value {
    let outgoing = client.counters();
    let incoming = server.counters();
    let sent = outgoing.bytes_sent as u128 + incoming.bytes_sent as u128;
    let received = outgoing.bytes_received as u128 + incoming.bytes_received as u128;
    json!({"coordinator_client":outgoing,"coordinator_server":incoming,"application_framed_bytes_sent":unsigned_count_json(sent),"application_framed_bytes_received":unsigned_count_json(received),"application_framed_bytes_total":unsigned_count_json(sent+received),"overflow_integer_encoding":"aggregate byte values above u64::MAX use exact decimal strings","counter_kind":"cumulative snapshots; subtract phase before from phase after","scope":"JSON request/reply envelopes and length prefixes, including partial or failed transfers; each connection is observed once at the coordinator; excludes TCP/IP headers and retransmissions"})
}
fn check_deadline(args: &Args) -> Result<(), String> {
    if now() >= args.shutdown_at_unix {
        Err("absolute execution deadline reached".into())
    } else {
        Ok(())
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.save_unchanged_generation && args.storage_config.is_none() {
        return Err("--save-unchanged-generation requires --storage-config".into());
    }
    let reader_binary = if args.mode == "run" {
        let path = match &args.reader_binary {
            Some(p) => p.clone(),
            None => std::env::current_exe()?.with_file_name("two_machine_reader"),
        };
        if !path.is_file() {
            return Err("a prebuilt --reader-binary is required before construction".into());
        }
        Some(path)
    } else {
        None
    };
    if ![1, 2].contains(&args.owner)
        || !matches!(args.mode.as_str(), "serve" | "run")
        || args.workers == 0
        || args.workers > 32
        || args.rows == 0
        || args.initial_batch == 0
        || args.later_batch == 0
        || args.owner_budget == 0
    {
        return Err(
            "invalid owner, mode or bound; this admitted transport fixture supports 1..32 workers"
                .into(),
        );
    }
    if (args.balance_only || args.checkpoint_after_add_at.is_some()) && !args.construction_only {
        return Err("balance replay and post-insertion checkpoint require --construction-only".into());
    }
    if args.checkpoint_after_add_at.is_some_and(|position| position == 0 || position > args.rows) {
        return Err("post-insertion checkpoint must be within workload rows".into());
    }
    if args.mode == "run" && args.owner != 1 {
        return Err("only owner one coordinates the campaign".into());
    }
    if args.shutdown_at_unix <= now() || args.shutdown_at_unix - now() > args.max_run_seconds {
        return Err("requires a fresh absolute deadline within --max-run-seconds".into());
    }
    if args.output.exists() {
        return Err("output exists; preserve prior attempts".into());
    }
    fs::create_dir_all(&args.output)?;
    let mut events = File::create(args.output.join("events.jsonl"))?;
    let token = fs::read_to_string(&args.token_file)?.trim().to_owned();
    if token.len() < 32 {
        return Err("run token must contain at least32characters".into());
    }
    let descriptor: VectorFileDescriptor =
        serde_json::from_slice(&fs::read(&args.input_descriptor)?)?;
    if descriptor.first_id != 0 || descriptor.rows != args.rows {
        return Err("canonical input must contain exactly the workload rows".into());
    }
    let input_time = Instant::now();
    let vector_source = CanonicalVectorSource::open(descriptor.clone())?;
    let vector_identity_seconds = input_time.elapsed().as_secs_f64();
    let estimated_cache_bytes = vector_source.estimated_resident_bytes()?;
    let cache_warm = Instant::now();
    // A zero allowance reads each vector from the input file on demand,
    // through the page cache, instead of holding a resident copy. A ladder
    // build uses it on the idle second owner, whose rows arrive only at
    // placement, so two owners fit on one machine.
    let vectors = Arc::new(if args.vector_cache_bytes == 0 {
        vector_source
    } else {
        vector_source.preload_with_limit(args.vector_cache_bytes)?
    });
    event(
        &mut events,
        json!({"event":"input_ready","seconds":input_time.elapsed().as_secs_f64(),"owner":args.owner,"input":descriptor,"immutable_corpus_cache":"full input replicated independently on each owner, not posting membership replication","vector_cache_limit_bytes":args.vector_cache_bytes,"estimated_cache_resident_bytes":estimated_cache_bytes,"vector_identity_seconds":vector_identity_seconds,"cache_warming_seconds":cache_warm.elapsed().as_secs_f64(),"peak_rss":mem_probe::read_self().rss_peak}),
    )?;
    let config = HierarchicalSpannConfig {
        branching_factor: args.branching_factor,
        split_threshold: args.split_threshold,
        merge_threshold: args.merge_threshold,
        ..Default::default()
    };
    let settings = json!({
        "metric":"l2", "dimensions":descriptor.dimensions,
        "branching_factor":config.branching_factor,
        "split_threshold":config.split_threshold, "merge_threshold":config.merge_threshold,
        "write_beam_tau":config.write_beam_tau, "write_beam_min":config.write_beam_min,
        "write_beam_max":config.write_beam_max, "write_level_taus":config.write_level_taus,
        "write_level_min_pcts":config.write_level_min_pcts,
        "reassign_neighbor_count":config.reassign_neighbor_count,
        "write_navigation":config.write_navigation.to_string(), "fp_npa":config.fp_npa,
        "saved_query_centroid_bits":4, "posting_bits":1, "full_vector_bits":32,
        "insertion_workers":args.workers, "balance_workers_per_owner":args.workers,
        "batch_cadence":{"initial":args.initial_batch,"transition":args.transition,"later":args.later_batch,"final_partial_balanced":true},
        "seed":"not controlled; reference split kernel uses its existing random initialization",
        "scheduler":"thread interleavings are not deterministic",
        "owner_vector_capacity":args.owner_budget,"first_interval_owner_vector_capacity_override":args.first_owner_budget,
        "immutable_vector_cache_limit_bytes_per_owner":args.vector_cache_bytes
    });
    event(
        &mut events,
        json!({"event":"benchmark_settings","owner":args.owner,"settings":settings}),
    )?;
    let owner = Arc::new(OwnerService::new(
        args.owner,
        args.run,
        descriptor.dimensions,
        DistanceFunction::Euclidean,
        config,
        vectors,
    ));
    if let Some(directory) = &args.checkpoint_directory { fs::create_dir_all(directory)?; }
    let checkpoint = args.restore_checkpoint.as_ref().map(|dir| WriterCheckpoint::read(&dir.join(format!("owner-{}.json", args.owner)))).transpose()?;
    let resume_input = checkpoint.as_ref().map_or(0, |c| c.next_input);
    let mut balance_pending = checkpoint.as_ref().is_some_and(|c| c.balance_pending);
    if args.checkpoint_every == Some(0) { return Err("checkpoint interval must be nonzero".into()); }
    if resume_input > args.rows { return Err("checkpoint input position exceeds workload".into()); }
    if args.balance_only && (resume_input == 0 || resume_input != args.rows) {
        return Err("balance-only requires checkpoint next_input equal to nonzero workload rows".into());
    }
    if args.checkpoint_after_add_at.is_some_and(|position| position <= resume_input) {
        return Err("post-insertion checkpoint must follow restored input position".into());
    }
    let mut initial = owner.initial_root();
    initial.routing.target = NodeRef {
        id: 1u128 << 64,
        authority: 1,
        placement: 0,
    };
    if args.owner == 1 && checkpoint.is_none() {
        owner.install_initial(vec![initial.clone()])?;
    }
    let peer = if args.owner == 1 { 2 } else { 1 };
    // Data jobs can wait for peer navigation and transfer acknowledgements.
    // Reserve independent bounded capacity for all 32 construction workers and
    // their nested control requests instead of rejecting the baseline workload.
    let transport = TransportConfig {
        max_frame_bytes: args.max_frame_bytes,
        max_data_handlers: 64,
        max_control_handlers: 128,
        ..TransportConfig::default()
    };
    let capacity = transport.capacity();
    let transport_capacity = json!({"execution_and_decoder_capacity":capacity,"max_operation_seconds":transport.max_operation_time.as_secs_f64(),"maximum_simultaneous_receive_frame_bytes":(capacity.data_connections+capacity.control_connections)as u64*transport.max_frame_bytes as u64,"receive_buffer_scope":"logical frame payload bound only; allocator slack, decoded requests, response buffers, engine data and worker stacks require additional memory; actual process peak RSS is measured separately"});
    let client = RpcClient::new(
        args.peer_data,
        args.peer_control,
        token.clone(),
        transport.clone(),
    )?;
    let shutdown_deadline =
        Instant::now() + Duration::from_secs(args.shutdown_at_unix.saturating_sub(now()));
    let mut cluster = Cluster::new(
        owner.clone(),
        BTreeMap::from([(peer, client.clone())]),
        initial.routing,
    );
    if let Some(checkpoint) = checkpoint { cluster.restore_checkpoint(checkpoint, &descriptor.sha256)?; }
    // Saves also write each owner's full vectors to the store as a blockfile.
    cluster.save_vector_store.store(args.save_vector_store, std::sync::atomic::Ordering::Relaxed);
    cluster.shutdown_deadline = Some(shutdown_deadline);
    let cluster = Arc::new(cluster);
    owner.bind_cluster(&cluster);
    // Each owner samples itself, avoiding a monitoring RPC and its queue delay.
    let sample_owner = owner.clone();
    let sample_path = args.output.join("owner-seconds.jsonl");
    let sample_deadline = args.shutdown_at_unix;
    std::thread::spawn(move || {
        if let Ok(mut file) = File::create(sample_path) {
            while now() < sample_deadline {
                if let Ok(EngineResponse::Counts(value)) = sample_owner.handle(EngineRequest::Counts) {
                    let _ = event(&mut file, json!({"event":"owner_sample", "counts":value}));
                }
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    });
    let handler = owner.clone();
    let ordinary_timeout = transport.max_operation_time;
    let mut server = RpcServer::bind_with_request_timeout(
        args.data_bind,
        args.control_bind,
        token,
        transport,
        Arc::new(move |request: EngineRequest| handler.handle(request)),
        Arc::new(move |request: &EngineRequest| {
            let cap = if matches!(request, EngineRequest::SaveOwner(_) | EngineRequest::WriterCheckpoint { .. } | EngineRequest::ValidateWriter) {
                hierarchical_index::distributed::save_runtime::save_operation_timeout()
            } else {
                ordinary_timeout
            };
            cap.min(shutdown_deadline.saturating_duration_since(Instant::now()))
        }),
    )?;
    event(
        &mut events,
        json!({"event":"server_ready","owner":args.owner,"data":server.data_addr(),"control":server.control_addr(),"deadline":args.shutdown_at_unix,"transport_capacity":transport_capacity}),
    )?;
    if args.mode == "serve" {
        // Persist peer transport evidence while it is serving: the benchmark
        // supervisor can terminate this process before its normal deadline.
        // These cumulative samples are endpoint observations, not extra traffic.
        let mut next_transport_sample = Instant::now();
        while now() < args.shutdown_at_unix {
            if hierarchical_index::distributed::observability::detailed()
                && Instant::now() >= next_transport_sample
            {
                event(
                    &mut events,
                    json!({"event":"peer_transport_sample","owner":args.owner,"local_outgoing_client":client.counters(),"local_incoming_server":server.counters(),"scope":"cumulative endpoint observations; do not add to coordinator endpoint totals"}),
                )?;
                next_transport_sample = Instant::now() + Duration::from_secs(1);
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        server.shutdown();
        event(
            &mut events,
            json!({"event":"server_shutdown","owner":args.owner,"local_outgoing_client":client.counters(),"local_incoming_server":server.counters(),"scope":"peer endpoint confirmation; do not add these bytes to the coordinator endpoint totals"}),
        )?;
        return Ok(());
    }
    // Fail closed rather than launch a partially connected benchmark.
    let before = counts(&cluster)?;
    event(
        &mut events,
        json!({"event":"peer_verified","counts":before}),
    )?;
    let build = Instant::now();
    if args.replay_queries > 0 {
        let began = Instant::now();
        let mut results = Vec::new();
        for index in 0..args.replay_queries {
            let id = index.wrapping_mul(104729) % resume_input.max(1);
            let vector = owner.vectors.get(id)?;
            results.push(cluster.navigate(&[vector.to_vec()], &cluster.write_policy(), None)?);
        }
        let replay_seconds = began.elapsed().as_secs_f64();
        // Serialize wide node IDs directly; serde_json::Value is limited to u64.
        fs::write(args.output.join("replay-results.json"), serde_json::to_vec(&results)?)?;
        fs::write(args.output.join("replay.json"), serde_json::to_vec(&json!({"seconds":replay_seconds,"queries":args.replay_queries,"owners":counts(&cluster)?}))?)?;
        return Ok(());
    }
    if args.place_after_restore {
        if resume_input != args.rows || balance_pending {
            return Err("--place-after-restore needs a balanced checkpoint at exactly the workload rows".into());
        }
        let t = Instant::now();
        let report = cluster.redistribute(BTreeMap::from([(1, args.owner_budget), (2, args.owner_budget)]))?;
        event(
            &mut events,
            json!({"event":"restored_placement_complete","seconds":t.elapsed().as_secs_f64(),"placement_report":placement_plan_json(&report),"vector_budget_per_owner":args.owner_budget,"owners":counts(&cluster)?}),
        )?;
    }
    let mut start = resume_input;
    let mut cycles = vec![];
    while start < args.rows || balance_pending || (args.balance_only && cycles.is_empty()) {
        check_deadline(&args)?;
        let skip_add = args.balance_only || balance_pending;
        let end = if skip_add { start } else if start < args.transition {
            start
                .saturating_add(args.initial_batch)
                .min(args.transition)
                .min(args.rows)
        } else {
            start.saturating_add(args.later_batch).min(args.rows)
        };
        // A requested insertion checkpoint is an explicit batch boundary.
        let end = args.checkpoint_after_add_at.map_or(end, |position| end.min(position));
        let end = if !skip_add {
            let end = args.checkpoint_every.map_or(end, |interval| end.min((start / interval + 1).saturating_mul(interval)));
            args.checkpoint_rows.iter().copied().filter(|r| *r > start).min().map_or(end, |r| end.min(r))
        } else { end };
        let ids = (start..end).collect::<Vec<_>>();
        let before_add = counts(&cluster)?;
        let rpc_before_add = rpc_accounting(&client, &server);
        let t = Instant::now();
        if !skip_add { cluster.add_batch(&ids, args.workers)?; }
        let add = if skip_add { 0.0 } else { t.elapsed().as_secs_f64() };
        let rpc_after_add = rpc_accounting(&client, &server);
        let after_add = counts(&cluster)?;
        // Persist completed phase evidence before balancing can fail. Reuse the
        // existing boundary snapshots so observation adds no in-phase RPC work.
        event(
            &mut events,
            json!({"event":if skip_add { "add_skipped_for_balance_replay" } else { "add_complete" },"rows_before":start,"rows_after":end,"seconds":add,"before":before_add,"after":after_add,"rpc_before":rpc_before_add,"rpc_after":rpc_after_add}),
        )?;
        if args.checkpoint_after_add_at == Some(end) {
            let directory = args.checkpoint_directory.as_ref().ok_or("checkpoint directory required")?;
            cluster.checkpoint_phase(directory, end, &descriptor.sha256, true, false)?;
            let receipt = json!({"status":"writer_checkpoint_complete", "mode":"post_insertion_checkpoint", "next_input":end, "resume_input":resume_input, "add_seconds":add, "directory":directory, "owners":counts(&cluster)?});
            event(&mut events, json!({"event":"writer_checkpoint_complete", "checkpoint_phase":"after_add_before_balance", "receipt":receipt}))?;
            fs::write(args.output.join("result.json"), serde_json::to_vec_pretty(&receipt)?)?;
            return Ok(());
        }
        let periodic_boundary = args.checkpoint_every.is_some_and(|interval| end % interval == 0 || end == args.rows)
            || args.checkpoint_rows.contains(&end);
        if periodic_boundary && !skip_add && !args.checkpoint_after_balance_only {
            let directory = args.checkpoint_directory.as_ref().unwrap().join(format!("rows-{end}-after-add"));
            cluster.checkpoint_phase(&directory, end, &descriptor.sha256, true, true)?;
            event(&mut events, json!({"event":"writer_checkpoint_complete", "checkpoint_phase":"after_add_before_balance", "next_input":end, "directory":directory}))?;
        }
        let before_balance = counts(&cluster)?;
        let kernel_before = chroma_index::spann::utils::clustering_profile_snapshot();
        let gate = ProfileGate::before(args.rows, start)?;
        event(
            &mut events,
            json!({"event":"balance_started","rows":end,"owners":before_balance}),
        )?;
        let rpc_before_balance = rpc_accounting(&client, &server);
        let coalescing_before = cluster.reassignment_coalescing();
        let producer_skips_before = cluster.producer_dependency_skips();
        let cpu_before_balance = hierarchical_index::distributed::metrics::cpu_seconds();
        let t = Instant::now();
        let balance_result = cluster.balance(args.workers);
        let balance = t.elapsed().as_secs_f64();
        let cpu_after_balance = hierarchical_index::distributed::metrics::cpu_seconds();
        let producer_skips_after = cluster.producer_dependency_skips();
        let coalescing_after = cluster.reassignment_coalescing();
        let coalescing = json!({"before":coalescing_before,"after":coalescing_after,"delta":{"dispatches":coalescing_after.dispatches.saturating_sub(coalescing_before.dispatches),"extra_jobs_combined":coalescing_after.merged_jobs.saturating_sub(coalescing_before.merged_jobs),"duplicate_pending_ids_removed":coalescing_after.duplicate_pending_ids.saturating_sub(coalescing_before.duplicate_pending_ids)},"unit":"queue dispatches, extra queued jobs combined, and duplicate IDs removed between combined jobs; within-request normalization excluded; not guaranteed execution rows saved"});
        let producer_dependencies = json!({"before":producer_skips_before,"after":producer_skips_after,"delta":producer_skips_after.saturating_sub(producer_skips_before),"unit":"queue visits withheld by an active producer attempt; not unique jobs or elapsed time"});
        let balance_cpu_seconds = cpu_before_balance
            .zip(cpu_after_balance)
            .map(|(a, b)| b - a);
        let gate_result = gate
            .as_ref()
            .map(|g| g.after(balance, balance_result.is_ok()))
            .transpose();
        if let Err(error) = balance_result {
            let failed_seconds = balance;
            // Keep the original failure even when a poisoned owner cannot
            // provide counters. These observations are outside the failed phase.
            let owner_counts = counts(&cluster);
            let _ = event(
                &mut events,
                json!({"event":"balance_failed","rows":end,"seconds":failed_seconds,"scheduler_producer_dependencies":producer_dependencies,"reassignment_coalescing":coalescing,"error":error,"owners":owner_counts,"rpc":rpc_accounting(&client, &server)}),
            );
            return Err(error.into());
        }
        gate_result?;
        let kernel_after = chroma_index::spann::utils::clustering_profile_snapshot();
        let rpc_after_balance = rpc_accounting(&client, &server);
        let before_placement = counts(&cluster)?;
        event(
            &mut events,
            json!({"event":"balance_complete","rows":end,"seconds":balance,"scheduler_producer_dependencies":producer_dependencies,"reassignment_coalescing":coalescing,"coordinator_process_cpu_seconds":balance_cpu_seconds,"clustering_local_process_delta":kernel_after.delta(&kernel_before),"owners":before_placement,"rpc_before":rpc_before_balance,"rpc_after":rpc_after_balance}),
        )?;
        let budget = if start == 0 && args.first_owner_budget > 0 {
            args.first_owner_budget
        } else {
            args.owner_budget
        };
        let placement_statistics_before = placement_statistics(&cluster, args.run)?;
        let rpc_before_placement = rpc_accounting(&client, &server);
        let t = Instant::now();
        let placement_report = if end >= args.placement_start {
            cluster.redistribute(BTreeMap::from([(1, budget), (2, budget)]))?
        } else { Default::default() };
        let placement = t.elapsed().as_secs_f64();
        let rpc_after_placement = rpc_accounting(&client, &server);
        let placement_statistics_after = placement_statistics(&cluster, args.run)?;
        let cycle = json!({"mode":if args.balance_only {"balance_only"} else {"construction"},"add_skipped_for_balance_replay":skip_add,"rows_before":start,"rows_after":end,"add_seconds":add,"balance_seconds":balance,"scheduler_producer_dependencies":producer_dependencies,"reassignment_coalescing":coalescing,"coordinator_balance_cpu_seconds":balance_cpu_seconds,"owner_count_snapshot_interval":"OwnerCounts CPU deltas include profile-gate waits and boundary observations; coordinator_balance_cpu_seconds surrounds timed balance directly","placement_seconds":placement,"placement_report":placement_plan_json(&placement_report),"placement_statistics":{"before":placement_statistics_before,"after":placement_statistics_after},"placement_byte_accounting":"Completed outgoing payload counters count each serialized handoff payload once at its source, excluding envelopes/framing; RPC phase snapshots separately count framed application bytes","vector_budget_per_owner":budget,"before_add":before_add,"after_add":after_add,"before_balance":before_balance,"clustering_local_process_delta":kernel_after.delta(&kernel_before),"before_placement":before_placement,"after_placement":counts(&cluster)?,"peer_transport":client.counters(),"rpc_phases":{"add":{"before":rpc_before_add,"after":rpc_after_add},"balance":{"before":rpc_before_balance,"after":rpc_after_balance},"placement":{"before":rpc_before_placement,"after":rpc_after_placement}},"rpc_accounting":rpc_accounting(&client,&server),"peak_rss":mem_probe::read_self().rss_peak});
        event(&mut events, json!({"event":"cycle_complete","cycle":cycle}))?;
        cycles.push(cycle);
        start = end;
        balance_pending = false;
        if periodic_boundary {
            let directory = args.checkpoint_directory.as_ref().unwrap().join(format!("rows-{end}-after-placement"));
            cluster.checkpoint_phase(&directory, end, &descriptor.sha256, false, true)?;
            event(&mut events, json!({"event":"writer_checkpoint_complete", "checkpoint_phase":"after_balance_and_placement", "next_input":end, "directory":directory}))?;
        }
        if args.checkpoint_at == Some(end) {
            let directory = args.checkpoint_directory.as_ref().ok_or("checkpoint directory required")?;
            cluster.checkpoint_completed(directory, end, &descriptor.sha256)?;
            event(&mut events, json!({"event":"writer_checkpoint_complete", "next_input":end, "directory":directory, "owners":counts(&cluster)?}))?;
            return Ok(());
        }
    }
    let construction = build.elapsed().as_secs_f64();
    if args.construction_only {
        let validation = Instant::now();
        let mut membership = BTreeSet::new();
        for owner in 1..=2 {
            match cluster.request_until(owner, EngineRequest::ValidateWriter, Instant::now()+Duration::from_secs(600))? {
                EngineResponse::Membership(groups) => for (_, ids) in groups { for id in ids {
                    if id >= args.rows || !membership.insert(id) { return Err("invalid global writer membership".into()); }
                } },
                _ => return Err("invalid writer validation response".into()),
            }
        }
        if membership.len() as u64 != args.rows { return Err("incomplete writer membership".into()); }
        fs::write(args.output.join("result.json"), serde_json::to_vec_pretty(&json!({"status":"writer_validated", "mode":if args.balance_only {"balance_only"} else {"construction"}, "add_skipped_for_balance_replay":args.balance_only, "rows":args.rows,"resume_input":resume_input,"construction_seconds":construction,"cycles":cycles,"owners":counts(&cluster)?,"validation_seconds":validation.elapsed().as_secs_f64(),"validation":"exact global membership and every one-bit posting code checked against full-precision centroid and immutable vector","rpc_accounting":rpc_accounting(&client,&server)}))?)?;
        return Ok(());
    }

    let shape_started = Instant::now();
    let final_routing_shape = routing_shape(&cluster)?;
    let shape_seconds = shape_started.elapsed().as_secs_f64();
    event(
        &mut events,
        json!({"event":"routing_shape","seconds":shape_seconds,"shape":final_routing_shape}),
    )?;
    let final_counts = counts(&cluster)?;
    let final_placement_statistics = placement_statistics(&cluster, args.run)?;
    let total = final_counts.iter().map(|c| c.vectors).sum::<u64>();
    if total != args.rows {
        return Err("owner counts do not match input; not a membership proof".into());
    }
    let min_share = final_counts
        .iter()
        .map(|c| c.vectors as f64 / args.rows as f64)
        .fold(1.0, f64::min);
    // Counts are an occupancy check. Exact membership and durable-save checks must
    // pass before this campaign can produce an admitted performance report.
    let result = json!({"status":"construction_complete_pending_durable_validation","rows":args.rows,"run":args.run.to_string(),"construction_seconds":construction,"construction_seconds_scope":"gross elapsed build, including external profile-gate waits and phase-boundary reporting; compare balance_seconds for measured construction work","settings":settings,"routing_shape":final_routing_shape,"routing_shape_seconds":shape_seconds,"cycles":cycles,"owners":final_counts,"placement_statistics":final_placement_statistics,"minimum_owner_share":min_share,"placement_acceptance":min_share>=0.30,"unplaced_single_owner":args.placement_start > args.rows,"peer_transport":client.counters(),"rpc_accounting":rpc_accounting(&client,&server),"physical_distribution":"requires independently retained instance and process receipts","validation_complete":false});
    fs::write(
        args.output.join("construction.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    // A save that never places (placement start beyond the workload) keeps
    // every vector on owner 1 on purpose: readers split it at open time.
    let unplaced = args.placement_start > args.rows;
    if min_share < 0.30 && !unplaced {
        return Err(
            "actual smaller owner holds below30%; benchmark placement requirement failed".into(),
        );
    }
    check_deadline(&args)?;
    let saved_root = args.output.join("saved-generation");
    let rpc_before_save = rpc_accounting(&client, &server);
    let save = Instant::now();
    let durable = args
        .storage_config
        .as_ref()
        .map(
            |path| -> Result<
                hierarchical_index::distributed::generation_backend::DurableStoreConfig,
                Box<dyn std::error::Error>,
            > { Ok(serde_json::from_slice(&fs::read(path)?)?) },
        )
        .transpose()?;
    let (saved, save_profile) = if let Some(config) = &durable {
        cluster.save_completed_durable_profiled(
            &saved_root,
            config,
            descriptor.clone(),
            &format!("gen-{}", args.run),
            &format!("save-{}", args.run),
        )?
    } else {
        cluster.save_completed_profiled(
            &saved_root,
            descriptor.clone(),
            &format!("gen-{}", args.run),
            &format!("save-{}", args.run),
        )?
    };
    let save_seconds = save.elapsed().as_secs_f64();
    event(
        &mut events,
        json!({"event":"generation_saved","save_seconds":save_seconds,"save_profile":save_profile,"descriptor":saved,"store":saved_root,"peer_transport":client.counters(),"rpc_before_save":rpc_before_save,"rpc_after_save":rpc_accounting(&client,&server)}),
    )?;
    let mut repeat_save = None;
    let mut repeated_descriptor = None;
    if args.save_unchanged_generation {
        check_deadline(&args)?;
        let repeat_started = Instant::now();
        let (descriptor, profile) = cluster.save_completed_durable_profiled(
            &saved_root,
            durable.as_ref().unwrap(),
            descriptor.clone(),
            &format!("gen-{}-unchanged", args.run),
            &format!("save-{}-unchanged", args.run),
        )?;
        let receipt = json!({"descriptor": descriptor, "save_seconds": repeat_started.elapsed().as_secs_f64(), "save_profile": profile, "scope": "second save of the same sealed state; excluded from first construction_and_save_seconds"});
        event(
            &mut events,
            json!({"event": "unchanged_generation_saved", "repeat_save": receipt}),
        )?;
        repeated_descriptor = Some(descriptor);
        repeat_save = Some(receipt);
    }
    // The child reads immutable saved files only. Release the coordinator's live
    // writer and vector cache before its independent validation allocates memory.
    server.shutdown();
    let final_rpc_accounting = rpc_accounting(&client, &server);
    drop(server);
    drop(cluster);
    drop(owner);
    check_deadline(&args)?;
    let reader = reader_binary
        .as_ref()
        .ok_or("missing saved-reader executable")?;
    let (validation_seconds, validation_output) =
        validate_generation_process(&args, reader, &saved_root, &saved, "first")?;
    if let Some(descriptor) = &repeated_descriptor {
        let (seconds, output) =
            validate_generation_process(&args, reader, &saved_root, descriptor, "repeat")?;
        repeat_save.as_mut().unwrap()["fresh_reader_validation_seconds"] = json!(seconds);
        repeat_save.as_mut().unwrap()["validation_report"] = json!(output);
    }
    let result = json!({"status":"construction_save_and_fresh_reader_validated","rows":args.rows,"run":args.run.to_string(),"construction_seconds":construction,"construction_seconds_scope":"gross elapsed build, including external profile-gate waits and phase-boundary reporting; compare balance_seconds for measured construction work","settings":settings,"routing_shape":final_routing_shape,"routing_shape_seconds":shape_seconds,"save_seconds":save_seconds,"save_profile":save_profile,"repeat_save":repeat_save,"save_profile_scope":"owner_calls_nanos sums overlapping owner call wall times; owner_materialization_wall_nanos is joined elapsed wall time; owner_materialization_max_concurrency is the configured cap, not observed CPU concurrency; owner/store sync times are nested; local_reuse bytes are logical verified payloads, not writes; immutable bytes count supplied payloads and completed temporary writes, not device IO","construction_and_save_seconds":construction+save_seconds,"fresh_reader_validation_seconds":validation_seconds,"cycles":cycles,"owners":final_counts,"placement_statistics":final_placement_statistics,"minimum_owner_share":min_share,"placement_acceptance":true,"peer_transport":client.counters(),"rpc_accounting":final_rpc_accounting,"physical_distribution":"requires independently retained instance and process receipts","save_adapter":if durable.is_some(){"owners publish changed immutable posting blocks directly and reuse unchanged references; coordinator publishes compatible metadata without payload relay"}else{"verified local owner objects reused in coordinator POSIX generation namespace; remote immutable owner files copied there"},"durable_store":durable,"validation_complete":true,"validation_report":validation_output,"saved_descriptor":saved});
    fs::write(
        args.output.join("result.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    event(
        &mut events,
        json!({"event":"campaign_validated","result":result}),
    )?;
    Ok(())
}

fn validate_generation_process(
    args: &Args,
    reader: &std::path::Path,
    store: &std::path::Path,
    descriptor: &hierarchical_index::distributed::generation::GenerationDescriptor,
    label: &str,
) -> Result<(f64, PathBuf), Box<dyn std::error::Error>> {
    check_deadline(args)?;
    let descriptor_path = args
        .output
        .join(format!("{label}-generation-descriptor.json"));
    fs::write(&descriptor_path, serde_json::to_vec_pretty(descriptor)?)?;
    let output = args.output.join(if label == "first" {
        "durable-validation.json"
    } else {
        "repeat-durable-validation.json"
    });
    let start = Instant::now();
    let mut command = std::process::Command::new(reader);
    command
        .arg("--store")
        .arg(store)
        .arg("--descriptor")
        .arg(&descriptor_path)
        .arg("--expected-rows")
        .arg(args.rows.to_string())
        .arg("--output")
        .arg(&output)
        .arg("--query-count")
        .arg(args.validation_queries.to_string())
        .stdout(std::process::Stdio::from(File::create(args.output.join(
            if label == "first" {
                "reader.stdout"
            } else {
                "repeat-reader.stdout"
            },
        ))?))
        .stderr(std::process::Stdio::from(File::create(args.output.join(
            if label == "first" {
                "reader.stderr"
            } else {
                "repeat-reader.stderr"
            },
        ))?));
    if let Some(path) = &args.storage_config {
        command.arg("--storage-config").arg(path);
    }
    let mut child = command.spawn()?;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if now() >= args.shutdown_at_unix {
            let _ = child.kill();
            let _ = child.wait();
            return Err("saved-reader validation reached absolute execution deadline".into());
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    if !status.success() {
        return Err(format!("{label} saved-reader process failed: {status}").into());
    }
    let report: Value = serde_json::from_slice(&fs::read(&output)?)?;
    if report["status"] != "durable_reader_validated"
        || report["rows"] != args.rows
        || report["validated_posting_codes"] != args.rows
        || report["exact_membership"] != true
        || report["vector_sha256_verified"] != true
    {
        return Err("fresh-reader report lacks complete content validation".into());
    }
    Ok((start.elapsed().as_secs_f64(), output))
}
