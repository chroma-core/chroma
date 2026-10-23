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
    #[arg(long, default_value_t = 16)]
    validation_queries: usize,
    /// Conservative resident corpus-cache allowance per machine, including row allocations.
    #[arg(long, default_value_t = 12 * 1024 * 1024 * 1024u64)]
    vector_cache_bytes: u64,
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
    if args.mode == "run" && args.owner != 1 {
        return Err("only owner one coordinates the campaign".into());
    }
    if args.shutdown_at_unix <= now() || args.shutdown_at_unix - now() > 5400 {
        return Err("requires a fresh absolute deadline within90minutes".into());
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
    let vectors = Arc::new(vector_source.preload_with_limit(args.vector_cache_bytes)?);
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
    let mut initial = owner.initial_root();
    initial.routing.target = NodeRef {
        id: 1u128 << 64,
        authority: 1,
        placement: 0,
    };
    if args.owner == 1 {
        owner.install_initial(vec![initial.clone()])?;
    }
    let peer = if args.owner == 1 { 2 } else { 1 };
    // Data jobs can wait for peer navigation and transfer acknowledgements.
    // Reserve independent bounded capacity for all 32 construction workers and
    // their nested control requests instead of rejecting the baseline workload.
    let transport = TransportConfig {
        max_data_handlers: 64,
        max_control_handlers: 128,
        ..TransportConfig::default()
    };
    let transport_capacity = json!({"data_handlers":transport.max_data_handlers,"control_handlers":transport.max_control_handlers,"max_frame_bytes":transport.max_frame_bytes,"max_operation_seconds":transport.max_operation_time.as_secs_f64(),"maximum_simultaneous_receive_frame_bytes":(transport.max_data_handlers+transport.max_control_handlers)as u64*transport.max_frame_bytes as u64,"receive_buffer_scope":"frame bytes only; decoded requests, responses, engine data and handler stacks require additional memory; actual process peak RSS is measured separately"});
    let client = RpcClient::new(
        args.peer_data,
        args.peer_control,
        token.clone(),
        transport.clone(),
    )?;
    let cluster = Arc::new(Cluster::new(
        owner.clone(),
        BTreeMap::from([(peer, client.clone())]),
        initial.routing,
    ));
    owner.bind_cluster(&cluster);
    let handler = owner.clone();
    let mut server = RpcServer::bind(
        args.data_bind,
        args.control_bind,
        token,
        transport,
        Arc::new(move |request: EngineRequest| handler.handle(request)),
    )?;
    event(
        &mut events,
        json!({"event":"server_ready","owner":args.owner,"data":server.data_addr(),"control":server.control_addr(),"deadline":args.shutdown_at_unix,"transport_capacity":transport_capacity}),
    )?;
    if args.mode == "serve" {
        while now() < args.shutdown_at_unix {
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
    let mut start = 0;
    let mut cycles = vec![];
    while start < args.rows {
        check_deadline(&args)?;
        let end = if start < args.transition {
            start
                .saturating_add(args.initial_batch)
                .min(args.transition)
                .min(args.rows)
        } else {
            start.saturating_add(args.later_batch).min(args.rows)
        };
        let ids = (start..end).collect::<Vec<_>>();
        let before_add = counts(&cluster)?;
        let rpc_before_add = rpc_accounting(&client, &server);
        let t = Instant::now();
        cluster.add_batch(&ids, args.workers)?;
        let add = t.elapsed().as_secs_f64();
        let rpc_after_add = rpc_accounting(&client, &server);
        let after_add = counts(&cluster)?;
        // Persist completed phase evidence before balancing can fail. Reuse the
        // existing boundary snapshots so observation adds no in-phase RPC work.
        event(
            &mut events,
            json!({"event":"add_complete","rows_before":start,"rows_after":end,"seconds":add,"before":before_add,"after":after_add,"rpc_before":rpc_before_add,"rpc_after":rpc_after_add}),
        )?;
        let before_balance = counts(&cluster)?;
        let kernel_before = chroma_index::spann::utils::clustering_profile_snapshot();
        let gate = ProfileGate::before(args.rows, start)?;
        event(
            &mut events,
            json!({"event":"balance_started","rows":end,"owners":before_balance}),
        )?;
        let rpc_before_balance = rpc_accounting(&client, &server);
        let producer_skips_before = cluster.producer_dependency_skips();
        let cpu_before_balance = hierarchical_index::distributed::metrics::cpu_seconds();
        let t = Instant::now();
        let balance_result = cluster.balance(args.workers);
        let balance = t.elapsed().as_secs_f64();
        let cpu_after_balance = hierarchical_index::distributed::metrics::cpu_seconds();
        let producer_skips_after = cluster.producer_dependency_skips();
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
                json!({"event":"balance_failed","rows":end,"seconds":failed_seconds,"scheduler_producer_dependencies":producer_dependencies,"error":error,"owners":owner_counts,"rpc":rpc_accounting(&client, &server)}),
            );
            return Err(error.into());
        }
        gate_result?;
        let kernel_after = chroma_index::spann::utils::clustering_profile_snapshot();
        let rpc_after_balance = rpc_accounting(&client, &server);
        let before_placement = counts(&cluster)?;
        event(
            &mut events,
            json!({"event":"balance_complete","rows":end,"seconds":balance,"scheduler_producer_dependencies":producer_dependencies,"coordinator_process_cpu_seconds":balance_cpu_seconds,"clustering_local_process_delta":kernel_after.delta(&kernel_before),"owners":before_placement,"rpc_before":rpc_before_balance,"rpc_after":rpc_after_balance}),
        )?;
        let budget = if start == 0 && args.first_owner_budget > 0 {
            args.first_owner_budget
        } else {
            args.owner_budget
        };
        let placement_statistics_before = placement_statistics(&cluster, args.run)?;
        let rpc_before_placement = rpc_accounting(&client, &server);
        let t = Instant::now();
        let placement_report = cluster.redistribute(BTreeMap::from([(1, budget), (2, budget)]))?;
        let placement = t.elapsed().as_secs_f64();
        let rpc_after_placement = rpc_accounting(&client, &server);
        let placement_statistics_after = placement_statistics(&cluster, args.run)?;
        let cycle = json!({"rows_before":start,"rows_after":end,"add_seconds":add,"balance_seconds":balance,"scheduler_producer_dependencies":producer_dependencies,"coordinator_balance_cpu_seconds":balance_cpu_seconds,"owner_count_snapshot_interval":"OwnerCounts CPU deltas include profile-gate waits and boundary observations; coordinator_balance_cpu_seconds surrounds timed balance directly","placement_seconds":placement,"placement_report":placement_plan_json(&placement_report),"placement_statistics":{"before":placement_statistics_before,"after":placement_statistics_after},"placement_byte_accounting":"Completed outgoing payload counters count each serialized handoff payload once at its source, excluding envelopes/framing; RPC phase snapshots separately count framed application bytes","vector_budget_per_owner":budget,"before_add":before_add,"after_add":after_add,"before_balance":before_balance,"clustering_local_process_delta":kernel_after.delta(&kernel_before),"before_placement":before_placement,"after_placement":counts(&cluster)?,"peer_transport":client.counters(),"rpc_phases":{"add":{"before":rpc_before_add,"after":rpc_after_add},"balance":{"before":rpc_before_balance,"after":rpc_after_balance},"placement":{"before":rpc_before_placement,"after":rpc_after_placement}},"rpc_accounting":rpc_accounting(&client,&server),"peak_rss":mem_probe::read_self().rss_peak});
        event(&mut events, json!({"event":"cycle_complete","cycle":cycle}))?;
        cycles.push(cycle);
        start = end;
    }
    let construction = build.elapsed().as_secs_f64();
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
    let result = json!({"status":"construction_complete_pending_durable_validation","rows":args.rows,"run":args.run.to_string(),"construction_seconds":construction,"construction_seconds_scope":"gross elapsed build, including external profile-gate waits and phase-boundary reporting; compare balance_seconds for measured construction work","settings":settings,"routing_shape":final_routing_shape,"routing_shape_seconds":shape_seconds,"cycles":cycles,"owners":final_counts,"placement_statistics":final_placement_statistics,"minimum_owner_share":min_share,"placement_acceptance":min_share>=0.30,"peer_transport":client.counters(),"rpc_accounting":rpc_accounting(&client,&server),"physical_distribution":"requires independently retained instance and process receipts","validation_complete":false});
    fs::write(
        args.output.join("construction.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    if min_share < 0.30 {
        return Err(
            "actual smaller owner holds below30%; benchmark placement requirement failed".into(),
        );
    }
    check_deadline(&args)?;
    let saved_root = args.output.join("saved-generation");
    let rpc_before_save = rpc_accounting(&client, &server);
    let save = Instant::now();
    let (saved, save_profile) = cluster.save_completed_profiled(
        &saved_root,
        descriptor.clone(),
        &format!("gen-{}", args.run),
        &format!("save-{}", args.run),
    )?;
    let save_seconds = save.elapsed().as_secs_f64();
    event(
        &mut events,
        json!({"event":"generation_saved","save_seconds":save_seconds,"save_profile":save_profile,"descriptor":saved,"store":saved_root,"peer_transport":client.counters(),"rpc_before_save":rpc_before_save,"rpc_after_save":rpc_accounting(&client,&server)}),
    )?;
    // The child reads immutable saved files only. Release the coordinator's live
    // writer and vector cache before its independent validation allocates memory.
    server.shutdown();
    let final_rpc_accounting = rpc_accounting(&client, &server);
    drop(server);
    drop(cluster);
    drop(owner);
    check_deadline(&args)?;
    let validation_output = args.output.join("durable-validation.json");
    let validation = Instant::now();
    let mut child = std::process::Command::new(
        reader_binary
            .as_ref()
            .ok_or("missing saved-reader executable")?,
    )
    .arg("--store")
    .arg(&saved_root)
    .arg("--expected-rows")
    .arg(args.rows.to_string())
    .arg("--output")
    .arg(&validation_output)
    .arg("--query-count")
    .arg(args.validation_queries.to_string())
    .stdout(std::process::Stdio::from(File::create(
        args.output.join("reader.stdout"),
    )?))
    .stderr(std::process::Stdio::from(File::create(
        args.output.join("reader.stderr"),
    )?))
    .spawn()?;
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
        event(
            &mut events,
            json!({"event":"durable_validation_failed","status":status.to_string(),"seconds":validation.elapsed().as_secs_f64()}),
        )?;
        return Err("fresh saved-reader process failed; benchmark is not admitted".into());
    }
    let validation_report: Value = serde_json::from_slice(&fs::read(&validation_output)?)?;
    if validation_report["status"] != "durable_reader_validated"
        || validation_report["rows"] != args.rows
        || validation_report["validated_posting_codes"] != args.rows
        || validation_report["exact_membership"] != true
        || validation_report["vector_sha256_verified"] != true
    {
        return Err("fresh-reader report lacks complete content validation".into());
    }
    let validation_seconds = validation.elapsed().as_secs_f64();
    let result = json!({"status":"construction_save_and_fresh_reader_validated","rows":args.rows,"run":args.run.to_string(),"construction_seconds":construction,"construction_seconds_scope":"gross elapsed build, including external profile-gate waits and phase-boundary reporting; compare balance_seconds for measured construction work","settings":settings,"routing_shape":final_routing_shape,"routing_shape_seconds":shape_seconds,"save_seconds":save_seconds,"save_profile":save_profile,"save_profile_scope":"coarse wall phases; owner and store sync times are nested; local_reuse bytes are logical verified payloads, not writes; immutable bytes count supplied payloads and completed temporary writes, not device IO","construction_and_save_seconds":construction+save_seconds,"fresh_reader_validation_seconds":validation_seconds,"cycles":cycles,"owners":final_counts,"placement_statistics":final_placement_statistics,"minimum_owner_share":min_share,"placement_acceptance":true,"peer_transport":client.counters(),"rpc_accounting":final_rpc_accounting,"physical_distribution":"requires independently retained instance and process receipts","save_adapter":"verified local owner objects reused in coordinator POSIX generation namespace; remote immutable owner files copied there; external durable S3 publication requires separate evidence","validation_complete":true,"validation_report":validation_output,"saved_descriptor":saved});
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
