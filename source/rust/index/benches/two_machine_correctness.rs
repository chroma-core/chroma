//! A bounded structural fixture runs the real owner service on two hosts. Every
//! required transition has an explicit gate; missing coverage fails the fixture.
#![recursion_limit = "256"]
#![allow(unexpected_cfgs, dead_code)]
mod hierarchical_index;
use chroma_distance::DistanceFunction;
use clap::Parser;
use hierarchical_index::{
    common::ReadBeamPolicy,
    config::HierarchicalSpannConfig,
    distributed::{
        engine::*,
        placement_runtime::{PlacementRequest, PlacementResponse},
        protocol::handoff::HandoffPayload,
        routing::*,
        saved_reader::{CanonicalVectorSource, VectorFileDescriptor},
        transport::*,
    },
    mem_probe,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
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
    input_file: PathBuf,
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
    #[arg(long)]
    reader_binary: Option<PathBuf>,
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
fn event(file: &mut File, value: Value) -> Result<(), Box<dyn std::error::Error>> {
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
                _ => Err("invalid counts".into()),
            },
        )
        .collect()
}
fn leaves(cluster: &Cluster) -> Result<Vec<StoredNode>, String> {
    let mut out = vec![];
    for owner in 1..=2 {
        let nodes = match cluster.request(owner, EngineRequest::ListNodes)? {
            EngineResponse::Nodes(n) => n,
            _ => return Err("invalid node list".into()),
        };
        for target in nodes {
            let n = cluster.node(target)?;
            if n.leaf {
                match cluster.request(
                    owner,
                    EngineRequest::SnapshotNodes {
                        nodes: vec![target],
                    },
                )? {
                    EngineResponse::Snapshot(mut snapshot) => out.append(&mut snapshot),
                    _ => return Err("invalid leaf snapshot".into()),
                }
            }
        }
    }
    Ok(out)
}
fn shape(cluster: &Cluster) -> Result<Value, String> {
    let root = cluster.refresh_root()?;
    let mut pending = vec![(root.target, 0usize)];
    let mut seen = BTreeSet::new();
    let (mut internal, mut leaves, mut depth) = (0usize, 0usize, 0usize);
    while let Some((target, level)) = pending.pop() {
        if !seen.insert(target.id) {
            return Err("fixture topology repeats a node".into());
        }
        let node = cluster.node(target)?;
        depth = depth.max(level);
        if node.leaf {
            leaves += 1
        } else {
            internal += 1;
            pending.extend(node.children.into_iter().map(|c| (c.target, level + 1)));
        }
    }
    Ok(json!({"nodes":seen.len(),"internal_nodes":internal,"leaves":leaves,"maximum_depth":depth}))
}
fn policy(config: &HierarchicalSpannConfig) -> ReadBeamPolicy {
    ReadBeamPolicy::with_level_overrides(
        Some(config.write_beam_tau),
        config.write_beam_min,
        config.write_beam_max,
        config.write_level_taus.clone(),
        config.write_level_min_pcts.clone(),
        vec![],
    )
}
fn check_deadline(args: &Args) -> Result<(), String> {
    if now() >= args.shutdown_at_unix {
        Err("absolute correctness deadline reached".into())
    } else {
        Ok(())
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if ![1, 2].contains(&args.owner)
        || !matches!(args.mode.as_str(), "serve" | "run")
        || args.shutdown_at_unix <= now()
        || args.shutdown_at_unix - now() > 1800
    {
        return Err(
            "requires owner1/2, serve/run and fresh absolute deadline within30minutes".into(),
        );
    }
    if args.mode == "run" && args.owner != 1 {
        return Err("owner1 coordinates correctness".into());
    }
    if args.output.exists() {
        return Err("preserve previous fixture artifacts".into());
    }
    fs::create_dir_all(&args.output)?;
    let mut events = File::create(args.output.join("events.jsonl"))?;
    let reader_binary = if args.mode == "run" {
        let path = args
            .reader_binary
            .clone()
            .ok_or("--reader-binary must name a prebuilt executable")?;
        if !path.is_file() {
            return Err("prebuilt reader is absent".into());
        }
        Some(path)
    } else {
        None
    };
    // 100 initial points with split8/fanout4 force many leaves and internal branch
    // replacements. The second25-point interval exercises add/balance after real
    // placement. This is a mechanism fixture, never a throughput estimate.
    const ROWS: u64 = 125;
    const DIM: usize = 17;
    let mut bytes = vec![];
    for id in 0..ROWS {
        for d in 0..DIM {
            let x = (((id + 1) * (d as u64 + 13) * 7919) % 1009) as f32 / 1009.;
            bytes.extend_from_slice(&x.to_le_bytes())
        }
    }
    if args.input_file.exists() {
        if fs::read(&args.input_file)? != bytes {
            return Err("existing fixture corpus has different bytes".into());
        }
    } else {
        if let Some(parent) = args.input_file.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut f = File::create(&args.input_file)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    let descriptor = VectorFileDescriptor {
        path: args.input_file.to_str().ok_or("input path UTF8")?.into(),
        rows: ROWS,
        dimensions: DIM,
        stride_bytes: DIM as u64 * 4,
        offset_bytes: 0,
        first_id: 0,
        file_bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        immutable_locators: vec![
            "deterministic-fixture:(((id+1)*(dimension+13)*7919)%1009)/1009:f32le:v1".into(),
        ],
    };
    let vectors = Arc::new(CanonicalVectorSource::open(descriptor.clone())?.preload()?);
    let config = HierarchicalSpannConfig {
        split_threshold: 8,
        merge_threshold: 2,
        branching_factor: 4,
        ..Default::default()
    };
    let write_policy = policy(&config);
    let owner = Arc::new(OwnerService::new(
        args.owner,
        args.run,
        DIM,
        DistanceFunction::Euclidean,
        config,
        vectors.clone(),
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
    let token = fs::read_to_string(&args.token_file)?.trim().to_string();
    if token.len() < 32 {
        return Err("fixture token requires32characters".into());
    }
    let transport = TransportConfig::default();
    let peer = if args.owner == 1 { 2 } else { 1 };
    let client = RpcClient::new(
        args.peer_data,
        args.peer_control,
        token.clone(),
        transport.clone(),
    )?;
    let cluster = Arc::new(Cluster::new(
        owner.clone(),
        BTreeMap::from([(peer, client.clone())]),
        initial.routing.clone(),
    ));
    owner.bind_cluster(&cluster);
    let handler = owner.clone();
    let mut server = RpcServer::bind(
        args.data_bind,
        args.control_bind,
        token,
        transport,
        Arc::new(move |r: EngineRequest| handler.handle(r)),
    )?;
    event(
        &mut events,
        json!({"event":"server_ready","owner":args.owner,"data":server.data_addr(),"control":server.control_addr(),"input":descriptor,"rows":ROWS,"dimension":DIM,"split_threshold":8,"merge_threshold":2,"branching_factor":4,"deadline":args.shutdown_at_unix}),
    )?;
    if args.mode == "serve" {
        while now() < args.shutdown_at_unix {
            std::thread::sleep(Duration::from_millis(100));
        }
        server.shutdown();
        return Ok(());
    }
    let started = Instant::now();
    counts(&cluster)?;
    // A real destination reservation must reject excess receiving capacity, leave
    // no membership change and permit subsequent ordinary construction/save.
    match cluster.request(
        2,
        EngineRequest::Placement(PlacementRequest::Configure {
            run: args.run,
            vector_capacity: 1,
            byte_capacity: 1,
        }),
    )? {
        EngineResponse::Placement(PlacementResponse::Ack) => {}
        _ => return Err("capacity config ack absent".into()),
    }
    let envelope = owner.envelope("capacity-rejection", vec![])?;
    let rejected = cluster.request(
        2,
        EngineRequest::Placement(PlacementRequest::Stage {
            envelope: envelope.clone(),
            payload: HandoffPayload {
                root: u128::MAX - 10,
                source: 1,
                destination: 2,
                old_placement: 0,
                new_placement: 1,
                vectors: 2,
                bytes: 1,
                manifest_digest: [0; 32],
            },
            chunks: 1,
        }),
    );
    let capacity_error = rejected.err().ok_or("excess capacity was accepted")?;
    if !capacity_error.to_ascii_lowercase().contains("capacity") {
        return Err(format!("expected capacity rejection, got {capacity_error}").into());
    }
    event(
        &mut events,
        json!({"event":"capacity_rejection","error":capacity_error,"counts":counts(&cluster)?}),
    )?;
    cluster.add_batch(&(0..100).collect::<Vec<_>>(), 4)?;
    cluster.balance(4)?;
    let first_root = cluster.refresh_root()?;
    if first_root.target.id == initial.routing.target.id || first_root.leaf {
        return Err("fixture missed root boundary replacement".into());
    }
    let initial_shape = shape(&cluster)?;
    if initial_shape["maximum_depth"].as_u64().unwrap_or(0) < 2 {
        return Err("fixture missed multi-level internal branching".into());
    }
    event(
        &mut events,
        json!({"event":"initial_tree_built","root_id":first_root.target.id.to_string(),"shape":initial_shape,"counts":counts(&cluster)?}),
    )?;
    check_deadline(&args)?;
    let placement = cluster.redistribute(BTreeMap::from([(1, 80), (2, 80)]))?;
    if placement.moves.is_empty() || placement.final_vectors.values().any(|n| *n == 0) {
        return Err("fixture missed whole-subtree handoff or occupied both owners".into());
    }
    let after_handoff = counts(&cluster)?;
    event(
        &mut events,
        json!({"event":"handoff_complete","moves":placement.moves.len(),"shared_promotions":placement.promoted_shared.len(),"counts":after_handoff}),
    )?;
    cluster.add_batch(&(100..ROWS).collect::<Vec<_>>(), 4)?;
    let after_add = counts(&cluster)?;
    if after_add[1].vectors <= after_handoff[1].vectors
        || after_add[1].expansions <= after_handoff[1].expansions
    {
        return Err("fixture missed posthandoff remote add or remote navigation".into());
    }
    cluster.balance(4)?;
    event(
        &mut events,
        json!({"event":"posthandoff_construction","after_add":after_add,"after_balance":counts(&cluster)?}),
    )?;
    check_deadline(&args)?;
    // Isolate a merge whose first reference-policy neighbor is on the other host.
    // Evacuate its current postings, then put one suitable destination vector into
    // it. A single balancing job must transfer that vector back and remove source.
    let current = leaves(&cluster)?;
    let mut selected = None;
    for source in &current {
        let nearest = cluster
            .navigate(
                &[source.routing.centroid.clone()],
                &write_policy,
                Some(source.routing.target.id),
            )?
            .pop()
            .unwrap_or_default();
        let Some((destination, _)) = nearest.first() else {
            continue;
        };
        if destination.authority == source.routing.target.authority {
            continue;
        }
        let Some(target) = current
            .iter()
            .find(|n| n.routing.target.id == destination.id)
        else {
            continue;
        };
        let Some(postings) = &target.postings else {
            continue;
        };
        for id in &postings.ids {
            let v = vectors.get(*id)?;
            if DistanceFunction::Euclidean.distance(&v, &target.routing.centroid)
                < DistanceFunction::Euclidean.distance(&v, &source.routing.centroid)
            {
                selected = Some((source.clone(), target.clone(), *id));
                break;
            }
        }
        if selected.is_some() {
            break;
        }
    }
    let (source, destination, probe) = selected
        .ok_or("fixture has no isolated cross-owner merge candidate; coverage gate fails")?;
    let source_ref = source.routing.target;
    let destination_ref = destination.routing.target;
    let evacuated = source
        .postings
        .as_ref()
        .ok_or("selected source has no postings")?
        .ids
        .clone();
    if evacuated.is_empty() {
        return Err("selected source is already empty".into());
    }
    cluster.transfer(source_ref, destination_ref, evacuated.clone())?;
    cluster.transfer(destination_ref, source_ref, vec![probe])?;
    cluster.balance_leaf_once_for_correctness(source_ref)?;

    if cluster.node(source_ref).is_ok() {
        return Err("cross-owner merge did not remove its source leaf".into());
    }
    let after_merge = leaves(&cluster)?;
    let probe_owner = after_merge
        .iter()
        .filter(|n| n.postings.as_ref().is_some_and(|p| p.ids.contains(&probe)))
        .map(|n| n.routing.target.authority)
        .collect::<Vec<_>>();
    if probe_owner != vec![destination_ref.authority] {
        return Err("merge did not preserve the singleton on the remote owner".into());
    }
    event(
        &mut events,
        json!({"event":"cross_owner_merge","source":source_ref.id.to_string(),"destination":destination_ref.id.to_string(),"probe_vector":probe,"evacuated_vectors":evacuated.len(),"counts":counts(&cluster)?}),
    )?;
    cluster.balance(4)?;
    check_deadline(&args)?;
    let final_counts = counts(&cluster)?;
    if final_counts.iter().any(|c| c.vectors == 0)
        || final_counts.iter().map(|c| c.vectors).sum::<u64>() != ROWS
    {
        return Err("final owners are empty or vector counts changed".into());
    }
    let final_shape = shape(&cluster)?;
    let save_path = args.output.join("saved-generation");
    let saved = cluster.save_completed(
        &save_path,
        descriptor,
        &format!("correctness-{}", args.run),
        &format!("save-{}", args.run),
    )?;
    let wire = client.counters();
    server.shutdown();
    drop(server);
    drop(cluster);
    drop(owner);
    drop(vectors);
    let validation_path = args.output.join("durable-validation.json");
    let mut child = std::process::Command::new(reader_binary.ok_or("reader absent")?)
        .arg("--store")
        .arg(&save_path)
        .arg("--expected-rows")
        .arg(ROWS.to_string())
        .arg("--output")
        .arg(&validation_path)
        .arg("--query-count")
        .arg("16")
        .arg("--beam")
        .arg("4")
        .stdout(std::process::Stdio::from(File::create(
            args.output.join("reader.stdout"),
        )?))
        .stderr(std::process::Stdio::from(File::create(
            args.output.join("reader.stderr"),
        )?))
        .spawn()?;
    let status = loop {
        if let Some(s) = child.try_wait()? {
            break s;
        }
        if now() >= args.shutdown_at_unix {
            let _ = child.kill();
            let _ = child.wait();
            return Err("freshreader hit absolute deadline".into());
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    if !status.success() {
        return Err("freshreader failed; preserve artifacts".into());
    }
    let validation: Value = serde_json::from_slice(&fs::read(validation_path)?)?;
    if validation["status"] != "durable_reader_validated"
        || validation["validated_posting_codes"] != ROWS
        || validation["exact_membership"] != true
        || validation["vector_sha256_verified"] != true
    {
        return Err("freshreader omitted a required correctness check".into());
    }
    let result = json!({"status":"all_required_mechanisms_validated","rows":ROWS,"dimension":DIM,"initial_rows":100,"posthandoff_adds":25,"root_replacement":true,"whole_branch_handoff":true,"remote_navigation":true,"remote_posthandoff_add":true,"cross_owner_posting_transfer":true,"cross_owner_merge":true,"capacity_rejection":true,"durable_freshreader":true,"owners":final_counts,"initial_shape":initial_shape,"final_shape":final_shape,"saved_descriptor":saved,"transport":wire,"elapsed_seconds":started.elapsed().as_secs_f64(),"peak_rss":mem_probe::read_self().rss_peak,"scope":"structural correctness only; physical hosts require independent instance/process receipts"});
    fs::write(
        args.output.join("result.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    event(
        &mut events,
        json!({"event":"fixture_validated","result":result}),
    )?;
    Ok(())
}
