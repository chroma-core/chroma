//! One retained-input construction comparison. Both arms use this source.
//! Build with --cfg single_shard_owner for the owner facade; otherwise use the
//! unsharded writer. Run one process per arm with identical arguments/settings.
#![recursion_limit = "256"]
#![allow(unexpected_cfgs)]
#[allow(dead_code)]
mod hierarchical_index;

use chroma_blockstore::{arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider};
use chroma_cache::new_cache_for_test;
use chroma_distance::DistanceFunction;
use chroma_index::quantization::{Code, PackedFourBitCentroid};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::hierarchical_spann::{
    HierarchicalInternalNode, HierarchicalLeafNode, HierarchicalSpannPostingList,
};
use clap::Parser;
use hierarchical_index::{
    common::ReadBeamPolicy,
    config::HierarchicalSpannConfig,
    mem_probe,
    persistance::*,
    reader::HierarchicalSpannReader,
    writer::{HierarchicalSpannIds, HierarchicalSpannWriter},
};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
struct Args {
    /// Retained row-major little-endian f32 input. No network access is performed.
    #[arg(long)]
    input: PathBuf,
    /// New directory for this execution's blockfiles and result JSON.
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long, default_value_t = 100_000)]
    rows: usize,
    #[arg(long, default_value_t = 768)]
    dim: usize,
    #[arg(long, default_value_t = 32)]
    workers: usize,
    /// External manifest's SHA256; independently verified by the operator.
    #[arg(long)]
    input_sha256: String,
}

struct Clock {
    wall: Instant,
    cpu: Option<f64>,
}
fn cpu_seconds() -> Option<f64> {
    // Linux reports cumulative process CPU ticks. Read the final ')' because
    // the process name may contain spaces or parentheses.
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields: Vec<_> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    let user: f64 = fields.get(11)?.parse().ok()?;
    let system: f64 = fields.get(12)?.parse().ok()?;
    static HZ: std::sync::OnceLock<Option<f64>> = std::sync::OnceLock::new();
    let hz = HZ.get_or_init(|| {
        let output = std::process::Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .ok()?;
        String::from_utf8(output.stdout).ok()?.trim().parse().ok()
    });
    Some((user + system) / (*hz)?)
}
impl Clock {
    fn start() -> Self {
        Self {
            wall: Instant::now(),
            cpu: cpu_seconds(),
        }
    }
    fn finish(self) -> Value {
        let seconds = self.wall.elapsed().as_secs_f64();
        let cpu = self.cpu.zip(cpu_seconds()).map(|(a, b)| b - a);
        let memory = mem_probe::read_self();
        json!({"wall_seconds": seconds, "process_cpu_seconds": cpu,
            "effective_cpu_cores": cpu.map(|v| v/seconds), "rss_bytes": memory.rss,
            "process_peak_rss_bytes": memory.rss_peak})
    }
}
fn check(condition: bool, message: impl Into<String>) -> Result<(), Box<dyn std::error::Error>> {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message.into()).into())
    }
}
fn load(args: &Args) -> Result<Vec<(u32, Arc<[f32]>)>, Box<dyn std::error::Error>> {
    let expected = args
        .rows
        .checked_mul(args.dim)
        .and_then(|n| n.checked_mul(4))
        .ok_or("input length overflow")?;
    check(
        std::fs::metadata(&args.input)?.len() == expected as u64,
        "input byte length differs from rows * dimensions * 4",
    )?;
    let mut input = BufReader::new(std::fs::File::open(&args.input)?);
    let mut row = vec![0u8; args.dim * 4];
    let mut vectors = Vec::with_capacity(args.rows);
    for id in 0..args.rows {
        input.read_exact(&mut row)?;
        let vector: Vec<_> = row
            .chunks_exact(4)
            .map(|v| f32::from_le_bytes(v.try_into().unwrap()))
            .collect();
        check(
            vector.iter().all(|v| v.is_finite()),
            format!("input {id} contains a nonfinite coordinate"),
        )?;
        vectors.push((u32::try_from(id)?, Arc::from(vector)));
    }
    Ok(vectors)
}

/// Read persisted records after the writer has been dropped. This validates
/// global reachability, exact-once membership, vector bits, and posting codes.
async fn validate(
    provider: &BlockfileProvider,
    ids: &HierarchicalSpannIds,
    vectors: &[(u32, Arc<[f32]>)],
    dim: usize,
) -> Result<Value, Box<dyn std::error::Error>> {
    let metadata = provider
        .read::<u32, u32>(BlockfileReaderOptions::new(
            ids.scalar_metadata_id,
            "".into(),
        ))
        .await?;
    check(
        metadata.get(PREFIX_FORMAT, SINGLETON_KEY).await? == Some(VERSION_FREE_FORMAT),
        "wrong membership format",
    )?;
    check(
        metadata
            .get(PREFIX_NAVIGATION_FORMAT, SINGLETON_KEY)
            .await?
            == Some(PACKED_NAVIGATION_FORMAT),
        "reader centroid format is not persisted four-bit",
    )?;
    let root = metadata
        .get(PREFIX_ROOT, SINGLETON_KEY)
        .await?
        .ok_or("missing root")?;
    let leaves = provider
        .read::<u32, HierarchicalLeafNode<'static>>(BlockfileReaderOptions::new(
            ids.leaf_node_id,
            "".into(),
        ))
        .await?;
    let internals = provider
        .read::<u32, HierarchicalInternalNode<'static>>(BlockfileReaderOptions::new(
            ids.internal_node_id,
            "".into(),
        ))
        .await?;
    let postings = provider
        .read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
            ids.posting_list_id,
            "".into(),
        ))
        .await?;
    let data = provider
        .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(ids.vector_data_id, "".into()))
        .await?;
    let mut nodes: HashMap<u32, (u32, Vec<u32>, Option<usize>)> = HashMap::new();
    let mut seen_vectors = vec![false; vectors.len()];
    let mut posting_rows = 0;
    let zero = vec![0.0; dim];
    for (_, id, leaf) in leaves.get_range(""..="", ..).await? {
        PackedFourBitCentroid::from_bytes(leaf.centroid_code, dim)?;
        let centroid = data
            .get(PREFIX_CENTROID, id)
            .await?
            .ok_or("missing leaf centroid")?;
        check(
            centroid.len() == dim && centroid.iter().all(|v| v.is_finite()),
            "invalid leaf centroid",
        )?;
        let expected_code = Code::<4>::quantize(centroid, &zero);
        check(
            PackedFourBitCentroid::from_origin_code(&expected_code, dim).to_bytes()
                == leaf.centroid_code,
            "saved centroid scoring code mismatch",
        )?;
        check(
            nodes
                .insert(id, (leaf.parent, vec![], Some(leaf.length as usize)))
                .is_none(),
            "duplicate node",
        )?;
        let posting = postings.get("", id).await?.ok_or("missing posting list")?;
        check(
            posting.ids.len() == leaf.length as usize
                && posting.codes.len() == posting.ids.len() * Code::<1>::size(dim),
            "posting length mismatch",
        )?;
        for (&vector_id, code) in posting
            .ids
            .iter()
            .zip(posting.codes.chunks_exact(Code::<1>::size(dim)))
        {
            let index = vector_id as usize;
            check(index < vectors.len(), "unexpected vector ID")?;
            check(
                !seen_vectors[index],
                format!("duplicate membership {vector_id}"),
            )?;
            seen_vectors[index] = true;
            let embedding = data
                .get(PREFIX_EMBEDDING, vector_id)
                .await?
                .ok_or("missing full vector")?;
            check(
                embedding.len() == dim
                    && embedding
                        .iter()
                        .zip(vectors[index].1.iter())
                        .all(|(a, b)| a.to_bits() == b.to_bits()),
                "full vector differs from input",
            )?;
            check(
                Code::<1>::quantize(embedding, centroid).as_ref() == code,
                "one-bit posting code differs from vector and leaf centroid",
            )?;
            posting_rows += 1;
        }
    }
    let leaf_count = nodes.len();
    for (_, id, node) in internals.get_range(""..="", ..).await? {
        PackedFourBitCentroid::from_bytes(node.centroid_code, dim)?;
        let centroid = data
            .get(PREFIX_CENTROID, id)
            .await?
            .ok_or("missing internal centroid")?;
        check(
            centroid.len() == dim && centroid.iter().all(|v| v.is_finite()),
            "invalid internal centroid",
        )?;
        let expected_code = Code::<4>::quantize(centroid, &zero);
        check(
            PackedFourBitCentroid::from_origin_code(&expected_code, dim).to_bytes()
                == node.centroid_code,
            "saved internal scoring code mismatch",
        )?;
        check(
            nodes
                .insert(id, (node.parent, node.children.to_vec(), None))
                .is_none(),
            "node is both leaf and internal",
        )?;
    }
    check(
        seen_vectors.iter().all(|v| *v),
        "saved tree is missing vectors",
    )?;
    check(
        nodes.get(&root).map(|n| n.0) == Some(NO_PARENT),
        "root parent is invalid",
    )?;
    let mut visited = HashSet::new();
    let mut queue = vec![(root, 0)];
    let mut levels = Vec::<usize>::new();
    while let Some((id, depth)) = queue.pop() {
        check(visited.insert(id), "cycle or duplicate child link")?;
        let node = nodes.get(&id).ok_or("dangling child")?;
        if levels.len() <= depth {
            levels.resize(depth + 1, 0);
        }
        levels[depth] += 1;
        for &child in &node.1 {
            check(
                nodes.get(&child).map(|n| n.0) == Some(id),
                "child parent differs from parent link",
            )?;
            queue.push((child, depth + 1));
        }
    }
    check(visited.len() == nodes.len(), "orphaned saved nodes")?;
    Ok(
        json!({"exact_once_vectors":posting_rows,"leaves":leaf_count,"internal_nodes":nodes.len()-leaf_count,
        "levels":levels,"one_bit_posting_codes_recomputed":true,"full_vector_bits_checked":true,
        "saved_four_bit_centroid_codes_checked":true,"writer_retired":true}),
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    check(
        args.rows > 0 && args.dim > 0 && args.workers > 0,
        "rows, dimension and workers must be positive",
    )?;
    check(
        !args.output_dir.exists(),
        "output directory already exists; preserve old attempts and use a new path",
    )?;
    std::fs::create_dir_all(&args.output_dir)?;
    let total = Clock::start();
    let mut phases = serde_json::Map::new();
    let load_time = Clock::start();
    let vectors = load(&args)?;
    phases.insert("input_load".into(), load_time.finish());
    let config = HierarchicalSpannConfig::default();
    let provider = BlockfileProvider::new_arrow(
        Storage::Local(LocalStorage::new(
            args.output_dir
                .join("blocks")
                .to_str()
                .ok_or("non-UTF8 output path")?,
        )),
        16 * 1024 * 1024,
        new_cache_for_test(),
        new_cache_for_test(),
        args.workers,
        0,
    );
    let build_start = Clock::start();
    let init = Clock::start();
    #[cfg(single_shard_owner)]
    let mut writer = hierarchical_index::owner::LocalSubtreeOwner::new(
        0,
        args.dim,
        DistanceFunction::Euclidean,
        config.clone(),
    );
    #[cfg(not(single_shard_owner))]
    let mut writer =
        HierarchicalSpannWriter::new(args.dim, DistanceFunction::Euclidean, config.clone());
    phases.insert("initialize".into(), init.finish());
    let add = Clock::start();
    let add_parts = writer.add_batch_buffered(&vectors, args.workers, || {})?;
    phases.insert("add".into(), add.finish());
    let balance = Clock::start();
    writer.balance_index_parallel(args.workers)?;
    phases.insert("balance".into(), balance.finish());
    #[cfg(single_shard_owner)]
    let engine = writer.engine();
    #[cfg(not(single_shard_owner))]
    let engine = &writer;
    let stats = engine.stats.snapshot();
    #[cfg(single_shard_owner)]
    let coordination = json!({"available":true,"counters":engine.coordination_counters()});
    #[cfg(not(single_shard_owner))]
    let coordination = json!({"available":false,"reason":"reference has no equivalent reservation and scheduler counters"});
    let shape = json!({"depth":engine.depth(),"level_node_counts":engine.level_node_counts(),"leaf_sizes":engine.leaf_sizes(),"total_vectors":engine.total_vectors()});
    let work = json!({"task_calls":stats.calls,"task_elapsed_sum_nanos":stats.nanos,"navigate_distances":stats.navigate_dist_count,
        "navigate_levels":stats.navigate_levels,"split_sizes":stats.split_sizes,"split_substep_nanos":stats.split_substeps,
        "reassign_substep_nanos":stats.reassign_substeps,"register_substep_nanos":stats.register_substeps,
        "posting_loads":stats.posting_loads,"posting_load_entries":stats.posting_load_entries,"embedding_loads":stats.embedding_loads,
        "claim_conflicts":stats.claim_conflicts,"transferred_vectors":stats.transferred_vectors,"transfer_batches":stats.transfer_batches,
        "transfer_source_entries_scanned":stats.transfer_source_entries_scanned,"transfer_retries":stats.transfer_retries,
        "navigate_missing_nodes":stats.navigate_missing_nodes,"balance_rounds":stats.balance_rounds,
        "balance_iteration_diagnostics":format!("{:?}",stats.balance_iterations)});
    let commit = Clock::start();
    let flusher = Box::pin(writer.commit(&provider, None)).await?;
    phases.insert("commit".into(), commit.finish());
    let flush = Clock::start();
    let ids = Box::pin(flusher.flush()).await?;
    phases.insert("flush".into(), flush.finish());
    let build_total = build_start.finish();
    let retire = Clock::start();
    drop(writer);
    phases.insert("retire_writer".into(), retire.finish());
    let reopen = Clock::start();
    let reader = HierarchicalSpannReader::open(
        &provider,
        ids.clone(),
        DistanceFunction::Euclidean,
        config.clone(),
    )
    .await?;
    phases.insert("reader_open".into(), reopen.finish());
    let validation = Clock::start();
    let checks = validate(&provider, &ids, &vectors, args.dim).await?;
    phases.insert("validation".into(), validation.finish());
    let query = Clock::start();
    let (results, scanned, leaves, _) = reader
        .search_with_policy_lazy(&vectors[0].1, 10, 2, &ReadBeamPolicy::uniform(None, 32, 32))
        .await?;
    check(
        !results.is_empty()
            && results
                .iter()
                .all(|(id, d)| (*id as usize) < vectors.len() && d.is_finite()),
        "query smoke returned invalid results",
    )?;
    phases.insert("query_smoke".into(), query.finish());
    let result = json!({"arm":if cfg!(single_shard_owner){"single_shard_owner"}else{"unsharded_reference"},"input":args.input,"input_sha256_operator_verified":args.input_sha256,
        "rows":args.rows,"dimensions":args.dim,"workers":args.workers,"metric":"squared_l2","add_batch_rows":args.rows,
        "settings":{"branching_factor":config.branching_factor,"split_threshold":config.split_threshold,"merge_threshold":config.merge_threshold,
            "write_beam_tau":config.write_beam_tau,"write_beam_min":config.write_beam_min,"write_beam_max":config.write_beam_max,
            "reassign_neighbor_count":config.reassign_neighbor_count,"fp_npa":config.fp_npa,"construction_navigation":"full_precision","posting_bits":1,"reader_centroid_bits":4},
        "randomness":"kmeans random initialization and thread scheduling are not seed-controlled; work counts accompany timing",
        "cache":"fresh process/new index; OS input page-cache state controlled externally; validation may warm provider cache before query smoke",
        "phases":phases,"build_and_save_total":build_total,"total":total.finish(),"shape":shape,"work":work,"coordination":coordination,"validation":checks,
        "add_breakdown":{"preflight_seconds":add_parts.preflight.total.as_secs_f64(),"initialize_seconds":add_parts.initialization.as_secs_f64(),"collect_seconds":add_parts.collection.as_secs_f64(),"flush_seconds":add_parts.flush.as_secs_f64()},
        "query_smoke":{"reused_input_query":true,"quality_comparison":false,"scanned_vectors":scanned,"leaves":leaves,"results":results},
        "remote_execution":false,"cross_machine_messages":0,"ownership_redistributions":0,
        "limitations":["one machine and one owner only","query smoke is not held-out recall evidence","effective CPU cores includes runtime work and is not a direct worker-occupancy measurement","task durations overlap and must not be summed into wall time"]});
    let json = serde_json::to_string_pretty(&result)?;
    std::fs::write(args.output_dir.join("result.json"), &json)?;
    println!("{json}");
    Ok(())
}
