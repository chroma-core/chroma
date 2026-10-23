//! Fresh-process validation consumes only saved files and an independently
//! verified immutable vector corpus; it starts no writer service.
#![recursion_limit = "256"]
#![allow(unexpected_cfgs, dead_code)]
mod hierarchical_index;
use clap::Parser;
use hierarchical_index::{
    common::ReadBeamPolicy,
    distributed::{
        generation::{GenerationDescriptor, GenerationStore},
        generation_backend::DurableStoreConfig,
        saved_reader::{SavedReader, VectorSource},
    },
    mem_probe,
};
use serde_json::json;
use std::{
    fs::{self, File},
    io::Write,
    path::PathBuf,
    time::Instant,
};
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
#[derive(Parser)]
struct Args {
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
    #[arg(long)]
    expected_rows: u64,
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
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.output.exists() {
        return Err("validation output already exists; preserve prior attempt".into());
    }
    if args.expected_rows == 0 || args.query_count == 0 || args.beam == 0 || args.k == 0 {
        return Err("positive validation bounds are required".into());
    }
    let begin = Instant::now();
    let store = if let Some(path) = &args.storage_config {
        GenerationStore::open_durable(&serde_json::from_slice::<DurableStoreConfig>(&fs::read(
            path,
        )?)?)?
    } else {
        GenerationStore::open(&args.store)?
    };
    let descriptor: GenerationDescriptor = if let Some(path) = args.descriptor {
        serde_json::from_slice(&fs::read(path)?)?
    } else {
        store.latest()?.ok_or("saved generation is absent")?
    };
    let reader = SavedReader::open(store, &descriptor)?;
    if reader.manifest.total_vectors != args.expected_rows {
        return Err("saved generation count differs from expected workload".into());
    }
    let t = Instant::now();
    let vectors = reader
        .open_vector_source_at(args.vector_path.as_deref())?
        .preload()?;
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
