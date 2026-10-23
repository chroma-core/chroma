#![recursion_limit = "256"]
#![allow(dead_code)]

mod hierarchical_index;

use std::path::PathBuf;

use chroma_blockstore::provider::BlockfileProvider;
use chroma_cache::new_cache_for_test;
use chroma_distance::DistanceFunction;
use chroma_storage::{local::LocalStorage, Storage};
use clap::Parser;
use hierarchical_index::config::HierarchicalSpannConfig;
use hierarchical_index::writer::{HierarchicalSpannIds, HierarchicalSpannWriter};
use serde::Deserialize;
use uuid::Uuid;

/// Verify that a saved index contains exactly the expected vector-ID prefix.
#[derive(Parser)]
struct Args {
    /// Saved index directory containing checkpoint.json and data/.
    directory: PathBuf,
    /// Expected live IDs are zero through this count minus one.
    vectors: usize,
}

#[derive(Deserialize)]
struct Checkpoint {
    total_vectors: usize,
    posting_list_id: Uuid,
    scalar_metadata_id: Uuid,
    vector_data_id: Uuid,
    leaf_node_id: Uuid,
    internal_node_id: Uuid,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let checkpoint: Checkpoint =
        serde_json::from_slice(&std::fs::read(args.directory.join("checkpoint.json"))?)?;
    if checkpoint.total_vectors != args.vectors {
        return Err(format!(
            "checkpoint contains {} vectors; expected {}",
            checkpoint.total_vectors, args.vectors
        )
        .into());
    }
    let data = args.directory.join("data");
    if !data.is_dir() {
        return Err("saved index data directory is missing".into());
    }
    let provider = BlockfileProvider::new_arrow(
        Storage::Local(LocalStorage::new(
            data.to_str().ok_or("index path is not UTF-8")?,
        )),
        3 * 1024 * 1024,
        new_cache_for_test(),
        new_cache_for_test(),
        4,
        0,
    );
    let ids = HierarchicalSpannIds {
        posting_list_id: checkpoint.posting_list_id,
        scalar_metadata_id: checkpoint.scalar_metadata_id,
        vector_data_id: checkpoint.vector_data_id,
        leaf_node_id: checkpoint.leaf_node_id,
        internal_node_id: checkpoint.internal_node_id,
    };
    let writer = HierarchicalSpannWriter::open(
        &provider,
        ids,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
    )
    .await
    .map_err(|err| format!("cannot open saved index: {err}"))?;
    writer
        .load_all_postings()
        .await
        .map_err(|err| format!("cannot read postings: {err}"))?;
    let reachable = writer.root_reachable_valid_ids()?;
    if reachable.len() != args.vectors || reachable.iter().any(|&id| id as usize >= args.vectors) {
        return Err(format!(
            "reachable current-version IDs do not equal the expected prefix: found {} IDs",
            reachable.len()
        )
        .into());
    }
    println!(
        "{}",
        serde_json::json!({"expected_vectors":args.vectors,"reachable_current_versions":reachable.len(),"prefix_valid":true})
    );
    Ok(())
}
