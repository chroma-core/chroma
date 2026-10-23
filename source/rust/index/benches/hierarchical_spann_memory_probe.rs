//! Read-only, phase-barrier diagnostic; this is not a latency benchmark.
mod hierarchical_index;
#[path = "sharding/memory_probe/cache.rs"]
mod observed;
use anyhow::{bail, Context, Result};
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::FoyerCacheConfig;
use chroma_distance::DistanceFunction;
use chroma_storage::{local::LocalStorage, Storage};
use hierarchical_index::{
    config::HierarchicalSpannConfig,
    reader::{HierarchicalSpannReader, StreamingLimits, WorkingSetLimits},
    writer::persistence::HierarchicalSpannIds,
};
use observed::{Ledger, Observed};
use serde_json::{json, Value};
use std::{
    io::{BufRead, Write},
    path::Path,
    sync::Arc,
};

fn estimated<V: chroma_cache::foyer::Code>(value: &V) -> u64 {
    value.estimated_size() as u64
}
fn error(e: Box<dyn chroma_error::ChromaError>) -> anyhow::Error {
    anyhow::anyhow!(e.to_string())
}
async fn open(
    p: &BlockfileProvider,
    dir: &Path,
    generation: u64,
) -> Result<HierarchicalSpannReader> {
    let m: Value = serde_json::from_slice(&std::fs::read(
        dir.join(format!("manifest-{generation}.json")),
    )?)?;
    anyhow::ensure!(
        m["dimension"] == 4
            && m["metric"] == "l2"
            && m["vector_count"].as_u64().unwrap_or(1000) <= 16,
        "fixture only"
    );
    let b = &m["block_ids"];
    let id =
        |name: &str| -> Result<uuid::Uuid> { Ok(b[name].as_str().context("block UUID")?.parse()?) };
    HierarchicalSpannReader::open(
        p,
        HierarchicalSpannIds {
            posting_list_id: id("posting_list_id")?,
            scalar_metadata_id: id("scalar_metadata_id")?,
            vector_data_id: id("vector_data_id")?,
            leaf_node_id: id("leaf_node_id")?,
            internal_node_id: id("internal_node_id")?,
        },
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
    )
    .await
    .map_err(error)
}
fn leaves(reader: &HierarchicalSpannReader) -> Result<Vec<u32>> {
    let root = reader.describe_root().map_err(error)?;
    let mut frontier = vec![root.node_id];
    let mut result = Vec::new();
    if root.is_leaf {
        return Ok(frontier);
    }
    for _ in 0..16 {
        let children = reader
            .expand_nodes(&[4.5, 1., 0., 0.], &frontier)
            .map_err(error)?;
        frontier.clear();
        for child in children {
            if child.is_leaf {
                result.push(child.node_id)
            } else {
                frontier.push(child.node_id)
            }
        }
        if frontier.is_empty() {
            result.sort_unstable();
            anyhow::ensure!(result.len() <= 16, "leaf bound");
            return Ok(result);
        }
    }
    bail!("depth bound")
}
fn memory(reader: &HierarchicalSpannReader) -> Value {
    let m = reader.memory_usage();
    json!({"tree_payload_bytes":m.tree_bytes,"centroid_payload_bytes":m.centroid_bytes,"posting_entries":m.posting_entries,"posting_payload_bytes":m.posting_bytes,"embedding_entries":m.embedding_count,"embedding_payload_bytes":m.embedding_bytes,"reader_pins":reader.loaded_blocks_stats()})
}
async fn score(reader: &mut HierarchicalSpannReader, ids: &[u32], mode: &str) -> Result<Value> {
    let q = [4.5, 1., 0., 0.];
    let (hits, unique, metrics) = if mode == "stream" {
        let (h, u, m) = reader
            .score_selected_leaves_streaming_with_candidates(
                &q,
                ids,
                1,
                16,
                StreamingLimits {
                    max_leaf_entries: 16,
                    max_total_entries: 64,
                    max_total_posting_bytes: 4096,
                    max_unique_candidates: 16,
                },
                Some(WorkingSetLimits {
                    max_retained_entries: 16,
                    max_staging_entries: 1,
                    max_cutoff_ties: 16,
                    max_output_entries: 16,
                    max_seen_bitmap_bytes: 2,
                }),
            )
            .await
            .map_err(error)?;
        (h, u, serde_json::to_value(m)?)
    } else {
        let (h, u) = reader
            .score_selected_leaves(&q, ids, 1)
            .await
            .map_err(error)?;
        (h, u, Value::Null)
    };
    let before_clear = memory(reader);
    if mode == "evict" {
        reader.clear_loaded_postings();
    }
    reader.clear_loaded_blocks();
    Ok(
        json!({"hits":hits,"unique":unique,"returned_vec_capacity":hits.capacity(),"returned_vec_capacity_payload_bytes":hits.capacity()*std::mem::size_of::<(u32,f32)>(),"streaming":metrics,"before_optional_clear":before_clear}),
    )
}
fn emit(value: Value) -> Result<()> {
    println!("{}", serde_json::to_string(&value)?);
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    anyhow::ensure!(
        line.trim() == "continue",
        "controller acknowledgement required"
    );
    Ok(())
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    anyhow::ensure!(args.len() == 3, "directory mode");
    let dir = Path::new(&args[1]);
    let mode = &args[2];
    anyhow::ensure!(
        ["retain", "evict", "stream"].contains(&mode.as_str()),
        "mode"
    );
    let blocks = Observed {
        inner: Arc::from(
            FoyerCacheConfig {
                name: "diagnostic_blocks".into(),
                capacity: 256,
                ..Default::default()
            }
            .build_memory_persistent()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
        ),
        ledger: Arc::new(Ledger::default()),
        size: estimated,
    };
    let roots = Observed {
        inner: Arc::from(
            FoyerCacheConfig {
                name: "diagnostic_roots".into(),
                capacity: 4096,
                ..Default::default()
            }
            .build_memory_persistent()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
        ),
        ledger: Arc::new(Ledger::default()),
        size: estimated,
    };
    let provider = BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
        Storage::Local(LocalStorage::new(
            dir.join("data").to_str().context("UTF8")?,
        )),
        3 * 1024 * 1024,
        Box::new(blocks.clone()),
        Box::new(roots.clone()),
        16,
    ));
    macro_rules! phase {($name:expr,$readers:expr,$work:expr)=>{{emit(json!({"phase":$name,"mode":mode,"readers":$readers,"work":$work,"provider_blocks":blocks.census().await?,"provider_roots":roots.census().await?,"census_touches_recency":true}))?;}};}
    phase!("baseline", json!([]), Value::Null);
    let mut old = open(&provider, dir, 1).await?;
    let ids = leaves(&old)?;
    phase!("open_gen1", json!([memory(&old)]), json!({"leaves":ids}));
    for n in 1..=ids.len() {
        let work = score(&mut old, &ids[..n], mode).await?;
        phase!(format!("progressive_{n}"), json!([memory(&old)]), work);
    }
    for n in 0..2 {
        let work = score(&mut old, &ids, mode).await?;
        phase!(format!("repeat_{n}"), json!([memory(&old)]), work);
    }
    let exact = old
        .rerank_selected(&[4.5, 1., 0., 0.], &(0..8).collect::<Vec<_>>())
        .await
        .map_err(error)?;
    old.clear_loaded_blocks();
    phase!("rerank", json!([memory(&old)]), json!({"exact":exact}));
    let mut new = open(&provider, dir, 2).await?;
    let next = leaves(&new)?;
    let work = score(&mut new, &next, mode).await?;
    phase!(
        "both_generations",
        json!([memory(&old), memory(&new)]),
        work
    );
    drop(old);
    phase!("drop_gen1", json!([memory(&new)]), Value::Null);
    drop(new);
    phase!("drop_all_readers", json!([]), Value::Null);
    provider
        .clear()
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    phase!("clear_provider", json!([]), Value::Null);
    Ok(())
}
