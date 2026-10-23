#![recursion_limit = "256"]
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
mod hierarchical_index;
#[allow(dead_code)]
mod datasets;
use std::{path::Path, time::Instant};
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::{new_cache_for_test, FoyerCacheConfig};
use chroma_distance::DistanceFunction;
use chroma_storage::{local::LocalStorage, Storage};
use hierarchical_index::{reader::HierarchicalSpannReader, writer::{HierarchicalSpannIds, HierarchicalSpannWriter}, config::HierarchicalSpannConfig};
const BLOCK_SIZE_BYTES: usize = 3 * 1024 * 1024;
#[derive(serde::Serialize, serde::Deserialize)]
struct CheckpointMeta {
    checkpoint_idx: usize,
    total_vectors: usize,
    posting_list_id: String,
    scalar_metadata_id: String,
    vector_data_id: String,
    leaf_node_id: String,
    internal_node_id: String,
}

impl CheckpointMeta {
    fn from_ids(checkpoint_idx: usize, total_vectors: usize, ids: &HierarchicalSpannIds) -> Self {
        Self {
            checkpoint_idx,
            total_vectors,
            posting_list_id: ids.posting_list_id.to_string(),
            scalar_metadata_id: ids.scalar_metadata_id.to_string(),
            vector_data_id: ids.vector_data_id.to_string(),
            leaf_node_id: ids.leaf_node_id.to_string(),
            internal_node_id: ids.internal_node_id.to_string(),
        }
    }

    fn to_ids(&self) -> HierarchicalSpannIds {
        HierarchicalSpannIds {
            posting_list_id: self
                .posting_list_id
                .parse()
                .expect("invalid posting_list_id UUID"),
            scalar_metadata_id: self
                .scalar_metadata_id
                .parse()
                .expect("invalid scalar_metadata_id UUID"),
            vector_data_id: self
                .vector_data_id
                .parse()
                .expect("invalid vector_data_id UUID"),
            leaf_node_id: self
                .leaf_node_id
                .parse()
                .expect("invalid leaf_node_id UUID"),
            internal_node_id: self
                .internal_node_id
                .parse()
                .expect("invalid internal_node_id UUID"),
        }
    }
}

async fn make_blockfile_provider(storage_path: &Path, max_cache_bytes: u64) -> BlockfileProvider {
    let storage = Storage::Local(LocalStorage::new(storage_path.to_str().unwrap()));
    let (block_cache, sparse_index_cache) = if max_cache_bytes == 0 {
        // Legacy unbounded cache. Useful when correctness depends on
        // never evicting (e.g. small/medium runs), but grows monotonically
        // and will eventually OOM the bench on long runs.
        (new_cache_for_test(), new_cache_for_test())
    } else {
        // Foyer's `capacity` is the total weight budget; the cache
        // weighter returns each value's `Weighted::weight()`. Crucially:
        //   * `Block::weight()` returns MiB (rounded up, min 1), NOT
        //     bytes (see rust/blockstore/src/arrow/block/types.rs). So
        //     the block cache capacity must be in MiB units. Passing
        //     bytes here makes the cap ~1M times too large and the cache
        //     effectively unbounded.
        //   * `RootReader::weight()` returns 1 (a count), so the
        //     sparse-index cache capacity is in #-of-readers, not bytes.
        const MIB_BYTES: u64 = 1024 * 1024;
        let block_cap_mib = (max_cache_bytes / MIB_BYTES).max(64) as usize;
        // Each cached RootReader is small (KBs); 4096 entries is plenty
        // of headroom for any benchmark workload but keeps it bounded.
        let root_cap_entries = 4096usize;
        let block_cfg = FoyerCacheConfig {
            name: "bench_block_cache".to_string(),
            capacity: block_cap_mib,
            ..FoyerCacheConfig::default()
        };
        let root_cfg = FoyerCacheConfig {
            name: "bench_sparse_index_cache".to_string(),
            capacity: root_cap_entries,
            ..FoyerCacheConfig::default()
        };
        let block_cache = block_cfg
            .build_memory_persistent()
            .await
            .expect("failed to build foyer block cache");
        let sparse_index_cache = root_cfg
            .build_memory_persistent()
            .await
            .expect("failed to build foyer sparse-index cache");
        (block_cache, sparse_index_cache)
    };
    let arrow_blockfile_provider = ArrowBlockfileProvider::new(
        storage,
        BLOCK_SIZE_BYTES,
        block_cache,
        sparse_index_cache,
        16,
        0,
    );
    BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider)
}

fn main() {
    rayon::ThreadPoolBuilder::new().num_threads(32).build_global().unwrap();
    tokio::runtime::Builder::new_multi_thread().worker_threads(32).enable_all().build().unwrap().block_on(run());
}
async fn run() {
    let args: Vec<_> = std::env::args().collect();
    let action = &args[1];
    assert!(matches!(action.as_str(), "open" | "commit"));
    let dir = Path::new(&args[2]);
    let rounds: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(1);
    for round in 0..rounds {
        let meta: CheckpointMeta = serde_json::from_slice(&std::fs::read(dir.join("checkpoint.json")).unwrap()).unwrap();
        let start = Instant::now();
        let provider = make_blockfile_provider(&dir.join("data"), 1073741824).await;
        let provider_seconds = start.elapsed().as_secs_f64();
        let start = Instant::now();
        if action == "open" {
            let reader = HierarchicalSpannReader::open(&provider, meta.to_ids(), DistanceFunction::Euclidean, HierarchicalSpannConfig::default()).await.unwrap();
            let seconds = start.elapsed().as_secs_f64();
            let memory = reader.memory_usage();
            let vector_pins = reader.loaded_blocks_stats().1;
            let queries = datasets::ground_truth::load(Path::new("/mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet"), DistanceFunction::Euclidean).unwrap();
            let query = queries.iter().find(|q| q.max_vector_id == meta.total_vectors as u64).expect("matching exact query");
            let policy = hierarchical_index::common::ReadBeamPolicy::with_level_overrides(Some(2.0), 10, 256, Vec::new(), vec![50.0,5.0,0.0], reader.level_node_counts());
            let first_start = Instant::now();
            let (results, scanned, leaves, timings) = reader.search_with_policy_lazy(&query.vector, 100, 8, &policy).await.unwrap();
            let first_seconds = first_start.elapsed().as_secs_f64();
            let ids: Vec<_> = results.iter().map(|(id,_)| *id).collect();
            let recall = datasets::recall_at_k(&ids, &query.neighbors, 100);
            println!("{}", serde_json::json!({"action":action,"round":round,"provider_seconds":provider_seconds,"open_seconds":seconds,"first_query_seconds":first_seconds,"open_plus_first_query_seconds":provider_seconds+seconds+first_seconds,"first_recall":recall,"first_result_ids":ids,"scanned":scanned,"leaves":leaves,"navigate_nanos":timings.navigate_nanos,"nodes":reader.node_count(),"navigation_payload_bytes":memory.tree_bytes,"vector_pins_at_open":vector_pins}));
        } else {
            let ids = meta.to_ids();
            let writer = HierarchicalSpannWriter::open(&provider, ids.clone(), DistanceFunction::Euclidean, HierarchicalSpannConfig::default()).await.unwrap();
            let writer_open_seconds = start.elapsed().as_secs_f64();
            let start = Instant::now();
            let ids = Box::pin(Box::pin(writer.commit(&provider, Some(&ids))).await.unwrap().flush()).await.unwrap();
            let commit_seconds = start.elapsed().as_secs_f64();
            let next = CheckpointMeta::from_ids(meta.checkpoint_idx, meta.total_vectors, &ids);
            std::fs::write(dir.join("checkpoint.centroid-profile.tmp"), serde_json::to_vec_pretty(&next).unwrap()).unwrap();
            std::fs::rename(dir.join("checkpoint.centroid-profile.tmp"), dir.join("checkpoint.json")).unwrap();
            println!("{}", serde_json::json!({"action":action,"round":round,"provider_seconds":provider_seconds,"writer_open_seconds":writer_open_seconds,"commit_seconds":commit_seconds,"total_vectors":meta.total_vectors}));
        }
    }
}
