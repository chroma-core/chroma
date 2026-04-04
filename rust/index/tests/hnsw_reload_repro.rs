use std::{
    env,
    process::{Command, Output},
    sync::Arc,
};

use chroma_blockstore::provider::BlockfileProvider;
use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
use chroma_config::{registry::Registry, Configurable};
use chroma_distance::DistanceFunction;
use chroma_index::{
    config::{HnswGarbageCollectionConfig, PlGarbageCollectionConfig},
    hnsw_provider::HnswIndexProvider,
    spann::types::{GarbageCollectionContext, SpannIndexReader, SpannIndexWriter, SpannMetrics},
};
use chroma_storage::test_storage;
use chroma_types::{CollectionUuid, InternalSpannConfiguration};
use rand::{rngs::StdRng, Rng, SeedableRng};

const CHILD_ENV: &str = "CHROMA_HNSW_RELOAD_REPRO_CHILD";
const SEED_ENV: &str = "CHROMA_HNSW_RELOAD_REPRO_SEED";
const BATCHES_ENV: &str = "CHROMA_HNSW_RELOAD_REPRO_BATCH_COUNT";
const WORKERS_ENV: &str = "CHROMA_HNSW_RELOAD_REPRO_WORKER_COUNT";
const TEST_NAME: &str = "deleted_heads_can_abort_a_future_reload";

// Broad command that is known to exercise this repro reliably.
//
//   CHROMA_HNSW_RELOAD_REPRO_CHILD=1 \
//   CHROMA_HNSW_RELOAD_REPRO_BATCH_COUNT=5 \
//   CHROMA_HNSW_RELOAD_REPRO_WORKER_COUNT=10 \
//   CHROMA_HNSW_RELOAD_REPRO_SEED=4 \
//   cargo test -p chroma-index --test hnsw_reload_repro -- --exact deleted_heads_can_abort_a_future_reload --nocapture

const SEED_START_ENV: &str = "CHROMA_HNSW_RELOAD_REPRO_SEED_START";
const SEED_END_ENV: &str = "CHROMA_HNSW_RELOAD_REPRO_SEED_END";

#[test]
fn deleted_heads_can_abort_a_future_reload() {
    if env::var_os(CHILD_ENV).is_some() {
        let seed = env::var(SEED_ENV)
            .expect("missing seed")
            .parse::<u64>()
            .expect("seed should parse");
        run_repro(seed);
        return;
    }

    let start_seed: u64 = env::var(SEED_START_ENV)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(0);
    let end_seed: u64 = env::var(SEED_END_ENV)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(16);

    for seed in start_seed..end_seed {
        let output = spawn_child(seed);
        if output.status.success() {
            continue;
        }
        let output_text = combined_output(&output);
        panic!(
            "HNSW reload failed on child seed {seed} in range [{start_seed}, {end_seed}):\n{output_text}",
        );
    }
}

fn spawn_child(seed: u64) -> Output {
    Command::new(env::current_exe().expect("current test binary"))
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .env(SEED_ENV, seed.to_string())
        .env(BATCHES_ENV, "5")
        .env(WORKERS_ENV, "10")
        .output()
        .expect("child process should start")
}

fn combined_output(output: &Output) -> String {
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    text
}

fn run_repro(seed: u64) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("runtime should build");
    runtime.block_on(async move {
        let (_storage_root, storage) = test_storage();
        let collection_id = CollectionUuid::new();
        let params = InternalSpannConfiguration {
            split_threshold: 100,
            merge_threshold: 50,
            reassign_neighbor_count: 8,
            max_neighbors: 16,
            ..Default::default()
        };
        let gc_context = GarbageCollectionContext::try_from_config(
            &(
                PlGarbageCollectionConfig::default(),
                HnswGarbageCollectionConfig::default(),
            ),
            &Registry::default(),
        )
        .await
        .expect("gc context should build");

        let dimensionality = 1000;
        let batch_count = env::var(BATCHES_ENV)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(5);
        let docs_per_batch = 1000;
        let worker_count = env::var(WORKERS_ENV)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(10);
        let prefix_path = "";
        let distance_function: DistanceFunction = params.space.clone().into();
        let mut rng = StdRng::seed_from_u64(seed);
        let docs: Arc<Vec<Vec<f32>>> = Arc::new(
            (0..batch_count * docs_per_batch)
                .map(|_| {
                    (0..dimensionality)
                        .map(|_| rng.gen::<f32>())
                        .collect::<Vec<f32>>()
                })
                .collect(),
        );

        let mut hnsw_id = None;
        let mut versions_map_id = None;
        let mut posting_list_id = None;
        let mut max_head_id_id = None;

        for batch in 0..batch_count {
            let blockfile_provider = new_blockfile_provider(storage.clone());
            let hnsw_provider = new_hnsw_provider(storage.clone());
            let writer = SpannIndexWriter::from_id(
                &hnsw_provider,
                hnsw_id.as_ref(),
                versions_map_id.as_ref(),
                posting_list_id.as_ref(),
                max_head_id_id.as_ref(),
                &collection_id,
                prefix_path,
                dimensionality,
                &blockfile_provider,
                params.clone(),
                gc_context.clone(),
                5 * 1024 * 1024,
                SpannMetrics::default(),
                None,
            )
            .await
            .expect("writer should open");

            let mut join_handles = Vec::new();
            for worker in 0..worker_count {
                let writer = writer.clone();
                let docs = docs.clone();
                let start = batch * docs_per_batch + worker * (docs_per_batch / worker_count);
                let end = batch * docs_per_batch + (worker + 1) * (docs_per_batch / worker_count);
                join_handles.push(tokio::spawn(async move {
                    for doc_index in start..end {
                        writer
                            .add((doc_index + 1) as u32, &docs[doc_index])
                            .await
                            .expect("insert should succeed");
                    }
                }));
            }
            for handle in join_handles {
                handle.await.expect("worker should complete");
            }

            let flusher = Box::pin(writer.commit())
                .await
                .expect("commit should succeed");
            let ids = Box::pin(flusher.flush())
                .await
                .expect("flush should succeed");
            hnsw_id = Some(ids.hnsw_id);
            versions_map_id = Some(ids.versions_map_id);
            posting_list_id = Some(ids.pl_id);
            max_head_id_id = Some(ids.max_head_id_id);
        }

        let blockfile_provider = new_blockfile_provider(storage.clone());
        let hnsw_provider = new_hnsw_provider(storage);
        let _reader = Box::pin(SpannIndexReader::from_id(
            hnsw_id.as_ref(),
            &hnsw_provider,
            &collection_id,
            distance_function,
            dimensionality,
            params.ef_search,
            posting_list_id.as_ref(),
            versions_map_id.as_ref(),
            &blockfile_provider,
            prefix_path,
            true,
            params,
        ))
        .await
        .expect("final reader open should reach the HNSW load");
    });
}

fn new_blockfile_provider(storage: chroma_storage::Storage) -> BlockfileProvider {
    BlockfileProvider::new_arrow(
        storage,
        8 * 1024 * 1024,
        new_cache_for_test(),
        new_cache_for_test(),
        chroma_blockstore::arrow::config::BlockManagerConfig::default_num_concurrent_block_flushes(
        ),
    )
}

fn new_hnsw_provider(storage: chroma_storage::Storage) -> HnswIndexProvider {
    HnswIndexProvider::new(storage, new_non_persistent_cache_for_test(), 16)
}
