use core::num;
use std::{path::PathBuf, sync::Arc};

use chroma_benchmark::{benchmark::tokio_multi_thread, datasets::gist::Gist1MDataset};
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
use chroma_index::{
    hnsw_provider::HnswIndexProvider,
    spann::{
        types::{SpannIndexIds, SpannIndexReader, SpannIndexWriter},
        utils::rng_query,
    },
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_system::{dispatcher, Dispatcher, DispatcherConfig, Operator, System};
use chroma_types::{Chunk, CollectionUuid, Segment, SegmentUuid};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::StreamExt;
use ndarray::{s, Array2, ArrayView1};
use rand::seq::SliceRandom;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use tempfile::TempDir;
use uuid::Uuid;
use worker::execution::{
    operators::{
        filter::FilterOutput,
        spann_bf_pl::{SpannBfPlInput, SpannBfPlOperator},
        spann_knn_merge::{SpannKnnMergeInput, SpannKnnMergeOperator},
    },
    orchestration::{knn_filter::KnnFilterOutput, spann_knn::SpannKnnOrchestrator},
};

fn get_flat_records(runtime: &tokio::runtime::Runtime, num_records: usize) -> (usize, Vec<f32>) {
    runtime.block_on(async {
        let gist_dataset = Gist1MDataset::init()
            .await
            .expect("Failed to initialize Gist dataset");
        let mut records_stream = gist_dataset
            .create_records_stream(num_records)
            .await
            .expect("Failed to create records stream");
        let mut records = Vec::new();
        while let Some(record) = records_stream.next().await {
            let unerred_record = record.expect("Failed to get record");
            records.extend(unerred_record.embedding.unwrap());
        }
        (Gist1MDataset::DIMENSION, records)
    })
}

fn add_to_index_and_get_reader<'a>(
    runtime: &tokio::runtime::Runtime,
    records: &'a Array2<f32>,
) -> SpannIndexReader<'a> {
    runtime.block_on(async {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let max_block_size_bytes = 8388608; // 8 MB.
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            max_block_size_bytes,
            block_cache,
            sparse_index_cache,
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let hnsw_cache = new_non_persistent_cache_for_test();
        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            16,
            rx,
        );
        let m = 32;
        let ef_construction = 100;
        let ef_search = 100;
        let collection_id = CollectionUuid::new();
        let distance_function = chroma_distance::DistanceFunction::Euclidean;
        let dimensionality = 960;
        let writer = SpannIndexWriter::from_id(
            &hnsw_provider,
            None,
            None,
            None,
            None,
            Some(m),
            Some(ef_construction),
            Some(ef_search),
            &collection_id,
            distance_function.clone(),
            dimensionality,
            &blockfile_provider,
        )
        .await
        .expect("Error creating spann index writer");
        let mut id = 1;
        for record in records.rows() {
            // println!("Adding record with id: {}", id);
            writer
                .add(id, record.as_slice().unwrap())
                .await
                .expect("Error adding record");
            id += 1;
        }

        let flusher = writer.commit().await.expect("Error committing writer");
        let paths = flusher.flush().await.expect("Error flushing writer");
        SpannIndexReader::from_id(
            Some(&paths.hnsw_id),
            &hnsw_provider,
            &collection_id,
            distance_function,
            dimensionality,
            Some(&paths.pl_id),
            Some(&paths.versions_map_id),
            &blockfile_provider,
        )
        .await
        .expect("Error creating spann index reader")
    })
}

fn add_to_index_and_get_flusher(
    runtime: &tokio::runtime::Runtime,
    records: &Array2<f32>,
    dim: usize,
) -> SpannIndexIds {
    let tmp_dir = tempfile::tempdir().unwrap();
    let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
    println!("(Sanket-temp) tmp_dir: {:?}", tmp_dir.path());
    let block_cache = new_cache_for_test();
    let sparse_index_cache = new_cache_for_test();
    let max_block_size_bytes = 8388608; // 8 MB.
    let arrow_blockfile_provider = ArrowBlockfileProvider::new(
        storage.clone(),
        max_block_size_bytes,
        block_cache,
        sparse_index_cache,
    );
    let blockfile_provider = BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
    let m = 32;
    let ef_construction = 100;
    let ef_search = 100;
    let collection_id = CollectionUuid::new();
    let distance_function = chroma_distance::DistanceFunction::Euclidean;
    let writer = runtime.block_on(async {
        let hnsw_cache = new_non_persistent_cache_for_test();
        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            16,
            rx,
        );
        SpannIndexWriter::from_id(
            &hnsw_provider,
            None,
            None,
            None,
            None,
            Some(m),
            Some(ef_construction),
            Some(ef_search),
            &collection_id,
            distance_function.clone(),
            dim,
            &blockfile_provider,
        )
        .await
        .expect("Error creating spann index writer")
    });
    (1..=records.nrows()).into_par_iter().for_each(|id| {
        runtime
            .block_on(writer.add(id as u32, records.row(id - 1).as_slice().unwrap()))
            .expect("Error writing using spann writer");
    });

    runtime.block_on(async {
        let flusher = writer.commit().await.expect("Error committing writer");
        flusher.flush().await.expect("Error flushing writer")
    })
}

// TODO(Sanket): Change this.
fn add_to_index_and_get_flusher_sequential(
    runtime: &tokio::runtime::Runtime,
    records: &Array2<f32>,
    dim: usize,
) -> SpannIndexIds {
    let writer = runtime.block_on(async {
        let tmp_dir = tempfile::tempdir().unwrap();
        println!("(Sanket-temp) tmp_dir: {:?}", tmp_dir.path());
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let max_block_size_bytes = 8388608; // 8 MB.
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            max_block_size_bytes,
            block_cache,
            sparse_index_cache,
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let hnsw_cache = new_non_persistent_cache_for_test();
        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            16,
            rx,
        );
        let m = 32;
        let ef_construction = 100;
        let ef_search = 100;
        let collection_id = CollectionUuid::new();
        let distance_function = chroma_distance::DistanceFunction::Euclidean;
        SpannIndexWriter::from_id(
            &hnsw_provider,
            None,
            None,
            None,
            None,
            Some(m),
            Some(ef_construction),
            Some(ef_search),
            &collection_id,
            distance_function.clone(),
            dim,
            &blockfile_provider,
        )
        .await
        .expect("Error creating spann index writer")
    });
    (1..=records.nrows()).into_par_iter().for_each(|id| {
        runtime
            .block_on(writer.add(id as u32, records.row(id - 1).as_slice().unwrap()))
            .expect("Error writing using spann writer");
    });

    runtime.block_on(async {
        let flusher = writer.commit().await.expect("Error committing writer");
        flusher.flush().await.expect("Error flushing writer")
    })
}

fn query_parallel(
    runtime: &tokio::runtime::Runtime,
    query: &Array2<f32>,
    flusher_paths: &SpannIndexIds,
    collection_id: CollectionUuid,
    distance_function: chroma_distance::DistanceFunction,
    tmp_dir: TempDir,
    storage: Storage,
    blockfile_provider: BlockfileProvider,
    hnsw_provider: HnswIndexProvider,
    dim: usize,
) {
    // Get all the heads.
    let probe_nbr = 128;
    let k = 10;
    let rng_epsilon = 10.0;
    let rng_factor = 1.0;
    let distance_function = chroma_distance::DistanceFunction::Euclidean;
    runtime.block_on(async {
        let mut avg_recall = 0.0;
        for row in query_emb.rows() {
            let (head_ids, _, _) = rng_query(
                row.as_slice().expect("Expected to get slice"),
                spann_reader.hnsw_index.clone(),
                probe_nbr,
                rng_epsilon,
                rng_factor,
                distance_function.clone(),
                false,
            )
            .await
            .expect("Error running rng query");
            let mut merge_list = Vec::new();
            for head_id in head_ids {
                let pl = spann_reader
                    .fetch_posting_list(head_id as u32)
                    .await
                    .expect("Error fetching posting list");
                let bf_operator_input = SpannBfPlInput {
                    posting_list: pl,
                    k,
                    query: row.as_slice().expect("Expected to get slice").to_vec(),
                    distance_function: distance_function.clone(),
                    filter: chroma_types::SignedRoaringBitmap::Exclude(RoaringBitmap::new()),
                };
                let bf_operator_operator = SpannBfPlOperator::new();
                let bf_output = bf_operator_operator
                    .run(&bf_operator_input)
                    .await
                    .expect("Error running operator");
                merge_list.push(bf_output.records);
            }
            // Now merge.
            let knn_input = SpannKnnMergeInput {
                records: merge_list,
            };
            let knn_operator = SpannKnnMergeOperator { k: k as u32 };
            let knn_output = knn_operator
                .run(&knn_input)
                .await
                .expect("Error running knn merge operator");
        }
    });
}

fn split_and_add_to_index_and_get_flusher(
    runtime: &tokio::runtime::Runtime,
    records: &Array2<f32>,
    dim: usize,
    num_runs: usize,
) -> SpannIndexIds {
    runtime.block_on(async {
        let num_records = records.nrows();
        let batch_size = num_records / num_runs;
        let tmp_dir = tempfile::tempdir().unwrap();
        println!("(Sanket-temp) tmp_dir: {:?}", tmp_dir.path());
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let mut block_cache = new_cache_for_test();
        let mut sparse_index_cache = new_cache_for_test();
        let max_block_size_bytes = 8388608; // 8 MB.
        let mut arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage.clone(),
            max_block_size_bytes,
            block_cache,
            sparse_index_cache,
        );
        let mut blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut hnsw_cache = new_non_persistent_cache_for_test();
        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmp_dir.path().to_str().unwrap()),
            hnsw_cache,
            16,
            rx,
        );
        let m = 32;
        let ef_construction = 100;
        let ef_search = 100;
        let collection_id = CollectionUuid::new();
        let distance_function = chroma_distance::DistanceFunction::Euclidean;
        let mut writer = SpannIndexWriter::from_id(
            &hnsw_provider,
            None,
            None,
            None,
            None,
            Some(m),
            Some(ef_construction),
            Some(ef_search),
            &collection_id,
            distance_function.clone(),
            dim,
            &blockfile_provider,
        )
        .await
        .expect("Error creating spann index writer");
        let mut id = 1;
        let start_time = std::time::Instant::now();
        for record in records.slice(s![0..batch_size, ..]).rows() {
            writer
                .add(id, record.as_slice().unwrap())
                .await
                .expect("Error adding record");
            id += 1;
        }
        println!(
            "Added first batch {:?} records to spann segment in {:?} ms",
            batch_size,
            start_time.elapsed().as_millis()
        );

        let mut flusher = writer.commit().await.expect("Error committing writer");
        let mut paths = flusher.flush().await.expect("Error flushing writer");

        for run in 2..=num_runs {
            block_cache = new_cache_for_test();
            sparse_index_cache = new_cache_for_test();
            arrow_blockfile_provider = ArrowBlockfileProvider::new(
                storage.clone(),
                max_block_size_bytes,
                block_cache,
                sparse_index_cache,
            );
            blockfile_provider =
                BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
            hnsw_cache = new_non_persistent_cache_for_test();
            let (_, rx) = tokio::sync::mpsc::unbounded_channel();
            hnsw_provider = HnswIndexProvider::new(
                storage.clone(),
                PathBuf::from(tmp_dir.path().to_str().unwrap()),
                hnsw_cache,
                16,
                rx,
            );
            writer = SpannIndexWriter::from_id(
                &hnsw_provider,
                Some(&paths.hnsw_id),
                Some(&paths.versions_map_id),
                Some(&paths.pl_id),
                Some(&paths.max_head_id_id),
                None,
                None,
                None,
                &collection_id,
                distance_function.clone(),
                dim,
                &blockfile_provider,
            )
            .await
            .expect("Error creating spann index writer");

            let start_time = std::time::Instant::now();
            let end = if run == num_runs {
                num_records
            } else {
                batch_size * run
            };
            for record in records.slice(s![batch_size * (run - 1)..end, ..]).rows() {
                writer
                    .add(id, record.as_slice().unwrap())
                    .await
                    .expect("Error adding record");
                id += 1;
            }
            println!(
                "Added {} batch {:?} records to spann segment in {:?} ms",
                run,
                end - (batch_size * (run - 1)),
                start_time.elapsed().as_millis()
            );

            flusher = writer.commit().await.expect("Error committing writer");
            paths = flusher.flush().await.expect("Error flushing writer");
        }

        paths
    })
}

fn compute_nearest_neighbors(
    base_embeddings: &Array2<f32>,
    query_embedding: ArrayView1<f32>,
    k: usize,
) -> Vec<(usize, f32)> {
    // Measure time and print it.
    let start_time = std::time::Instant::now();
    let query_norm = query_embedding.dot(&query_embedding);
    let mut distances: Vec<(usize, f32)> = (0..base_embeddings.nrows())
        .into_par_iter()
        .map(|idx| {
            let base_vec = base_embeddings.row(idx);
            let base_norm = base_vec.dot(&base_vec);
            let dot_product = base_vec.dot(&query_embedding);
            // Euclidean distance = sqrt(||a||^2 + ||b||^2 - 2<a,b>)
            let dist = base_norm + query_norm - 2.0 * dot_product;
            (idx + 1, dist)
        })
        .collect();
    // println!(
    //     "Computed distances in {:?} ms",
    //     start_time.elapsed().as_millis()
    // );

    let sort_start_time = std::time::Instant::now();
    distances.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    distances.truncate(k);
    // println!(
    //     "Sorted distances in {:?} ms",
    //     sort_start_time.elapsed().as_millis()
    // );
    distances
}

fn calculate_recall<'a>(
    runtime: &tokio::runtime::Runtime,
    spann_reader: SpannIndexReader<'a>,
    query_emb: &'a Array2<f32>,
    base_emb: &'a Array2<f32>,
) {
    let probe_nbr = 128;
    let k = 10;
    let rng_epsilon = 10.0;
    let rng_factor = 1.0;
    let distance_function = chroma_distance::DistanceFunction::Euclidean;
    runtime.block_on(async {
        let mut avg_recall = 0.0;
        for row in query_emb.rows() {
            let (head_ids, _, _) = rng_query(
                row.as_slice().expect("Expected to get slice"),
                spann_reader.hnsw_index.clone(),
                probe_nbr,
                rng_epsilon,
                rng_factor,
                distance_function.clone(),
                false,
            )
            .await
            .expect("Error running rng query");
            let mut merge_list = Vec::new();
            for head_id in head_ids {
                let pl = spann_reader
                    .fetch_posting_list(head_id as u32)
                    .await
                    .expect("Error fetching posting list");
                let bf_operator_input = SpannBfPlInput {
                    posting_list: pl,
                    k,
                    query: row.as_slice().expect("Expected to get slice").to_vec(),
                    distance_function: distance_function.clone(),
                    filter: chroma_types::SignedRoaringBitmap::Exclude(RoaringBitmap::new()),
                };
                let bf_operator_operator = SpannBfPlOperator::new();
                let bf_output = bf_operator_operator
                    .run(&bf_operator_input)
                    .await
                    .expect("Error running operator");
                merge_list.push(bf_output.records);
            }
            // Now merge.
            let knn_input = SpannKnnMergeInput {
                records: merge_list,
            };
            let knn_operator = SpannKnnMergeOperator { k: k as u32 };
            let knn_output = knn_operator
                .run(&knn_input)
                .await
                .expect("Error running knn merge operator");

            // Get the ground truth.
            let ground_truth = compute_nearest_neighbors(base_emb, row, k);

            let mut recall = 0;
            for bf_record in ground_truth.iter() {
                // println!("bf_record: {:?}", bf_record);
                for spann_record in knn_output.merged_records.iter() {
                    if bf_record.0 as u32 == spann_record.offset_id {
                        recall += 1;
                    }
                }
            }
            // println!(
            //     "Recall@{} with probe_nbr_count {} for query {}: {}",
            //     k,
            //     probe_nbr,
            //     index,
            //     recall as f32 / k as f32
            // );
            avg_recall += recall as f32 / k as f32;
        }
        let num_queries = query_emb.rows().into_iter().len();
        println!(
            "Avg recall@{} with probe_nbr_count {} across {} queries: {}",
            k,
            probe_nbr,
            num_queries,
            avg_recall / num_queries as f32
        );
    });
}

fn bench_compaction(c: &mut Criterion) {
    let runtime = tokio_multi_thread();

    let num_records = std::env::var("NUM_RECORDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10000);

    let (dim, flat_records) = get_flat_records(&runtime, num_records);

    println!(
        "Benchmarking {} records with dimension {} for compaction time",
        flat_records.len() / dim,
        dim
    );

    let base_set = Array2::from_shape_vec((flat_records.len() / dim, dim), flat_records)
        .expect("Expected to convert to ndarray");

    println!("Base set shape: {:?}", base_set.shape());

    c.bench_function("spann_compaction", |b| {
        b.iter(|| {
            let start_time = std::time::Instant::now();
            println!("Adding {:?} records to spann segment", base_set.nrows());
            let _ = add_to_index_and_get_flusher(&runtime, &base_set, dim);
            println!(
                "Added {:?} records to spann segment in {:?} ms",
                base_set.nrows(),
                start_time.elapsed().as_millis()
            );
        })
    });
}

fn bench_compaction_n_runs(c: &mut Criterion) {
    let runtime = tokio_multi_thread();

    let num_records = std::env::var("NUM_RECORDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10000);

    let num_runs = std::env::var("NUM_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    let (dim, flat_records) = get_flat_records(&runtime, num_records);

    println!(
        "Benchmarking {} records with dimension {} for compaction time for {} runs",
        flat_records.len() / dim,
        dim,
        num_runs
    );

    let base_set = Array2::from_shape_vec((flat_records.len() / dim, dim), flat_records)
        .expect("Expected to convert to ndarray");

    println!("Base set shape: {:?}", base_set.shape());

    c.bench_function("spann_compaction_n_runs", |b| {
        b.iter(|| {
            let start_time = std::time::Instant::now();
            println!(
                "Adding {:?} records to spann segment in {} runs",
                base_set.nrows(),
                num_runs
            );
            let _ = split_and_add_to_index_and_get_flusher(&runtime, &base_set, dim, num_runs);
            println!(
                "Added {:?} records to spann segment in {:?} ms in {} runs",
                base_set.nrows(),
                start_time.elapsed().as_millis(),
                num_runs
            );
        })
    });
}

// fn bench_query_recall(c: &mut Criterion) {
//     let runtime = tokio_multi_thread();

//     let num_records = std::env::args()
//         .nth(1)
//         .and_then(|s| s.parse().ok())
//         .unwrap_or(10000);

//     let mut records = get_records(&runtime, num_records);
//     // randomly shuffle the records.
//     records.shuffle(&mut rand::thread_rng());

//     c.bench_with_input(
//         BenchmarkId::new("spann_benchmark", records.len()),
//         &records,
//         |b, records| {
//             b.iter(|| {
//                 let num_records = records.len();
//                 let dim = records[0].len();
//                 // 10% of records are query set and 90% is base set.
//                 let base_set_len = (0.9 * num_records as f32) as usize;
//                 let base_vectors = &records[0..base_set_len];
//                 let flat_base: Vec<f32> = base_vectors.iter().flatten().cloned().collect();
//                 let base_set = Array2::from_shape_vec((base_set_len, dim), flat_base)
//                     .expect("Expected to convert to ndarray");
//                 let query_vectors =
//                     &records[base_set_len..std::cmp::min(num_records, base_set_len + 1000)];
//                 let flat_query: Vec<f32> = query_vectors.iter().flatten().cloned().collect();
//                 let query_set = Array2::from_shape_vec(
//                     (
//                         std::cmp::min(num_records, base_set_len + 1000) - base_set_len,
//                         dim,
//                     ),
//                     flat_query,
//                 )
//                 .expect("Expected to convert to ndarray");
//                 let start_time = std::time::Instant::now();
//                 let reader = add_to_index_and_get_reader(&runtime, &base_set);
//                 println!(
//                     "Added {:?} records to spann segment in {:?} ms",
//                     base_set_len,
//                     start_time.elapsed().as_millis()
//                 );
//                 println!(
//                     "Getting recall on {:?} records",
//                     std::cmp::min(num_records, base_set_len + 1000) - base_set_len
//                 );
//                 calculate_recall(&runtime, reader, &query_set, &base_set);
//             })
//         },
//     );
// }

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_compaction, bench_compaction_n_runs
}

criterion_main!(benches);
