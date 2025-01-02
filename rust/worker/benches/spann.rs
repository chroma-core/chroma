use std::{collections::HashSet, path::PathBuf};

use chroma_benchmark::{
    benchmark::tokio_multi_thread,
    datasets::{gist::GistDataset, types::RecordDataset},
};
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
use chroma_index::{
    hnsw_provider::HnswIndexProvider,
    spann::{
        types::{SpannIndexReader, SpannIndexWriter, SpannPosting},
        utils::rng_query,
    },
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::CollectionUuid;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::StreamExt;
use rand::seq::SliceRandom;
use roaring::RoaringBitmap;
use worker::execution::{
    operator::Operator,
    operators::{
        spann_bf_pl::{SpannBfPlInput, SpannBfPlOperator},
        spann_knn_merge::{SpannKnnMergeInput, SpannKnnMergeOperator},
    },
};

fn get_records(runtime: &tokio::runtime::Runtime) -> Vec<(u32, Vec<f32>)> {
    runtime.block_on(async {
        let gist_dataset = GistDataset::init()
            .await
            .expect("Failed to initialize Gist dataset");
        let mut records_stream = gist_dataset
            .create_records_stream()
            .await
            .expect("Failed to create records stream");
        let mut records = Vec::new();
        let mut id = 1;
        while let Some(record) = records_stream.next().await {
            let unerred_record = record.expect("Failed to get record");
            records.push((id, unerred_record.embedding.unwrap()));
            id += 1;
        }
        records
    })
}

fn add_to_index_and_get_reader<'a>(
    runtime: &tokio::runtime::Runtime,
    records: &'a [(u32, Vec<f32>)],
    delete: bool,
    run_gc: bool,
) -> (SpannIndexReader<'a>, HashSet<u32>) {
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
            rx,
        );
        let m = 32;
        let ef_construction = 100;
        let ef_search = 100;
        let collection_id = CollectionUuid::new();
        let distance_function = chroma_distance::DistanceFunction::Euclidean;
        let dimensionality = 128;
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
        for (id, record) in records {
            writer
                .add(*id, record.as_slice())
                .await
                .expect("Error adding record");
        }
        let mut deleted_set = HashSet::new();
        // Delete 50% of records.
        if delete {
            let delete_count = records.len() / 2;
            for (count, (id, _)) in records.iter().enumerate() {
                if count >= delete_count {
                    break;
                }
                writer.delete(*id).await.expect("Error deleting point");
                deleted_set.insert(*id);
            }
        }
        // Run GC if needed.
        if run_gc {
            writer
                .garbage_collect()
                .await
                .expect("Error garbage collecting records");
        }

        let flusher = writer.commit().await.expect("Error committing writer");
        let paths = flusher.flush().await.expect("Error flushing writer");
        (
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
            .expect("Error creating spann index reader"),
            deleted_set,
        )
    })
}

fn calculate_recall<'a>(
    runtime: &tokio::runtime::Runtime,
    spann_reader: SpannIndexReader<'a>,
    query_emb: &'a [(u32, Vec<f32>)],
    base_emb: &'a [(u32, Vec<f32>)],
    deleted_set: HashSet<u32>,
) {
    let probe_nbr = 4;
    let k = 10;
    let rng_epsilon = 10.0;
    let rng_factor = 1.0;
    let distance_function = chroma_distance::DistanceFunction::Euclidean;
    runtime.block_on(async {
        let mut avg_recall = 0.0;
        for (index, emb) in query_emb {
            let (head_ids, _, _) = rng_query(
                emb,
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
                    query: emb.clone(),
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
            let mut input_set = Vec::new();
            for (id, emb) in base_emb {
                if deleted_set.contains(id) {
                    continue;
                }
                let posting = SpannPosting {
                    doc_offset_id: *id,
                    doc_embedding: emb.clone(),
                };
                input_set.push(posting);
            }
            let bf_operator_input = SpannBfPlInput {
                posting_list: input_set,
                k,
                query: emb.clone(),
                distance_function: distance_function.clone(),
                filter: chroma_types::SignedRoaringBitmap::Exclude(RoaringBitmap::new()),
            };
            let bf_operator_operator = SpannBfPlOperator::new();
            let bf_output = bf_operator_operator
                .run(&bf_operator_input)
                .await
                .expect("Error running operator");
            let mut recall = 0;
            for bf_record in bf_output.records.iter() {
                for spann_record in knn_output.merged_records.iter() {
                    if bf_record.offset_id == spann_record.offset_id {
                        recall += 1;
                    }
                }
            }
            println!(
                "Recall@{} with probe_nbr_count {} for query {}: {}",
                k,
                probe_nbr,
                index,
                recall as f32 / k as f32
            );
            avg_recall += recall as f32 / k as f32;
        }
        println!(
            "Avg recall@{} with probe_nbr_count {} across 1000 queries: {}",
            k,
            probe_nbr,
            avg_recall / query_emb.len() as f32
        );
    });
}

fn bench_spann_compaction(c: &mut Criterion) {
    let runtime = tokio_multi_thread();

    println!("Loading sift dataset with 128 embeddings");
    let mut records = get_records(&runtime);
    println!("Loaded 10000 sift dataset with 128 embeddings");
    // randomly shuffle the records.
    records.shuffle(&mut rand::thread_rng());

    c.bench_with_input(
        BenchmarkId::new("add_records_to_spann_segment", records.len()),
        &records,
        |b, records| {
            b.iter(|| {
                let start_time = std::time::Instant::now();
                let (reader, deleted_set) =
                    add_to_index_and_get_reader(&runtime, records[0..9000].as_ref(), false, false);
                println!(
                    "Added 9000 records to spann segment in {:?} ms",
                    start_time.elapsed().as_millis()
                );
                println!("Getting recall on 1000 records");
                calculate_recall(
                    &runtime,
                    reader,
                    &records[9000..10000],
                    &records[0..9000],
                    deleted_set,
                );
            })
        },
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_spann_compaction
}

criterion_main!(benches);
