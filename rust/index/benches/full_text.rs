use anyhow::Result;
use chroma_benchmark::datasets::types::Record;
use chroma_benchmark::datasets::{
    ms_marco_queries::MicrosoftMarcoQueriesDataset, scidocs::SciDocsDataset, types::RecordDataset,
};
use chroma_blockstore::BlockfileWriterOptions;
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::UnboundedCacheConfig;
use chroma_index::fulltext::types::{DocumentMutation, FullTextIndexReader, FullTextIndexWriter};
use chroma_storage::{local::LocalStorage, Storage};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;
mod dataset_utilities;
use dataset_utilities::{get_record_dataset, get_record_query_dataset_pair};
use rayon::prelude::*;
use tantivy::tokenizer::NgramTokenizer;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Clone, Copy)]
struct NumWorkersParameter {
    num_workers: usize,
}

impl std::fmt::Display for NumWorkersParameter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} workers", self.num_workers)
    }
}

impl From<NumWorkersParameter> for usize {
    fn from(p: NumWorkersParameter) -> Self {
        p.num_workers
    }
}

async fn compact_log_and_get_reader<'a>(
    blockfile_provider: &BlockfileProvider,
    mut chunked_mutations: Vec<Vec<DocumentMutation<'a>>>,
) -> Result<FullTextIndexReader<'a>> {
    let postings_blockfile_writer = blockfile_provider
        .write::<u32, Vec<u32>>(BlockfileWriterOptions::new().ordered_mutations())
        .await
        .unwrap();
    let postings_blockfile_id = postings_blockfile_writer.id();

    let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();

    let mut full_text_index_writer = FullTextIndexWriter::new(postings_blockfile_writer, tokenizer);

    chunked_mutations.par_drain(..).for_each(|chunk| {
        full_text_index_writer.handle_batch(chunk).unwrap();
    });

    full_text_index_writer.write_to_blockfiles().await.unwrap();
    let flusher = full_text_index_writer.commit().await.unwrap();
    flusher.flush().await.unwrap();

    let postings_blockfile_reader = blockfile_provider
        .open::<u32, &[u32]>(&postings_blockfile_id)
        .await
        .unwrap();

    let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();

    Ok(FullTextIndexReader::new(
        postings_blockfile_reader,
        tokenizer,
    ))
}

const BLOCK_SIZE: usize = 8 * 1024 * 1024; // 8MB

fn create_blockfile_provider(storage_dir: &str) -> BlockfileProvider {
    let storage = Storage::Local(LocalStorage::new(storage_dir));
    let block_cache = Box::new(UnboundedCacheConfig {}.build()) as _;
    let sparse_index_cache = Box::new(UnboundedCacheConfig {}.build()) as _;
    let arrow_blockfile_provider =
        ArrowBlockfileProvider::new(storage.clone(), BLOCK_SIZE, block_cache, sparse_index_cache);
    BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider)
}

pub fn bench_compaction(c: &mut Criterion) {
    let runner = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create runtime");

    let (record_corpus, corpus_content_size) = runner
        .block_on(async {
            let corpus = get_record_dataset::<SciDocsDataset>().await;
            let stream = corpus.create_records_stream().await?;

            let corpus_content_size = stream
                .try_fold(
                    0,
                    |acc, record| async move { Ok(acc + record.document.len()) },
                )
                .await?;

            Ok::<(SciDocsDataset, usize), anyhow::Error>((corpus, corpus_content_size))
        })
        .unwrap();

    let mut compaction_group = c.benchmark_group("compaction");
    compaction_group.throughput(Throughput::Bytes(corpus_content_size as u64));

    let tmp_dir = tempfile::tempdir().unwrap();
    let blockfile_provider = create_blockfile_provider(tmp_dir.path().to_str().unwrap());

    let records = runner.block_on(async {
        let stream = record_corpus.create_records_stream().await.unwrap();

        stream
            .enumerate()
            .map(|(i, record)| record.map(|r| (i, r)))
            .boxed_local()
            .try_collect::<Vec<(usize, Record)>>()
            .await
            .unwrap()
    });
    let prepared_corpus = records
        .iter()
        .map(|(i, r)| DocumentMutation::Create {
            offset_id: *i as u32,
            new_document: &r.document,
        })
        .collect::<Vec<_>>();
    let prepared_corpus = Arc::new(prepared_corpus);

    for num_workers in (1..=4).map(|i| NumWorkersParameter { num_workers: i }) {
        compaction_group.bench_function(BenchmarkId::from_parameter(num_workers), |b| {
            b.to_async(&runner).iter_batched(
                || {
                    let chunked_corpus = prepared_corpus
                        .chunks(prepared_corpus.len() / usize::from(num_workers))
                        .map(|chunk| chunk.to_vec())
                        .collect::<Vec<_>>();
                    (chunked_corpus, blockfile_provider.clone())
                },
                |(chunked_mutations, blockfile_provider)| async move {
                    compact_log_and_get_reader(&blockfile_provider, black_box(chunked_mutations))
                        .await
                        .unwrap();
                },
                criterion::BatchSize::LargeInput,
            )
        });
    }
}

fn bench_querying(c: &mut Criterion) {
    let runner = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create runtime");

    let (record_corpus, query_subset) = runner.block_on(get_record_query_dataset_pair::<
        SciDocsDataset,
        MicrosoftMarcoQueriesDataset,
    >(2, 10_000));

    let tmp_dir = tempfile::tempdir().unwrap();
    let blockfile_provider = create_blockfile_provider(tmp_dir.path().to_str().unwrap());

    let mut querying_group = c.benchmark_group("querying");
    querying_group.throughput(Throughput::Elements(1));

    let mut query_iter = query_subset.queries.iter().cycle();

    let records = runner.block_on(async {
        let stream = record_corpus.create_records_stream().await.unwrap();

        stream
            .enumerate()
            .map(|(i, record)| record.map(|r| (i, r)))
            .boxed_local()
            .try_collect::<Vec<(usize, Record)>>()
            .await
            .unwrap()
    });
    let prepared_corpus = records
        .iter()
        .map(|(i, r)| DocumentMutation::Create {
            offset_id: *i as u32,
            new_document: &r.document,
        })
        .collect::<Vec<_>>();

    let index_reader = runner.block_on(async {
        compact_log_and_get_reader(&blockfile_provider, vec![prepared_corpus])
            .await
            .unwrap()
    });

    querying_group.bench_function("scidocs", |b| {
        b.to_async(&runner).iter_batched(
            || (index_reader.clone(), query_iter.next().unwrap().clone()),
            |(index_reader, query)| async move {
                let result = black_box(index_reader)
                    .search(black_box(&query))
                    .await
                    .unwrap();

                assert!(!result.is_empty(), "Query result is empty");
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench_querying, bench_compaction);
criterion_main!(benches);
