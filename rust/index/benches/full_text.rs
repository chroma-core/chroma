use std::sync::Arc;

use anyhow::Result;
use chroma_benchmark_datasets::{
    datasets::{ms_marco_queries::MicrosoftMarcoQueriesDataset, scidocs::SciDocsDataset},
    types::RecordDataset,
};
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::{
    cache::Cache,
    config::{CacheConfig, UnboundedCacheConfig},
};
use chroma_index::fulltext::{
    tokenizer::TantivyChromaTokenizer,
    types::{FullTextIndexReader, FullTextIndexWriter},
};
use chroma_storage::{local::LocalStorage, Storage};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use futures::{StreamExt, TryStreamExt};
use tantivy::tokenizer::NgramTokenizer;

mod dataset_utilities;
use dataset_utilities::{get_document_dataset, get_document_query_dataset_pair};

async fn compact_log_to_storage<'a, T>(
    blockfile_provider: &BlockfileProvider,
    // chunk: Chunk<LogRecord>,
    corpus: &T,
) -> Result<FullTextIndexReader<'a>>
where
    T: RecordDataset,
{
    let postings_blockfile_writer = blockfile_provider.create::<u32, Vec<u32>>().unwrap();
    let frequencies_blockfile_writer = blockfile_provider.create::<u32, u32>().unwrap();
    let postings_blockfile_id = postings_blockfile_writer.id();
    let frequencies_blockfile_id = frequencies_blockfile_writer.id();

    let tokenizer = Box::new(TantivyChromaTokenizer::new(
        NgramTokenizer::new(3, 3, false).unwrap(),
    ));

    let mut full_text_index_writer = FullTextIndexWriter::new(
        None,
        postings_blockfile_writer,
        frequencies_blockfile_writer,
        tokenizer,
    );

    let mut corpus_stream = corpus
        .create_records_stream()
        .await?
        .enumerate()
        .boxed_local();
    while let Some((i, record)) = corpus_stream.next().await {
        let record = record?;
        full_text_index_writer
            .add_document(&record.document, i as u32)
            .await
            .unwrap();
    }

    full_text_index_writer.write_to_blockfiles().await.unwrap();
    let flusher = full_text_index_writer.commit().unwrap();
    flusher.flush().await.unwrap();

    let postings_blockfile_reader = blockfile_provider
        .open::<u32, &[u32]>(&postings_blockfile_id)
        .await
        .unwrap();
    let frequencies_blockfile_reader = blockfile_provider
        .open::<u32, u32>(&frequencies_blockfile_id)
        .await
        .unwrap();

    let tokenizer = Box::new(TantivyChromaTokenizer::new(
        NgramTokenizer::new(3, 3, false).unwrap(),
    ));

    Ok(FullTextIndexReader::new(
        postings_blockfile_reader,
        frequencies_blockfile_reader,
        tokenizer,
    ))
}

const BLOCK_SIZE: usize = 8 * 1024 * 1024; // 8MB

fn create_blockfile_provider(storage_dir: &str) -> BlockfileProvider {
    let storage = Storage::Local(LocalStorage::new(storage_dir));
    let block_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
    let sparse_index_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
    let arrow_blockfile_provider =
        ArrowBlockfileProvider::new(storage.clone(), BLOCK_SIZE, block_cache, sparse_index_cache);
    BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider)
}

pub fn bench_compaction(c: &mut Criterion) {
    let runner = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create runtime");

    let (document_corpus, corpus_content_size) = runner
        .block_on(async {
            let corpus = get_document_dataset::<SciDocsDataset>().await;
            let stream = corpus.create_records_stream().await?;

            let corpus_content_size = stream
                .try_fold(
                    0,
                    |acc, record| async move { Ok(acc + record.document.len()) },
                )
                .await?;

            Ok::<(SciDocsDataset, usize), anyhow::Error>((corpus, corpus_content_size))
        })
        .unwrap(); // todo ?

    let mut compaction_group = c.benchmark_group("compaction");
    compaction_group.throughput(Throughput::Bytes(corpus_content_size as u64));

    let tmp_dir = tempfile::tempdir().unwrap();
    let blockfile_provider = create_blockfile_provider(tmp_dir.path().to_str().unwrap());

    let document_corpus = Arc::new(document_corpus);

    compaction_group.bench_function("scidocs", |b| {
        b.to_async(&runner).iter_batched(
            || (document_corpus.clone(), blockfile_provider.clone()),
            |(document_corpus, blockfile_provider)| async move {
                compact_log_to_storage(&blockfile_provider, black_box(&document_corpus))
                    .await
                    .unwrap();
            },
            criterion::BatchSize::LargeInput,
        )
    });
}

fn bench_querying(c: &mut Criterion) {
    let runner = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create runtime");

    let (document_corpus, query_subset) = runner.block_on(get_document_query_dataset_pair::<
        SciDocsDataset,
        MicrosoftMarcoQueriesDataset,
    >(2, 10_000));

    let tmp_dir = tempfile::tempdir().unwrap();
    let blockfile_provider = create_blockfile_provider(tmp_dir.path().to_str().unwrap());

    let mut querying_group = c.benchmark_group("querying");
    querying_group.throughput(Throughput::Elements(1));

    let mut query_iter = query_subset.queries.iter().cycle();

    let index_reader = runner.block_on(async {
        compact_log_to_storage(&blockfile_provider, &document_corpus)
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

                assert!(result.len() > 0, "Query result is empty");
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench_querying);
criterion_main!(benches);
