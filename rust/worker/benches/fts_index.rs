use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use chroma_benchmark_datasets::{
    datasets::{ms_marco_queries::MicrosoftMarcoQueriesDataset, scidocs::SciDocsDataset},
    types::{DocumentDataset, FrozenQuerySubset, QueryDataset},
};
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::{
    cache::Cache,
    config::{CacheConfig, UnboundedCacheConfig},
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::{
    Chunk, DirectDocumentComparison, LogRecord, WhereDocument, WhereDocumentOperator,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use futures::{StreamExt, TryFutureExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use uuid::Uuid;
use worker::segment::{
    metadata_segment::{MetadataSegmentReader, MetadataSegmentWriter},
    types::{LogMaterializer, SegmentFlusher, SegmentWriter},
};

async fn get_log_chunk<T: DocumentDataset>(corpus: &T) -> Chunk<LogRecord> {
    // todo: Result?
    let log_records = corpus
        .create_log_stream(|_| async { vec![0.0, 0.0, 0.0] })
        .await
        .expect("Failed to create log stream")
        .filter_map(|record| async {
            match record {
                Ok(record) => Some(record),
                Err(_) => None,
            }
        })
        .collect::<Vec<_>>()
        .await;

    Chunk::new(log_records.into())
}

async fn get_reader(storage: &Storage, chunk: Chunk<LogRecord>) -> MetadataSegmentReader<'static> {
    let materializer = LogMaterializer::new(None, chunk, None);

    let mut metadata_segment = chroma_types::Segment {
        id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
        r#type: chroma_types::SegmentType::BlockfileMetadata,
        scope: chroma_types::SegmentScope::METADATA,
        collection: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
        metadata: None,
        file_path: HashMap::new(),
    };

    const BLOCK_SIZE: usize = 8 * 1024 * 1024; // 8MB

    let block_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
    let sparse_index_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
    let arrow_blockfile_provider =
        ArrowBlockfileProvider::new(storage.clone(), BLOCK_SIZE, block_cache, sparse_index_cache);
    let blockfile_provider = BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);

    let mut metadata_writer =
        MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
            .await
            .expect("Error creating segment writer");

    metadata_writer
        .apply_materialized_log_chunk(materializer.materialize().await.unwrap())
        .await
        .unwrap();
    metadata_writer.write_to_blockfiles().await.unwrap();
    let flusher = metadata_writer.commit().unwrap();
    metadata_segment.file_path = flusher.flush().await.unwrap();

    MetadataSegmentReader::from_segment(&metadata_segment, &blockfile_provider)
        .await
        .expect("Metadata segment reader construction failed")
}

async fn get_datasets<DocumentCorpus: DocumentDataset, QueryCorpus: QueryDataset>(
) -> (DocumentCorpus, FrozenQuerySubset) {
    let progress = MultiProgress::new();

    let style = ProgressStyle::default_spinner()
        .template("  {spinner:.green} {msg}")
        .unwrap();

    let finish_style = ProgressStyle::default_spinner()
        .template("  {prefix:.green} {msg}")
        .unwrap();

    let parent_task_style = ProgressStyle::default_spinner()
        .template("📁 initializing datasets...")
        .unwrap();
    let parent_task = ProgressBar::new_spinner().with_style(parent_task_style);
    // parent_task.tick();

    let document_corpus_spinner = ProgressBar::new_spinner()
        .with_message(DocumentCorpus::get_display_name())
        .with_style(style.clone());
    let query_corpus_spinner = ProgressBar::new_spinner()
        .with_message(QueryCorpus::get_display_name())
        .with_style(style.clone());

    let parent_task = progress.add(parent_task);
    let document_corpus_spinner = progress.add(document_corpus_spinner);
    let query_corpus_spinner = progress.add(query_corpus_spinner);

    parent_task.enable_steady_tick(Duration::from_millis(50));
    document_corpus_spinner.enable_steady_tick(Duration::from_millis(50));
    query_corpus_spinner.enable_steady_tick(Duration::from_millis(50));

    let document_corpus_init = DocumentCorpus::init().and_then(|r| async {
        document_corpus_spinner.set_style(finish_style.clone());
        document_corpus_spinner.set_prefix("✔︎");
        document_corpus_spinner.finish_and_clear();
        Ok(r)
    });
    let query_corpus_init = QueryCorpus::init().and_then(|r| async {
        query_corpus_spinner.set_style(finish_style.clone());
        query_corpus_spinner.set_prefix("✔");
        query_corpus_spinner.finish_and_clear();
        Ok(r)
    });

    let (document_corpus, query_corpus) = futures::join!(document_corpus_init, query_corpus_init);
    let document_corpus = document_corpus.expect("Failed to initialize document corpus");
    let query_corpus = query_corpus.expect("Failed to initialize query corpus");

    let query_spinner = ProgressBar::new_spinner()
        .with_message("creating query subset...")
        .with_style(style.clone());

    let query_spinner = progress.add(query_spinner);
    query_spinner.enable_steady_tick(Duration::from_millis(50));

    let subset = query_corpus
        .get_or_create_frozen_query_subset(&document_corpus, 2, 10_000, None)
        .and_then(|r| async {
            query_spinner.set_style(finish_style.clone());
            query_spinner.set_prefix("✔");
            query_spinner.finish_and_clear();
            Ok(r)
        })
        .await
        .expect("Failed to create query subset");

    progress.clear().unwrap();

    (document_corpus, subset)
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let runner = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create runtime");

    let (document_corpus, query_subset) =
        runner.block_on(get_datasets::<SciDocsDataset, MicrosoftMarcoQueriesDataset>());

    let log_chunk = runner.block_on(get_log_chunk(&document_corpus));

    let tmp_dir = tempfile::tempdir().unwrap();
    let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

    c.bench_function("compaction", |b| {
        b.to_async(&runner).iter_batched(
            || (log_chunk.clone(), storage.clone()),
            |(chunk, storage)| async move {
                get_reader(&storage, black_box(chunk)).await;
            },
            // todo: correct size?
            criterion::BatchSize::SmallInput,
        )
    });

    // todo: progress bar for compaction
    let reader = Arc::new(runner.block_on(get_reader(&storage, log_chunk.clone())));

    let mut query_iter = query_subset.queries.iter().cycle();

    c.bench_function("querying", |b| {
        b.to_async(&runner).iter_batched(
            || (reader.clone(), query_iter.next().unwrap().clone()),
            |(reader, query)| async move {
                let where_document =
                    WhereDocument::DirectWhereDocumentComparison(DirectDocumentComparison {
                        document: black_box(query),
                        operator: WhereDocumentOperator::Contains,
                    });

                let result = black_box(reader)
                    .query(None, Some(&where_document), None, 0, 0)
                    .await
                    .unwrap();

                assert!(result.is_some(), "Query result is None");
                assert!(result.unwrap().len() > 0, "Query result is empty");
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
