use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::Result;
use chroma_benchmark_datasets::{
    datasets::{ms_marco_queries::MicrosoftMarcoQueriesDataset, scidocs::SciDocsDataset},
    types::DocumentDataset,
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
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use futures::StreamExt;
use uuid::Uuid;
use worker::segment::{
    metadata_segment::{MetadataSegmentReader, MetadataSegmentWriter},
    types::{LogMaterializer, SegmentFlusher, SegmentWriter},
};

mod dataset_utilities;
use dataset_utilities::{get_document_dataset, get_document_query_dataset_pair};

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

async fn compact_log_to_storage(
    blockfile_provider: &BlockfileProvider,
    chunk: Chunk<LogRecord>,
) -> Result<chroma_types::Segment> {
    let materializer = LogMaterializer::new(None, chunk, None);

    let mut metadata_segment = chroma_types::Segment {
        id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
        r#type: chroma_types::SegmentType::BlockfileMetadata,
        scope: chroma_types::SegmentScope::METADATA,
        collection: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
        metadata: None,
        file_path: HashMap::new(),
    };

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

    Ok(metadata_segment)
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

    let document_corpus = runner.block_on(get_document_dataset::<SciDocsDataset>());
    let log_chunk = runner.block_on(get_log_chunk(&document_corpus));

    let log_chunk_content_size = log_chunk
        .clone()
        .iter()
        .map(|(r, _)| r.record.document.as_ref().unwrap().len())
        .sum::<usize>();

    let mut compaction_group = c.benchmark_group("compaction");
    compaction_group.throughput(Throughput::Bytes(log_chunk_content_size as u64));

    let tmp_dir = tempfile::tempdir().unwrap();
    let blockfile_provider = create_blockfile_provider(tmp_dir.path().to_str().unwrap());

    compaction_group.bench_function(format!("scidocs ({} records)", log_chunk.len()), |b| {
        b.to_async(&runner).iter_batched(
            || (log_chunk.clone(), blockfile_provider.clone()),
            |(chunk, blockfile_provider)| async move {
                compact_log_to_storage(&blockfile_provider, black_box(chunk))
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
    let log_chunk = runner.block_on(get_log_chunk(&document_corpus));

    let tmp_dir = tempfile::tempdir().unwrap();
    let blockfile_provider = create_blockfile_provider(tmp_dir.path().to_str().unwrap());

    let mut querying_group = c.benchmark_group("querying");
    querying_group.throughput(Throughput::Elements(1));

    let segment_reader = runner.block_on(async {
        let metadata_segment = compact_log_to_storage(&blockfile_provider, log_chunk.clone())
            .await
            .unwrap();
        Arc::new(
            MetadataSegmentReader::from_segment(&metadata_segment, &blockfile_provider)
                .await
                .unwrap(),
        )
    });

    let mut query_iter = query_subset.queries.iter().cycle();

    querying_group.bench_function(format!("scidocs ({} records)", log_chunk.len()), |b| {
        b.to_async(&runner).iter_batched(
            || (segment_reader.clone(), query_iter.next().unwrap().clone()),
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

criterion_group!(benches, bench_compaction, bench_querying);
criterion_main!(benches);
