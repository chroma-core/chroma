use std::fs::{read_dir, File};

use arrow::array::AsArray;
use chroma_benchmark::benchmark::{bench_run, tokio_multi_thread};
use chroma_blockstore::{
    arrow::provider::{ArrowBlockfileProvider, BlockfileReaderOptions},
    provider::BlockfileProvider,
    BlockfileWriterOptions,
};
use chroma_cache::new_cache_for_test;
use chroma_index::fulltext::types::{DocumentMutation, FullTextIndexReader, FullTextIndexWriter};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::regex::{literal_expr::NgramLiteralProvider, ChromaRegex};
use criterion::{criterion_group, criterion_main, Criterion};
use indicatif::{ParallelProgressIterator, ProgressIterator};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tantivy::tokenizer::NgramTokenizer;
use tempfile::tempdir;

const BLOCK_SIZE: usize = 1 << 23;
const MAX_CODE_LENGTH: usize = 1 << 12;

const FTS_PATTERNS: &[&str] = &[r"unreachable", r"unsafe", r"use std::collections::HashMap;"];
const REGEX_PATTERNS: &[&str] = &[
    r"\.collect::<.+>()",
    r"(?i)(TODO|FIXME)",
    r"!\[allow\(clippy::.+\)\]",
];

fn collect_rust_code() -> Vec<String> {
    let files = read_dir("/Users/macronova/Desktop/rust-stack")
        .expect("Directory should exist")
        .collect::<Result<Vec<_>, _>>()
        .expect("Files should be present");
    let parquet_batches = files
        .into_iter()
        .progress()
        .flat_map(|entry| {
            ParquetRecordBatchReaderBuilder::try_new(
                File::open(entry.path()).expect("File should be readable"),
            )
            .expect("Parquet file should be present")
            .build()
            .expect("Parquet file should be readable")
        })
        .collect::<Result<Vec<_>, _>>()
        .expect("Parquet file should be valid");
    parquet_batches
        .into_par_iter()
        .progress()
        .flat_map(|batch| {
            batch
                .column_by_name("content")
                .expect("Content column should be present")
                .as_string::<i32>()
                .iter()
                .map(|os| os.unwrap_or_default().to_string())
                .collect::<Vec<_>>()
        })
        .filter(|code| code.len() < MAX_CODE_LENGTH)
        .collect()
}

async fn bench_fts_query((reader, pattern): (FullTextIndexReader<'_>, &str)) {
    reader
        .search(pattern)
        .await
        .expect("FTS match should not fail");
}

async fn bench_literal_expr((reader, pattern): (FullTextIndexReader<'_>, ChromaRegex)) {
    reader
        .match_literal_expression(&pattern.hir().clone().into())
        .await
        .expect("Regex match should not fail");
}

fn bench_literal(criterion: &mut Criterion) {
    let runtime = tokio_multi_thread();
    let source_code_chunk = collect_rust_code();

    let temp_dir = tempdir().expect("Temporary directory should be creatable");
    let storage = Storage::Local(LocalStorage::new(
        temp_dir
            .path()
            .as_os_str()
            .to_str()
            .expect("Temporary path should be valid"),
    ));
    let arrow_blockfile_provider = ArrowBlockfileProvider::new(
        storage.clone(),
        BLOCK_SIZE,
        new_cache_for_test(),
        new_cache_for_test(),
    );
    let blockfile_provider = BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
    let prefix_path = String::from("");
    let blockfile_writer = runtime
        .block_on(blockfile_provider.write::<u32, Vec<u32>>(
            BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations(),
        ))
        .expect("Blockfile writer should be creatable");
    let blockfile_id = blockfile_writer.id();
    let tokenizer = NgramTokenizer::new(3, 3, false).expect("Tokenizer should be creatable");
    let mut full_text_writer = FullTextIndexWriter::new(blockfile_writer, tokenizer.clone());
    full_text_writer
        .handle_batch(source_code_chunk.iter().enumerate().map(|(index, code)| {
            DocumentMutation::Create {
                offset_id: index as u32,
                new_document: code,
            }
        }))
        .expect("Full text writer should be writable");
    runtime
        .block_on(full_text_writer.write_to_blockfiles())
        .expect("Blockfile should be writable");
    let flusher = runtime
        .block_on(full_text_writer.commit())
        .expect("Changes should be commitable");
    runtime
        .block_on(flusher.flush())
        .expect("Changes should be flushable");
    let reader_options = BlockfileReaderOptions::new(blockfile_id, prefix_path);
    let full_text_readar = FullTextIndexReader::new(
        runtime
            .block_on(blockfile_provider.read(reader_options))
            .expect("Blockfile reader should be creatable"),
        tokenizer,
    );

    for pattern in FTS_PATTERNS {
        bench_run(
            &format!("FTS-[{pattern}]"),
            criterion,
            &runtime,
            || (full_text_readar.clone(), *pattern),
            bench_fts_query,
        );
        bench_run(
            &format!("REGEX-[{pattern}]"),
            criterion,
            &runtime,
            || {
                (
                    full_text_readar.clone(),
                    pattern
                        .to_string()
                        .try_into()
                        .expect("Regex should be valid"),
                )
            },
            bench_literal_expr,
        );
    }

    for pattern in REGEX_PATTERNS {
        bench_run(
            &format!("REGEX-[{pattern}]"),
            criterion,
            &runtime,
            || {
                (
                    full_text_readar.clone(),
                    pattern
                        .to_string()
                        .try_into()
                        .expect("Regex should be valid"),
                )
            },
            bench_literal_expr,
        );
    }
}

criterion_group!(benches, bench_literal);
criterion_main!(benches);
