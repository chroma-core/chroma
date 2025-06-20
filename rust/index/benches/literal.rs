use chroma_benchmark::{
    benchmark::{bench_run, tokio_multi_thread},
    datasets::rust::TheStackDedupRust,
};
use chroma_blockstore::{
    arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider, BlockfileWriterOptions,
};
use chroma_cache::new_cache_for_test;
use chroma_index::fulltext::types::{DocumentMutation, FullTextIndexReader, FullTextIndexWriter};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::regex::{literal_expr::NgramLiteralProvider, ChromaRegex};
use criterion::{criterion_group, criterion_main, Criterion};
use tantivy::tokenizer::NgramTokenizer;
use tempfile::tempdir;

const BLOCK_SIZE: usize = 1 << 23;

const FTS_PATTERNS: &[&str] = &[
    r"std::ptr::",
    r"env_logger::",
    r"tracing::",
    r"futures::",
    r"tokio::",
    r"async_std::",
    r"crossbeam::",
    r"atomic::",
    r"mpsc::",
    r"Some(",
    r"Ok(",
    r"Err(",
    r"None",
    r"unwrap()",
    r"expect()",
    r"clone()",
    r"Box::new",
    r"Rc::new",
    r"RefCell::new",
    r"debug!(",
    r"error!(",
    r"warn!(",
    r"panic!(",
    r"todo!(",
    r"join!(",
    r"select!(",
    r"unimplemented!(",
    r"std::mem::transmute",
    r"std::ffi::",
    r"thread::sleep",
    r"std::fs::File::open",
    r"std::net::TcpListener",
    r"use serde::",
    r"use rand::",
    r"use tokio::",
    r"use futures::",
    r"use anyhow::",
    r"use thiserror::",
    r"use chrono::",
    r"serde::Serialize",
    r"serde::Deserialize",
    r"regex::Regex::new",
    r"chrono::DateTime",
    r"uuid::Uuid::new_v4",
    r"proc_macro::TokenStream",
    r"assert_eq!(",
    r"assert_ne!(",
    r"#[allow(dead_code)]",
    r"#[allow(unused)]",
    r"#[allow(unused_variables)]",
    r"#[allow(unused_mut)]",
    r"#[allow",
    r"#[deny",
    r"#[warn",
    r"#[cfg",
    r"#[feature",
    r"#[derive(",
    r"#[proc_macro]",
    r"#[proc_macro_derive(",
    r"#[proc_macro_attribute]",
    r"#[test]",
    r"#[tokio::test]",
    r"///",
    r"//!",
    r"test_",
    r"_tmp",
    r"_old",
];
const REGEX_PATTERNS: &[&str] = &[
    r"(?m)^\s*fn\s+\w+",
    r"(?m)^\s*pub\s+fn\s+\w+",
    r"(?m)^\s*async\s+fn\s+\w+",
    r"(?m)^\s*pub\s+async\s+fn\s+\w+",
    r"fn\s+\w+\s*\([^)]*\)\s*->\s*\w+",
    r"fn\s+\w+\s*\([^)]*Result<[^>]+>",
    r"fn\s+\w+\s*\([^)]*Option<[^>]+>",
    r"(\w+)::(\w+)\(",
    r"\w+\.\w+\(",
    r"(?m)^\s*struct\s+\w+",
    r"(?m)^\s*pub\s+struct\s+\w+",
    r"(?m)^\s*enum\s+\w+",
    r"(?m)^\s*pub\s+enum\s+\w+",
    r"(?m)^\s*trait\s+\w+",
    r"(?m)^\s*pub\s+trait\s+\w+",
    r"impl\s+(\w+)\s+for\s+(\w+)",
    r"impl\s+(\w+)",
    r"impl\s*<.*>\s*\w+",
    r"\bSelf::\w+\(",
    r"(?m)^\s*unsafe\s+fn\s+",
    r"(?m)^\s*unsafe\s+\{",
    r"\bunsafe\b",
    r"fn\s+\w+\s*<",
    r"struct\s+\w+\s*<",
    r"enum\s+\w+\s*<",
    r"impl\s*<.*>",
    r"<[A-Za-z, ]+>",
    r"\b'\w+\b",
    r"&'\w+",
    r"<'\w+>",
    r"for<'\w+>",
    r"macro_rules!\s*\w+",
    r"\w+!\s*\(",
    r"\blog!\s*\(",
    r"\bdbg!\s*\(",
    r"\bprintln!\s*\(",
    r"\bassert!\s*\(",
    r"log::\w+\(",
    r"Result<[^>]+>",
    r"Option<[^>]+>",
    r"match\s+\w+\s*\{",
    r"mod\s+tests\s*\{",
    r"async\s+fn\s+\w+",
    r"await\s*;?",
    r"std::thread::spawn",
    r"tokio::spawn",
    r"match\s+.+\s*\{",
    r"if\s+let\s+Some\(",
    r"while\s+let\s+Some\(",
    r"//.*",
    r"/\*.*?\*/",
    r"//\s*TODO",
    r"//\s*FIXME",
    r"//\s*HACK",
    r"unsafe\s*\{",
    r"<'\w+,\s*'\w+>",
    r"for<'\w+>",
    r"&'\w+\s*\w+",
    r"where\s+",
    r"T:\s*\w+",
    r"dyn\s+\w+",
    r"Box<dyn\s+\w+>",
    r"impl\s+Trait",
    r"temp\w*",
    r"foo|bar|baz",
    r"let\s+mut\s+\w+",
];

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
    let source_code_chunk = runtime.block_on(async {
        TheStackDedupRust::init()
            .await
            .expect("the-stack-dedup-rust dataset should be initializable")
            .documents()
            .await
            .expect("the dataset should contain documents")
    });

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
    let blockfile_writer = runtime
        .block_on(
            blockfile_provider
                .write::<u32, Vec<u32>>(BlockfileWriterOptions::new().ordered_mutations()),
        )
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
    let full_text_readar = FullTextIndexReader::new(
        runtime
            .block_on(blockfile_provider.read(&blockfile_id))
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
