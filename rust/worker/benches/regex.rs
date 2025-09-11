use std::collections::HashMap;
use std::time::Duration;

use chroma_benchmark::benchmark::{bench_run, tokio_multi_thread};
use chroma_benchmark::datasets::rust::TheStackDedupRust;
use chroma_log::test::{int_as_id, random_embedding};
use chroma_segment::test::TestDistributedSegment;
use chroma_system::Operator;
use chroma_types::operator::Filter;
use chroma_types::{
    Chunk, DocumentExpression, DocumentOperator, LogRecord, Operation, OperationRecord,
    ScalarEncoding, SignedRoaringBitmap, Where,
};
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use indicatif::ProgressIterator;
use regex::Regex;
use roaring::RoaringBitmap;
use tokio::time::Instant;
use worker::execution::operators::filter::FilterInput;

const LOG_CHUNK_SIZE: usize = 2 << 12;
const DOCUMENT_SIZE: usize = 2 << 16;
const MAX_DOCUMENT_LENGTH: usize = 1 << 12;
const REGEX_PATTERNS: &[&str] = &[
    r"std::ptr::",
    r"env_logger::",
    r"tracing::",
    r"futures::",
    r"tokio::",
    r"async_std::",
    r"crossbeam::",
    r"atomic::",
    r"mpsc::",
    r"Some\(",
    r"Ok\(",
    r"Err\(",
    r"None",
    r"unwrap\(\)",
    r"expect\(\)",
    r"clone\(\)",
    r"Box::new",
    r"Rc::new",
    r"RefCell::new",
    r"debug!\(",
    r"error!\(",
    r"warn!\(",
    r"panic!\(",
    r"todo!\(",
    r"join!\(",
    r"select!\(",
    r"unimplemented!\(",
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
    r"assert_eq!\(",
    r"assert_ne!\(",
    r"#\[allow\(dead_code\)\]",
    r"#\[allow\(unused\)\]",
    r"#\[allow\(unused_variables\)\]",
    r"#\[allow\(unused_mut\)\]",
    r"#\[allow",
    r"#\[deny",
    r"#\[warn",
    r"#\[cfg",
    r"#\[feature",
    r"#\[derive\(",
    r"#\[proc_macro\]",
    r"#\[proc_macro_derive\(",
    r"#\[proc_macro_attribute\]",
    r"#\[test\]",
    r"#\[tokio::test\]",
    r"///",
    r"//!",
    r"test_",
    r"_tmp",
    r"_old",
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

fn bench_regex(criterion: &mut Criterion) {
    let runtime = tokio_multi_thread();

    let (test_segment, expected_results, bruteforce_time) = runtime.block_on(async {
        let documents = TheStackDedupRust::init()
            .await
            .expect("the-stack-dedup-rust dataset should be initializable")
            .documents()
            .await
            .expect("the dataset should contain documents");
        let selected_documents = documents
            .into_iter()
            .filter(|document| document.len() <= MAX_DOCUMENT_LENGTH)
            .take(DOCUMENT_SIZE)
            .collect::<Vec<_>>();

        let mut expected_results = HashMap::<String, RoaringBitmap>::new();
        let mut bruteforce_time = HashMap::<_, Duration>::new();
        let regexes = REGEX_PATTERNS
            .iter()
            .map(|pattern_str| {
                (
                    *pattern_str,
                    Regex::new(pattern_str).expect("Regex pattern should be valid"),
                )
            })
            .collect::<Vec<_>>();

        let logs = selected_documents
            .into_iter()
            .progress()
            .enumerate()
            .map(|(offset, document)| {
                for (pattern_str, pattern) in &regexes {
                    let now = Instant::now();
                    let is_match = pattern.is_match(&document);
                    let elapsed = now.elapsed();
                    *bruteforce_time.entry(pattern_str.to_string()).or_default() += elapsed;
                    if is_match {
                        expected_results
                            .entry(pattern_str.to_string())
                            .or_default()
                            .insert(offset as u32);
                    }
                }
                LogRecord {
                    log_offset: offset as i64 + 1,
                    record: OperationRecord {
                        id: int_as_id(offset),
                        embedding: Some(random_embedding(3)),
                        encoding: Some(ScalarEncoding::FLOAT32),
                        metadata: None,
                        document: Some(document),
                        operation: Operation::Upsert,
                    },
                }
            })
            .collect::<Vec<_>>();
        let mut segment = TestDistributedSegment::new().await;
        for (idx, batch) in logs.chunks(LOG_CHUNK_SIZE).enumerate().progress() {
            Box::pin(segment.compact_log(Chunk::new(batch.into()), idx * LOG_CHUNK_SIZE)).await;
        }
        (segment, expected_results, bruteforce_time)
    });

    let filter_input = FilterInput {
        logs: Chunk::new(Vec::new().into()),
        blockfile_provider: test_segment.blockfile_provider,
        metadata_segment: test_segment.metadata_segment,
        record_segment: test_segment.record_segment,
    };

    for pattern in REGEX_PATTERNS {
        let filter_operator = Filter {
            query_ids: None,
            where_clause: Some(Where::Document(DocumentExpression {
                operator: DocumentOperator::Regex,
                pattern: pattern.to_string(),
            })),
        };

        let routine = |(op, input, expected): (
            Filter,
            FilterInput,
            HashMap<String, RoaringBitmap>,
        )| async move {
            let results = op
                .run(&input)
                .await
                .expect("FilterOperator should not fail");
            assert_eq!(
                results.compact_offset_ids,
                SignedRoaringBitmap::Include(expected.get(*pattern).cloned().unwrap_or_default())
            )
        };

        let setup = || {
            (
                filter_operator.clone(),
                filter_input.clone(),
                expected_results.clone(),
            )
        };

        bench_run(
            format!(
                "Pattern: [{pattern}], Result size: [{}/{DOCUMENT_SIZE}], Reference duration: [{}µs]",
                expected_results
                    .get(*pattern)
                    .map(|res| res.len())
                    .unwrap_or_default(),
                bruteforce_time
                    .get(*pattern)
                    .expect("Reference bruteforce time should be present")
                    .as_micros(),
            )
            .as_str(),
            criterion,
            &runtime,
            setup,
            routine,
        );
    }
}

criterion_group!(benches, bench_regex);
criterion_main!(benches);
