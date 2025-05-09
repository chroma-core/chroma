use std::collections::HashMap;
use std::time::Duration;

use chroma_benchmark::benchmark::{bench_run, tokio_multi_thread};
use chroma_benchmark::datasets::types::RecordDataset;
use chroma_benchmark::datasets::wikipedia::WikipediaDataset;
use chroma_log::test::{int_as_id, random_embedding};
use chroma_segment::test::TestDistributedSegment;
use chroma_system::Operator;
use chroma_types::{
    Chunk, DocumentExpression, DocumentOperator, LogRecord, Operation, OperationRecord,
    ScalarEncoding, SignedRoaringBitmap, Where,
};
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use futures::{StreamExt, TryStreamExt};
use indicatif::ProgressIterator;
use regex::Regex;
use roaring::RoaringBitmap;
use tokio::time::Instant;
use worker::execution::operators::filter::{FilterInput, FilterOperator};

const LOG_CHUNK_SIZE: usize = 10000;
const DOCUMENT_SIZE: usize = 100000;
const REGEX_PATTERNS: &[&str] = &[
    r"wikipedia",
    r"wikipedia.*",
    r"(?i)wikipedia",
    r"(?i)wikipedia.*",
    r"20\d\d",
    r".*wiki.*",
    r"May|June",
    r"(March|April) 19\d\d",
    r"\w{6}",
];

fn bench_regex(criterion: &mut Criterion) {
    let runtime = tokio_multi_thread();

    let (test_segment, expected_results, bruteforce_time) = runtime.block_on(async {
        let wikipedia = WikipediaDataset::init()
            .await
            .expect("Wikipedia dataset should exist");
        let records = wikipedia
            .create_records_stream()
            .await
            .expect("Wikipedia dataset should have content")
            .take(DOCUMENT_SIZE)
            .try_collect::<Vec<_>>()
            .await
            .expect("Wikipedia dataset should have valid records");

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

        let logs = records
            .into_iter()
            .progress()
            .with_message("Bruteforcing regex for reference")
            .enumerate()
            .map(|(offset, record)| {
                for (pattern_str, pattern) in &regexes {
                    let now = Instant::now();
                    let is_match = pattern.is_match(&record.document);
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
                        document: Some(record.document),
                        operation: Operation::Upsert,
                    },
                }
            })
            .collect::<Vec<_>>();
        let mut segment = TestDistributedSegment::default();
        for (idx, batch) in logs.chunks(LOG_CHUNK_SIZE).enumerate() {
            segment
                .compact_log(Chunk::new(batch.into()), idx * LOG_CHUNK_SIZE)
                .await;
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
        let filter_operator = FilterOperator {
            query_ids: None,
            where_clause: Some(Where::Document(DocumentExpression {
                operator: DocumentOperator::Regex,
                pattern: pattern.to_string(),
            })),
        };

        let routine = |(op, input, expected): (
            FilterOperator,
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
