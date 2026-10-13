use super::CompactionDiagnostics;
use std::sync::{Arc, Mutex};
use tracing_subscriber::prelude::*;

#[test]
fn allocator_stats_are_supplied_by_the_worker() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured = events.clone();
    let diagnostics = CompactionDiagnostics::with_emitter(Arc::new(move |v| {
        captured.lock().unwrap().push(v);
    }))
    .with_allocator_stats(|| serde_json::json!({"allocated_bytes": 123}));
    diagnostics.snapshot();
    assert_eq!(
        events.lock().unwrap()[0]["data"]["memory"]["jemalloc"]["allocated_bytes"],
        123
    );
}

#[test]
fn active_nested_stages_survive_snapshots_and_close_without_claiming_success() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured = events.clone();
    let diagnostics =
        CompactionDiagnostics::with_emitter(Arc::new(move |v| captured.lock().unwrap().push(v)));
    let subscriber = tracing_subscriber::registry().with(diagnostics.clone());
    tracing::subscriber::with_default(subscriber, || {
        let job = tracing::info_span!("CompactionManager::compact", collection_id = "abc");
        let _job = job.enter();
        let middle = tracing::info_span!("untracked component");
        let _middle = middle.enter();
        {
            let phase = tracing::info_span!(
                "Write offset values and compute block maxes",
                diagnostic.postings = tracing::field::Empty
            );
            let _phase = phase.enter();
            phase.record("diagnostic.postings", 42_u64);
            diagnostics.snapshot();
        }
        diagnostics.snapshot();
    });
    diagnostics.snapshot();
    let events = events.lock().unwrap();
    let snapshots: Vec<_> = events
        .iter()
        .filter(|e| e["event"] == "compaction_memory_snapshot")
        .collect();
    assert_eq!(
        snapshots[0]["data"]["active_stages"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        snapshots[0]["data"]["active_stages"][1]["diagnostic.postings"],
        42
    );
    assert_eq!(
        snapshots[0]["data"]["active_stages"][0]["job_attempt_id"],
        snapshots[0]["data"]["active_stages"][1]["job_attempt_id"]
    );
    assert_eq!(
        snapshots[1]["data"]["active_stages"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(snapshots[2]["data"]["active_stages"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        events
            .iter()
            .filter(|e| e["event"] == "compaction_stage_finish")
            .count(),
        2
    );
    assert!(events
        .iter()
        .filter(|e| e["event"] == "compaction_stage_finish")
        .all(|e| e["data"]["finish_reason"] == "span_closed"));
}

#[test]
fn unrelated_query_spans_are_not_reported() {
    let diagnostics =
        CompactionDiagnostics::with_emitter(Arc::new(|_| panic!("unexpected diagnostic")));
    tracing::subscriber::with_default(tracing_subscriber::registry().with(diagnostics), || {
        let span = tracing::info_span!("Apply materialized logs");
        let _guard = span.enter();
    });
}

#[test]
fn simultaneous_attempts_of_the_same_collection_have_distinct_identity() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured = events.clone();
    let diagnostics =
        CompactionDiagnostics::with_emitter(Arc::new(move |v| captured.lock().unwrap().push(v)));
    tracing::subscriber::with_default(
        tracing_subscriber::registry().with(diagnostics.clone()),
        || {
            let first = tracing::info_span!(parent: None, "CompactionManager::compact", collection_id = "same");
            let second = tracing::info_span!(parent: None, "CompactionManager::compact", collection_id = "same");
            diagnostics.snapshot();
            drop(first);
            drop(second);
        },
    );
    let events = events.lock().unwrap();
    let snapshot = events
        .iter()
        .find(|e| e["event"] == "compaction_memory_snapshot")
        .unwrap();
    let work = snapshot["data"]["active_stages"].as_array().unwrap();
    assert_eq!(work.len(), 2);
    assert_ne!(work[0]["job_attempt_id"], work[1]["job_attempt_id"]);
}

#[test]
fn detached_prefetch_retains_collection_identity() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured = events.clone();
    let diagnostics =
        CompactionDiagnostics::with_emitter(Arc::new(move |v| captured.lock().unwrap().push(v)));
    tracing::subscriber::with_default(
        tracing_subscriber::registry().with(diagnostics.clone()),
        || {
            let prefetch = tracing::info_span!(parent: None, "Prefetch segment", collection_id = "collection", segment_id = "segment");
            diagnostics.snapshot();
            drop(prefetch);
        },
    );
    let events = events.lock().unwrap();
    let start = events
        .iter()
        .find(|e| e["event"] == "compaction_stage_start")
        .unwrap();
    assert_eq!(start["data"]["collection_id"], "collection");
    assert_eq!(start["data"]["segment"], "segment");
}
