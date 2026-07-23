//! Integration tests: drive the SSE stream against a mocked deep-research
//! dependency (`httpmock`) and assert the events we emit end-to-end.

use super::super::events::{RankedDocument, SubagentResultError};
use super::super::{collect_subagent_search_final, stream_subagent_search, SubagentSearchCreds};
use futures::StreamExt;
use httpmock::MockServer;

fn test_creds() -> SubagentSearchCreds {
    SubagentSearchCreds {
        chroma_api_key: "tok".to_string(),
        chroma_tenant: "team".to_string(),
        chroma_database: "FOUNDATION".to_string(),
        collection_name: "wiki".to_string(),
    }
}

#[tokio::test]
async fn streams_and_collects_final_from_mocked_sse() {
    let server = MockServer::start_async().await;
    // Two action events (the second carries the terminal answer) then done,
    // framed as SSE `data:` lines like the upstream emits.
    let body = concat!(
        "data: {\"type\":\"action\",\"data\":{\"tools\":[{\"name\":\"search\"}],\"params\":[{\"query\":\"rag\"}]}}\n\n",
        "data: {\"type\":\"observation\",\"data\":{\"sources\":[\"a\"]}}\n\n",
        "data: {\"type\":\"action\",\"data\":{\"tools\":[{\"name\":\"user_text\"}],\"params\":[{\"text\":\"<Document id=doc-1><Justification>Relevant to rag.</Justification></Document>\"}]}}\n\n",
        "data: {\"type\":\"usage\",\"data\":{\"model\":\"scout\",\"input_tokens\":123,\"output_tokens\":456}}\n\n",
        "data: {\"type\":\"done\",\"data\":{}}\n\n",
    );
    let mock = server
        .mock_async(|when, then| {
            when.method("POST").path("/search");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        })
        .await;

    // The stream forwards the search action + observation (2 events), drops
    // the raw `user_text` answer action, and injects `result` + `done` —
    // so: action, observation, result, done = 4.
    let stream = stream_subagent_search(
        reqwest::Client::new(),
        server.base_url(),
        test_creds(),
        "rag".to_string(),
    );
    let count = stream
        .fold(0usize, |acc, ev| async move {
            assert!(ev.is_ok());
            acc + 1
        })
        .await;
    assert_eq!(count, 4, "action + observation + result + done");

    // The collect core resolves the terminal answer into ranked documents.
    let result = collect_subagent_search_final(
        reqwest::Client::new(),
        server.base_url(),
        test_creds(),
        "rag".to_string(),
    )
    .await
    .expect("documents parse");
    assert_eq!(
        result.documents,
        vec![RankedDocument {
            id: "doc-1".to_string(),
            justification: "Relevant to rag.".to_string(),
        }]
    );
    let usage = result.usage.expect("usage should be present");
    assert_eq!(usage.model, "scout");
    assert_eq!(usage.input_tokens, 123);
    assert_eq!(usage.output_tokens, 456);
    assert_eq!(usage.cache_read_tokens, 0);
    assert_eq!(usage.cache_write_tokens, 0);
    assert_eq!(mock.calls(), 2);
}

#[tokio::test]
async fn upstream_error_event_ends_stream_with_error() {
    let server = MockServer::start_async().await;
    // A well-formed stream that ends in an `error` event instead of `done`.
    let body = concat!(
        "data: {\"type\":\"action\",\"data\":{\"tools\":[{\"name\":\"search\"}],\"params\":[{\"query\":\"rag\"}]}}\n\n",
        "data: {\"type\":\"error\",\"data\":{\"message\":\"agent exploded\"}}\n\n",
    );
    let mock = server
        .mock_async(|when, then| {
            when.method("POST").path("/search");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        })
        .await;

    let stream = stream_subagent_search(
        reqwest::Client::new(),
        server.base_url(),
        test_creds(),
        "rag".to_string(),
    );
    let events: Vec<_> = stream.collect().await;
    // The action streams through, then the error terminates the stream.
    assert_eq!(events.len(), 2);
    assert!(events[0].is_ok());
    assert!(events[1].is_err(), "the error event ends the stream");
    assert_eq!(mock.calls(), 1);
}

#[tokio::test]
async fn collect_final_propagates_upstream_error_event() {
    let server = MockServer::start_async().await;
    let body = concat!(
        "data: {\"type\":\"action\",\"data\":{\"tools\":[{\"name\":\"user_text\"}],\"params\":[{\"text\":\"<Document id=doc-1><Justification>Relevant.</Justification></Document>\"}]}}\n\n",
        "data: {\"type\":\"error\",\"data\":{\"message\":\"agent exploded\"}}\n\n",
    );
    let mock = server
        .mock_async(|when, then| {
            when.method("POST").path("/search");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        })
        .await;

    let err = collect_subagent_search_final(
        reqwest::Client::new(),
        server.base_url(),
        test_creds(),
        "rag".to_string(),
    )
    .await
    .expect_err("upstream error should fail collection");

    assert_eq!(
        err,
        SubagentResultError::Upstream("agent exploded".to_string())
    );
    assert_eq!(mock.calls(), 1);
}

#[tokio::test]
async fn answer_with_no_documents_emits_empty_result_then_done() {
    let server = MockServer::start_async().await;
    // A terminal answer that yields no `<Document>` blocks (a legitimate
    // "found nothing" result) followed by done.
    let body = concat!(
        "data: {\"type\":\"action\",\"data\":{\"tools\":[{\"name\":\"user_text\"}],\"params\":[{\"text\":\"I could not find anything relevant.\"}]}}\n\n",
        "data: {\"type\":\"done\",\"data\":{}}\n\n",
    );
    let mock = server
        .mock_async(|when, then| {
            when.method("POST").path("/search");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        })
        .await;

    // The answer-only action is dropped; result + done are injected. No error.
    let stream = stream_subagent_search(
        reqwest::Client::new(),
        server.base_url(),
        test_creds(),
        "rag".to_string(),
    );
    let events: Vec<_> = stream.collect().await;
    assert_eq!(events.len(), 2, "empty result + done");
    assert!(events.iter().all(|e| e.is_ok()), "no hits is not an error");

    // The collect core resolves the same stream to an empty document list.
    let result = collect_subagent_search_final(
        reqwest::Client::new(),
        server.base_url(),
        test_creds(),
        "rag".to_string(),
    )
    .await
    .expect("empty results are Ok");
    assert!(result.documents.is_empty());
    assert!(result.usage.is_none());
    assert_eq!(mock.calls(), 2);
}

#[tokio::test]
async fn stream_without_terminator_ends_with_synthesized_error() {
    let server = MockServer::start_async().await;
    // A stream that ends after an action, never sending `done` or `error`.
    let body = concat!(
        "data: {\"type\":\"action\",\"data\":{\"tools\":[{\"name\":\"search\"}],\"params\":[{\"query\":\"rag\"}]}}\n\n",
        "data: {\"type\":\"observation\",\"data\":{\"sources\":[\"a\"]}}\n\n",
    );
    let mock = server
        .mock_async(|when, then| {
            when.method("POST").path("/search");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        })
        .await;

    let stream = stream_subagent_search(
        reqwest::Client::new(),
        server.base_url(),
        test_creds(),
        "rag".to_string(),
    );
    let events: Vec<_> = stream.collect().await;
    // action + observation stream through, then a synthesized terminal error
    // so the caller never sees a silent close.
    assert_eq!(events.len(), 3);
    assert!(events[0].is_ok());
    assert!(events[1].is_ok());
    assert!(
        events[2].is_err(),
        "a terminator-less close should surface as a stream error"
    );
    assert_eq!(mock.calls(), 1);
}

#[tokio::test]
async fn collect_final_requires_done_after_user_text() {
    let server = MockServer::start_async().await;
    let body = "data: {\"type\":\"action\",\"data\":{\"tools\":[{\"name\":\"user_text\"}],\"params\":[{\"text\":\"<Document id=doc-1><Justification>Relevant.</Justification></Document>\"}]}}\n\n";
    let mock = server
        .mock_async(|when, then| {
            when.method("POST").path("/search");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        })
        .await;

    let err = collect_subagent_search_final(
        reqwest::Client::new(),
        server.base_url(),
        test_creds(),
        "rag".to_string(),
    )
    .await
    .expect_err("a partial answer without done should fail collection");

    assert_eq!(err, SubagentResultError::MissingTerminalEvent);
    assert_eq!(mock.calls(), 1);
}

#[tokio::test]
async fn upstream_error_status_ends_stream_with_error() {
    let server = MockServer::start_async().await;
    let mock = server
        .mock_async(|when, then| {
            when.method("POST").path("/search");
            then.status(500).body("boom");
        })
        .await;

    let stream = stream_subagent_search(
        reqwest::Client::new(),
        server.base_url(),
        test_creds(),
        "rag".to_string(),
    );
    let events: Vec<_> = stream.collect().await;
    assert_eq!(events.len(), 1, "a failed request should yield one item");
    assert!(
        events[0].is_err(),
        "the failure should surface as a stream error, not an event"
    );
    assert_eq!(mock.calls(), 1);
}

#[tokio::test]
async fn collect_final_propagates_request_failure() {
    let server = MockServer::start_async().await;
    let mock = server
        .mock_async(|when, then| {
            when.method("POST").path("/search");
            then.status(500).body("boom");
        })
        .await;

    let err = collect_subagent_search_final(
        reqwest::Client::new(),
        server.base_url(),
        test_creds(),
        "rag".to_string(),
    )
    .await
    .expect_err("request failure should fail collection");

    assert!(matches!(err, SubagentResultError::Stream(_)));
    assert_eq!(mock.calls(), 1);
}
