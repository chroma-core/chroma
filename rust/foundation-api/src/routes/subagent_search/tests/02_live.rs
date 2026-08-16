//! Live end-to-end test against the real deep-research API. Ignored by default
//! (no network in CI); run explicitly with credentials.

use super::super::events::{parse_ranked_documents, AgentEvent};
use super::super::{subagent_search_data_stream, SubagentSearchCreds};
use futures::StreamExt;

/// Live end-to-end test against the real deep-research API. Ignored by
/// default (no network in CI). Run with the credentials/endpoint set:
///
/// ```bash
/// DEEP_RESEARCH_API_URL=https://chroma-core--search-agent-api-serve.modal.run \
/// CHROMA_API_KEY=ck-... CHROMA_TENANT=... CHROMA_DATABASE=foundation-research \
/// DEEP_RESEARCH_COLLECTION=wiki_master \
/// cargo test -p foundation-api -- --ignored --nocapture live_subagent_search
/// ```
#[tokio::test]
#[ignore = "hits the live deep-research API; requires credentials"]
async fn live_subagent_search_streams_events() {
    let url = std::env::var("DEEP_RESEARCH_API_URL").expect("DEEP_RESEARCH_API_URL");
    let creds = SubagentSearchCreds {
        chroma_api_key: std::env::var("CHROMA_API_KEY").expect("CHROMA_API_KEY"),
        chroma_tenant: std::env::var("CHROMA_TENANT").expect("CHROMA_TENANT"),
        chroma_database: std::env::var("CHROMA_DATABASE").expect("CHROMA_DATABASE"),
        collection_name: std::env::var("DEEP_RESEARCH_COLLECTION")
            .expect("DEEP_RESEARCH_COLLECTION"),
    };
    let query = std::env::var("DEEP_RESEARCH_QUERY")
        .unwrap_or_else(|_| "What does the Chroma compactor do?".to_string());

    let http = reqwest::Client::new();
    let raw = subagent_search_data_stream(http, url, creds, query);
    futures::pin_mut!(raw);

    let mut final_answer: Option<String> = None;
    let mut count = 0usize;
    let mut saw_done = false;
    let mut saw_error = false;
    while let Some(item) = raw.next().await {
        let data = match item {
            Ok(data) => data,
            Err(message) => panic!("stream transport error: {message}"),
        };
        count += 1;
        match AgentEvent::parse(&data) {
            AgentEvent::Action(action) => {
                eprintln!("event: action");
                if let Some(text) = action.user_text() {
                    final_answer = Some(text.to_string());
                }
            }
            AgentEvent::Observation(_) => eprintln!("event: observation"),
            AgentEvent::Done => {
                eprintln!("event: done");
                saw_done = true;
            }
            AgentEvent::Error(error) => {
                saw_error = true;
                eprintln!("event: error -> {}", error.message);
            }
            AgentEvent::Unknown => eprintln!("event: unknown"),
        }
    }

    assert!(count > 0, "expected at least one SSE event");
    assert!(!saw_error, "upstream returned an error event");
    assert!(saw_done, "expected a terminal `done` event");

    let answer = final_answer.expect("terminal user_text answer");
    eprintln!("\n=== raw final answer ===\n{answer}");

    let documents = parse_ranked_documents(&answer);
    eprintln!("\n=== parsed {} document(s) ===", documents.len());
    for (rank, doc) in documents.iter().enumerate() {
        eprintln!("{:>3}. {}\n     {}", rank + 1, doc.id, doc.justification);
    }
    assert!(
        !documents.is_empty(),
        "expected the answer to parse into ranked documents"
    );
}
