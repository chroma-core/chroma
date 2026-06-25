//! Synchronous unit tests: event parsing/serialization, SSE line decoding, and
//! request-body building. No network or async runtime.

use super::super::events::{
    parse_ranked_documents, ActionData, AgentEvent, ErrorData, RankedDocument, SubagentSearchEvent,
};
use super::super::{parse_sse_data_line, subagent_search_payload, SubagentSearchCreds};
use serde_json::{json, Value};

fn test_creds() -> SubagentSearchCreds {
    SubagentSearchCreds {
        chroma_api_key: "tok".to_string(),
        chroma_tenant: "team".to_string(),
        chroma_database: "FOUNDATION".to_string(),
        collection_name: "wiki".to_string(),
    }
}

fn parse_action(value: Value) -> ActionData {
    match AgentEvent::parse(&value.to_string()) {
        AgentEvent::Action(action) => action,
        other => panic!("expected an action, got {other:?}"),
    }
}

#[test]
fn agent_event_parses_each_kind() {
    assert!(matches!(
        AgentEvent::parse(&json!({"type":"action","data":{"tools":[],"params":[]}}).to_string()),
        AgentEvent::Action(_)
    ));
    assert!(matches!(
        AgentEvent::parse(
            &json!({"type":"observation","data":{"step":1,"sources":["a"]}}).to_string()
        ),
        AgentEvent::Observation(_)
    ));
    assert!(matches!(
        AgentEvent::parse(&json!({"type":"done","data":{}}).to_string()),
        AgentEvent::Done
    ));
    assert!(matches!(
        AgentEvent::parse(&json!({"type":"error","data":{"message":"boom"}}).to_string()),
        AgentEvent::Error(ErrorData { message }) if message == "boom"
    ));
    // Unknown event types and unparseable lines degrade to `Unknown`.
    assert!(matches!(
        AgentEvent::parse(&json!({"type":"surprise","data":{}}).to_string()),
        AgentEvent::Unknown
    ));
    assert!(matches!(AgentEvent::parse("not json"), AgentEvent::Unknown));
}

#[test]
fn action_user_text_takes_last_and_detects_answer_only() {
    // A tool-call action with no user_text.
    let search = parse_action(json!({
        "type": "action",
        "data": { "tools": [{ "name": "search" }], "params": [{ "query": "rag" }] }
    }));
    assert_eq!(search.user_text(), None);
    assert!(!search.is_answer_only());

    // A mixed action: keeps the last user_text but is not answer-only.
    let mixed = parse_action(json!({
        "type": "action",
        "data": {
            "tools": [{ "name": "search" }, { "name": "user_text" }],
            "params": [{ "query": "rag" }, { "text": "mid-chain" }],
        }
    }));
    assert_eq!(mixed.user_text(), Some("mid-chain"));
    assert!(!mixed.is_answer_only());

    // A pure answer action.
    let answer = parse_action(json!({
        "type": "action",
        "data": { "tools": [{ "name": "user_text" }], "params": [{ "text": "final answer" }] }
    }));
    assert_eq!(answer.user_text(), Some("final answer"));
    assert!(answer.is_answer_only());
}

#[test]
fn result_event_serializes_with_documents() {
    let event = SubagentSearchEvent::Result {
        documents: vec![RankedDocument {
            id: "doc-1".to_string(),
            justification: "Relevant.".to_string(),
        }],
    };
    let value: Value = serde_json::to_value(&event).unwrap();
    assert_eq!(value["type"], "result");
    assert_eq!(value["data"]["documents"][0]["id"], "doc-1");
    assert_eq!(value["data"]["documents"][0]["justification"], "Relevant.");
}

#[test]
fn done_event_serializes_without_data() {
    let value: Value = serde_json::to_value(SubagentSearchEvent::Done).unwrap();
    assert_eq!(value, json!({ "type": "done" }));
}

#[test]
fn parses_ranked_documents_in_order() {
    // Mixes unquoted, double-, and single-quoted ids and multi-line
    // justifications, with surrounding prose the parser should ignore.
    let answer = "\
Here are the results:

<Document id=compactor-1>
<Justification>
The compactor merges log segments
into the index.
</Justification>
</Document>

<Document id=\"query-2\">
<Justification>Query nodes serve reads.</Justification>
</Document>

<Document id='gc-3'>
<Justification>GC reclaims storage.</Justification>
</Document>";

    let docs = parse_ranked_documents(answer);
    assert_eq!(
        docs,
        vec![
            RankedDocument {
                id: "compactor-1".to_string(),
                justification: "The compactor merges log segments into the index.".to_string(),
            },
            RankedDocument {
                id: "query-2".to_string(),
                justification: "Query nodes serve reads.".to_string(),
            },
            RankedDocument {
                id: "gc-3".to_string(),
                justification: "GC reclaims storage.".to_string(),
            },
        ]
    );
}

#[test]
fn parses_no_documents_from_unstructured_text() {
    assert!(parse_ranked_documents("I could not find anything relevant.").is_empty());
}

#[test]
fn ranked_document_slug_drops_chunk_suffix() {
    let slug_of = |id: &str| {
        RankedDocument {
            id: id.to_string(),
            justification: String::new(),
        }
        .slug()
        .to_string()
    };
    // The `{slug}-{chunk_id}` chunk suffix is dropped...
    assert_eq!(slug_of("getting-started-3"), "getting-started");
    assert_eq!(slug_of("compactor-1"), "compactor");
    // ...but a non-numeric trailing segment is part of the slug, not a chunk.
    assert_eq!(slug_of("release-notes-v2"), "release-notes-v2");
    assert_eq!(slug_of("overview"), "overview");
}

#[test]
fn parses_only_sse_data_lines() {
    assert_eq!(
        parse_sse_data_line(b"data: {\"type\":\"done\"}\n").as_deref(),
        Some("{\"type\":\"done\"}")
    );
    assert_eq!(parse_sse_data_line(b""), None);
    assert_eq!(parse_sse_data_line(b": keep-alive comment"), None);
    assert_eq!(parse_sse_data_line(b"event: action"), None);
}

#[test]
fn builds_subagent_search_payload() {
    let creds = test_creds();
    let payload = subagent_search_payload(&creds, "what is rag");
    assert_eq!(payload["query"], "what is rag");
    assert_eq!(payload["model"], "scout");
    assert_eq!(payload["collection_name"], "wiki");
    assert_eq!(payload["chroma_api_key"], "tok");
    assert_eq!(payload["chroma_tenant"], "team");
    assert_eq!(payload["chroma_database"], "FOUNDATION");
}
