//! Pure unit tests: model-string mapping, trajectory-to-event projections, and
//! the wire serialization of [`AgentSseEvent`].

use super::super::events::{
    action_event, action_text, observation_event, usage_event, AgentSseEvent, AgentToolCall,
};
use super::super::{default_model, default_system_prompt, AgentRequest};
use chroma_agent::{ActionBuilder, AnthropicModel, Call, InferenceUsage, ObservationBuilder, Reasoning};
use serde_json::json;
use validator::Validate;

#[test]
fn request_defaults_model_and_omits_system_prompt() {
    // Only `input` is required; `model` and `system` fall back to defaults.
    let request: AgentRequest =
        serde_json::from_value(json!({ "input": "hello" })).expect("deserialize");
    assert_eq!(request.model, AnthropicModel::Sonnet4_5.id());
    assert_eq!(request.system, default_system_prompt());

    // The default model string must always map to a known model, or every
    // model-omitting request would 400.
    assert!(default_model().parse::<AnthropicModel>().is_ok());

    // A seeded system prompt + explicit model round-trips through the body.
    let seeded: AgentRequest = serde_json::from_value(
        json!({ "input": "hi", "model": AnthropicModel::Opus4_5.id(), "system": "be terse" }),
    )
    .expect("deserialize");
    assert_eq!(seeded.model, AnthropicModel::Opus4_5.id());
    assert_eq!(seeded.system, "be terse");
}

#[test]
fn request_validation_requires_non_empty_query() {
    let empty = AgentRequest {
        input: String::new(),
        model: default_model(),
        system: default_system_prompt(),
    };
    assert!(empty.validate().is_err());

    let ok = AgentRequest {
        input: "hi".to_string(),
        model: default_model(),
        system: default_system_prompt(),
    };
    assert!(ok.validate().is_ok());
}

#[test]
fn not_provisioned_resolve_error_maps_to_not_found() {
    use super::super::AgentRouteError;
    use crate::wiki::WikiClientError;
    use chroma::client::ChromaHttpClientError;
    use chroma_error::{ChromaError, ErrorCodes};
    use reqwest::StatusCode;

    // A 404 resolving the wiki collection means Foundation isn't provisioned
    // for the tenant; the route must surface NotFound (404), not a 500, so
    // dashboard-api can render its "set up Foundation" reply.
    let not_found = AgentRouteError::Resolve(WikiClientError::Client(
        ChromaHttpClientError::ApiError("collection not found".to_string(), StatusCode::NOT_FOUND),
    ));
    assert!(matches!(not_found.code(), ErrorCodes::NotFound));

    // Other downstream failures still collapse to Internal (500).
    let server_err = AgentRouteError::Resolve(WikiClientError::Client(
        ChromaHttpClientError::ApiError("boom".to_string(), StatusCode::INTERNAL_SERVER_ERROR),
    ));
    assert!(matches!(server_err.code(), ErrorCodes::Internal));
}

#[test]
fn action_event_splits_reasoning_text_and_calls() {
    let mut builder = ActionBuilder::new();
    builder.set_reasoning(Reasoning {
        text: "thinking".to_string(),
        signature: None,
    });
    builder.push_send_user_text("hello");
    builder.push_call(Call {
        name: "search".to_string(),
        params: json!({ "query": "q" }),
        id: "call_1".to_string(),
    });
    let action = builder.build();

    match action_event(&action) {
        AgentSseEvent::Action {
            reasoning,
            text,
            calls,
        } => {
            assert_eq!(reasoning.as_deref(), Some("thinking"));
            assert_eq!(text, vec!["hello".to_string()]);
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].name, "search");
            assert_eq!(calls[0].id, "call_1");
        }
        other => panic!("expected Action, got {other:?}"),
    }
    assert_eq!(action_text(&action), "hello");
}

#[test]
fn observation_event_keeps_tool_results_only() {
    let mut builder = ObservationBuilder::new();
    builder.push_user("ignored");
    builder.push_tool_result("call_1", "ok", None);
    builder.push_tool_error("call_2", "boom");
    let observation = builder.build();

    match observation_event(&observation) {
        AgentSseEvent::Observation { results } => {
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].call_id, "call_1");
            assert!(!results[0].is_error);
            assert_eq!(results[1].call_id, "call_2");
            assert!(results[1].is_error);
        }
        other => panic!("expected Observation, got {other:?}"),
    }
}

#[test]
fn events_serialize_with_type_and_data_tags() {
    let action = AgentSseEvent::Action {
        reasoning: None,
        text: vec![],
        calls: vec![AgentToolCall {
            id: "c1".to_string(),
            name: "search".to_string(),
            params: json!({ "query": "q" }),
        }],
    };
    let value = serde_json::to_value(&action).expect("serialize");
    assert_eq!(value["type"], "action");
    assert_eq!(value["data"]["calls"][0]["name"], "search");
    // Empty reasoning/text are omitted from the wire form.
    assert!(value["data"].get("reasoning").is_none());
    assert!(value["data"].get("text").is_none());

    let done = serde_json::to_value(AgentSseEvent::Done {
        final_text: "answer".to_string(),
    })
    .expect("serialize");
    assert_eq!(done["type"], "done");
    assert_eq!(done["data"]["final_text"], "answer");

    let usage = serde_json::to_value(usage_event(&InferenceUsage {
        model: "scout".to_string(),
        input_tokens: 123,
        output_tokens: 456,
    }))
    .expect("serialize");
    assert_eq!(usage["type"], "usage");
    assert_eq!(usage["data"]["model"], "scout");
    assert_eq!(usage["data"]["input_tokens"], 123);
    assert_eq!(usage["data"]["output_tokens"], 456);
}
