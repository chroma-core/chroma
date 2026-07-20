//! Loop-driver tests: drive [`drive_agent`] with stub inference models and
//! tools, asserting the emitted [`AgentSseEvent`] sequence (incl. the
//! tool-error-as-observation and inference-error paths) without any network.

use super::super::drive_agent;
use super::super::events::AgentSseEvent;
use async_trait::async_trait;
use chroma_agent::{
    Action, ActionBuilder, Agent, AgentError, AgentInferenceModel, Call, Entry, InferenceContext,
    ObservationItem, Tool, ToolCallMetadata, ToolSet,
};
use futures::StreamExt;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

/// A tool whose result text is fixed; optionally always fails.
struct StubTool {
    name: &'static str,
    fail: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct StubParams {
    #[allow(dead_code)]
    query: String,
}

#[async_trait]
impl Tool for StubTool {
    type ModelSuppliedParams = StubParams;
    type RuntimeParams = ();

    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        "stub"
    }
    async fn call(
        &self,
        _params: StubParams,
        _runtime: (),
    ) -> Result<(String, Option<ToolCallMetadata>), AgentError> {
        if self.fail {
            Err(AgentError::Tool("stub tool failed".to_string()))
        } else {
            Ok(("stub result".to_string(), None))
        }
    }
}

/// Calls `search` once, then ends with text once a tool result exists.
struct CallThenAnswer;

#[async_trait]
impl AgentInferenceModel for CallThenAnswer {
    async fn infer(&self, ctx: &InferenceContext<'_>) -> Result<Option<Action>, AgentError> {
        let has_tool_result = ctx.trajectory.entries.iter().any(|entry| {
            matches!(entry, Entry::Observation(obs)
                if obs.items.iter().any(|i|
                    matches!(i, ObservationItem::ToolResult { .. })))
        });

        let mut builder = ActionBuilder::new();
        if has_tool_result {
            builder.push_send_user_text("final answer");
        } else {
            builder.push_call(Call {
                name: "search".to_string(),
                params: json!({ "query": "q" }),
                id: "call_1".to_string(),
            });
        }
        Ok(Some(builder.build()))
    }
}

fn search_agent(fail: bool) -> Agent {
    let mut toolset = ToolSet::new();
    toolset.add(StubTool {
        name: "search",
        fail,
    });
    Agent::new(toolset, Box::new(CallThenAnswer))
}

/// Drives the typed-event loop to completion and collects every event.
async fn collect_events(agent: Agent, query: &str) -> Vec<AgentSseEvent> {
    let stream = drive_agent(
        agent,
        query.to_string(),
        "test-tenant".to_string(),
        "FOUNDATION".to_string(),
    );
    futures::pin_mut!(stream);
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event);
    }
    events
}

#[tokio::test]
async fn drives_loop_and_emits_action_observation_done() {
    let events = collect_events(search_agent(false), "hello").await;

    // action(call) -> observation -> action(text) -> done.
    assert_eq!(events.len(), 4, "events: {events:?}");
    assert!(matches!(
        &events[0],
        AgentSseEvent::Action { calls, .. } if calls.len() == 1 && calls[0].name == "search"
    ));
    match &events[1] {
        AgentSseEvent::Observation { results } => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].text, "stub result");
            assert!(!results[0].is_error);
        }
        other => panic!("expected observation, got {other:?}"),
    }
    assert!(matches!(&events[2], AgentSseEvent::Action { .. }));
    assert!(matches!(
        &events[3],
        AgentSseEvent::Done { final_text, .. } if final_text == "final answer"
    ));
}

#[tokio::test]
async fn tool_error_is_reported_as_observation_then_done() {
    let events = collect_events(search_agent(true), "hello").await;

    // The failed call still yields an observation (is_error) and the run
    // continues to a terminal answer rather than aborting.
    assert!(matches!(&events[0], AgentSseEvent::Action { .. }));
    match &events[1] {
        AgentSseEvent::Observation { results } => {
            assert!(results[0].is_error);
            assert!(results[0].text.contains("stub tool failed"));
        }
        other => panic!("expected observation, got {other:?}"),
    }
    assert!(matches!(events.last(), Some(AgentSseEvent::Done { .. })));
}

/// Answers immediately with text and never requests a tool.
struct AnswerImmediately;

#[async_trait]
impl AgentInferenceModel for AnswerImmediately {
    async fn infer(&self, _ctx: &InferenceContext<'_>) -> Result<Option<Action>, AgentError> {
        let mut builder = ActionBuilder::new();
        builder.push_send_user_text("direct answer");
        Ok(Some(builder.build()))
    }
}

#[tokio::test]
async fn answer_without_tool_calls_emits_action_then_done() {
    // A terminal text action (no calls) yields no observation: just the action
    // and the terminal done carrying that text.
    let agent = Agent::new(ToolSet::new(), Box::new(AnswerImmediately));
    let events = collect_events(agent, "hello").await;

    assert_eq!(events.len(), 2, "events: {events:?}");
    match &events[0] {
        AgentSseEvent::Action { calls, text, .. } => {
            assert!(calls.is_empty());
            assert_eq!(text, &vec!["direct answer".to_string()]);
        }
        other => panic!("expected action, got {other:?}"),
    }
    assert!(matches!(
        &events[1],
        AgentSseEvent::Done { final_text, .. } if final_text == "direct answer"
    ));
}

/// Returns nothing actionable on the first inference (the model declined).
struct NoAction;

#[async_trait]
impl AgentInferenceModel for NoAction {
    async fn infer(&self, _ctx: &InferenceContext<'_>) -> Result<Option<Action>, AgentError> {
        Ok(None)
    }
}

#[tokio::test]
async fn no_actionable_inference_ends_with_empty_done() {
    let agent = Agent::new(ToolSet::new(), Box::new(NoAction));
    let events = collect_events(agent, "hello").await;

    // No action was produced, so the only event is a `done` with no answer.
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        AgentSseEvent::Done { final_text, .. } if final_text.is_empty()
    ));
}

/// An inference model that always errors, to exercise the in-band error event
/// path.
struct AlwaysError;

#[async_trait]
impl AgentInferenceModel for AlwaysError {
    async fn infer(&self, _ctx: &InferenceContext<'_>) -> Result<Option<Action>, AgentError> {
        Err(AgentError::Unsupported("inference boom".to_string()))
    }
}

#[tokio::test]
async fn inference_error_ends_stream_with_error_event() {
    let agent = Agent::new(ToolSet::new(), Box::new(AlwaysError));
    let events = collect_events(agent, "hello").await;

    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        AgentSseEvent::Error { message } if message.contains("inference boom")
    ));
}
