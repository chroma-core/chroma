//! Live end-to-end test of the loop driver against the real Anthropic Messages
//! API. Ignored by default; requires `ANTHROPIC_API_KEY` and network access.
//!
//! This exercises the seam the offline stubs cannot: a real model emitting
//! `tool_use` blocks, parsed into the trajectory, driven through
//! [`drive_agent`], and projected into our SSE event sequence. The tool is a
//! canned stub, so the test needs only the Anthropic key — no FE collection or
//! deep-research deployment.

use super::super::drive_agent;
use super::super::events::AgentSseEvent;
use async_trait::async_trait;
use chroma_agent::{
    Agent, AgentError, AnthropicAgentInferenceModel, AnthropicModel, Tool, ToolCallMetadata,
    ToolSet,
};
use futures::StreamExt;
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
struct LookupParams {
    /// The topic to look up.
    #[allow(dead_code)]
    topic: String,
}

/// A canned knowledge-base tool: returns a fixed fact for any topic so the loop
/// can run end-to-end without a real retrieval backend.
struct LookupTool;

#[async_trait]
impl Tool for LookupTool {
    type ModelSuppliedParams = LookupParams;
    type RuntimeParams = ();

    fn name(&self) -> &str {
        "lookup_fact"
    }
    fn description(&self) -> &str {
        "Look up a fact about a topic in the knowledge base."
    }
    async fn call(
        &self,
        params: LookupParams,
        _runtime: (),
    ) -> Result<(String, Option<ToolCallMetadata>), AgentError> {
        Ok((
            format!(
                "The knowledge base says {} was founded in 2022.",
                params.topic
            ),
            None,
        ))
    }
}

#[tokio::test]
#[ignore = "requires ANTHROPIC_API_KEY and network access"]
async fn live_agent_calls_tool_then_answers() {
    let model = AnthropicAgentInferenceModel::from_env(AnthropicModel::Sonnet4_5)
        .expect("ANTHROPIC_API_KEY");

    let mut toolset = ToolSet::new();
    toolset.add(LookupTool);

    // Steer the model to exercise the full call -> observe -> answer loop, the
    // way the system-prompt seam is used in production.
    let agent = Agent::new(toolset, Box::new(model)).with_system_prompt(
        "You are a research assistant. You MUST call the `lookup_fact` tool to \
         answer any question before responding, then summarize what it returned.",
    );

    let stream = drive_agent(
        agent,
        "When was Chroma founded?".to_string(),
        "test-tenant".to_string(),
        "FOUNDATION".to_string(),
    );
    futures::pin_mut!(stream);

    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event);
    }

    // The model should have requested our tool (an action with a call),
    // observed its result, and ended with a non-empty answer.
    assert!(
        events
            .iter()
            .any(|e| matches!(e, AgentSseEvent::Action { calls, .. } if !calls.is_empty())),
        "expected at least one tool call: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, AgentSseEvent::Observation { .. })),
        "expected an observation from the tool: {events:?}"
    );
    match events.last() {
        Some(AgentSseEvent::Done { final_text, .. }) => {
            assert!(!final_text.is_empty(), "expected a non-empty final answer");
        }
        other => panic!("expected terminal done, got {other:?}"),
    }
}
