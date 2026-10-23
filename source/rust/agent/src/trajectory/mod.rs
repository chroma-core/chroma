//! The agent trajectory: the recorded history of [`Action`]s and
//! [`Observation`]s.
//!
//! Unlike the Python original, the trajectory records tool *names* and text
//! only (the live tools live in the `ToolSet`), so it is plain
//! `#[derive(Serialize, Deserialize)]` with no custom serde or tool hydration.
//! Provider rendering goes through [`Trajectory::to_provider_format`], which
//! dispatches to a per-provider module (see [`anthropic`]); only Anthropic is
//! supported for now.

mod anthropic;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::provider::ProviderFormat;
use crate::tool::ToolCallMetadata;

/// A single tool invocation requested by the model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Call {
    pub name: String,
    /// Model-supplied params (validated/decoded when the tool runs).
    pub params: Value,
    /// Provider-assigned id used to correlate the matching tool result.
    pub id: String,
}

/// One element of an [`Action`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ActionItem {
    /// Send a text message to the user. This is the "talk to the user" action
    /// (Python modeled it as a `UserTextTool`); unlike a [`Call`] it expects no
    /// tool result back.
    SendUserText(String),
    /// A tool call.
    Call(Call),
}

/// Reasoning ("thinking") emitted alongside an action.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reasoning {
    pub text: String,
    /// Provider round-trip data (e.g. Anthropic's thinking signature); `None`
    /// for providers that don't use it.
    pub signature: Option<String>,
}

/// What the agent does in a single step: zero or more items, plus optional
/// reasoning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Action {
    pub items: Vec<ActionItem>,
    pub reasoning: Option<Reasoning>,
}

/// One element of an [`Observation`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ObservationItem {
    /// Text from the user (e.g. the initial prompt).
    User(String),
    /// The result of a tool call, correlated by `call_id`. `is_error` marks a
    /// failed call whose `text` carries the error message (rendered with the
    /// provider's error flag, e.g. Anthropic's `is_error: true`), so the model
    /// can see the failure and self-correct.
    ToolResult {
        call_id: String,
        text: String,
        metadata: Option<ToolCallMetadata>,
        is_error: bool,
    },
}

/// What the agent observes in a single step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Observation {
    pub items: Vec<ObservationItem>,
}

/// A single entry in the trajectory: either an action or an observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Entry {
    Action(Action),
    Observation(Observation),
}

/// The full, ordered history of a run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trajectory {
    pub entries: Vec<Entry>,
    pub id: Uuid,
}

impl Trajectory {
    /// Number of [`Action`] entries (i.e. agent/inference steps).
    ///
    /// Counts entries, not content blocks: an `Action` with interleaved
    /// reasoning still contributes exactly one, since one `infer()` produces
    /// one `Action` regardless of how many thinking/tool_use blocks it holds.
    pub fn num_actions(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| matches!(e, Entry::Action(_)))
            .count()
    }

    /// Render the trajectory into `provider`'s message format.
    ///
    /// The dispatch seam mirrors `Tool`/`DynTool::to_provider_format`; only
    /// Anthropic is supported for now.
    pub fn to_provider_format(&self, provider: ProviderFormat) -> Value {
        match provider {
            ProviderFormat::Anthropic => anthropic::to_messages(self),
        }
    }
}

/// Incrementally assembles an [`Action`] (e.g. while parsing model output).
#[derive(Debug, Default)]
pub struct ActionBuilder {
    items: Vec<ActionItem>,
    reasoning: Option<Reasoning>,
}

impl ActionBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_send_user_text(&mut self, text: impl Into<String>) -> &mut Self {
        self.items.push(ActionItem::SendUserText(text.into()));
        self
    }

    pub fn push_call(&mut self, call: Call) -> &mut Self {
        self.items.push(ActionItem::Call(call));
        self
    }

    pub fn set_reasoning(&mut self, reasoning: Reasoning) -> &mut Self {
        self.reasoning = Some(reasoning);
        self
    }

    /// True when the action carries neither items nor reasoning.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty() && self.reasoning.is_none()
    }

    pub fn build(self) -> Action {
        Action {
            items: self.items,
            reasoning: self.reasoning,
        }
    }
}

/// Incrementally assembles an [`Observation`].
#[derive(Debug, Default)]
pub struct ObservationBuilder {
    items: Vec<ObservationItem>,
}

impl ObservationBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_user(&mut self, text: impl Into<String>) -> &mut Self {
        self.items.push(ObservationItem::User(text.into()));
        self
    }

    /// Record a successful tool result.
    pub fn push_tool_result(
        &mut self,
        call_id: impl Into<String>,
        text: impl Into<String>,
        metadata: Option<ToolCallMetadata>,
    ) -> &mut Self {
        self.items.push(ObservationItem::ToolResult {
            call_id: call_id.into(),
            text: text.into(),
            metadata,
            is_error: false,
        });
        self
    }

    /// Record a failed tool result whose `text` is the error message. Rendered
    /// with the provider's error flag so the model can self-correct.
    pub fn push_tool_error(
        &mut self,
        call_id: impl Into<String>,
        text: impl Into<String>,
    ) -> &mut Self {
        self.items.push(ObservationItem::ToolResult {
            call_id: call_id.into(),
            text: text.into(),
            metadata: None,
            is_error: true,
        });
        self
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn build(self) -> Observation {
        Observation { items: self.items }
    }
}

/// Builds a [`Trajectory`], assigning it a fresh id.
#[derive(Debug)]
pub struct TrajectoryBuilder {
    trajectory: Trajectory,
}

impl TrajectoryBuilder {
    pub fn new() -> Self {
        Self {
            trajectory: Trajectory {
                entries: Vec::new(),
                id: Uuid::new_v4(),
            },
        }
    }

    pub fn push_action(&mut self, action: Action) -> &mut Self {
        self.trajectory.entries.push(Entry::Action(action));
        self
    }

    pub fn push_observation(&mut self, observation: Observation) -> &mut Self {
        self.trajectory
            .entries
            .push(Entry::Observation(observation));
        self
    }

    pub fn len(&self) -> usize {
        self.trajectory.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.trajectory.entries.is_empty()
    }

    /// Borrow the trajectory accumulated so far without consuming the builder.
    ///
    /// The driver needs to inspect/clone the in-progress trajectory between
    /// steps (e.g. to build a masked inference view or to return the final
    /// record), so this complements the consuming [`Self::build`].
    pub fn trajectory(&self) -> &Trajectory {
        &self.trajectory
    }

    pub fn build(self) -> Trajectory {
        self.trajectory
    }
}

impl Default for TrajectoryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_trajectory() -> Trajectory {
        let mut builder = TrajectoryBuilder::new();

        let mut user = ObservationBuilder::new();
        user.push_user("What's the weather in Paris?");
        builder.push_observation(user.build());

        let mut action = ActionBuilder::new();
        action.set_reasoning(Reasoning {
            text: "I should look up the weather.".to_string(),
            signature: Some("sig-abc".to_string()),
        });
        action.push_call(Call {
            name: "get_weather".to_string(),
            params: json!({ "location": "Paris" }),
            id: "call_1".to_string(),
        });
        builder.push_action(action.build());

        let mut result = ObservationBuilder::new();
        result.push_tool_result("call_1", "It is 72F and sunny in Paris.", None);
        builder.push_observation(result.build());

        builder.build()
    }

    #[test]
    fn serde_round_trip() {
        let trajectory = sample_trajectory();
        let json = serde_json::to_string(&trajectory).expect("serialize");
        let back: Trajectory = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(trajectory, back);
    }

    #[test]
    fn num_actions_counts_action_entries() {
        assert_eq!(sample_trajectory().num_actions(), 1);
    }

    #[test]
    fn send_user_text_renders_as_text_block() {
        let mut builder = TrajectoryBuilder::new();
        let mut action = ActionBuilder::new();
        action.push_send_user_text("Here is the forecast.");
        builder.push_action(action.build());

        let messages = builder
            .build()
            .to_provider_format(ProviderFormat::Anthropic);
        let block = &messages.as_array().expect("array")[0]["content"][0];
        assert_eq!(block["type"], "text");
        assert_eq!(block["text"], "Here is the forecast.");
    }

    #[test]
    fn to_anthropic_messages_shape() {
        let messages = sample_trajectory().to_provider_format(ProviderFormat::Anthropic);
        let messages = messages.as_array().expect("messages array");
        assert_eq!(messages.len(), 3);

        // User prompt -> user/text.
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"][0]["type"], "text");
        assert_eq!(
            messages[0]["content"][0]["text"],
            "What's the weather in Paris?"
        );

        // Action -> assistant with thinking + tool_use.
        assert_eq!(messages[1]["role"], "assistant");
        let content = messages[1]["content"].as_array().expect("content array");
        assert_eq!(content[0]["type"], "thinking");
        assert_eq!(content[0]["thinking"], "I should look up the weather.");
        assert_eq!(content[0]["signature"], "sig-abc");
        assert_eq!(content[1]["type"], "tool_use");
        assert_eq!(content[1]["id"], "call_1");
        assert_eq!(content[1]["name"], "get_weather");
        assert_eq!(content[1]["input"]["location"], "Paris");

        // Tool result -> user/tool_result (successful -> is_error false).
        assert_eq!(messages[2]["role"], "user");
        assert_eq!(messages[2]["content"][0]["type"], "tool_result");
        assert_eq!(messages[2]["content"][0]["tool_use_id"], "call_1");
        assert_eq!(
            messages[2]["content"][0]["content"][0]["text"],
            "It is 72F and sunny in Paris."
        );
        assert_eq!(messages[2]["content"][0]["is_error"], false);
    }

    #[test]
    fn tool_error_renders_with_is_error_flag() {
        let mut builder = TrajectoryBuilder::new();
        let mut action = ActionBuilder::new();
        action.push_call(Call {
            name: "get_weather".to_string(),
            params: json!({ "location": "Paris" }),
            id: "call_1".to_string(),
        });
        builder.push_action(action.build());

        let mut obs = ObservationBuilder::new();
        obs.push_tool_error("call_1", "unsupported: boom");
        builder.push_observation(obs.build());

        let messages = builder
            .build()
            .to_provider_format(ProviderFormat::Anthropic);
        let block = &messages.as_array().expect("array")[1]["content"][0];
        assert_eq!(block["type"], "tool_result");
        assert_eq!(block["tool_use_id"], "call_1");
        assert_eq!(block["is_error"], true);
        assert_eq!(block["content"][0]["text"], "unsupported: boom");
    }
}
