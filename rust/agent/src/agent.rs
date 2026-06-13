//! The agent state machine and behavior composition.
//!
//! [`Agent`] drives the `infer -> act -> observe` loop, mirroring the Python
//! `Agent` class. It exposes the same two modes:
//!
//! - **Manual driving** (for RL / external control): call [`Agent::reset`],
//!   [`Agent::observe`], [`Agent::infer`], [`Agent::act`], and
//!   [`Agent::is_done`] yourself, inspecting/modifying state between steps.
//! - **Automatic driving**: [`Agent::run`] is the built-in runner (Python's
//!   `__call__`); it resets, observes the initial observation, then loops until
//!   the agent is done.
//!
//! Python composed behavior via subclassing + `super()` chaining. Rust trait
//! default methods have no `super`, so we instead use composable
//! [`AgentBehavior`] hooks (middleware): the [`Agent`] holds a `Vec` of
//! behaviors and invokes each hook in registration order. The stored
//! [`Trajectory`] is always the complete, un-pruned history; behaviors that
//! want a masked view mutate the *clone* handed to
//! [`AgentBehavior::prepare_for_inference`], leaving the record intact.

use futures::future::join_all;

use crate::error::AgentError;
use crate::inference::{AgentInferenceModel, InferenceContext};
use crate::tool::{ToolCallMetadata, ToolSet};
use crate::trajectory::{
    Action, ActionItem, Call, Entry, Observation, ObservationBuilder, Trajectory, TrajectoryBuilder,
};

/// Default cap on trajectory entries, matching the Python default.
const DEFAULT_MAX_TRAJECTORY_LENGTH: usize = 32;

/// How [`Agent::act`] handles a tool call that fails (errors or unknown tool).
///
/// A failed call still leaves an unanswered `tool_use` in the trajectory; how
/// it is resolved is the difference between a self-correcting agent and a hard
/// stop. Note that providers like Anthropic *require* every `tool_use` to have
/// a matching `tool_result` in the next turn, so [`Self::ReportToModel`] is
/// what keeps a continuing run well-formed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ToolErrorPolicy {
    /// Record the error as a `tool_result` (marked `is_error`) so the model
    /// sees the failure and can self-correct; the run continues. Default.
    #[default]
    ReportToModel,
    /// Abort [`Agent::act`] and propagate the error, terminating the run.
    Terminate,
}

/// Composable lifecycle hooks layered onto the [`Agent`] driver.
///
/// Each hook has an empty default, so a behavior overrides only what it needs.
/// Behaviors hold their own state and are invoked in registration order,
/// reproducing the Python `super()` chain (e.g. dedup `after_act` then budget
/// `after_act`). For the dummy-weather milestone the behavior vec is empty.
pub trait AgentBehavior: Send + Sync {
    /// Called by [`Agent::reset`]; clear any per-run state.
    fn reset(&mut self) {}

    /// Mutate the (cloned, possibly-masked) inference view before it is sent to
    /// the model. The [`Agent`]'s stored trajectory is never touched here.
    fn prepare_for_inference(&mut self, _ctx: &mut InferenceContext<'_>) {}

    /// Called once per tool call just before it executes.
    // Later milestones will let this return `Option<Box<dyn Any + Send>>` to
    // inject a tool's `RuntimeParams`.
    fn before_tool_call(&mut self, _call: &Call) {}

    /// Called once per tool call just after it returns.
    fn after_tool_call(
        &mut self,
        _call: &Call,
        _output: &str,
        _metadata: &Option<ToolCallMetadata>,
    ) {
    }

    /// Mutate the observation produced by a completed [`Agent::act`] step.
    fn after_act(&mut self, _observation: &mut Observation) {}

    /// Mutate an observation as it is recorded via [`Agent::observe`].
    fn on_observe(&mut self, _observation: &mut Observation) {}
}

/// The agent driver: owns the toolset, inference model, behaviors, and the
/// in-progress trajectory.
pub struct Agent {
    toolset: ToolSet,
    inference_model: Box<dyn AgentInferenceModel>,
    behaviors: Vec<Box<dyn AgentBehavior>>,
    max_trajectory_length: usize,
    tool_error_policy: ToolErrorPolicy,
    builder: TrajectoryBuilder,
}

impl Agent {
    /// Create an agent with no behaviors and the default trajectory cap.
    pub fn new(toolset: ToolSet, inference_model: Box<dyn AgentInferenceModel>) -> Self {
        Self {
            toolset,
            inference_model,
            behaviors: Vec::new(),
            max_trajectory_length: DEFAULT_MAX_TRAJECTORY_LENGTH,
            tool_error_policy: ToolErrorPolicy::default(),
            builder: TrajectoryBuilder::new(),
        }
    }

    /// Register a behavior (invoked after any already-registered behaviors).
    pub fn with_behavior(mut self, behavior: Box<dyn AgentBehavior>) -> Self {
        self.behaviors.push(behavior);
        self
    }

    /// Set how a failed tool call is handled (see [`ToolErrorPolicy`]).
    pub fn with_tool_error_policy(mut self, policy: ToolErrorPolicy) -> Self {
        self.tool_error_policy = policy;
        self
    }

    /// Override the maximum number of trajectory entries before [`Agent::infer`]
    /// errors with [`AgentError::MaxTrajectoryLengthExceeded`].
    pub fn with_max_trajectory_length(mut self, max: usize) -> Self {
        self.max_trajectory_length = max;
        self
    }

    /// Borrow the full, un-pruned trajectory recorded so far.
    pub fn trajectory(&self) -> &Trajectory {
        self.builder.trajectory()
    }

    /// Clear the trajectory and reset every behavior.
    pub fn reset(&mut self) {
        self.builder = TrajectoryBuilder::new();
        for behavior in &mut self.behaviors {
            behavior.reset();
        }
    }

    /// Record an observation, running `on_observe` hooks first.
    pub fn observe(&mut self, mut observation: Observation) {
        for behavior in &mut self.behaviors {
            behavior.on_observe(&mut observation);
        }
        self.builder.push_observation(observation);
    }

    /// True once the last entry is a terminal action (no tool calls left to
    /// run); an action of only [`ActionItem::SendUserText`] (or no items) ends
    /// the run. Mirrors Python's "no non-text tool calls" check.
    pub fn is_done(&self) -> bool {
        match self.builder.trajectory().entries.last() {
            Some(Entry::Action(action)) => !action
                .items
                .iter()
                .any(|item| matches!(item, ActionItem::Call(_))),
            _ => false,
        }
    }

    /// Produce the next action from the current (masked) trajectory view.
    ///
    /// Returns `Err(MaxTrajectoryLengthExceeded)` if the trajectory has already
    /// hit its cap, or `Ok(None)` when the model returns nothing actionable.
    pub async fn infer(&mut self) -> Result<Option<Action>, AgentError> {
        if self.builder.len() >= self.max_trajectory_length {
            return Err(AgentError::MaxTrajectoryLengthExceeded(
                self.max_trajectory_length,
            ));
        }

        // The stored trajectory is the source of truth; behaviors mask a clone.
        let mut ctx = InferenceContext {
            trajectory: self.builder.trajectory().clone(),
            toolset: &self.toolset,
            max_tokens: None,
        };
        for behavior in &mut self.behaviors {
            behavior.prepare_for_inference(&mut ctx);
        }

        self.inference_model.infer(&ctx).await
    }

    /// Record `action`, then execute its tool calls (in parallel) and return the
    /// resulting observation. Returns `Ok(None)` for a terminal action with no
    /// tool calls.
    ///
    /// A failed tool call is handled per the [`ToolErrorPolicy`]: by default the
    /// error becomes a `tool_result` (so the observation answers every
    /// `tool_use` and the model can self-correct); under
    /// [`ToolErrorPolicy::Terminate`] the error propagates. Either way the
    /// `action` has already been recorded before this returns.
    pub async fn act(&mut self, action: Action) -> Result<Option<Observation>, AgentError> {
        let calls: Vec<Call> = action
            .items
            .iter()
            .filter_map(|item| match item {
                ActionItem::Call(call) => Some(call.clone()),
                ActionItem::SendUserText(_) => None,
            })
            .collect();

        self.builder.push_action(action);

        if calls.is_empty() {
            return Ok(None);
        }

        for call in &calls {
            for behavior in &mut self.behaviors {
                behavior.before_tool_call(call);
            }
        }

        let toolset = &self.toolset;
        let results = join_all(calls.iter().map(|call| async move {
            let tool = toolset
                .get(&call.name)
                .ok_or_else(|| AgentError::UnknownTool(call.name.clone()))?;
            // RuntimeParams injection is a later milestone; pass `None` for now.
            let (output, metadata) = tool.call_json(call.params.clone(), None).await?;
            Ok::<(String, String, Option<ToolCallMetadata>), AgentError>((
                call.id.clone(),
                output,
                metadata,
            ))
        }))
        .await;

        let mut observation_builder = ObservationBuilder::new();
        for (call, result) in calls.iter().zip(results) {
            match result {
                Ok((call_id, output, metadata)) => {
                    for behavior in &mut self.behaviors {
                        behavior.after_tool_call(call, &output, &metadata);
                    }
                    observation_builder.push_tool_result(call_id, output, metadata);
                }
                // Terminate: propagate, leaving the recorded action unanswered.
                Err(error) if self.tool_error_policy == ToolErrorPolicy::Terminate => {
                    return Err(error);
                }
                // ReportToModel: surface the error as a tool_result so the
                // observation answers every tool_use and the model can correct.
                Err(error) => {
                    let message = error.to_string();
                    for behavior in &mut self.behaviors {
                        behavior.after_tool_call(call, &message, &None);
                    }
                    observation_builder.push_tool_error(&call.id, message);
                }
            }
        }

        let mut observation = observation_builder.build();
        for behavior in &mut self.behaviors {
            behavior.after_act(&mut observation);
        }
        Ok(Some(observation))
    }

    /// Auto-drive the agent from `initial_observation` to completion, returning
    /// the full trajectory. This is the default runner (Python's `__call__`).
    pub async fn run(
        &mut self,
        initial_observation: Observation,
    ) -> Result<Trajectory, AgentError> {
        self.reset();
        self.observe(initial_observation);

        while !self.is_done() {
            let action = match self.infer().await? {
                Some(action) => action,
                None => break,
            };
            if let Some(observation) = self.act(action).await? {
                self.observe(observation);
            }
        }

        Ok(self.builder.trajectory().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inference::AgentInferenceModel;
    use crate::tools::weather::GetWeatherTool;
    use crate::trajectory::{ActionBuilder, Entry, ObservationBuilder, ObservationItem};
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Offline inference model: calls `get_weather` once, then ends with text.
    ///
    /// The decision is purely a function of trajectory state (whether a tool
    /// result already exists), so no interior mutability is needed.
    struct StubInferenceModel;

    #[async_trait]
    impl AgentInferenceModel for StubInferenceModel {
        async fn infer(&self, ctx: &InferenceContext<'_>) -> Result<Option<Action>, AgentError> {
            let has_tool_result = ctx.trajectory.entries.iter().any(|entry| {
                matches!(entry, Entry::Observation(obs)
                    if obs.items.iter().any(|i| matches!(i, ObservationItem::ToolResult { .. })))
            });

            let mut action = ActionBuilder::new();
            if has_tool_result {
                action.push_send_user_text("The weather in Paris is 72F and sunny.");
            } else {
                action.push_call(Call {
                    name: "get_weather".to_string(),
                    params: json!({ "location": "Paris" }),
                    id: "call_1".to_string(),
                });
            }
            Ok(Some(action.build()))
        }
    }

    fn weather_agent() -> Agent {
        let mut toolset = ToolSet::new();
        toolset.add(GetWeatherTool);
        Agent::new(toolset, Box::new(StubInferenceModel))
    }

    #[tokio::test]
    async fn run_drives_to_completion() {
        let mut agent = weather_agent();
        let mut initial = ObservationBuilder::new();
        initial.push_user("What's the weather in Paris?");

        let trajectory = agent.run(initial.build()).await.expect("run succeeds");

        // user obs -> action(call) -> tool-result obs -> action(text).
        assert_eq!(trajectory.entries.len(), 4);
        assert_eq!(trajectory.num_actions(), 2);

        // The tool actually ran via the toolset.
        let tool_output = trajectory.entries.iter().find_map(|entry| match entry {
            Entry::Observation(obs) => obs.items.iter().find_map(|item| match item {
                ObservationItem::ToolResult { text, .. } => Some(text.clone()),
                _ => None,
            }),
            _ => None,
        });
        assert_eq!(
            tool_output.as_deref(),
            Some("It is 72F and sunny in Paris.")
        );

        // Final entry is a terminal text action.
        match trajectory.entries.last().expect("non-empty") {
            Entry::Action(action) => assert!(action
                .items
                .iter()
                .all(|item| matches!(item, ActionItem::SendUserText(_)))),
            other => panic!("expected terminal action, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn behavior_hooks_fire_during_run() {
        struct CountObserves {
            count: Arc<AtomicUsize>,
        }
        impl AgentBehavior for CountObserves {
            fn on_observe(&mut self, _observation: &mut Observation) {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
        }

        let observes = Arc::new(AtomicUsize::new(0));
        let mut agent = weather_agent().with_behavior(Box::new(CountObserves {
            count: observes.clone(),
        }));

        let mut initial = ObservationBuilder::new();
        initial.push_user("What's the weather in Paris?");
        agent.run(initial.build()).await.expect("run succeeds");

        // Initial user observation + the tool-result observation.
        assert_eq!(observes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn manual_driving_steps() {
        let mut agent = weather_agent();
        agent.reset();

        let mut initial = ObservationBuilder::new();
        initial.push_user("What's the weather in Paris?");
        agent.observe(initial.build());
        assert!(!agent.is_done());

        let action = agent.infer().await.expect("infer").expect("action");
        let observation = agent.act(action).await.expect("act").expect("observation");
        agent.observe(observation);
        assert!(!agent.is_done());

        let action = agent.infer().await.expect("infer").expect("action");
        assert!(agent.act(action).await.expect("act").is_none());
        assert!(agent.is_done());
    }

    #[tokio::test]
    async fn infer_errors_when_trajectory_cap_hit() {
        let mut agent = weather_agent().with_max_trajectory_length(0);
        let mut initial = ObservationBuilder::new();
        initial.push_user("hi");
        agent.observe(initial.build());

        let err = agent.infer().await.expect_err("cap exceeded");
        assert!(matches!(err, AgentError::MaxTrajectoryLengthExceeded(0)));
    }

    #[derive(serde::Deserialize, schemars::JsonSchema)]
    struct NoParams {}

    /// A tool that always fails.
    struct BoomTool;

    #[async_trait]
    impl crate::tool::Tool for BoomTool {
        type ModelSuppliedParams = NoParams;
        type RuntimeParams = ();

        fn name(&self) -> &str {
            "boom"
        }
        fn description(&self) -> &str {
            "Always fails."
        }
        async fn call(
            &self,
            _params: NoParams,
            _runtime: (),
        ) -> Result<(String, Option<ToolCallMetadata>), AgentError> {
            Err(AgentError::Unsupported("boom exploded".to_string()))
        }
    }

    /// Calls `boom` first; once it sees a tool result, ends with text.
    struct BoomThenText;

    #[async_trait]
    impl AgentInferenceModel for BoomThenText {
        async fn infer(&self, ctx: &InferenceContext<'_>) -> Result<Option<Action>, AgentError> {
            let has_tool_result = ctx.trajectory.entries.iter().any(|entry| {
                matches!(entry, Entry::Observation(obs)
                    if obs.items.iter().any(|i| matches!(i, ObservationItem::ToolResult { .. })))
            });

            let mut action = ActionBuilder::new();
            if has_tool_result {
                action.push_send_user_text("Sorry, the tool failed.");
            } else {
                action.push_call(Call {
                    name: "boom".to_string(),
                    params: json!({}),
                    id: "call_boom".to_string(),
                });
            }
            Ok(Some(action.build()))
        }
    }

    fn boom_agent() -> Agent {
        let mut toolset = ToolSet::new();
        toolset.add(BoomTool);
        Agent::new(toolset, Box::new(BoomThenText))
    }

    #[tokio::test]
    async fn tool_error_reported_to_model_by_default() {
        let mut agent = boom_agent();
        let mut initial = ObservationBuilder::new();
        initial.push_user("do it");

        let trajectory = agent
            .run(initial.build())
            .await
            .expect("run continues past a tool error");

        // user -> action(boom) -> error observation -> terminal text action.
        assert_eq!(trajectory.entries.len(), 4);

        // The failed call still produced a tool_result, flagged as an error,
        // carrying the message — so the trajectory answers the tool_use.
        let errored = trajectory.entries.iter().find_map(|entry| match entry {
            Entry::Observation(obs) => obs.items.iter().find_map(|item| match item {
                ObservationItem::ToolResult {
                    call_id,
                    text,
                    is_error: true,
                    ..
                } => Some((call_id.clone(), text.clone())),
                _ => None,
            }),
            _ => None,
        });
        let (call_id, text) = errored.expect("an error tool result");
        assert_eq!(call_id, "call_boom");
        assert!(text.contains("boom exploded"), "got: {text}");
    }

    #[tokio::test]
    async fn tool_error_terminates_when_configured() {
        let mut agent = boom_agent().with_tool_error_policy(ToolErrorPolicy::Terminate);
        let mut initial = ObservationBuilder::new();
        initial.push_user("do it");

        let err = agent
            .run(initial.build())
            .await
            .expect_err("run propagates the tool error");
        assert!(matches!(err, AgentError::Unsupported(_)));

        // The action was recorded before the error propagated (no trailing
        // observation), so the run stopped cleanly at the failing step.
        assert!(matches!(
            agent.trajectory().entries.last(),
            Some(Entry::Action(_))
        ));
    }
}
