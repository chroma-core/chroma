//! Model snapshots, `anthropic-beta` flags, and the tunable request knobs.
//!
//! These are the parts of the request that the caller chooses, separated
//! from the parts the agent loop builds.

use std::str::FromStr;

/// Opt-in feature flags sent in the `anthropic-beta` header.
///
/// The header is a comma-separated list, so several betas can be enabled at
/// once (see [`AnthropicAgentInferenceModel::with_betas`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnthropicBeta {
    /// Allow `thinking` blocks to interleave with `tool_use`
    /// (`interleaved-thinking-2025-05-14`). Pairs with the `thinking` config in
    /// [`AnthropicAgentInferenceModel::request_body`].
    InterleavedThinking,
}

impl AnthropicBeta {
    /// The flag token as it appears in the `anthropic-beta` header.
    pub fn id(self) -> &'static str {
        match self {
            AnthropicBeta::InterleavedThinking => "interleaved-thinking-2025-05-14",
        }
    }
}

/// The set of `anthropic-beta` flags enabled on a request.
///
/// [`Default`] enables interleaved thinking, which pairs with the always-on
/// `thinking` config; an empty set omits the header entirely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnthropicBetas(pub Vec<AnthropicBeta>);

impl Default for AnthropicBetas {
    fn default() -> Self {
        Self(vec![AnthropicBeta::InterleavedThinking])
    }
}

impl AnthropicBetas {
    /// Render the comma-separated `anthropic-beta` header value, or `None` when
    /// no betas are enabled (in which case the header should be omitted).
    pub(super) fn header_value(&self) -> Option<String> {
        if self.0.is_empty() {
            return None;
        }
        Some(
            self.0
                .iter()
                .map(|beta| beta.id())
                .collect::<Vec<_>>()
                .join(","),
        )
    }
}

impl From<Vec<AnthropicBeta>> for AnthropicBetas {
    fn from(betas: Vec<AnthropicBeta>) -> Self {
        Self(betas)
    }
}

/// Anthropic model snapshots offered to callers. Every variant must have a
/// billing rate: the Orb metrics and the Foundation price card match on these
/// exact wire ids, so an unpriced variant here bills its runs at $0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnthropicModel {
    /// `claude-sonnet-4-5-20250929`
    Sonnet4_5,
}

impl AnthropicModel {
    /// Every known model. Keep in sync with the enum variants; this backs
    /// [`from_str`](Self::from_str) so parsing stays a single source of truth.
    pub const ALL: [AnthropicModel; 1] = [AnthropicModel::Sonnet4_5];

    /// The API model identifier sent on the wire.
    pub fn id(self) -> &'static str {
        match self {
            AnthropicModel::Sonnet4_5 => "claude-sonnet-4-5-20250929",
        }
    }
}

/// Error returned by [`AnthropicModel::from_str`] when a string names no known
/// model.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown Anthropic model: {0}")]
pub struct UnknownAnthropicModel(pub String);

impl FromStr for AnthropicModel {
    type Err = UnknownAnthropicModel;

    /// Resolves a model from its exact wire [`id`](Self::id),
    /// case-insensitively. Only full snapshot ids are accepted; family
    /// shorthands like `opus` are intentionally rejected because they are
    /// ambiguous across snapshots.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        Self::ALL
            .into_iter()
            .find(|model| model.id().eq_ignore_ascii_case(s))
            .ok_or_else(|| UnknownAnthropicModel(s.to_string()))
    }
}

/// Tunable Messages API request knobs, separated from the required api key and
/// model so they can carry sensible defaults via [`Default`].
#[derive(Debug, Clone, PartialEq)]
pub struct AnthropicRequestConfig {
    /// Default max output tokens (an [`InferenceContext`] may override per call).
    pub max_tokens: u32,
    pub temperature: f64,
    /// Token budget for the always-on `thinking` block.
    pub thinking_budget: u32,
    /// `anthropic-beta` feature flags to enable.
    pub betas: AnthropicBetas,
}

impl Default for AnthropicRequestConfig {
    fn default() -> Self {
        Self {
            max_tokens: 4096,
            temperature: 1.0,
            thinking_budget: 6000,
            betas: AnthropicBetas::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_from_str_matches_wire_id_only() {
        // Every variant's wire id round-trips back to the variant, ignoring
        // surrounding whitespace and case.
        for model in AnthropicModel::ALL {
            assert_eq!(model.id().parse::<AnthropicModel>(), Ok(model));
            assert_eq!(
                format!("  {}  ", model.id().to_ascii_uppercase()).parse::<AnthropicModel>(),
                Ok(model)
            );
        }
        // Ambiguous family shorthands and unknown ids are rejected — including
        // full snapshot ids of models that carry no billing rate.
        for s in [
            "opus",
            "opus-4.5",
            "sonnet",
            "haiku",
            "",
            "claude-opus-4-5-20251101",
        ] {
            assert!(s.parse::<AnthropicModel>().is_err());
        }
    }

    #[test]
    fn beta_header_value_renders_and_omits() {
        assert_eq!(
            AnthropicBetas::default().header_value().as_deref(),
            Some("interleaved-thinking-2025-05-14")
        );
        assert_eq!(
            AnthropicBetas(vec![
                AnthropicBeta::InterleavedThinking,
                AnthropicBeta::InterleavedThinking,
            ])
            .header_value()
            .as_deref(),
            Some("interleaved-thinking-2025-05-14,interleaved-thinking-2025-05-14")
        );
        assert_eq!(AnthropicBetas(vec![]).header_value(), None);
    }
}
