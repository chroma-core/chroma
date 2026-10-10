//! The price card's token rates and the agent-run pricing rule.

use chroma_agent::InferenceUsage;
use serde::Deserialize;

/// Micro-USD per million tokens for one model, mirroring the card's
/// `TokenRate` (chroma-core/hosted-chroma `chroma_sync_common`). A cache rate
/// the card leaves out or sets to null prices those tokens at zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub(crate) struct TokenRate {
    pub input_micros_per_m_tokens: u64,
    pub output_micros_per_m_tokens: u64,
    #[serde(default)]
    pub cache_read_micros_per_m_tokens: Option<u64>,
    #[serde(default)]
    pub cache_write_micros_per_m_tokens: Option<u64>,
}

impl TokenRate {
    /// Price of one model's usage in micro-USD: tokens × rate / 1M, in
    /// integer arithmetic (same rounding as the card's own `TokenRate::price`).
    fn price(&self, usage: &InferenceUsage) -> u64 {
        let micros_times_m = [
            (usage.input_tokens, self.input_micros_per_m_tokens),
            (usage.output_tokens, self.output_micros_per_m_tokens),
            (
                usage.cache_read_tokens,
                self.cache_read_micros_per_m_tokens.unwrap_or(0),
            ),
            (
                usage.cache_write_tokens,
                self.cache_write_micros_per_m_tokens.unwrap_or(0),
            ),
        ]
        .into_iter()
        .map(|(tokens, rate)| u128::from(tokens) * u128::from(rate))
        .sum::<u128>();
        u64::try_from(micros_times_m / 1_000_000).unwrap_or(u64::MAX)
    }
}

/// The two rates an agent run spends against: the planner model driving the
/// loop, and the context-1 deep-research subagent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub(crate) struct TokenRates {
    pub planner: TokenRate,
    pub context_1: TokenRate,
}

/// The slice of the price card this crate reads; unknown fields are the
/// card's other prices.
#[derive(Debug, Deserialize)]
pub(super) struct PriceCard {
    pub tokens: TokenRates,
}

/// Price one run's aggregated per-model usage in micro-USD. Usage from the
/// request's own planner model is priced at the planner rate; everything else
/// in an agent run is context-1 subagent usage.
pub(crate) fn price_agent_usage(
    rates: &TokenRates,
    planner_model: &str,
    usage: &[InferenceUsage],
) -> u64 {
    usage
        .iter()
        .map(|usage| {
            let rate = if usage.model == planner_model {
                rates.planner
            } else {
                rates.context_1
            };
            rate.price(usage)
        })
        .sum()
}
