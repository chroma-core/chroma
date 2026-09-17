//! The price card's token rates and the agent-run pricing rule.

use serde::Deserialize;

/// Micro-USD per million input/output tokens for one model, mirroring the
/// card's `TokenRate` (chroma-core/hosted-chroma `chroma_sync_common`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub(crate) struct TokenRate {
    pub input_micros_per_m_tokens: u64,
    pub output_micros_per_m_tokens: u64,
}

impl TokenRate {
    /// Price of a call in micro-USD: tokens × rate / 1M, in integer
    /// arithmetic (same rounding as the card's own `TokenRate::price`).
    fn price(&self, input_tokens: u64, output_tokens: u64) -> u64 {
        let input = u128::from(input_tokens) * u128::from(self.input_micros_per_m_tokens);
        let output = u128::from(output_tokens) * u128::from(self.output_micros_per_m_tokens);
        u64::try_from((input + output) / 1_000_000).unwrap_or(u64::MAX)
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

/// Price one run's aggregated per-model usage `(model, input, output)` in
/// micro-USD. Usage from the request's own planner model is priced at the
/// planner rate; everything else in an agent run is context-1 subagent
/// usage. Cache tokens are not represented here at all — unpriced.
pub(crate) fn price_agent_usage(
    rates: &TokenRates,
    planner_model: &str,
    usage: &[(String, u64, u64)],
) -> u64 {
    usage
        .iter()
        .map(|(model, input, output)| {
            let rate = if model == planner_model {
                rates.planner
            } else {
                rates.context_1
            };
            rate.price(*input, *output)
        })
        .sum()
}
