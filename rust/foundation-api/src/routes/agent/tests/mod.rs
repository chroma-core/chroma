//! `/api/agent` tests, split by type and numbered for readability.
//!
//! - `00_unit` — pure synchronous tests: model-string mapping, event
//!   projections, and event serialization.
//! - `01_drive` — the async loop driver exercised with stub inference models
//!   and tools, asserting the emitted event sequence.
//! - `02_live` — end-to-end against the real Anthropic API (`#[ignore]`,
//!   requires `ANTHROPIC_API_KEY`).

#[path = "01_drive.rs"]
mod drive;
#[path = "02_live.rs"]
mod live;
#[path = "00_unit.rs"]
mod unit;
