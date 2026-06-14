//! Tests for the `subagent_search` route, split by type and numbered for
//! readability. The numeric filename prefixes require `#[path]` (a module name
//! can't begin with a digit); the files are plain siblings of this `mod.rs`.

#[path = "01_integration.rs"]
mod integration;
#[path = "02_live.rs"]
mod live;
#[path = "00_unit.rs"]
mod unit;
