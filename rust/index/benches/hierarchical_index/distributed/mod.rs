//! Remote construction and durable publication for the evolving global tree.
// This module is research bench code shared by several bench and test crates,
// each of which uses only part of it; its lint cleanup is separate work.
#![allow(dead_code, unused_imports, clippy::all)]
mod construction;
pub use construction::BalanceJob;
pub mod engine;
pub mod generation;
pub mod generation_backend;
pub mod mapped_vectors;
pub mod metrics;
pub mod owner_scan;
pub mod placement;
pub mod placement_runtime;
pub mod protocol;
pub mod reader_bench;
pub mod routing;
pub mod save_runtime;
pub mod saved_reader;
pub mod transport;
pub mod vector_store;

#[cfg(test)]
mod engine_integration_tests;

pub(super) mod ingestion;

pub mod shared_runtime;

pub mod reporting;

pub mod observability;

pub mod profile_gate;

mod local_transfer;

mod batched_transfer;
