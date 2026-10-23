#![recursion_limit = "256"]
#![allow(dead_code, unused_imports, unused_attributes)]

// Include the benchmark to exercise its query-only entry point with the normal test harness.
#[path = "../benches/hierarchical_spann_profile_quantized.rs"]
mod benchmark;

use benchmark::hierarchical_index;
