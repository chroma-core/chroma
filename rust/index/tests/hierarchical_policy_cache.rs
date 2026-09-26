#![allow(dead_code, unused_imports)]

// The prototype writer lives under benches, so include its modules here to
// exercise structural cache invalidation with Cargo's normal test harness.
#[path = "../benches/hierarchical_index/mod.rs"]
mod hierarchical_index;
