// Compile the benchmark-only index implementation under the test harness so
// its focused NPA tests run without loading a benchmark dataset.
#[path = "../benches/hierarchical_index/mod.rs"]
mod hierarchical_index;
