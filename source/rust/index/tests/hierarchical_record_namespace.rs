//! Compile the actual namespace adapter against actual record/materializer APIs.
//! No worker server or publication path is substituted by this focused target.
#[path = "../../worker/src/execution/orchestration/hierarchical_materialized_input.rs"]
mod hierarchical_materialized_input;
#[path = "../../worker/src/execution/orchestration/hierarchical_record_namespace.rs"]
mod hierarchical_record_namespace;
