mod proptest_helpers;

// Tip: run with `RUST_LOG=prop_test_garbage_collection=debug,garbage_collector=trace`
//
// Fast/default timing:
// /usr/bin/time -p cargo test -p garbage_collector --test prop_test_garbage_collection -- --exact tests::test_k8s_integration_garbage_collection --nocapture
//
// Deep timing:
// env CHROMA_GC_PROPTEST_PROFILE=deep /usr/bin/time -p cargo test -p garbage_collector --test prop_test_garbage_collection -- --exact tests::test_k8s_integration_garbage_collection --nocapture
#[cfg(test)]
mod tests {
    use crate::proptest_helpers::garbage_collector_under_test::GarbageCollectorUnderTest;
    use proptest::test_runner::{Config, FileFailurePersistence};
    use proptest_state_machine::prop_state_machine;
    use std::ops::Range;

    struct GarbageCollectorProptestProfile {
        cases: u32,
        max_shrink_iters: u32,
        transition_range: Range<usize>,
    }

    fn garbage_collector_proptest_profile() -> GarbageCollectorProptestProfile {
        match std::env::var("CHROMA_GC_PROPTEST_PROFILE").as_deref() {
            Ok("deep") => GarbageCollectorProptestProfile {
                cases: 256,
                max_shrink_iters: 1024,
                transition_range: 1..50,
            },
            _ => GarbageCollectorProptestProfile {
                cases: 32,
                max_shrink_iters: 256,
                transition_range: 1..25,
            },
        }
    }

    fn garbage_collector_proptest_config() -> Config {
        let profile = garbage_collector_proptest_profile();

        Config {
            cases: profile.cases,
            max_shrink_iters: profile.max_shrink_iters,
            // Integration tests live outside src/, so SourceParallel cannot
            // find a crate root. Keep using this test's checked-in sidecar.
            failure_persistence: Some(Box::new(FileFailurePersistence::WithSource(
                "proptest-regressions",
            ))),
            fork: false,
            ..Config::default()
        }
    }

    fn garbage_collector_transition_range() -> Range<usize> {
        garbage_collector_proptest_profile().transition_range
    }

    prop_state_machine! {
        #![proptest_config(garbage_collector_proptest_config())]
        fn run_test(
            sequential
            garbage_collector_transition_range()
            =>
          GarbageCollectorUnderTest
        );
    }

    #[test]
    fn test_k8s_integration_garbage_collection() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        run_test();
    }
}
