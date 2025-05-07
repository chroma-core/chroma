mod proptest_helpers;

// Tip: run with `RUST_LOG=prop_test_garbage_collection=debug,garbage_collector=trace`
#[cfg(test)]
mod tests {
    use crate::proptest_helpers::garbage_collector_under_test::GarbageCollectorUnderTest;
    use proptest_state_machine::prop_state_machine;

    prop_state_machine! {
        fn run_test(
            sequential
            1..50
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
