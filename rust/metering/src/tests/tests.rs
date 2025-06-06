// NOTE(c-gamble): Procedural macros cannot be used in the same crates in which they are defined.
// Instead, it is recommended to create a `tests/` directory in which to write unit and integration
// tests. See https://github.com/rust-lang/rust/issues/110247 for additional information.

#[cfg(test)]
mod tests {
    use super::initialize_metering;

    initialize_metering! {
        #[attribute(name = "test_attribute")]
        type TestAttribute = Option<String>;

        #[event]
        struct TestEvent {
            test_unannotated_field: u64,
            #[field(attribute = "test_attribute", mutator = "test_mutator")]
            test_annotated_field: Option<String>
        }
    }

    fn test_mutator(event: &mut TestEvent, value: Option<String>) {
        event.test_annotated_field = value;
    }

    #[test]
    fn test_register_custom_receiver() {}

    #[tokio::test]
    async fn test_single_metering_event() {}

    #[tokio::test]
    async fn test_many_metering_events_uniform_type_single_context() {}

    #[tokio::test]
    async fn test_many_metering_events_varying_type_single_context() {}

    #[tokio::test]
    async fn test_many_metering_events_uniform_type_multi_context() {}

    #[tokio::test]
    async fn test_many_metering_events_varying_type_multi_context() {}

    #[tokio::test]
    async fn test_metered_future() {}
}
