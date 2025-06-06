// NOTE(c-gamble): Procedural macros cannot be used in the same crates in which they are defined.
// Instead, it is recommended to create a `tests/` directory in which to write unit and integration
// tests. See https://github.com/rust-lang/rust/issues/110247 for additional information.

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use chroma_metering::initialize_metering;
    use tracing::Span;

    use crate::tests::metering::{MeteredFutureExt, SubmitExt};

    /// Represents a user defining their own metering module.
    mod metering {
        use super::initialize_metering;

        initialize_metering! {
            #[attribute(name = "test_attribute")]
            type TestAttribute = Option<String>;

            #[event]
            #[derive(Default, Debug, Clone)]
            pub struct TestEventA {
                // This field is not read in the tests
                #[allow(dead_code)]
                test_unannotated_field: u64,
                #[field(attribute = "test_attribute", mutator = "test_mutator_a")]
                pub test_annotated_field: Option<String>
            }

            #[event]
            #[derive(Default, Debug, Clone)]
            pub struct TestEventB {
                // This field is not read in the tests
                #[allow(dead_code)]
                test_unannotated_field: u64,
                #[field(attribute = "test_attribute", mutator = "test_mutator_b")]
                pub test_annotated_field: Option<String>
            }
        }

        fn test_mutator_a(event: &mut TestEventA, value: Option<String>) {
            event.test_annotated_field = value;
        }

        fn test_mutator_b(event: &mut TestEventB, value: Option<String>) {
            event.test_annotated_field = value;
        }
    }

    #[derive(Clone, Debug)]
    struct TestReceiver {
        messages: Arc<Mutex<Vec<String>>>,
    }

    impl TestReceiver {
        pub fn new() -> Self {
            Self {
                messages: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl chroma_system::ReceiverForMessage<Box<dyn metering::MeteringEvent>> for TestReceiver {
        async fn send(
            &self,
            message: Box<dyn metering::MeteringEvent>,
            _tracing_context: Option<Span>,
        ) -> Result<(), chroma_system::ChannelError> {
            let mut guard = self.messages.lock().unwrap();
            guard.push(format!("{:?}", message));
            Ok(())
        }
    }

    #[test]
    fn test_register_custom_receiver() {
        let custom_receiver = TestReceiver::new();
        metering::register_receiver(Box::new(custom_receiver));
        println!("Custom receiver successfully registered!")
    }

    #[tokio::test]
    async fn test_single_metering_event() {
        // Create a metering event of type `TestEventA`
        let _metering_event_guard =
            metering::create::<metering::TestEventA>(metering::TestEventA::new(100u64));

        // Set the value of `test_annotated_field` to "value"
        metering::current().test_attribute(Some("value".to_string()));

        // Pop the event off of the stack
        let expected_metering_event = metering::close::<metering::TestEventA>();
        assert!(expected_metering_event.is_some());
        let metering_event = expected_metering_event.unwrap(); // only unwrap once
        assert_eq!(
            metering_event.test_annotated_field,
            Some("value".to_string())
        );

        // Submit the event to the receiver
        metering_event.submit().await;

        // Verify that the stack is empty
        let expected_none = metering::close::<metering::TestEventA>();
        assert!(expected_none.is_none());
    }

    #[test]
    fn test_close_nonexistent_event_type() {
        // Create a metering event of type `TestEventA`
        let _metering_event_guard =
            metering::create::<metering::TestEventA>(metering::TestEventA::new(100u64));

        // Set the value of `test_annotated_field` to "value"
        metering::current().test_attribute(Some("value".to_string()));

        // Try to pop event B off of the stack
        let expected_none_pop_b = metering::close::<metering::TestEventB>();
        assert!(expected_none_pop_b.is_none());

        // Pop event A off of of the stack
        let expected_metering_event = metering::close::<metering::TestEventA>();
        assert!(expected_metering_event.is_some());
        assert!(expected_metering_event.unwrap().test_annotated_field == Some("value".to_string()));

        // Verify that the stack is empty
        let expected_none_pop_a = metering::close::<metering::TestEventA>();
        assert!(expected_none_pop_a.is_none());
    }

    #[test]
    fn test_many_metering_events_uniform_type_single_context() {
        // Create a metering event of type `TestEventA`
        let _metering_event_1_guard =
            metering::create::<metering::TestEventA>(metering::TestEventA::new(100u64));

        // Create another metering event of type `TestEventA`
        let _metering_event_2_guard =
            metering::create::<metering::TestEventA>(metering::TestEventA::new(50u64));

        // Set the value of `test_annotated_field` on event 2 to be "2"
        metering::current().test_attribute(Some("2".to_string()));

        // Pop event 2 off of the stack
        let expected_metering_event_2 = metering::close::<metering::TestEventA>();
        assert!(expected_metering_event_2.is_some());
        assert!(expected_metering_event_2.unwrap().test_annotated_field == Some("2".to_string()));

        // Set the value of `test_annotated_field` on event 1 to be "1"
        metering::current().test_attribute(Some("1".to_string()));

        // Pop event 1 off of the stack
        let expected_metering_event_1 = metering::close::<metering::TestEventA>();
        assert!(expected_metering_event_1.is_some());
        assert!(expected_metering_event_1.unwrap().test_annotated_field == Some("1".to_string()));

        // Verify that the stack is empty
        let expected_none = metering::close::<metering::TestEventA>();
        assert!(expected_none.is_none());
    }

    #[test]
    fn test_many_metering_events_varying_type_single_context() {
        // Create a metering event of type `TestEventA`
        let _metering_event_a_guard =
            metering::create::<metering::TestEventA>(metering::TestEventA::new(100u64));

        // Create a metering event of type `TestEventB`
        let _metering_event_b_guard =
            metering::create::<metering::TestEventB>(metering::TestEventB::new(50u64));

        // Set the value of `test_annotated_field` on event B to be "B"
        metering::current().test_attribute(Some("B".to_string()));

        // Pop event B off of the stack
        let expected_metering_event_b = metering::close::<metering::TestEventB>();
        assert!(expected_metering_event_b.is_some());
        assert!(expected_metering_event_b.unwrap().test_annotated_field == Some("B".to_string()));

        // Set the value of `test_annotated_field` on event A to be "A"
        metering::current().test_attribute(Some("A".to_string()));

        // Pop event A off of the stack
        let expected_metering_event_b = metering::close::<metering::TestEventA>();
        assert!(expected_metering_event_b.is_some());
        assert!(expected_metering_event_b.unwrap().test_annotated_field == Some("A".to_string()));

        // Verify that the stack is empty
        let expected_none = metering::close::<metering::TestEventA>();
        assert!(expected_none.is_none());
    }

    #[test]
    fn test_nested_mutation() {
        // Define a helper function that sets a value for `test_attribute`
        fn helper_fn() {
            metering::current().test_attribute(Some("helper".to_string()));
        }

        // Create a metering event of type `TestEventA`
        let _metering_event_guard =
            metering::create::<metering::TestEventA>(metering::TestEventA::new(100u64));

        // Call the helper function
        helper_fn();

        // Pop the event off of the stack
        let expected_metering_event = metering::close::<metering::TestEventA>();
        assert!(expected_metering_event.is_some());
        assert!(
            expected_metering_event.unwrap().test_annotated_field == Some("helper".to_string())
        );

        // Verify that the stack is empty
        let expected_none = metering::close::<metering::TestEventA>();
        assert!(expected_none.is_none());
    }

    #[tokio::test]
    async fn test_nested_async_context_single_thread() {
        // Define an asynchronous helper function that sets a value for `test_attribute`
        async fn async_helper_fn() {
            metering::current().test_attribute(Some("async_helper".to_string()));
        }

        // Create a metering event of type `TestEventA`
        let _metering_event_guard =
            metering::create::<metering::TestEventA>(metering::TestEventA::new(100u64));

        // Call the helper function
        async_helper_fn().await;

        // Pop the event off of the stack
        let expected_metering_event = metering::close::<metering::TestEventA>();
        assert!(expected_metering_event.is_some());
        assert!(
            expected_metering_event.unwrap().test_annotated_field
                == Some("async_helper".to_string())
        );

        // Verify that the stack is empty
        let expected_none = metering::close::<metering::TestEventA>();
        assert!(expected_none.is_none());
    }

    #[tokio::test]
    async fn test_nested_mutation_multi_thread() {
        // Define an asynchronous helper function that sets a value for `test_attribute`
        async fn async_helper_fn() {
            metering::current().test_attribute(Some("async_helper".to_string()));
        }

        // Create a metering event of type `TestEventA`
        let metering_event_guard =
            metering::create::<metering::TestEventA>(metering::TestEventA::new(100u64));

        // Call the helper function in another process, passing the context through `metered`
        let handle = tokio::spawn(async move {
            async_helper_fn().metered(metering_event_guard).await;
        });

        // Wait for the handle to resolve
        handle.await.unwrap();

        // Pop the event off of the stack
        let expected_metering_event = metering::close::<metering::TestEventA>();
        assert!(expected_metering_event.is_some());
        assert!(
            expected_metering_event.unwrap().test_annotated_field
                == Some("async_helper".to_string())
        );

        // Verify that the stack is empty
        let expected_none = metering::close::<metering::TestEventA>();
        assert!(expected_none.is_none());
    }
}
