// NOTE(c-gamble): Procedural macros cannot be used in the same crates in which they are defined.
// Instead, it is recommended to create a `tests/` directory in which to write unit and integration
// tests. See https://github.com/rust-lang/rust/issues/110247 for additional information.
#[cfg(test)]
use async_trait::async_trait;
use chroma_metering::initialize_metering;
use chroma_system::{Component, ComponentContext, Handler, ReceiverForMessage, System};

use crate::metering::{MeteredFutureExt, SubmitExt};
/// Represents a user defining their own metering module.
mod metering {
    use super::initialize_metering;

    initialize_metering! {
        #[attribute(name = "test_attribute")]
        type TestAttribute = Option<String>;

        #[context]
        #[derive(Default, Debug, Clone)]
        pub struct TestContextA {
            // This field is not read in the tests
            #[allow(dead_code)]
            test_unannotated_field: u64,
            #[field(attribute = "test_attribute", mutator = "test_mutator_a")]
            pub test_annotated_field: Option<String>
        }

        #[context]
        #[derive(Default, Debug, Clone)]
        pub struct TestContextB {
            // This field is not read in the tests
            #[allow(dead_code)]
            test_unannotated_field: u64,
            #[field(attribute = "test_attribute", mutator = "test_mutator_b")]
            pub test_annotated_field: Option<String>
        }
    }

    fn test_mutator_a(context: &mut TestContextA, value: Option<String>) {
        context.test_annotated_field = value;
    }

    fn test_mutator_b(context: &mut TestContextB, value: Option<String>) {
        context.test_annotated_field = value;
    }
}

// NOTE(c-gamble): This needs to be async because `chroma_system::System::start_component` expects
// to be inside of a Tokio runtime.
#[tokio::test]
async fn test_init_custom_receiver() {
    /// A test component so we can test registering a custom receiver
    #[derive(Clone, Debug)]
    struct TestComponent {
        messages: Vec<String>,
    }

    /// Implement the `Component` trait for our test component
    #[async_trait]
    impl Component for TestComponent {
        fn get_name() -> &'static str {
            "TestComponent"
        }

        fn queue_size(&self) -> usize {
            100
        }

        async fn on_start(&mut self, _: &ComponentContext<Self>) {}

        fn on_stop_timeout(&self) -> std::time::Duration {
            std::time::Duration::from_secs(1)
        }
    }

    /// Implement `Handler` for our test component
    #[async_trait]
    impl Handler<Box<dyn metering::MeteringContext>> for TestComponent {
        type Result = Option<()>;

        async fn handle(
            &mut self,
            message: Box<dyn metering::MeteringContext>,
            _context: &ComponentContext<Self>,
        ) -> Self::Result {
            self.messages.push(format!("{:?}", message));
            None
        }
    }

    // Initialize a new Chroma system
    let system = System::new();

    // Create a test component
    let test_component = TestComponent {
        messages: Vec::new(),
    };

    // Start the component
    let component_handle = system.start_component(test_component);

    // Extract the receiver and force its type resolution
    let custom_receiver: Box<dyn ReceiverForMessage<Box<dyn metering::MeteringContext>>> =
        component_handle.receiver();

    // Initialize the custom receiver
    let _ = metering::init_receiver(custom_receiver);
}

#[tokio::test]
async fn test_single_metering_context() {
    // Create a metering context of type `TestContextA`
    let _metering_context_guard =
        metering::create::<metering::TestContextA>(metering::TestContextA::new(100u64));

    // Set the value of `test_annotated_field` to "value"
    metering::with_current(|metering_context| {
        metering_context.test_attribute(Some("value".to_string()))
    });

    // Close the metering context
    let expected_metering_context = metering::close::<metering::TestContextA>();
    assert!(expected_metering_context.is_ok());
    let metering_context = expected_metering_context.unwrap(); // only unwrap once
    assert_eq!(
        metering_context.test_annotated_field,
        Some("value".to_string())
    );

    // Submit the context to the receiver
    metering_context.submit().await;

    // Verify that the metering context is empty
    let expected_error = metering::close::<metering::TestContextA>();
    assert!(expected_error.is_err());
}

#[test]
fn test_close_nonexistent_context_type() {
    // Create a metering context of type `TestContextA`
    let _metering_context_guard =
        metering::create::<metering::TestContextA>(metering::TestContextA::new(100u64));

    // Set the value of `test_annotated_field` to "value"
    metering::with_current(|metering_context| {
        metering_context.test_attribute(Some("value".to_string()))
    });

    // Try to pop context B off of the stack
    let expected_none_pop_b = metering::close::<metering::TestContextB>();
    assert!(expected_none_pop_b.is_err());

    // Pop context A off of of the stack
    let expected_metering_context = metering::close::<metering::TestContextA>();
    assert!(expected_metering_context.is_ok());
    assert!(expected_metering_context.unwrap().test_annotated_field == Some("value".to_string()));

    // Verify that the metering context is empty
    let expected_none_pop_a = metering::close::<metering::TestContextA>();
    assert!(expected_none_pop_a.is_err());
}

#[test]
fn test_nested_mutation() {
    // Define a helper function that sets a value for `test_attribute`
    fn helper_fn() {
        metering::with_current(|metering_context| {
            metering_context.test_attribute(Some("helper".to_string()))
        });
    }

    // Create a metering context of type `TestContextA`
    let _metering_context_guard =
        metering::create::<metering::TestContextA>(metering::TestContextA::new(100u64));

    // Call the helper function
    helper_fn();

    // Close the metering context
    let expected_metering_context = metering::close::<metering::TestContextA>();
    assert!(expected_metering_context.is_ok());
    assert!(expected_metering_context.unwrap().test_annotated_field == Some("helper".to_string()));

    // Verify that the metering context is empty
    let expected_error = metering::close::<metering::TestContextA>();
    assert!(expected_error.is_err());
}

#[tokio::test]
async fn test_nested_async_context_single_thread() {
    // Define an asynchronous helper function that sets a value for `test_attribute`
    async fn async_helper_fn() {
        metering::with_current(|metering_context| {
            metering_context.test_attribute(Some("async_helper".to_string()))
        });
    }

    // Create a metering context of type `TestContextA`
    let _metering_context_guard =
        metering::create::<metering::TestContextA>(metering::TestContextA::new(100u64));

    // Call the helper function
    async_helper_fn().await;

    // Close the metering context
    let expected_metering_context = metering::close::<metering::TestContextA>();
    assert!(expected_metering_context.is_ok());
    assert!(
        expected_metering_context.unwrap().test_annotated_field == Some("async_helper".to_string())
    );

    // Verify that the metering context is empty
    let expected_error = metering::close::<metering::TestContextA>();
    assert!(expected_error.is_err());
}

#[tokio::test]
async fn test_nested_mutation_multi_thread() {
    // Define an asynchronous helper function that sets a value for `test_attribute`
    async fn async_helper_fn() {
        metering::with_current(|metering_context| {
            metering_context.test_attribute(Some("async_helper".to_string()))
        });
    }

    // Create a metering context of type `TestContextA`
    let metering_context_guard =
        metering::create::<metering::TestContextA>(metering::TestContextA::new(100u64));

    // Call the helper function in another process, passing the context through `metered`
    let handle = tokio::spawn(async move {
        async_helper_fn().metered(metering_context_guard).await;
    });

    // Wait for the handle to resolve
    handle.await.unwrap();

    // Close the metering context
    let expected_metering_context = metering::close::<metering::TestContextA>();
    assert!(expected_metering_context.is_ok());
    assert!(
        expected_metering_context.unwrap().test_annotated_field == Some("async_helper".to_string())
    );

    // Verify that the metering context is empty
    let expected_error = metering::close::<metering::TestContextA>();
    assert!(expected_error.is_err());
}

#[tokio::test]
async fn test_nested_mutation_then_close_multi_thread() {
    // Define an asynchronous helper function that sets a value for `test_attribute`
    async fn async_helper_fn() {
        metering::with_current(|metering_context| {
            metering_context.test_attribute(Some("async_helper".to_string()))
        });
        let expected_metering_context = metering::close::<metering::TestContextA>();
        assert!(expected_metering_context.is_ok());
    }

    // Create a metering context of type `TestContextA`
    let metering_context_guard =
        metering::create::<metering::TestContextA>(metering::TestContextA::new(100u64));

    // Call the helper function in another process, passing the context through `metered`
    let handle = tokio::spawn(async move {
        async_helper_fn().metered(metering_context_guard).await;
    });

    // Wait for the handle to resolve. The metering context should be cleared
    handle.await.unwrap();

    // Verify that the metering context is empty
    let expected_error = metering::close::<metering::TestContextA>();
    assert!(expected_error.is_err());
}
