use std::sync::atomic::AtomicU8;

// NOTE(c-gamble): Procedural macros cannot be used in the same crates in which they are defined.
// Instead, it is recommended to create a `tests/` directory in which to write unit and integration
// tests. See https://github.com/rust-lang/rust/issues/110247 for additional information.
#[cfg(test)]
use async_trait::async_trait;
use chroma_metering::initialize_metering;
use chroma_system::{Component, ComponentContext, Handler, ReceiverForMessage, System};

use crate::metering::{Enter, Exit, MeteredFutureExt, SubmitExt};

/// Represents a user defining their own metering module.
mod metering {
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };

    use super::initialize_metering;

    initialize_metering! {
        #[subscription(id = "test_subscription")]
        trait TestSubscription {
            fn test_subscription(&self, increment_num: u64);
        }

        #[context(subscriptions = ["test_subscription"], handlers = [test_handler_a])]
        #[derive(Debug)]
        pub struct TestContextA {
            pub test_annotated_field: Arc<AtomicU64>
        }

        impl std::default::Default for TestContextA {
            fn default() -> Self {
                TestContextA {
                    test_annotated_field: Arc::new(AtomicU64::new(0))
                }
            }
        }

        #[context(subscriptions = ["test_subscription"], handlers = [test_handler_b])]
        #[derive(Default, Debug, Clone)]
        pub struct TestContextB {
            pub test_annotated_field: Arc<AtomicU64>
        }

        impl std::default::Default for TestContextB {
            fn default() -> Self {
                TestContextB {
                    test_annotated_field: Arc::new(AtomicU64::new(0))
                }
            }
        }
    }

    fn test_handler_a(context: &TestContextA, increment_value: u64) {
        context
            .test_annotated_field
            .fetch_add(increment_value, Ordering::Relaxed);
    }

    fn test_handler_b(context: &TestContextB, increment_value: u64) {
        context
            .test_annotated_field
            .fetch_add(increment_value, Ordering::Relaxed);
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
    let metering_context_container =
        metering::create::<metering::TestContextA>(metering::TestContextA::new(100u64));

    // Enter the metering context (required if not using `.metered` on a future)
    metering_context_container.enter();

    // Set the value of `test_annotated_field` to "value"
    metering::with_current(|metering_context| {
        metering_context.test_subscription(Some("value".to_string()))
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

    // Exit the metering context (not required)
    metering_context_container.exit();
}

#[test]
fn test_close_nonexistent_context_type() {
    // Create a metering context of type `TestContextA`
    let metering_context_container =
        metering::create::<metering::TestContextA>(metering::TestContextA::new(100u64));

    // Enter the metering context (required if not using `.metered` on a future)
    metering_context_container.enter();

    // Set the value of `test_annotated_field` to "value"
    metering::with_current(|metering_context| {
        metering_context.test_subscription(Some("value".to_string()))
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

    // Exit the metering context (not required)
    metering_context_container.exit();
}

#[test]
fn test_nested_mutation() {
    // Define a helper function that sets a value for `test_subscription`
    fn helper_fn() {
        metering::with_current(|metering_context| {
            metering_context.test_subscription(Some("helper".to_string()))
        });
    }

    // Create a metering context of type `TestContextA`
    let metering_context_container =
        metering::create::<metering::TestContextA>(metering::TestContextA::new(100u64));

    // Enter the metering context (required if not using `.metered` on a future)
    metering_context_container.enter();

    // Call the helper function
    helper_fn();

    // Close the metering context
    let expected_metering_context = metering::close::<metering::TestContextA>();
    assert!(expected_metering_context.is_ok());
    assert!(expected_metering_context.unwrap().test_annotated_field == Some("helper".to_string()));

    // Verify that the metering context is empty
    let expected_error = metering::close::<metering::TestContextA>();
    assert!(expected_error.is_err());

    // Exit the metering context (not required)
    metering_context_container.exit();
}

#[tokio::test]
async fn test_nested_async_context_single_thread() {
    // Define an asynchronous helper function that sets a value for `test_subscription`
    async fn async_helper_fn() {
        metering::with_current(|metering_context| {
            metering_context.test_subscription(Some("async_helper".to_string()))
        });
    }

    // Create a metering context of type `TestContextA`
    let metering_context_container =
        metering::create::<metering::TestContextA>(metering::TestContextA::new(100u64));

    // Enter the metering context (required if not using `.metered` on a future)
    metering_context_container.enter();

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

    // Exit the metering context (not required)
    metering_context_container.exit();
}

#[tokio::test]
async fn test_nested_mutation_multi_thread() {
    // Define an asynchronous helper function that sets a value for `test_subscription`
    async fn async_helper_fn() {
        let _ = metering::with_current(|metering_context| {
            metering_context.test_subscription(Some("async_helper".to_string()))
        });
    }

    // Create a metering context of type `TestContextA`
    let metering_context_container =
        metering::create::<metering::TestContextA>(metering::TestContextA::new(100u64));

    (async move {
        // Get the current metering context
        let current = metering::get_current();

        // Call the helper function in another process, passing the context through `metered`
        let handle = tokio::spawn(async move {
            async_helper_fn().metered(current).await;
        });

        // Wait for the handle to resolve
        handle.await.unwrap();

        // Close the metering context
        let expected_metering_context = metering::close::<metering::TestContextA>();
        assert!(expected_metering_context.is_ok());
        println!("expected: {:?}", expected_metering_context);
        assert!(
            expected_metering_context.unwrap().test_annotated_field
                == Some("async_helper".to_string())
        );

        // Verify that the metering context is empty
        let expected_error = metering::close::<metering::TestContextA>();
        assert!(expected_error.is_err())
    })
    .metered(metering_context_container.clone())
    .await;

    println!("current: {:?}", metering::get_current());
}

#[tokio::test]
async fn test_nested_mutation_then_close_multi_thread() {
    // Define an asynchronous helper function that sets a value for `test_subscription`
    async fn async_helper_fn() {
        metering::with_current(|metering_context| {
            metering_context.test_subscription(Some("async_helper".to_string()))
        });
        let expected_metering_context = metering::close::<metering::TestContextA>();
        assert!(expected_metering_context.is_ok());
    }

    // Create a metering context of type `TestContextA`
    let metering_context_container =
        metering::create::<metering::TestContextA>(metering::TestContextA::new(100u64));

    // Call the helper function in another process, passing the context through `metered`
    let handle = tokio::spawn(async move {
        async_helper_fn().metered(metering_context_container).await;
    });

    // Wait for the handle to resolve. The metering context should be cleared
    handle.await.unwrap();

    // Verify that the metering context is empty
    let expected_error = metering::close::<metering::TestContextA>();
    assert!(expected_error.is_err());
}
