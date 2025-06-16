// NOTE(c-gamble): Procedural macros cannot be used in the same crates in which they are defined.
// Instead, it is recommended to create a `tests/` directory in which to write unit and integration
// tests. See https://github.com/rust-lang/rust/issues/110247 for additional information.
#[cfg(test)]
use chroma_metering_macros::initialize_metering;
use std::sync::atomic::Ordering;

use crate::metering::{Enterable, MeteredFutureExt, TestCapability};

/// Represents a user defining their own metering module.
mod metering {
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };

    use super::initialize_metering;

    initialize_metering! {
        #[capability]
        pub trait TestCapability {
            fn test_capability(&self, increment_num: u64);
        }

        #[context(capabilities = [TestCapability], handlers = [test_handler_a])]
        #[derive(Debug, Clone)]
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

        #[context(capabilities = [TestCapability], handlers = [test_handler_b])]
        #[derive(Debug, Clone)]
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

#[tokio::test]
async fn test_single_metering_context() {
    // Create a metering context of type `TestContextA`
    let metering_context_container =
        metering::create::<metering::TestContextA>(metering::TestContextA::default());

    // Enter the metering context (required if not using `.meter` on a future)
    metering_context_container.enter();

    // Set the value of `test_annotated_field` to "value"
    metering::with_current(|metering_context| metering_context.test_capability(100));

    // Close the metering context
    let expected_metering_context = metering::close::<metering::TestContextA>();
    assert!(expected_metering_context.is_ok());
    let metering_context = expected_metering_context.unwrap();
    assert_eq!(
        metering_context.test_annotated_field.load(Ordering::SeqCst),
        100u64
    );

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
        metering::create::<metering::TestContextA>(metering::TestContextA::default());

    // Enter the metering context (required if not using `.meter` on a future)
    metering_context_container.enter();

    // Set the value of `test_annotated_field` to "value"
    metering::with_current(|metering_context| metering_context.test_capability(100));

    // Try to pop context B off of the stack
    let expected_none_pop_b = metering::close::<metering::TestContextB>();
    assert!(expected_none_pop_b.is_err());

    // Pop context A off of of the stack
    let expected_metering_context = metering::close::<metering::TestContextA>();
    assert!(expected_metering_context.is_ok());
    assert_eq!(
        expected_metering_context
            .unwrap()
            .test_annotated_field
            .load(Ordering::SeqCst),
        100u64
    );

    // Verify that the metering context is empty
    let expected_none_pop_a = metering::close::<metering::TestContextA>();
    assert!(expected_none_pop_a.is_err());

    metering_context_container.exit();
}

#[test]
fn test_nested_mutation() {
    // Define a helper function that sets a value for `test_capability`
    fn helper_fn() {
        metering::with_current(|metering_context| metering_context.test_capability(50));
    }

    // Create a metering context of type `TestContextA`
    let metering_context_container =
        metering::create::<metering::TestContextA>(metering::TestContextA::default());

    // Enter the metering context (required if not using `.meter` on a future)
    metering_context_container.enter();

    helper_fn();

    // Close the metering context
    let expected_metering_context = metering::close::<metering::TestContextA>();
    assert!(expected_metering_context.is_ok());
    assert_eq!(
        expected_metering_context
            .unwrap()
            .test_annotated_field
            .load(Ordering::SeqCst),
        50u64
    );

    // Verify that the metering context is empty
    let expected_error = metering::close::<metering::TestContextA>();
    assert!(expected_error.is_err());

    metering_context_container.exit();
}

#[tokio::test]
async fn test_nested_async_context_single_thread() {
    // Define an asynchronous helper function that sets a value for `test_capability`
    async fn async_helper_fn() {
        metering::with_current(|metering_context| metering_context.test_capability(25));
    }

    // Create a metering context of type `TestContextA`
    let metering_context_container =
        metering::create::<metering::TestContextA>(metering::TestContextA::default());

    // Enter the metering context (required if not using `.meter` on a future)
    metering_context_container.enter();

    async_helper_fn().await;

    // Close the metering context
    let expected_metering_context = metering::close::<metering::TestContextA>();
    assert!(expected_metering_context.is_ok());
    assert_eq!(
        expected_metering_context
            .unwrap()
            .test_annotated_field
            .load(Ordering::SeqCst),
        25u64
    );

    // Verify that the metering context is empty
    let expected_error = metering::close::<metering::TestContextA>();
    assert!(expected_error.is_err());

    metering_context_container.exit();
}

#[tokio::test]
async fn test_nested_mutation_multi_thread() {
    // Define an asynchronous helper function that sets a value for `test_capability`
    async fn async_helper_fn() {
        metering::with_current(|metering_context| metering_context.test_capability(25));
    }

    // Create a metering context of type `TestContextA`
    let metering_context_container =
        metering::create::<metering::TestContextA>(metering::TestContextA::default());

    (async move {
        // Get the current metering context
        let current = metering::get_current();

        // Call the helper function in another process, passing the context through `metered`
        let handle = tokio::spawn(async move {
            async_helper_fn().meter(current).await;
        });

        handle.await.unwrap();

        // Close the metering context
        let expected_metering_context = metering::close::<metering::TestContextA>();
        assert!(expected_metering_context.is_ok());
        println!("expected: {:?}", expected_metering_context);
        assert_eq!(
            expected_metering_context
                .unwrap()
                .test_annotated_field
                .load(Ordering::SeqCst),
            25u64
        );

        // Verify that the metering context is empty
        let expected_error = metering::close::<metering::TestContextA>();
        assert!(expected_error.is_err())
    })
    .meter(metering_context_container.clone())
    .await;

    println!("current: {:?}", metering::get_current());
}

#[tokio::test]
async fn test_nested_mutation_then_close_multi_thread() {
    // Define an asynchronous helper function that sets a value for `test_capability`
    async fn async_helper_fn() {
        metering::with_current(|metering_context| metering_context.test_capability(25));
        let expected_metering_context = metering::close::<metering::TestContextA>();
        assert!(expected_metering_context.is_ok());
    }

    // Create a metering context of type `TestContextA`
    let metering_context_container =
        metering::create::<metering::TestContextA>(metering::TestContextA::default());

    // Call the helper function in another process, passing the context through `metered`
    let handle = tokio::spawn(async move {
        async_helper_fn().meter(metering_context_container).await;
    });

    // The metering context should be cleared
    handle.await.unwrap();

    // Verify that the metering context is empty
    let expected_error = metering::close::<metering::TestContextA>();
    assert!(expected_error.is_err());
}
