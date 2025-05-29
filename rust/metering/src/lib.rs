pub mod errors;
pub mod types;

use crate::errors::MeteringError;
use chroma_system::ReceiverForMessage;
use chrono::{DateTime, Utc};
use std::{
    any::Any,
    fmt::Debug,
    sync::{Arc, Mutex, OnceLock},
};
use tracing::Span;
pub use types::*;

/// Trait representing the payload data for a metering event.
///
/// Types implementing this trait can carry arbitrary structured data
/// which will be serialized and sent when the event is submitted.
#[typetag::serde(tag = "type")]
pub trait MeterEventData: Debug + Send + Sync + 'static {
    /// Convert to `Any` for downcasting.
    ///
    /// Each implementor must override this method to return `self`:
    ///
    /// ```ignore
    /// fn as_any(&mut self) -> &mut dyn Any { self }
    /// ```
    ///
    /// Trait-object upcasting coercion is currently unstable
    /// (see https://github.com/rust-lang/rust/issues/65991), so this boilerplate is required.
    fn as_any(&mut self) -> &mut dyn Any;

    // NOTE(c-gamble): We have to define setters for every field in structs that implement
    // `MeterEventData` which we wish to set downstream to avoid having to give lower-level
    // functions information about the nature of their callers.
    fn set_request_received_at_timestamp(
        &mut self,
        _setter_fn: &mut dyn FnMut(&mut DateTime<Utc>),
    ) {
    }
    fn set_request_completed_at_timestamp(
        &mut self,
        _setter_fn: &mut dyn FnMut(&mut Option<DateTime<Utc>>),
    ) {
    }
    fn set_request_execution_time_ns(&mut self, _setter_fn: &mut dyn FnMut(&mut Option<u128>)) {}
}

/// Global, thread-safe receiver for dispatching completed meter events.
///
/// Must be initialized once by calling `MeterEvent::init_receiver` before
/// any events are submitted.
pub static METER_EVENT_RECEIVER: OnceLock<Box<dyn ReceiverForMessage<MeterEvent>>> =
    OnceLock::new();

impl MeterEvent {
    /// Initialize the global receiver for meter events.
    ///
    /// If called more than once, logs an error.
    pub fn init_receiver(receiver: Box<dyn ReceiverForMessage<MeterEvent>>) {
        if let Err(_) = METER_EVENT_RECEIVER.set(receiver) {
            tracing::error!("{}", MeteringError::ReceiverAlreadyInitialized);
        }
    }

    /// Submit this event to the configured receiver.
    ///
    /// Logs an error if no receiver is set or if sending fails.
    pub async fn submit(self) {
        if let Some(event_receiver) = METER_EVENT_RECEIVER.get() {
            if let Err(send_error) = event_receiver.send(self, Some(Span::current())).await {
                tracing::error!("Failed to send MeterEvent: {:?}", send_error);
            }
        } else {
            tracing::error!("{}", MeteringError::ReceiverNotInitialized);
        }
    }
}

thread_local! {
    /// Stack of active meter events, managed per thread.
    static METER_EVENT_STACK: Mutex<Vec<Arc<Mutex<MeterEvent>>>> = Mutex::new(Vec::new());
}

/// RAII guard which ensures a meter event is submitted when it goes out of scope.
///
/// Create one via `open`, and dropping this guard (even on panic) will submit
/// the event at the top of the stack.
pub struct MeterEventGuard;

impl MeterEventGuard {
    /// Open a new meter event and push it onto the thread-local stack.
    ///
    /// Returns an RAII guard that, when dropped, will submit the event.
    pub fn open<T: MeterEventData>(action: Action, data: T) -> Self {
        let meter_event = MeterEvent {
            action: action,
            data: Box::new(data),
        };
        let meter_event_handle = Arc::new(Mutex::new(meter_event));

        METER_EVENT_STACK.with(|meter_event_stack| {
            let mut meter_event_stack_locked = match meter_event_stack.lock() {
                Ok(meter_event_stack_locked) => meter_event_stack_locked,
                Err(err) => {
                    tracing::error!("{}", MeteringError::MutexLockError(err.to_string()));
                    return;
                }
            };
            meter_event_stack_locked.push(meter_event_handle.clone());
        });

        MeterEventGuard
    }
}

impl Drop for MeterEventGuard {
    fn drop(&mut self) {
        // Pop the most recent event from the stack and submit it.
        let maybe_meter_event_handle =
            METER_EVENT_STACK.with(|meter_event_stack| match meter_event_stack.lock() {
                Ok(mut meter_event_stack_locked) => meter_event_stack_locked.pop(),
                Err(err) => {
                    tracing::error!("{}", MeteringError::MutexLockError(err.to_string()));
                    None
                }
            });

        if let Some(meter_event_handle) = maybe_meter_event_handle {
            match Arc::try_unwrap(meter_event_handle)
                .map_err(|_| MeteringError::ArcUnwrapError)
                .and_then(|meter_event_guard| {
                    meter_event_guard
                        .into_inner()
                        .map_err(|err| MeteringError::MutexLockError(err.to_string()))
                }) {
                Ok(event) => {
                    tokio::spawn(async move {
                        event.submit().await;
                    });
                }
                Err(err) => {
                    tracing::error!("Failed to unwrap MeterEvent: {}", err);
                }
            }
        }
    }
}

/// Convenience function to open a meter event with RAII submission.
///
/// Equivalent to `MeterEventGuard::open(...)`.
pub fn open<T: MeterEventData>(action: Action, data: T) -> MeterEventGuard {
    MeterEventGuard::open(action, data)
}

/// Apply a mutation to the payload of the most recently opened event, if any.
pub fn apply_top(mutator: impl FnOnce(&mut dyn MeterEventData)) {
    METER_EVENT_STACK.with(|meter_event_stack| {
        let meter_event_stack_locked = match meter_event_stack.lock() {
            Ok(meter_event_stack_locked) => meter_event_stack_locked,
            Err(err) => {
                tracing::error!("{}", MeteringError::MutexLockError(err.to_string()));
                return;
            }
        };
        if let Some(meter_event_handle) = meter_event_stack_locked.last() {
            match meter_event_handle.lock() {
                Ok(mut meter_event) => mutator(&mut *meter_event.data),
                Err(err) => {
                    tracing::error!("{}", MeteringError::MutexLockError(err.to_string()));
                }
            }
        }
    });
}

/// Apply a mutation to the payload of all currently open events on the stack.
pub fn apply_all(mutator: impl Fn(&mut dyn MeterEventData)) {
    METER_EVENT_STACK.with(|meter_event_stack| {
        let meter_event_stack_locked = match meter_event_stack.lock() {
            Ok(meter_event_stack_locked) => meter_event_stack_locked,
            Err(err) => {
                tracing::error!("{}", MeteringError::MutexLockError(err.to_string()));
                return;
            }
        };
        for meter_event_handle in meter_event_stack_locked.iter() {
            match meter_event_handle.lock() {
                Ok(mut meter_event) => mutator(&mut *meter_event.data),
                Err(err) => {
                    tracing::error!("{}", MeteringError::MutexLockError(err.to_string()));
                }
            }
        }
    });
}

/// Pop and submit the most recently opened meter event, if any.
///
/// Runs the submission immediately in the current async context.
pub async fn close_top() {
    let maybe_top_meter_event_handle =
        METER_EVENT_STACK.with(|meter_event_stack| match meter_event_stack.lock() {
            Ok(mut meter_event_stack_locked) => meter_event_stack_locked.pop(),
            Err(err) => {
                tracing::error!("{}", MeteringError::MutexLockError(err.to_string()));
                None
            }
        });

    if let Some(top_meter_event_handle) = maybe_top_meter_event_handle {
        match Arc::try_unwrap(top_meter_event_handle)
            .map_err(|_| MeteringError::ArcUnwrapError)
            .and_then(|meter_event_guard| {
                meter_event_guard
                    .into_inner()
                    .map_err(|err| MeteringError::MutexLockError(err.to_string()))
            }) {
            Ok(meter_event) => meter_event.submit().await,
            Err(err) => tracing::error!("Failed to close top MeterEvent: {}", err),
        }
    }
}

/// Pop and submit all currently open meter events in LIFO order.
///
/// Each submission runs sequentially in this async context.
pub async fn close_all() {
    // Pop all events into a vector in LIFO order.
    let meter_event_handles =
        METER_EVENT_STACK.with(|meter_event_stack| match meter_event_stack.lock() {
            Ok(mut meter_event_stack_locked) => {
                let mut meter_event_stack_owned = Vec::new();
                while let Some(meter_event_handle) = meter_event_stack_locked.pop() {
                    meter_event_stack_owned.push(meter_event_handle);
                }
                meter_event_stack_owned
            }
            Err(err) => {
                tracing::error!("{}", MeteringError::MutexLockError(err.to_string()));
                Vec::new()
            }
        });

    // Submit each event one by one in that order.
    for meter_event_handle in meter_event_handles {
        match Arc::try_unwrap(meter_event_handle)
            .map_err(|_| MeteringError::ArcUnwrapError)
            .and_then(|meter_event_guard| {
                meter_event_guard
                    .into_inner()
                    .map_err(|err| MeteringError::MutexLockError(err.to_string()))
            }) {
            Ok(meter_event) => meter_event.submit().await,
            Err(err) => tracing::error!("Failed to close MeterEvent: {}", err),
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use chroma_system::{ChannelError, ReceiverForMessage};
    use chrono::{DateTime, Days, Utc};
    use once_cell::sync::Lazy;
    use serde::{Deserialize, Serialize};
    use serial_test::serial;
    use std::{
        any::Any,
        sync::{Arc, Mutex},
    };
    use tokio::time::{sleep, Duration};
    use tracing::Span;

    use super::{
        apply_all, apply_top, close_all, close_top, open,
        types::{Action, ReadAction},
        MeterEvent, MeterEventData,
    };

    /// Payload containing both request-received and request-completed timestamps.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct ReceivedAndCompleted {
        pub request_received_at_timestamp: DateTime<Utc>,
        pub request_completed_at_timestamp: Option<DateTime<Utc>>,
    }

    #[typetag::serde]
    impl MeterEventData for ReceivedAndCompleted {
        fn as_any(&mut self) -> &mut dyn Any {
            self
        }
        fn set_request_received_at_timestamp(
            &mut self,
            setter_fn: &mut dyn FnMut(&mut DateTime<Utc>),
        ) {
            setter_fn(&mut self.request_received_at_timestamp);
        }
        fn set_request_completed_at_timestamp(
            &mut self,
            setter_fn: &mut dyn FnMut(&mut Option<DateTime<Utc>>),
        ) {
            setter_fn(&mut self.request_completed_at_timestamp);
        }
    }

    /// Payload containing only the request-completed timestamp.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct CompletedOnly {
        pub request_completed_at_timestamp: Option<DateTime<Utc>>,
    }

    #[typetag::serde]
    impl MeterEventData for CompletedOnly {
        fn as_any(&mut self) -> &mut dyn Any {
            self
        }
        fn set_request_completed_at_timestamp(
            &mut self,
            setter_fn: &mut dyn FnMut(&mut Option<DateTime<Utc>>),
        ) {
            setter_fn(&mut self.request_completed_at_timestamp);
        }
    }

    /// Shared buffer for collecting all submitted MeterEvent instances.
    static SHARED_EVENT_BUFFER: Lazy<Arc<Mutex<Vec<MeterEvent>>>> =
        Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

    /// Test receiver that pushes incoming events into the shared buffer.
    #[derive(Clone, Debug)]
    struct TestEventReceiver {
        event_buffer: Arc<Mutex<Vec<MeterEvent>>>,
    }

    impl TestEventReceiver {
        /// Clear all events currently stored in the buffer.
        fn clear_buffer(&self) {
            let mut buffer_guard = self.event_buffer.lock().unwrap();
            buffer_guard.clear();
        }
        /// Obtain a handle to the buffer for inspection in tests.
        fn buffer_handle(&self) -> Arc<Mutex<Vec<MeterEvent>>> {
            self.event_buffer.clone()
        }
    }

    #[async_trait]
    impl ReceiverForMessage<MeterEvent> for TestEventReceiver {
        async fn send(&self, message: MeterEvent, _span: Option<Span>) -> Result<(), ChannelError> {
            let mut buffer_guard = self.event_buffer.lock().unwrap();
            buffer_guard.push(message);
            Ok(())
        }
    }

    /// Global, lazy-initialized test receiver.
    static SHARED_EVENT_RECEIVER: Lazy<TestEventReceiver> = Lazy::new(|| {
        let receiver = TestEventReceiver {
            event_buffer: SHARED_EVENT_BUFFER.clone(),
        };
        MeterEvent::init_receiver(Box::new(receiver.clone()));
        receiver
    });

    /// Reset the shared event buffer and return a handle for test assertions.
    fn reset_and_get_event_buffer_handle() -> Arc<Mutex<Vec<MeterEvent>>> {
        SHARED_EVENT_RECEIVER.clear_buffer();
        SHARED_EVENT_RECEIVER.buffer_handle()
    }

    #[tokio::test]
    #[serial]
    /// Dropping the guard without explicit close should still submit the event.
    async fn test_guard_drop_submits_event() {
        let event_buffer_handle = reset_and_get_event_buffer_handle();
        {
            // Open a new event and immediately drop it at end of scope.
            let _event_guard = open(
                Action::Read(ReadAction::Get),
                ReceivedAndCompleted {
                    request_received_at_timestamp: DateTime::<Utc>::from_timestamp_millis(0)
                        .unwrap(),
                    request_completed_at_timestamp: Some(
                        DateTime::<Utc>::from_timestamp_millis(1).unwrap(),
                    ),
                },
            );
        }

        // Allow the background submit task to run.
        sleep(Duration::from_millis(10)).await;

        let mut submitted_events = event_buffer_handle.lock().unwrap();
        assert_eq!(submitted_events.len(), 1);
        let first_submitted = &mut submitted_events[0];
        let data = first_submitted
            .data
            .as_any()
            .downcast_ref::<ReceivedAndCompleted>()
            .unwrap();
        assert_eq!(
            data.request_received_at_timestamp,
            DateTime::<Utc>::from_timestamp_millis(0).unwrap()
        );
        assert_eq!(
            data.request_completed_at_timestamp,
            Some(DateTime::<Utc>::from_timestamp_millis(1).unwrap())
        );
    }

    #[tokio::test]
    #[serial]
    /// `close_top()` should submit only the most recently opened event immediately.
    async fn test_close_top_only_submits_most_recent_event() {
        let event_buffer_handle = reset_and_get_event_buffer_handle();

        let first_event_guard = open(
            Action::Read(ReadAction::Get),
            ReceivedAndCompleted {
                request_received_at_timestamp: DateTime::<Utc>::from_timestamp_millis(1).unwrap(),
                request_completed_at_timestamp: Some(
                    DateTime::<Utc>::from_timestamp_millis(1).unwrap(),
                ),
            },
        );
        let _second_event_guard = open(
            Action::Read(ReadAction::Get),
            CompletedOnly {
                request_completed_at_timestamp: Some(
                    DateTime::<Utc>::from_timestamp_millis(2).unwrap(),
                ),
            },
        );

        // Close only the top event on the stack.
        close_top().await;
        // Drop the first guard to trigger its submission as well.
        drop(first_event_guard);

        sleep(Duration::from_millis(10)).await;

        let mut submitted_events = event_buffer_handle.lock().unwrap();
        assert_eq!(submitted_events.len(), 2);
        // The first submitted should be the CompletedOnly payload.
        let first_submitted = &mut submitted_events[0];
        assert!(first_submitted
            .data
            .as_any()
            .downcast_ref::<CompletedOnly>()
            .is_some());
        // The second submitted should match the ReceivedAndCompleted payload.
        let second_submitted = &mut submitted_events[1];
        let data = second_submitted
            .data
            .as_any()
            .downcast_ref::<ReceivedAndCompleted>()
            .unwrap();
        assert_eq!(
            data.request_received_at_timestamp,
            DateTime::<Utc>::from_timestamp_millis(1).unwrap()
        );
    }

    #[tokio::test]
    #[serial]
    /// `close_all()` should submit all open events in LIFO order.
    async fn test_close_all_submits_events_in_lifo_order() {
        let event_buffer_handle = reset_and_get_event_buffer_handle();

        let _first_guard = open(
            Action::Read(ReadAction::Get),
            ReceivedAndCompleted {
                request_received_at_timestamp: DateTime::<Utc>::from_timestamp_millis(10).unwrap(),
                request_completed_at_timestamp: Some(
                    DateTime::<Utc>::from_timestamp_millis(30).unwrap(),
                ),
            },
        );
        let _second_guard = open(
            Action::Read(ReadAction::Get),
            CompletedOnly {
                request_completed_at_timestamp: Some(
                    DateTime::<Utc>::from_timestamp_millis(20).unwrap(),
                ),
            },
        );

        close_all().await;
        sleep(Duration::from_millis(10)).await;

        let mut submitted_events = event_buffer_handle.lock().unwrap();
        assert_eq!(submitted_events.len(), 2);
        // LIFO means the second guard's event is submitted first.
        assert!(submitted_events[0]
            .data
            .as_any()
            .downcast_ref::<CompletedOnly>()
            .is_some());
        // Then the first guard's event.
        assert!(submitted_events[1]
            .data
            .as_any()
            .downcast_ref::<ReceivedAndCompleted>()
            .is_some());
    }

    #[tokio::test]
    #[serial]
    /// `apply_all()` should mutate the "request-completed" timestamp on all open events.
    async fn test_apply_all_increments_all_completion_timestamps() {
        let event_buffer_handle = reset_and_get_event_buffer_handle();

        let first_event_guard = open(
            Action::Read(ReadAction::Get),
            ReceivedAndCompleted {
                request_received_at_timestamp: DateTime::<Utc>::from_timestamp_millis(0).unwrap(),
                request_completed_at_timestamp: Some(
                    DateTime::<Utc>::from_timestamp_millis(5).unwrap(),
                ),
            },
        );
        let _second_event_guard = open(
            Action::Read(ReadAction::Get),
            CompletedOnly {
                request_completed_at_timestamp: Some(
                    DateTime::<Utc>::from_timestamp_millis(7).unwrap(),
                ),
            },
        );

        // Increment completion timestamps on every open event by one day.
        apply_all(|data| {
            data.set_request_completed_at_timestamp(&mut |timestamp| {
                *timestamp = if let Some(timestamp) = timestamp {
                    Some(timestamp.checked_add_days(Days::new(1)).unwrap())
                } else {
                    None
                }
            });
        });

        drop(_second_event_guard);
        drop(first_event_guard);
        sleep(Duration::from_millis(10)).await;

        let mut submitted_events = event_buffer_handle.lock().unwrap();
        assert_eq!(submitted_events.len(), 2);
        // First submitted should be the CompletedOnly payload with +1 day.
        let completed_only_data = submitted_events[0]
            .data
            .as_any()
            .downcast_ref::<CompletedOnly>()
            .unwrap();
        assert_eq!(
            completed_only_data.request_completed_at_timestamp,
            Some(
                DateTime::<Utc>::from_timestamp_millis(7)
                    .unwrap()
                    .checked_add_days(Days::new(1))
                    .unwrap()
            )
        );
        // Then the ReceivedAndCompleted payload with +1 day on completion.
        let received_and_completed_data = submitted_events[1]
            .data
            .as_any()
            .downcast_ref::<ReceivedAndCompleted>()
            .unwrap();
        assert_eq!(
            received_and_completed_data.request_completed_at_timestamp,
            Some(
                DateTime::<Utc>::from_timestamp_millis(5)
                    .unwrap()
                    .checked_add_days(Days::new(1))
                    .unwrap()
            )
        );
    }

    #[tokio::test]
    #[serial]
    /// `apply_top()` should mutate only the most recently opened event’s received timestamp,
    /// and then `apply_all()` applies to the new top as well.
    async fn test_apply_top_and_apply_all_affect_received_timestamp_correctly() {
        let event_buffer_handle = reset_and_get_event_buffer_handle();

        let first_event_guard = open(
            Action::Read(ReadAction::Get),
            ReceivedAndCompleted {
                request_received_at_timestamp: DateTime::<Utc>::from_timestamp_millis(0).unwrap(),
                request_completed_at_timestamp: Some(
                    DateTime::<Utc>::from_timestamp_millis(0).unwrap(),
                ),
            },
        );
        let _second_event_guard = open(
            Action::Read(ReadAction::Get),
            CompletedOnly {
                request_completed_at_timestamp: Some(
                    DateTime::<Utc>::from_timestamp_millis(0).unwrap(),
                ),
            },
        );

        // Set received timestamp on top only by +42 days.
        apply_top(|data| {
            data.set_request_received_at_timestamp(&mut |timestamp| {
                *timestamp = timestamp.checked_add_days(Days::new(42)).unwrap()
            });
        });
        // Then apply +99 days to all open events (now the ReceivedAndCompleted is on top).
        apply_all(|data| {
            data.set_request_received_at_timestamp(&mut |timestamp| {
                *timestamp = timestamp.checked_add_days(Days::new(99)).unwrap()
            });
        });

        drop(_second_event_guard);
        drop(first_event_guard);
        sleep(Duration::from_millis(10)).await;

        let mut submitted_events = event_buffer_handle.lock().unwrap();
        assert_eq!(submitted_events.len(), 2);
        // First submitted is the CompletedOnly payload (unchanged in received timestamp).
        assert!(submitted_events[0]
            .data
            .as_any()
            .downcast_ref::<CompletedOnly>()
            .is_some());
        // Second submitted is ReceivedAndCompleted with +99 days on received timestamp.
        let received_and_completed_data = submitted_events[1]
            .data
            .as_any()
            .downcast_ref::<ReceivedAndCompleted>()
            .unwrap();
        assert_eq!(
            received_and_completed_data.request_received_at_timestamp,
            DateTime::<Utc>::from_timestamp_millis(0)
                .unwrap()
                .checked_add_days(Days::new(99))
                .unwrap()
        );
    }
}
