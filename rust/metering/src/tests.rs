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
use uuid::Uuid;

use crate::{attach_all, attach_top, close_all, close_top, open, MeterEvent, MeterEventData};

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
    fn set_request_received_at_timestamp(&mut self, setter_fn: &mut dyn FnMut(&mut DateTime<Utc>)) {
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
            "tenant1".into(),
            "database1".into(),
            Uuid::new_v4(),
            ReceivedAndCompleted {
                request_received_at_timestamp: DateTime::<Utc>::from_timestamp_millis(0).unwrap(),
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
        "tenant2".into(),
        "database2".into(),
        Uuid::new_v4(),
        ReceivedAndCompleted {
            request_received_at_timestamp: DateTime::<Utc>::from_timestamp_millis(1).unwrap(),
            request_completed_at_timestamp: Some(
                DateTime::<Utc>::from_timestamp_millis(1).unwrap(),
            ),
        },
    );
    let _second_event_guard = open(
        "tenant2".into(),
        "database2".into(),
        Uuid::new_v4(),
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
        "tenant3".into(),
        "database3".into(),
        Uuid::new_v4(),
        ReceivedAndCompleted {
            request_received_at_timestamp: DateTime::<Utc>::from_timestamp_millis(10).unwrap(),
            request_completed_at_timestamp: Some(
                DateTime::<Utc>::from_timestamp_millis(30).unwrap(),
            ),
        },
    );
    let _second_guard = open(
        "tenant3".into(),
        "database3".into(),
        Uuid::new_v4(),
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
/// `attach_all()` should mutate the "request-completed" timestamp on all open events.
async fn test_attach_all_increments_all_completion_timestamps() {
    let event_buffer_handle = reset_and_get_event_buffer_handle();

    let first_event_guard = open(
        "tenant4".into(),
        "database4".into(),
        Uuid::new_v4(),
        ReceivedAndCompleted {
            request_received_at_timestamp: DateTime::<Utc>::from_timestamp_millis(0).unwrap(),
            request_completed_at_timestamp: Some(
                DateTime::<Utc>::from_timestamp_millis(5).unwrap(),
            ),
        },
    );
    let _second_event_guard = open(
        "tenant4".into(),
        "database4".into(),
        Uuid::new_v4(),
        CompletedOnly {
            request_completed_at_timestamp: Some(
                DateTime::<Utc>::from_timestamp_millis(7).unwrap(),
            ),
        },
    );

    // Increment completion timestamps on every open event by one day.
    attach_all(|data| {
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
/// `attach_top()` should mutate only the most recently opened event’s received timestamp,
/// and then `attach_all()` applies to the new top as well.
async fn test_attach_top_and_attach_all_affect_received_timestamp_correctly() {
    let event_buffer_handle = reset_and_get_event_buffer_handle();

    let first_event_guard = open(
        "tenant5".into(),
        "database5".into(),
        Uuid::new_v4(),
        ReceivedAndCompleted {
            request_received_at_timestamp: DateTime::<Utc>::from_timestamp_millis(0).unwrap(),
            request_completed_at_timestamp: Some(
                DateTime::<Utc>::from_timestamp_millis(0).unwrap(),
            ),
        },
    );
    let _second_event_guard = open(
        "tenant5".into(),
        "database5".into(),
        Uuid::new_v4(),
        CompletedOnly {
            request_completed_at_timestamp: Some(
                DateTime::<Utc>::from_timestamp_millis(0).unwrap(),
            ),
        },
    );

    // Set received timestamp on top only by +42 days.
    attach_top(|data| {
        data.set_request_received_at_timestamp(&mut |timestamp| {
            *timestamp = timestamp.checked_add_days(Days::new(42)).unwrap()
        });
    });
    // Then apply +99 days to all open events (now the ReceivedAndCompleted is on top).
    attach_all(|data| {
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
