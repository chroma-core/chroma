use chroma_system::ReceiverForMessage;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    fmt::Debug,
    sync::{Arc, Mutex, OnceLock},
};
use tracing::Span;
use uuid::Uuid;

/// Trait representing the payload data for a metering event.
///
/// Types implementing this trait can carry arbitrary structured data
/// which will be serialized and sent when the event is submitted.
#[typetag::serde(tag = "type")]
pub trait MeterEventData: Debug + Send + Sync + 'static {
    /// Convert this data to a dynamic reference for downcasting.
    fn as_any(&mut self) -> &mut dyn Any;

    // NOTE(c-gamble): We have to define setters for every field in structs that implement
    // `MeterEventData` which we wish to set downstream to avoid having to give lower-level
    // functions information about the nature of their callers.
    fn set_request_received_at_timestamp(
        &mut self,
        _applicator: &mut dyn FnMut(&mut DateTime<Utc>),
    ) {
    }
    fn set_request_completed_at_timestamp(
        &mut self,
        _applicator: &mut dyn FnMut(&mut DateTime<Utc>),
    ) {
    }
}

/// Core structure representing a single metering event.
///
/// Contains tenant and database identifiers, the related collection ID,
/// and the payload data implementing `MeterEventData`.
#[derive(Debug, Serialize, Deserialize)]
pub struct MeterEvent {
    /// Identifier for the tenant or namespace.
    pub tenant: String,
    /// Identifier for the database associated with this event.
    pub database: String,
    /// UUID of the collection this event pertains to.
    pub collection_id: Uuid,
    /// User-defined payload data for this event.
    #[serde(flatten)]
    pub data: Box<dyn MeterEventData>,
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
    /// Calling this more than once has no effect.
    pub fn init_receiver(receiver: Box<dyn ReceiverForMessage<MeterEvent>>) {
        let _ = METER_EVENT_RECEIVER.set(receiver);
    }

    /// Submit this event to the configured receiver.
    ///
    /// If no receiver has been initialized, this is a no-op.
    pub async fn submit(self) {
        if let Some(meter_event_receiver) = METER_EVENT_RECEIVER.get() {
            let _ = meter_event_receiver.send(self, Some(Span::current())).await;
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
    pub fn open<T: MeterEventData>(
        tenant: String,
        database: String,
        collection_id: Uuid,
        payload_data: T,
    ) -> Self {
        let new_event = MeterEvent {
            tenant,
            database,
            collection_id,
            data: Box::new(payload_data),
        };
        let meter_event_handle = Arc::new(Mutex::new(new_event));
        METER_EVENT_STACK.with(|meter_event_stack| {
            let mut meter_event_stack_locked = meter_event_stack.lock().unwrap();
            meter_event_stack_locked.push(meter_event_handle.clone());
        });
        MeterEventGuard
    }
}

impl Drop for MeterEventGuard {
    fn drop(&mut self) {
        // Pop the most recent event from the stack and submit it.
        if let Some(meter_event_handle) =
            METER_EVENT_STACK.with(|meter_event_stack| meter_event_stack.lock().unwrap().pop())
        {
            if let Ok(meter_event) = Arc::try_unwrap(meter_event_handle)
                .map_err(|_| ())
                .and_then(|meter_event_guard| meter_event_guard.into_inner().map_err(|_| ()))
            {
                // Spawn an async task to submit without blocking.
                tokio::spawn(async move {
                    meter_event.submit().await;
                });
            }
        }
    }
}

/// Convenience function to open a meter event with RAII submission.
///
/// Equivalent to `MeterEventGuard::open(...)`.
pub fn open<T: MeterEventData>(
    tenant: String,
    database: String,
    collection_id: Uuid,
    payload_data: T,
) -> MeterEventGuard {
    MeterEventGuard::open(tenant, database, collection_id, payload_data)
}

/// Apply a mutation to the payload of the most recently opened event, if any.
pub fn attach_top(mutator: impl FnOnce(&mut dyn MeterEventData)) {
    METER_EVENT_STACK.with(|meter_event_stack| {
        let meter_event_stack_locked = meter_event_stack.lock().unwrap();
        if let Some(meter_event_handle) = meter_event_stack_locked.last() {
            if let Ok(mut meter_event_locked) = meter_event_handle.lock() {
                mutator(&mut *meter_event_locked.data);
            }
        }
    });
}

/// Apply a mutation to the payload of all currently open events on the stack.
pub fn attach_all(mutator: impl Fn(&mut dyn MeterEventData)) {
    METER_EVENT_STACK.with(|meter_event_stack| {
        let meter_event_stack_locked = meter_event_stack.lock().unwrap();
        for meter_event_handle in meter_event_stack_locked.iter() {
            if let Ok(mut meter_event_locked) = meter_event_handle.lock() {
                mutator(&mut *meter_event_locked.data);
            }
        }
    });
}

/// Pop and submit the most recently opened meter event, if any.
///
/// Runs the submission immediately in the current async context.
pub async fn close_top() {
    if let Some(meter_event_handle) =
        METER_EVENT_STACK.with(|meter_event_stack| meter_event_stack.lock().unwrap().pop())
    {
        if let Ok(meter_event) = Arc::try_unwrap(meter_event_handle)
            .map_err(|_| ())
            .and_then(|meter_event_guard| meter_event_guard.into_inner().map_err(|_| ()))
        {
            meter_event.submit().await;
        }
    }
}

/// Pop and submit all currently open meter events in LIFO order.
///
/// Each submission runs sequentially in this async context.
pub async fn close_all() {
    // Drain the thread-local stack into a vector.
    let mut meter_event_handles_to_submit = Vec::new();
    METER_EVENT_STACK.with(|meter_event_stack| {
        let mut meter_event_stack_locked = meter_event_stack.lock().unwrap();
        while let Some(meter_event_handle) = meter_event_stack_locked.pop() {
            meter_event_handles_to_submit.push(meter_event_handle);
        }
    });

    // Submit each event one by one.
    for meter_event_handle in meter_event_handles_to_submit {
        if let Ok(meter_event) = Arc::try_unwrap(meter_event_handle)
            .map_err(|_| ())
            .and_then(|meter_event_guard| meter_event_guard.into_inner().map_err(|_| ()))
        {
            meter_event.submit().await;
        }
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
