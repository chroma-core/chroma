pub mod errors;

use crate::errors::MeteringError;
use chroma_system::ReceiverForMessage;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    fmt::Debug,
    sync::{Arc, Mutex, OnceLock},
};
use tracing::Span;

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

/// Core structure representing a single metering event.
///
/// Contains tenant and database identifiers, the related collection ID,
/// and the payload data implementing `MeterEventData`.
#[derive(Debug, Serialize, Deserialize)]
pub struct MeterEvent {
    /// Identifier for the tenant.
    pub tenant: String,
    /// Identifier for the database.
    pub database: String,
    /// UUID of the collection to which this event pertains.
    pub collection_id: String,
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
    pub fn open<T: MeterEventData>(
        tenant: String,
        database: String,
        collection_id: String,
        data: T,
    ) -> Self {
        let meter_event = MeterEvent {
            tenant,
            database,
            collection_id,
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
pub fn open<T: MeterEventData>(
    tenant: String,
    database: String,
    collection_id: String,
    data: T,
) -> MeterEventGuard {
    MeterEventGuard::open(tenant, database, collection_id, data)
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
#[path = "tests.rs"]
mod tests;
