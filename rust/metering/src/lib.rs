pub mod errors;

use crate::errors::MeteringError;
use chroma_system::ReceiverForMessage;
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    cell::RefCell,
    fmt::Debug,
    sync::{Arc, Mutex, OnceLock},
};
use tracing::Span;
use uuid::Uuid;

#[typetag::serde(tag = "type")]
pub trait MeterEventData: Debug + Send + Sync + 'static {
    fn as_any(&mut self) -> &mut dyn Any;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MeterEvent {
    pub tenant: String,
    pub database: String,
    pub collection_id: Uuid,
    #[serde(flatten)]
    pub data: Box<dyn MeterEventData>,
}

pub static METER_EVENT_RECEIVER: OnceLock<Box<dyn ReceiverForMessage<MeterEvent>>> =
    OnceLock::new();

impl MeterEvent {
    pub fn init_receiver(receiver: Box<dyn ReceiverForMessage<MeterEvent>>) {
        if METER_EVENT_RECEIVER.set(receiver).is_err() {
            tracing::error!("{}", MeteringError::ReceiverAlreadyInitialized);
        }
    }

    pub async fn submit(self) {
        if let Some(handler) = METER_EVENT_RECEIVER.get() {
            if let Err(err) = handler.send(self, Some(Span::current())).await {
                tracing::error!("{}", MeteringError::SubmitError(Box::new(err)));
            }
        }
    }
}

thread_local! {
    static METER_EVENT_STACK: RefCell<Vec<Arc<Mutex<MeterEvent>>>> = RefCell::new(Vec::new());
}

pub struct MeterEventGuard {
    inner: Option<Arc<Mutex<MeterEvent>>>,
}

impl MeterEventGuard {
    pub fn begin<T: MeterEventData>(
        tenant: String,
        database: String,
        collection_id: Uuid,
        meter_event_data: T,
    ) -> Self {
        let event = MeterEvent {
            tenant,
            database,
            collection_id,
            data: Box::new(meter_event_data),
        };

        let meter_event_arc = Arc::new(Mutex::new(event));

        METER_EVENT_STACK.with(|slot| slot.borrow_mut().push(meter_event_arc.clone()));
        MeterEventGuard {
            inner: Some(meter_event_arc),
        }
    }

    pub async fn finish(mut self) {
        if let Some(meter_event_arc) = self.inner.take() {
            METER_EVENT_STACK.with(|slot| {
                slot.borrow_mut().pop();
            });
            let meter_event = Arc::try_unwrap(meter_event_arc)
                .map_err(|_| MeteringError::ArcCloneError)
                .expect("Multiple Arc clones exist; cannot unwrap")
                .into_inner()
                .map_err(|_| MeteringError::MutexPoisonedError)
                .expect("Mutex poisoned");

            meter_event.submit().await;
        }
    }
}

impl Drop for MeterEventGuard {
    fn drop(&mut self) {
        if let Some(meter_event_arc) = self.inner.take() {
            METER_EVENT_STACK.with(|slot| {
                slot.borrow_mut().pop();
            });
            if let Ok(meter_event_guard) = Arc::try_unwrap(meter_event_arc) {
                if let Ok(meter_event_guard) = meter_event_guard.into_inner() {
                    let _ = tokio::spawn(async move {
                        meter_event_guard.submit().await;
                    });
                }
            }
        }
    }
}

pub fn attach<T: MeterEventData>(mutator: impl FnOnce(&mut T)) {
    METER_EVENT_STACK.with(|slot| {
        if let Some(meter_event_arc) = slot.borrow().last() {
            if let Ok(mut meter_event_locked) = meter_event_arc.lock() {
                if let Some(meter_event_data) = meter_event_locked.data.as_any().downcast_mut::<T>()
                {
                    mutator(meter_event_data);
                }
            }
        }
    })
}
