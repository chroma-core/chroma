use std::sync::LazyLock;

use thiserror::Error;
use tokio::{
    runtime::Handle,
    sync::mpsc::{unbounded_channel, UnboundedSender},
};
use tracing::{instrument::WithSubscriber, Event, Subscriber};
use tracing_subscriber::{
    layer::{Context, SubscriberExt},
    registry, Layer,
};

// NOTE: Metering events should be issued under tokio runtime
pub static METER_EVENT_SENDER: LazyLock<UnboundedSender<MeterEvent>> = LazyLock::new(|| {
    let runtime_handle = Handle::current();
    let (tx, mut rx) = unbounded_channel::<MeterEvent>();
    runtime_handle.spawn(
        async move {
            while let Some(event) = rx.recv().await {
                event.emit()
            }
        }
        .with_subscriber(registry().with(MeterLayer {})),
    );
    tx
});

#[derive(Clone, Debug)]
pub enum MeterEvent {
    Heartbeat(u128),
}

impl MeterEvent {
    fn emit(self) {
        match self {
            MeterEvent::Heartbeat(epoch) => {
                tracing::info!(meter_event = "heartbeat", epoch)
            }
        }
    }

    pub fn submit(self) {
        if let Err(err) = METER_EVENT_SENDER.send(self) {
            tracing::error!("Unable to send meter event: {err}")
        }
    }
}

#[derive(Debug, Error)]
pub enum MeterEventConversionError {
    #[error("Invalid meter event field: {0}")]
    Field(String),
    #[error("Invalid meter event kind: {0}")]
    Kind(String),
    #[error("Not a meter event: {0}")]
    Invalid(String),
}

pub struct MeterLayer {}

impl<S: Subscriber> Layer<S> for MeterLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        println!("Intercepted event: {event:?}")
    }
}
