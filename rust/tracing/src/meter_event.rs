use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};

#[derive(Clone, Debug)]
pub enum MeterEvent {
    CollectionAdd,
    CollectionGet,
    CollectionQuery,
    Heartbeat,
}

impl MeterEvent {
    pub const fn trace_event_type(&self) -> &'static str {
        match self {
            MeterEvent::CollectionAdd => "collection_add",
            MeterEvent::CollectionGet => "collection_get",
            MeterEvent::CollectionQuery => "collection_query",
            MeterEvent::Heartbeat => "heartbeat",
        }
    }

    pub const fn trace_target() -> &'static str {
        "meter"
    }
}

pub struct MeterLayer {}

impl<S: Subscriber> Layer<S> for MeterLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() == MeterEvent::trace_target() {
            println!(">>>>>> {event:?} <<<<<<")
        }
    }
}

#[macro_export]
macro_rules! meter {
    // Event, Payloads
    ($event:expr, $($arg:tt)+) => {
        tracing::info!(target: MeterEvent::trace_target(), event_type = MeterEvent::trace_event_type(&$event), $($arg)+);
    };
}
