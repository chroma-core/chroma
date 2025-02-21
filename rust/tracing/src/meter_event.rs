use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};
use valuable::Valuable;

#[derive(Clone, Debug, Valuable)]
pub enum MeterEvent {
    CollectionAdd,
    CollectionGet,
    CollectionQuery,
    Heartbeat(u32),
}

impl MeterEvent {
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
    ($event:expr) => {
        tracing::info!(target: MeterEvent::trace_target(), meter_event = <MeterEvent as $crate::Valuable>::as_value(&$event));
    };
}
