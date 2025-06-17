use chroma_system::ReceiverForMessage;
use std::sync::OnceLock;
use tracing::Span;

use crate::core::MeterEvent;

pub static METER_EVENT_RECEIVER: OnceLock<Box<dyn ReceiverForMessage<MeterEvent>>> =
    OnceLock::new();

impl MeterEvent {
    pub fn init_receiver(receiver: Box<dyn ReceiverForMessage<MeterEvent>>) {
        if METER_EVENT_RECEIVER.set(receiver).is_err() {
            tracing::error!("Meter event handler is already initialized")
        }
    }

    pub async fn submit(self) {
        if let Some(handler) = METER_EVENT_RECEIVER.get() {
            if let Err(err) = handler.send(self, Some(Span::current())).await {
                tracing::error!("Unable to send meter event: {err}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chroma_system::{Component, ComponentContext, Handler, ReceiverForMessage, System};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    use crate::{
        core::{Enterable, MeterEvent},
        test_utils::MeteringTestComponent,
    };

    #[tokio::test]
    async fn test_init_custom_receiver() {
        let system = System::new();

        let shared_messages = Arc::new(Mutex::new(vec![]));

        let test_component = MeteringTestComponent {
            messages: shared_messages.clone(),
        };

        let component_handle = system.start_component(test_component);

        let custom_receiver: Box<dyn ReceiverForMessage<MeterEvent>> = component_handle.receiver();

        let _ = MeterEvent::init_receiver(custom_receiver);

        let metering_context_container = crate::create::<crate::core::CollectionForkContext>(
            crate::core::CollectionForkContext {
                tenant: "tenant".to_string(),
                database: "database".to_string(),
                collection_id: Uuid::new_v4(),
                latest_collection_logical_size_bytes: 0,
            },
        );
        metering_context_container.enter();

        let expected_metering_context = crate::close::<crate::core::CollectionForkContext>();
        assert!(expected_metering_context.is_ok());

        MeterEvent::CollectionFork(expected_metering_context.unwrap())
            .submit()
            .await;

        // Wait a bit to allow the message to propagate
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(shared_messages.lock().unwrap().len(), 1);
    }
}
