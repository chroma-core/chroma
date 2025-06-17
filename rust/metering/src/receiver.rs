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
    use uuid::Uuid;

    use crate::core::{Enterable, MeterEvent};

    #[tokio::test]
    async fn test_init_custom_receiver() {
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        struct TestComponent {
            pub messages: Arc<Mutex<Vec<String>>>,
        }

        impl std::fmt::Debug for TestComponent {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("TestComponent").finish()
            }
        }

        #[async_trait::async_trait]
        impl Component for TestComponent {
            fn get_name() -> &'static str {
                "TestComponent"
            }

            fn queue_size(&self) -> usize {
                100
            }

            async fn on_start(&mut self, _: &ComponentContext<Self>) {}

            fn on_stop_timeout(&self) -> std::time::Duration {
                std::time::Duration::from_secs(1)
            }
        }

        #[async_trait::async_trait]
        impl Handler<MeterEvent> for TestComponent {
            type Result = Option<()>;

            async fn handle(
                &mut self,
                message: MeterEvent,
                _context: &ComponentContext<Self>,
            ) -> Self::Result {
                self.messages.lock().unwrap().push(format!("{:?}", message));
                None
            }
        }

        let system = System::new();

        let shared_messages = Arc::new(Mutex::new(vec![]));

        let test_component = TestComponent {
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
