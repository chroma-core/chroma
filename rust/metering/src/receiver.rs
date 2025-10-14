use chroma_system::ReceiverForMessage;
use std::sync::OnceLock;
use tracing::Span;

use crate::{core::MeterEvent, MeteringError};

pub static METER_EVENT_RECEIVER: OnceLock<Box<dyn ReceiverForMessage<MeterEvent>>> =
    OnceLock::new();

impl MeterEvent {
    pub fn init_receiver(receiver: Box<dyn ReceiverForMessage<MeterEvent>>) {
        if METER_EVENT_RECEIVER.set(receiver).is_err() {
            tracing::error!("Meter event handler is already initialized")
        }
    }

    pub async fn submit(self) -> Result<(), MeteringError> {
        if let Some(handler) = METER_EVENT_RECEIVER.get() {
            match handler.send(self, Some(Span::current())).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::error!("Unable to send meter event: {error}");
                    return Err(MeteringError::SendError(error.to_string()));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, OnceLock};

    use crate::{core::MeterEvent, CollectionForkContext, MeteredFutureExt};
    use chroma_system::{Component, ComponentContext, Handler, ReceiverForMessage, System};
    #[derive(Clone)]
    struct MeteringTestComponent {
        messages: Arc<Mutex<Vec<String>>>,
    }

    impl std::fmt::Debug for MeteringTestComponent {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MeteringTestComponent").finish()
        }
    }

    #[async_trait::async_trait]
    impl Component for MeteringTestComponent {
        fn get_name() -> &'static str {
            "MeteringTestComponent"
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
    impl Handler<MeterEvent> for MeteringTestComponent {
        type Result = Option<()>;
        async fn handle(
            &mut self,
            message: MeterEvent,
            _ctx: &ComponentContext<Self>,
        ) -> Self::Result {
            self.messages.lock().unwrap().push(format!("{:?}", message));
            None
        }
    }
    static TEST_ENV: OnceLock<(System, Arc<Mutex<Vec<String>>>)> = OnceLock::new();
    fn test_env() -> Arc<Mutex<Vec<String>>> {
        TEST_ENV
            .get_or_init(|| {
                let system = System::new();
                let messages = Arc::new(Mutex::new(Vec::new()));
                let component = MeteringTestComponent {
                    messages: messages.clone(),
                };
                let handle = system.start_component(component);
                let rx: Box<dyn ReceiverForMessage<MeterEvent>> = handle.receiver();
                MeterEvent::init_receiver(rx);
                (system, messages.clone())
            })
            .1
            .clone()
    }

    #[tokio::test]
    async fn test_init_custom_receiver() {
        let messages = test_env();
        messages.lock().unwrap().clear();

        async fn helper() {
            if let Ok(metering_context) = crate::core::close::<CollectionForkContext>() {
                let _ = MeterEvent::CollectionFork(metering_context).submit().await;
            }
        }

        let metering_context_container =
            crate::create::<CollectionForkContext>(CollectionForkContext::new(
                "tenant".to_string(),
                "database".to_string(),
                "collection".to_string(),
            ));
        helper().meter(metering_context_container).await;

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(messages.lock().unwrap().len(), 1);
    }
}
