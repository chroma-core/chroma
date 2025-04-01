use crate::events::ProductTelemetryEvent;
use async_trait::async_trait;

#[async_trait]
pub trait TelemetryClient {
    async fn capture(&mut self, event: Box<dyn ProductTelemetryEvent + Send + Sync>);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::ClientStartEvent;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    struct TestTelemetryClient {
        captured_events: Arc<Mutex<Vec<Box<dyn ProductTelemetryEvent + Send + Sync>>>>,
        should_fail: Arc<AtomicBool>,
    }

    impl TestTelemetryClient {
        fn new() -> Self {
            Self {
                captured_events: Arc::new(Mutex::new(Vec::new())),
                should_fail: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    #[async_trait]
    impl TelemetryClient for TestTelemetryClient {
        async fn capture(&mut self, event: Box<dyn ProductTelemetryEvent + Send + Sync>) {
            if self.should_fail.load(Ordering::SeqCst) {
                return;
            }
            self.captured_events.lock().await.push(event);
        }
    }

    #[tokio::test]
    async fn test_telemetry_client_capture() {
        let mut client = TestTelemetryClient::new();
        let event = Box::new(ClientStartEvent::new());

        client.capture(event).await;

        let captured = client.captured_events.lock().await;
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].name(), "ClientStartEvent");
    }

    #[tokio::test]
    async fn test_telemetry_client_error_handling() {
        let mut client = TestTelemetryClient::new();
        client.should_fail.store(true, Ordering::SeqCst);

        let event = Box::new(ClientStartEvent::new());
        client.capture(event).await;

        let captured = client.captured_events.lock().await;
        assert_eq!(captured.len(), 0);
    }
}
