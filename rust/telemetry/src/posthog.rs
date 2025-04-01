use crate::client::TelemetryClient;
use crate::events::ProductTelemetryEvent;
use async_trait::async_trait;
use log::error;
use posthog_rs::{Client, Event};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

const POSTHOG_API_KEY: &str = "phc_YeUxaojbKk5KPi8hNlx1bBKHzuZ4FDtl67kH1blv8Bh";
const UNKNOWN_USER_ID: &str = "UNKNOWN";

pub struct Posthog {
    client: Client,
    user_id: String,
    is_server: bool,
    chroma_version: String,
    batched_events: Arc<Mutex<HashMap<String, Box<dyn ProductTelemetryEvent + Send + Sync>>>>,
    seen_event_types: Arc<Mutex<HashSet<String>>>,
}

impl Posthog {
    pub async fn new(user_id: Option<String>, is_server: bool, chroma_version: String) -> Self {
        let client = posthog_rs::client(POSTHOG_API_KEY).await;
        let user_id = user_id.unwrap_or_else(|| UNKNOWN_USER_ID.to_string());

        Posthog {
            client,
            user_id,
            is_server,
            chroma_version,
            batched_events: Arc::new(Mutex::new(HashMap::new())),
            seen_event_types: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    async fn direct_capture(&self, event: Box<dyn ProductTelemetryEvent + Send + Sync>) {
        let event_name = event.name();
        let event_properties = event.properties();

        let mut posthog_event = Event::new(&event_name, &self.user_id);

        posthog_event
            .insert_prop("chroma_version", &self.chroma_version)
            .ok();
        posthog_event
            .insert_prop("server_context", self.is_server)
            .ok();

        posthog_event
            .insert_prop("properties", &event_properties)
            .ok();

        if let Err(e) = self.client.capture(posthog_event).await {
            error!("Failed to send telemetry event {}: {}", event_name, e);
        }
    }
}

#[async_trait]
impl TelemetryClient for Posthog {
    async fn capture(&mut self, event: Box<dyn ProductTelemetryEvent + Send + Sync>) {
        // Disable telemetry capture when running Rust tests (`cargo test`)
        // or when run from Python tests (checking env var)
        let in_pytest = std::env::var("CHROMA_IN_PYTEST").map_or(false, |val| val == "1");
        if cfg!(test) || in_pytest {
            return;
        }

        let batch_key = event.batch_key();
        let max_batch_size = event.max_batch_size();

        let mut seen_types = self.seen_event_types.lock().await;

        if max_batch_size == 1 || !seen_types.contains(&batch_key) {
            seen_types.insert(batch_key.clone());
            drop(seen_types);
            self.direct_capture(event).await;
            return;
        }

        drop(seen_types);

        let mut batched = self.batched_events.lock().await;

        if let Some(existing_event) = batched.remove(&batch_key) {
            match existing_event.batch(event) {
                Ok(batched_event) => {
                    if batched_event.batch_size() >= batched_event.max_batch_size() {
                        drop(batched);
                        self.direct_capture(batched_event).await;
                    } else {
                        batched.insert(batch_key, batched_event);
                    }
                }
                Err(e) => {
                    error!(
                        "Error batching event type {}: {}. Discarding events.",
                        batch_key, e
                    );
                }
            }
        } else {
            batched.insert(batch_key, event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::*;

    #[tokio::test]
    async fn test_posthog_initialization() {
        let posthog = Posthog::new(None, false, "1.0.0".to_string()).await;
        assert_eq!(posthog.user_id, UNKNOWN_USER_ID);
        assert!(!posthog.is_server);
        assert_eq!(posthog.chroma_version, "1.0.0");

        let posthog = Posthog::new(Some("test-user".to_string()), true, "2.0.0".to_string()).await;
        assert_eq!(posthog.user_id, "test-user");
        assert!(posthog.is_server);
        assert_eq!(posthog.chroma_version, "2.0.0");
    }

    #[tokio::test]
    async fn test_capture_non_batchable_event() {
        let mut posthog =
            Posthog::new(Some("test-user".to_string()), false, "1.0.0".to_string()).await;
        let event = Box::new(ClientStartEvent::new());
        posthog.capture(event).await;
        // Note: We can't verify the actual event was sent since we're using the real client
    }

    #[tokio::test]
    async fn test_capture_batchable_event() {
        let mut posthog =
            Posthog::new(Some("test-user".to_string()), false, "1.0.0".to_string()).await;
        let uuid = "test-uuid".to_string();
        let event1 = Box::new(CollectionAddEvent::new(uuid.clone(), 10, 5, 3, 2, 1));
        let event2 = Box::new(CollectionAddEvent::new(uuid.clone(), 20, 10, 6, 4, 1));

        posthog.capture(event1).await;
        posthog.capture(event2).await;
        // Note: We can't verify the actual event was sent since we're using the real client
    }
}
