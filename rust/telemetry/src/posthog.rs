use crate::client::TelemetryClient;
use crate::client::EVENT_SENDER;
use crate::events::ProductTelemetryEvent;
use async_trait::async_trait;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_system::ReceiverForMessage;
use chroma_system::{Component, ComponentContext, Handler};
use chrono::{DateTime, Utc};
use posthog_rs::{Client, Event};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tracing::span;
use tracing::warn;

const POSTHOG_API_KEY: &str = "phc_YeUxaojbKk5KPi8hNlx1bBKHzuZ4FDtl67kH1blv8Bh";
const UNKNOWN_USER_ID: &str = "UNKNOWN";
const MAX_BUFFER_SIZE: usize = 100;
const FLUSH_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PosthogConfig {
    pub user_id: Option<String>,
    pub is_server: bool,
    pub chroma_version: String,
}
pub struct PosthogClient {
    client: Arc<Client>,
    user_id: String,
    is_server: bool,
    chroma_version: String,
    last_flush_time: DateTime<Utc>,
    batched_events: Arc<Mutex<HashMap<String, Box<dyn ProductTelemetryEvent + Send + Sync>>>>,
    seen_event_types: Arc<RwLock<HashSet<String>>>,
    buffer: Vec<Event>,
}

impl fmt::Debug for PosthogClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Posthog")
    }
}
#[async_trait]
impl Configurable<PosthogConfig> for PosthogClient {
    async fn try_from_config(
        config: &PosthogConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        Ok(PosthogClient::new(
            config.user_id.clone(),
            config.is_server,
            config.chroma_version.clone(),
        )
        .await)
    }
}

#[async_trait]
impl Component for PosthogClient {
    fn get_name() -> &'static str {
        "PosthogClient"
    }

    fn queue_size(&self) -> usize {
        MAX_BUFFER_SIZE
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) -> () {
        init_receiver(ctx.receiver());
        ctx.scheduler
            .schedule(ScheduleEvent, FLUSH_INTERVAL, ctx, || {
                Some(span!(parent: None, tracing::Level::INFO, "Scheduled telemetry event flush"))
            });
    }

    async fn on_stop(&mut self) -> Result<(), Box<dyn ChromaError>> {
        self.flush().await;
        Ok(())
    }

    fn on_stop_timeout(&self) -> Duration {
        FLUSH_INTERVAL
    }
}

#[async_trait]
impl Handler<Box<dyn ProductTelemetryEvent + Send + Sync>> for PosthogClient {
    type Result = ();

    async fn handle(
        &mut self,
        event: Box<dyn ProductTelemetryEvent + Send + Sync>,
        _: &ComponentContext<Self>,
    ) -> () {
        self.aggregate(event).await;
        self.batch_and_flush().await;
    }
}

#[derive(Debug)]
pub struct ScheduleEvent;

#[async_trait]
impl Handler<ScheduleEvent> for PosthogClient {
    type Result = ();

    async fn handle(&mut self, _: ScheduleEvent, ctx: &ComponentContext<Self>) {
        self.batch_and_flush().await;
        ctx.scheduler
            .schedule(ScheduleEvent, FLUSH_INTERVAL, ctx, || {
                Some(span!(parent: None, tracing::Level::INFO, "Scheduled telemetry event flush"))
            });
    }
}

impl PosthogClient {
    pub async fn new(user_id: Option<String>, is_server: bool, chroma_version: String) -> Self {
        let client = posthog_rs::client(POSTHOG_API_KEY).await;
        let user_id = user_id.unwrap_or_else(|| UNKNOWN_USER_ID.to_string());

        PosthogClient {
            client: Arc::new(client),
            user_id,
            is_server,
            chroma_version,
            last_flush_time: Utc::now(),
            batched_events: Arc::new(Mutex::new(HashMap::new())),
            seen_event_types: Arc::new(RwLock::new(HashSet::new())),
            buffer: Vec::new(),
        }
    }

    async fn convert_to_posthog_event(
        &mut self,
        event: Box<dyn ProductTelemetryEvent + Send + Sync>,
    ) {
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

        self.buffer.push(posthog_event);
    }
}

pub fn init_receiver(
    receiver: Box<dyn ReceiverForMessage<Box<dyn ProductTelemetryEvent + Send + Sync>>>,
) {
    if EVENT_SENDER.set(receiver).is_err() {
        warn!("Failed to initialize telemetry sender");
    }
}

#[async_trait]
impl TelemetryClient for PosthogClient {
    async fn aggregate(&mut self, event: Box<dyn ProductTelemetryEvent + Send + Sync>) {
        let batch_key = event.batch_key();
        let max_batch_size = event.max_batch_size();

        let should_capture_directly = {
            let seen_types = self.seen_event_types.read().await;
            max_batch_size == 1 || !seen_types.contains(&batch_key)
        };

        if should_capture_directly {
            self.seen_event_types
                .write()
                .await
                .insert(batch_key.clone());
        }

        let event_to_process = {
            let mut batched = self.batched_events.lock().await;

            if let Some(existing_event) = batched.remove(&batch_key) {
                match existing_event.batch(event) {
                    Ok(batched_event) => {
                        if batched_event.batch_size() >= batched_event.max_batch_size() {
                            // Return event here to be processed outside of lock
                            Some(batched_event)
                        } else {
                            batched.insert(batch_key, batched_event);
                            None
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Error batching event type {}: {}. Discarding events.",
                            batch_key, e
                        );
                        None
                    }
                }
            } else {
                batched.insert(batch_key, event);
                None
            }
        }; // Guard released here

        if let Some(event) = event_to_process {
            self.convert_to_posthog_event(event).await;
        }
    }

    async fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        let buffer = std::mem::take(&mut self.buffer);
        if let Err(e) = self.client.capture_batch(buffer).await {
            warn!("Failed to send telemetry events: {}", e);
        }

        self.last_flush_time = Utc::now();
    }

    async fn batch_and_flush(&mut self) {
        if self.buffer.len() >= MAX_BUFFER_SIZE
            || (Utc::now() - self.last_flush_time).to_std().unwrap() > FLUSH_INTERVAL
        {
            self.flush().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::*;

    #[tokio::test]
    async fn test_posthog_initialization() {
        let posthog = PosthogClient::new(None, false, "1.0.0".to_string()).await;
        assert_eq!(posthog.user_id, UNKNOWN_USER_ID);
        assert!(!posthog.is_server);
        assert_eq!(posthog.chroma_version, "1.0.0");

        let posthog =
            PosthogClient::new(Some("test-user".to_string()), true, "2.0.0".to_string()).await;
        assert_eq!(posthog.user_id, "test-user");
        assert!(posthog.is_server);
        assert_eq!(posthog.chroma_version, "2.0.0");
    }

    #[tokio::test]
    async fn test_event_batching_logic() {
        let mut batched_events: HashMap<String, Box<dyn ProductTelemetryEvent + Send + Sync>> =
            HashMap::new();
        let mut seen_event_types: HashSet<String> = HashSet::new();
        let mut captured_events: Vec<Box<dyn ProductTelemetryEvent + Send + Sync>> = Vec::new();

        // Mock direct_capture
        let mock_direct_capture =
            |event: Box<dyn ProductTelemetryEvent + Send + Sync>,
             captured: &mut Vec<Box<dyn ProductTelemetryEvent + Send + Sync>>| {
                captured.push(event);
            };

        // Simulate capturing CollectionAddEvents (assuming max_batch_size = 3 for testing)
        let uuid = "test-batch-uuid".to_string();

        // Event 1
        let event1 = Box::new(CollectionAddEvent::new(uuid.clone(), 10, 5, 3, 2, 1));
        let batch_key1 = event1.batch_key();
        let max_batch_size1 = 3; // Assume max_batch_size is 3 for this test

        // --- Simulate capture call 1 ---
        if max_batch_size1 == 1 || !seen_event_types.contains(&batch_key1) {
            seen_event_types.insert(batch_key1.clone());
            mock_direct_capture(event1, &mut captured_events);
        }
        assert_eq!(captured_events.len(), 1);
        assert_eq!(captured_events[0].name(), "CollectionAddEvent".to_string());

        // Event 2
        let event2 = Box::new(CollectionAddEvent::new(uuid.clone(), 20, 10, 6, 4, 1));
        let batch_key2 = event2.batch_key();
        let max_batch_size2 = 3;

        // --- Simulate capture call 2 ---
        if max_batch_size2 != 1 && seen_event_types.contains(&batch_key2) {
            if let Some(existing_event) = batched_events.remove(&batch_key2) {
                match existing_event.batch(event2) {
                    Ok(batched_event) => {
                        if batched_event.batch_size() >= max_batch_size2 {
                            mock_direct_capture(batched_event, &mut captured_events);
                        } else {
                            batched_events.insert(batch_key2, batched_event);
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Error batching event type {}: {}. Discarding events.",
                            batch_key2, e
                        );
                    }
                }
            } else {
                batched_events.insert(batch_key2, event2);
            }
        }
        assert_eq!(captured_events.len(), 1); // Still 1, event 2 should be batched
        assert!(batched_events.contains_key(&batch_key1));
        assert_eq!(batched_events[&batch_key1].batch_size(), 1); // Event 2 is now batched

        // Event 3
        let event3 = Box::new(CollectionAddEvent::new(uuid.clone(), 5, 2, 1, 1, 1));
        let batch_key3 = event3.batch_key();
        let max_batch_size3 = 3;

        // --- Simulate capture call 3 ---
        if max_batch_size3 != 1 && seen_event_types.contains(&batch_key3) {
            if let Some(existing_event) = batched_events.remove(&batch_key3) {
                match existing_event.batch(event3) {
                    Ok(batched_event) => {
                        // Batch size is now 1 (existing) + 1 (event3) = 2
                        assert_eq!(batched_event.batch_size(), 2);
                        if batched_event.batch_size() >= max_batch_size3 {
                            mock_direct_capture(batched_event, &mut captured_events);
                        } else {
                            batched_events.insert(batch_key3, batched_event);
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Error batching event type {}: {}. Discarding events.",
                            batch_key3, e
                        );
                    }
                }
            }
        }
        assert_eq!(captured_events.len(), 1); // Still 1, batch size is 2, max is 3
        assert!(batched_events.contains_key(&batch_key1));
        assert_eq!(batched_events[&batch_key1].batch_size(), 2);

        // Event 4
        let event4 = Box::new(CollectionAddEvent::new(uuid.clone(), 15, 7, 4, 2, 1));
        let batch_key4 = event4.batch_key();
        let max_batch_size4 = 3;

        // --- Simulate capture call 4 ---
        if max_batch_size4 != 1 && seen_event_types.contains(&batch_key4) {
            if let Some(existing_event) = batched_events.remove(&batch_key4) {
                match existing_event.batch(event4) {
                    Ok(batched_event) => {
                        // Batch size is now 2 (existing) + 1 (event4) = 3
                        assert_eq!(batched_event.batch_size(), 3);
                        if batched_event.batch_size() >= max_batch_size4 {
                            mock_direct_capture(batched_event, &mut captured_events);
                        }
                    }
                    Err(_) => { /* handle error - maybe warn! here too if needed in test */ }
                }
            }
        }
        assert_eq!(captured_events.len(), 2);
        assert!(!batched_events.contains_key(&batch_key1));
        assert_eq!(captured_events[1].name(), "CollectionAddEvent".to_string());
        assert_eq!(captured_events[1].batch_size(), 3);

        let props = captured_events[1].properties();
        assert_eq!(props["add_amount"].as_u64(), Some((20 + 5 + 15) as u64));
        assert_eq!(props["with_documents"].as_u64(), Some((10 + 2 + 7) as u64));
        assert_eq!(props["with_metadata"].as_u64(), Some((6 + 1 + 4) as u64));
        assert_eq!(props["with_uris"].as_u64(), Some((4 + 1 + 2) as u64));
        assert_eq!(props["batch_size"].as_u64(), Some(3_u64));
    }
}
