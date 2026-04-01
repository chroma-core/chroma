//! HTTP-based meter event sender that forwards events to a billing
//! metering sidecar. Events are buffered and flushed in batches to
//! reduce per-event HTTP overhead.

use async_trait::async_trait;
use chroma_metering::MeterEvent;
use chroma_system::{ChannelError, ReceiverForMessage};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Configuration for the HTTP meter event sender.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpMeteringConfig {
    /// Base URL of the metering sidecar (e.g., "http://localhost:8083").
    pub url: String,
    /// Request timeout in milliseconds. Default: 5000.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Maximum number of events to buffer before flushing. Default: 50.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_timeout_ms() -> u64 {
    5000
}

fn default_batch_size() -> usize {
    50
}

/// HTTP meter event sender that forwards MeterEvents to a sidecar.
///
/// Events are sent individually in a fire-and-forget manner.
/// Failures are logged but never block the caller.
#[derive(Debug, Clone)]
pub struct HttpMeterEventSender {
    client: Client,
    events_url: String,
    buffer: Arc<Mutex<Vec<MeterEvent>>>,
    batch_size: usize,
}

impl HttpMeterEventSender {
    pub fn new(config: &HttpMeteringConfig) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .build()
            .expect("Failed to build HTTP client for metering");

        let base = config.url.trim_end_matches('/');
        Self {
            client,
            events_url: format!("{}/events", base),
            buffer: Arc::new(Mutex::new(Vec::with_capacity(config.batch_size))),
            batch_size: config.batch_size,
        }
    }

    /// Flush buffered events to the sidecar.
    async fn flush(&self, events: Vec<MeterEvent>) {
        if events.is_empty() {
            return;
        }
        let client = self.client.clone();
        let url = self.events_url.clone();
        // Fire-and-forget: spawn in background so we don't block the caller.
        tokio::spawn(async move {
            if let Err(e) = client.post(&url).json(&events).send().await {
                tracing::warn!("Failed to send metering events to sidecar: {}", e);
            }
        });
    }
}

#[async_trait]
impl ReceiverForMessage<MeterEvent> for HttpMeterEventSender {
    async fn send(
        &self,
        message: MeterEvent,
        _tracing_context: Option<tracing::Span>,
    ) -> Result<(), ChannelError> {
        let events_to_flush = {
            let mut buffer = self.buffer.lock().await;
            buffer.push(message);
            if buffer.len() >= self.batch_size {
                Some(std::mem::take(&mut *buffer))
            } else {
                None
            }
        };

        if let Some(events) = events_to_flush {
            self.flush(events).await;
        }

        Ok(())
    }
}
