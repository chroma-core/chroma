use std::{sync::OnceLock, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{
    runtime::Handle,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::{error::Elapsed, timeout},
};
use tonic::async_trait;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event_name")]
pub enum MeterEventType {
    CollectionRead {
        collection_id: Uuid,
        collection_dimension: u64,
        latest_collection_record_count: u64,
        metadata_bytes_read: u64,
        vector_bytes_read: u64,
    },
    CollectionWrite {
        collection_id: Uuid,
        log_bytes: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MeterEvent {
    #[serde(rename = "idempotency_key")]
    pub event_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub tenant: String,
    pub database: String,
    #[serde(flatten)]
    pub io: MeterEventType,
}

#[derive(Clone, Debug)]
pub enum MeterMessage {
    Event(MeterEvent),
    Stop,
}

pub static METER_EVENT_SENDER: OnceLock<UnboundedSender<MeterMessage>> = OnceLock::new();

#[async_trait]
pub trait MeterEventHandler {
    fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(60)
    }
    async fn handle(&mut self, _event: MeterEvent) {}
    async fn listen(&mut self, mut rx: UnboundedReceiver<MeterMessage>) {
        self.on_start().await;
        loop {
            match timeout(self.heartbeat_interval(), rx.recv()).await {
                Ok(Some(MeterMessage::Event(event))) => self.handle(event).await,
                Ok(Some(MeterMessage::Stop)) | Ok(None) => break,
                Err(elapsed) => self.on_heartbeat(elapsed).await,
            }
        }
        self.on_stop().await;
    }
    async fn on_heartbeat(&mut self, _elapsed: Elapsed) {}
    async fn on_start(&mut self) {}
    async fn on_stop(&mut self) {}
}

#[async_trait]
impl MeterEventHandler for () {}

impl MeterEvent {
    pub fn collection_read(
        tenant: String,
        database: String,
        collection_id: Uuid,
        collection_dimension: u64,
        latest_collection_record_count: u64,
        metadata_complexity: u64,
        vector_complexity: u64,
    ) -> Self {
        // TODO: Properly calculate number of bytes read
        Self {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            tenant,
            database,
            io: MeterEventType::CollectionRead {
                collection_id,
                collection_dimension,
                latest_collection_record_count,
                metadata_bytes_read: metadata_complexity * latest_collection_record_count,
                vector_bytes_read: vector_complexity
                    * collection_dimension
                    * latest_collection_record_count,
            },
        }
    }

    pub fn collection_write(
        tenant: String,
        database: String,
        collection_id: Uuid,
        log_bytes: u64,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            tenant,
            database,
            io: MeterEventType::CollectionWrite {
                collection_id,
                log_bytes,
            },
        }
    }

    pub fn submit(self) {
        if let Some(handler) = METER_EVENT_SENDER.get() {
            if let Err(err) = handler.send(MeterMessage::Event(self)) {
                tracing::error!("Unable to send meter event: {err}")
            }
        }
    }

    pub fn init_handler(mut handler: impl MeterEventHandler + Send + Sync + 'static) {
        let (tx, rx) = unbounded_channel();
        if METER_EVENT_SENDER.set(tx).is_err() {
            tracing::error!("Meter event handler is already initialized")
        }
        let runtime_handle = Handle::current();
        runtime_handle.spawn(async move { handler.listen(rx).await });
    }

    pub fn stop_handler() {
        if let Some(handler) = METER_EVENT_SENDER.get() {
            if let Err(err) = handler.send(MeterMessage::Stop) {
                tracing::error!("Unable to stop meter event handler: {err}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::MeterEvent;

    #[test]
    fn test_event_serialization() {
        let event = MeterEvent::collection_read(
            "test_tenant".to_string(),
            "test_database".to_string(),
            Uuid::new_v4(),
            1000,
            384,
            1,
            3,
        );
        let json_str = serde_json::to_string(&event).expect("The event should be serializable");
        let json_event =
            serde_json::from_str::<MeterEvent>(&json_str).expect("Json should be deserializable");
        assert_eq!(json_event, event);
    }
}
