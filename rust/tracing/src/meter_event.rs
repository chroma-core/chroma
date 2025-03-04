use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{
    runtime::Handle,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
use tonic::async_trait;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event_name")]
pub enum IoEvent {
    CollectionRead {
        collection_id: Uuid,
        collection_record_count: u64,
        collection_dimension: u64,
        metadata: u64,
        vector: u64,
    },
    CollectionWrite {
        collection_id: Uuid,
        log_bytes: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MeterEvent {
    #[serde(rename = "idempotency_key")]
    event_id: Uuid,
    timestamp: DateTime<Utc>,
    tenant: String,
    database: String,
    #[serde(flatten)]
    io: IoEvent,
}

impl MeterEvent {
    pub fn collection_read(
        tenant: String,
        database: String,
        collection_id: Uuid,
        collection_record_count: u64,
        collection_dimension: u64,
        metadata: u64,
        vector: u64,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            tenant,
            database,
            io: IoEvent::CollectionRead {
                collection_id,
                collection_record_count,
                collection_dimension,
                metadata,
                vector,
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
            io: IoEvent::CollectionWrite {
                collection_id,
                log_bytes,
            },
        }
    }
}

pub static METER_EVENT_SENDER: OnceLock<UnboundedSender<Option<MeterEvent>>> = OnceLock::new();

#[async_trait]
pub trait MeterEventHandler {
    async fn handle(&mut self, _event: MeterEvent) {}
    async fn listen(&mut self, mut rx: UnboundedReceiver<Option<MeterEvent>>) {
        while let Some(event) = rx.recv().await.flatten() {
            self.handle(event).await
        }
        self.on_stop().await;
    }
    async fn on_start(&mut self) {}
    async fn on_stop(&mut self) {}
}

#[async_trait]
impl MeterEventHandler for () {}

impl MeterEvent {
    pub fn submit(self) {
        if let Some(handler) = METER_EVENT_SENDER.get() {
            if let Err(err) = handler.send(Some(self)) {
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
        runtime_handle.spawn(async move {
            handler.on_start().await;
            handler.listen(rx).await;
        });
    }

    pub fn stop_handler() {
        if let Some(handler) = METER_EVENT_SENDER.get() {
            if let Err(err) = handler.send(None) {
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
