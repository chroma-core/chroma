use std::sync::OnceLock;

use chroma_system::{ChannelError, ReceiverForMessage};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tonic::async_trait;
use tracing::Span;
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

pub static METER_EVENT_RECEIVER: OnceLock<Box<dyn ReceiverForMessage<MeterEvent>>> =
    OnceLock::new();

#[async_trait]
impl ReceiverForMessage<MeterEvent> for () {
    async fn send(&self, _: MeterEvent, _: Option<Span>) -> Result<(), ChannelError> {
        Ok(())
    }
}

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
