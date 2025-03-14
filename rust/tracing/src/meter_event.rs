use std::sync::OnceLock;

use chroma_system::ReceiverForMessage;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::Span;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ReadAction {
    Count,
    Get,
    Query,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum WriteAction {
    Add,
    Delete,
    Update,
    Upsert,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "event_name")]
pub enum MeterEvent {
    CollectionRead {
        #[serde(rename = "idempotency_key")]
        event_id: Uuid,
        timestamp: DateTime<Utc>,
        tenant: String,
        database: String,
        collection_id: Uuid,
        action: ReadAction,
        read_bytes: u64,
        return_bytes: u64,
    },
    CollectionWrite {
        #[serde(rename = "idempotency_key")]
        event_id: Uuid,
        timestamp: DateTime<Utc>,
        tenant: String,
        database: String,
        collection_id: Uuid,
        action: WriteAction,
        write_bytes: u64,
    },
}

pub static METER_EVENT_RECEIVER: OnceLock<Box<dyn ReceiverForMessage<MeterEvent>>> =
    OnceLock::new();

impl MeterEvent {
    pub fn collection_read(
        tenant: String,
        database: String,
        collection_id: Uuid,
        action: ReadAction,
        read_bytes: u64,
        return_bytes: u64,
    ) -> Self {
        Self::CollectionRead {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            tenant,
            database,
            collection_id,
            action,
            read_bytes,
            return_bytes,
        }
    }

    pub fn collection_write(
        tenant: String,
        database: String,
        collection_id: Uuid,
        action: WriteAction,
        write_bytes: u64,
    ) -> Self {
        Self::CollectionWrite {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            tenant,
            database,
            collection_id,
            action,
            write_bytes,
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
            crate::meter_event::ReadAction::Query,
            1000000,
            1000,
        );
        let json_str = serde_json::to_string(&event).expect("The event should be serializable");
        let json_event =
            serde_json::from_str::<MeterEvent>(&json_str).expect("Json should be deserializable");
        assert_eq!(json_event, event);
    }
}
