use std::sync::OnceLock;

use chroma_system::ReceiverForMessage;
use serde::{Deserialize, Serialize};
use tracing::Span;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "read_action")]
pub enum ReadAction {
    Count,
    Get,
    GetForDelete,
    Query,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "write_action")]
pub enum WriteAction {
    Add,
    Delete,
    Update,
    Upsert,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "event_name")]
pub enum MeterEvent {
    CollectionFork {
        tenant: String,
        database: String,
        collection_id: Uuid,
        latest_collection_logical_size_bytes: u64,
    },
    CollectionRead {
        tenant: String,
        database: String,
        collection_id: Uuid,
        #[serde(flatten)]
        action: ReadAction,
        fts_query_length: u64,
        metadata_predicate_count: u64,
        query_embedding_count: u64,
        pulled_log_size_bytes: u64,
        latest_collection_logical_size_bytes: u64,
        return_bytes: u64,
    },
    CollectionWrite {
        tenant: String,
        database: String,
        collection_id: Uuid,
        #[serde(flatten)]
        action: WriteAction,
        log_size_bytes: u64,
    },
}

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
    use uuid::Uuid;

    use crate::meter_event::WriteAction;

    use super::MeterEvent;

    #[test]
    fn test_event_serialization() {
        let event = MeterEvent::CollectionWrite {
            tenant: "test_tenant".to_string(),
            database: "test_database".to_string(),
            collection_id: Uuid::new_v4(),
            action: WriteAction::Add,
            log_size_bytes: 1000,
        };
        let json_str = serde_json::to_string(&event).expect("The event should be serializable");
        let json_event =
            serde_json::from_str::<MeterEvent>(&json_str).expect("Json should be deserializable");
        assert_eq!(json_event, event);
    }
}
