use chroma_metering_macros::initialize_metering;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use uuid::Uuid;

use crate::types::{MeteringAtomicU64, MeteringMutex};

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

initialize_metering! {
    #[capability]
    pub trait RequestReceivedAt {
        fn request_received_at(&self, received_at: DateTime<Utc>);
    }

    #[capability]
    pub trait RequestHandlingDuration {
        fn request_handling_duration(&self, completed_at: DateTime<Utc>);
    }

    ////////////////////////////////// collection_fork //////////////////////////////////
    #[context(capabilities = [], handlers = [])]
    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    #[serde(rename_all = "snake_case")]
    pub struct CollectionForkContext {
        pub tenant: String,
        pub database: String,
        pub collection_id: Uuid,
        pub latest_collection_logical_size_bytes: u64,
    }

    ////////////////////////////////// collection_read //////////////////////////////////
    #[context(
        capabilities = [
            RequestReceivedAt,
            RequestHandlingDuration
            ],
        handlers = [
            __handler_collection_read_request_received_at,
            __handler_collection_read_request_handling_duration
        ]
    )]
    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    #[serde(rename_all = "snake_case")]
    pub struct CollectionReadContext {
        pub tenant: String,
        pub database: String,
        pub collection_id: Uuid,
        #[serde(flatten)]
        pub action: ReadAction,
        pub fts_query_length: u64,
        pub metadata_predicate_count: u64,
        pub query_embedding_count: u64,
        pub pulled_log_size_bytes: u64,
        pub latest_collection_logical_size_bytes: u64,
        pub return_bytes: u64,
        // NOTE(c-gamble): We use chrono's `DateTime` object here because `std::time::Instant`
        // is not compatible with serde.
        pub request_received_at: MeteringMutex<DateTime<Utc>>,
        pub request_handling_duration_ms: MeteringAtomicU64,
    }

    /// Handler for [`crate::RequestReceivedAt`] capability for collection read requests
    fn __handler_collection_read_request_received_at(
        context: &CollectionReadContext,
        received_at: DateTime<Utc>,
    ) {
        if let Ok(mut guard) = context.request_received_at.lock() {
            *guard = received_at;
        }
    }

    /// Handler for [`crate::RequestHandlingDuration`] capability for collection read requests
    fn __handler_collection_read_request_handling_duration(
        context: &CollectionReadContext,
        completed_at: DateTime<Utc>,
    ) {
        let received_at = context.request_received_at.lock().unwrap();
        let duration_ms = completed_at
            .signed_duration_since(*received_at) // NOTE(c-gamble): We use signed to suppress "time went backward" errors.
            .num_milliseconds()
            .max(0) as u64;

        context
            .request_handling_duration_ms
            .store(duration_ms, Ordering::SeqCst);
    }

    ////////////////////////////////// collection_write //////////////////////////////////
    #[context(
        capabilities = [
            RequestReceivedAt,
            RequestHandlingDuration
            ],
        handlers = [
            __handler_collection_write_request_received_at,
            __handler_collection_write_request_handling_duration
        ]
    )]
    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    #[serde(rename_all = "snake_case")]
    pub struct CollectionWriteContext {
        pub tenant: String,
        pub database: String,
        pub collection_id: Uuid,
        #[serde(flatten)]
        pub action: WriteAction,
        pub log_size_bytes: u64,
        pub request_received_at: MeteringMutex<DateTime<Utc>>,
        pub request_handling_duration_ms: MeteringAtomicU64,
    }

    /// Handler for [`crate::RequestReceivedAt`] capability for collection write requests
    fn __handler_collection_write_request_received_at(
        context: &CollectionWriteContext,
        received_at: DateTime<Utc>,
    ) {
        if let Ok(mut guard) = context.request_received_at.lock() {
            *guard = received_at;
        }
    }

    /// Handler for [`crate::RequestHandlingDuration`] capability for collection write requests
    fn __handler_collection_write_request_handling_duration(
        context: &CollectionWriteContext,
        completed_at: DateTime<Utc>,
    ) {
        let received_at = context.request_received_at.lock().unwrap();
        let duration_ms = completed_at
            .signed_duration_since(*received_at) // NOTE(c-gamble): We use signed to suppress "time went backward" errors.
            .num_milliseconds()
            .max(0) as u64;

        context
            .request_handling_duration_ms
            .store(duration_ms, Ordering::SeqCst);
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "event_name")]
pub enum MeterEvent {
    CollectionFork(CollectionForkContext),
    CollectionRead(CollectionReadContext),
    CollectionWrite(CollectionWriteContext),
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use std::sync::{atomic::AtomicU64, Arc, Mutex};
    use uuid::Uuid;

    use super::{CollectionWriteContext, MeterEvent, WriteAction};
    use crate::types::{MeteringAtomicU64, MeteringMutex};

    #[test]
    fn test_event_serialization() {
        let event = MeterEvent::CollectionWrite(CollectionWriteContext {
            tenant: "test_tenant".to_string(),
            database: "test_database".to_string(),
            collection_id: Uuid::new_v4(),
            action: WriteAction::Add,
            log_size_bytes: 1000,
            request_received_at: MeteringMutex(Mutex::new(Utc::now())),
            request_handling_duration_ms: MeteringAtomicU64(Arc::new(AtomicU64::new(0))),
        });
        let json_str = serde_json::to_string(&event).expect("The event should be serializable");
        let json_event =
            serde_json::from_str::<MeterEvent>(&json_str).expect("Json should be deserializable");
        assert_eq!(json_event, event);
    }
}
