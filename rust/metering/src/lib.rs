use chroma_metering_macros::initialize_metering;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
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

initialize_metering! {
    #[capability]
    pub trait RequestReceivedAt {
        fn request_received_at(&self, received_at: DateTime<Utc>);
    }

    ///////////////// collection_fork /////////////////
    #[context(capabilities = [RequestReceivedAt], handlers = [request_received_at_handler_collection_fork])]
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct CollectionForkContext {
        tenant: String,
        database: String,
        collection_id: Uuid,
        latest_collection_logical_size_bytes: u64,
        // NOTE(c-gamble): We use chrono here because `std::time::Instant` is not serializable via serde.
        request_received_at: Mutex<DateTime<Utc>>,
    }

    pub fn request_received_at_handler_collection_fork(
        context: &CollectionForkContext,
        received_at: DateTime<Utc>,
    ) {
        if let Ok(mut guard) = context.request_received_at.lock() {
            *guard = received_at;
        }
    }

    ///////////////// collection_read /////////////////
    #[context(capabilities = [RequestReceivedAt], handlers = [request_received_at_handler_collection_read])]
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct CollectionReadContext {
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
        request_received_at: Mutex<DateTime<Utc>>,
    }

    pub fn request_received_at_handler_collection_read(
        context: &CollectionReadContext,
        received_at: DateTime<Utc>,
    ) {
        if let Ok(mut guard) = context.request_received_at.lock() {
            *guard = received_at;
        }
    }

    ///////////////// collection_write /////////////////
    #[context(capabilities = [RequestReceivedAt], handlers = [request_received_at_handler_collection_write])]
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct CollectionWriteContext {
        tenant: String,
        database: String,
        collection_id: Uuid,
        #[serde(flatten)]
        action: WriteAction,
        fts_query_length: u64,
        metadata_predicate_count: u64,
        query_embedding_count: u64,
        pulled_log_size_bytes: u64,
        latest_collection_logical_size_bytes: u64,
        return_bytes: u64,
        request_received_at: Mutex<DateTime<Utc>>,
    }

    pub fn request_received_at_handler_collection_write(
        context: &CollectionWriteContext,
        received_at: DateTime<Utc>,
    ) {
        if let Ok(mut guard) = context.request_received_at.lock() {
            *guard = received_at;
        }
    }
}
