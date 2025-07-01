use chroma_metering_macros::initialize_metering;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;

use crate::types::MeteringAtomicU64;

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
    /// The latest logical size of a collection in bytes
    #[capability]
    pub trait LatestCollectionLogicalSizeBytes {
        fn latest_collection_logical_size_bytes(&self, latest_collection_logical_size_bytes: u64);
    }

    /// The size of data written to the log in bytes
    #[capability]
    pub trait LogSizeBytes {
        fn log_size_bytes(&self, log_size_bytes: u64);
    }

    /// The number of trigram tokens in a full-text search query
    #[capability]
    pub trait FtsQueryLength {
        fn fts_query_length(&self, fts_query_length: u64);
    }

    /// The number of metadata predicates in a `WHERE` query
    #[capability]
    pub trait MetadataPredicateCount {
        fn metadata_predicate_count(&self, metadata_predicate_count: u64);
    }

    /// The length of the embedded query vector
    #[capability]
    pub trait QueryEmbeddingCount {
        fn query_embedding_count(&self, query_embedding_count: u64);
    }

    /// The size in bytes of data pulled from the log during a get
    #[capability]
    pub trait PulledLogSizeBytes {
        fn pulled_log_size_bytes(&self, pulled_log_size_bytes: u64);
    }

    /// The size in bytes of data returned to the client
    #[capability]
    pub trait ReturnBytes {
        fn return_bytes(&self, return_bytes: u64);
    }

    ////////////////////////////////// collection_fork //////////////////////////////////
    #[context(
        capabilities = [
            LatestCollectionLogicalSizeBytes
        ],
        handlers = [
            __handler_collection_fork_latest_collection_logical_size_bytes
        ]
    )]
    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    #[serde(rename_all = "snake_case")]
    pub struct CollectionForkContext {
        pub tenant: String,
        pub database: String,
        pub collection_id: String,
        pub latest_collection_logical_size_bytes: MeteringAtomicU64,
    }

    impl CollectionForkContext {
        pub fn new(tenant: String, database: String, collection_id: String) -> Self {
            CollectionForkContext {
                tenant,
                database,
                collection_id,
                latest_collection_logical_size_bytes: MeteringAtomicU64::new(0)
            }
        }
    }

    /// Handler for [`crate::core::LatestCollectionLogicalSizeBytes`] capability for collection fork contexts
    fn __handler_collection_fork_latest_collection_logical_size_bytes(
        context: &CollectionForkContext,
        latest_collection_logical_size_bytes: u64,
    ) {
        context
            .latest_collection_logical_size_bytes
            .store(latest_collection_logical_size_bytes, Ordering::SeqCst);
    }

    ////////////////////////////////// collection_read //////////////////////////////////
    #[context(
        capabilities = [
            FtsQueryLength,
            MetadataPredicateCount,
            QueryEmbeddingCount,
            PulledLogSizeBytes,
            LatestCollectionLogicalSizeBytes,
            ReturnBytes,
            ],
        handlers = [
            __handler_collection_read_fts_query_length,
            __handler_collection_read_metadata_predicate_count,
            __handler_collection_read_query_embedding_count,
            __handler_collection_read_pulled_log_size_bytes,
            __handler_collection_read_latest_collection_logical_size_bytes,
            __handler_collection_read_return_bytes,
        ]
    )]
    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    #[serde(rename_all = "snake_case")]
    pub struct CollectionReadContext {
        pub tenant: String,
        pub database: String,
        pub collection_id: String,
        #[serde(flatten)]
        pub action: ReadAction,
        pub fts_query_length: MeteringAtomicU64,
        pub metadata_predicate_count: MeteringAtomicU64,
        pub query_embedding_count: MeteringAtomicU64,
        pub pulled_log_size_bytes: MeteringAtomicU64,
        pub latest_collection_logical_size_bytes: MeteringAtomicU64,
        pub return_bytes: MeteringAtomicU64,
    }

    impl CollectionReadContext {
        pub fn new(tenant: String, database: String, collection_id: String, action: ReadAction) -> Self {
            CollectionReadContext {
                tenant,
                database,
                collection_id,
                action,
                fts_query_length: MeteringAtomicU64::new(0),
                metadata_predicate_count: MeteringAtomicU64::new(0),
                query_embedding_count: MeteringAtomicU64::new(0),
                pulled_log_size_bytes: MeteringAtomicU64::new(0),
                latest_collection_logical_size_bytes: MeteringAtomicU64::new(0),
                return_bytes: MeteringAtomicU64::new(0),
            }
        }
    }

    /// Handler for [`crate::core::FtsQueryLength`] capability for collection read contexts
    fn __handler_collection_read_fts_query_length(
        context: &CollectionReadContext,
        fts_query_length: u64,
    ) {
        context
            .fts_query_length
            .store(fts_query_length, Ordering::SeqCst);
    }

    /// Handler for [`crate::core::MetadataPredicateCount`] capability for collection read contexts
    fn __handler_collection_read_metadata_predicate_count(
        context: &CollectionReadContext,
        metadata_predicate_count: u64,
    ) {
        context
            .metadata_predicate_count
            .store(metadata_predicate_count, Ordering::SeqCst);
    }

    /// Handler for [`crate::core::QueryEmbeddingCount`] capability for collection read contexts
    fn __handler_collection_read_query_embedding_count(
        context: &CollectionReadContext,
        query_embedding_count: u64,
    ) {
        context
            .query_embedding_count
            .store(query_embedding_count, Ordering::SeqCst);
    }

    /// Handler for [`crate::core::PulledLogSizeBytes`] capability for collection read contexts
    fn __handler_collection_read_pulled_log_size_bytes(
        context: &CollectionReadContext,
        pulled_log_size_bytes: u64,
    ) {
        context
            .pulled_log_size_bytes
            .store(pulled_log_size_bytes, Ordering::SeqCst);
    }

    /// Handler for [`crate::core::LatestCollectionLogicalSizeBytes`] capability for collection read contexts
    fn __handler_collection_read_latest_collection_logical_size_bytes(
        context: &CollectionReadContext,
        latest_collection_logical_size_bytes: u64,
    ) {
        context
            .latest_collection_logical_size_bytes
            .store(latest_collection_logical_size_bytes, Ordering::SeqCst);
    }

    /// Handler for [`crate::core::ReturnBytes`] capability for collection read contexts
    fn __handler_collection_read_return_bytes(
        context: &CollectionReadContext,
        return_bytes: u64,
    ) {
        context
            .return_bytes
            .store(return_bytes, Ordering::SeqCst);
    }


    ////////////////////////////////// collection_write //////////////////////////////////
    #[context(
        capabilities = [
            LogSizeBytes
            ],
        handlers = [
            __handler_collection_write_log_size_bytes
        ]
    )]
    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    #[serde(rename_all = "snake_case")]
    pub struct CollectionWriteContext {
        pub tenant: String,
        pub database: String,
        pub collection_id: String,
        #[serde(flatten)]
        pub action: WriteAction,
        pub log_size_bytes: MeteringAtomicU64,
    }

    impl CollectionWriteContext {
        pub fn new(tenant: String, database: String, collection_id: String, action: WriteAction) -> Self {
            CollectionWriteContext {
                tenant,
                database,
                collection_id,
                action,
                log_size_bytes: MeteringAtomicU64::new(0),
            }
        }
    }

    /// Handler for [`crate::core::LogSizeBytes`] capability for collection write contexts
    fn __handler_collection_write_log_size_bytes(
        context: &CollectionWriteContext,
        log_size_bytes: u64,
    ) {
        context
            .log_size_bytes
            .store(log_size_bytes, Ordering::SeqCst);
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
    use super::{CollectionWriteContext, MeterEvent, WriteAction};
    use crate::types::MeteringAtomicU64;

    #[test]
    fn test_event_serialization() {
        let event = MeterEvent::CollectionWrite(CollectionWriteContext {
            tenant: "test_tenant".to_string(),
            database: "test_database".to_string(),
            collection_id: "test_collection".to_string(),
            action: WriteAction::Add,
            log_size_bytes: MeteringAtomicU64::new(1000),
        });
        let json_str = serde_json::to_string(&event).expect("The event should be serializable");
        let json_event =
            serde_json::from_str::<MeterEvent>(&json_str).expect("Json should be deserializable");
        assert_eq!(json_event, event);
    }
}
