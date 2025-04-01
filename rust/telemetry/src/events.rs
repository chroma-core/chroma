use crate::client::EVENT_SENDER;
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use std::any::Any;
use std::collections::HashMap;
use std::default::Default;
use std::fmt::Debug;
use tracing::warn;

pub async fn submit_event<T: ProductTelemetryEvent + Send + Sync>(event: T) {
    // Disable telemetry capture when running tests
    let in_pytest = std::env::var("CHROMA_IN_PYTEST").is_ok_and(|val| val == "1");
    if cfg!(test) || in_pytest {
        return;
    }

    if let Some(handler) = EVENT_SENDER.get() {
        if let Err(e) = handler.send(Box::new(event), None).await {
            warn!("Failed to submit telemetry event: {}", e);
        }
    }
}

#[async_trait]
pub trait EventSubmit {
    async fn submit(self)
    where
        Self: Sized + Send + Sync + 'static;
}

#[async_trait]
impl<T: ProductTelemetryEvent + Send + Sync + 'static> EventSubmit for T {
    async fn submit(self)
    where
        Self: Sized + Send + Sync + 'static,
    {
        submit_event(self).await;
    }
}

pub trait ProductTelemetryEvent: Debug + Any + Send + Sync {
    fn name(&self) -> String;
    fn properties(&self) -> HashMap<String, Value>;
    fn max_batch_size(&self) -> usize {
        1
    }
    fn batch_size(&self) -> usize {
        1
    }
    fn batch_key(&self) -> String;
    fn batch(
        &mut self,
        other: Box<dyn ProductTelemetryEvent + Send + Sync>,
    ) -> Result<(), &'static str>;

    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

#[derive(Serialize, Debug, Clone)]
pub struct ServerStartEvent {
    pub is_cli: bool,
}

impl ServerStartEvent {
    pub fn new() -> Self {
        // Check environment variable, default to false
        let is_cli = std::env::var("CHROMA_CLI").is_ok_and(|val| val.eq_ignore_ascii_case("true"));
        Self { is_cli }
    }
}

impl Default for ServerStartEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl ProductTelemetryEvent for ServerStartEvent {
    fn name(&self) -> String {
        "ServerStartEvent".to_string()
    }
    fn properties(&self) -> HashMap<String, Value> {
        match serde_json::to_value(self) {
            Ok(Value::Object(map)) => map.into_iter().collect(),
            _ => HashMap::new(),
        }
    }
    fn batch_key(&self) -> String {
        self.name()
    }
    fn batch(
        &mut self,
        _other: Box<dyn ProductTelemetryEvent + Send + Sync>,
    ) -> Result<(), &'static str> {
        Err("Batching not supported for ServerStartEvent")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ClientCreateCollectionEvent
#[derive(Serialize, Debug, Clone)]
pub struct ClientCreateCollectionEvent {
    pub collection_uuid: String,
    pub embedding_function: String, // TODO: Re-enable if needed
}

impl ClientCreateCollectionEvent {
    pub fn new(collection_uuid: String, embedding_function: String) -> Self {
        Self {
            collection_uuid,
            embedding_function,
        }
    }
}

impl ProductTelemetryEvent for ClientCreateCollectionEvent {
    fn name(&self) -> String {
        "ClientCreateCollectionEvent".to_string()
    }
    fn properties(&self) -> HashMap<String, Value> {
        match serde_json::to_value(self) {
            Ok(Value::Object(map)) => map.into_iter().collect(),
            _ => HashMap::new(),
        }
    }
    fn batch_key(&self) -> String {
        self.name()
    }
    fn batch(
        &mut self,
        _other: Box<dyn ProductTelemetryEvent + Send + Sync>,
    ) -> Result<(), &'static str> {
        Err("Batching not supported for ClientCreateCollectionEvent")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct CollectionAddEvent {
    pub collection_uuid: String,
    pub add_amount: usize,
    pub with_documents: usize,
    pub with_metadata: usize,
    pub with_uris: usize,
    pub batch_size: usize,
}

impl CollectionAddEvent {
    const MAX_BATCH_SIZE: usize = 3000;

    pub fn new(
        collection_uuid: String,
        add_amount: usize,
        with_documents: usize,
        with_metadata: usize,
        with_uris: usize,
        batch_size: usize,
    ) -> Self {
        Self {
            collection_uuid,
            add_amount,
            with_documents,
            with_metadata,
            with_uris,
            batch_size,
        }
    }
}

impl ProductTelemetryEvent for CollectionAddEvent {
    fn name(&self) -> String {
        "CollectionAddEvent".to_string()
    }
    fn properties(&self) -> HashMap<String, Value> {
        match serde_json::to_value(self) {
            Ok(Value::Object(map)) => map.into_iter().collect(),
            _ => HashMap::new(),
        }
    }
    fn max_batch_size(&self) -> usize {
        CollectionAddEvent::MAX_BATCH_SIZE
    }
    fn batch_size(&self) -> usize {
        self.batch_size
    }
    fn batch_key(&self) -> String {
        format!("{}{}", self.collection_uuid, self.name())
    }

    fn batch(
        &mut self,
        other: Box<dyn ProductTelemetryEvent + Send + Sync>,
    ) -> Result<(), &'static str> {
        if let Some(other_event) = other.as_any().downcast_ref::<CollectionAddEvent>() {
            if self.batch_key() != other_event.batch_key() {
                return Err("Cannot batch events with different keys");
            }
            self.add_amount += other_event.add_amount;
            self.with_documents += other_event.with_documents;
            self.with_metadata += other_event.with_metadata;
            self.with_uris += other_event.with_uris;
            self.batch_size += other_event.batch_size;
            Ok(())
        } else {
            Err("Event type mismatch")
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct CollectionUpdateEvent {
    pub collection_uuid: String,
    pub update_amount: usize,
    pub with_embeddings: usize,
    pub with_metadata: usize,
    pub with_documents: usize,
    pub with_uris: usize,
    batch_count: usize,
}

impl CollectionUpdateEvent {
    const MAX_BATCH_SIZE: usize = 300;

    pub fn new(
        collection_uuid: String,
        update_amount: usize,
        with_embeddings: usize,
        with_metadata: usize,
        with_documents: usize,
        with_uris: usize,
    ) -> Self {
        Self {
            collection_uuid,
            update_amount,
            with_embeddings,
            with_metadata,
            with_documents,
            with_uris,
            batch_count: 1,
        }
    }
}

impl ProductTelemetryEvent for CollectionUpdateEvent {
    fn name(&self) -> String {
        "CollectionUpdateEvent".to_string()
    }
    fn properties(&self) -> HashMap<String, Value> {
        match serde_json::to_value(self) {
            Ok(Value::Object(map)) => map.into_iter().collect(),
            _ => HashMap::new(),
        }
    }
    fn max_batch_size(&self) -> usize {
        CollectionUpdateEvent::MAX_BATCH_SIZE
    }
    fn batch_size(&self) -> usize {
        self.batch_count
    }
    fn batch_key(&self) -> String {
        format!("{}{}", self.collection_uuid, self.name())
    }

    fn batch(
        &mut self,
        other: Box<dyn ProductTelemetryEvent + Send + Sync>,
    ) -> Result<(), &'static str> {
        if let Some(other_event) = other.as_any().downcast_ref::<CollectionUpdateEvent>() {
            if self.batch_key() != other_event.batch_key() {
                return Err("Cannot batch events with different keys");
            }
            self.update_amount += other_event.update_amount;
            self.with_embeddings += other_event.with_embeddings;
            self.with_metadata += other_event.with_metadata;
            self.with_documents += other_event.with_documents;
            self.with_uris += other_event.with_uris;
            self.batch_count += other_event.batch_count;
            Ok(())
        } else {
            Err("Cannot batch events of different types")
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct CollectionQueryEvent {
    pub collection_uuid: String,
    pub query_amount: usize,
    pub with_metadata_filter: usize,
    pub with_document_filter: usize,
    pub n_results: usize,
    pub include_metadatas: usize,
    pub include_documents: usize,
    pub include_uris: usize,
    pub include_distances: usize,
    batch_count: usize,
}

impl CollectionQueryEvent {
    const MAX_BATCH_SIZE: usize = 3000;

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        collection_uuid: String,
        query_amount: usize,
        with_metadata_filter: usize,
        with_document_filter: usize,
        n_results: usize,
        include_metadatas: usize,
        include_documents: usize,
        include_uris: usize,
        include_distances: usize,
    ) -> Self {
        Self {
            collection_uuid,
            query_amount,
            with_metadata_filter,
            with_document_filter,
            n_results,
            include_metadatas,
            include_documents,
            include_uris,
            include_distances,
            batch_count: 1,
        }
    }
}

impl ProductTelemetryEvent for CollectionQueryEvent {
    fn name(&self) -> String {
        "CollectionQueryEvent".to_string()
    }
    fn properties(&self) -> HashMap<String, Value> {
        match serde_json::to_value(self) {
            Ok(Value::Object(map)) => map.into_iter().collect(),
            _ => HashMap::new(),
        }
    }
    fn max_batch_size(&self) -> usize {
        CollectionQueryEvent::MAX_BATCH_SIZE
    }
    fn batch_size(&self) -> usize {
        self.batch_count
    }
    fn batch_key(&self) -> String {
        format!("{}{}", self.collection_uuid, self.name())
    }

    fn batch(
        &mut self,
        other: Box<dyn ProductTelemetryEvent + Send + Sync>,
    ) -> Result<(), &'static str> {
        if let Some(other_event) = other.as_any().downcast_ref::<CollectionQueryEvent>() {
            if self.batch_key() != other_event.batch_key() {
                return Err("Cannot batch events with different keys");
            }
            self.query_amount += other_event.query_amount;
            self.with_metadata_filter += other_event.with_metadata_filter;
            self.with_document_filter += other_event.with_document_filter;
            self.n_results += other_event.n_results;
            self.include_metadatas += other_event.include_metadatas;
            self.include_documents += other_event.include_documents;
            self.include_uris += other_event.include_uris;
            self.include_distances += other_event.include_distances;
            self.batch_count += other_event.batch_count;
            Ok(())
        } else {
            Err("Cannot batch events of different types")
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct CollectionGetEvent {
    pub collection_uuid: String,
    pub ids_count: usize,
    pub include_metadata: usize,
    pub include_documents: usize,
    pub include_uris: usize,
    batch_count: usize,
}

impl CollectionGetEvent {
    const MAX_BATCH_SIZE: usize = 300;

    pub fn new(
        collection_uuid: String,
        ids_count: usize,
        include_metadata: usize,
        include_documents: usize,
        include_uris: usize,
    ) -> Self {
        Self {
            collection_uuid,
            ids_count,
            include_metadata,
            include_documents,
            include_uris,
            batch_count: 1,
        }
    }
}

impl ProductTelemetryEvent for CollectionGetEvent {
    fn name(&self) -> String {
        "CollectionGetEvent".to_string()
    }
    fn properties(&self) -> HashMap<String, Value> {
        match serde_json::to_value(self) {
            Ok(Value::Object(map)) => map.into_iter().collect(),
            _ => HashMap::new(),
        }
    }
    fn max_batch_size(&self) -> usize {
        CollectionGetEvent::MAX_BATCH_SIZE
    }
    fn batch_size(&self) -> usize {
        self.batch_count
    }
    fn batch_key(&self) -> String {
        format!("{}{}", self.collection_uuid, self.name())
    }

    fn batch(
        &mut self,
        other: Box<dyn ProductTelemetryEvent + Send + Sync>,
    ) -> Result<(), &'static str> {
        if let Some(other_event) = other.as_any().downcast_ref::<CollectionGetEvent>() {
            if self.batch_key() != other_event.batch_key() {
                return Err("Cannot batch events with different keys (check limit)");
            }
            self.ids_count += other_event.ids_count;
            self.include_metadata += other_event.include_metadata;
            self.include_documents += other_event.include_documents;
            self.include_uris += other_event.include_uris;
            self.batch_count += other_event.batch_count;
            Ok(())
        } else {
            Err("Cannot batch events of different types")
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct CollectionDeleteEvent {
    pub collection_uuid: String,
    pub delete_amount: usize,
}

impl CollectionDeleteEvent {
    pub fn new(collection_uuid: String, delete_amount: usize) -> Self {
        Self {
            collection_uuid,
            delete_amount,
        }
    }
}

impl ProductTelemetryEvent for CollectionDeleteEvent {
    fn name(&self) -> String {
        "CollectionDeleteEvent".to_string()
    }
    fn properties(&self) -> HashMap<String, Value> {
        match serde_json::to_value(self) {
            Ok(Value::Object(map)) => map.into_iter().collect(),
            _ => HashMap::new(),
        }
    }
    // No batching needed - use defaults
    fn batch_key(&self) -> String {
        self.name()
    } // Default batch key
    fn batch(
        &mut self,
        _other: Box<dyn ProductTelemetryEvent + Send + Sync>,
    ) -> Result<(), &'static str> {
        Err("Batching not supported for CollectionDeleteEvent")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_server_start_event() {
        let mut event = ServerStartEvent::new();
        assert_eq!(event.name(), "ServerStartEvent");
        assert_eq!(event.batch_key(), "ServerStartEvent");
        assert_eq!(event.max_batch_size(), 1);
        assert_eq!(event.batch_size(), 1);

        let properties = event.properties();
        assert_eq!(properties.get("is_cli").unwrap(), &json!(false));

        // Test batching not supported
        let other_event = Box::new(ServerStartEvent::new());
        assert!(event.batch(other_event).is_err());
    }

    #[test]
    fn test_client_create_collection_event() {
        let uuid = "test-uuid".to_string();
        let embedding_function = "default".to_string();
        let mut event = ClientCreateCollectionEvent::new(uuid.clone(), embedding_function.clone());
        assert_eq!(event.name(), "ClientCreateCollectionEvent");
        assert_eq!(event.batch_key(), "ClientCreateCollectionEvent");
        assert_eq!(event.max_batch_size(), 1);
        assert_eq!(event.batch_size(), 1);

        let properties = event.properties();
        assert_eq!(properties.get("collection_uuid").unwrap(), &json!(uuid));

        // Test batching not supported
        let other_event = Box::new(ClientCreateCollectionEvent::new(
            "other-uuid".to_string(),
            "default".to_string(),
        ));
        assert!(event.batch(other_event).is_err());
    }

    #[test]
    fn test_collection_add_event() {
        let uuid = "test-uuid".to_string();
        let mut event = CollectionAddEvent::new(uuid.clone(), 10, 5, 3, 2, 1);
        assert_eq!(event.name(), "CollectionAddEvent");
        assert_eq!(
            event.batch_key(),
            format!("{}{}", uuid, "CollectionAddEvent")
        );
        assert_eq!(event.max_batch_size(), CollectionAddEvent::MAX_BATCH_SIZE);
        assert_eq!(event.batch_size(), 1);

        let properties = event.properties();
        assert_eq!(properties.get("collection_uuid").unwrap(), &json!(uuid));
        assert_eq!(properties.get("add_amount").unwrap(), &json!(10));
        assert_eq!(properties.get("with_documents").unwrap(), &json!(5));
        assert_eq!(properties.get("with_metadata").unwrap(), &json!(3));
        assert_eq!(properties.get("with_uris").unwrap(), &json!(2));

        // Test successful batching
        let other_event = Box::new(CollectionAddEvent::new(uuid.clone(), 20, 10, 6, 4, 2));
        event.batch(other_event).unwrap();
        assert_eq!(event.add_amount, 30);
        assert_eq!(event.with_documents, 15);
        assert_eq!(event.with_metadata, 9);
        assert_eq!(event.with_uris, 6);
        assert_eq!(event.batch_size, 3);

        // Test batching with different collection UUID
        let other_event = Box::new(CollectionAddEvent::new(
            "other-uuid".to_string(),
            20,
            10,
            6,
            4,
            2,
        ));
        assert!(event.batch(other_event).is_err());
    }

    #[test]
    fn test_collection_update_event() {
        let uuid = "test-uuid".to_string();
        let mut event = CollectionUpdateEvent::new(uuid.clone(), 10, 5, 3, 2, 1);
        assert_eq!(event.name(), "CollectionUpdateEvent");
        assert_eq!(
            event.batch_key(),
            format!("{}{}", uuid, "CollectionUpdateEvent")
        );
        assert_eq!(
            event.max_batch_size(),
            CollectionUpdateEvent::MAX_BATCH_SIZE
        );
        assert_eq!(event.batch_size(), 1);

        let properties = event.properties();
        assert_eq!(properties.get("collection_uuid").unwrap(), &json!(uuid));
        assert_eq!(properties.get("update_amount").unwrap(), &json!(10));
        assert_eq!(properties.get("with_embeddings").unwrap(), &json!(5));
        assert_eq!(properties.get("with_metadata").unwrap(), &json!(3));
        assert_eq!(properties.get("with_documents").unwrap(), &json!(2));
        assert_eq!(properties.get("with_uris").unwrap(), &json!(1));

        // Test successful batching
        let other_event = Box::new(CollectionUpdateEvent::new(uuid.clone(), 20, 10, 6, 4, 2));
        event.batch(other_event).unwrap();
        assert_eq!(event.update_amount, 30);
        assert_eq!(event.with_embeddings, 15);
        assert_eq!(event.with_metadata, 9);
        assert_eq!(event.with_documents, 6);
        assert_eq!(event.with_uris, 3);
        assert_eq!(event.batch_count, 2);

        // Test batching with different collection UUID
        let other_event = Box::new(CollectionUpdateEvent::new(
            "other-uuid".to_string(),
            20,
            10,
            6,
            4,
            2,
        ));
        assert!(event.batch(other_event).is_err());
    }

    #[test]
    fn test_collection_query_event() {
        let uuid = "test-uuid".to_string();
        let mut event = CollectionQueryEvent::new(uuid.clone(), 10, 5, 3, 2, 1, 1, 1, 1);
        assert_eq!(event.name(), "CollectionQueryEvent");
        assert_eq!(
            event.batch_key(),
            format!("{}{}", uuid, "CollectionQueryEvent")
        );
        assert_eq!(event.max_batch_size(), CollectionQueryEvent::MAX_BATCH_SIZE);
        assert_eq!(event.batch_size(), 1);

        let properties = event.properties();
        assert_eq!(properties.get("collection_uuid").unwrap(), &json!(uuid));
        assert_eq!(properties.get("query_amount").unwrap(), &json!(10));
        assert_eq!(properties.get("with_metadata_filter").unwrap(), &json!(5));
        assert_eq!(properties.get("with_document_filter").unwrap(), &json!(3));
        assert_eq!(properties.get("n_results").unwrap(), &json!(2));
        assert_eq!(properties.get("include_metadatas").unwrap(), &json!(1));
        assert_eq!(properties.get("include_documents").unwrap(), &json!(1));
        assert_eq!(properties.get("include_uris").unwrap(), &json!(1));
        assert_eq!(properties.get("include_distances").unwrap(), &json!(1));

        // Test successful batching
        let other_event = Box::new(CollectionQueryEvent::new(
            uuid.clone(),
            20,
            10,
            6,
            4,
            2,
            2,
            2,
            2,
        ));
        event.batch(other_event).unwrap();
        assert_eq!(event.query_amount, 30);
        assert_eq!(event.with_metadata_filter, 15);
        assert_eq!(event.with_document_filter, 9);
        assert_eq!(event.n_results, 6);
        assert_eq!(event.include_metadatas, 3);
        assert_eq!(event.include_documents, 3);
        assert_eq!(event.include_uris, 3);
        assert_eq!(event.include_distances, 3);
        assert_eq!(event.batch_count, 2);

        // Test batching with different collection UUID
        let other_event = Box::new(CollectionQueryEvent::new(
            "other-uuid".to_string(),
            20,
            10,
            6,
            4,
            2,
            2,
            2,
            2,
        ));
        assert!(event.batch(other_event).is_err());
    }

    #[test]
    fn test_collection_get_event() {
        let uuid = "test-uuid".to_string();
        let mut event = CollectionGetEvent::new(uuid.clone(), 10, 3, 2, 1);
        assert_eq!(event.name(), "CollectionGetEvent");
        assert_eq!(
            event.batch_key(),
            format!("{}{}", uuid, "CollectionGetEvent")
        );
        assert_eq!(event.max_batch_size(), CollectionGetEvent::MAX_BATCH_SIZE);
        assert_eq!(event.batch_size(), 1);

        let properties = event.properties();
        assert_eq!(properties.get("collection_uuid").unwrap(), &json!(uuid));
        assert_eq!(properties.get("ids_count").unwrap(), &json!(10));
        assert_eq!(properties.get("include_metadata").unwrap(), &json!(3));
        assert_eq!(properties.get("include_documents").unwrap(), &json!(2));
        assert_eq!(properties.get("include_uris").unwrap(), &json!(1));

        // Test successful batching
        let other_event = Box::new(CollectionGetEvent::new(uuid.clone(), 20, 6, 4, 2));
        event.batch(other_event).unwrap();
        assert_eq!(event.ids_count, 30);
        assert_eq!(event.include_metadata, 9);
        assert_eq!(event.include_documents, 6);
        assert_eq!(event.include_uris, 3);
        assert_eq!(event.batch_count, 2);

        // Test batching with different collection UUID
        let other_event = Box::new(CollectionGetEvent::new(
            "other-uuid".to_string(),
            20,
            6,
            4,
            2,
        ));
        assert!(event.batch(other_event).is_err());
    }

    #[test]
    fn test_collection_delete_event() {
        let uuid = "test-uuid".to_string();
        let mut event = CollectionDeleteEvent::new(uuid.clone(), 10);
        assert_eq!(event.name(), "CollectionDeleteEvent");
        assert_eq!(event.batch_key(), "CollectionDeleteEvent");
        assert_eq!(event.max_batch_size(), 1);
        assert_eq!(event.batch_size(), 1);

        let properties = event.properties();
        assert_eq!(properties.get("collection_uuid").unwrap(), &json!(uuid));
        assert_eq!(properties.get("delete_amount").unwrap(), &json!(10));

        // Test batching not supported
        let other_event = Box::new(CollectionDeleteEvent::new("other-uuid".to_string(), 20));
        assert!(event.batch(other_event).is_err());
    }
}
