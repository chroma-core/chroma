//! Chroma WASM - Vector database for the browser.
//!
//! This crate provides a WebAssembly-compatible Chroma collection
//! with brute-force vector search and Chroma's distance functions.
//! It exposes a JavaScript API via wasm-bindgen.
//!
//! # Usage from JavaScript
//!
//! ```js
//! import init, { ChromaCollection } from './chroma_wasm.js';
//!
//! await init();
//!
//! const collection = new ChromaCollection("my_collection", "cosine");
//! collection.add(
//!     ["id1", "id2"],
//!     [[0.1, 0.2, ...], [0.3, 0.4, ...]],
//!     ["document 1", "document 2"],
//!     [{"key": "value"}, {"key": "value2"}]
//! );
//!
//! const results = collection.query([0.1, 0.2, ...], 10);
//! // results = [{ id, distance, document, metadata }, ...]
//!
//! // Persist to IndexedDB or localStorage
//! const snapshot = collection.save();
//! // ... store snapshot string ...
//!
//! // Restore later
//! const restored = ChromaCollection.load(snapshot);
//! ```

mod collection;
mod distance;

use collection::{Collection, CollectionSnapshot, MetadataValue};
use distance::DistanceFunction;
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

/// A Chroma collection that runs entirely in WebAssembly.
#[wasm_bindgen]
pub struct ChromaCollection {
    inner: Collection,
}

#[wasm_bindgen]
impl ChromaCollection {
    /// Create a new empty collection.
    ///
    /// `distance_fn` must be one of: "cosine", "l2", "ip"
    #[wasm_bindgen(constructor)]
    pub fn new(name: &str, distance_fn: &str) -> Result<ChromaCollection, JsValue> {
        let df = match distance_fn {
            "cosine" => DistanceFunction::Cosine,
            "l2" => DistanceFunction::Euclidean,
            "ip" => DistanceFunction::InnerProduct,
            _ => {
                return Err(JsValue::from_str(
                    "Invalid distance function. Use 'cosine', 'l2', or 'ip'.",
                ))
            }
        };

        Ok(ChromaCollection {
            inner: Collection::new(name.to_string(), df),
        })
    }

    /// Add entries to the collection.
    ///
    /// - `ids`: JSON array of string IDs
    /// - `embeddings`: JSON array of float arrays
    /// - `documents`: optional JSON array of strings (or nulls)
    /// - `metadatas`: optional JSON array of objects
    pub fn add(
        &mut self,
        ids: JsValue,
        embeddings: JsValue,
        documents: JsValue,
        metadatas: JsValue,
    ) -> Result<(), JsValue> {
        let ids: Vec<String> = serde_wasm_bindgen::from_value(ids)
            .map_err(|e| JsValue::from_str(&format!("Invalid ids: {e}")))?;

        let embeddings: Vec<Vec<f32>> = serde_wasm_bindgen::from_value(embeddings)
            .map_err(|e| JsValue::from_str(&format!("Invalid embeddings: {e}")))?;

        if ids.len() != embeddings.len() {
            return Err(JsValue::from_str("ids and embeddings must have the same length"));
        }

        let documents: Option<Vec<Option<String>>> = if documents.is_null() || documents.is_undefined() {
            None
        } else {
            Some(
                serde_wasm_bindgen::from_value(documents)
                    .map_err(|e| JsValue::from_str(&format!("Invalid documents: {e}")))?,
            )
        };

        let metadatas: Option<Vec<HashMap<String, MetadataValue>>> =
            if metadatas.is_null() || metadatas.is_undefined() {
                None
            } else {
                Some(
                    serde_wasm_bindgen::from_value(metadatas)
                        .map_err(|e| JsValue::from_str(&format!("Invalid metadatas: {e}")))?,
                )
            };

        self.inner.add(ids, embeddings, documents, metadatas);
        Ok(())
    }

    /// Query the collection for nearest neighbors.
    ///
    /// Returns a JSON array of `{ id, distance, document, metadata }` objects.
    pub fn query(
        &self,
        query_embedding: JsValue,
        n_results: usize,
        where_filter: JsValue,
    ) -> Result<JsValue, JsValue> {
        let query: Vec<f32> = serde_wasm_bindgen::from_value(query_embedding)
            .map_err(|e| JsValue::from_str(&format!("Invalid query embedding: {e}")))?;

        let filter: Option<HashMap<String, MetadataValue>> =
            if where_filter.is_null() || where_filter.is_undefined() {
                None
            } else {
                Some(
                    serde_wasm_bindgen::from_value(where_filter)
                        .map_err(|e| JsValue::from_str(&format!("Invalid where filter: {e}")))?,
                )
            };

        let results = self.inner.query(&query, n_results, filter.as_ref());
        serde_wasm_bindgen::to_value(&results)
            .map_err(|e| JsValue::from_str(&format!("Failed to serialize results: {e}")))
    }

    /// Get entries by IDs.
    pub fn get(&self, ids: JsValue) -> Result<JsValue, JsValue> {
        let ids: Vec<String> = serde_wasm_bindgen::from_value(ids)
            .map_err(|e| JsValue::from_str(&format!("Invalid ids: {e}")))?;

        let entries = self.inner.get(&ids);
        serde_wasm_bindgen::to_value(&entries)
            .map_err(|e| JsValue::from_str(&format!("Failed to serialize entries: {e}")))
    }

    /// Delete entries by IDs.
    pub fn delete(&mut self, ids: JsValue) -> Result<(), JsValue> {
        let ids: Vec<String> = serde_wasm_bindgen::from_value(ids)
            .map_err(|e| JsValue::from_str(&format!("Invalid ids: {e}")))?;

        self.inner.delete(&ids);
        Ok(())
    }

    /// Check if an entry exists.
    pub fn contains(&self, id: &str) -> bool {
        self.inner.contains(id)
    }

    /// Return the number of entries in the collection.
    pub fn count(&self) -> usize {
        self.inner.count()
    }

    /// Serialize the collection to a JSON string for persistence.
    ///
    /// Store this string in IndexedDB, localStorage, etc. and use
    /// `ChromaCollection.load()` to restore it later.
    pub fn save(&self) -> Result<String, JsValue> {
        let snapshot = self.inner.snapshot();
        serde_json::to_string(&snapshot)
            .map_err(|e| JsValue::from_str(&format!("Failed to serialize: {e}")))
    }

    /// Restore a collection from a JSON snapshot string.
    pub fn load(json: &str) -> Result<ChromaCollection, JsValue> {
        let snapshot: CollectionSnapshot = serde_json::from_str(json)
            .map_err(|e| JsValue::from_str(&format!("Failed to deserialize: {e}")))?;

        Ok(ChromaCollection {
            inner: Collection::from_snapshot(snapshot),
        })
    }
}

