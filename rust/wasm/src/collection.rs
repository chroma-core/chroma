/// In-memory Chroma collection for WASM.
///
/// Provides add, query, get, delete, update, and count operations
/// backed by brute-force vector search with Chroma's distance functions.
use crate::distance::{normalize, DistanceFunction};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metadata value that can be stored with each document.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum MetadataValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// A single entry in the collection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub id: String,
    pub embedding: Vec<f32>,
    #[serde(default)]
    pub document: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, MetadataValue>,
}

/// A search result with distance score.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub id: String,
    pub distance: f32,
    pub document: Option<String>,
    pub metadata: HashMap<String, MetadataValue>,
}

/// Serializable snapshot of a collection for persistence.
#[derive(Serialize, Deserialize)]
pub struct CollectionSnapshot {
    pub name: String,
    pub distance_fn: String,
    pub entries: Vec<Entry>,
}

/// An in-memory vector collection with brute-force search.
pub struct Collection {
    pub name: String,
    distance_fn: DistanceFunction,
    entries: HashMap<String, Entry>,
    /// Ordered list of IDs for consistent iteration
    id_order: Vec<String>,
}

impl Collection {
    /// Create a new empty collection.
    pub fn new(name: String, distance_fn: DistanceFunction) -> Self {
        Collection {
            name,
            distance_fn,
            entries: HashMap::new(),
            id_order: Vec::new(),
        }
    }

    /// Add entries to the collection. Overwrites existing entries with the same ID.
    pub fn add(
        &mut self,
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        documents: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<HashMap<String, MetadataValue>>>,
    ) {
        for (i, id) in ids.into_iter().enumerate() {
            let embedding = if self.distance_fn == DistanceFunction::Cosine {
                normalize(&embeddings[i])
            } else {
                embeddings[i].clone()
            };

            let document = documents
                .as_ref()
                .and_then(|docs| docs.get(i))
                .and_then(|d| d.clone());

            let metadata = metadatas
                .as_ref()
                .and_then(|metas| metas.get(i))
                .cloned()
                .unwrap_or_default();

            if !self.entries.contains_key(&id) {
                self.id_order.push(id.clone());
            }

            self.entries.insert(
                id.clone(),
                Entry {
                    id,
                    embedding,
                    document,
                    metadata,
                },
            );
        }
    }

    /// Query the collection for the nearest neighbors of the given embedding.
    pub fn query(
        &self,
        query_embedding: &[f32],
        n_results: usize,
        where_filter: Option<&HashMap<String, MetadataValue>>,
    ) -> Vec<SearchResult> {
        let query = if self.distance_fn == DistanceFunction::Cosine {
            normalize(query_embedding)
        } else {
            query_embedding.to_vec()
        };

        let mut scored: Vec<(f32, &Entry)> = self
            .entries
            .values()
            .filter(|entry| {
                if let Some(filter) = where_filter {
                    filter
                        .iter()
                        .all(|(key, val)| entry.metadata.get(key) == Some(val))
                } else {
                    true
                }
            })
            .map(|entry| {
                let distance = self.distance_fn.distance(&query, &entry.embedding);
                (distance, entry)
            })
            .collect();

        // Sort by distance (ascending = most similar first)
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(n_results);

        scored
            .into_iter()
            .map(|(distance, entry)| SearchResult {
                id: entry.id.clone(),
                distance,
                document: entry.document.clone(),
                metadata: entry.metadata.clone(),
            })
            .collect()
    }

    /// Get entries by IDs.
    pub fn get(&self, ids: &[String]) -> Vec<&Entry> {
        ids.iter().filter_map(|id| self.entries.get(id)).collect()
    }

    /// Delete entries by IDs.
    pub fn delete(&mut self, ids: &[String]) {
        for id in ids {
            self.entries.remove(id);
            self.id_order.retain(|i| i != id);
        }
    }

    /// Update entries. Only updates fields that are Some.
    pub fn update(
        &mut self,
        ids: Vec<String>,
        embeddings: Option<Vec<Option<Vec<f32>>>>,
        documents: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<HashMap<String, MetadataValue>>>>,
    ) {
        for (i, id) in ids.iter().enumerate() {
            if let Some(entry) = self.entries.get_mut(id) {
                if let Some(ref embs) = embeddings {
                    if let Some(Some(ref emb)) = embs.get(i) {
                        entry.embedding = if self.distance_fn == DistanceFunction::Cosine {
                            normalize(emb)
                        } else {
                            emb.clone()
                        };
                    }
                }
                if let Some(ref docs) = documents {
                    if let Some(doc) = docs.get(i) {
                        entry.document = doc.clone();
                    }
                }
                if let Some(ref metas) = metadatas {
                    if let Some(Some(ref meta)) = metas.get(i) {
                        entry.metadata = meta.clone();
                    }
                }
            }
        }
    }

    /// Return the number of entries.
    pub fn count(&self) -> usize {
        self.entries.len()
    }

    /// Check if an entry exists.
    pub fn contains(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    /// Serialize the collection to a snapshot for persistence.
    pub fn snapshot(&self) -> CollectionSnapshot {
        let entries: Vec<Entry> = self
            .id_order
            .iter()
            .filter_map(|id| self.entries.get(id).cloned())
            .collect();

        let distance_fn = match self.distance_fn {
            DistanceFunction::Euclidean => "l2",
            DistanceFunction::Cosine => "cosine",
            DistanceFunction::InnerProduct => "ip",
        };

        CollectionSnapshot {
            name: self.name.clone(),
            distance_fn: distance_fn.to_string(),
            entries,
        }
    }

    /// Restore a collection from a snapshot.
    pub fn from_snapshot(snapshot: CollectionSnapshot) -> Self {
        let distance_fn = match snapshot.distance_fn.as_str() {
            "l2" => DistanceFunction::Euclidean,
            "cosine" => DistanceFunction::Cosine,
            "ip" => DistanceFunction::InnerProduct,
            _ => DistanceFunction::Cosine,
        };

        let mut collection = Collection::new(snapshot.name, distance_fn);
        let mut entries = HashMap::new();
        let mut id_order = Vec::new();

        for entry in snapshot.entries {
            id_order.push(entry.id.clone());
            entries.insert(entry.id.clone(), entry);
        }

        collection.entries = entries;
        collection.id_order = id_order;
        collection
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_count() {
        let mut col = Collection::new("test".into(), DistanceFunction::Cosine);
        col.add(
            vec!["a".into(), "b".into()],
            vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            None,
            None,
        );
        assert_eq!(col.count(), 2);
    }

    #[test]
    fn test_query() {
        let mut col = Collection::new("test".into(), DistanceFunction::Cosine);
        col.add(
            vec!["a".into(), "b".into(), "c".into()],
            vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![0.707, 0.707]],
            Some(vec![
                Some("doc_a".into()),
                Some("doc_b".into()),
                Some("doc_c".into()),
            ]),
            None,
        );

        let results = col.query(&[1.0, 0.0], 2, None);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "a"); // Most similar to [1, 0]
    }

    #[test]
    fn test_delete() {
        let mut col = Collection::new("test".into(), DistanceFunction::Cosine);
        col.add(
            vec!["a".into(), "b".into()],
            vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            None,
            None,
        );
        col.delete(&["a".into()]);
        assert_eq!(col.count(), 1);
        assert!(!col.contains("a"));
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let mut col = Collection::new("test".into(), DistanceFunction::Cosine);
        col.add(
            vec!["a".into()],
            vec![vec![1.0, 0.0]],
            Some(vec![Some("hello".into())]),
            None,
        );

        let snap = col.snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        let restored_snap: CollectionSnapshot = serde_json::from_str(&json).unwrap();
        let restored = Collection::from_snapshot(restored_snap);

        assert_eq!(restored.count(), 1);
        assert!(restored.contains("a"));
    }
}
