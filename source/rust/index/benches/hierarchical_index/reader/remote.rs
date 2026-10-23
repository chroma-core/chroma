//! One-level traversal and candidate operations for a coordinator that owns the beam.
use super::super::{
    common::{NodeId, TreeNode},
    persistance::PREFIX_EMBEDDING,
};
use super::{HierarchicalSpannReader, ReaderIntegrityError};
use chroma_error::ChromaError;
use chroma_index::quantization::{Code, QuantizedQuery};
use futures::stream::{self, StreamExt, TryStreamExt};
use serde::Serialize;
use std::{collections::HashSet, time::Instant};

#[derive(Debug, Serialize)]
pub struct RootDescriptor {
    pub id: NodeId,
    pub node_id: NodeId,
    pub kind: &'static str,
    pub is_leaf: bool,
    pub level: usize,
}
#[derive(Debug, Serialize)]
pub struct ChildScore {
    pub parent_id: NodeId,
    pub node_id: NodeId,
    pub is_leaf: bool,
    pub distance: f32,
}
fn integrity(message: impl Into<String>) -> Box<dyn ChromaError> {
    Box::new(ReaderIntegrityError(message.into()))
}
impl HierarchicalSpannReader {
    pub fn describe_root(&self) -> Result<RootDescriptor, Box<dyn ChromaError>> {
        let node = self
            .nodes
            .get(&self.root_id)
            .ok_or_else(|| integrity("missing root"))?;
        let is_leaf = matches!(node.value(), TreeNode::Leaf(_));
        Ok(RootDescriptor {
            id: self.root_id,
            node_id: self.root_id,
            kind: if is_leaf { "leaf" } else { "internal" },
            is_leaf,
            level: 0,
        })
    }
    /// Score every child at one level. The caller applies the shared global beam.
    pub fn expand_nodes(
        &self,
        query: &[f32],
        parents: &[NodeId],
    ) -> Result<Vec<ChildScore>, Box<dyn ChromaError>> {
        let qq = QuantizedQuery::new(query, self.padded_bytes(), 0.0, 0.0, Self::vec_norm(query));
        let mut seen = HashSet::new();
        let mut children = Vec::new();
        for &parent_id in parents {
            if !seen.insert(parent_id) {
                return Err(integrity("duplicate expansion parent"));
            }
            let ids = {
                let parent = self
                    .nodes
                    .get(&parent_id)
                    .ok_or_else(|| integrity(format!("missing parent {parent_id}")))?;
                let TreeNode::Internal(internal) = parent.value() else {
                    return Err(integrity(format!("cannot expand leaf {parent_id}")));
                };
                internal.children.clone()
            };
            for node_id in ids {
                let node = self
                    .nodes
                    .get(&node_id)
                    .ok_or_else(|| integrity(format!("missing child {node_id}")))?;
                let distance = Code::<1, _>::new(node.centroid_code())
                    .distance_quantized_query(&self.distance_fn, &qq);
                if !distance.is_finite() {
                    return Err(integrity("non-finite child navigation distance"));
                }
                children.push(ChildScore {
                    parent_id,
                    node_id,
                    is_leaf: matches!(node.value(), TreeNode::Leaf(_)),
                    distance,
                });
            }
        }
        Ok(children)
    }
    /// Return nearest unique candidates plus every tie at the budget boundary.
    /// The worker resolves those ties using external IDs before returning top-M.
    pub async fn score_selected_leaves(
        &self,
        query: &[f32],
        leaves: &[NodeId],
        budget: usize,
    ) -> Result<(Vec<(u32, f32)>, usize), Box<dyn ChromaError>> {
        if budget == 0 {
            return Err(integrity("candidate budget must be positive"));
        }
        let mut seen = HashSet::new();
        for &id in leaves {
            if !seen.insert(id) {
                return Err(integrity("duplicate selected leaf"));
            }
            if !self
                .nodes
                .get(&id)
                .is_some_and(|node| matches!(node.value(), TreeNode::Leaf(_)))
            {
                return Err(integrity(format!("selected node {id} is not a leaf")));
            }
        }
        let started = Instant::now();
        stream::iter(
            leaves
                .iter()
                .copied()
                .map(|id| self.load_node_posting_list(id)),
        )
        .buffer_unordered(32)
        .try_collect::<Vec<_>>()
        .await?;
        let selected: Vec<_> = leaves.iter().map(|&id| (id, 0.0)).collect();
        let (mut hits, unique, _, _) = self.score_leaves(
            query,
            usize::MAX,
            1,
            &selected,
            started.elapsed().as_nanos() as u64,
        );
        if hits.iter().any(|(_, distance)| !distance.is_finite()) {
            return Err(integrity("non-finite approximate candidate distance"));
        }
        if hits.len() > budget {
            let boundary = hits[budget - 1].1;
            let keep = hits
                .iter()
                .take_while(|(_, distance)| *distance <= boundary)
                .count();
            hits.truncate(keep);
        }
        Ok((hits, unique))
    }
    /// Score only the coordinator's globally selected candidates using raw vectors.
    pub async fn rerank_selected(
        &self,
        query: &[f32],
        ids: &[u32],
    ) -> Result<Vec<(u32, f32)>, Box<dyn ChromaError>> {
        let unique: HashSet<_> = ids.iter().copied().collect();
        if unique.len() != ids.len() {
            return Err(integrity("duplicate exact-rerank ID"));
        }
        let reader = &self.vector_data_reader;
        stream::iter(ids.iter().copied().map(|id| async move {
            let vector = reader
                .get(PREFIX_EMBEDDING, id)
                .await?
                .ok_or_else(|| integrity(format!("missing exact embedding for {id}")))?;
            let distance = self.dist(query, vector);
            if !distance.is_finite() {
                return Err(integrity("non-finite exact distance"));
            }
            Ok((id, distance))
        }))
        .buffer_unordered(32)
        .try_collect()
        .await
    }
}
