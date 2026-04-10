#![allow(dead_code)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions,
    provider::BlockfileProvider,
    BlockfileFlusher, BlockfileWriterOptions,
};
use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;
use chroma_index::quantization::Code;
use chroma_types::QuantizedCluster;
use dashmap::{DashMap, DashSet};
use parking_lot::ReentrantMutex;
use uuid::Uuid;

use super::{
    HierarchicalSpannConfig, HierarchicalSpannWriter, InternalNode, LeafNode, NodeId, TreeNode,
    WriterStats,
};

// Blockfile prefix constants
const PREFIX_ROOT: &str = "root";
const PREFIX_NEXT_NODE: &str = "next_node";
const PREFIX_DIM: &str = "dim";
const PREFIX_NODE_TYPE: &str = "node_type";
const PREFIX_PARENT: &str = "parent";
const PREFIX_LENGTH: &str = "length";
const PREFIX_VERSION: &str = "version";
const PREFIX_CENTROID: &str = "centroid";
const PREFIX_EMBEDDING: &str = "embedding";
const PREFIX_CHILDREN: &str = "children";

const SINGLETON_KEY: u32 = 0;
const NODE_TYPE_LEAF: u32 = 0;
const NODE_TYPE_INTERNAL: u32 = 1;
const NO_PARENT: u32 = u32::MAX;

#[derive(Clone, Debug)]
pub struct HierarchicalSpannIds {
    pub posting_list_id: Uuid,
    pub scalar_metadata_id: Uuid,
    pub vector_data_id: Uuid,
    pub list_data_id: Uuid,
}

pub struct HierarchicalSpannFlusher {
    posting_list_flusher: BlockfileFlusher,
    scalar_metadata_flusher: BlockfileFlusher,
    vector_data_flusher: BlockfileFlusher,
    list_data_flusher: BlockfileFlusher,
}

impl HierarchicalSpannFlusher {
    pub async fn flush(self) -> Result<HierarchicalSpannIds, Box<dyn ChromaError>> {
        let posting_list_id = self.posting_list_flusher.id();
        let scalar_metadata_id = self.scalar_metadata_flusher.id();
        let vector_data_id = self.vector_data_flusher.id();
        let list_data_id = self.list_data_flusher.id();

        self.posting_list_flusher
            .flush::<u32, QuantizedCluster<'_>>()
            .await?;
        self.scalar_metadata_flusher.flush::<u32, u32>().await?;
        self.vector_data_flusher
            .flush::<u32, Vec<f32>>()
            .await?;
        self.list_data_flusher
            .flush::<u32, Vec<u32>>()
            .await?;

        Ok(HierarchicalSpannIds {
            posting_list_id,
            scalar_metadata_id,
            vector_data_id,
            list_data_id,
        })
    }
}

impl HierarchicalSpannWriter {
    /// Commit all in-memory state to blockfiles and return a flusher.
    pub async fn commit(
        self,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<HierarchicalSpannFlusher, Box<dyn ChromaError>> {
        let pl_options = BlockfileWriterOptions::new("".to_string()).ordered_mutations();
        let sm_options = BlockfileWriterOptions::new("".to_string()).ordered_mutations();
        let vd_options = BlockfileWriterOptions::new("".to_string()).ordered_mutations();
        let ld_options = BlockfileWriterOptions::new("".to_string()).ordered_mutations();

        let posting_list_writer = blockfile_provider
            .write::<u32, QuantizedCluster<'_>>(pl_options)
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;
        let scalar_metadata_writer = blockfile_provider
            .write::<u32, u32>(sm_options)
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;
        let vector_data_writer = blockfile_provider
            .write::<u32, Vec<f32>>(vd_options)
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;
        let list_data_writer = blockfile_provider
            .write::<u32, Vec<u32>>(ld_options)
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        // --- Scalar metadata: singletons ---
        scalar_metadata_writer
            .set(PREFIX_ROOT, SINGLETON_KEY, self.root_id.load(Ordering::Relaxed))
            .await?;
        scalar_metadata_writer
            .set(PREFIX_NEXT_NODE, SINGLETON_KEY, self.next_node_id.load(Ordering::Relaxed))
            .await?;
        scalar_metadata_writer
            .set(PREFIX_DIM, SINGLETON_KEY, self.dim as u32)
            .await?;

        // --- Per-node data (sorted by node_id for ordered mutations) ---
        let mut node_ids: Vec<NodeId> = self.nodes.iter().map(|e| *e.key()).collect();
        node_ids.sort_unstable();

        for &node_id in &node_ids {
            let node_ref = self.nodes.get(&node_id).unwrap();
            match node_ref.value() {
                TreeNode::Leaf(leaf) => {
                    // Scalar metadata
                    scalar_metadata_writer
                        .set(PREFIX_NODE_TYPE, node_id, NODE_TYPE_LEAF)
                        .await?;
                    scalar_metadata_writer
                        .set(
                            PREFIX_PARENT,
                            node_id,
                            leaf.parent_id.unwrap_or(NO_PARENT),
                        )
                        .await?;
                    scalar_metadata_writer
                        .set(PREFIX_LENGTH, node_id, leaf.ids.len() as u32)
                        .await?;

                    // Vector data: centroid
                    vector_data_writer
                        .set(PREFIX_CENTROID, node_id, leaf.centroid.clone())
                        .await?;

                    let cluster = QuantizedCluster {
                        center: &leaf.centroid,
                        codes: &leaf.codes,
                        ids: &leaf.ids,
                        versions: &leaf.versions,
                    };
                    posting_list_writer.set("", node_id, cluster).await?;
                }
                TreeNode::Internal(internal) => {
                    // Scalar metadata
                    scalar_metadata_writer
                        .set(PREFIX_NODE_TYPE, node_id, NODE_TYPE_INTERNAL)
                        .await?;
                    scalar_metadata_writer
                        .set(
                            PREFIX_PARENT,
                            node_id,
                            internal.parent_id.unwrap_or(NO_PARENT),
                        )
                        .await?;

                    // Vector data: centroid
                    vector_data_writer
                        .set(PREFIX_CENTROID, node_id, internal.centroid.clone())
                        .await?;

                    // List data: children
                    list_data_writer
                        .set(PREFIX_CHILDREN, node_id, internal.children.clone())
                        .await?;
                }
            }
        }

        // --- Versions (sorted by data_id for ordered mutations) ---
        let mut version_entries: Vec<(u32, u32)> =
            self.versions.iter().map(|e| (*e.key(), *e.value())).collect();
        version_entries.sort_unstable();
        for (data_id, version) in version_entries {
            scalar_metadata_writer
                .set(PREFIX_VERSION, data_id, version)
                .await?;
        }

        // --- Embeddings (sorted by data_id for ordered mutations) ---
        let mut embedding_entries: Vec<(u32, Arc<[f32]>)> =
            self.embeddings.iter().map(|e| (*e.key(), e.value().clone())).collect();
        embedding_entries.sort_unstable_by_key(|(k, _)| *k);
        for (data_id, embedding) in embedding_entries {
            vector_data_writer
                .set(PREFIX_EMBEDDING, data_id, embedding.to_vec())
                .await?;
        }

        // --- Commit all writers ---
        let posting_list_flusher = posting_list_writer
            .commit::<u32, QuantizedCluster<'_>>()
            .await?;
        let scalar_metadata_flusher = scalar_metadata_writer.commit::<u32, u32>().await?;
        let vector_data_flusher = vector_data_writer.commit::<u32, Vec<f32>>().await?;
        let list_data_flusher = list_data_writer.commit::<u32, Vec<u32>>().await?;

        Ok(HierarchicalSpannFlusher {
            posting_list_flusher,
            scalar_metadata_flusher,
            vector_data_flusher,
            list_data_flusher,
        })
    }

    /// Open a persisted index from blockfiles with lazy leaf loading.
    pub async fn open(
        blockfile_provider: &BlockfileProvider,
        ids: HierarchicalSpannIds,
        distance_fn: DistanceFunction,
        config: HierarchicalSpannConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        // --- Step 1: Read scalar metadata ---
        let sm_reader = blockfile_provider
            .read::<u32, u32>(BlockfileReaderOptions::new(
                ids.scalar_metadata_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        let root_id = sm_reader
            .get(PREFIX_ROOT, SINGLETON_KEY)
            .await?
            .expect("missing root_id");
        let next_node_id = sm_reader
            .get(PREFIX_NEXT_NODE, SINGLETON_KEY)
            .await?
            .expect("missing next_node_id");
        let dim = sm_reader
            .get(PREFIX_DIM, SINGLETON_KEY)
            .await?
            .expect("missing dim") as usize;

        // Load node types
        let mut node_types: Vec<(u32, u32)> = Vec::new();
        for (_prefix, key, value) in sm_reader
            .get_range(PREFIX_NODE_TYPE..=PREFIX_NODE_TYPE, ..)
            .await?
        {
            node_types.push((key, value));
        }

        // Load parent ids
        let mut parent_ids: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
        for (_prefix, key, value) in sm_reader
            .get_range(PREFIX_PARENT..=PREFIX_PARENT, ..)
            .await?
        {
            parent_ids.insert(key, value);
        }

        // Load lengths (leaf posting counts)
        let mut lengths: std::collections::HashMap<u32, usize> =
            std::collections::HashMap::new();
        for (_prefix, key, value) in sm_reader
            .get_range(PREFIX_LENGTH..=PREFIX_LENGTH, ..)
            .await?
        {
            lengths.insert(key, value as usize);
        }

        // Load versions
        let versions = DashMap::new();
        for (_prefix, key, value) in sm_reader
            .get_range(PREFIX_VERSION..=PREFIX_VERSION, ..)
            .await?
        {
            versions.insert(key, value);
        }

        // --- Step 2: Read centroids ---
        let vd_reader = blockfile_provider
            .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(
                ids.vector_data_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        let mut centroids: std::collections::HashMap<u32, Vec<f32>> =
            std::collections::HashMap::new();
        for (_prefix, key, value) in vd_reader
            .get_range(PREFIX_CENTROID..=PREFIX_CENTROID, ..)
            .await?
        {
            centroids.insert(key, value.to_vec());
        }

        // --- Step 3: Read children lists ---
        let ld_reader = blockfile_provider
            .read::<u32, &'static [u32]>(BlockfileReaderOptions::new(
                ids.list_data_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        let mut children_map: std::collections::HashMap<u32, Vec<u32>> =
            std::collections::HashMap::new();
        for (_prefix, key, value) in ld_reader
            .get_range(PREFIX_CHILDREN..=PREFIX_CHILDREN, ..)
            .await?
        {
            children_map.insert(key, value.to_vec());
        }

        // --- Step 4: Build the node tree ---
        let nodes: DashMap<NodeId, TreeNode> = DashMap::new();

        for &(node_id, ntype) in &node_types {
            let parent_id = parent_ids
                .get(&node_id)
                .copied()
                .map(|p| if p == NO_PARENT { None } else { Some(p) })
                .unwrap_or(None);
            let centroid = centroids
                .remove(&node_id)
                .unwrap_or_else(|| vec![0.0; dim]);

            if ntype == NODE_TYPE_LEAF {
                let length = lengths.get(&node_id).copied().unwrap_or(0);
                nodes.insert(
                    node_id,
                    TreeNode::Leaf(LeafNode {
                        centroid,
                        centroid_code: Vec::new(),
                        ids: Vec::new(),
                        versions: Vec::new(),
                        codes: Vec::new(),
                        parent_id,
                        length,
                    }),
                );
            } else {
                let children = children_map
                    .remove(&node_id)
                    .unwrap_or_default();
                nodes.insert(
                    node_id,
                    TreeNode::Internal(InternalNode {
                        centroid,
                        centroid_code: Vec::new(),
                        children,
                        parent_id,
                    }),
                );
            }
        }

        // --- Step 5: Recompute centroid_codes from centroids + parent centroids ---
        let zero_centroid = vec![0.0f32; dim];
        let all_node_ids: Vec<NodeId> = nodes.iter().map(|e| *e.key()).collect();
        for &nid in &all_node_ids {
            let parent_centroid = {
                let node_ref = nodes.get(&nid).unwrap();
                let pid = node_ref.parent_id();
                match pid {
                    Some(p) => nodes
                        .get(&p)
                        .map(|n| n.centroid().to_vec())
                        .unwrap_or_else(|| zero_centroid.clone()),
                    None => zero_centroid.clone(),
                }
            };
            let centroid = {
                let node_ref = nodes.get(&nid).unwrap();
                node_ref.centroid().to_vec()
            };
            let code = Code::<1>::quantize(&centroid, &parent_centroid);
            let code_bytes = code.as_ref().to_vec();
            if let Some(mut node_ref) = nodes.get_mut(&nid) {
                match node_ref.value_mut() {
                    TreeNode::Leaf(leaf) => leaf.centroid_code = code_bytes,
                    TreeNode::Internal(internal) => internal.centroid_code = code_bytes,
                }
            }
        }

        // --- Step 6: Open posting list and vector data readers for lazy loading ---
        let posting_list_reader = Some(
            blockfile_provider
                .read::<u32, QuantizedCluster<'static>>(BlockfileReaderOptions::new(
                    ids.posting_list_id,
                    "".to_string(),
                ))
                .await
                .map_err(|e| e as Box<dyn ChromaError>)?,
        );
        let vector_data_reader = Some(vd_reader);

        Ok(Self {
            dim,
            distance_fn,
            config,
            nodes,
            balancing: DashSet::new(),
            tree_lock: ReentrantMutex::new(()),
            root_id: AtomicU32::new(root_id),
            next_node_id: AtomicU32::new(next_node_id),
            embeddings: DashMap::new(),
            versions,
            stats: WriterStats::default(),
            posting_list_reader,
            vector_data_reader,
        })
    }

    /// Lazily load a leaf node's posting data (ids, codes, versions) from the
    /// persisted blockfile.
    pub async fn load(&self, node_id: NodeId) -> Result<(), Box<dyn ChromaError>> {
        let Some(reader) = &self.posting_list_reader else {
            return Ok(());
        };

        {
            let node_ref = self.nodes.get(&node_id);
            match node_ref.as_ref().map(|r| r.value()) {
                Some(TreeNode::Leaf(leaf)) if leaf.ids.len() >= leaf.length => return Ok(()),
                Some(TreeNode::Leaf(_)) => {}
                _ => return Ok(()),
            }
        }

        let Some(cluster) = reader.get("", node_id).await? else {
            return Ok(());
        };

        if let Some(mut node_ref) = self.nodes.get_mut(&node_id) {
            if let TreeNode::Leaf(leaf) = node_ref.value_mut() {
                if leaf.ids.len() < leaf.length {
                    leaf.ids = cluster.ids.to_vec();
                    leaf.versions = cluster.versions.to_vec();
                    leaf.codes = cluster.codes.to_vec();
                    leaf.length = leaf.ids.len();
                }
            }
        }

        Ok(())
    }

    /// Lazily load raw f32 embeddings from the persisted blockfile.
    pub async fn load_raw(&self, ids: &[u32]) -> Result<(), Box<dyn ChromaError>> {
        let Some(reader) = &self.vector_data_reader else {
            return Ok(());
        };

        let missing_ids: Vec<u32> = ids
            .iter()
            .copied()
            .filter(|id| !self.embeddings.contains_key(id))
            .collect();

        for id in missing_ids {
            if let Some(embedding) = reader.get(PREFIX_EMBEDDING, id).await? {
                self.embeddings.insert(id, Arc::from(embedding));
            }
        }

        Ok(())
    }
}
