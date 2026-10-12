use std::collections::{HashMap, HashSet};

use chroma_error::ChromaError;
use chroma_index::quantization::{
    four_bit_query_values, Code, FourBitLayout, PreparedFourBit, QuantizedQuery,
};
use clap::ValueEnum;
use rayon::prelude::*;

use super::super::common::{effective_beam, NodeId, ReadBeamPolicy, TreeNode};
use super::super::persistance::PREFIX_CENTROID;
use super::HierarchicalSpannReader;

/// Precision of the stored navigation centroids and the query used to score
/// them. Posting codes and final full-precision reranking remain unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum NavigationPrecision {
    #[value(name = "1bit-4bit")]
    OneBitFourBit,
    #[value(name = "4bit-4bit")]
    FourBitFourBit,
    #[value(name = "4bit-fp")]
    FourBitFp,
    #[value(name = "4bit-planes-4bit")]
    FourBitPlanes,
    #[value(name = "4bit-packed-4bit")]
    FourBitPacked,
    #[value(name = "4bit-packed-fp")]
    FourBitPackedFp,
}

impl NavigationPrecision {
    pub fn label(self) -> &'static str {
        match self {
            Self::OneBitFourBit => "1bit-4bit",
            Self::FourBitFourBit => "4bit-4bit",
            Self::FourBitFp => "4bit-fp",
            Self::FourBitPlanes => "4bit-planes-4bit",
            Self::FourBitPacked => "4bit-packed-4bit",
            Self::FourBitPackedFp => "4bit-packed-fp",
        }
    }
}

pub struct NavigationTrace {
    pub levels: Vec<Vec<NodeId>>,
    pub candidates: Vec<usize>,
}

impl HierarchicalSpannReader {
    /// Prepare navigation codes from an existing checkpoint's original
    /// centroids. This reader experiment never rewrites checkpoint files.
    /// Four-bit payloads are built once at setup and retained across queries.
    pub async fn set_navigation_precision(
        &mut self,
        precision: NavigationPrecision,
    ) -> Result<(), Box<dyn ChromaError>> {
        if precision != self.navigation_precision {
            self.four_bit_centroids.clear();
            self.prepared_four_bit_centroids.clear();
        }
        if precision != NavigationPrecision::OneBitFourBit
            && self.four_bit_centroids.is_empty()
            && self.prepared_four_bit_centroids.is_empty()
        {
            let centroids = self
                .vector_data_reader
                .get_range(PREFIX_CENTROID..=PREFIX_CENTROID, ..)
                .await?;
            let owned: Vec<_> = centroids
                .into_iter()
                .filter(|(_, id, _)| self.nodes.contains_key(id))
                .map(|(_, id, centroid)| (id, centroid.to_vec()))
                .collect();
            assert_eq!(
                owned.len(),
                self.nodes.len(),
                "checkpoint is missing navigation centroids"
            );
            assert!(owned.iter().all(|(_, centroid)| centroid.len() == self.dim));
            let zero = vec![0.0; self.dim];
            self.four_bit_centroids = owned
                .into_par_iter()
                .map(|(id, centroid)| (id, Code::<4>::quantize(&centroid, &zero).as_ref().to_vec()))
                .collect();
            self.clear_loaded_blocks();
        }
        if matches!(
            precision,
            NavigationPrecision::FourBitPlanes
                | NavigationPrecision::FourBitPacked
                | NavigationPrecision::FourBitPackedFp
        ) && self.prepared_four_bit_centroids.is_empty()
        {
            self.prepared_four_bit_centroids = self
                .four_bit_centroids
                .drain()
                .collect::<Vec<_>>()
                .into_par_iter()
                .map(|(id, bytes)| {
                    let layout = if precision == NavigationPrecision::FourBitPlanes {
                        FourBitLayout::BitPlanes
                    } else {
                        FourBitLayout::Packed
                    };
                    (
                        id,
                        PreparedFourBit::new(&Code::<4, _>::new(bytes), self.dim, layout),
                    )
                })
                .collect();
        }
        self.navigation_precision = precision;
        Ok(())
    }

    /// Logical code payload if this mode replaces the stored navigation codes.
    /// Experimental memory also retains the checkpoint's original one-bit codes.
    pub fn navigation_code_bytes(&self) -> usize {
        match self.navigation_precision {
            NavigationPrecision::OneBitFourBit => {
                self.nodes.iter().map(|n| n.centroid_code().len()).sum()
            }
            NavigationPrecision::FourBitPlanes
            | NavigationPrecision::FourBitPacked
            | NavigationPrecision::FourBitPackedFp => self
                .prepared_four_bit_centroids
                .values()
                .map(PreparedFourBit::bytes)
                .sum(),
            _ => self.four_bit_centroids.values().map(Vec::len).sum(),
        }
    }

    /// Estimate owned scoring-cache allocations from capacities, including map
    /// entries and their heap buffers. Hash-table allocator metadata is excluded.
    pub fn navigation_cache_allocation_bytes(&self) -> usize {
        self.four_bit_centroids.capacity() * std::mem::size_of::<(NodeId, Vec<u8>)>()
            + self
                .four_bit_centroids
                .values()
                .map(Vec::capacity)
                .sum::<usize>()
            + self.prepared_four_bit_centroids.capacity()
                * std::mem::size_of::<(NodeId, PreparedFourBit)>()
            + self
                .prepared_four_bit_centroids
                .values()
                .map(PreparedFourBit::heap_bytes)
                .sum::<usize>()
    }

    pub fn navigation_kernel_label(&self) -> &'static str {
        match self.navigation_precision {
            NavigationPrecision::FourBitPlanes => self.four_bit_kernels.plane_label,
            NavigationPrecision::FourBitPacked | NavigationPrecision::FourBitPackedFp => {
                self.four_bit_kernels.packed_label
            }
            _ => "existing",
        }
    }

    pub fn benchmark_navigation_scores(&self, query: &[f32], limit: usize) -> (f32, f64, usize) {
        let quantized =
            QuantizedQuery::new(query, self.padded_bytes(), 0.0, 0.0, Self::vec_norm(query));
        let values = if self.navigation_precision == NavigationPrecision::FourBitPackedFp {
            let mut values = query.to_vec();
            values.resize(self.dim.div_ceil(16) * 16, 0.0);
            values
        } else {
            four_bit_query_values(&quantized, self.dim)
        };
        let sum = query.iter().sum();
        let mut ids: Vec<_> = self.nodes.iter().map(|node| *node.key()).collect();
        ids.sort_unstable();
        let count = ids.len().min(limit);
        let start = std::time::Instant::now();
        let checksum = ids
            .into_iter()
            .take(limit)
            .map(|id| match self.navigation_precision {
                NavigationPrecision::OneBitFourBit => {
                    Code::<1, _>::new(self.nodes.get(&id).unwrap().centroid_code())
                        .distance_quantized_query(&self.distance_fn, &quantized)
                }
                NavigationPrecision::FourBitFourBit => Code::<4, _>::new(
                    &self.four_bit_centroids[&id],
                )
                .distance_quantized_query(&self.distance_fn, &quantized, self.dim),
                NavigationPrecision::FourBitFp => Code::<4, _>::new(&self.four_bit_centroids[&id])
                    .distance_query(&self.distance_fn, query, 0.0, 0.0, quantized.q_norm),
                NavigationPrecision::FourBitPlanes | NavigationPrecision::FourBitPacked => {
                    self.prepared_four_bit_centroids[&id].distance_quantized(
                        &quantized,
                        &values,
                        &self.four_bit_kernels,
                        &self.distance_fn,
                    )
                }
                NavigationPrecision::FourBitPackedFp => self.prepared_four_bit_centroids[&id]
                    .distance_fp(
                        &values,
                        sum,
                        quantized.q_norm,
                        &self.four_bit_kernels,
                        &self.distance_fn,
                    ),
            })
            .sum();
        (checksum, start.elapsed().as_nanos() as f64, count)
    }

    pub(super) fn navigate(&self, query: &[f32], policy: &ReadBeamPolicy) -> Vec<(NodeId, f32)> {
        self.navigate_recording(query, policy, |_, _, _| {})
    }

    pub fn navigation_trace(&self, query: &[f32], policy: &ReadBeamPolicy) -> NavigationTrace {
        let mut trace = NavigationTrace {
            levels: Vec::new(),
            candidates: Vec::new(),
        };
        self.navigate_recording(query, policy, |selected, leaves, candidates| {
            trace
                .levels
                .push(leaves.iter().chain(selected).map(|&(id, _)| id).collect());
            trace.candidates.push(candidates);
        });
        trace
    }

    fn navigate_recording(
        &self,
        query: &[f32],
        policy: &ReadBeamPolicy,
        mut record: impl FnMut(&[(NodeId, f32)], &[(NodeId, f32)], usize),
    ) -> Vec<(NodeId, f32)> {
        assert_eq!(query.len(), self.dim);
        let Some(root_node) = self.nodes.get(&self.root_id) else {
            return Vec::new();
        };
        if matches!(root_node.value(), TreeNode::Leaf(_)) {
            record(&[(self.root_id, 0.0)], &[], 1);
            return vec![(self.root_id, 0.0)];
        }
        drop(root_node);
        let q_norm = Self::vec_norm(query);
        let quantized = (!matches!(
            self.navigation_precision,
            NavigationPrecision::FourBitFp | NavigationPrecision::FourBitPackedFp
        ))
        .then(|| QuantizedQuery::new(query, self.padded_bytes(), 0.0, 0.0, q_norm));
        let values = if self.navigation_precision == NavigationPrecision::FourBitPacked {
            four_bit_query_values(quantized.as_ref().unwrap(), self.dim)
        } else if self.navigation_precision == NavigationPrecision::FourBitPackedFp {
            let mut values = query.to_vec();
            values.resize(self.dim.div_ceil(16) * 16, 0.0);
            values
        } else {
            Vec::new()
        };
        let query_sum = if self.navigation_precision == NavigationPrecision::FourBitPackedFp {
            query.iter().sum()
        } else {
            0.0
        };
        let mut leaves = Vec::new();
        let mut beam = vec![self.root_id];
        let mut level = 0;
        loop {
            let mut scored = Vec::new();
            for id in &beam {
                let Some(parent) = self.nodes.get(id) else {
                    continue;
                };
                let TreeNode::Internal(internal) = parent.value() else {
                    continue;
                };
                let children = internal.children().to_vec();
                drop(parent);
                for child_id in children {
                    let Some(child) = self.nodes.get(&child_id) else {
                        continue;
                    };
                    let distance =
                        match self.navigation_precision {
                            NavigationPrecision::OneBitFourBit => {
                                Code::<1, _>::new(child.centroid_code()).distance_quantized_query(
                                    &self.distance_fn,
                                    quantized.as_ref().unwrap(),
                                )
                            }
                            NavigationPrecision::FourBitFourBit => {
                                Code::<4, _>::new(&self.four_bit_centroids[&child_id])
                                    .distance_quantized_query(
                                        &self.distance_fn,
                                        quantized.as_ref().unwrap(),
                                        self.dim,
                                    )
                            }
                            NavigationPrecision::FourBitPlanes
                            | NavigationPrecision::FourBitPacked => {
                                self.prepared_four_bit_centroids[&child_id].distance_quantized(
                                    quantized.as_ref().unwrap(),
                                    &values,
                                    &self.four_bit_kernels,
                                    &self.distance_fn,
                                )
                            }
                            NavigationPrecision::FourBitPackedFp => {
                                self.prepared_four_bit_centroids[&child_id].distance_fp(
                                    &values,
                                    query_sum,
                                    q_norm,
                                    &self.four_bit_kernels,
                                    &self.distance_fn,
                                )
                            }
                            NavigationPrecision::FourBitFp => {
                                Code::<4, _>::new(&self.four_bit_centroids[&child_id])
                                    .distance_query(&self.distance_fn, query, 0.0, 0.0, q_norm)
                            }
                        };
                    scored.push((child_id, distance));
                }
            }
            if scored.is_empty() {
                break;
            }
            level += 1;
            let candidates = scored.len();
            let params = policy.level_params(level);
            scored.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
            let width = effective_beam(&scored, params.tau, params.beam_min, params.beam_max);
            scored.truncate(width);
            // Leaves accepted earlier remain reachable in an uneven tree.
            record(&scored, &leaves, candidates);
            let mut internals = Vec::new();
            for &(id, distance) in &scored {
                if let Some(node) = self.nodes.get(&id) {
                    match node.value() {
                        TreeNode::Leaf(_) => leaves.push((id, distance)),
                        TreeNode::Internal(_) => internals.push(id),
                    }
                }
            }
            if internals.is_empty() {
                break;
            }
            beam = internals;
        }
        leaves.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
        leaves
    }

    /// Map exact neighbor IDs to every ancestor of their loaded leaf. The
    /// benchmark builds this once outside query timings and reuses it for all
    /// precision modes; navigation traces therefore measure the actual reader.
    pub fn ground_truth_reachability(
        &self,
        neighbors: &[Vec<u32>],
    ) -> Vec<HashMap<NodeId, HashSet<u32>>> {
        let mut queries_by_id: HashMap<u32, Vec<usize>> = HashMap::new();
        for (query, ids) in neighbors.iter().enumerate() {
            for &id in ids {
                queries_by_id.entry(id).or_default().push(query);
            }
        }
        let mut reachable: Vec<HashMap<NodeId, HashSet<u32>>> =
            vec![HashMap::new(); neighbors.len()];
        for entry in self.nodes.iter() {
            let TreeNode::Leaf(leaf) = entry.value() else {
                continue;
            };
            let leaf_id = *entry.key();
            let hits: Vec<_> = leaf
                .ids
                .iter()
                .filter_map(|id| queries_by_id.get(id).map(|queries| (*id, queries.clone())))
                .collect();
            drop(entry);
            if hits.is_empty() {
                continue;
            }
            let mut node_id = Some(leaf_id);
            let mut visited = HashSet::new();
            while let Some(id) = node_id {
                assert!(visited.insert(id), "cycle in checkpoint tree");
                for (vector_id, queries) in &hits {
                    for &query in queries {
                        reachable[query].entry(id).or_default().insert(*vector_id);
                    }
                }
                node_id = self.nodes.get(&id).and_then(|node| match node.value() {
                    TreeNode::Leaf(leaf) => leaf.parent_id,
                    TreeNode::Internal(internal) => internal.parent_id,
                });
            }
        }
        reachable
    }
}
