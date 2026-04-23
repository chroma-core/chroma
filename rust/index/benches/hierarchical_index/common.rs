pub type NodeId = u32;

// =============================================================================
// Node types
// =============================================================================
pub struct LeafNode {
    // full precision centroid relative to the origin (used on the write path)
    pub centroid: Vec<f32>,
    /// Quantized (1-bit RaBitQ) centroid code (used on the read path)
    pub centroid_code: Vec<u8>,

    // Codes
    /// Per-vector 1-bit RaBitQ codes packed into one contiguous buffer.
    pub codes: Vec<u8>,
    // The ids of the vectors in the leaf.
    pub ids: Vec<u32>,
    // The versions of the vectors in the leaf.
    pub versions: Vec<u8>,
    /// Total posting count for lazy-load detection. When `ids.len() < length`,
    /// the posting data has not yet been loaded from the blockfile.
    pub length: usize,

    // Parent Node ID
    pub parent_id: Option<NodeId>,
}

pub struct InternalNode {
    // full precision centroid (used on the write path)
    pub centroid: Vec<f32>,
    /// Quantized (1-bit RaBitQ) centroid code (used on the read path)
    pub centroid_code: Vec<u8>,
    // The children of the internal node. Can be Leaf or Internal nodes.
    pub children: Vec<NodeId>,
    // The parent node ID. Null if this is the root node.
    pub parent_id: Option<NodeId>,
}

pub enum TreeNode {
    Leaf(LeafNode),
    Internal(InternalNode),
}

#[derive(Clone, Debug)]
pub struct ReadBeamPolicy {
    pub(super) default_tau: Option<f64>,
    pub(super) default_beam_min: usize,
    pub(super) default_beam_max: usize,
    pub(super) level_taus: Vec<Option<f64>>,
    pub(super) level_min_pcts: Vec<f64>,
    pub(super) level_widths: Vec<usize>,
}

#[derive(Clone, Copy, Debug)]
pub struct LevelBeamParams {
    pub tau: Option<f64>,
    pub beam_min: usize,
    pub beam_max: usize,
}

impl ReadBeamPolicy {
    pub fn uniform(tau: Option<f64>, beam_min: usize, beam_max: usize) -> Self {
        Self {
            default_tau: tau,
            default_beam_min: beam_min,
            default_beam_max: beam_max,
            level_taus: Vec::new(),
            level_min_pcts: Vec::new(),
            level_widths: Vec::new(),
        }
    }

    pub fn with_level_overrides(
        tau: Option<f64>,
        beam_min: usize,
        beam_max: usize,
        level_taus: Vec<Option<f64>>,
        level_min_pcts: Vec<f64>,
        level_widths: Vec<usize>,
    ) -> Self {
        Self {
            default_tau: tau,
            default_beam_min: beam_min,
            default_beam_max: beam_max,
            level_taus,
            level_min_pcts,
            level_widths,
        }
    }

    /// Compute the beam parameters for a single level (1-indexed).
    ///
    /// Pipeline (matches `effective_beam` below):
    ///   1. Pick a tau: `level_taus[idx]` if set (and not `_`), else
    ///      `default_tau`. The tau filter selects how many candidates
    ///      pass `dist <= d_best * tau`.
    ///   2. Apply `level_min_pcts[idx]` as a *floor* scaled to the
    ///      level width: `floor = max(default_beam_min,
    ///      ceil(level_width * pct/100))`. When `level_min_pcts` has no
    ///      entry for this level, the floor is just `default_beam_min`.
    ///   3. Cap absolutely with `default_beam_max` (`--write-beam-max` /
    ///      `--read-beam-max`). This applies at every level, not just
    ///      the leaf, so a per-level percentage floor cannot blow past
    ///      the user's hard cap.
    ///   4. Defensive clamp: if a high `min_pct` produced a floor above
    ///      `default_beam_max`, the cap wins
    ///      (`beam_min = beam_min.min(beam_max)`).
    pub fn level_params(&self, level: usize) -> LevelBeamParams {
        let idx = level.saturating_sub(1);
        let level_width = self.level_widths.get(idx).copied();
        let pct_min = self.level_min_pcts.get(idx).copied();

        let scaled_floor = match (level_width, pct_min) {
            (Some(width), Some(pct)) => Some(((width as f64) * (pct / 100.0)).ceil() as usize),
            _ => None,
        };
        let beam_min = match scaled_floor {
            Some(floor) => floor.max(self.default_beam_min),
            None => self.default_beam_min,
        };
        let beam_max = self.default_beam_max;
        let beam_min = beam_min.min(beam_max);

        LevelBeamParams {
            tau: self
                .level_taus
                .get(idx)
                .copied()
                .flatten()
                .or(self.default_tau),
            beam_min,
            beam_max,
        }
    }
}

pub fn effective_beam(
    sorted: &[(NodeId, f32)],
    tau: Option<f64>,
    beam_min: usize,
    beam_max: usize,
) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    match tau {
        None => beam_min.min(sorted.len()),
        Some(tau) => {
            let d_best = sorted[0].1.max(1e-10_f32);
            let threshold = d_best * (tau as f32);
            let count = sorted.iter().take_while(|(_, d)| *d <= threshold).count();
            let floor = beam_min.min(beam_max);
            count.clamp(floor, beam_max).min(sorted.len())
        }
    }
}

impl TreeNode {
    pub fn centroid(&self) -> &[f32] {
        match self {
            TreeNode::Leaf(l) => &l.centroid,
            TreeNode::Internal(i) => &i.centroid,
        }
    }

    pub fn centroid_code(&self) -> &[u8] {
        match self {
            TreeNode::Leaf(l) => &l.centroid_code,
            TreeNode::Internal(i) => &i.centroid_code,
        }
    }

    pub fn set_parent_id(&mut self, parent: Option<NodeId>) {
        match self {
            TreeNode::Leaf(l) => l.parent_id = parent,
            TreeNode::Internal(i) => i.parent_id = parent,
        }
    }
}

pub fn code_slice(codes: &[u8], index: usize, code_size: usize) -> &[u8] {
    let start = index * code_size;
    &codes[start..start + code_size]
}