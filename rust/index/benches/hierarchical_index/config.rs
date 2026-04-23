#[derive(Clone, Copy, PartialEq, Eq)]
pub enum NavigationMode {
    Fp,
    FourBit,
}

impl std::fmt::Display for NavigationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NavigationMode::Fp => write!(f, "fp"),
            NavigationMode::FourBit => write!(f, "4bit"),
        }
    }
}

impl std::fmt::Debug for NavigationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

#[derive(Clone)]
pub struct HierarchicalSpannConfig {
    pub branching_factor: usize,
    pub split_threshold: usize,
    pub merge_threshold: usize,
    /// Dynamic beam tau for the write path (add/reassign/merge navigate).
    pub write_beam_tau: f64,
    pub write_beam_min: usize,
    pub write_beam_max: usize,
    pub write_level_taus: Vec<Option<f64>>,
    pub write_level_min_pcts: Vec<f64>,
    /// Dynamic beam tau for the search/query path.
    /// Include children with dist <= d_best * (1 + beam_tau), clamped to [beam_min, beam_max].
    pub beam_tau: f64,
    pub beam_min: usize,
    pub beam_max: usize,
    pub max_replicas: usize,
    pub write_rng_epsilon: f32,
    pub write_rng_factor: f32,
    pub reassign_neighbor_count: usize,
    pub write_navigation: NavigationMode,
    /// If true, NPA uses full precision f32 distances; if false, NPA uses quantized distances.
    pub fp_npa: bool,
}

impl Default for HierarchicalSpannConfig {
    fn default() -> Self {
        Self {
            branching_factor: 100,
            split_threshold: 2048,
            merge_threshold: 512,
            write_beam_tau: 1.5,
            write_beam_min: 10,
            write_beam_max: 16,
            write_level_taus: Vec::new(),
            write_level_min_pcts: Vec::new(),
            beam_tau: 2.0,
            beam_min: 10,
            beam_max: 256,
            max_replicas: 1,
            write_rng_epsilon: 0.0,
            write_rng_factor: 1.0,
            reassign_neighbor_count: 32,
            write_navigation: NavigationMode::Fp,
            fp_npa: true,
        }
    }
}
