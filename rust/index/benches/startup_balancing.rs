//! Choose add intervals while a fresh tree gains independent balancing work.

pub const STARTUP_VECTORS: usize = 1_000_000;
const MIN_ADD_SIZE: usize = 5_000;
const MAX_ADD_SIZE: usize = 100_000;

pub struct StartupBalanceSchedule {
    pub enabled: bool,
    pub checkpoint_size: usize,
    pub split_threshold: usize,
    pub fresh: bool,
}

impl StartupBalanceSchedule {
    pub fn is_adaptive(&self, indexed: usize) -> bool {
        self.enabled && self.fresh && indexed < STARTUP_VECTORS
    }

    /// Estimate enough additions to fill each leaf by one split threshold.
    /// Bounds limit repeated balance overhead and overly large initial splits.
    pub fn next_len(&self, indexed: usize, remaining: usize, leaf_count: usize) -> usize {
        if remaining == 0 || indexed >= STARTUP_VECTORS {
            return remaining;
        }
        let adaptive = self.is_adaptive(indexed);
        // Default and resumed sessions retain whole small checkpoints, even
        // when they cross the first-million boundary.
        if !adaptive && self.checkpoint_size <= MAX_ADD_SIZE {
            return remaining;
        }
        let interval = if adaptive {
            leaf_count
                .max(1)
                .saturating_mul(self.split_threshold)
                .clamp(MIN_ADD_SIZE, MAX_ADD_SIZE)
        } else {
            MAX_ADD_SIZE
        };
        interval.min(remaining).min(STARTUP_VECTORS - indexed)
    }
}
