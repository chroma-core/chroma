//! Choose add intervals while a fresh tree gains independent balancing work.

use clap::ValueEnum;

pub const STARTUP_VECTORS: usize = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum StartupBalancePolicy {
    Fixed,
    Adaptive,
}

pub struct StartupBalanceSchedule {
    pub policy: StartupBalancePolicy,
    pub min_size: usize,
    pub max_size: usize,
    pub checkpoint_size: usize,
    pub split_threshold: usize,
    pub fresh: bool,
}

impl StartupBalanceSchedule {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_size == 0 || self.checkpoint_size == 0 {
            return Err("startup balance size and checkpoint size must be positive".into());
        }
        if self.policy == StartupBalancePolicy::Adaptive
            && (self.min_size == 0 || self.min_size > self.max_size)
        {
            return Err(
                "adaptive startup minimum must be positive and no larger than its maximum".into(),
            );
        }
        Ok(())
    }

    pub fn is_adaptive(&self, indexed: usize) -> bool {
        self.fresh && self.policy == StartupBalancePolicy::Adaptive && indexed < STARTUP_VECTORS
    }

    /// Uniform routing would add about one split-threshold of rows per leaf.
    /// Bound that estimate because skewed routing and balance overhead can make
    /// either very small or very large add intervals expensive.
    pub fn next_len(&self, indexed: usize, remaining: usize, leaf_count: usize) -> usize {
        if remaining == 0 || indexed >= STARTUP_VECTORS {
            return remaining;
        }
        let adaptive = self.is_adaptive(indexed);
        // Match the existing fixed cadence: small checkpoints need no extra
        // split at the startup boundary, and resume keeps the fixed schedule.
        if !adaptive && self.checkpoint_size <= self.max_size {
            return remaining;
        }
        let interval = if adaptive {
            leaf_count
                .max(1)
                .saturating_mul(self.split_threshold)
                .clamp(self.min_size, self.max_size)
        } else {
            self.max_size
        };
        interval.min(remaining).min(STARTUP_VECTORS - indexed)
    }
}
