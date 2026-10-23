//! Exclusive elapsed-time accounting for the benchmark driver.
//!
//! Only the driver changes the active phase. Worker task timers remain separate:
//! they overlap and nest, so their sum is neither wall time nor CPU time.

use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Phase {
    Outer,
    Load,
    Add,
    Preflight,
    Balance,
    CommitFork,
    CommitFlush,
    Reopen,
    Metadata,
    Diagnostics,
    Validation,
    ReaderCleanup,
    WriterDrop,
    InputDrop,
    Gc,
    Snapshot,
    Output,
    SummaryLoad,
    SummaryStats,
}

const LABELS: [&str; 19] = [
    "Outer-loop setup and bookkeeping",
    "Load input vectors",
    "Add vectors after preflight",
    "Serial insertion preflight",
    "Balance",
    "Commit: fork",
    "Commit: flush",
    "Reopen writer",
    "Save checkpoint metadata",
    "Checkpoint memory and statistics diagnostics",
    "Posting validation",
    "Clear reader pins",
    "Drop old writer",
    "Drop input batch",
    "Garbage collection",
    "Retain checkpoint snapshot",
    "Checkpoint output and stdout flushing",
    "Final diagnostics: load all postings",
    "Final diagnostics: tree statistics",
];

pub struct BuildWallClock {
    start: Instant,
    boundary: Instant,
    phase: Phase,
    totals: [Duration; LABELS.len()],
    checkpoints: Vec<(usize, [Duration; LABELS.len()])>,
}

impl BuildWallClock {
    pub fn new(start: Instant) -> Self {
        Self {
            start,
            boundary: start,
            phase: Phase::Outer,
            totals: [Duration::ZERO; LABELS.len()],
            checkpoints: Vec::new(),
        }
    }

    pub fn enter(&mut self, phase: Phase) {
        self.enter_at(phase, Instant::now());
    }

    fn enter_at(&mut self, phase: Phase, now: Instant) {
        self.totals[self.phase as usize] += now.duration_since(self.boundary);
        self.boundary = now;
        self.phase = phase;
    }

    pub fn checkpoint_start(&mut self) -> [Duration; LABELS.len()] {
        self.enter(Phase::Outer);
        self.totals
    }

    /// Read a completed phase from the same intervals used by the wall report.
    /// The caller must leave the phase before reading its duration.
    pub fn elapsed_since(&self, phase: Phase, before: &[Duration; LABELS.len()]) -> Duration {
        self.totals[phase as usize] - before[phase as usize]
    }

    /// Reclassify a measured part of a completed phase without changing the
    /// total elapsed time. The caller must leave the enclosing phase first.
    pub fn reclassify_completed(&mut self, from: Phase, to: Phase, duration: Duration) {
        self.totals[from as usize] = self.totals[from as usize]
            .checked_sub(duration)
            .expect("subphase exceeds enclosing wall interval");
        self.totals[to as usize] += duration;
    }

    /// Store checkpoint durations without writing to stdout during the build.
    pub fn record_checkpoint(&mut self, checkpoint: usize, before: [Duration; LABELS.len()]) {
        self.enter(Phase::Outer);
        let delta = std::array::from_fn(|i| self.totals[i] - before[i]);
        self.checkpoints.push((checkpoint, delta));
    }

    /// Freeze the same boundary used by the build summary. Formatting this
    /// report and subsequent recall evaluation occur after that boundary.
    pub fn finish(mut self) -> Duration {
        let end = Instant::now();
        self.enter_at(Phase::Outer, end);
        let elapsed = end.duration_since(self.start);
        let accounted: Duration = self.totals.iter().sum();
        assert_eq!(
            accounted, elapsed,
            "wall-time intervals must partition the build"
        );
        for (checkpoint, totals) in &self.checkpoints {
            print_totals(&format!("Checkpoint {checkpoint} wall time"), totals);
        }
        print_totals("Build wall time", &self.totals);
        println!(
            "Build wall time reconciliation: measured={:.6}s accounted={:.6}s difference={:.6}s",
            elapsed.as_secs_f64(),
            accounted.as_secs_f64(),
            elapsed.abs_diff(accounted).as_secs_f64()
        );
        println!("Worker task durations overlap and nest; their sum is neither wall time nor CPU time. Recall and wall-time report formatting follow the build boundary.");
        elapsed
    }
}

fn print_totals(title: &str, totals: &[Duration; LABELS.len()]) {
    println!("--- {title} (exclusive seconds) ---");
    for (label, duration) in LABELS.iter().zip(totals) {
        println!("  {label}: {:.6}s", duration.as_secs_f64());
    }
    println!(
        "  Total: {:.6}s",
        totals.iter().sum::<Duration>().as_secs_f64()
    );
}

#[cfg(test)]
mod tests {

    #[test]
    fn interrupted_add_and_balance_partition_wall_time() {
        let start = super::Instant::now();
        let mut clock = super::BuildWallClock::new(start);
        let before = clock.totals;
        for (seconds, phase) in [
            (2, super::Phase::Add),
            (5, super::Phase::Balance),
            (12, super::Phase::Add),
            (16, super::Phase::SummaryLoad),
            (21, super::Phase::Outer),
        ] {
            clock.enter_at(phase, start + super::Duration::from_secs(seconds));
        }
        assert_eq!(
            clock.elapsed_since(super::Phase::Add, &before),
            super::Duration::from_secs(7)
        );
        assert_eq!(
            clock.elapsed_since(super::Phase::Balance, &before),
            super::Duration::from_secs(7)
        );
        assert_eq!(
            clock.totals[super::Phase::SummaryLoad as usize],
            super::Duration::from_secs(5)
        );
        assert_eq!(
            clock.totals.iter().sum::<super::Duration>(),
            super::Duration::from_secs(21)
        );
        assert_eq!(
            clock.totals[super::Phase::Snapshot as usize],
            super::Duration::ZERO
        );
    }

    #[test]
    fn reclassifying_preflight_preserves_exclusive_wall_totals() {
        let start = super::Instant::now();
        let mut clock = super::BuildWallClock::new(start);
        let before = clock.totals;
        clock.enter_at(super::Phase::Add, start);
        clock.enter_at(super::Phase::Outer, start + super::Duration::from_secs(10));
        clock.reclassify_completed(
            super::Phase::Add,
            super::Phase::Preflight,
            super::Duration::from_secs(3),
        );
        clock.enter_at(super::Phase::Add, start + super::Duration::from_secs(11));
        clock.enter_at(super::Phase::Outer, start + super::Duration::from_secs(16));
        clock.reclassify_completed(
            super::Phase::Add,
            super::Phase::Preflight,
            super::Duration::from_secs(2),
        );
        assert_eq!(
            clock.elapsed_since(super::Phase::Preflight, &before),
            super::Duration::from_secs(5)
        );
        assert_eq!(
            clock.elapsed_since(super::Phase::Add, &before),
            super::Duration::from_secs(10)
        );
        assert_eq!(
            clock.totals.iter().sum::<super::Duration>(),
            super::Duration::from_secs(16)
        );
    }

    #[test]
    fn checkpoint_reports_are_buffered_until_finish() {
        let mut clock = super::BuildWallClock::new(super::Instant::now());
        let before = clock.checkpoint_start();
        clock.enter(super::Phase::Add);
        clock.enter(super::Phase::Outer);
        clock.record_checkpoint(1, before);
        assert_eq!(clock.checkpoints.len(), 1);
        assert_eq!(clock.checkpoints[0].0, 1);
        assert_eq!(
            clock.checkpoints[0].1[super::Phase::Add as usize],
            clock.elapsed_since(super::Phase::Add, &before)
        );
    }

    #[test]
    fn checkpoint_deltas_do_not_recount_prior_work() {
        let start = super::Instant::now();
        let mut clock = super::BuildWallClock::new(start);
        clock.enter_at(super::Phase::Load, start);
        clock.enter_at(super::Phase::Outer, start + super::Duration::from_secs(4));
        let before = clock.totals;
        clock.enter_at(super::Phase::Gc, start + super::Duration::from_secs(5));
        clock.enter_at(
            super::Phase::Snapshot,
            start + super::Duration::from_secs(7),
        );
        clock.enter_at(super::Phase::Outer, start + super::Duration::from_secs(10));
        let delta: super::Duration = clock
            .totals
            .iter()
            .zip(before)
            .map(|(after, before)| *after - before)
            .sum();
        assert_eq!(delta, super::Duration::from_secs(6));
        assert_eq!(
            clock.totals[super::Phase::Load as usize] - before[super::Phase::Load as usize],
            super::Duration::ZERO
        );
    }
}
