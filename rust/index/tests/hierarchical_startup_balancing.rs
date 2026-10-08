#![recursion_limit = "256"]
#[path = "../benches/hierarchical_index/mod.rs"]
mod hierarchical_index;
#[path = "../benches/startup_balancing.rs"]
mod startup_balancing;

use chroma_distance::DistanceFunction;
use hierarchical_index::config::HierarchicalSpannConfig;
use hierarchical_index::writer::HierarchicalSpannWriter;
use startup_balancing::{StartupBalancePolicy, StartupBalanceSchedule, STARTUP_VECTORS};
use std::sync::Arc;

fn schedule(policy: StartupBalancePolicy, checkpoint_size: usize) -> StartupBalanceSchedule {
    StartupBalanceSchedule {
        policy,
        min_size: 5_000,
        max_size: 100_000,
        checkpoint_size,
        split_threshold: 2_048,
        fresh: true,
    }
}

fn plan(schedule: &StartupBalanceSchedule, mut indexed: usize, mut remaining: usize) -> Vec<usize> {
    let mut lengths = Vec::new();
    while remaining > 0 {
        let len = schedule.next_len(indexed, remaining, 1);
        assert!(len > 0);
        lengths.push(len);
        indexed += len;
        remaining -= len;
    }
    lengths
}

#[test]
fn default_fixed_schedule_matches_existing_checkpoint_and_startup_boundaries() {
    assert_eq!(
        plan(
            &schedule(StartupBalancePolicy::Fixed, 1_000_000),
            0,
            1_000_000
        ),
        vec![100_000; 10]
    );
    assert_eq!(
        plan(&schedule(StartupBalancePolicy::Fixed, 100_000), 0, 100_000),
        [100_000]
    );
    assert_eq!(
        plan(
            &schedule(StartupBalancePolicy::Fixed, 100_000),
            990_000,
            100_000
        ),
        [100_000]
    );
    assert_eq!(
        plan(
            &schedule(StartupBalancePolicy::Fixed, 500_000),
            900_000,
            500_000
        ),
        [100_000, 400_000]
    );
    assert_eq!(
        plan(
            &schedule(StartupBalancePolicy::Fixed, 500_000),
            STARTUP_VECTORS,
            500_000
        ),
        [500_000]
    );
    assert_eq!(
        plan(&schedule(StartupBalancePolicy::Fixed, 50_000), 0, 50_000),
        [50_000]
    );
}

#[test]
fn fixed_intervals_support_the_proposed_frequency_comparison() {
    for interval in [5_000, 10_000, 50_000, 100_000] {
        let mut config = schedule(StartupBalancePolicy::Fixed, 100_000);
        config.max_size = interval;
        assert_eq!(
            plan(&config, 0, 100_000),
            vec![interval; 100_000 / interval]
        );
    }
}

#[test]
fn adaptive_interval_tracks_leaf_capacity_with_explicit_bounds() {
    let config = schedule(StartupBalancePolicy::Adaptive, 1_000_000);
    for (leaves, expected) in [
        (0, 5_000),
        (1, 5_000),
        (2, 5_000),
        (4, 8_192),
        (16, 32_768),
        (64, 100_000),
    ] {
        assert_eq!(config.next_len(0, 1_000_000, leaves), expected);
    }
    assert_eq!(config.next_len(0, 1_000_000, usize::MAX), 100_000);
    assert_eq!(config.next_len(STARTUP_VECTORS - 17, 100_000, 64), 17);
    assert_eq!(config.next_len(0, 17, 64), 17);
    assert_eq!(config.next_len(0, 0, 64), 0);
    assert_eq!(config.next_len(STARTUP_VECTORS, 1_000_000, 64), 1_000_000);
}

#[test]
fn resumed_or_established_indexes_do_not_use_adaptive_startup() {
    let mut config = schedule(StartupBalancePolicy::Adaptive, 1_000_000);
    config.fresh = false;
    assert!(!config.is_adaptive(0));
    assert_eq!(config.next_len(0, 1_000_000, 1), 100_000);
    config.fresh = true;
    assert!(!config.is_adaptive(STARTUP_VECTORS));
    assert_eq!(config.next_len(STARTUP_VECTORS, 1_000_000, 1), 1_000_000);
}

#[test]
fn invalid_intervals_fail_before_an_add_loop_can_stall() {
    let mut config = schedule(StartupBalancePolicy::Adaptive, 1_000_000);
    config.max_size = 0;
    assert!(config.validate().is_err());
    config.max_size = 1_000;
    assert!(config.validate().is_err());
    config.min_size = 0;
    assert!(config.validate().is_err());
    config.min_size = 500;
    assert!(config.validate().is_ok());
    config.checkpoint_size = 0;
    assert!(config.validate().is_err());
}

#[test]
fn adaptive_add_boundaries_preserve_all_ids_after_joined_balances() {
    let mut writer = HierarchicalSpannWriter::new(
        32,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig {
            split_threshold: 8,
            merge_threshold: 0,
            ..Default::default()
        },
    );
    let config = StartupBalanceSchedule {
        policy: StartupBalancePolicy::Adaptive,
        min_size: 16,
        max_size: 64,
        checkpoint_size: 100,
        split_threshold: 8,
        fresh: true,
    };
    let vectors: Vec<(u32, Arc<[f32]>)> = (0..100)
        .map(|id| (id, Arc::from(vec![id as f32 * 0.1; 32])))
        .collect();
    let mut indexed = 0;
    let mut batches = 0;
    while indexed < vectors.len() {
        let take = config.next_len(indexed, vectors.len() - indexed, writer.leaf_count());
        writer.add_batch_buffered(&vectors[indexed..indexed + take], 2, || {});
        writer.balance_index_parallel(2);
        indexed += take;
        batches += 1;
        assert_eq!(
            writer.root_reachable_valid_ids().unwrap(),
            (0..indexed as u32).collect()
        );
    }
    assert!(batches > 1);
    assert_eq!(indexed, 100);
}
