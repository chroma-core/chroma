//! Model-based property test for the MaxScore sparse index.
//!
//! Each proptest case generates a small random program — a sequence of
//! commit batches (adds / overwrites / deletes across 1-4 dimensions and
//! a bounded offset space) interleaved with a few on-disk faults — and
//! runs it against both the real engine (MaxScoreWriter/MaxScoreReader
//! over a blockfile, forked and flushed per commit like compaction) and
//! an in-memory oracle (per dimension, a `BTreeMap<offset, weight>`).
//!
//! Invariants checked after every step:
//!   1. Healthy (version-1) dimensions: `count_postings` and the stored
//!      directory count equal the oracle's exact df.
//!   2. Degraded dimensions (legacy v0 downgrade, corrupt count stamp):
//!      `count_postings` returns without error; the value is known to be
//!      inexact, so no equality — but the next commit that touches the
//!      dimension must converge it back to the exact df (migration for
//!      v0, underflow-recount self-heal for corrupt counts).
//!   3. Corrupt (undecodable) directories: reads and queries skip the
//!      dimension without error; the next touching commit rebuilds it
//!      from that commit's delta alone, after which the directory must
//!      decode and store exactly the delta's surviving postings.
//!   4. Query correctness: for every dimension with a live directory, a
//!      single-term query at k in {1, 3, 10} returns exactly the
//!      oracle's exhaustive top-k. Weights are distinct and exactly
//!      f16-representable (blocks store f16), so the ground truth is
//!      tie-free and round-trips the index losslessly.
//!   5. Stored postings match the oracle entry-for-entry, and nothing
//!      panics.
//!
//! Known exclusion: rebuilding past a corrupt directory can strand stale
//! higher-seq posting blocks on disk, which later reads or commits may
//! resurrect (parked issue). After asserting the rebuild's own outcome,
//! the dimension is retired from the model — no further ops or
//! assertions target it.
//!
//! Program modes: 3/4 of cases are *mixed* (adds / overwrites / deletes
//! over a revisited offset space, interleaved with faults); 1/4 are
//! *append-only* (every commit strictly above the global high-water
//! mark), deliberately generating the compaction geometry that strands
//! partial interior posting blocks — see `arb_append_only_program`.
//!
//! A second proptest fn (`maxscore_pruning_matches_exhaustive_oracle`)
//! covers multi-window, multi-term programs where the engine's window
//! skip and budget pruning actually execute; see the "Pruning variant"
//! section below. Known failure geometries are additionally pinned as
//! deterministic `pinned_*` tests — see the "Pinned regression
//! programs" section.
//!
//! Determinism: the oracle is BTreeMap-based, weights derive from a
//! per-program counter, and the interpreter branches only on generated
//! values plus deterministic engine state, so every case replays
//! byte-identically from its seed. Failing seeds persist via proptest's
//! standard `proptest-regressions` convention.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;

use crate::common;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, test_arrow_blockfile_provider, BlockfileWriterOptions,
};
use chroma_index::sparse::maxscore::{
    MaxScoreReader, MaxScoreWriter, SPARSE_POSTING_BLOCK_SIZE_BYTES,
};
use chroma_index::sparse::types::encode_u32;
use chroma_types::{
    Directory, DirectoryBlock, SignedRoaringBitmap, SparsePostingBlock, DIRECTORY_PREFIX,
};

/// Offsets are drawn from [0, MAX_OFFSET) so programs revisit the same
/// postings often enough to exercise overwrites, deletes, and suffix
/// rewrites rather than degenerating into pure appends.
const MAX_OFFSET: u32 = 64;
const MAX_OPS_PER_COMMIT: usize = 16;
const MAX_FAULTS_PER_CASE: usize = 3;
const QUERY_KS: [u32; 3] = [1, 3, 10];

/// Distinct weight, exactly representable in f16 (same construction as
/// the ms_99 repro): j = (i * 5741) mod 9216 is injective for i < 9216
/// (gcd(5741, 9216) = 1), and 2^(j/1024) * (1 + (j%1024)/1024) walks
/// steps of 2^-10 within a binade in [1, 512) — all exact f16 values,
/// all distinct. Posting blocks store f16, so oracle weights round-trip
/// the index losslessly and single-term rankings are tie-free.
fn weight_for(i: u32) -> f32 {
    assert!(i < 9216, "weight counter exhausted the injective range");
    let j = (i as u64 * 5741) % 9216;
    let binade = (j / 1024) as i32;
    let step = (j % 1024) as f32;
    let weight = 2f32.powi(binade) * (1.0 + step / 1024.0);
    debug_assert_eq!(
        f32::from(half::f16::from_f32(weight)),
        weight,
        "weight must be exactly f16-representable"
    );
    weight
}

fn exclude_none() -> SignedRoaringBitmap {
    SignedRoaringBitmap::Exclude(Default::default())
}

// ── Generated program ────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum FaultKind {
    /// Rewrite the dimension's directory as version 0 (no posting
    /// count), as a pre-count writer would during a rolling downgrade.
    LegacyDowngrade,
    /// Stamp a wrong (too-low) posting count onto the directory, as a
    /// buggy past writer might have persisted.
    CorruptCount,
    /// Plant undecodable bytes (a plain posting block) at the directory
    /// key, modeling on-disk corruption.
    CorruptDirectory,
}

#[derive(Debug, Clone, Copy)]
struct OpSpec {
    dim: u32,
    offset: u32,
    delete: bool,
}

#[derive(Debug, Clone)]
enum StepSpec {
    Commit {
        ops: Vec<OpSpec>,
    },
    Fault {
        dim: u32,
        kind: FaultKind,
        seed: u32,
    },
}

#[derive(Debug, Clone)]
struct Program {
    num_dims: u32,
    block_size: u32,
    /// Diagnostic tag: true when generated by `arb_append_only_program`
    /// (fresh monotonically-increasing offsets only — the compaction
    /// geometry that strands partial interior blocks).
    append_only: bool,
    steps: Vec<StepSpec>,
}

fn arb_fault_kind() -> impl Strategy<Value = FaultKind> {
    prop_oneof![
        Just(FaultKind::LegacyDowngrade),
        Just(FaultKind::CorruptCount),
        Just(FaultKind::CorruptDirectory),
    ]
}

fn arb_op(num_dims: u32) -> impl Strategy<Value = OpSpec> {
    (0..num_dims, 0..MAX_OFFSET, proptest::bool::weighted(0.25)).prop_map(
        |(dim, offset, delete)| OpSpec {
            dim,
            offset,
            delete,
        },
    )
}

fn arb_step(num_dims: u32) -> impl Strategy<Value = StepSpec> {
    prop_oneof![
        4 => proptest::collection::vec(arb_op(num_dims), 1..=MAX_OPS_PER_COMMIT)
            .prop_map(|ops| StepSpec::Commit { ops }),
        1 => (0..num_dims, arb_fault_kind(), any::<u32>())
            .prop_map(|(dim, kind, seed)| StepSpec::Fault { dim, kind, seed }),
    ]
}

fn arb_mixed_program() -> impl Strategy<Value = Program> {
    (1u32..=4u32, 2u32..=8u32).prop_flat_map(|(num_dims, block_size)| {
        proptest::collection::vec(arb_step(num_dims), 3..=10).prop_map(move |steps| Program {
            num_dims,
            block_size,
            append_only: false,
            steps,
        })
    })
}

/// Strictly append-only program: every commit writes only fresh offsets
/// strictly above the global high-water mark — no updates or deletes
/// below it, no faults. This is the compaction geometry that strands
/// partial interior posting blocks: a pure above-max append leaves the
/// previous (usually partial) last block untouched, turning it into a
/// partial *interior* block. The mixed generator almost never produces
/// this shape organically — offsets are uniform over [0, 64), so a
/// commit landing entirely above the running max is exponentially
/// unlikely in its op count (measured ~1.6e-3 of programs, i.e. ~0.4
/// expected per 256-case run, and those are dominated by degenerate 1-2
/// op commits) — so it is generated deliberately here. The count
/// invariant (stored == oracle df) over these programs is exactly the
/// regression for the original `count_postings` estimate bug, which
/// assumed only the last block could be partial.
fn arb_append_only_program() -> impl Strategy<Value = Program> {
    (1u32..=4u32, 2u32..=8u32).prop_flat_map(|(num_dims, block_size)| {
        proptest::collection::vec(
            proptest::collection::vec((0..num_dims, 1u32..=4u32), 1..=MAX_OPS_PER_COMMIT),
            2..=8,
        )
        .prop_map(move |commits| {
            let mut offset = 0u32;
            let steps = commits
                .into_iter()
                .map(|gaps| {
                    let ops = gaps
                        .into_iter()
                        .map(|(dim, gap)| {
                            offset += gap;
                            OpSpec {
                                dim,
                                offset,
                                delete: false,
                            }
                        })
                        .collect();
                    StepSpec::Commit { ops }
                })
                .collect();
            Program {
                num_dims,
                block_size,
                append_only: true,
                steps,
            }
        })
    })
}

fn arb_program() -> impl Strategy<Value = Program> {
    prop_oneof![
        3 => arb_mixed_program(),
        1 => arb_append_only_program(),
    ]
}

// ── Interpreter state ────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum Outstanding {
    /// Directory downgraded to version 0: count degrades to an estimate.
    Legacy,
    /// Directory stamped with a too-low count. `forced_offset` is an
    /// existing posting past block 0 that the next touching commit is
    /// forced to overwrite, so the rewritten suffix always holds more
    /// old entries than the corrupt count — guaranteeing the commit hits
    /// the underflow-recount self-heal path rather than laundering the
    /// bad count through prefix arithmetic.
    CorruptCount { forced_offset: u32 },
    /// Directory bytes undecodable: the dimension must be skipped.
    CorruptDirectory,
}

#[derive(Debug, Clone, Copy, Default)]
struct DimState {
    fault: Option<Outstanding>,
    /// Set for the single invariant pass right after a commit rebuilds
    /// the dimension past a corrupt directory; the dimension is retired
    /// immediately afterwards (see module docs, known exclusion).
    just_rebuilt: bool,
    retired: bool,
}

/// Rewrite a dimension's directory key through the raw blockfile (fork,
/// overwrite part 0, commit, flush, reopen) — the ms_17/ms_18 fault
/// pattern.
async fn plant_directory_block(
    provider: &BlockfileProvider,
    reader: &MaxScoreReader<'static>,
    dim: u32,
    stored: SparsePostingBlock,
) -> MaxScoreReader<'static> {
    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES)
                .fork(reader.posting_id()),
        )
        .await
        .unwrap();
    let dir_prefix = format!("{}{}", DIRECTORY_PREFIX, encode_u32(dim));
    posting_writer
        .set(dir_prefix.as_str(), 0u32, stored)
        .await
        .unwrap();
    let flusher = posting_writer
        .commit::<u32, SparsePostingBlock>()
        .await
        .unwrap();
    let posting_id = flusher.id();
    flusher.flush::<u32, SparsePostingBlock>().await.unwrap();
    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(posting_id, "".to_string()))
        .await
        .unwrap();
    let reader = MaxScoreReader::new(posting_reader);
    // SAFETY: same pattern as common::commit_writer — the provider and
    // TempDir outlive the reader for the whole program run.
    unsafe { std::mem::transmute::<MaxScoreReader<'_>, MaxScoreReader<'static>>(reader) }
}

async fn check_entries(
    reader: &MaxScoreReader<'static>,
    dim: u32,
    expected: &BTreeMap<u32, f32>,
    step_idx: usize,
) {
    let actual: BTreeMap<u32, f32> = common::get_all_entries(reader, dim)
        .await
        .into_iter()
        .collect();
    assert_eq!(
        &actual, expected,
        "step {step_idx} dim {dim}: stored postings diverge from oracle"
    );
}

async fn check_query(
    reader: &MaxScoreReader<'static>,
    dim: u32,
    expected: &BTreeMap<u32, f32>,
    step_idx: usize,
) {
    // Oracle exhaustive ranking: with a single-term weight-1.0 query, a
    // doc's dot-product contribution is exactly its stored weight, and
    // weights are globally distinct, so descending weight is the unique
    // top-k order (matching TopKHeap without exercising tie-breaks).
    let mut ranked: Vec<(u32, f32)> = expected.iter().map(|(&off, &w)| (off, w)).collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1));
    for k in QUERY_KS {
        let results = reader
            .query(vec![(dim, 1.0)], k, exclude_none())
            .await
            .expect("query over a live directory must not error");
        let want = &ranked[..ranked.len().min(k as usize)];
        let got: Vec<u32> = results.iter().map(|s| s.offset).collect();
        let want_offsets: Vec<u32> = want.iter().map(|&(off, _)| off).collect();
        assert_eq!(
            got, want_offsets,
            "step {step_idx} dim {dim} k={k}: engine top-k diverges from oracle"
        );
        for (score, &(_, weight)) in results.iter().zip(want) {
            assert!(
                (score.score - weight).abs() <= weight.abs() * 1e-3,
                "step {step_idx} dim {dim} k={k}: score {} != oracle weight {}",
                score.score,
                weight
            );
        }
    }
}

async fn check_invariants(
    reader: &MaxScoreReader<'static>,
    oracle: &BTreeMap<u32, BTreeMap<u32, f32>>,
    states: &BTreeMap<u32, DimState>,
    step_idx: usize,
) {
    let empty = BTreeMap::new();
    for (&dim, state) in states {
        if state.retired {
            continue;
        }
        let enc = encode_u32(dim);
        let expected = oracle.get(&dim).unwrap_or(&empty);
        let df = expected.len();
        match state.fault {
            Some(Outstanding::CorruptDirectory) => {
                // Invariant 3 (pre-rebuild): the dimension is skipped
                // everywhere, never surfaced as an error.
                assert!(
                    reader.get_directory(&enc).await.unwrap().is_none(),
                    "step {step_idx} dim {dim}: corrupt directory must read as None"
                );
                assert_eq!(
                    reader.count_postings(&enc).await.unwrap(),
                    0,
                    "step {step_idx} dim {dim}: corrupt directory must count as 0"
                );
                let results = reader
                    .query(vec![(dim, 1.0)], 10, exclude_none())
                    .await
                    .expect("query must skip a corrupt directory without error");
                assert!(
                    results.is_empty(),
                    "step {step_idx} dim {dim}: query must skip the corrupt dimension"
                );
            }
            Some(Outstanding::Legacy) | Some(Outstanding::CorruptCount { .. }) => {
                // Invariant 2: the count degrades (v0 estimate / wrong
                // stamp) but must not error; no equality until the next
                // touching commit converges it. Postings and queries are
                // unaffected — only the count metadata is degraded.
                reader
                    .count_postings(&enc)
                    .await
                    .expect("degraded count must not error");
                check_entries(reader, dim, expected, step_idx).await;
                check_query(reader, dim, expected, step_idx).await;
            }
            None => {
                // Invariant 1: exact df, both via the read API and the
                // stored directory stamp.
                assert_eq!(
                    reader.count_postings(&enc).await.unwrap(),
                    df,
                    "step {step_idx} dim {dim}: count_postings != oracle df"
                );
                match reader.get_directory(&enc).await.unwrap() {
                    Some((dir, _)) => {
                        assert!(
                            df > 0,
                            "step {step_idx} dim {dim}: directory exists for an empty dimension"
                        );
                        assert_eq!(
                            dir.posting_count()
                                .expect("healthy directory must store an exact count"),
                            df as u32,
                            "step {step_idx} dim {dim}: stored count != oracle df"
                        );
                    }
                    None => assert_eq!(
                        df, 0,
                        "step {step_idx} dim {dim}: directory missing for a non-empty dimension"
                    ),
                }
                if !state.just_rebuilt {
                    check_entries(reader, dim, expected, step_idx).await;
                    check_query(reader, dim, expected, step_idx).await;
                }
                // just_rebuilt: the count assertions above cover the
                // rebuild delta's own postings; postings/query-level
                // checks are skipped because stale higher-seq blocks may
                // survive the rebuild (known exclusion, module docs).
            }
        }
    }
}

// ── Interpreter ──────────────────────────────────────────────────────

async fn run_program(program: Program) {
    let Program {
        num_dims,
        block_size,
        append_only,
        steps,
    } = program;

    if append_only {
        // Generator self-check: strictly increasing fresh offsets, no
        // deletes, no faults — the stranded-partial-interior-block
        // geometry depends on it.
        let mut high_water = None;
        for step in &steps {
            let StepSpec::Commit { ops } = step else {
                panic!("append-only programs must not contain faults");
            };
            for op in ops {
                assert!(!op.delete, "append-only programs must not delete");
                assert!(
                    high_water.is_none_or(|hw| op.offset > hw),
                    "append-only offsets must strictly increase"
                );
                high_water = Some(op.offset);
            }
        }
    }

    let (_temp_dir, provider) = test_arrow_blockfile_provider(SPARSE_POSTING_BLOCK_SIZE_BYTES);
    let mut reader: Option<MaxScoreReader<'static>> = None;
    let mut oracle: BTreeMap<u32, BTreeMap<u32, f32>> = BTreeMap::new();
    let mut states: BTreeMap<u32, DimState> = (0..num_dims)
        .map(|dim| (dim, DimState::default()))
        .collect();
    let mut weight_counter = 0u32;
    let mut faults_applied = 0usize;

    for (step_idx, step) in steps.into_iter().enumerate() {
        match step {
            StepSpec::Commit { ops } => {
                let mut ops: Vec<OpSpec> = ops
                    .into_iter()
                    .filter(|op| !states[&op.dim].retired)
                    .collect();
                // Force corrupt-count dims touched by this batch to also
                // overwrite a posting past block 0, so the heal path is
                // deterministically reachable (see Outstanding::CorruptCount).
                let touched: BTreeSet<u32> = ops.iter().map(|op| op.dim).collect();
                for &dim in &touched {
                    if let Some(Outstanding::CorruptCount { forced_offset }) = states[&dim].fault {
                        ops.push(OpSpec {
                            dim,
                            offset: forced_offset,
                            delete: false,
                        });
                    }
                }
                if ops.is_empty() && reader.is_none() {
                    continue;
                }

                let prev = reader.clone();
                let writer = match &prev {
                    Some(r) => {
                        common::fork_writer_with_block_size(&provider, r, Some(block_size)).await
                    }
                    None => {
                        let posting_writer = provider
                            .write::<u32, SparsePostingBlock>(
                                BlockfileWriterOptions::new("".to_string())
                                    .ordered_mutations()
                                    .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES),
                            )
                            .await
                            .unwrap();
                        MaxScoreWriter::new(posting_writer, None).with_block_size(block_size)
                    }
                };

                // Interpreter's view of the commit delta: last write per
                // (dim, offset) wins, mirroring the writer's delta map.
                let mut delta: BTreeMap<u32, BTreeMap<u32, Option<f32>>> = BTreeMap::new();
                for op in &ops {
                    if op.delete {
                        writer.delete(op.offset, [op.dim]).await;
                        delta.entry(op.dim).or_default().insert(op.offset, None);
                    } else {
                        let weight = weight_for(weight_counter);
                        weight_counter += 1;
                        writer.set(op.offset, [(op.dim, weight)]).await;
                        delta
                            .entry(op.dim)
                            .or_default()
                            .insert(op.offset, Some(weight));
                    }
                }
                reader = Some(common::commit_writer(&provider, writer).await);

                for (dim, dim_delta) in delta {
                    let state = states.get_mut(&dim).unwrap();
                    if matches!(state.fault, Some(Outstanding::CorruptDirectory)) {
                        // The engine cannot see past the undecodable
                        // directory: it rebuilds the dimension from this
                        // commit's surviving sets alone (deletes have
                        // nothing visible to delete).
                        let survivors: BTreeMap<u32, f32> = dim_delta
                            .iter()
                            .filter_map(|(&off, w)| w.map(|w| (off, w)))
                            .collect();
                        if survivors.is_empty() {
                            // Nothing survives the delta: the engine
                            // writes no directory, so the corrupt one
                            // stays on disk and the fault persists.
                            continue;
                        }
                        oracle.insert(dim, survivors);
                        state.fault = None;
                        state.just_rebuilt = true;
                    } else {
                        let entries = oracle.entry(dim).or_default();
                        for (off, weight) in dim_delta {
                            match weight {
                                Some(weight) => {
                                    entries.insert(off, weight);
                                }
                                None => {
                                    entries.remove(&off);
                                }
                            }
                        }
                        // Invariant 2 convergence: any legacy/corrupt
                        // count heals on the first commit touching the
                        // dimension (verified by the check below).
                        state.fault = None;
                    }
                }

                check_invariants(reader.as_ref().unwrap(), &oracle, &states, step_idx).await;

                for state in states.values_mut() {
                    if state.just_rebuilt {
                        state.just_rebuilt = false;
                        state.retired = true;
                    }
                }
            }
            StepSpec::Fault { dim, kind, seed } => {
                if faults_applied >= MAX_FAULTS_PER_CASE {
                    continue;
                }
                let Some(cur) = reader.clone() else {
                    continue;
                };
                // Faults only target healthy dimensions with data; an
                // inapplicable fault is a deterministic no-op.
                if states[&dim].retired || states[&dim].fault.is_some() {
                    continue;
                }
                let df = oracle.get(&dim).map_or(0, |entries| entries.len());
                if df == 0 {
                    continue;
                }
                let enc = encode_u32(dim);
                let (dir, part_count) = cur
                    .get_directory(&enc)
                    .await
                    .unwrap()
                    .expect("healthy dimension with df > 0 must have a directory");
                assert_eq!(part_count, 1, "test-sized directories are single-part");
                match kind {
                    FaultKind::LegacyDowngrade => {
                        let legacy =
                            DirectoryBlock::new(dir.max_offsets(), dir.max_weights()).unwrap();
                        reader = Some(
                            plant_directory_block(&provider, &cur, dim, legacy.into_block()).await,
                        );
                        states.get_mut(&dim).unwrap().fault = Some(Outstanding::Legacy);
                        faults_applied += 1;
                    }
                    FaultKind::CorruptCount => {
                        if dir.num_blocks() < 2 {
                            // Need postings past block 0 so a too-low
                            // count is guaranteed to underflow on heal.
                            continue;
                        }
                        let block0_len = cur.count_posting_entries_below(&enc, 1).await.unwrap();
                        // Every block is non-empty, so >= 1.
                        let past_block0 = df as u64 - block0_len;
                        let wrong = (seed as u64 % past_block0) as u32;
                        let block0_max = dir.max_offsets()[0];
                        let forced_offset = oracle[&dim]
                            .keys()
                            .copied()
                            .find(|&off| off > block0_max)
                            .expect("a second block implies a posting past block 0");
                        let corrupt =
                            Directory::new(dir.max_offsets().to_vec(), dir.max_weights().to_vec())
                                .unwrap()
                                .with_posting_count(wrong)
                                .into_parts(Directory::max_entries_for_block_size(
                                    SPARSE_POSTING_BLOCK_SIZE_BYTES,
                                ))
                                .remove(0);
                        reader = Some(
                            plant_directory_block(&provider, &cur, dim, corrupt.into_block()).await,
                        );
                        states.get_mut(&dim).unwrap().fault =
                            Some(Outstanding::CorruptCount { forced_offset });
                        faults_applied += 1;
                    }
                    FaultKind::CorruptDirectory => {
                        // A plain posting block at the directory key fails
                        // DirectoryBlock decoding (ms_18 pattern).
                        let junk = SparsePostingBlock::from_sorted_entries(&[(0, 1.0)]).unwrap();
                        reader = Some(plant_directory_block(&provider, &cur, dim, junk).await);
                        states.get_mut(&dim).unwrap().fault = Some(Outstanding::CorruptDirectory);
                        faults_applied += 1;
                    }
                }
                check_invariants(reader.as_ref().unwrap(), &oracle, &states, step_idx).await;
            }
        }
    }
}

// ── Pruning variant: multi-window, multi-term programs ───────────────
//
// The invariant programs above draw offsets from [0, 64), which fits a
// single query window: the window loop runs once, the heap threshold
// starts unset, and neither window-skip pruning nor the essential /
// non-essential term partition (budget pruning via filter_competitive)
// ever fires. This variant deliberately generates layouts where they
// do:
//
//   * Offsets span several query windows (the engine's window loop uses
//     WINDOW_WIDTH = 4096; see the mirror constant below), so from the
//     second window on, the heap threshold is live and windows whose
//     upper bound cannot beat it are skipped.
//   * Queries carry 2-4 terms with distinct positive weights, and each
//     dimension holds hundreds of postings whose per-block max weights
//     diverge (posting block_size 32..=128 over a sparse offset space),
//     so per-window term ordering varies and weak terms drop out of the
//     essential prefix — exercising score_candidates + budget pruning.
// Invariant: engine top-k == oracle exhaustive top-k for k in {1, 3,
// 10}, offsets and scores bit-exact. Exactness holds because stored
// weights are f16-exact multiples of 2^-10 in [1, 512), query weights
// are powers of two, and every partial sum stays below 2^12 — so f32
// accumulation is exact in any association order and the oracle's
// (score desc, offset asc) ranking is exactly the engine's (TopKHeap
// admits strictly-better scores only and tie-breaks by ascending
// offset, and candidates arrive in ascending offset order).

/// Mirror of the private `WINDOW_WIDTH` constant in the engine's query
/// loop (rust/index/src/sparse/maxscore.rs). If the engine constant
/// changes, update this so programs still span multiple windows.
const ENGINE_WINDOW_WIDTH: u32 = 4096;
/// Offset space spanning several query windows.
const PRUNING_NUM_WINDOWS: u32 = 5;
const PRUNING_MAX_OFFSET: u32 = ENGINE_WINDOW_WIDTH * PRUNING_NUM_WINDOWS;
/// Arrow block size for the pruning programs. Kept at the production
/// size: shrinking it (e.g. to 4096, to push dimensions past
/// MAX_VIEW_BLOCKS and exercise Lazy cursors) currently trips a
/// pre-existing blockstore bug — `SparseIndex::get_block_ids_range`'s
/// single-prefix fast path returns no blocks when the range's start key
/// lands in an Arrow block whose start delimiter carries an earlier
/// prefix, so the suffix-rewrite commit silently drops all prior
/// postings for such dimensions. Revisit once that is fixed upstream.
const PRUNING_ARROW_BLOCK_SIZE: usize = SPARSE_POSTING_BLOCK_SIZE_BYTES;

#[derive(Debug, Clone)]
struct PruningDimSpec {
    /// Commit 1: initial postings (distinct offsets).
    base: Vec<u32>,
    /// Commit 2: sets — overwrites where they collide with `base`,
    /// fresh postings elsewhere (drawn from the same window band, so
    /// some land above the previous max: append + suffix-rewrite
    /// layouts feed the query path too).
    touch: Vec<u32>,
    /// Commit 2: deletes — hits and no-op misses.
    deletes: Vec<u32>,
    /// Positive query weight for this dimension. Powers of two keep
    /// f32 score accumulation exact (see module comment above).
    query_weight: f32,
}

#[derive(Debug, Clone)]
struct PruningProgram {
    block_size: u32,
    dims: Vec<PruningDimSpec>,
}

/// Each dimension lives in a window *band* (a contiguous span of 1-5
/// query windows) rather than covering the whole offset space: bands
/// make per-window coverage uneven across terms, so some windows see
/// only weak (or no) terms and the threshold-based whole-window skip
/// becomes reachable alongside the essential/non-essential partition
/// (verified via temporary engine counters).
fn arb_pruning_dim() -> impl Strategy<Value = PruningDimSpec> {
    (
        0u32..PRUNING_NUM_WINDOWS,
        1u32..=PRUNING_NUM_WINDOWS,
        prop_oneof![Just(0.5f32), Just(1.0f32), Just(2.0f32)],
    )
        .prop_flat_map(|(band_start, band_len, query_weight)| {
            let lo = band_start * ENGINE_WINDOW_WIDTH;
            let hi = (band_start + band_len).min(PRUNING_NUM_WINDOWS) * ENGINE_WINDOW_WIDTH;
            (
                proptest::collection::btree_set(lo..hi, 150..=500),
                proptest::collection::vec(lo..hi, 0..=60),
                proptest::collection::vec(lo..hi, 0..=30),
            )
                .prop_map(move |(base, touch, deletes)| PruningDimSpec {
                    base: base.into_iter().collect(),
                    touch,
                    deletes,
                    query_weight,
                })
        })
}

fn arb_pruning_program() -> impl Strategy<Value = PruningProgram> {
    (
        32u32..=128u32,
        proptest::collection::vec(arb_pruning_dim(), 2..=4),
    )
        .prop_map(|(block_size, dims)| PruningProgram { block_size, dims })
}

/// Open a fresh or forked writer against the pruning provider, using
/// PRUNING_ARROW_BLOCK_SIZE for the Arrow blocks (single knob for the
/// revisit noted on that constant; common::fork_writer hardcodes the
/// production size).
async fn pruning_writer(
    provider: &BlockfileProvider,
    fork_from: Option<&MaxScoreReader<'static>>,
    block_size: u32,
) -> MaxScoreWriter<'static> {
    let mut options = BlockfileWriterOptions::new("".to_string())
        .ordered_mutations()
        .max_block_size_bytes(PRUNING_ARROW_BLOCK_SIZE);
    if let Some(reader) = fork_from {
        options = options.fork(reader.posting_id());
    }
    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(options)
        .await
        .unwrap();
    MaxScoreWriter::new(posting_writer, fork_from.cloned()).with_block_size(block_size)
}

async fn check_pruning_invariants(
    reader: &MaxScoreReader<'static>,
    oracle: &BTreeMap<u32, BTreeMap<u32, f32>>,
    query: &[(u32, f32)],
    step_idx: usize,
) {
    // Exact df per dimension (same count invariant as the main harness).
    for (&dim, entries) in oracle {
        assert_eq!(
            reader.count_postings(&encode_u32(dim)).await.unwrap(),
            entries.len(),
            "step {step_idx} dim {dim}: count_postings != oracle df"
        );
    }

    // Oracle exhaustive scores. f32 accumulation is exact here (see the
    // variant comment), so scores compare with == and the unique engine
    // ranking is (score desc, offset asc).
    let mut scores: BTreeMap<u32, f32> = BTreeMap::new();
    for &(dim, query_weight) in query {
        if let Some(entries) = oracle.get(&dim) {
            for (&off, &w) in entries {
                *scores.entry(off).or_insert(0.0) += query_weight * w;
            }
        }
    }
    let mut ranked: Vec<(u32, f32)> = scores.into_iter().collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));

    for k in QUERY_KS {
        let results = reader
            .query(query.to_vec(), k, exclude_none())
            .await
            .expect("multi-term query must not error");
        let want = &ranked[..ranked.len().min(k as usize)];
        let got: Vec<(u32, f32)> = results.iter().map(|s| (s.offset, s.score)).collect();
        assert_eq!(
            got,
            want.to_vec(),
            "step {step_idx} k={k}: engine top-k diverges from oracle"
        );
    }
}

async fn run_pruning_program(program: PruningProgram) {
    let (_temp_dir, provider) = test_arrow_blockfile_provider(PRUNING_ARROW_BLOCK_SIZE);
    let mut oracle: BTreeMap<u32, BTreeMap<u32, f32>> = BTreeMap::new();
    let mut weight_counter = 0u32;
    let query: Vec<(u32, f32)> = program
        .dims
        .iter()
        .enumerate()
        .map(|(dim, spec)| (dim as u32, spec.query_weight))
        .collect();

    // Commit 1: base postings.
    let writer = pruning_writer(&provider, None, program.block_size).await;
    for (dim, spec) in program.dims.iter().enumerate() {
        let dim = dim as u32;
        for &off in &spec.base {
            let weight = weight_for(weight_counter);
            weight_counter += 1;
            writer.set(off, [(dim, weight)]).await;
            oracle.entry(dim).or_default().insert(off, weight);
        }
    }
    let reader = common::commit_writer(&provider, writer).await;
    check_pruning_invariants(&reader, &oracle, &query, 0).await;

    // Commit 2: overwrites, fresh appends, deletes (suffix rewrite at
    // multi-window scale). Sets are applied before deletes in both the
    // engine delta and the oracle, so colliding ops resolve identically.
    let writer = pruning_writer(&provider, Some(&reader), program.block_size).await;
    for (dim, spec) in program.dims.iter().enumerate() {
        let dim = dim as u32;
        let entries = oracle.entry(dim).or_default();
        for &off in &spec.touch {
            let weight = weight_for(weight_counter);
            weight_counter += 1;
            writer.set(off, [(dim, weight)]).await;
            entries.insert(off, weight);
        }
        for &off in &spec.deletes {
            writer.delete(off, [dim]).await;
            entries.remove(&off);
        }
    }
    let reader = common::commit_writer(&provider, writer).await;
    check_pruning_invariants(&reader, &oracle, &query, 1).await;
}

// ── Pinned regression programs ───────────────────────────────────────
//
// Deterministic replays of known failure geometries, run through the
// same interpreter/oracle as the proptest cases so they stay in sync
// with the harness. Explicit programs are preferred over committed
// proptest-regressions seed entries because seeds are sensitive to the
// strategy shape (this commit itself changed the strategies, which
// would have invalidated any previously recorded seeds); concrete
// programs replay identically regardless of generator changes. One
// shrunk seed entry is additionally checked in for the tight
// window-bound geometry that resists compact hand construction (see 3
// below). All four pass with the current engine — they pin geometries that caught (or
// were verified to catch) past bugs:
//
//   1. `pinned_corrupt_count_suffix_heal` — corrupt stored count healed
//      by a commit whose suffix rewrite leaves block 0 untouched; the
//      pre-self-heal writer laundered the bad count through prefix
//      arithmetic (fixed in "Self-heal corrupt posting counts").
//   2. `pinned_append_only_strands_partial_interior_blocks` — repeated
//      pure above-max appends strand partial interior blocks; the count
//      invariant here is the regression for the original
//      `count_postings` estimate bug (assumed only the last block could
//      be partial) and for the incremental `prefix + merged suffix`
//      count arithmetic (verified: dropping the suffix term fails here).
//   3. `pinned_multi_window_threshold_pruning` — top-k winners live in
//      the last of several windows while earlier windows fill the heap,
//      so a wrong window upper bound loses them (verified: halving
//      `window_upper_bound` fails here; a tight 5% deflation is missed
//      by this program but caught by the random pruning cases, whose
//      minimal shrunk detector is checked in as the seed entry in
//      tests/proptest-regressions/ms_19_proptest_invariants.txt — that
//      seed replays first on every run, with the standard caveat that
//      proptest seeds are sensitive to strategy shape).
//   4. `pinned_budget_pruning_candidates` — dense multi-term layout
//      where the essential/non-essential partition and
//      filter_competitive demonstrably engage (verified via temporary
//      engine counters in a local instrumented run, and it also fails
//      under a halved `window_upper_bound`).

fn pinned_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn commit(ops: Vec<OpSpec>) -> StepSpec {
    StepSpec::Commit { ops }
}

fn set(dim: u32, offset: u32) -> OpSpec {
    OpSpec {
        dim,
        offset,
        delete: false,
    }
}

#[test]
fn pinned_corrupt_count_suffix_heal() {
    // block_size 2 over offsets 0..6 => blocks [0,1][2,3][4,5]. The
    // fault stamps a too-low count (seed 0 => wrong count 0), then the
    // next commit touches only offset 5 => suffix rewrite from block 2,
    // block 0 untouched. Pre-fix, the prefix count was derived from the
    // corrupt stamp by subtraction (laundering); the invariant pass
    // after the heal commit requires stored count == oracle df == 6.
    let program = Program {
        num_dims: 1,
        block_size: 2,
        append_only: false,
        steps: vec![
            commit((0..6).map(|off| set(0, off)).collect()),
            StepSpec::Fault {
                dim: 0,
                kind: FaultKind::CorruptCount,
                seed: 0,
            },
            commit(vec![set(0, 5)]),
        ],
    };
    pinned_runtime().block_on(run_program(program));
}

#[test]
fn pinned_append_only_strands_partial_interior_blocks() {
    // block_size 4: commit 1 writes offsets 1..=6 => blocks [4][2]
    // (last block partial). Commits 2 and 3 write strictly above the
    // high-water mark, so the suffix rewrite starts past the end and
    // the partial blocks become *interior*: [4][2][4][3][4][1] after
    // commit 3. The old count estimate (num_blocks * first_block_len)
    // would report 24; the oracle df is 18. Also the exact regression
    // for the incremental count: new_count = prefix + merged suffix.
    let program = Program {
        num_dims: 1,
        block_size: 4,
        append_only: true,
        steps: vec![
            commit((1..=6).map(|off| set(0, off)).collect()),
            commit((10..=16).map(|off| set(0, off)).collect()),
            commit((20..=24).map(|off| set(0, off)).collect()),
        ],
    };
    pinned_runtime().block_on(run_program(program));
}

/// Deterministic offset spray for pinned pruning programs: strides
/// coprime with PRUNING_MAX_OFFSET (= 2^12 * 5: odd, not a multiple of
/// 5) make the walk injective for len <= PRUNING_MAX_OFFSET, spreading
/// offsets across all query windows.
fn spray(len: u32, stride: u32, salt: u32) -> Vec<u32> {
    debug_assert!(stride % 2 == 1 && !stride.is_multiple_of(5));
    let mut offsets: Vec<u32> = (0..len)
        .map(|i| (salt + i * stride) % PRUNING_MAX_OFFSET)
        .collect();
    offsets.sort_unstable();
    offsets
}

#[test]
fn pinned_multi_window_threshold_pruning() {
    // Dim 0 sprays 300 postings across all 5 windows and fills the heap
    // early; dim 1 concentrates 40 postings in the final window. The
    // weight counter walks the full [1, 512) range, so the last window
    // holds top-k members that only survive if window upper bounds are
    // honest — a deflated `window_upper_bound` makes the engine skip
    // the final window once the threshold is live and lose them.
    let program = PruningProgram {
        block_size: 32,
        dims: vec![
            PruningDimSpec {
                base: spray(300, 6_971, 0),
                touch: vec![],
                deletes: vec![],
                query_weight: 2.0,
            },
            PruningDimSpec {
                base: (0..40).map(|i| PRUNING_MAX_OFFSET - 1 - 7 * i).collect(),
                touch: vec![],
                deletes: vec![],
                query_weight: 1.0,
            },
        ],
    };
    pinned_runtime().block_on(run_pruning_program(program));
}

#[test]
fn pinned_budget_pruning_candidates() {
    // Three dense overlapping terms with asymmetric query weights: the
    // 0.5-weight terms fall out of the essential prefix once the heap
    // threshold rises, so candidates drained from the essential term
    // are completed via score_candidates under a shrinking budget
    // (filter_competitive). Commit 2 overwrites/deletes/appends to run
    // the same query over a suffix-rewritten multi-window layout.
    let program = PruningProgram {
        block_size: 64,
        dims: vec![
            PruningDimSpec {
                base: spray(500, 8_807, 3),
                touch: spray(60, 11_213, 7),
                deletes: spray(30, 13_711, 3),
                query_weight: 2.0,
            },
            PruningDimSpec {
                base: spray(450, 15_013, 11),
                touch: vec![],
                deletes: vec![],
                query_weight: 0.5,
            },
            PruningDimSpec {
                base: spray(400, 6_971, 5),
                touch: spray(50, 9_973, 1),
                deletes: vec![],
                query_weight: 0.5,
            },
        ],
    };
    pinned_runtime().block_on(run_pruning_program(program));
}

proptest! {
    // Bounded for CI: each case is ~10 ms (<=10 commits + invariant
    // queries), so 256 cases stay in single-digit seconds. 256 rather
    // than a few dozen because the rarest scripted scenario (corrupt
    // count + heal via a suffix rewrite that leaves block 0 untouched)
    // needs a few hundred cases to be hit reliably — measured 5/5
    // detection of the pre-fix count-laundering bug at 256 cases vs a
    // miss at 48; that geometry is additionally pinned above, so the
    // random cases extend coverage rather than carry it alone. Raise
    // via PROPTEST_CASES for deeper local soaks.
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    #[test]
    fn maxscore_matches_in_memory_oracle(program in arb_program()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(run_program(program));
    }
}

proptest! {
    // Fewer cases than the invariant suite: each pruning case commits
    // hundreds of postings and runs multi-window queries (~13 ms), so
    // 96 cases add ~1.3 s and keep the whole ms_19 file well within the
    // CI budget (~4 s total, measured) while exercising window-skip and
    // budget pruning in most cases — a local counter-instrumented run
    // over the suite measured ~130 whole-window threshold skips, ~680
    // windows with an active essential/non-essential partition, and
    // ~65k budget-pruned candidates. Detection power: a 5% deflation of
    // `window_upper_bound` was caught at 48 cases (shrunk detector now
    // checked in as a seed); see the pinned tests for deterministically
    // pinned geometries. Raise via PROPTEST_CASES for local soaks.
    #![proptest_config(ProptestConfig {
        cases: 96,
        ..ProptestConfig::default()
    })]

    #[test]
    fn maxscore_pruning_matches_exhaustive_oracle(program in arb_pruning_program()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(run_pruning_program(program));
    }
}
