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

fn arb_program() -> impl Strategy<Value = Program> {
    (1u32..=4u32, 2u32..=8u32).prop_flat_map(|(num_dims, block_size)| {
        proptest::collection::vec(arb_step(num_dims), 3..=10).prop_map(move |steps| Program {
            num_dims,
            block_size,
            steps,
        })
    })
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
        steps,
    } = program;

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

proptest! {
    // Bounded for CI: each case is ~10 ms (<=10 commits + invariant
    // queries), so 256 cases stay in single-digit seconds. 256 rather
    // than a few dozen because the rarest scripted scenario (corrupt
    // count + heal via a suffix rewrite that leaves block 0 untouched)
    // needs a few hundred cases to be hit reliably — measured 5/5
    // detection of the pre-fix count-laundering bug at 256 cases vs a
    // miss at 48. Raise via PROPTEST_CASES for deeper local soaks.
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
