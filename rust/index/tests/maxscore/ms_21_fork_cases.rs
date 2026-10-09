//! Fork-case coverage for exact posting counts.
//!
//! Production forks share parent posting blocks by reference, so a
//! parent's blocks can be prefix-reused by several diverging children
//! at once. These tests pin down the incident geometry: counts must
//! stay exact and isolated per child across fork divergence, deep
//! fork chains with stranded partial tail blocks, and forks of legacy
//! (version-0, uncounted) indexes migrating lazily to exact counts.

use std::collections::BTreeMap;

use crate::common;
use crate::ms_17_exact_count::build_legacy_index;
use chroma_index::sparse::maxscore::MaxScoreReader;
use chroma_index::sparse::types::encode_u32;
use chroma_types::SignedRoaringBitmap;

async fn count(reader: &MaxScoreReader<'_>, dim: u32) -> usize {
    reader.count_postings(&encode_u32(dim)).await.unwrap()
}

async fn stored_count(reader: &MaxScoreReader<'_>, dim: u32) -> Option<u32> {
    let (dir, _) = reader
        .get_directory(&encode_u32(dim))
        .await
        .unwrap()
        .expect("directory should exist");
    dir.posting_count().ok()
}

/// Distinct weight, exactly representable in f16 (the ms_19/ms_99
/// injective construction): all values are exact f16 in [1, 512) and
/// globally distinct, so oracle rankings are tie-free and round-trip
/// the index losslessly.
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

/// Stored postings must match the oracle entry-for-entry.
async fn assert_entries(reader: &MaxScoreReader<'_>, dim: u32, oracle: &BTreeMap<u32, f32>) {
    let actual: BTreeMap<u32, f32> = common::get_all_entries(reader, dim)
        .await
        .into_iter()
        .collect();
    assert_eq!(
        &actual, oracle,
        "dim {dim}: stored postings diverge from oracle"
    );
}

/// Engine top-k must equal the oracle's exhaustive top-k exactly.
/// With a single positive-weight term and globally distinct weights,
/// descending weight is the unique oracle order.
async fn assert_topk(
    reader: &MaxScoreReader<'_>,
    dim: u32,
    query_weight: f32,
    k: usize,
    oracle: &BTreeMap<u32, f32>,
) {
    let mut ranked: Vec<(u32, f32)> = oracle.iter().map(|(&off, &w)| (off, w)).collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1));
    ranked.truncate(k);
    let results = reader
        .query(vec![(dim, query_weight)], k as u32, exclude_none())
        .await
        .expect("query over a live directory must not error");
    let got: Vec<u32> = results.iter().map(|s| s.offset).collect();
    let want: Vec<u32> = ranked.iter().map(|&(off, _)| off).collect();
    assert_eq!(
        got, want,
        "dim {dim} k={k}: engine top-k diverges from oracle"
    );
}

/// Two children forked from the same parent state diverge (one
/// appends, one deletes): each child's stored count and top-k must
/// track its own oracle exactly, with no leakage between siblings or
/// back into the parent.
#[tokio::test]
async fn fork_divergence_keeps_children_exact_and_isolated() {
    const DIM: u32 = 1;
    const BLOCK: u32 = 16;

    // Parent: a few hundred postings on one common dim, many blocks.
    let parent_oracle: BTreeMap<u32, f32> = (0..300).map(|i| (i, weight_for(i))).collect();
    let parent_docs: Vec<(u32, Vec<(u32, f32)>)> = parent_oracle
        .iter()
        .map(|(&off, &w)| (off, vec![(DIM, w)]))
        .collect();
    let (_dir, provider, parent) =
        common::build_index_with_block_size(parent_docs, Some(BLOCK)).await;

    // Child A: appends new postings at fresh offsets past the parent.
    let mut oracle_a = parent_oracle.clone();
    let writer_a = common::fork_writer_with_block_size(&provider, &parent, Some(BLOCK)).await;
    for i in 0..100 {
        let (off, w) = (1000 + i, weight_for(300 + i));
        writer_a.set(off, vec![(DIM, w)]).await;
        oracle_a.insert(off, w);
    }
    let reader_a = common::commit_writer(&provider, writer_a).await;

    // Child B: forked from the same parent state, deletes a chunk of
    // existing postings instead.
    let mut oracle_b = parent_oracle.clone();
    let writer_b = common::fork_writer_with_block_size(&provider, &parent, Some(BLOCK)).await;
    for off in 50..150 {
        writer_b.delete(off, vec![DIM]).await;
        oracle_b.remove(&off);
    }
    let reader_b = common::commit_writer(&provider, writer_b).await;

    // Each child is exact against its own oracle.
    assert_eq!(
        stored_count(&reader_a, DIM).await,
        Some(oracle_a.len() as u32)
    );
    assert_eq!(count(&reader_a, DIM).await, oracle_a.len());
    assert_entries(&reader_a, DIM, &oracle_a).await;
    assert_topk(&reader_a, DIM, 1.0, 10, &oracle_a).await;

    assert_eq!(
        stored_count(&reader_b, DIM).await,
        Some(oracle_b.len() as u32)
    );
    assert_eq!(count(&reader_b, DIM).await, oracle_b.len());
    assert_entries(&reader_b, DIM, &oracle_b).await;
    assert_topk(&reader_b, DIM, 1.0, 10, &oracle_b).await;

    // Isolation: A's appended offsets never surface in B, and B's
    // deletions never remove postings from A.
    let entries_b: BTreeMap<u32, f32> = common::get_all_entries(&reader_b, DIM)
        .await
        .into_iter()
        .collect();
    assert!(
        entries_b.keys().all(|&off| off < 1000),
        "child B must not see child A's appended postings"
    );
    let full_b = reader_b
        .query(vec![(DIM, 1.0)], oracle_b.len() as u32, exclude_none())
        .await
        .unwrap();
    assert!(
        full_b.iter().all(|s| s.offset < 1000),
        "child B results must not contain child A's offsets"
    );
    let entries_a: BTreeMap<u32, f32> = common::get_all_entries(&reader_a, DIM)
        .await
        .into_iter()
        .collect();
    assert!(
        (50..150).all(|off| entries_a.contains_key(&off)),
        "child B's deletions must not leak into child A"
    );

    // The parent state stays readable and unchanged.
    assert_eq!(
        stored_count(&parent, DIM).await,
        Some(parent_oracle.len() as u32)
    );
    assert_eq!(count(&parent, DIM).await, parent_oracle.len());
    assert_entries(&parent, DIM, &parent_oracle).await;
    assert_topk(&parent, DIM, 1.0, 10, &parent_oracle).await;
}

/// The incident geometry: parent → fork → append+commit → fork →
/// append+commit → fork → append+commit (tip), with every appended
/// batch larger than the posting block size so each commit strands a
/// partial tail block for the next fork to inherit. At the tip the
/// stored count must equal the oracle df exactly, IDF_raw must stay
/// positive, and the engine top-10 must equal the oracle top-10.
#[tokio::test]
async fn three_deep_fork_chain_keeps_exact_counts_and_recall() {
    const COMMON: u32 = 1;
    const OTHER: u32 = 7;
    const BLOCK: u32 = 64;
    const PARENT_DOCS: u32 = 300;
    const DOCS_PER_LEVEL: u32 = 100; // 90 common postings > BLOCK

    let mut weight_counter = 0;
    let mut oracle_common: BTreeMap<u32, f32> = BTreeMap::new();
    let mut make_batch = |start: u32, len: u32| -> Vec<(u32, Vec<(u32, f32)>)> {
        (start..start + len)
            .map(|off| {
                let w = weight_for(weight_counter);
                weight_counter += 1;
                // Common dim in 90% of docs; the rest land on a filler
                // dim so every doc carries a posting.
                if off % 10 != 0 {
                    oracle_common.insert(off, w);
                    (off, vec![(COMMON, w)])
                } else {
                    (off, vec![(OTHER, w)])
                }
            })
            .collect()
    };

    let (_dir, provider, mut reader) =
        common::build_index_with_block_size(make_batch(0, PARENT_DOCS), Some(BLOCK)).await;

    for level in 0..3 {
        let batch = make_batch(PARENT_DOCS + level * DOCS_PER_LEVEL, DOCS_PER_LEVEL);
        let writer = common::fork_writer_with_block_size(&provider, &reader, Some(BLOCK)).await;
        for (off, dims) in batch {
            writer.set(off, dims).await;
        }
        reader = common::commit_writer(&provider, writer).await;
    }

    let n = PARENT_DOCS + 3 * DOCS_PER_LEVEL;
    let tip_count = count(&reader, COMMON).await;
    assert_eq!(tip_count, oracle_common.len(), "tip count must be exact");
    assert_eq!(
        stored_count(&reader, COMMON).await,
        Some(oracle_common.len() as u32)
    );

    let idf_raw = ((n as f64 - tip_count as f64 + 0.5) / (tip_count as f64 + 0.5)).ln_1p();
    assert!(idf_raw > 0.0, "IDF_raw must stay positive, got {idf_raw}");

    // Engine top-10 under the IDF_raw-weighted query == oracle top-10.
    assert_topk(&reader, COMMON, idf_raw as f32, 10, &oracle_common).await;
}

/// Migration under forking: a fork of a legacy (version-0, uncounted)
/// index backfills the exact count on its first touching commit, while
/// an untouched sibling fork of the same legacy parent keeps the
/// legacy estimate.
#[tokio::test]
async fn fork_of_legacy_index_backfills_on_touch() {
    const DIM: u32 = 3;
    const OTHER: u32 = 4;
    // Stranded partial interior block: 3 + 10 entries, so the legacy
    // estimate (num_blocks * first_block_len) reports 6 for 13 real
    // postings.
    let partial_blocks = vec![
        (0..3).map(|i| (i, 0.5)).collect::<Vec<_>>(),
        (10..20).map(|i| (i, 0.5)).collect::<Vec<_>>(),
    ];
    let (_dir, provider, parent) =
        build_legacy_index(vec![(DIM, partial_blocks.clone()), (OTHER, partial_blocks)]).await;
    assert_eq!(stored_count(&parent, DIM).await, None);
    assert_eq!(count(&parent, DIM).await, 6, "legacy estimate undercounts");

    // Fork a child; its first commit touching DIM backfills the exact
    // count (upgrading the directory to the counted format).
    let writer = common::fork_writer(&provider, &parent).await;
    writer.set(100, vec![(DIM, 0.9)]).await;
    let touched = common::commit_writer(&provider, writer).await;

    let oracle_df = 14; // 13 legacy postings + 1 appended
    assert_eq!(
        stored_count(&touched, DIM).await,
        Some(oracle_df),
        "touching fork must persist the exact count (v1 directory)"
    );
    assert_eq!(count(&touched, DIM).await, oracle_df as usize);
    assert_eq!(
        common::get_all_entries(&touched, DIM).await.len(),
        oracle_df as usize
    );
    // Lazy migration: the dimension this fork never touched keeps the
    // legacy directory and its estimate.
    assert_eq!(stored_count(&touched, OTHER).await, None);
    assert_eq!(count(&touched, OTHER).await, 6);

    // The sibling fork of the same legacy parent touches only OTHER:
    // DIM keeps estimating there, while OTHER backfills.
    let writer = common::fork_writer(&provider, &parent).await;
    writer.set(100, vec![(OTHER, 0.9)]).await;
    let sibling = common::commit_writer(&provider, writer).await;

    assert_eq!(
        stored_count(&sibling, DIM).await,
        None,
        "untouched sibling fork must keep the legacy directory"
    );
    assert_eq!(count(&sibling, DIM).await, 6, "sibling keeps estimating");
    assert_eq!(stored_count(&sibling, OTHER).await, Some(oracle_df));
    assert_eq!(count(&sibling, OTHER).await, oracle_df as usize);
}
