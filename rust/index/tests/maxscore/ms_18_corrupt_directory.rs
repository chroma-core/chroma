use crate::common;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, test_arrow_blockfile_provider, BlockfileWriterOptions,
};
use chroma_index::sparse::maxscore::{MaxScoreReader, SPARSE_POSTING_BLOCK_SIZE_BYTES};
use chroma_index::sparse::types::encode_u32;
use chroma_types::{DirectoryBlock, SignedRoaringBitmap, SparsePostingBlock, DIRECTORY_PREFIX};

const DIM_GOOD: u32 = 1;
const DIM_CORRUPT: u32 = 2;
const DIM_ABSENT: u32 = 999;

/// Build an index where both dimensions have valid posting blocks, but
/// `DIM_CORRUPT`'s directory key holds a plain (non-directory) posting
/// block — modeling a corrupt/undecodable on-disk directory. The reader
/// must treat the dimension as skippable, never as an error.
async fn build_corrupt_index() -> (
    tempfile::TempDir,
    BlockfileProvider,
    MaxScoreReader<'static>,
) {
    let (temp_dir, provider) = test_arrow_blockfile_provider(SPARSE_POSTING_BLOCK_SIZE_BYTES);
    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES),
        )
        .await
        .unwrap();

    let entries_for = |dim: u32| -> Vec<(u32, f32)> {
        let weight = dim as f32;
        (0..5).map(|off| (off, weight)).collect()
    };

    let mut encoded_dims: Vec<(String, u32)> = [DIM_GOOD, DIM_CORRUPT]
        .iter()
        .map(|&dim| (encode_u32(dim), dim))
        .collect();
    encoded_dims.sort_by(|a, b| a.0.cmp(&b.0));

    // Posting blocks first, then directory parts — same ordered-mutations
    // discipline as MaxScoreWriter::commit.
    let mut directories: Vec<(String, u32)> = Vec::new();
    for (encoded_dim, dim) in &encoded_dims {
        let block = SparsePostingBlock::from_sorted_entries(&entries_for(*dim)).unwrap();
        posting_writer
            .set(encoded_dim.as_str(), 0u32, block)
            .await
            .unwrap();
        directories.push((format!("{}{}", DIRECTORY_PREFIX, encoded_dim), *dim));
    }
    directories.sort_by(|a, b| a.0.cmp(&b.0));
    for (dir_prefix, dim) in directories {
        let block = SparsePostingBlock::from_sorted_entries(&entries_for(dim)).unwrap();
        let stored = if dim == DIM_CORRUPT {
            // A plain posting block at the directory key: it fails
            // DirectoryBlock decoding, so the directory is undecodable.
            block
        } else {
            DirectoryBlock::new(&[block.header.max_offset], &[block.header.max_weight])
                .unwrap()
                .into_block()
        };
        posting_writer
            .set(dir_prefix.as_str(), 0u32, stored)
            .await
            .unwrap();
    }

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
    // SAFETY: same pattern as common::build_index — the provider and
    // TempDir outlive the reader in every caller.
    let reader: MaxScoreReader<'static> =
        unsafe { std::mem::transmute::<MaxScoreReader<'_>, MaxScoreReader<'static>>(reader) };

    (temp_dir, provider, reader)
}

#[tokio::test]
async fn corrupt_directory_yields_none_without_error() {
    let (_dir, _provider, reader) = build_corrupt_index().await;

    // Corrupt: parts exist on disk but cannot be decoded. The trace
    // error emitted here is best-effort observability; the observable
    // contract is unchanged — no error, directory reported as None.
    assert!(reader
        .get_directory(&encode_u32(DIM_CORRUPT))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        reader
            .count_postings(&encode_u32(DIM_CORRUPT))
            .await
            .unwrap(),
        0
    );

    // Absent: no directory on disk at all (unknown query term) — the
    // quiet path behaves identically.
    assert!(reader
        .get_directory(&encode_u32(DIM_ABSENT))
        .await
        .unwrap()
        .is_none());

    // Intact dimension is unaffected.
    let (dir, part_count) = reader
        .get_directory(&encode_u32(DIM_GOOD))
        .await
        .unwrap()
        .expect("good directory should decode");
    assert_eq!(part_count, 1);
    assert_eq!(dir.num_blocks(), 1);
}

#[tokio::test]
async fn query_skips_corrupt_dimension() {
    let (_dir, _provider, reader) = build_corrupt_index().await;

    let mask = SignedRoaringBitmap::Exclude(Default::default());
    let results = reader
        .query(vec![(DIM_GOOD, 1.0), (DIM_CORRUPT, 1.0)], 10, mask)
        .await
        .unwrap();

    // All five docs contain both dimensions (weights 1.0 and 2.0). With
    // the corrupt dimension skipped, each doc scores 1.0 from DIM_GOOD
    // alone; 3.0 would mean the corrupt dimension was somehow scored.
    assert_eq!(results.len(), 5);
    for score in &results {
        common::assert_approx(score.score, 1.0, 1e-3);
    }
}

#[tokio::test]
async fn commit_rebuilds_corrupt_dimension_from_deltas() {
    let (_dir, provider, reader) = build_corrupt_index().await;

    // The writer sees an existing reader whose directory for this
    // dimension is undecodable: it must fall into the fresh-dimension
    // path (alerting via trace error) and commit cleanly.
    let writer = common::fork_writer(&provider, &reader).await;
    writer.set(10, vec![(DIM_CORRUPT, 0.7)]).await;
    let reader2 = common::commit_writer(&provider, writer).await;

    let (dir, _) = reader2
        .get_directory(&encode_u32(DIM_CORRUPT))
        .await
        .unwrap()
        .expect("rewritten directory should decode");
    assert_eq!(dir.num_blocks(), 1);
    let entries = common::get_all_entries(&reader2, DIM_CORRUPT).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, 10);
    common::assert_approx(entries[0].1, 0.7, 1e-3);

    let mask = SignedRoaringBitmap::Exclude(Default::default());
    let results = reader2
        .query(vec![(DIM_CORRUPT, 1.0)], 10, mask)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].offset, 10);
    common::assert_approx(results[0].score, 0.7, 1e-3);
}
