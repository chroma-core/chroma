mod common;

use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, test_arrow_blockfile_provider,
    BlockfileWriterOptions,
};
use chroma_index::sparse::maxscore::SparsePostingBlock;
use chroma_index::sparse::types::encode_u32;
use common::make_block;

async fn write_and_read_blocks(
    blocks: Vec<(&str, u32, SparsePostingBlock)>,
) -> (
    tempfile::TempDir,
    chroma_blockstore::BlockfileReader<'static, u32, SparsePostingBlock>,
) {
    let (temp_dir, provider) = test_arrow_blockfile_provider(8 * 1024 * 1024);

    let writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string()).ordered_mutations(),
        )
        .await
        .unwrap();

    for (prefix, key, block) in blocks {
        writer.set(prefix, key, block).await.unwrap();
    }

    let flusher = writer
        .commit::<u32, SparsePostingBlock>()
        .await
        .unwrap();
    let id = flusher.id();
    flusher
        .flush::<u32, SparsePostingBlock>()
        .await
        .unwrap();

    let reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(id, "".to_string()))
        .await
        .unwrap();

    // Leak temp_dir so the files survive for 'static reader lifetime.
    let reader: chroma_blockstore::BlockfileReader<'static, u32, SparsePostingBlock> =
        unsafe { std::mem::transmute(reader) };
    (temp_dir, reader)
}

fn quantization_tolerance(max_weight: f32) -> f32 {
    max_weight / 255.0 + 1e-6
}

#[tokio::test]
async fn test_ms_01_single_block() {
    let entries = vec![(0u32, 1.0f32), (5, 0.5), (100, 0.8)];
    let block = make_block(&entries);
    let prefix = &encode_u32(0);

    let (_dir, reader) = write_and_read_blocks(vec![(prefix, 0, block.clone())]).await;
    let result = reader.get(prefix, 0u32).await.unwrap().unwrap();

    assert_eq!(result.offsets(), block.offsets());
    assert_eq!(result.min_offset, block.min_offset);
    assert_eq!(result.max_offset, block.max_offset);
    assert_eq!(result.max_weight, block.max_weight);

    let tol = quantization_tolerance(block.max_weight);
    for (&orig, &restored) in block.values().iter().zip(result.values().iter()) {
        assert!(
            (restored - orig).abs() <= tol,
            "expected {orig} ± {tol}, got {restored}"
        );
    }
}

#[tokio::test]
async fn test_ms_01_multiple_blocks_one_prefix() {
    let prefix = encode_u32(42);
    let mut items = Vec::new();
    for key in 0u32..10 {
        let start = key * 256;
        let entries: Vec<(u32, f32)> = (0..256).map(|i| (start + i, 0.5)).collect();
        items.push((prefix.as_str(), key, make_block(&entries)));
    }

    let (_dir, reader) = write_and_read_blocks(items).await;
    let results: Vec<(u32, SparsePostingBlock)> =
        reader.get_prefix(&prefix).await.unwrap().collect();

    assert_eq!(results.len(), 10);
    for (i, (key, block)) in results.iter().enumerate() {
        assert_eq!(*key, i as u32);
        assert_eq!(block.offsets().len(), 256);
    }
}

#[tokio::test]
async fn test_ms_01_multiple_prefixes() {
    let mut items = Vec::new();
    let prefixes: Vec<String> = (0..5).map(|d| encode_u32(d)).collect();
    for (d, prefix) in prefixes.iter().enumerate() {
        let entries: Vec<(u32, f32)> = (0..10).map(|i| (i, d as f32 * 0.1 + 0.1)).collect();
        items.push((prefix.as_str(), 0u32, make_block(&entries)));
    }

    let (_dir, reader) = write_and_read_blocks(items).await;

    for (d, prefix) in prefixes.iter().enumerate() {
        let result = reader.get(prefix, 0u32).await.unwrap().unwrap();
        assert_eq!(result.offsets().len(), 10);
        let expected_weight = d as f32 * 0.1 + 0.1;
        let tol = quantization_tolerance(result.max_weight);
        for &v in result.values() {
            assert!(
                (v - expected_weight).abs() <= tol,
                "dim {d}: expected {expected_weight} ± {tol}, got {v}"
            );
        }
    }
}

#[tokio::test]
async fn test_ms_01_ordered_writer() {
    let prefix = encode_u32(7);
    let entries: Vec<(u32, f32)> = (0..128).map(|i| (i, 0.3)).collect();
    let block = make_block(&entries);

    let (temp_dir, provider) = test_arrow_blockfile_provider(8 * 1024 * 1024);

    let writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string()).ordered_mutations(),
        )
        .await
        .unwrap();

    writer.set(&prefix, 0u32, block.clone()).await.unwrap();

    let flusher = writer
        .commit::<u32, SparsePostingBlock>()
        .await
        .unwrap();
    let id = flusher.id();
    flusher
        .flush::<u32, SparsePostingBlock>()
        .await
        .unwrap();

    let reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(id, "".to_string()))
        .await
        .unwrap();

    let result = reader.get(&prefix, 0u32).await.unwrap().unwrap();
    assert_eq!(result.offsets().len(), 128);
    drop(temp_dir);
}

#[tokio::test]
async fn test_ms_01_empty_prefix() {
    let prefix_written = encode_u32(0);
    let prefix_empty = encode_u32(999);
    let entries: Vec<(u32, f32)> = vec![(0, 0.5)];
    let block = make_block(&entries);

    let (_dir, reader) =
        write_and_read_blocks(vec![(prefix_written.as_str(), 0u32, block)]).await;

    let results: Vec<(u32, SparsePostingBlock)> = reader
        .get_prefix(&prefix_empty)
        .await
        .unwrap()
        .collect();
    assert!(results.is_empty());
}
