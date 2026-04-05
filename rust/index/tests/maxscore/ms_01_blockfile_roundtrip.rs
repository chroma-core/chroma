use crate::common;
use chroma_index::sparse::types::encode_u32;
use chroma_types::{DirectoryBlock, SparsePostingBlock};

#[tokio::test]
async fn blockfile_roundtrip_basic() {
    let docs = vec![
        (0u32, vec![(1u32, 0.5f32), (2, 0.3)]),
        (1, vec![(1, 0.8), (3, 0.2)]),
        (2, vec![(2, 0.6), (3, 0.9)]),
    ];

    let (_dir, _provider, reader) = common::build_index(docs).await;

    let entries_dim1 = common::get_all_entries(&reader, 1).await;
    assert_eq!(entries_dim1.len(), 2);
    let offsets: Vec<u32> = entries_dim1.iter().map(|(o, _)| *o).collect();
    assert_eq!(offsets, vec![0, 1]);

    let entries_dim2 = common::get_all_entries(&reader, 2).await;
    assert_eq!(entries_dim2.len(), 2);

    let entries_dim3 = common::get_all_entries(&reader, 3).await;
    assert_eq!(entries_dim3.len(), 2);
}

#[tokio::test]
async fn directory_block_exists_and_decodable() {
    let docs = vec![
        (0u32, vec![(1u32, 0.5f32)]),
        (1, vec![(1, 0.8)]),
        (2, vec![(1, 0.9)]),
    ];

    let (_dir, _provider, reader) = common::build_index(docs).await;

    let encoded = encode_u32(1);
    let all: Vec<(u32, SparsePostingBlock)> = reader
        .posting_reader()
        .get_prefix(&encoded)
        .await
        .unwrap()
        .collect();

    let dir_entry = all.iter().find(|(k, _)| *k == u32::MAX);
    assert!(dir_entry.is_some(), "directory block should be present");
    let (_, dir_block) = dir_entry.unwrap();
    assert!(dir_block.is_directory());
    let dir = DirectoryBlock::from_block(dir_block.clone()).unwrap();
    assert_eq!(dir.num_blocks(), 1);
}

#[tokio::test]
async fn multi_block_dimension_roundtrip() {
    let entries: Vec<(u32, f32)> = (0..100).map(|i| (i, 0.5)).collect();
    let docs: Vec<(u32, Vec<(u32, f32)>)> = entries
        .iter()
        .map(|&(off, _)| (off, vec![(1u32, 0.5f32)]))
        .collect();

    let (_dir, _provider, reader) = common::build_index_with_block_size(docs, Some(10)).await;

    let blocks = common::count_blocks(&reader, 1).await;
    assert_eq!(blocks, 10);

    let all_entries = common::get_all_entries(&reader, 1).await;
    assert_eq!(all_entries.len(), 100);
    for (i, (off, _)) in all_entries.iter().enumerate() {
        assert_eq!(*off, i as u32);
    }
}
