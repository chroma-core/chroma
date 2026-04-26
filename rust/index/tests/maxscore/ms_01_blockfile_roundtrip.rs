use crate::common;
use chroma_index::sparse::types::encode_u32;
use chroma_types::DIRECTORY_PREFIX;

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
async fn directory_stored_under_prefix() {
    let docs = vec![
        (0u32, vec![(1u32, 0.5f32)]),
        (1, vec![(1, 0.8)]),
        (2, vec![(1, 0.9)]),
    ];

    let (_dir, _provider, reader) = common::build_index(docs).await;

    let (dir, _part_count) = reader
        .get_directory(&encode_u32(1))
        .await
        .unwrap()
        .expect("directory should exist");
    assert_eq!(dir.num_blocks(), 1);

    // Posting prefix should contain only data blocks, no directory sentinel
    let encoded = encode_u32(1);
    let posting_blocks: Vec<_> = reader
        .posting_reader()
        .get_prefix(&encoded)
        .await
        .unwrap()
        .collect();
    assert!(
        posting_blocks.iter().all(|(_, b)| !b.is_directory()),
        "posting prefix should not contain directory blocks"
    );

    // Directory parts should be under DIRECTORY_PREFIX
    let dir_prefix = format!("{}{}", DIRECTORY_PREFIX, encoded);
    let dir_parts: Vec<_> = reader
        .posting_reader()
        .get_prefix(&dir_prefix)
        .await
        .unwrap()
        .collect();
    assert!(!dir_parts.is_empty(), "directory parts should be present");
    assert!(dir_parts.iter().all(|(_, b)| b.is_directory()));
}

#[tokio::test]
async fn multi_block_dimension_roundtrip() {
    let num_docs = 100u32;
    let block_size = 10u32;
    let docs: Vec<(u32, Vec<(u32, f32)>)> = (0..num_docs)
        .map(|i| {
            let weight = 0.1 + (i as f32) * 0.01;
            (i, vec![(1u32, weight)])
        })
        .collect();

    let (_dir, _provider, reader) =
        common::build_index_with_block_size(docs.clone(), Some(block_size)).await;

    let blocks = common::count_blocks(&reader, 1).await;
    assert_eq!(blocks, (num_docs / block_size) as usize);

    let all_entries = common::get_all_entries(&reader, 1).await;
    assert_eq!(all_entries.len(), num_docs as usize);
    for (i, (off, val)) in all_entries.iter().enumerate() {
        assert_eq!(*off, i as u32);
        let expected = 0.1 + (i as f32) * 0.01;
        common::assert_approx(*val, expected, 5e-4);
    }

    let (dir, _part_count) = reader
        .get_directory(&encode_u32(1))
        .await
        .unwrap()
        .expect("directory should exist");
    let expected_blocks = (num_docs / block_size) as usize;
    assert_eq!(dir.num_blocks(), expected_blocks);

    let max_offsets = dir.max_offsets();
    let max_weights = dir.max_weights();
    assert_eq!(max_offsets.len(), expected_blocks);
    for block_idx in 0..expected_blocks {
        let expected_last_offset = (block_idx as u32 + 1) * block_size - 1;
        assert_eq!(max_offsets[block_idx], expected_last_offset);
        assert!(
            max_weights[block_idx] > 0.0,
            "block {block_idx} max_weight should be positive"
        );
    }
}
