use crate::common;

#[tokio::test]
async fn writer_single_doc() {
    let docs = vec![(0u32, vec![(1u32, 0.5f32)])];
    let (_dir, _provider, reader) = common::build_index(docs).await;

    let entries = common::get_all_entries(&reader, 1).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, 0);
    common::assert_approx(entries[0].1, 0.5, 1e-3);
}

#[tokio::test]
async fn writer_multi_doc_multi_dim() {
    let docs = vec![
        (0u32, vec![(1u32, 0.1), (2, 0.2), (3, 0.3)]),
        (1, vec![(1, 0.4), (3, 0.5)]),
        (2, vec![(2, 0.6)]),
    ];
    let (_dir, _provider, reader) = common::build_index(docs).await;

    let dim1 = common::get_all_entries(&reader, 1).await;
    assert_eq!(dim1.len(), 2);

    let dim2 = common::get_all_entries(&reader, 2).await;
    assert_eq!(dim2.len(), 2);

    let dim3 = common::get_all_entries(&reader, 3).await;
    assert_eq!(dim3.len(), 2);
}

#[tokio::test]
async fn writer_preserves_offset_order() {
    let docs = vec![
        (10u32, vec![(1u32, 0.5)]),
        (2, vec![(1, 0.3)]),
        (7, vec![(1, 0.8)]),
    ];
    let (_dir, _provider, reader) = common::build_index(docs).await;

    let entries = common::get_all_entries(&reader, 1).await;
    let offsets: Vec<u32> = entries.iter().map(|(o, _)| *o).collect();
    assert_eq!(offsets, vec![2, 7, 10]);
}
