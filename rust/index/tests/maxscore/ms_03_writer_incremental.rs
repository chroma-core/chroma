use crate::common;

#[tokio::test]
async fn incremental_add() {
    let docs = vec![(0u32, vec![(1u32, 0.5)]), (1, vec![(1, 0.8)])];
    let (_dir, provider, reader) = common::build_index(docs).await;

    let writer = common::fork_writer(&provider, &reader).await;
    writer.set(2, vec![(1u32, 0.3)]).await;

    let reader2 = common::commit_writer(&provider, writer).await;
    let entries = common::get_all_entries(&reader2, 1).await;
    assert_eq!(entries.len(), 3);
    let offsets: Vec<u32> = entries.iter().map(|(o, _)| *o).collect();
    assert_eq!(offsets, vec![0, 1, 2]);
}

#[tokio::test]
async fn incremental_delete() {
    let docs = vec![
        (0u32, vec![(1u32, 0.5)]),
        (1, vec![(1, 0.8)]),
        (2, vec![(1, 0.3)]),
    ];
    let (_dir, provider, reader) = common::build_index(docs).await;

    let writer = common::fork_writer(&provider, &reader).await;
    writer.delete(1, vec![1u32]).await;

    let reader2 = common::commit_writer(&provider, writer).await;
    let entries = common::get_all_entries(&reader2, 1).await;
    assert_eq!(entries.len(), 2);
    let offsets: Vec<u32> = entries.iter().map(|(o, _)| *o).collect();
    assert_eq!(offsets, vec![0, 2]);
}

#[tokio::test]
async fn incremental_update() {
    let docs = vec![(0u32, vec![(1u32, 0.5)]), (1, vec![(1, 0.8)])];
    let (_dir, provider, reader) = common::build_index(docs).await;

    let writer = common::fork_writer(&provider, &reader).await;
    writer.set(1, vec![(1u32, 0.1)]).await;

    let reader2 = common::commit_writer(&provider, writer).await;
    let entries = common::get_all_entries(&reader2, 1).await;
    assert_eq!(entries.len(), 2);
    common::assert_approx(entries[1].1, 0.1, 1e-3);
}

#[tokio::test]
async fn incremental_delete_all_in_dimension() {
    let docs = vec![(0u32, vec![(1u32, 0.5)]), (1, vec![(1, 0.8)])];
    let (_dir, provider, reader) = common::build_index(docs).await;

    let writer = common::fork_writer(&provider, &reader).await;
    writer.delete(0, vec![1u32]).await;
    writer.delete(1, vec![1u32]).await;

    let reader2 = common::commit_writer(&provider, writer).await;
    let entries = common::get_all_entries(&reader2, 1).await;
    assert_eq!(entries.len(), 0);
}
