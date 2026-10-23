#![recursion_limit = "256"]
#[allow(dead_code)]
#[path = "../benches/hierarchical_index/mod.rs"]
mod hierarchical_index;

use chroma_blockstore::{arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider};
use chroma_cache::new_cache_for_test;
use chroma_distance::DistanceFunction;
use chroma_index::quantization::Code;
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::hierarchical_spann::HierarchicalSpannPostingList;
use hierarchical_index::config::HierarchicalSpannConfig;
use hierarchical_index::persistance::{
    PREFIX_EMBEDDING, PREFIX_FORMAT, SINGLETON_KEY, VERSION_FREE_FORMAT,
};
use hierarchical_index::writer::upgrade::PREFIX_VERSION;
use hierarchical_index::writer::{HierarchicalSpannIds, HierarchicalSpannWriter};
use std::sync::{atomic::Ordering, Arc};

fn embedding(id: u32) -> Vec<f32> {
    (0..32).map(|dim| id as f32 + dim as f32 * 0.01).collect()
}
fn config() -> HierarchicalSpannConfig {
    HierarchicalSpannConfig {
        split_threshold: 512,
        merge_threshold: 0,
        ..Default::default()
    }
}
fn provider(dir: &tempfile::TempDir) -> BlockfileProvider {
    BlockfileProvider::new_arrow(
        Storage::Local(LocalStorage::new(dir.path().to_str().unwrap())),
        1024 * 1024,
        new_cache_for_test(),
        new_cache_for_test(),
        4,
        0,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn lazy_append_rejects_duplicates_and_commits_exact_codes() {
    let dir = tempfile::tempdir().unwrap();
    let blockfiles = provider(&dir);
    let mut writer = HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, config());
    writer.add(10, &embedding(10)).unwrap();
    writer.add(1_000_000, &embedding(20)).unwrap();
    let first: HierarchicalSpannIds = Box::pin(
        Box::pin(writer.commit(&blockfiles, None))
            .await
            .unwrap()
            .flush(),
    )
    .await
    .unwrap();
    let mut reopened = HierarchicalSpannWriter::open(
        &blockfiles,
        first.clone(),
        DistanceFunction::Euclidean,
        config(),
    )
    .await
    .unwrap();
    assert_eq!(reopened.memory_usage().posting_entries, 0);
    assert!(reopened
        .add(10, &embedding(110))
        .unwrap_err()
        .contains("duplicate"));
    assert!(reopened
        .add_batch_buffered(
            &[
                (30, Arc::from(embedding(30))),
                (30, Arc::from(embedding(300)))
            ],
            2,
            || {}
        )
        .is_err());
    assert_eq!(reopened.total_leaf_entries(), 2);
    assert_eq!(reopened.stats.embeddings_added.load(Ordering::Relaxed), 0);
    // A hole below the maximum is valid; the new maximum uses the fast path.
    let timing = reopened
        .add_batch_buffered(
            &[
                (30, Arc::from(embedding(30))),
                (1_000_001, Arc::from(embedding(40))),
            ],
            2,
            || {},
        )
        .unwrap();
    assert_eq!(timing.preflight.checked_ids, 2);
    assert_eq!(timing.preflight.persisted_probes, 1);
    assert_eq!(timing.preflight.checkpoint_fast_path_ids, 1);
    assert!(timing.preflight.total >= timing.preflight.coordinates + timing.preflight.ids);
    assert_eq!(reopened.stats.posting_loads.load(Ordering::Relaxed), 0);
    let second = Box::pin(
        Box::pin(reopened.commit(&blockfiles, Some(&first)))
            .await
            .unwrap()
            .flush(),
    )
    .await
    .unwrap();
    let metadata = blockfiles
        .read::<u32, u32>(BlockfileReaderOptions::new(
            second.scalar_metadata_id,
            "".into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        metadata.get(PREFIX_FORMAT, SINGLETON_KEY).await.unwrap(),
        Some(VERSION_FREE_FORMAT)
    );
    assert!(metadata.get(PREFIX_VERSION, 10).await.unwrap().is_none());
    let postings = blockfiles
        .read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
            second.posting_list_id,
            "".into(),
        ))
        .await
        .unwrap();
    let posting = postings.get("", 0).await.unwrap().unwrap();
    assert_eq!(posting.ids, &[10, 1_000_000, 30, 1_000_001]);
    let data = blockfiles
        .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(
            second.vector_data_id,
            "".into(),
        ))
        .await
        .unwrap();
    for (slot, &id) in posting.ids.iter().enumerate() {
        let value = data.get(PREFIX_EMBEDDING, id).await.unwrap().unwrap();
        assert_eq!(
            value,
            embedding(match id {
                1_000_000 => 20,
                1_000_001 => 40,
                _ => id,
            })
        );
        assert_eq!(
            &posting.codes[slot * Code::<1>::size(32)..(slot + 1) * Code::<1>::size(32)],
            Code::<1>::quantize(value, &[0.0; 32]).as_ref()
        );
    }
    let again =
        HierarchicalSpannWriter::open(&blockfiles, second, DistanceFunction::Euclidean, config())
            .await
            .unwrap();
    again.load_all_postings().await.unwrap();
    assert_eq!(
        again.root_reachable_valid_ids().unwrap(),
        [10, 1_000_000, 30, 1_000_001].into_iter().collect()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn parallel_balance_preserves_every_id_through_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let blockfiles = provider(&dir);
    for fp_npa in [true, false] {
        let cfg = HierarchicalSpannConfig {
            split_threshold: 24,
            merge_threshold: 0,
            branching_factor: 4,
            fp_npa,
            ..Default::default()
        };
        let mut writer = HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, cfg.clone());
        let batch: Vec<_> = (0..240).map(|id| (id, Arc::from(embedding(id)))).collect();
        writer.add_batch_buffered(&batch, 4, || {}).unwrap();
        writer.balance_index_parallel(4).unwrap();
        let expected = (0..240).collect();
        assert_eq!(writer.root_reachable_valid_ids().unwrap(), expected);
        let ids = Box::pin(
            Box::pin(writer.commit(&blockfiles, None))
                .await
                .unwrap()
                .flush(),
        )
        .await
        .unwrap();
        let reopened = HierarchicalSpannWriter::open(
            &blockfiles,
            ids.clone(),
            DistanceFunction::Euclidean,
            cfg,
        )
        .await
        .unwrap();
        reopened.load_all_postings().await.unwrap();
        assert_eq!(reopened.root_reachable_valid_ids().unwrap(), expected);
        let data = blockfiles
            .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(ids.vector_data_id, "".into()))
            .await
            .unwrap();
        let postings = blockfiles
            .read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
                ids.posting_list_id,
                "".into(),
            ))
            .await
            .unwrap();
        let leaves = blockfiles
            .read::<u32, chroma_types::hierarchical_spann::HierarchicalLeafNode<'static>>(
                BlockfileReaderOptions::new(ids.leaf_node_id, "".into()),
            )
            .await
            .unwrap();
        for (_, leaf_id, leaf) in leaves.get_range(""..="", ..).await.unwrap() {
            let posting = postings.get("", leaf_id).await.unwrap().unwrap();
            assert_eq!(posting.ids.len(), leaf.length as usize);
            let centroid = data
                .get(hierarchical_index::persistance::PREFIX_CENTROID, leaf_id)
                .await
                .unwrap()
                .unwrap();
            for (slot, &id) in posting.ids.iter().enumerate() {
                let emb = data.get(PREFIX_EMBEDDING, id).await.unwrap().unwrap();
                assert_eq!(emb, embedding(id));
                assert_eq!(
                    &posting.codes[slot * Code::<1>::size(32)..(slot + 1) * Code::<1>::size(32)],
                    Code::<1>::quantize(emb, centroid).as_ref()
                );
            }
        }
    }
}

/// Build the legacy Arrow layout directly inside a test directory. This keeps
/// write support for versioned rows out of the live blockstore API.
async fn legacy_fixture(dir: &tempfile::TempDir, duplicate_live: bool) -> HierarchicalSpannIds {
    legacy_fixture_sized(dir, duplicate_live, 0).await
}

async fn legacy_fixture_sized(
    dir: &tempfile::TempDir,
    duplicate_live: bool,
    extra: u32,
) -> HierarchicalSpannIds {
    use arrow::{
        array::{
            Array, ArrayRef, ListArray, ListBuilder, StructArray, UInt32Array, UInt32Builder,
            UInt8Builder,
        },
        datatypes::{DataType, Field, Schema},
        ipc::{reader::FileReader, writer::FileWriter},
        record_batch::RecordBatch,
    };
    let files = provider(dir);
    let mut writer = HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, config());
    writer.add(10, &embedding(10)).unwrap();
    writer.add(11, &embedding(10)).unwrap();
    writer.add(12, &embedding(12)).unwrap();
    let extras: Vec<_> = (13..13 + extra)
        .map(|id| (id, Arc::from(embedding(id))))
        .collect();
    writer.add_batch_buffered(&extras, 4, || {}).unwrap();
    let mut ids = Box::pin(Box::pin(writer.commit(&files, None)).await.unwrap().flush())
        .await
        .unwrap();
    let metadata = files
        .write::<u32, u32>(
            chroma_blockstore::BlockfileWriterOptions::new("".into()).fork(ids.scalar_metadata_id),
        )
        .await
        .unwrap();
    metadata
        .delete::<_, u32>(PREFIX_FORMAT, SINGLETON_KEY)
        .await
        .unwrap();
    for (id, version) in [(10, 2), (11, 0x81), (12, 0x81)] {
        metadata.set(PREFIX_VERSION, id, version).await.unwrap();
    }
    for id in 13..13 + extra {
        metadata.set(PREFIX_VERSION, id, 1).await.unwrap();
    }
    let flusher = metadata.commit::<u32, u32>().await.unwrap();
    ids.scalar_metadata_id = flusher.id();
    flusher.flush::<u32, u32>().await.unwrap();
    drop(files);
    drop(writer);
    let mut changed = 0;
    for entry in std::fs::read_dir(dir.path().join("block")).unwrap() {
        let path = entry.unwrap().path();
        let batch = FileReader::try_new(std::fs::File::open(&path).unwrap(), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let Some(values) = batch.column(2).as_any().downcast_ref::<StructArray>() else {
            continue;
        };
        if values
            .fields()
            .first()
            .is_none_or(|field| field.name() != "codes")
        {
            continue;
        }
        let original_ids = values
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let mut versions = ListBuilder::new(UInt8Builder::new());
        let mut posting_ids = ListBuilder::new(UInt32Builder::new());
        for row in 0..values.len() {
            let list = original_ids.value(row);
            let list = list.as_any().downcast_ref::<UInt32Array>().unwrap();
            for &id in list.values() {
                posting_ids
                    .values()
                    .append_value(if id == 11 { 10 } else { id });
                versions
                    .values()
                    .append_value(if id == 10 && !duplicate_live {
                        1
                    } else if id == 10 || id == 11 {
                        2
                    } else {
                        1
                    });
            }
            versions.append(true);
            posting_ids.append(true);
        }
        let versions = versions.finish();
        let posting_ids = posting_ids.finish();
        let version_field = Arc::new(Field::new("versions", versions.data_type().clone(), true));
        let value = StructArray::from(vec![
            (values.fields()[0].clone(), values.column(0).clone()),
            (
                values.fields()[1].clone(),
                Arc::new(posting_ids) as ArrayRef,
            ),
            (version_field, Arc::new(versions) as ArrayRef),
        ]);
        let mut fields = batch.schema().fields().to_vec();
        fields[2] = Arc::new(Field::new(
            "value",
            DataType::Struct(value.fields().clone()),
            true,
        ));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                batch.column(0).clone(),
                batch.column(1).clone(),
                Arc::new(value),
            ],
        )
        .unwrap();
        let mut output = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut output, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        std::fs::write(&path, output).unwrap();
        changed += 1;
    }
    assert_eq!(changed, 1);
    ids
}

fn file_bytes(dir: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    let mut result = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            result.extend(file_bytes(&path));
        } else {
            result.insert(path.clone(), std::fs::read(path).unwrap());
        }
    }
    result
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_upgrade_filters_stale_and_deleted_rows_and_preserves_source() {
    let source_dir = tempfile::tempdir().unwrap();
    let output_dir = tempfile::tempdir().unwrap();
    let ids = legacy_fixture(&source_dir, false).await;
    let before = file_bytes(source_dir.path());
    let source = provider(&source_dir);
    let output = provider(&output_dir);
    assert!(HierarchicalSpannWriter::open(
        &source,
        ids.clone(),
        DistanceFunction::Euclidean,
        config()
    )
    .await
    .err()
    .unwrap()
    .to_string()
    .contains("requires explicit"));
    assert!(hierarchical_index::reader::HierarchicalSpannReader::open(
        &source,
        ids.clone(),
        DistanceFunction::Euclidean,
        config()
    )
    .await
    .err()
    .unwrap()
    .to_string()
    .contains("requires explicit"));
    let upgraded =
        hierarchical_index::writer::upgrade::upgrade_legacy_checkpoint(&source, &output, &ids)
            .await
            .unwrap();
    assert_eq!(upgraded.vector_count, 1);
    let writer = HierarchicalSpannWriter::open(
        &output,
        upgraded.ids.clone(),
        DistanceFunction::Euclidean,
        config(),
    )
    .await
    .unwrap();
    writer.load_all_postings().await.unwrap();
    assert_eq!(
        writer.root_reachable_valid_ids().unwrap(),
        [10].into_iter().collect()
    );
    let data = output
        .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(
            upgraded.ids.vector_data_id,
            "".into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        data.get(PREFIX_EMBEDDING, 10).await.unwrap().unwrap(),
        embedding(10)
    );
    assert!(data.get(PREFIX_EMBEDDING, 12).await.unwrap().is_none());
    assert_eq!(before, file_bytes(source_dir.path()));
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_upgrade_rejects_multiple_live_copies() {
    let source_dir = tempfile::tempdir().unwrap();
    let output_dir = tempfile::tempdir().unwrap();
    let ids = legacy_fixture(&source_dir, true).await;
    let before = file_bytes(source_dir.path());
    let error = hierarchical_index::writer::upgrade::upgrade_legacy_checkpoint(
        &provider(&source_dir),
        &provider(&output_dir),
        &ids,
    )
    .await
    .err()
    .unwrap();
    assert!(error.to_string().contains("multiple live copies"));
    assert_eq!(before, file_bytes(source_dir.path()));
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_upgrade_rejects_an_authoritative_vector_without_a_posting() {
    let dir = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let mut ids = legacy_fixture(&dir, false).await;
    let files = provider(&dir);
    let metadata = files
        .write::<u32, u32>(
            chroma_blockstore::BlockfileWriterOptions::new("".into()).fork(ids.scalar_metadata_id),
        )
        .await
        .unwrap();
    metadata.set(PREFIX_VERSION, 11, 1).await.unwrap();
    let flushed = metadata.commit::<u32, u32>().await.unwrap();
    ids.scalar_metadata_id = flushed.id();
    flushed.flush::<u32, u32>().await.unwrap();
    let error = hierarchical_index::writer::upgrade::upgrade_legacy_checkpoint(
        &provider(&dir),
        &provider(&output),
        &ids,
    )
    .await
    .err()
    .unwrap();
    assert!(error
        .to_string()
        .contains("authoritative live vector 11 has no reachable posting"));
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_upgrade_rejects_a_missing_immutable_embedding() {
    let dir = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let mut ids = legacy_fixture(&dir, false).await;
    let files = provider(&dir);
    let data = files
        .write::<u32, Vec<f32>>(
            chroma_blockstore::BlockfileWriterOptions::new("".into()).fork(ids.vector_data_id),
        )
        .await
        .unwrap();
    data.delete::<_, Vec<f32>>(PREFIX_EMBEDDING, 10)
        .await
        .unwrap();
    let flushed = data.commit::<u32, Vec<f32>>().await.unwrap();
    ids.vector_data_id = flushed.id();
    flushed.flush::<u32, Vec<f32>>().await.unwrap();
    let error = hierarchical_index::writer::upgrade::upgrade_legacy_checkpoint(
        &provider(&dir),
        &provider(&output),
        &ids,
    )
    .await
    .err()
    .unwrap();
    assert!(error.to_string().contains("missing embedding 10"));
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_upgrade_preserves_ids_across_output_flush_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let ids = legacy_fixture_sized(&dir, false, 4096).await;
    let files = provider(&output);
    let upgraded = hierarchical_index::writer::upgrade::upgrade_legacy_checkpoint(
        &provider(&dir),
        &files,
        &ids,
    )
    .await
    .unwrap();
    assert_eq!(upgraded.vector_count, 4097);
    let writer =
        HierarchicalSpannWriter::open(&files, upgraded.ids, DistanceFunction::Euclidean, config())
            .await
            .unwrap();
    writer.load_all_postings().await.unwrap();
    assert_eq!(
        writer.root_reachable_valid_ids().unwrap(),
        std::iter::once(10).chain(13..4109).collect()
    );
    writer.load_raw(&[10, 4108]).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_flush_retains_dirty_state_for_retry() {
    let dir = tempfile::tempdir().unwrap();
    let files = provider(&dir);
    let mut writer = HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, config());
    writer.add(10, &embedding(10)).unwrap();
    let flushed = Box::pin(writer.commit(&files, None)).await.unwrap();
    std::fs::write(dir.path().join("block"), b"force storage failure").unwrap();
    use futures::FutureExt;
    let failure = std::panic::AssertUnwindSafe(Box::pin(flushed.flush()))
        .catch_unwind()
        .await;
    assert!(failure.is_err() || failure.unwrap().is_err());
    assert_eq!(writer.memory_usage().dirty_embeddings_count, 1);
    std::fs::remove_file(dir.path().join("block")).unwrap();
    let ids = Box::pin(Box::pin(writer.commit(&files, None)).await.unwrap().flush())
        .await
        .unwrap();
    let reopened =
        HierarchicalSpannWriter::open(&files, ids, DistanceFunction::Euclidean, config())
            .await
            .unwrap();
    reopened.load_all_postings().await.unwrap();
    assert_eq!(
        reopened.root_reachable_valid_ids().unwrap(),
        [10].into_iter().collect()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_posting_lists_round_trip_with_the_version_free_schema() {
    let dir = tempfile::tempdir().unwrap();
    let files = provider(&dir);
    let mut writer = HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, config());
    let ids = Box::pin(Box::pin(writer.commit(&files, None)).await.unwrap().flush())
        .await
        .unwrap();
    let postings = files
        .read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
            ids.posting_list_id,
            "".into(),
        ))
        .await
        .unwrap();
    let posting = postings.get("", 0).await.unwrap().unwrap();
    assert!(posting.ids.is_empty());
    assert!(posting.codes.is_empty());
}
