use super::ArrowOrderedBlockfileWriter;
use crate::arrow::provider::BlockfileReaderOptions;
use crate::{
    test_arrow_blockfile_provider, BlockfileProvider, BlockfileWriter, BlockfileWriterOptions,
};

async fn ordered_writer(
    provider: &BlockfileProvider,
    fork: Option<uuid::Uuid>,
) -> ArrowOrderedBlockfileWriter {
    let mut options = BlockfileWriterOptions::new(String::new()).ordered_mutations();
    if let Some(id) = fork {
        options = options.fork(id);
    }
    let BlockfileWriter::ArrowOrderedBlockfileWriter(writer) =
        provider.write::<u32, f32>(options).await.unwrap()
    else {
        panic!("expected ordered Arrow writer")
    };
    writer
}

fn enable_uploads(provider: BlockfileProvider) -> BlockfileProvider {
    let BlockfileProvider::ArrowBlockfileProvider(provider) = provider else {
        panic!("expected Arrow provider")
    };
    BlockfileProvider::ArrowBlockfileProvider(provider.with_compaction_memory_options(false, true))
}

#[tokio::test]
async fn test_compaction_completed_output_upload_boundary() {
    let (dir, provider) = test_arrow_blockfile_provider(1024);
    let provider = enable_uploads(provider);
    let writer = ordered_writer(&provider, None).await;
    let id = writer.id();
    let limit = writer.block_manager.num_concurrent_block_flushes();
    assert!(limit > 1);
    let mut key = 0;

    for (completed, retained, uploaded) in [
        (limit - 1, limit - 1, 0),
        (limit, 0, limit),
        (limit + 1, 1, limit),
    ] {
        loop {
            let inner = writer.inner.lock().await;
            let produced = inner.completed_blocks.len() + inner.uploaded_block_ids.len();
            drop(inner);
            if produced >= completed {
                assert_eq!(produced, completed);
                break;
            }
            assert!(key < 10000, "writer never reaches the batch boundary");
            Box::pin(writer.set("a", key, key as f32)).await.unwrap();
            key += 1;
        }
        let inner = writer.inner.lock().await;
        assert_eq!(inner.completed_blocks.len(), retained);
        assert_eq!(inner.uploaded_block_ids.len(), uploaded);
        drop(inner);
        let block_dir = dir.path().join("block");
        let stored = if block_dir.exists() {
            block_dir.read_dir().unwrap().count()
        } else {
            0
        };
        assert_eq!(stored, uploaded, "wrong upload count at {completed} blocks");
        assert!(provider
            .read::<u32, f32>(BlockfileReaderOptions::new(id, String::new()))
            .await
            .is_err());
    }

    Box::pin(writer.commit::<u32, f32>())
        .await
        .unwrap()
        .flush::<u32, f32>()
        .await
        .unwrap();
    provider.clear().await.unwrap();
    let reader = provider
        .read::<u32, f32>(BlockfileReaderOptions::new(id, String::new()))
        .await
        .unwrap();
    assert_eq!(reader.count().await.unwrap(), key as usize);
    assert_eq!(
        reader.get_prefix("a").await.unwrap().collect::<Vec<_>>(),
        (0..key).map(|key| (key, key as f32)).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_compaction_completed_output_is_bounded() {
    let (dir, provider) = test_arrow_blockfile_provider(1024);
    let provider = enable_uploads(provider);
    let writer = ordered_writer(&provider, None).await;
    let id = writer.id();
    for key in 0..10000 {
        writer.set("a", key, key as f32).await.unwrap();
        assert!(
            writer.inner.lock().await.completed_blocks.len() < 40,
            "completed output buffers accumulate instead of uploading"
        );
    }
    assert!(dir.path().join("block").read_dir().unwrap().count() > 0);
    assert!(
        provider
            .read::<u32, f32>(BlockfileReaderOptions::new(id, String::new()))
            .await
            .is_err(),
        "an incomplete index must remain unpublished"
    );
    let flusher = writer.commit::<u32, f32>().await.unwrap();
    assert_eq!(flusher.count(), 10000);
    assert_eq!(flusher.num_entries(), 10000);
    assert_eq!(flusher.pending_blocks(), 0);
    flusher.flush::<u32, f32>().await.unwrap();
    provider.clear().await.unwrap();
    let reader = provider
        .read::<u32, f32>(BlockfileReaderOptions::new(id, String::new()))
        .await
        .unwrap();
    assert_eq!(reader.count().await.unwrap(), 10000);
    assert_eq!(
        reader.get_prefix("a").await.unwrap().collect::<Vec<_>>(),
        (0..10000).map(|key| (key, key as f32)).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_compaction_upload_failure_preserves_published_input() {
    let (dir, provider) = test_arrow_blockfile_provider(1024);
    let initial = ordered_writer(&provider, None).await;
    let old_id = initial.id();
    for key in 0..10 {
        initial.set("a", key, key as f32).await.unwrap();
    }
    initial
        .commit::<u32, f32>()
        .await
        .unwrap()
        .flush::<u32, f32>()
        .await
        .unwrap();
    let provider = enable_uploads(provider);
    let writer = ordered_writer(&provider, Some(old_id)).await;
    writer.set("a", 0, 999.0).await.unwrap();
    let new_id = writer.id();
    let block_id = writer
        .inner
        .lock()
        .await
        .current_block_delta
        .as_ref()
        .unwrap()
        .0
        .id;
    // A directory at the destination makes LocalStorage return a write error.
    std::fs::create_dir_all(dir.path().join("block").join(block_id.to_string())).unwrap();
    assert!(writer.clone().commit::<u32, f32>().await.is_err());
    assert!(writer.set("a", 1, 1.0).await.is_err());
    assert!(writer.commit::<u32, f32>().await.is_err());
    provider.clear().await.unwrap();
    assert!(provider
        .read::<u32, f32>(BlockfileReaderOptions::new(new_id, String::new()))
        .await
        .is_err());
    let reader = provider
        .read::<u32, f32>(BlockfileReaderOptions::new(old_id, String::new()))
        .await
        .unwrap();
    assert_eq!(reader.get("a", 0).await.unwrap(), Some(0.0));
    assert_eq!(reader.count().await.unwrap(), 10);
}

#[tokio::test]
async fn test_compaction_incremental_upload_delete_all() {
    let (_dir, provider) = test_arrow_blockfile_provider(1024);
    let provider = enable_uploads(provider);
    let writer = ordered_writer(&provider, None).await;
    let old_id = writer.id();
    for key in 0..1000 {
        writer.set("a", key, key as f32).await.unwrap();
    }
    writer
        .commit::<u32, f32>()
        .await
        .unwrap()
        .flush::<u32, f32>()
        .await
        .unwrap();
    let writer = ordered_writer(&provider, Some(old_id)).await;
    let id = writer.id();
    for key in 0..1000 {
        writer.delete::<u32, f32>("a", key).await.unwrap();
    }
    let flusher = writer.commit::<u32, f32>().await.unwrap();
    assert_eq!(flusher.count(), 0);
    flusher.flush::<u32, f32>().await.unwrap();
    provider.clear().await.unwrap();
    let reader = provider
        .read::<u32, f32>(BlockfileReaderOptions::new(id, String::new()))
        .await
        .unwrap();
    assert_eq!(reader.count().await.unwrap(), 0);
    assert!(reader.get_prefix("a").await.unwrap().next().is_none());
}

#[tokio::test]
async fn test_compaction_batch_upload_failure_poisoned_writer() {
    let (dir, provider) = test_arrow_blockfile_provider(1024);
    let provider = enable_uploads(provider);
    let writer = ordered_writer(&provider, None).await;
    let id = writer.id();
    let mut key = 0;
    while writer.inner.lock().await.completed_blocks.len() < 39 {
        assert!(key < 10000);
        writer.set("a", key, key as f32).await.unwrap();
        key += 1;
    }
    let block_id = writer
        .inner
        .lock()
        .await
        .current_block_delta
        .as_ref()
        .unwrap()
        .0
        .id;
    std::fs::create_dir_all(dir.path().join("block").join(block_id.to_string())).unwrap();
    loop {
        assert!(key < 10000);
        if writer.set("a", key, key as f32).await.is_err() {
            break;
        }
        key += 1;
    }
    assert!(writer.commit::<u32, f32>().await.is_err());
    assert!(provider
        .read::<u32, f32>(BlockfileReaderOptions::new(id, String::new()))
        .await
        .is_err());
}
