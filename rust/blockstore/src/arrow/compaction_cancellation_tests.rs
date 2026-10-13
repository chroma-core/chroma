use crate::arrow::provider::BlockfileReaderOptions;
use crate::{
    test_arrow_blockfile_provider, BlockfileProvider, BlockfileWriter, BlockfileWriterOptions,
};
use std::sync::Arc;
use tokio::sync::Notify;

#[tokio::test]
async fn test_compaction_cancelled_upload_cannot_publish() {
    let (_dir, provider) = test_arrow_blockfile_provider(1024);
    let BlockfileProvider::ArrowBlockfileProvider(provider) = provider else {
        panic!("expected Arrow provider")
    };
    let provider = provider.with_compaction_memory_options(false, true);
    let BlockfileWriter::ArrowOrderedBlockfileWriter(writer) = provider
        .write::<u32, f32>(BlockfileWriterOptions::new(String::new()).ordered_mutations())
        .await
        .unwrap()
    else {
        panic!("expected ordered Arrow writer")
    };
    let id = writer.id();
    let started = Arc::new(Notify::new());
    writer.inner.lock().await.upload_pause = Some((started.clone(), Arc::new(Notify::new())));
    let active_writer = writer.clone();
    let task = tokio::spawn(async move {
        for key in 0..10000 {
            active_writer.set("a", key, key as f32).await.unwrap();
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), started.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(writer.set("a", 10001, 1.0).await.is_err());
    assert!(writer.commit::<u32, f32>().await.is_err());
    assert!(provider
        .read::<u32, f32>(BlockfileReaderOptions::new(id, String::new()))
        .await
        .is_err());
}
