use crate::arrow::provider::BlockfileReaderOptions;
use crate::{test_arrow_blockfile_provider, BlockfileReader, BlockfileWriterOptions};

#[tokio::test]
async fn test_compaction_owned_prefix_releases_input_blocks() {
    let (_dir, provider) = test_arrow_blockfile_provider(1024);
    let crate::BlockfileProvider::ArrowBlockfileProvider(provider) = provider else {
        panic!("expected Arrow provider")
    };
    let provider = provider.with_compaction_memory_options(true, false);
    let writer = provider
        .write::<u32, f32>(BlockfileWriterOptions::new(String::new()).ordered_mutations())
        .await
        .unwrap();
    let id = writer.id();
    for prefix in ["a", "b", "c"] {
        for key in 0..1000 {
            writer.set(prefix, key, key as f32).await.unwrap();
        }
    }
    writer
        .commit::<u32, f32>()
        .await
        .unwrap()
        .flush::<u32, f32>()
        .await
        .unwrap();
    let BlockfileReader::ArrowBlockfileReader(reader) = provider
        .read::<u32, f32>(BlockfileReaderOptions::new(id, String::new()))
        .await
        .unwrap()
    else {
        panic!("expected Arrow reader")
    };
    for prefix in ["a", "b", "c"] {
        let values = reader.get_prefix_owned(prefix).await.unwrap();
        assert_eq!(
            values,
            (0..1000).map(|key| (key, key as f32)).collect::<Vec<_>>()
        );
        assert!(
            reader.loaded_blocks.read().is_empty(),
            "consumed blocks remain pinned by reader"
        );
    }
}

#[tokio::test]
async fn test_compaction_owned_reads_preserve_existing_borrowed_values() {
    let (_dir, provider) = test_arrow_blockfile_provider(1024);
    let crate::BlockfileProvider::ArrowBlockfileProvider(provider) = provider else {
        panic!("expected Arrow provider")
    };
    let provider = provider.with_compaction_memory_options(true, false);
    let writer = provider
        .write::<u32, f32>(BlockfileWriterOptions::new(String::new()).ordered_mutations())
        .await
        .unwrap();
    let id = writer.id();
    writer.set("a", 1, 2.0).await.unwrap();
    writer.set("b", 2, 3.0).await.unwrap();
    writer
        .commit::<u32, f32>()
        .await
        .unwrap()
        .flush::<u32, f32>()
        .await
        .unwrap();
    let BlockfileReader::ArrowBlockfileReader(reader) = provider
        .read::<u32, f32>(BlockfileReaderOptions::new(id, String::new()))
        .await
        .unwrap()
    else {
        panic!("expected Arrow reader")
    };
    let borrowed = reader.get_prefix("a").await.unwrap();
    let pinned = reader.loaded_blocks.read().len();
    assert_eq!(reader.get_prefix_owned("b").await.unwrap(), vec![(2, 3.0)]);
    assert_eq!(reader.loaded_blocks.read().len(), pinned);
    assert_eq!(borrowed.collect::<Vec<_>>(), vec![(1, 2.0)]);
    assert!(reader.get_prefix_owned("absent").await.unwrap().is_empty());
}

#[tokio::test]
async fn test_compaction_owned_reads_report_missing_block() {
    let (dir, provider) = test_arrow_blockfile_provider(1024);
    let crate::BlockfileProvider::ArrowBlockfileProvider(provider) = provider else {
        panic!("expected Arrow provider")
    };
    let provider = provider.with_compaction_memory_options(true, false);
    let writer = provider
        .write::<u32, f32>(BlockfileWriterOptions::new(String::new()).ordered_mutations())
        .await
        .unwrap();
    let id = writer.id();
    writer.set("a", 1, 2.0).await.unwrap();
    writer
        .commit::<u32, f32>()
        .await
        .unwrap()
        .flush::<u32, f32>()
        .await
        .unwrap();
    let BlockfileReader::ArrowBlockfileReader(reader) = provider
        .read::<u32, f32>(BlockfileReaderOptions::new(id, String::new()))
        .await
        .unwrap()
    else {
        panic!("expected Arrow reader")
    };
    let block_id = reader
        .root
        .sparse_index
        .data
        .forward
        .values()
        .next()
        .unwrap()
        .id;
    std::fs::remove_file(dir.path().join("block").join(block_id.to_string())).unwrap();
    provider.clear().await.unwrap();
    assert!(reader.get_prefix_owned("a").await.is_err());
    assert!(reader.loaded_blocks.read().is_empty());
}
