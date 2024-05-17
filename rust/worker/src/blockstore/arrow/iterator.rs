use super::{
    blockfile::ArrowBlockfileReader,
    types::{ArrowReadableKey, ArrowReadableValue},
};
use crate::blockstore::Value;
use async_stream::try_stream;
use futures::Stream;

pub(crate) struct ArrowBlockfileIterator<
    'me,
    K: ArrowReadableKey<'me>,
    V: ArrowReadableValue<'me> + Value,
> {
    reader: &'me ArrowBlockfileReader<'me, K, V>,
    // The index within the current block
    index: usize,
    // The index of the current block from the perspective of the sparse index of the reader
    block_index: usize,
}

impl<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me> + Value>
    ArrowBlockfileIterator<'me, K, V>
{
    pub fn new(reader: &'me ArrowBlockfileReader<'me, K, V>) -> Self {
        Self {
            reader,
            index: 0,
            block_index: 0,
        }
    }

    pub fn as_stream(
        &'me self,
    ) -> impl Stream<Item = Result<std::option::Option<(&'me str, K, V)>, ()>> {
        try_stream! {
            let sparse_index = &self.reader.sparse_index;
            let mut block_index = 0;
            while let Some(block_uuid) = sparse_index.forward.lock().iter().nth(block_index).map(|(_, uuid)| *uuid) {
                // TODO: don't unwrap
                let block = self.reader.get_block(block_uuid).await.unwrap();
                for i in 0..block.len() {
                    let res = block.get_at_index::<'me, K, V>(i);
                    yield res;
                }
                block_index += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{pin_mut, StreamExt};

    use super::*;
    use crate::{
        blockstore::{arrow::provider::ArrowBlockfileProvider, BlockfileReader},
        storage::{local::LocalStorage, Storage},
    };

    #[tokio::test]
    async fn test_iterator() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let blockfile_provider = ArrowBlockfileProvider::new(storage);

        let writer = blockfile_provider.create::<&str, &str>().unwrap();
        let id = writer.id();

        writer.set("prefix1", "key1", "value1").await.unwrap();
        writer.set("prefix1", "key2", "value2").await.unwrap();
        writer.set("prefix2", "key1", "value3").await.unwrap();
        writer.commit::<&str, &str>().unwrap();

        let reader = blockfile_provider.open::<&str, &str>(&id).await.unwrap();
        let reader = match reader {
            BlockfileReader::ArrowBlockfileReader(reader) => reader,
            _ => panic!("Invalid reader type"),
        };
        let iterator = ArrowBlockfileIterator::new(&reader);
        let stream = iterator.as_stream();
        pin_mut!(stream);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first, Some(("prefix1", "key1", "value1")));
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second, Some(("prefix1", "key2", "value2")));
        let third = stream.next().await.unwrap().unwrap();
        assert_eq!(third, Some(("prefix2", "key1", "value3")));
        let fourth = stream.next().await;
        assert_eq!(fourth, None);
    }
}
