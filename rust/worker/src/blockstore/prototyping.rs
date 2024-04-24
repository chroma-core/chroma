use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

// =====  Value Types  =====
// Example of a value type that references other data. (This would be data in the block)
struct NestedReferences<'value> {
    id: &'value [i32],
    value: &'value [i32],
}

trait Value {
    type Writeable<'writeable>: WriteableValue;
    type Readable<'referred_data>: ReadableValue<'referred_data>;
}

// Used for dynamic dispatch to get the type of the value.
// referred_data is the lifetime of the data that the value references (if any).
trait ReadableValue<'referred_data> {
    fn read_from_block(block: &'referred_data Block) -> Self;
}

trait WriteableValue {
    fn write_to_block(key: &str, value: &Self, block: &BlockBuilder);
}

// =====  Value Implementations  =====
impl<'referred_data> ReadableValue<'referred_data> for NestedReferences<'referred_data> {
    fn read_from_block(block: &'referred_data Block) -> Self {
        let id = block.id_storage.get("key").unwrap();
        let value = block.value_storage.get("key").unwrap();
        NestedReferences { id, value }
    }
}

impl<'referred_data> ReadableValue<'referred_data> for &'referred_data String {
    fn read_from_block(block: &'referred_data Block) -> Self {
        block.string_storage.get("key").unwrap()
    }
}

impl WriteableValue for NestedReferences<'_> {
    fn write_to_block(key: &str, value: &Self, block: &BlockBuilder) {
        block
            .id_storage
            .write()
            .as_mut()
            .unwrap()
            .insert(key.to_string(), value.id.to_vec());
        block
            .value_storage
            .write()
            .as_mut()
            .unwrap()
            .insert(key.to_string(), value.value.to_vec());
    }
}

impl WriteableValue for String {
    fn write_to_block(key: &str, value: &Self, block: &BlockBuilder) {
        block
            .string_storage
            .write()
            .as_mut()
            .unwrap()
            .insert(key.to_string(), value.clone());
    }
}

// =====  Block Provider & Cache  =====
// Thread-safe block cache and manager.
// This just loads blocks from the cache, but you could imagine it faults to s3
#[derive(Clone)]
struct BlockManager {
    read_block_cache: Arc<RwLock<HashMap<uuid::Uuid, Block>>>,
    write_block_builder_cache: Arc<RwLock<HashMap<uuid::Uuid, BlockBuilder>>>,
}

impl BlockManager {
    fn new() -> Self {
        Self {
            read_block_cache: Arc::new(RwLock::new(HashMap::new())),
            write_block_builder_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn get(&self, id: &uuid::Uuid) -> Option<Block> {
        let cache_guard = self.read_block_cache.read();
        let block = cache_guard.get(id)?.clone();
        Some(block)
    }

    fn create(&mut self) -> BlockBuilder {
        let builder = BlockBuilder::new(uuid::Uuid::new_v4());
        self.write_block_builder_cache
            .write()
            .insert(builder.id, builder.clone());
        builder
    }

    fn build(&mut self, id: uuid::Uuid) -> Block {
        let builder = self.write_block_builder_cache.write().remove(&id).unwrap();
        let block = builder.build();
        self.read_block_cache
            .write()
            .insert(block.id, block.clone());
        block
    }
}

// =====  Sparse Index  =====
#[derive(Clone)]
struct SparseIndex {
    storage: Vec<uuid::Uuid>,
}

impl SparseIndex {
    fn new() -> Self {
        Self {
            storage: Vec::new(),
        }
    }
}

// =====  Reader & Writer  =====
// Reader is a non-thread-safe reader that reads from the block store.
// Each thread can create its own reader, the blockmanager will handle the thread-safety
// of the block cache.
struct Reader<V> {
    sparse_index: SparseIndex,
    manager: BlockManager,
    // Used to keep a reference to the block, since we need to keep the block alive while we return values that reference it.
    loaded_blocks: HashMap<uuid::Uuid, Block>,
    marker: std::marker::PhantomData<V>,
}

trait ReaderTrait<'me, V> {
    fn get(&'me mut self, key: &str) -> Option<V>;
}

impl<'me, V: ReadableValue<'me>> ReaderTrait<'me, V> for Reader<V> {
    fn get(&'me mut self, key: &str) -> Option<V> {
        self.get(key)
    }
}

enum ReaderEnum<V> {
    Reader(Reader<V>),
}

impl<'me, V: ReadableValue<'me>> ReaderEnum<V> {
    fn get(&'me mut self, key: &str) -> Option<V> {
        match self {
            ReaderEnum::Reader(reader) => reader.get(key),
        }
    }
}

impl<'me, V> Reader<V>
where
    V: ReadableValue<'me>,
{
    fn from_sparse_index(sparse_index: SparseIndex, manager: BlockManager) -> Self {
        Self {
            sparse_index: sparse_index,
            manager,
            loaded_blocks: HashMap::new(),
            marker: std::marker::PhantomData,
        }
    }

    fn get(&'me mut self, key: &str) -> Option<V> {
        // Iterate over the blocks in reverse order to get the most recent value.
        // Since this implementation does not delete values from old blocks, the first value found is the most recent.
        for block_id in self.sparse_index.storage.iter().rev() {
            if !self.loaded_blocks.contains_key(block_id) {
                let block = self.manager.get(block_id)?;
                self.loaded_blocks.insert(*block_id, block);
            }
        }
        // This double for loop is to make the borrow-checker happy by scoping the mutable borrow of self.
        // It's not pretty, but it works.
        for block_id in self.sparse_index.storage.iter().rev() {
            let block = self.loaded_blocks.get(block_id).unwrap();
            let value = V::read_from_block(block);
            return Some(value);
        }
        None
    }
}

struct Writer {
    sparse_index: SparseIndex,
    active_block: Option<BlockBuilder>,
    block_manager: BlockManager,
}

impl Writer {
    fn new(mut block_manager: BlockManager) -> Self {
        let sparse_index = SparseIndex::new();
        let new_block = block_manager.create();
        Self {
            sparse_index: sparse_index,
            active_block: Some(new_block),
            block_manager: block_manager,
        }
    }

    fn from_sparse_index(sparse_index: SparseIndex, mut block_manager: BlockManager) -> Self {
        let new_block = block_manager.create();
        Self {
            sparse_index: sparse_index,
            active_block: Some(new_block),
            block_manager: block_manager,
        }
    }

    fn store<V: WriteableValue>(&self, key: String, value: &V) {
        V::write_to_block(&key, value, self.active_block.as_ref().unwrap());
    }

    fn commit(mut self) -> SparseIndex {
        // Concretize the active block
        let read_only_block = self
            .block_manager
            .build(self.active_block.as_ref().unwrap().id);
        self.sparse_index.storage.push(read_only_block.id);
        self.sparse_index

        // TODO: remove the write block when the writer is dropped. This will prevent leaking builders.
    }
}

struct HoldsWriter {
    writer: Writer,
}
// =====  Block Implementations  =====
#[derive(Clone)]
struct Block {
    // This pretends to be a RecordBatch in Arrow.
    // Storage for NestedReferences
    id_storage: Arc<HashMap<String, Vec<i32>>>,
    value_storage: Arc<HashMap<String, Vec<i32>>>,
    // Storage for String
    string_storage: Arc<HashMap<String, String>>,
    // Storage for the block id.
    id: uuid::Uuid,
}

// Mock BlockBuilder - it pretends like its arrow by allowing it to store ANY value type.
#[derive(Clone)]
struct BlockBuilder {
    // Storage for NestedReferences
    id_storage: Arc<RwLock<Option<HashMap<String, Vec<i32>>>>>,
    value_storage: Arc<RwLock<Option<HashMap<String, Vec<i32>>>>>,
    // Storage for String
    string_storage: Arc<RwLock<Option<HashMap<String, String>>>>,
    // Storage for the block id.
    id: uuid::Uuid,
}

impl BlockBuilder {
    fn new(id: uuid::Uuid) -> Self {
        Self {
            id_storage: Arc::new(RwLock::new(Some(HashMap::new()))),
            value_storage: Arc::new(RwLock::new(Some(HashMap::new()))),
            string_storage: Arc::new(RwLock::new(Some(HashMap::new()))),
            id,
        }
    }

    fn build(self) -> Block {
        Block {
            id_storage: self.id_storage.write().take().unwrap().into(),
            value_storage: self.value_storage.write().take().unwrap().into(),
            string_storage: self.string_storage.write().take().unwrap().into(),
            id: self.id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_self_reference_map() {
        let block_manager = BlockManager::new();
        let writer = Writer::new(block_manager.clone());
        let nested_references = NestedReferences {
            id: &[1, 2, 3],
            value: &[4, 5, 6],
        };
        writer.store("key".to_string(), &nested_references);
        let writer_sparse_index = writer.commit();

        let mut reader =
            Reader::<NestedReferences>::from_sparse_index(writer_sparse_index, block_manager);

        let value = reader.get("key").unwrap();
        assert_eq!(value.id, &[1, 2, 3]);
        assert_eq!(value.value, &[4, 5, 6]);
    }

    #[test]
    fn test_string_map() {
        let block_manager = BlockManager::new();
        let writer = Writer::new(block_manager.clone());
        let string = "value".to_string();
        writer.store("key".to_string(), &string);
        let writer_sparse_index = writer.commit();

        let mut reader = Reader::<&String>::from_sparse_index(writer_sparse_index, block_manager);

        let value = reader.get("key").unwrap();
        assert_eq!(value, "value");
    }

    #[test]
    fn test_non_static_self_reference_map() {
        let mut id_data_src = Vec::new();
        let mut value_data_src = Vec::new();

        for i in 0..10 {
            id_data_src.push(i);
            value_data_src.push(i + 10);
        }
        let id_data_src = id_data_src.as_slice();
        let value_data_src = value_data_src.as_slice();

        fn combine<'value>(id: &'value [i32], value: &'value [i32]) -> NestedReferences<'value> {
            NestedReferences { id, value }
        }
        let combined = combine(id_data_src, value_data_src);

        let block_manager = BlockManager::new();
        let writer = Writer::new(block_manager.clone());
        writer.store("key".to_string(), &combined);
        let writer_sparse_index = writer.commit();

        let mut reader =
            Reader::<NestedReferences>::from_sparse_index(writer_sparse_index, block_manager);

        let value = reader.get("key").unwrap();
        assert_eq!(value.id, id_data_src);
        assert_eq!(value.value, value_data_src);
    }

    #[test]
    fn test_reader_trait() {
        let block_manager = BlockManager::new();
        let writer = Writer::new(block_manager.clone());
        let string = "value".to_string();
        writer.store("key".to_string(), &string);
        let writer_sparse_index = writer.commit();

        // let mut reader = Reader::<&String>::from_sparse_index(writer_sparse_index, block_manager);
        // let mut reader: Box<dyn ReaderTrait<&String>> = Box::new(reader);
        let mut reader = ReaderEnum::Reader(Reader::<&String>::from_sparse_index(
            writer_sparse_index,
            block_manager,
        ));

        // RESUME: This is where the issue is. The compiler can't figure out the lifetime of the reader.
        let value = reader.get("key").unwrap();
        assert_eq!(value, "value");
    }
}
