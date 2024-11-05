use std::ops::Sub;

use crate::arrow::types::ArrowWriteableValue;

#[derive(Clone, Copy, Debug, Default)]
pub struct DataRecordSizeTracker {
    num_items: usize,
    prefix_size: usize,
    key_size: usize,
    id_size: usize,
    embedding_size: usize,
    metadata_size: usize,
    document_size: usize,
    embedding_dimension: Option<usize>,
}

impl Sub for DataRecordSizeTracker {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            num_items: self.num_items - rhs.num_items,
            prefix_size: self.prefix_size - rhs.prefix_size,
            key_size: self.key_size - rhs.key_size,
            id_size: self.id_size - rhs.id_size,
            embedding_size: self.embedding_size - rhs.embedding_size,
            metadata_size: self.metadata_size - rhs.metadata_size,
            document_size: self.document_size - rhs.document_size,
            embedding_dimension: self.embedding_dimension,
        }
    }
}

impl DataRecordSizeTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_num_items(&self) -> usize {
        self.num_items
    }

    pub fn get_prefix_size(&self) -> usize {
        self.prefix_size
    }

    pub fn get_key_size(&self) -> usize {
        self.key_size
    }

    pub fn get_id_size(&self) -> usize {
        self.id_size
    }

    pub fn get_embedding_size(&self) -> usize {
        self.embedding_size
    }

    pub fn get_metadata_size(&self) -> usize {
        self.metadata_size
    }

    pub fn get_document_size(&self) -> usize {
        self.document_size
    }

    pub fn get_embedding_dimension(&self) -> Option<usize> {
        self.embedding_dimension
    }

    pub fn add_prefix_size(&mut self, size: usize) {
        self.prefix_size += size;
    }

    pub fn add_key_size(&mut self, size: usize) {
        self.key_size += size;
    }

    pub fn subtract_prefix_size(&mut self, size: usize) {
        self.prefix_size -= size;
    }

    pub fn subtract_key_size(&mut self, size: usize) {
        self.key_size -= size;
    }

    pub fn add_value_size(
        &mut self,
        value: &<&chroma_types::DataRecord<'_> as ArrowWriteableValue>::PreparedValue,
    ) {
        let (id, embedding, metadata, document) = value;
        self.id_size += id.len();
        self.embedding_size += embedding.len() * std::mem::size_of::<f32>();
        self.metadata_size += metadata.as_ref().map(|m| m.len()).unwrap_or(0);
        self.document_size += document.as_ref().map(|d| d.len()).unwrap_or(0);
        self.embedding_dimension = Some(embedding.len()); // todo: return error if embedding size has changed
    }

    pub fn subtract_value_size(
        &mut self,
        value: &<&chroma_types::DataRecord<'_> as ArrowWriteableValue>::PreparedValue,
    ) {
        let (id, embedding, metadata, document) = value;
        self.id_size -= id.len();
        self.embedding_size -= embedding.len() * std::mem::size_of::<f32>();
        self.metadata_size -= metadata.as_ref().map(|m| m.len()).unwrap_or(0);
        self.document_size -= document.as_ref().map(|d| d.len()).unwrap_or(0);
    }

    pub fn increment_item_count(&mut self) {
        self.num_items += 1;
    }

    pub fn decrement_item_count(&mut self) {
        self.num_items -= 1;
    }
}
