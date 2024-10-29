use std::ops::Sub;

use crate::arrow::types::ArrowWriteableValue;

#[derive(Clone, Copy, Debug, Default)]
pub struct SpannPostingListSizeTracker {
    num_items: usize,
    prefix_size: usize,
    key_size: usize,
    doc_offset_ids_size: usize,
    doc_versions_size: usize,
    doc_embeddings_size: usize,
    embedding_dimension: Option<usize>,
}

impl Sub for SpannPostingListSizeTracker {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            num_items: self.num_items - rhs.num_items,
            prefix_size: self.prefix_size - rhs.prefix_size,
            key_size: self.key_size - rhs.key_size,
            doc_offset_ids_size: self.doc_offset_ids_size - rhs.doc_offset_ids_size,
            doc_versions_size: self.doc_versions_size - rhs.doc_versions_size,
            doc_embeddings_size: self.doc_embeddings_size - rhs.doc_embeddings_size,
            embedding_dimension: self.embedding_dimension,
        }
    }
}

impl SpannPostingListSizeTracker {
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

    pub fn get_embedding_dimension(&self) -> Option<usize> {
        self.embedding_dimension
    }

    pub fn get_doc_offset_ids_size(&self) -> usize {
        self.doc_offset_ids_size
    }

    pub fn get_doc_versions_size(&self) -> usize {
        self.doc_versions_size
    }

    pub fn get_doc_embeddings_size(&self) -> usize {
        self.doc_embeddings_size
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
        value: &<&chroma_types::SpannPostingList<'_> as ArrowWriteableValue>::PreparedValue,
    ) {
        let (doc_offset_ids, doc_versions, doc_embeddings) = value;
        self.doc_offset_ids_size += std::mem::size_of_val(doc_offset_ids);
        self.doc_versions_size += std::mem::size_of_val(doc_versions);
        self.doc_embeddings_size += std::mem::size_of_val(doc_embeddings);
        self.embedding_dimension = Some(doc_embeddings.len() / doc_offset_ids.len());
    }

    pub fn subtract_value_size(
        &mut self,
        value: &<&chroma_types::SpannPostingList<'_> as ArrowWriteableValue>::PreparedValue,
    ) {
        let (doc_offset_ids, doc_versions, doc_embeddings) = value;
        self.doc_offset_ids_size -= std::mem::size_of_val(doc_offset_ids);
        self.doc_versions_size -= std::mem::size_of_val(doc_versions);
        self.doc_embeddings_size -= std::mem::size_of_val(doc_embeddings);
    }

    pub fn increment_item_count(&mut self) {
        self.num_items += 1;
    }

    pub fn decrement_item_count(&mut self) {
        self.num_items -= 1;
    }
}
