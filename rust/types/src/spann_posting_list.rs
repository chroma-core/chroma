#[derive(Clone, Debug)]
pub struct SpannPostingList<'referred_data> {
    pub doc_offset_ids: &'referred_data [u32],
    pub doc_versions: &'referred_data [u32],
    // Flattened out embeddings for all documents.
    // Can extract individual embedding by slicing
    // the 1D array at the suitable start and end offset.
    pub doc_embeddings: &'referred_data [f32],
}

impl SpannPostingList<'_> {
    pub fn compute_size(&self) -> usize {
        let doc_offset_ids_size = std::mem::size_of_val(self.doc_offset_ids);
        let doc_versions_size = std::mem::size_of_val(self.doc_versions);
        let doc_embeddings_size = std::mem::size_of_val(self.doc_embeddings);
        doc_offset_ids_size + doc_versions_size + doc_embeddings_size
    }
}
