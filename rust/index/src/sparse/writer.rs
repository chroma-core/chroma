use std::collections::HashMap;

use chroma_blockstore::BlockfileWriter;

#[derive(Clone, Default)]
pub struct SparseDelta {
    // Posting list ID -> Offset ID -> Upsert/Delete value
    value_updates: HashMap<u32, (u32, Option<f32>)>,
}

impl SparseDelta {
    pub fn create(&self, offset_id: u32, sparse_vector: impl IntoIterator<Item = (u32, f32)>) {}
}
