use crate::Metadata;

#[derive(Debug, Clone)]
pub struct DataRecord<'a> {
    pub id: &'a str,
    pub embedding: &'a [f32],
    pub metadata: Option<Metadata>,
    pub document: Option<&'a str>,
}

impl DataRecord<'_> {
    pub fn get_size(&self) -> usize {
        let id_size = self.id.len();
        let embedding_size = self.embedding.len() * std::mem::size_of::<f32>();
        // TODO: use serialized_metadata size to calculate the size
        let metadata_size = 0;
        let document_size = match self.document {
            Some(document) => document.len(),
            None => 0,
        };
        id_size + embedding_size + metadata_size + document_size
    }
}
