use crate::chroma_proto;
use crate::Metadata;
use prost::Message;

#[derive(Debug, Clone)]
pub struct DataRecord<'a> {
    pub id: &'a str,
    pub embedding: &'a [f32],
    pub metadata: Option<Metadata>,
    pub document: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct OwnedDataRecord {
    pub id: String,
    pub embedding: Vec<f32>,
    pub metadata: Option<Metadata>,
    pub document: Option<String>,
}

impl<'a> From<&DataRecord<'a>> for OwnedDataRecord {
    fn from(data_record: &DataRecord<'a>) -> Self {
        let id = data_record.id.to_string();
        let embedding = data_record.embedding.to_vec();
        let metadata = data_record.metadata.clone();
        let document = data_record.document.map(|doc| doc.to_string());
        OwnedDataRecord {
            id,
            embedding,
            metadata,
            document,
        }
    }
}

impl DataRecord<'_> {
    pub fn get_size(&self) -> usize {
        let id_size = self.id.len();
        let embedding_size = std::mem::size_of_val(self.embedding);
        let metadata_size = match &self.metadata {
            Some(metadata) => {
                let metadata_proto = Into::<chroma_proto::UpdateMetadata>::into(metadata.clone());
                let metadata_as_bytes = metadata_proto.encode_to_vec();
                metadata_as_bytes.len()
            }
            None => 0,
        };
        let document_size = match self.document {
            Some(document) => document.len(),
            None => 0,
        };
        id_size + embedding_size + metadata_size + document_size
    }

    pub fn to_owned(&self) -> OwnedDataRecord {
        self.into()
    }
}
