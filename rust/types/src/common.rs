use std::fmt;

#[derive(Debug, Clone, Copy)]
pub enum ChromaS3FilePrefixes {
    HnswIndexFilePrefix,
    BlockfileIndexFilePrefix,
    RenamedFilePrefix,
    DeleteListFilePrefix,
}

impl fmt::Display for ChromaS3FilePrefixes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", String::from(*self))
    }
}

impl From<ChromaS3FilePrefixes> for String {
    fn from(value: ChromaS3FilePrefixes) -> Self {
        match value {
            ChromaS3FilePrefixes::HnswIndexFilePrefix => "hnsw/".to_string(),
            ChromaS3FilePrefixes::BlockfileIndexFilePrefix => "block/".to_string(),
            ChromaS3FilePrefixes::RenamedFilePrefix => "gc/renamed/".to_string(),
            ChromaS3FilePrefixes::DeleteListFilePrefix => "gc/delete-list/".to_string(),
        }
    }
}
