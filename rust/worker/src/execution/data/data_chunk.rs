use std::sync::Arc;

use crate::types::EmbeddingRecord;

#[derive(Clone, Debug)]
pub(crate) struct DataChunk {
    data: Arc<[EmbeddingRecord]>,
    visibility: Arc<[bool]>,
}

impl DataChunk {
    pub fn new(data: Arc<[EmbeddingRecord]>) -> Self {
        let len = data.len();
        DataChunk {
            data,
            visibility: vec![true; len].into(),
        }
    }

    /// Returns the total length of the data chunk
    pub fn total_len(&self) -> usize {
        self.data.len()
    }

    /// Returns the number of visible elements in the data chunk
    pub fn len(&self) -> usize {
        self.visibility.iter().filter(|&v| *v).count()
    }

    /// Returns the element at the given index
    /// if the index is out of bounds, it returns None
    /// # Arguments
    /// * `index` - The index of the element
    pub fn get(&self, index: usize) -> Option<&EmbeddingRecord> {
        if index < self.data.len() {
            Some(&self.data[index])
        } else {
            None
        }
    }

    /// Returns the visibility of the element at the given index
    /// if the index is out of bounds, it returns None
    /// # Arguments
    /// * `index` - The index of the element
    pub fn get_visibility(&self, index: usize) -> Option<bool> {
        if index < self.visibility.len() {
            Some(self.visibility[index])
        } else {
            None
        }
    }

    /// Sets the visibility of the elements in the data chunk.
    /// Note that the length of the visibility vector should be
    /// equal to the length of the data chunk.
    ///
    /// Note that this is the only way to change the visibility of the elements in the data chunk,
    /// the data chunk does not provide a way to change the visibility of individual elements.
    /// This is to ensure that the visibility of the elements is always in sync with the data.
    /// If you want to change the visibility of individual elements, you should create a new data chunk.
    ///
    /// # Arguments
    /// * `visibility` - A vector of boolean values indicating the visibility of the elements
    pub fn set_visibility(&mut self, visibility: Vec<bool>) {
        self.visibility = visibility.into();
    }

    /// Returns an iterator over the visible elements in the data chunk
    /// The iterator returns a tuple of the element and its index
    /// # Returns
    /// An iterator over the visible elements in the data chunk
    pub fn iter(&self) -> DataChunkIteraror<'_> {
        DataChunkIteraror {
            chunk: self,
            index: 0,
        }
    }
}

pub(crate) struct DataChunkIteraror<'a> {
    chunk: &'a DataChunk,
    index: usize,
}

impl<'a> Iterator for DataChunkIteraror<'a> {
    type Item = (&'a EmbeddingRecord, usize);

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.chunk.total_len() {
            let index = self.index;
            match self.chunk.get_visibility(index) {
                Some(true) => {
                    self.index += 1;
                    return self.chunk.get(index).map(|record| (record, index));
                }
                Some(false) => {
                    self.index += 1;
                }
                None => {
                    break;
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EmbeddingRecord;
    use crate::types::Operation;
    use num_bigint::BigInt;
    use std::str::FromStr;
    use uuid::Uuid;

    #[test]
    fn test_data_chunk() {
        let collection_uuid_1 = Uuid::from_str("00000000-0000-0000-0000-000000000001").unwrap();
        let data = vec![
            EmbeddingRecord {
                id: "embedding_id_1".to_string(),
                seq_id: BigInt::from(1),
                embedding: None,
                encoding: None,
                metadata: None,
                operation: Operation::Add,
                collection_id: collection_uuid_1,
            },
            EmbeddingRecord {
                id: "embedding_id_2".to_string(),
                seq_id: BigInt::from(2),
                embedding: None,
                encoding: None,
                metadata: None,
                operation: Operation::Add,
                collection_id: collection_uuid_1,
            },
        ];
        let data = data.into();
        let mut chunk = DataChunk::new(data);
        assert_eq!(chunk.len(), 2);
        let mut iter = chunk.iter();
        let elem = iter.next();
        assert_eq!(elem.is_some(), true);
        let (record, index) = elem.unwrap();
        assert_eq!(record.id, "embedding_id_1");
        assert_eq!(index, 0);
        let elem = iter.next();
        assert_eq!(elem.is_some(), true);
        let (record, index) = elem.unwrap();
        assert_eq!(record.id, "embedding_id_2");
        assert_eq!(index, 1);
        let elem = iter.next();
        assert_eq!(elem.is_none(), true);

        let visibility = vec![true, false].into();
        chunk.set_visibility(visibility);
        assert_eq!(chunk.len(), 1);
        let mut iter = chunk.iter();
        let elem = iter.next();
        assert_eq!(elem.is_some(), true);
        let (record, index) = elem.unwrap();
        assert_eq!(record.id, "embedding_id_1");
        assert_eq!(index, 0);
        let elem = iter.next();
        assert_eq!(elem.is_none(), true);
    }
}
