use std::sync::Arc;

#[derive(Debug)]
pub struct Chunk<T> {
    data: Arc<[T]>,
    visibility: Arc<[bool]>,
}

impl<T> Clone for Chunk<T> {
    fn clone(&self) -> Self {
        Chunk {
            data: self.data.clone(),
            visibility: self.visibility.clone(),
        }
    }
}

impl<T> Chunk<T> {
    pub fn new(data: Arc<[T]>) -> Self {
        let len = data.len();
        Chunk {
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

    /// Returns whether the chunk has zero visible elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the element at the given index
    /// if the index is out of bounds, it returns None
    /// # Arguments
    /// * `index` - The index of the element
    pub fn get(&self, index: usize) -> Option<&T> {
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
    pub fn iter(&self) -> DataChunkIteraror<'_, T> {
        DataChunkIteraror {
            chunk: self,
            index: 0,
        }
    }
}

pub struct DataChunkIteraror<'a, T> {
    chunk: &'a Chunk<T>,
    index: usize,
}

impl<'a, T> Iterator for DataChunkIteraror<'a, T> {
    type Item = (&'a T, usize);

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
    use crate::{LogRecord, Operation, OperationRecord};

    #[test]
    fn test_data_chunk() {
        let data = vec![
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
        ];
        let data = data.into();
        let mut chunk = Chunk::new(data);
        assert_eq!(chunk.len(), 2);
        let mut iter = chunk.iter();
        let elem = iter.next();
        assert!(elem.is_some());
        let (record, index) = elem.unwrap();
        assert_eq!(record.record.id, "embedding_id_1");
        assert_eq!(index, 0);
        let elem = iter.next();
        assert!(elem.is_some());
        let (record, index) = elem.unwrap();
        assert_eq!(record.record.id, "embedding_id_2");
        assert_eq!(index, 1);
        let elem = iter.next();
        assert!(elem.is_none());

        let visibility = vec![true, false];
        chunk.set_visibility(visibility);
        assert_eq!(chunk.len(), 1);
        let mut iter = chunk.iter();
        let elem = iter.next();
        assert!(elem.is_some());
        let (record, index) = elem.unwrap();
        assert_eq!(record.record.id, "embedding_id_1");
        assert_eq!(index, 0);
        let elem = iter.next();
        assert!(elem.is_none());
    }
}
