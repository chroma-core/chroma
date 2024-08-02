use arrow::{
    array::{Array, AsArray, Int32Array, Int32Builder, ListArray, ListBuilder},
    datatypes::Int32Type,
};
use chroma_error::{ChromaError, ErrorCodes};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug, Clone)]
pub(crate) struct PositionalPostingList {
    pub(crate) doc_ids: Int32Array,
    pub(crate) positions: ListArray,
}

impl PositionalPostingList {
    pub(crate) fn get_doc_ids(&self) -> Int32Array {
        return self.doc_ids.clone();
    }

    pub(crate) fn get_positions_for_doc_id(&self, doc_id: i32) -> Option<Int32Array> {
        let index = self.doc_ids.values().binary_search(&doc_id).ok();
        match index {
            Some(index) => {
                let target_positions = self.positions.value(index);
                // Int32Array is composed of a Datatype, ScalarBuffer, and a null bitmap, these are all cheap to clone since the buffer is Arc'ed
                let downcast = target_positions.as_primitive::<Int32Type>().clone();
                return Some(downcast);
            }
            None => None,
        }
    }

    pub(crate) fn size_in_bytes(&self) -> usize {
        let mut size = 0;
        size += self.doc_ids.len() * std::mem::size_of::<i32>();
        size += self.positions.len() * std::mem::size_of::<i32>();
        size
    }
}

#[derive(Error, Debug)]
pub(crate) enum PositionalPostingListBuilderError {
    #[error("Doc ID already exists in the list")]
    DocIdAlreadyExists,
    #[error("Doc ID does not exist in the list")]
    DocIdDoesNotExist,
    #[error("Incremental positions must be sorted")]
    UnsortedPosition,
}

impl ChromaError for PositionalPostingListBuilderError {
    fn code(&self) -> ErrorCodes {
        match self {
            PositionalPostingListBuilderError::DocIdAlreadyExists => ErrorCodes::AlreadyExists,
            PositionalPostingListBuilderError::DocIdDoesNotExist => ErrorCodes::InvalidArgument,
            PositionalPostingListBuilderError::UnsortedPosition => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Debug)]
pub(crate) struct PositionalPostingListBuilder {
    doc_ids: HashSet<i32>,
    positions: HashMap<i32, Vec<i32>>,
}

impl PositionalPostingListBuilder {
    pub(crate) fn new() -> Self {
        PositionalPostingListBuilder {
            doc_ids: HashSet::new(),
            positions: HashMap::new(),
        }
    }

    pub(crate) fn add_doc_id_and_positions(
        &mut self,
        doc_id: i32,
        positions: Vec<i32>,
    ) -> Result<(), PositionalPostingListBuilderError> {
        if self.doc_ids.contains(&doc_id) {
            return Err(PositionalPostingListBuilderError::DocIdAlreadyExists);
        }

        self.doc_ids.insert(doc_id);
        self.positions.insert(doc_id, positions);
        Ok(())
    }

    pub(crate) fn delete_doc_id(
        &mut self,
        doc_id: i32,
    ) -> Result<(), PositionalPostingListBuilderError> {
        self.doc_ids.remove(&doc_id);
        self.positions.remove(&doc_id);
        Ok(())
    }

    pub(crate) fn contains_doc_id(&self, doc_id: i32) -> bool {
        self.doc_ids.contains(&doc_id)
    }

    pub(crate) fn add_positions_for_doc_id(
        &mut self,
        doc_id: i32,
        positions: Vec<i32>,
    ) -> Result<(), PositionalPostingListBuilderError> {
        if !self.doc_ids.contains(&doc_id) {
            return Err(PositionalPostingListBuilderError::DocIdDoesNotExist);
        }

        // Safe to unwrap here since this is called for >= 2nd time a token
        // exists in the document.
        self.positions.get_mut(&doc_id).unwrap().extend(positions);
        Ok(())
    }

    pub(crate) fn build(&mut self) -> PositionalPostingList {
        let mut doc_ids_builder = Int32Builder::new();
        let mut positions_builder = ListBuilder::new(Int32Builder::new());

        let mut doc_ids_vec: Vec<i32> = self.doc_ids.drain().collect();
        doc_ids_vec.sort();
        let doc_ids_slice = doc_ids_vec.as_slice();
        doc_ids_builder.append_slice(doc_ids_slice);
        let doc_ids = doc_ids_builder.finish();

        for doc_id in doc_ids_slice.iter() {
            // Get positions for the doc ID, sort them, put them into the positions_builder
            let mut positions = self.positions.remove(doc_id).unwrap();
            positions.sort();
            let positions_as_some: Vec<Option<i32>> = positions.into_iter().map(Some).collect();
            positions_builder.append_value(positions_as_some);
        }
        let positions = positions_builder.finish();

        PositionalPostingList {
            doc_ids: doc_ids,
            positions: positions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_positional_posting_list_single_document() {
        let mut builder = PositionalPostingListBuilder::new();
        let _res = builder.add_doc_id_and_positions(1, vec![1, 2, 3]);
        let list = builder.build();
        assert_eq!(list.get_doc_ids().values()[0], 1);
        assert_eq!(
            list.get_positions_for_doc_id(1).unwrap(),
            Int32Array::from(vec![1, 2, 3])
        );
    }

    #[test]
    fn test_positional_posting_list_multiple_documents() {
        let mut builder = PositionalPostingListBuilder::new();
        let _res = builder.add_doc_id_and_positions(1, vec![1, 2, 3]);
        let _res = builder.add_doc_id_and_positions(2, vec![4, 5, 6]);
        let list = builder.build();
        assert_eq!(list.get_doc_ids().values()[0], 1);
        assert_eq!(list.get_doc_ids().values()[1], 2);
        assert_eq!(
            list.get_positions_for_doc_id(1).unwrap(),
            Int32Array::from(vec![1, 2, 3])
        );
        assert_eq!(
            list.get_positions_for_doc_id(2).unwrap(),
            Int32Array::from(vec![4, 5, 6])
        );
    }

    #[test]
    fn test_positional_posting_list_document_ids_sorted_after_build() {
        let mut builder = PositionalPostingListBuilder::new();
        let _res = builder.add_doc_id_and_positions(2, vec![4, 5, 6]);
        let _res = builder.add_doc_id_and_positions(1, vec![1, 2, 3]);
        let list = builder.build();
        assert_eq!(list.get_doc_ids().values()[0], 1);
        assert_eq!(list.get_doc_ids().values()[1], 2);
        assert_eq!(
            list.get_positions_for_doc_id(1).unwrap(),
            Int32Array::from(vec![1, 2, 3])
        );
        assert_eq!(
            list.get_positions_for_doc_id(2).unwrap(),
            Int32Array::from(vec![4, 5, 6])
        );
    }

    #[test]
    fn test_positional_posting_list_all_positions_sorted_after_build() {
        let mut builder = PositionalPostingListBuilder::new();
        let _res = builder.add_doc_id_and_positions(1, vec![3, 2, 1]);
        let list = builder.build();
        assert_eq!(list.get_doc_ids().values()[0], 1);
        assert_eq!(
            list.get_positions_for_doc_id(1).unwrap(),
            Int32Array::from(vec![1, 2, 3])
        );
    }

    #[test]
    fn test_positional_posting_list_incremental_build() {
        let mut builder = PositionalPostingListBuilder::new();

        let _res = builder.add_doc_id_and_positions(1, vec![1, 2, 3]);
        let _res = builder.add_positions_for_doc_id(1, [4].into());
        let _res = builder.add_positions_for_doc_id(1, [5].into());
        let _res = builder.add_positions_for_doc_id(1, [6].into());
        let _res = builder.add_doc_id_and_positions(2, vec![4, 5, 6]);
        let _res = builder.add_positions_for_doc_id(2, [7].into());

        let list = builder.build();
        assert_eq!(list.get_doc_ids().values()[0], 1);
        assert_eq!(list.get_doc_ids().values()[1], 2);
        assert_eq!(
            list.get_positions_for_doc_id(1).unwrap(),
            Int32Array::from(vec![1, 2, 3, 4, 5, 6])
        );
    }

    #[test]
    fn test_positional_posting_list_delete_doc_id() {
        let mut builder = PositionalPostingListBuilder::new();

        let _res = builder.add_doc_id_and_positions(1, vec![1, 2, 3]);
        let _res = builder.add_doc_id_and_positions(2, vec![4, 5, 6]);
        let _res = builder.delete_doc_id(1);

        let list = builder.build();
        assert_eq!(list.get_doc_ids().values()[0], 2);
        assert_eq!(
            list.get_positions_for_doc_id(2).unwrap(),
            Int32Array::from(vec![4, 5, 6])
        );
    }

    #[test]
    fn test_all_positional_posting_list_behaviors_together() {
        let mut builder = PositionalPostingListBuilder::new();

        let _res = builder.add_doc_id_and_positions(1, vec![3, 2, 1]);
        let _res = builder.add_positions_for_doc_id(1, [4].into());
        let _res = builder.add_positions_for_doc_id(1, [6].into());
        let _res = builder.add_positions_for_doc_id(1, [5].into());
        let _res = builder.add_doc_id_and_positions(2, vec![5, 4, 6]);
        let _res = builder.add_positions_for_doc_id(2, [7].into());

        let list = builder.build();
        assert_eq!(list.get_doc_ids().values()[0], 1);
        assert_eq!(list.get_doc_ids().values()[1], 2);
        assert_eq!(
            list.get_positions_for_doc_id(1).unwrap(),
            Int32Array::from(vec![1, 2, 3, 4, 5, 6])
        );
        assert_eq!(
            list.get_positions_for_doc_id(2).unwrap(),
            Int32Array::from(vec![4, 5, 6, 7])
        );
    }
}
