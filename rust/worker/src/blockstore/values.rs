use arrow::{
    array::{AsArray, Int32Array, Int32Builder, ListArray, ListBuilder},
    datatypes::Int32Type,
};

#[derive(Debug, Clone)]
pub(crate) struct PositionalPostingList {
    pub(crate) doc_ids: Int32Array,
    pub(crate) positions: ListArray,
}

pub(crate) struct PositionalPostingListBuilder {
    doc_ids_builder: Int32Builder,
    positions_builder: ListBuilder<Int32Builder>,
}

impl PositionalPostingListBuilder {
    pub(crate) fn new() -> Self {
        PositionalPostingListBuilder {
            doc_ids_builder: Int32Builder::new(),
            positions_builder: ListBuilder::new(Int32Builder::new()),
        }
    }
}

impl PositionalPostingList {
    pub(crate) fn get_doc_ids(&self) -> Int32Array {
        return self.doc_ids.clone();
    }

    pub(crate) fn get_positions_for_doc_id(&self, doc_id: i32) -> Option<Int32Array> {
        let index = self.doc_ids.iter().position(|x| x == Some(doc_id));
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
}

impl PositionalPostingListBuilder {
    pub(crate) fn add_doc_id_and_positions(&mut self, doc_id: i32, positions: Vec<i32>) {
        // TODO: make sure doc_id is not already in the list. Make sure positions is deduped
        self.doc_ids_builder.append_value(doc_id);
        let positions = positions
            .into_iter()
            .map(Some)
            .collect::<Vec<Option<i32>>>();
        self.positions_builder.append_value(positions);
    }

    pub(crate) fn build(&mut self) -> PositionalPostingList {
        let doc_ids = self.doc_ids_builder.finish();
        let positions = self.positions_builder.finish();
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
    fn test_positional_posting_list() {
        let mut builder = PositionalPostingListBuilder::new();

        builder.add_doc_id_and_positions(1, vec![1, 2, 3]);
        builder.add_doc_id_and_positions(2, vec![4, 5, 6]);

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
}
