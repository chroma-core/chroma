use super::types::Block;
use crate::blockstore::arrow::types::{ArrowReadableKey, ArrowReadableValue};
use arrow::array::{Array, StringArray};

/// An iterator over the contents of a block.
/// This is a simple wrapper around the Arrow array data that is stored in the block.
/// For now, it clones the data in the Block, since it is only used to populate BlockDeltas.
pub struct BlockIterator<'a, K: ArrowReadableKey<'a>, V: ArrowReadableValue<'a>> {
    block: &'a Block,
    index: usize,
    phantom: std::marker::PhantomData<&'a (K, V)>,
}

impl<'a, K: ArrowReadableKey<'a>, V: ArrowReadableValue<'a>> BlockIterator<'a, K, V> {
    pub fn new(block: &'a Block) -> Self {
        Self {
            block,
            index: 0,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, K: ArrowReadableKey<'a>, V: ArrowReadableValue<'a>> Iterator for BlockIterator<'a, K, V> {
    type Item = (&'a str, K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.block.len() {
            return None;
        }

        let prefix_arr = self
            .block
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let key_arr = self.block.data.column(1);
        let value_arr = self.block.data.column(2);

        let prefix = prefix_arr.value(self.index);
        let key = K::get(key_arr, self.index);
        let value = V::get(value_arr, self.index);

        self.index += 1;

        Some((prefix, key, value))
    }
}
