use std::mem::size_of;

use chroma_types::DataRecord;
use roaring::RoaringBitmap;

pub trait Value: Clone {
    fn get_size(&self) -> usize;
}

impl Value for Vec<u32> {
    fn get_size(&self) -> usize {
        self.len() * size_of::<u32>()
    }
}

impl Value for &[u32] {
    fn get_size(&self) -> usize {
        std::mem::size_of_val(*self)
    }
}

impl Value for &str {
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl Value for String {
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl Value for u32 {
    fn get_size(&self) -> usize {
        4
    }
}

impl Value for RoaringBitmap {
    fn get_size(&self) -> usize {
        self.serialized_size()
    }
}

impl Value for &RoaringBitmap {
    fn get_size(&self) -> usize {
        self.serialized_size()
    }
}

impl<'a> Value for DataRecord<'a> {
    fn get_size(&self) -> usize {
        DataRecord::get_size(self)
    }
}

impl<'a> Value for &DataRecord<'a> {
    fn get_size(&self) -> usize {
        DataRecord::get_size(self)
    }
}
