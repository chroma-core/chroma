use std::mem::size_of;

use chroma_types::{DataRecord, SpannPostingList};
use roaring::RoaringBitmap;

pub trait Value: Clone + Send + Sync {
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

impl Value for f32 {
    fn get_size(&self) -> usize {
        4
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

impl Value for DataRecord<'_> {
    fn get_size(&self) -> usize {
        DataRecord::get_size(self)
    }
}

impl Value for &DataRecord<'_> {
    fn get_size(&self) -> usize {
        DataRecord::get_size(self)
    }
}

impl Value for SpannPostingList<'_> {
    fn get_size(&self) -> usize {
        self.compute_size()
    }
}

impl Value for &SpannPostingList<'_> {
    fn get_size(&self) -> usize {
        self.compute_size()
    }
}
