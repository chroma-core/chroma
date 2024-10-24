use std::fmt::{Debug, Display};

use crate::key::KeyWrapper;

pub trait Key: PartialEq + Debug + Display + Into<KeyWrapper> + Clone {
    fn get_size(&self) -> usize;
}

impl Key for &str {
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl Key for f32 {
    fn get_size(&self) -> usize {
        4
    }
}

impl Key for bool {
    fn get_size(&self) -> usize {
        1
    }
}

impl Key for u32 {
    fn get_size(&self) -> usize {
        4
    }
}
