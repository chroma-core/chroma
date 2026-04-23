#[derive(Clone, Debug)]
pub struct HierarchicalLeafNode<'data> {
    pub parent: u32,
    pub length: u32,
    pub centroid_code: &'data [u8],
}

impl HierarchicalLeafNode<'_> {
    pub fn compute_size(&self) -> usize {
        std::mem::size_of::<u32>() * 2 + self.centroid_code.len()
    }
}

#[derive(Clone, Debug)]
pub struct HierarchicalLeafNodeOwned {
    pub parent: u32,
    pub length: u32,
    pub centroid_code: Vec<u8>,
}

impl From<HierarchicalLeafNode<'_>> for HierarchicalLeafNodeOwned {
    fn from(value: HierarchicalLeafNode<'_>) -> Self {
        Self {
            parent: value.parent,
            length: value.length,
            centroid_code: value.centroid_code.to_vec(),
        }
    }
}

impl<'data> From<&'data HierarchicalLeafNodeOwned> for HierarchicalLeafNode<'data> {
    fn from(value: &'data HierarchicalLeafNodeOwned) -> Self {
        Self {
            parent: value.parent,
            length: value.length,
            centroid_code: &value.centroid_code,
        }
    }
}
