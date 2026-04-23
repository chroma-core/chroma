#[derive(Clone, Debug)]
pub struct HierarchicalInternalNode<'data> {
    pub parent: u32,
    pub centroid_code: &'data [u8],
    pub children: &'data [u32],
}

impl HierarchicalInternalNode<'_> {
    pub fn compute_size(&self) -> usize {
        std::mem::size_of::<u32>() + self.centroid_code.len() + std::mem::size_of_val(self.children)
    }
}

#[derive(Clone, Debug)]
pub struct HierarchicalInternalNodeOwned {
    pub parent: u32,
    pub centroid_code: Vec<u8>,
    pub children: Vec<u32>,
}

impl From<HierarchicalInternalNode<'_>> for HierarchicalInternalNodeOwned {
    fn from(value: HierarchicalInternalNode<'_>) -> Self {
        Self {
            parent: value.parent,
            centroid_code: value.centroid_code.to_vec(),
            children: value.children.to_vec(),
        }
    }
}

impl<'data> From<&'data HierarchicalInternalNodeOwned> for HierarchicalInternalNode<'data> {
    fn from(value: &'data HierarchicalInternalNodeOwned) -> Self {
        Self {
            parent: value.parent,
            centroid_code: &value.centroid_code,
            children: &value.children,
        }
    }
}
