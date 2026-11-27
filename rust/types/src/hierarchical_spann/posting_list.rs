use std::mem::size_of_val;

#[derive(Clone, Debug)]
pub struct HierarchicalSpannPostingList<'data> {
    pub codes: &'data [u8],
    pub ids: &'data [u32],
    pub versions: &'data [u8],
}

impl HierarchicalSpannPostingList<'_> {
    pub fn compute_size(&self) -> usize {
        size_of_val(self.codes) + size_of_val(self.ids) + size_of_val(self.versions)
    }
}

#[derive(Clone, Debug)]
pub struct HierarchicalSpannPostingListOwned {
    pub codes: Vec<u8>,
    pub ids: Vec<u32>,
    pub versions: Vec<u8>,
}

impl From<HierarchicalSpannPostingList<'_>> for HierarchicalSpannPostingListOwned {
    fn from(value: HierarchicalSpannPostingList<'_>) -> Self {
        Self {
            codes: value.codes.to_vec(),
            ids: value.ids.to_vec(),
            versions: value.versions.to_vec(),
        }
    }
}

impl<'data> From<&'data HierarchicalSpannPostingListOwned> for HierarchicalSpannPostingList<'data> {
    fn from(value: &'data HierarchicalSpannPostingListOwned) -> Self {
        Self {
            codes: &value.codes,
            ids: &value.ids,
            versions: &value.versions,
        }
    }
}
