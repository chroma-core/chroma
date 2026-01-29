use std::{mem::size_of_val, sync::Arc};

#[derive(Clone, Debug)]
pub struct QuantizedCluster<'data> {
    pub center: &'data [f32],
    pub codes: &'data [u8],
    pub ids: &'data [u64],
    pub versions: &'data [u64],
}

impl QuantizedCluster<'_> {
    pub fn compute_size(&self) -> usize {
        size_of_val(self.center)
            + size_of_val(self.codes)
            + size_of_val(self.ids)
            + size_of_val(self.versions)
    }
}

#[derive(Clone, Debug)]
pub struct QuantizedClusterOwned {
    pub center: Arc<[f32]>,
    pub codes: Vec<u8>,
    pub ids: Vec<u64>,
    pub versions: Vec<u64>,
}

impl From<QuantizedCluster<'_>> for QuantizedClusterOwned {
    fn from(value: QuantizedCluster<'_>) -> Self {
        Self {
            center: value.center.into(),
            codes: value.codes.to_vec(),
            ids: value.ids.to_vec(),
            versions: value.versions.to_vec(),
        }
    }
}

impl<'data> From<&'data QuantizedClusterOwned> for QuantizedCluster<'data> {
    fn from(value: &'data QuantizedClusterOwned) -> Self {
        Self {
            center: &value.center,
            codes: &value.codes,
            ids: &value.ids,
            versions: &value.versions,
        }
    }
}

impl QuantizedClusterOwned {
    /// Create a new empty cluster with the given centroid.
    pub fn new(center: Arc<[f32]>) -> Self {
        Self {
            center,
            codes: Vec::new(),
            ids: Vec::new(),
            versions: Vec::new(),
        }
    }

    /// Append a point to the cluster.
    pub fn append(&mut self, id: u64, version: u64, code: &[u8]) {
        self.ids.push(id);
        self.versions.push(version);
        self.codes.extend_from_slice(code);
    }
}
