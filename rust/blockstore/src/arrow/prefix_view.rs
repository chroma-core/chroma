use std::cmp::Ordering;

use arrow::array::BinaryArray;

use super::block::Block;
use crate::arrow::types::{ArrowReadableKey, ArrowReadableValue};

pub(crate) struct PrefixSegment<'block> {
    pub(crate) block: &'block Block,
    pub(crate) offset: usize,
    pub(crate) length: usize,
}

/// A resolved view of all entries under a single prefix across one or more
/// Arrow blocks.  Supports efficient random access by key without per-call
/// blockfile overhead (no CompositeKey allocation, no sparse-index lookup,
/// no RwLock acquisition).
pub struct PrefixView<'block> {
    segments: Vec<PrefixSegment<'block>>,
}

impl<'block> PrefixView<'block> {
    pub(crate) fn new(segments: Vec<PrefixSegment<'block>>) -> Self {
        Self { segments }
    }

    pub fn is_empty(&self) -> bool {
        self.segments.iter().all(|s| s.length == 0)
    }

    pub fn len(&self) -> usize {
        self.segments.iter().map(|s| s.length).sum()
    }

    /// Point lookup by key.  Binary search within the pre-identified prefix
    /// range, then deserialize just the one hit.
    pub fn get<K: ArrowReadableKey<'block>, V: ArrowReadableValue<'block>>(
        &self,
        key: K,
    ) -> Option<V> {
        for seg in &self.segments {
            if seg.length == 0 {
                continue;
            }
            if let Ok(index) = binary_search_key_in_segment::<K>(seg, &key) {
                return Some(V::get(seg.block.data.column(2), index));
            }
        }
        None
    }

    /// Zero-copy raw byte access from the Arrow BinaryArray.
    /// Only works when the value column is Binary (e.g. SparsePostingBlock).
    pub fn get_raw_binary<K: ArrowReadableKey<'block>>(&self, key: K) -> Option<&'block [u8]> {
        for seg in &self.segments {
            if seg.length == 0 {
                continue;
            }
            if let Ok(index) = binary_search_key_in_segment::<K>(seg, &key) {
                let arr = seg
                    .block
                    .data
                    .column(2)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("value column should be BinaryArray for get_raw_binary");
                return Some(arr.value(index));
            }
        }
        None
    }

    /// Iterate all (key, value) pairs across segments.
    pub fn iter<'a, K: ArrowReadableKey<'block> + 'a, V: ArrowReadableValue<'block> + 'a>(
        &'a self,
    ) -> impl Iterator<Item = (K, V)> + 'a + use<'a, 'block, K, V> {
        self.segments.iter().flat_map(|seg| {
            let keys = K::get_range(seg.block.data.column(1), seg.offset, seg.length);
            let values = V::get_range(seg.block.data.column(2), seg.offset, seg.length);
            keys.into_iter().zip(values)
        })
    }

    /// Collect all raw binary values in key-sorted order across segments.
    /// O(N) linear scan -- one `downcast_ref` per segment, not per entry.
    pub fn collect_raw_binary_in_order(&self) -> Vec<&'block [u8]> {
        let mut result = Vec::with_capacity(self.len());
        for seg in &self.segments {
            let arr = seg
                .block
                .data
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("value column should be BinaryArray");
            for i in seg.offset..seg.offset + seg.length {
                result.push(arr.value(i));
            }
        }
        result
    }
}

fn binary_search_key_in_segment<'block, K: ArrowReadableKey<'block>>(
    seg: &PrefixSegment<'block>,
    target: &K,
) -> Result<usize, usize> {
    let col = seg.block.data.column(1);
    let mut lo = seg.offset;
    let mut hi = seg.offset + seg.length;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let mid_key = K::get(col, mid);
        match mid_key.partial_cmp(target) {
            Some(Ordering::Less) => lo = mid + 1,
            Some(Ordering::Greater) => hi = mid,
            Some(Ordering::Equal) => return Ok(mid),
            None => lo = mid + 1,
        }
    }
    Err(lo)
}
